// Package app provides the main application logic for decoding OTEL JSONL logs
package app

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// spanPartial represents an incomplete span being assembled from SpanStart/SpanEnd
type spanPartial struct {
	traceID       string
	spanID        string
	parent        string
	name          string
	start         uint64
	end           uint64
	attrs         []*commonpb.KeyValue
	statusCode    tracepb.Status_StatusCode
	statusMessage string
}

// Decoder decodes OTEL JSONL log lines into OTLP spans and log records.
// It maintains state to match SpanStart/SpanEnd pairs and only emits complete spans.
type Decoder struct {
	cutoffTimeNano       uint64
	spanPartials         map[string]*spanPartial
	attributeTransformer func([]*commonpb.KeyValue) []*commonpb.KeyValue
}

// NewDecoder creates a new Decoder with the given cutoff time.
// Lines with timestamps before cutoffTimeNano will be skipped (for log rotation handling).
func NewDecoder(cutoffTimeNano uint64) *Decoder {
	d := &Decoder{
		cutoffTimeNano: cutoffTimeNano,
		spanPartials:   make(map[string]*spanPartial),
	}
	d.AttributeTransformer(nil)
	return d
}

func defaultAttributeTransformer(attrs []*commonpb.KeyValue) []*commonpb.KeyValue {
	result := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		key := attr.Key
		switch {
		case key == "sql":
			key = "db.statement"
		case !strings.HasPrefix(key, "dbt."):
			key = "dbt." + key
		}
		result = append(result, &commonpb.KeyValue{
			Key:   key,
			Value: attr.Value,
		})
	}
	return result
}

func (d *Decoder) AttributeTransformer(f func([]*commonpb.KeyValue) []*commonpb.KeyValue) {
	if f == nil {
		f = defaultAttributeTransformer
	}
	d.attributeTransformer = f
}

// DecodeLines parses OTEL JSONL log lines and returns complete spans and log records.
// Only spans with both SpanStart and SpanEnd are returned.
// Call Flush() at the end to get any remaining incomplete spans.
func (d *Decoder) DecodeLines(lines []string) ([]*tracepb.Span, []*logspb.LogRecord, error) {
	var completeSpans []*tracepb.Span
	var logs []*logspb.LogRecord

	for _, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		recordType := stringFrom(obj, "record_type")
		if recordType == "" {
			continue
		}

		// Check cutoff time - skip logs older than command start time
		var logTimeNano uint64
		if timeStr := stringFrom(obj, "start_time_unix_nano"); timeStr != "" {
			logTimeNano = parseNano(timeStr, 0)
		} else if timeStr := stringFrom(obj, "time_unix_nano"); timeStr != "" {
			logTimeNano = parseNano(timeStr, 0)
		}
		if logTimeNano > 0 && logTimeNano < d.cutoffTimeNano {
			continue // Skip old logs from previous runs
		}

		switch recordType {
		case "SpanStart", "SpanEnd":
			spanID := stringFrom(obj, "span_id")
			if spanID == "" {
				continue
			}

			p := d.spanPartials[spanID]
			if p == nil {
				p = &spanPartial{}
				d.spanPartials[spanID] = p
			}
			p.spanID = spanID
			if traceID := stringFrom(obj, "trace_id"); traceID != "" {
				p.traceID = traceID
			}
			if parent := stringFrom(obj, "parent_span_id"); parent != "" {
				p.parent = parent
			}

			if recordType == "SpanStart" {
				if name := stringFrom(obj, "span_name"); name != "" {
					p.name = name
				}
				if start := stringFrom(obj, "start_time_unix_nano"); start != "" {
					p.start = parseNano(start, uint64(time.Now().UnixNano()))
				}
				p.attrs = extractAttributes(obj, p.attrs)
			} else { // SpanEnd
				if end := stringFrom(obj, "end_time_unix_nano"); end != "" {
					p.end = parseNano(end, p.start)
				}
				p.attrs = extractAttributes(obj, p.attrs)

				// Extract status information from SpanEnd
				if statusObj, ok := obj["status"].(map[string]any); ok {
					if code := getInt(statusObj, "code"); code > 0 {
						p.statusCode = tracepb.Status_StatusCode(code)
					}
					if msg := stringFrom(statusObj, "message"); msg != "" {
						p.statusMessage = msg
					}
				}

				// Check for test failure in node_test_detail
				if attrsObj, ok := obj["attributes"].(map[string]any); ok {
					if testDetail, ok := attrsObj["node_test_detail"].(map[string]any); ok {
						if outcome := stringFrom(testDetail, "test_outcome"); outcome == "TEST_OUTCOME_FAILED" {
							p.statusCode = tracepb.Status_STATUS_CODE_ERROR
							if p.statusMessage == "" {
								p.statusMessage = "Test failed"
							}
						}
					}
				}

				// SpanEnd received - if we have start time, emit the complete span
				if p.start > 0 {
					span := d.buildSpan(p)
					if span != nil {
						span.Attributes = d.attributeTransformer(span.Attributes)
						completeSpans = append(completeSpans, span)
						// Remove from partials map as it's now complete
						delete(d.spanPartials, spanID)
					}
				}
			}

		case "LogRecord":
			traceID := stringFrom(obj, "trace_id")
			spanID := stringFrom(obj, "span_id")
			if traceID == "" || spanID == "" {
				continue
			}

			logRecord := &logspb.LogRecord{
				TimeUnixNano:   logTimeNano,
				TraceId:        decodeHex(traceID),
				SpanId:         decodeHex(spanID),
				SeverityNumber: logspb.SeverityNumber(getInt(obj, "severity_number")),
				SeverityText:   stringFrom(obj, "severity_text"),
				Attributes:     d.attributeTransformer(extractAttributes(obj, nil)),
			}

			// Set body from "body" field
			if body := stringFrom(obj, "body"); body != "" {
				logRecord.Body = &commonpb.AnyValue{
					Value: &commonpb.AnyValue_StringValue{StringValue: body},
				}
			}

			logs = append(logs, logRecord)
		}
	}

	// Sort complete spans by start time for deterministic output
	sortSpansByStartTime(completeSpans)

	return completeSpans, logs, nil
}

// buildSpan converts a spanPartial to a complete OTLP Span
func (d *Decoder) buildSpan(p *spanPartial) *tracepb.Span {
	if p.start == 0 {
		return nil
	}

	span := &tracepb.Span{
		Name:              p.name,
		TraceId:           decodeHex(p.traceID),
		SpanId:            decodeHex(p.spanID),
		ParentSpanId:      decodeHex(p.parent),
		StartTimeUnixNano: p.start,
		EndTimeUnixNano:   p.end,
		Attributes:        deduplicateAttributes(p.attrs),
	}

	// If end time is not set, use start time
	if span.EndTimeUnixNano == 0 {
		span.EndTimeUnixNano = span.StartTimeUnixNano
	}

	// Set status if provided
	if p.statusCode != tracepb.Status_STATUS_CODE_UNSET {
		span.Status = &tracepb.Status{
			Code:    p.statusCode,
			Message: p.statusMessage,
		}
	}

	return span
}

// sortSpansByStartTime sorts spans by their start time (ascending), then by span_id for determinism
func sortSpansByStartTime(spans []*tracepb.Span) {
	// Simple bubble sort (good enough for moderate sized arrays)
	n := len(spans)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			// Sort by start time first
			if spans[j].StartTimeUnixNano > spans[j+1].StartTimeUnixNano {
				spans[j], spans[j+1] = spans[j+1], spans[j]
			} else if spans[j].StartTimeUnixNano == spans[j+1].StartTimeUnixNano {
				// If start time is equal, sort by span_id for deterministic output
				if compareBytes(spans[j].SpanId, spans[j+1].SpanId) > 0 {
					spans[j], spans[j+1] = spans[j+1], spans[j]
				}
			}
		}
	}
}

// compareBytes compares two byte slices lexicographically
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}
	return 0
}

// deduplicateAttributes removes duplicate attributes by key (keeps first occurrence)
func deduplicateAttributes(attrs []*commonpb.KeyValue) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return attrs
	}

	seen := make(map[string]bool)
	result := make([]*commonpb.KeyValue, 0, len(attrs))

	for _, attr := range attrs {
		if !seen[attr.Key] {
			seen[attr.Key] = true
			result = append(result, attr)
		}
	}

	return result
}

// getInt extracts an integer value from a JSON object field
func getInt(obj map[string]any, key string) int32 {
	if v, ok := obj[key]; ok {
		switch val := v.(type) {
		case float64:
			return int32(val)
		case int:
			return int32(val)
		case int32:
			return val
		case int64:
			return int32(val)
		}
	}
	return 0
}

func decodeHex(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

func stringFrom(obj map[string]any, key string) string {
	if obj == nil {
		return ""
	}
	if v, ok := obj[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func parseNano(val string, fallback uint64) uint64 {
	// val may be quoted integer or decimal string; fallback to provided.
	var n uint64
	_, err := fmt.Sscan(val, &n)
	if err != nil {
		return fallback
	}
	return n
}

func extractAttributes(obj map[string]any, attrs []*commonpb.KeyValue) []*commonpb.KeyValue {
	if obj == nil {
		return attrs
	}

	// Extract only the "attributes" field from the original OTEL log,
	// ignoring fields that are already represented in OTEL standard fields
	if attrsObj, ok := obj["attributes"].(map[string]any); ok {
		// Sort keys for deterministic output
		keys := make([]string, 0, len(attrsObj))
		for k := range attrsObj {
			keys = append(keys, k)
		}
		sortStrings(keys)

		for _, k := range keys {
			attrs = append(attrs, jsonValueToKeyValue(k, attrsObj[k]))
		}
	}

	// Also extract event_type as it's useful for correlation
	if eventType := stringFrom(obj, "event_type"); eventType != "" {
		attrs = append(attrs, &commonpb.KeyValue{
			Key: "event_type",
			Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: eventType},
			},
		})
	}

	return attrs
}

// jsonValueToKeyValue converts a JSON value to an OTEL KeyValue
func jsonValueToKeyValue(key string, value any) *commonpb.KeyValue {
	kv := &commonpb.KeyValue{Key: key}

	switch v := value.(type) {
	case string:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}
	case float64:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: v}}
	case int:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(v)}}
	case int64:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
	case bool:
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: v}}
	case map[string]any:
		// Convert nested object to KeyValueList (sorted by key for determinism)
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sortStrings(keys)

		var kvList []*commonpb.KeyValue
		for _, k := range keys {
			kvList = append(kvList, jsonValueToKeyValue(k, v[k]))
		}
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{
			KvlistValue: &commonpb.KeyValueList{Values: kvList},
		}}
	case []any:
		// Convert array to ArrayValue
		var arrayValues []*commonpb.AnyValue
		for _, item := range v {
			arrayValues = append(arrayValues, jsonValueToKeyValue("", item).Value)
		}
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{Values: arrayValues},
		}}
	default:
		// Fallback to string representation
		kv.Value = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}

	return kv
}

// sortStrings sorts strings in place (simple bubble sort)
func sortStrings(strs []string) {
	n := len(strs)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if strs[j] > strs[j+1] {
				strs[j], strs[j+1] = strs[j+1], strs[j]
			}
		}
	}
}
