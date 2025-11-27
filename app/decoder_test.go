package app

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/sebdah/goldie/v2"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestDecodeOTELLines_FailedSpans(t *testing.T) {
	// Load test data
	f, err := os.Open("testdata/otel_failed.jsonl")
	if err != nil {
		t.Fatalf("failed to open testdata: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxSize = 4 * 1024 * 1024
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, maxSize)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan testdata: %v", err)
	}

	g := goldie.New(t,
		goldie.WithFixtureDir("testdata"),
		goldie.WithNameSuffix(".golden.jsonl"),
	)

	t.Run("decode without cutoff", func(t *testing.T) {
		spans, logs, err := decodeOTELLines(lines, 0)
		if err != nil {
			t.Fatalf("decodeOTELLines failed: %v", err)
		}

		if len(spans) == 0 {
			t.Errorf("expected spans > 0, got %d", len(spans))
		}
		if len(logs) == 0 {
			t.Errorf("expected logs > 0, got %d", len(logs))
		}

		t.Logf("decoded %d spans and %d logs", len(spans), len(logs))

		// Serialize spans to JSONL
		spansJSON := serializeSpansToJSONL(t, spans)
		g.Assert(t, "decode_without_cutoff_failed.spans", spansJSON)

		// Serialize logs to JSONL
		logsJSON := serializeLogsToJSONL(t, logs)
		g.Assert(t, "decode_without_cutoff_failed.logs", logsJSON)
	})

}
func TestDecodeOTELLines(t *testing.T) {
	// Load test data
	f, err := os.Open("testdata/otel.jsonl")
	if err != nil {
		t.Fatalf("failed to open testdata: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	const maxSize = 4 * 1024 * 1024
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, maxSize)

	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan testdata: %v", err)
	}

	g := goldie.New(t,
		goldie.WithFixtureDir("testdata"),
		goldie.WithNameSuffix(".golden.jsonl"),
	)

	t.Run("decode without cutoff", func(t *testing.T) {
		spans, logs, err := decodeOTELLines(lines, 0)
		if err != nil {
			t.Fatalf("decodeOTELLines failed: %v", err)
		}

		if len(spans) == 0 {
			t.Errorf("expected spans > 0, got %d", len(spans))
		}
		if len(logs) == 0 {
			t.Errorf("expected logs > 0, got %d", len(logs))
		}

		t.Logf("decoded %d spans and %d logs", len(spans), len(logs))

		// Serialize spans to JSONL
		spansJSON := serializeSpansToJSONL(t, spans)
		g.Assert(t, "decode_without_cutoff.spans", spansJSON)

		// Serialize logs to JSONL
		logsJSON := serializeLogsToJSONL(t, logs)
		g.Assert(t, "decode_without_cutoff.logs", logsJSON)
	})

	t.Run("decode with cutoff", func(t *testing.T) {
		// Use a very high cutoff time to filter out all logs
		farFutureCutoff := uint64(9999999999999999999)
		spans, logs, err := decodeOTELLines(lines, farFutureCutoff)
		if err != nil {
			t.Fatalf("decodeOTELLines failed: %v", err)
		}

		if len(spans) != 0 {
			t.Errorf("expected 0 spans with far future cutoff, got %d", len(spans))
		}
		if len(logs) != 0 {
			t.Errorf("expected 0 logs with far future cutoff, got %d", len(logs))
		}
	})

	t.Run("decode with partial cutoff", func(t *testing.T) {
		// Use cutoff at midpoint (should filter some results)
		// First, get the time range
		allSpans, allLogs, _ := decodeOTELLines(lines, 0)
		if len(allSpans) == 0 {
			t.Skip("no spans to test with")
		}

		tracesStartTimeUnixNano := make(map[string]uint64)
		for _, span := range allSpans {
			traceID := hex.EncodeToString(span.TraceId)
			if existing, ok := tracesStartTimeUnixNano[traceID]; !ok || span.StartTimeUnixNano < existing {
				tracesStartTimeUnixNano[traceID] = span.StartTimeUnixNano
			}
		}
		var lastTraceStartTime uint64
		for _, startTime := range tracesStartTimeUnixNano {
			if startTime > lastTraceStartTime {
				lastTraceStartTime = startTime
			}
		}

		spans, logs, err := decodeOTELLines(lines, lastTraceStartTime)
		if err != nil {
			t.Fatalf("decodeOTELLines failed: %v", err)
		}

		// Should have fewer or equal results than without cutoff
		if len(spans) > len(allSpans) {
			t.Errorf("expected <= spans with cutoff, got %d vs %d", len(spans), len(allSpans))
		}
		if len(logs) > len(allLogs) {
			t.Errorf("expected <= logs with cutoff, got %d vs %d", len(logs), len(allLogs))
		}

		t.Logf("cutoff filtered %d->%d spans, %d->%d logs",
			len(allSpans), len(spans), len(allLogs), len(logs))

		// Serialize and compare with golden file
		spansJSON := serializeSpansToJSONL(t, spans)
		g.Assert(t, "decode_with_partial_cutoff.spans", spansJSON)

		logsJSON := serializeLogsToJSONL(t, logs)
		g.Assert(t, "decode_with_partial_cutoff.logs", logsJSON)
	})

	t.Run("empty input", func(t *testing.T) {
		spans, logs, err := decodeOTELLines([]string{}, 0)
		if err != nil {
			t.Fatalf("decodeOTELLines failed: %v", err)
		}
		if len(spans) != 0 {
			t.Errorf("expected 0 spans, got %d", len(spans))
		}
		if len(logs) != 0 {
			t.Errorf("expected 0 logs, got %d", len(logs))
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		invalidLines := []string{
			"not json",
			"{invalid}",
			"",
		}
		spans, logs, err := decodeOTELLines(invalidLines, 0)
		if err != nil {
			t.Fatalf("decodeOTELLines should not fail on invalid json: %v", err)
		}
		// Should gracefully skip invalid lines
		if len(spans) != 0 {
			t.Errorf("expected 0 spans from invalid input, got %d", len(spans))
		}
		if len(logs) != 0 {
			t.Errorf("expected 0 logs from invalid input, got %d", len(logs))
		}
	})
}

// decodeOTELLines is a helper function that uses Decoder to decode OTEL lines
func decodeOTELLines(lines []string, cutoffTimeNano uint64) ([]*tracepb.Span, []*logspb.LogRecord, error) {
	decoder := NewDecoder(cutoffTimeNano)
	return decoder.DecodeLines(lines)
}

func minifyJSON(input []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, input); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serializeSpansToJSONL converts spans to JSONL format using protojson
func serializeSpansToJSONL(t *testing.T, spans []*tracepb.Span) []byte {
	t.Helper()
	var result []byte
	marshaler := protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		EmitUnpopulated: false,
	}
	for _, span := range spans {
		jsonBytes, err := marshaler.Marshal(span)
		if err != nil {
			t.Fatalf("failed to marshal span: %v", err)
		}
		minified, err := minifyJSON(jsonBytes)
		if err != nil {
			t.Fatalf("failed to minify span json: %v", err)
		}
		result = append(result, minified...)
		result = append(result, '\n')
	}
	return result
}

// serializeLogsToJSONL converts log records to JSONL format using protojson
func serializeLogsToJSONL(t *testing.T, logs []*logspb.LogRecord) []byte {
	t.Helper()
	var result []byte
	marshaler := protojson.MarshalOptions{
		Multiline:       false,
		Indent:          "",
		EmitUnpopulated: false,
	}
	for _, log := range logs {
		jsonBytes, err := marshaler.Marshal(log)
		if err != nil {
			t.Fatalf("failed to marshal log: %v", err)
		}
		minified, err := minifyJSON(jsonBytes)
		if err != nil {
			t.Fatalf("failed to minify log json: %v", err)
		}
		result = append(result, minified...)
		result = append(result, '\n')
	}
	return result
}
