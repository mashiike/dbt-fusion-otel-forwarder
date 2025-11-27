package app

import (
	"encoding/hex"

	"github.com/google/cel-go/cel"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func NewSpanEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.Variable("traceId", cel.StringType),
		cel.Variable("spanId", cel.StringType),
		cel.Variable("parentSpanId", cel.StringType),
		cel.Variable("name", cel.StringType),
		cel.Variable("traceState", cel.StringType),
		cel.Variable("startTimeUnixNano", cel.UintType),
		cel.Variable("endTimeUnixNano", cel.UintType),
		cel.Variable("attributes", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("status", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("kind", cel.StringType),
		cel.Variable("events", cel.ListType(cel.MapType(cel.StringType, cel.DynType))),
		cel.Variable("links", cel.ListType(cel.MapType(cel.StringType, cel.DynType))),
	)
	return env, err
}

func NewLogEnv() (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.Variable("traceId", cel.StringType),
		cel.Variable("spanId", cel.StringType),
		cel.Variable("timeUnixNano", cel.UintType),
		cel.Variable("observedTimeUnixNano", cel.UintType),
		cel.Variable("severityNumber", cel.IntType),
		cel.Variable("severityText", cel.StringType),
		cel.Variable("body", cel.DynType),
		cel.Variable("attributes", cel.MapType(cel.StringType, cel.DynType)),
	)
	return env, err
}

func SpanForEval(span *tracepb.Span) any {
	status := span.GetStatus()
	spanStatus := map[string]any{
		"code":    nil,
		"message": status.GetMessage(),
	}
	switch status.GetCode() {
	case tracepb.Status_STATUS_CODE_UNSET:
		// do nothing
	case tracepb.Status_STATUS_CODE_OK:
		spanStatus["code"] = "OK"
	case tracepb.Status_STATUS_CODE_ERROR:
		spanStatus["code"] = "ERROR"
	}
	var kind string
	switch span.GetKind() {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		kind = "INTERNAL"
	case tracepb.Span_SPAN_KIND_SERVER:
		kind = "SERVER"
	case tracepb.Span_SPAN_KIND_CLIENT:
		kind = "CLIENT"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		kind = "PRODUCER"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		kind = "CONSUMER"
	default:
		kind = "UNSPECIFIED"
	}
	events := span.GetEvents()
	spanEvents := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		spanEvents = append(spanEvents, map[string]any{
			"name":         ev.GetName(),
			"attributes":   convertAttributesToMap(ev.GetAttributes()),
			"timeUnixNano": ev.GetTimeUnixNano(),
		})
	}
	links := span.GetLinks()
	spanLinks := make([]map[string]any, 0, len(links))
	for _, link := range links {
		spanLinks = append(spanLinks, map[string]any{
			"traceId":    hex.EncodeToString(link.GetTraceId()),
			"spanId":     hex.EncodeToString(link.GetSpanId()),
			"traceState": link.GetTraceState(),
			"attributes": convertAttributesToMap(link.GetAttributes()),
		})
	}
	obj := map[string]any{
		"traceId":           hex.EncodeToString(span.GetTraceId()),
		"spanId":            hex.EncodeToString(span.GetSpanId()),
		"parentSpanId":      hex.EncodeToString(span.GetParentSpanId()),
		"name":              span.GetName(),
		"traceState":        span.GetTraceState(),
		"startTimeUnixNano": span.GetStartTimeUnixNano(),
		"endTimeUnixNano":   span.GetEndTimeUnixNano(),
		"attributes":        convertAttributesToMap(span.GetAttributes()),
		"kind":              kind,
		"status":            spanStatus,
		"events":            spanEvents,
		"links":             spanLinks,
	}
	return obj
}

func LogForEval(log *logspb.LogRecord) any {
	obj := map[string]any{
		"traceId":              hex.EncodeToString(log.GetTraceId()),
		"spanId":               hex.EncodeToString(log.GetSpanId()),
		"timeUnixNano":         log.GetTimeUnixNano(),
		"observedTimeUnixNano": log.GetObservedTimeUnixNano(),
		"severityNumber":       int64(log.GetSeverityNumber()),
		"severityText":         log.GetSeverityText(),
		"attributes":           convertAttributesToMap(log.GetAttributes()),
	}
	if body := log.GetBody(); body != nil {
		obj["body"] = getAttributeValue(body)
	}
	return obj
}

func convertAttributesToMap(attrs []*commonpb.KeyValue) map[string]any {
	result := make(map[string]any)
	for _, attr := range attrs {
		result[attr.GetKey()] = getAttributeValue(attr.GetValue())
	}
	return result
}

func getAttributeValue(val *commonpb.AnyValue) any {
	switch v := val.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_BoolValue:
		return v.BoolValue
	case *commonpb.AnyValue_IntValue:
		return v.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return v.DoubleValue
	case *commonpb.AnyValue_ArrayValue:
		arr := make([]any, 0, len(v.ArrayValue.Values))
		for _, item := range v.ArrayValue.Values {
			arr = append(arr, getAttributeValue(item))
		}
		return arr
	case *commonpb.AnyValue_KvlistValue:
		m := make(map[string]any)
		for _, kv := range v.KvlistValue.Values {
			m[kv.Key] = getAttributeValue(kv.Value)
		}
		return m
	default:
		return nil
	}
}
