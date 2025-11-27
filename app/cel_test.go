package app

import (
	"slices"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestNewSpanEnvEval(t *testing.T) {
	env, err := NewSpanEnv()
	if err != nil {
		t.Fatalf("NewSpanEnv returned error: %v", err)
	}

	ast, issues := env.Compile(`
		traceId == "01020304" &&
		spanId == "05060708" &&
		parentSpanId == "" &&
		name == "test-span" &&
		startTimeUnixNano == 123u &&
		endTimeUnixNano == 456u &&
		traceState == "" &&
		kind == "CLIENT" &&
		size(events) == 1 &&
		size(links) == 1 &&
		attributes["foo"] == "bar"
	`)
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile failed: %v", issues.Err())
	}
	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program creation failed: %v", err)
	}

	span := &tracepb.Span{
		TraceId:           []byte{0x01, 0x02, 0x03, 0x04},
		SpanId:            []byte{0x05, 0x06, 0x07, 0x08},
		ParentSpanId:      []byte{},
		Name:              "test-span",
		StartTimeUnixNano: 123,
		EndTimeUnixNano:   456,
		Kind:              tracepb.Span_SPAN_KIND_CLIENT,
		Attributes: []*commonpb.KeyValue{
			{
				Key:   "foo",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "bar"}},
			},
		},
		Events: []*tracepb.Span_Event{
			{
				Name:         "event-1",
				TimeUnixNano: 789,
			},
		},
		Links: []*tracepb.Span_Link{
			{
				TraceId: []byte{0x0a},
				SpanId:  []byte{0x0b},
			},
		},
	}

	out, _, err := prog.Eval(SpanForEval(span))
	if err != nil {
		t.Fatalf("Eval returned error: %v", err)
	}
	val, ok := out.Value().(bool)
	if !ok {
		t.Fatalf("expected bool result, got %T", out.Value())
	}
	if !val {
		t.Fatalf("expression evaluated to false")
	}
}

func TestSpanForEvalBuildsMaps(t *testing.T) {
	span := &tracepb.Span{
		TraceId:           []byte{0xaa, 0xbb},
		SpanId:            []byte{0xcc, 0xdd},
		ParentSpanId:      []byte{0xee, 0xff},
		Name:              "span-name",
		TraceState:        "ts",
		StartTimeUnixNano: 1,
		EndTimeUnixNano:   2,
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		Status: &tracepb.Status{
			Message: "boom",
			Code:    tracepb.Status_STATUS_CODE_ERROR,
		},
		Events: []*tracepb.Span_Event{
			{
				Name:         "ev1",
				TimeUnixNano: 3,
				Attributes: []*commonpb.KeyValue{
					{Key: "flag", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
				},
			},
		},
		Links: []*tracepb.Span_Link{
			{
				TraceId: []byte{0x01, 0x02},
				SpanId:  []byte{0x03, 0x04},
				Attributes: []*commonpb.KeyValue{
					{Key: "src", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "l"}}},
				},
			},
		},
	}

	raw := SpanForEval(span)
	m, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("SpanForEval should return map[string]any, got %T", raw)
	}

	if got := m["traceId"]; got != "aabb" {
		t.Fatalf("traceId mismatch: %v", got)
	}
	if got := m["spanId"]; got != "ccdd" {
		t.Fatalf("spanId mismatch: %v", got)
	}
	if got := m["parentSpanId"]; got != "eeff" {
		t.Fatalf("parentSpanId mismatch: %v", got)
	}
	if got := m["name"]; got != "span-name" {
		t.Fatalf("name mismatch: %v", got)
	}
	if got := m["kind"]; got != "SERVER" {
		t.Fatalf("kind mismatch: %v", got)
	}

	status, ok := m["status"].(map[string]any)
	if !ok {
		t.Fatalf("status should be map[string]any, got %T", m["status"])
	}
	if status["code"] != "ERROR" || status["message"] != "boom" {
		t.Fatalf("status mismatch: %+v", status)
	}

	events, ok := m["events"].([]map[string]any)
	if !ok {
		t.Fatalf("events should be []map[string]any, got %T", m["events"])
	}
	if len(events) != 1 {
		t.Fatalf("events length mismatch: %d", len(events))
	}
	if events[0]["name"] != "ev1" {
		t.Fatalf("event name mismatch: %v", events[0]["name"])
	}
	evAttrs, ok := events[0]["attributes"].(map[string]any)
	if !ok || evAttrs["flag"] != true {
		t.Fatalf("event attributes mismatch: %+v", events[0]["attributes"])
	}

	links, ok := m["links"].([]map[string]any)
	if !ok {
		t.Fatalf("links should be []map[string]any, got %T", m["links"])
	}
	if len(links) != 1 {
		t.Fatalf("links length mismatch: %d", len(links))
	}
	if links[0]["traceId"] != "0102" || links[0]["spanId"] != "0304" {
		t.Fatalf("link ids mismatch: %+v", links[0])
	}
	linkAttrs, ok := links[0]["attributes"].(map[string]any)
	if !ok || !slices.Equal([]string{"src"}, mapKeys(linkAttrs)) || linkAttrs["src"] != "l" {
		t.Fatalf("link attributes mismatch: %+v", links[0]["attributes"])
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func TestNewLogEnvEval(t *testing.T) {
	env, err := NewLogEnv()
	if err != nil {
		t.Fatalf("NewLogEnv returned error: %v", err)
	}

	ast, issues := env.Compile(`
		traceId == "0a0b" &&
		spanId == "0c0d" &&
		timeUnixNano == 10u &&
		observedTimeUnixNano == 11u &&
		severityNumber == 5 &&
		severityText == "DEBUG" &&
		body == "hello" &&
		attributes["x"] == 1
	`)
	if issues != nil && issues.Err() != nil {
		t.Fatalf("Compile failed: %v", issues.Err())
	}
	prog, err := env.Program(ast)
	if err != nil {
		t.Fatalf("Program creation failed: %v", err)
	}

	log := &logspb.LogRecord{
		TraceId:              []byte{0x0a, 0x0b},
		SpanId:               []byte{0x0c, 0x0d},
		TimeUnixNano:         10,
		ObservedTimeUnixNano: 11,
		SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG,
		SeverityText:         "DEBUG",
		Body: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: "hello"},
		},
		Attributes: []*commonpb.KeyValue{
			{
				Key:   "x",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 1}},
			},
		},
	}

	out, _, err := prog.Eval(LogForEval(log))
	if err != nil {
		t.Fatalf("Eval returned error: %v", err)
	}
	if v, ok := out.Value().(bool); !ok || !v {
		t.Fatalf("expression evaluated to %v (type %T)", out.Value(), out.Value())
	}
}
