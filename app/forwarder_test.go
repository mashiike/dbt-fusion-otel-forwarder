package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/mock/gomock"
)

func TestNewForwarder(t *testing.T) {
	t.Run("basic configuration", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
			},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.NotNil(t, fw)
		assert.Equal(t, "test-forwarder", fw.name)
		assert.NotNil(t, fw.resourceAttributes)
	})

	t.Run("with custom resource attributes", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Resource: &ForwardResourceConfig{
				Attributes: map[string]any{
					"service.name": "custom-service",
					"environment":  "production",
				},
			},
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.NotNil(t, fw)

		attrs := convertAttributesToMap(fw.resourceAttributes)
		assert.Equal(t, "custom-service", attrs["service.name"])
		assert.Equal(t, "production", attrs["environment"])
	})

	t.Run("with span attribute modifiers", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		whenExpr := `attributes["test"] == "value"`
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action: "set",
						Key:    "modified",
						Value:  true,
						When:   &whenExpr,
					},
				},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.Len(t, fw.spanAttributeModifiers, 1)
	})

	t.Run("with log attribute modifiers", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action: "set",
						Key:    "source",
						Value:  `"dbt-fusion"`,
					},
				},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.Len(t, fw.logAttributeModifiers, 1)
	})

	t.Run("with multiple exporters", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter1 := NewMockExporter(ctrl)
		mockExporter2 := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"exporter1": mockExporter1,
			"exporter2": mockExporter2,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"exporter1", "exporter2"},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.NotNil(t, fw.tracesExporter)
	})

	t.Run("without exporters", func(t *testing.T) {
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs:   &LogsForwardConfig{},
		}
		exporters := map[string]Exporter{}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)
		assert.Nil(t, fw.logsExporter)
		assert.Nil(t, fw.tracesExporter)
	})
}

func TestForwarder_StartStop(t *testing.T) {
	t.Run("start and stop with exporters", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
			},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		ctx := context.Background()
		mockExporter.EXPECT().Start(ctx).Return(nil).Times(2)
		err = fw.Start(ctx)
		assert.NoError(t, err)

		mockExporter.EXPECT().Stop(ctx).Return(nil).Times(2)
		err = fw.Stop(ctx)
		assert.NoError(t, err)
	})

	t.Run("start and stop without exporters", func(t *testing.T) {
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs:   &LogsForwardConfig{},
		}
		exporters := map[string]Exporter{}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		ctx := context.Background()
		err = fw.Start(ctx)
		assert.NoError(t, err)

		err = fw.Stop(ctx)
		assert.NoError(t, err)
	})
}

func TestForwarder_UploadTraces(t *testing.T) {
	t.Run("basic trace upload", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeSpans := &tracepb.ScopeSpans{
			Spans: []*tracepb.Span{
				{
					Name:    "test-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
					Attributes: []*commonpb.KeyValue{
						{Key: "key1", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value1"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadTraces(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
				require.Len(t, protoSpans, 1)
				assert.NotNil(t, protoSpans[0].Resource)
				assert.Len(t, protoSpans[0].ScopeSpans, 1)
				assert.Len(t, protoSpans[0].ScopeSpans[0].Spans, 1)
				return nil
			},
		).Return(nil)

		err = fw.UploadTraces(ctx, scopeSpans)
		assert.NoError(t, err)
	})

	t.Run("trace upload with attribute modifier - set action", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action:    "set",
						Key:       "added_attribute",
						ValueExpr: `"test_value"`,
					},
				},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeSpans := &tracepb.ScopeSpans{
			Spans: []*tracepb.Span{
				{
					Name:    "test-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
					Attributes: []*commonpb.KeyValue{
						{Key: "original", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadTraces(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
				span := protoSpans[0].ScopeSpans[0].Spans[0]
				attrs := convertAttributesToMap(span.Attributes)
				assert.Equal(t, "value", attrs["original"])
				assert.Equal(t, "test_value", attrs["added_attribute"])
				return nil
			},
		).Return(nil)

		err = fw.UploadTraces(ctx, scopeSpans)
		assert.NoError(t, err)
	})

	t.Run("trace upload with attribute modifier - conditional set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		whenExpr := `name == "test-span"`
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action:    "set",
						Key:       "conditional_attr",
						ValueExpr: `"matched"`,
						When:      &whenExpr,
					},
				},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeSpans := &tracepb.ScopeSpans{
			Spans: []*tracepb.Span{
				{
					Name:    "test-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
				},
				{
					Name:    "other-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{9, 10, 11, 12, 13, 14, 15, 16},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadTraces(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
				spans := protoSpans[0].ScopeSpans[0].Spans
				span1Attrs := convertAttributesToMap(spans[0].Attributes)
				span2Attrs := convertAttributesToMap(spans[1].Attributes)
				assert.Equal(t, "matched", span1Attrs["conditional_attr"])
				assert.NotContains(t, span2Attrs, "conditional_attr")
				return nil
			},
		).Return(nil)

		err = fw.UploadTraces(ctx, scopeSpans)
		assert.NoError(t, err)
	})

	t.Run("trace upload with attribute modifier - remove action", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action: "remove",
						Key:    "remove_me",
					},
				},
			},
			Logs: &LogsForwardConfig{},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeSpans := &tracepb.ScopeSpans{
			Spans: []*tracepb.Span{
				{
					Name:    "test-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
					Attributes: []*commonpb.KeyValue{
						{Key: "keep_me", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value1"}}},
						{Key: "remove_me", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value2"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadTraces(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
				span := protoSpans[0].ScopeSpans[0].Spans[0]
				attrs := convertAttributesToMap(span.Attributes)
				assert.Equal(t, "value1", attrs["keep_me"])
				assert.NotContains(t, attrs, "remove_me")
				return nil
			},
		).Return(nil)

		err = fw.UploadTraces(ctx, scopeSpans)
		assert.NoError(t, err)
	})

	t.Run("trace upload without exporter", func(t *testing.T) {
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs:   &LogsForwardConfig{},
		}
		exporters := map[string]Exporter{}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeSpans := &tracepb.ScopeSpans{
			Spans: []*tracepb.Span{
				{
					Name:    "test-span",
					TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
				},
			},
		}

		ctx := context.Background()
		err = fw.UploadTraces(ctx, scopeSpans)
		assert.NoError(t, err)
	})
}

func TestForwarder_UploadLogs(t *testing.T) {
	t.Run("basic log upload", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeLogs := &logspb.ScopeLogs{
			LogRecords: []*logspb.LogRecord{
				{
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "test log message"},
					},
					Attributes: []*commonpb.KeyValue{
						{Key: "level", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "info"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadLogs(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoLogs []*logspb.ResourceLogs) error {
				require.Len(t, protoLogs, 1)
				assert.NotNil(t, protoLogs[0].Resource)
				assert.Len(t, protoLogs[0].ScopeLogs, 1)
				assert.Len(t, protoLogs[0].ScopeLogs[0].LogRecords, 1)
				return nil
			},
		).Return(nil)

		err = fw.UploadLogs(ctx, scopeLogs)
		assert.NoError(t, err)
	})

	t.Run("log upload with attribute modifier - set action", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action:    "set",
						Key:       "source",
						ValueExpr: `"dbt-fusion"`,
					},
				},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeLogs := &logspb.ScopeLogs{
			LogRecords: []*logspb.LogRecord{
				{
					Body: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "test log"},
					},
					Attributes: []*commonpb.KeyValue{
						{Key: "original", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadLogs(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoLogs []*logspb.ResourceLogs) error {
				log := protoLogs[0].ScopeLogs[0].LogRecords[0]
				attrs := convertAttributesToMap(log.Attributes)
				assert.Equal(t, "value", attrs["original"])
				assert.Equal(t, "dbt-fusion", attrs["source"])
				return nil
			},
		).Return(nil)

		err = fw.UploadLogs(ctx, scopeLogs)
		assert.NoError(t, err)
	})

	t.Run("log upload with attribute modifier - conditional set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		whenExpr := `severityText == "ERROR"`
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action: "set",
						Key:    "alert",
						Value:  true,
						When:   &whenExpr,
					},
				},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeLogs := &logspb.ScopeLogs{
			LogRecords: []*logspb.LogRecord{
				{
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "error message"}},
					SeverityText: "ERROR",
				},
				{
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "info message"}},
					SeverityText: "INFO",
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadLogs(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoLogs []*logspb.ResourceLogs) error {
				logs := protoLogs[0].ScopeLogs[0].LogRecords
				log1Attrs := convertAttributesToMap(logs[0].Attributes)
				log2Attrs := convertAttributesToMap(logs[1].Attributes)
				assert.Equal(t, true, log1Attrs["alert"])
				assert.NotContains(t, log2Attrs, "alert")
				return nil
			},
		).Return(nil)

		err = fw.UploadLogs(ctx, scopeLogs)
		assert.NoError(t, err)
	})

	t.Run("log upload with attribute modifier - remove action", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockExporter := NewMockExporter(ctrl)
		exporters := map[string]Exporter{
			"test-exporter": mockExporter,
		}

		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs: &LogsForwardConfig{
				Exporters: []string{"test-exporter"},
				Attributes: []AttributeModifierConfig{
					{
						Action: "remove",
						Key:    "sensitive",
					},
				},
			},
		}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeLogs := &logspb.ScopeLogs{
			LogRecords: []*logspb.LogRecord{
				{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}},
					Attributes: []*commonpb.KeyValue{
						{Key: "public", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "data"}}},
						{Key: "sensitive", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "secret"}}},
					},
				},
			},
		}

		ctx := context.Background()
		mockExporter.EXPECT().UploadLogs(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, protoLogs []*logspb.ResourceLogs) error {
				log := protoLogs[0].ScopeLogs[0].LogRecords[0]
				attrs := convertAttributesToMap(log.Attributes)
				assert.Equal(t, "data", attrs["public"])
				assert.NotContains(t, attrs, "sensitive")
				return nil
			},
		).Return(nil)

		err = fw.UploadLogs(ctx, scopeLogs)
		assert.NoError(t, err)
	})

	t.Run("log upload without exporter", func(t *testing.T) {
		cfg := ForwardConfig{
			Traces: &TracesForwardConfig{},
			Logs:   &LogsForwardConfig{},
		}
		exporters := map[string]Exporter{}

		fw, err := NewForwarder("test-forwarder", cfg, exporters)
		require.NoError(t, err)

		scopeLogs := &logspb.ScopeLogs{
			LogRecords: []*logspb.LogRecord{
				{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}},
				},
			},
		}

		ctx := context.Background()
		err = fw.UploadLogs(ctx, scopeLogs)
		assert.NoError(t, err)
	})
}

func TestAttributeModifier_Apply(t *testing.T) {
	t.Run("set action with static value", func(t *testing.T) {
		modifier := &attributeModifier{
			action: "set",
			key:    "test_key",
			value:  "test_value",
		}

		attrs := map[string]any{
			"existing": "value",
		}

		result, err := modifier.Apply(nil, attrs)
		require.NoError(t, err)
		assert.Equal(t, "value", result["existing"])
		assert.Equal(t, "test_value", result["test_key"])
	})

	t.Run("remove action", func(t *testing.T) {
		modifier := &attributeModifier{
			action: "remove",
			key:    "remove_me",
		}

		attrs := map[string]any{
			"keep_me":   "value1",
			"remove_me": "value2",
		}

		result, err := modifier.Apply(nil, attrs)
		require.NoError(t, err)
		assert.Equal(t, "value1", result["keep_me"])
		assert.NotContains(t, result, "remove_me")
	})

	t.Run("set action with CEL expression value", func(t *testing.T) {
		env, err := NewSpanEnv()
		require.NoError(t, err)

		cfg := AttributeModifierConfig{
			Action:    "set",
			Key:       "span_name_with_prefix",
			ValueExpr: `"prefix_" + name`,
		}

		modifier, err := newAttributeModifier(cfg, env)
		require.NoError(t, err)

		span := &tracepb.Span{
			Name:    "test-span",
			TraceId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			SpanId:  []byte{1, 2, 3, 4, 5, 6, 7, 8},
		}
		spanObj := SpanForEval(span)

		attrs := map[string]any{}
		result, err := modifier.Apply(spanObj, attrs)
		require.NoError(t, err)
		assert.Equal(t, "prefix_test-span", result["span_name_with_prefix"])
	})
}
