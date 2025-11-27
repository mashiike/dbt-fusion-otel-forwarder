package app

import (
	"context"
	"log/slog"

	"github.com/google/cel-go/cel"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Forwarder struct {
	name                   string
	resourceAttributes     []*commonpb.KeyValue
	cfg                    ForwardConfig
	logsExporter           Exporter
	tracesExporter         Exporter
	spanAttributeModifiers []*attributeModifier
	logAttributeModifiers  []*attributeModifier
}

func NewForwarder(name string, cfg ForwardConfig, exporters map[string]Exporter) (*Forwarder, error) {
	attrs := make(map[string]any)
	if cfg.Resource != nil && len(cfg.Resource.Attributes) > 0 {
		attrs = cfg.Resource.Attributes
	}
	if _, ok := attrs["service.name"]; !ok {
		attrs["service.name"] = "dbt"
	}
	spanAttrModifiers := make([]*attributeModifier, 0)
	if cfg.Traces != nil && len(cfg.Traces.Attributes) > 0 {
		env, err := NewSpanEnv()
		if err != nil {
			return nil, err
		}
		for _, modCfg := range cfg.Traces.Attributes {
			modifier, err := newAttributeModifier(modCfg, env)
			if err != nil {
				slog.Warn("failed to create span attribute modifier", "forwarder", name, "error", err)
				continue
			}
			spanAttrModifiers = append(spanAttrModifiers, modifier)
		}
	}
	logAttrModifiers := make([]*attributeModifier, 0)
	if cfg.Logs != nil && len(cfg.Logs.Attributes) > 0 {
		logEnv, err := NewLogEnv()
		if err != nil {
			return nil, err
		}
		for _, modCfg := range cfg.Logs.Attributes {
			modifier, err := newAttributeModifier(modCfg, logEnv)
			if err != nil {
				slog.Warn("failed to create log attribute modifier", "forwarder", name, "error", err)
				continue
			}
			logAttrModifiers = append(logAttrModifiers, modifier)
		}
	}
	fw := &Forwarder{
		name:                   name,
		cfg:                    cfg,
		resourceAttributes:     convertAttributesFromMap(attrs),
		spanAttributeModifiers: spanAttrModifiers,
		logAttributeModifiers:  logAttrModifiers,
	}
	logsExporters := make([]Exporter, 0)
	tracesExporters := make([]Exporter, 0)

	if cfg.Logs == nil {
		cfg.Logs = &LogsForwardConfig{}
	}
	for _, name := range cfg.Logs.Exporters {
		exp, ok := exporters[name]
		if !ok {
			slog.Warn("logs exporter not found", "name", name)
			continue
		}
		logsExporters = append(logsExporters, exp)
	}
	if len(logsExporters) == 1 {
		fw.logsExporter = logsExporters[0]
	} else if len(logsExporters) > 1 {
		fw.logsExporter = NewMultiplexExporter(logsExporters...)
	}

	if cfg.Traces == nil {
		cfg.Traces = &TracesForwardConfig{}
	}
	for _, name := range cfg.Traces.Exporters {
		exp, ok := exporters[name]
		if !ok {
			slog.Warn("traces exporter not found", "name", name)
			continue
		}
		tracesExporters = append(tracesExporters, exp)
	}
	if len(tracesExporters) == 1 {
		fw.tracesExporter = tracesExporters[0]
	} else if len(tracesExporters) > 1 {
		fw.tracesExporter = NewMultiplexExporter(tracesExporters...)
	}
	return fw, nil
}

func (f *Forwarder) Start(ctx context.Context) error {
	if f.logsExporter != nil {
		if err := f.logsExporter.Start(ctx); err != nil {
			return err
		}
	}
	if f.tracesExporter != nil {
		if err := f.tracesExporter.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (f *Forwarder) Stop(ctx context.Context) error {
	if f.logsExporter != nil {
		if err := f.logsExporter.Stop(ctx); err != nil {
			return err
		}
	}
	if f.tracesExporter != nil {
		if err := f.tracesExporter.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (f *Forwarder) UploadLogs(ctx context.Context, scopeLogs *logspb.ScopeLogs) error {
	logs := scopeLogs.GetLogRecords()
	if len(f.logAttributeModifiers) > 0 {
		for _, log := range logs {
			attrsMap := convertAttributesToMap(log.GetAttributes())
			logObj := LogForEval(log)
			for _, modifier := range f.logAttributeModifiers {
				var err error
				attrsMap, err = modifier.Apply(logObj, attrsMap)
				if err != nil {
					slog.Warn("failed to apply log attribute modifier", "forwarder", f.name, "error", err)
					continue
				}
			}
			log.Attributes = convertAttributesFromMap(attrsMap)
		}
	}
	resourceLogs := &logspb.ResourceLogs{
		Resource: &resourcepb.Resource{
			Attributes: f.resourceAttributes,
		},
		ScopeLogs: []*logspb.ScopeLogs{scopeLogs},
	}
	protoLogs := []*logspb.ResourceLogs{resourceLogs}
	if f.logsExporter != nil {
		slog.Debug("forwarder uploading logs", "forwarder", f.name, "log_count", len(logs))
		return f.logsExporter.UploadLogs(ctx, protoLogs)
	}
	return nil
}

func (f *Forwarder) UploadTraces(ctx context.Context, scopeSpans *tracepb.ScopeSpans) error {
	spans := scopeSpans.GetSpans()
	if len(f.spanAttributeModifiers) > 0 {
		for _, span := range spans {
			attrsMap := convertAttributesToMap(span.GetAttributes())
			spanObj := SpanForEval(span)
			for _, modifier := range f.spanAttributeModifiers {
				var err error
				attrsMap, err = modifier.Apply(spanObj, attrsMap)
				if err != nil {
					slog.Warn("failed to apply span attribute modifier", "forwarder", f.name, "error", err)
					continue
				}
			}
			span.Attributes = convertAttributesFromMap(attrsMap)
		}
	}
	resourceSpans := &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: f.resourceAttributes,
		},
		ScopeSpans: []*tracepb.ScopeSpans{scopeSpans},
	}
	protoSpans := []*tracepb.ResourceSpans{resourceSpans}
	if f.tracesExporter != nil {
		slog.Debug("forwarder uploading traces", "forwarder", f.name, "span_count", len(spans))
		return f.tracesExporter.UploadTraces(ctx, protoSpans)
	}
	return nil
}

func NewForwarders(ctx context.Context, cfg *Config) []*Forwarder {
	if len(cfg.Exporters) == 0 {
		slog.Warn("no exporters configured, using noop exporter")
		return []*Forwarder{}
	}
	exporters := NewExporters(ctx, cfg.Exporters)
	if len(exporters) == 0 {
		slog.Warn("no valid exporters configured, using noop exporter")
		return []*Forwarder{}
	}
	forwarders := make([]*Forwarder, 0, len(cfg.Forward))
	for name, fwCfg := range cfg.Forward {
		fw, err := NewForwarder(name, fwCfg, exporters)
		if err != nil {
			slog.Error("failed to create forwarder", "name", name, "error", err)
			continue
		}
		if err := fw.Start(context.WithoutCancel(ctx)); err != nil {
			slog.Error("failed to start forwarder", "name", name, "error", err)
			continue
		}
		forwarders = append(forwarders, fw)
	}
	return forwarders
}

type attributeModifier struct {
	action    string
	when      cel.Program
	key       string
	value     any
	valueProg cel.Program
}

func newAttributeModifier(cfg AttributeModifierConfig, env *cel.Env) (*attributeModifier, error) {
	var whenProg cel.Program
	var valueProg cel.Program
	var err error
	if cfg.When != nil {
		ast, issues := env.Compile(*cfg.When)
		if issues != nil && issues.Err() != nil {
			return nil, issues.Err()
		}
		whenProg, err = env.Program(ast)
		if err != nil {
			return nil, err
		}
	}
	if cfg.ValueExpr != "" {
		ast, issues := env.Compile(cfg.ValueExpr)
		if issues != nil && issues.Err() != nil {
			return nil, issues.Err()
		}
		valueProg, err = env.Program(ast)
		if err != nil {
			return nil, err
		}
	}
	return &attributeModifier{
		action:    cfg.Action,
		when:      whenProg,
		key:       cfg.Key,
		value:     cfg.Value,
		valueProg: valueProg,
	}, nil
}

func (m *attributeModifier) Apply(obj any, attrs map[string]any) (map[string]any, error) {
	if m.when != nil {
		out, _, err := m.when.Eval(obj)
		if err != nil {
			return attrs, err
		}
		if v, ok := out.Value().(bool); !ok || !v {
			return attrs, nil
		}
	}
	if m.action == "remove" {
		delete(attrs, m.key)
		return attrs, nil
	}
	var val any
	if m.valueProg != nil {
		out, _, err := m.valueProg.Eval(obj)
		if err != nil {
			return attrs, err
		}
		val = out.Value()
	} else {
		val = m.value
	}
	attrs[m.key] = val
	return attrs, nil
}
