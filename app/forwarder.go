package app

import (
	"context"
	"log/slog"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Forwarder struct {
	name               string
	resourceAttributes []*commonpb.KeyValue
	cfg                ForwardConfig
	logsExporter       Exporter
	metricsExporter    Exporter
	tracesExporter     Exporter
}

func NewForwarder(name string, cfg ForwardConfig, exporters map[string]Exporter) (*Forwarder, error) {
	attrs := make(map[string]any)
	if cfg.Resource != nil && len(cfg.Resource.Attributes) > 0 {
		attrs = cfg.Resource.Attributes
	}
	if _, ok := attrs["service.name"]; !ok {
		attrs["service.name"] = "dbt"
	}
	fw := &Forwarder{
		name:               name,
		cfg:                cfg,
		resourceAttributes: convertAttributes(attrs),
	}
	logsExporters := make([]Exporter, 0)
	metricsExporters := make([]Exporter, 0)
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

	if cfg.Metrics == nil {
		cfg.Metrics = &MetricsForwardConfig{}
	}
	for _, name := range cfg.Metrics.Exporters {
		exp, ok := exporters[name]
		if !ok {
			slog.Warn("metrics exporter not found", "name", name)
			continue
		}
		metricsExporters = append(metricsExporters, exp)
	}
	if len(metricsExporters) == 1 {
		fw.metricsExporter = metricsExporters[0]
	} else if len(metricsExporters) > 1 {
		fw.metricsExporter = NewMultiplexExporter(metricsExporters...)
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
	if f.metricsExporter != nil {
		if err := f.metricsExporter.Start(ctx); err != nil {
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
	if f.metricsExporter != nil {
		if err := f.metricsExporter.Stop(ctx); err != nil {
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

func (f *Forwarder) UploadMetrics(ctx context.Context, scopeMetrics *metricspb.ScopeMetrics) error {
	metrics := scopeMetrics.GetMetrics()
	resourceMetrics := &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{
			Attributes: f.resourceAttributes,
		},
		ScopeMetrics: []*metricspb.ScopeMetrics{scopeMetrics},
	}
	protoMetrics := []*metricspb.ResourceMetrics{resourceMetrics}
	if f.metricsExporter != nil {
		slog.Debug("forwarder uploading metrics", "forwarder", f.name, "metric_count", len(metrics))
		return f.metricsExporter.UploadMetrics(ctx, protoMetrics)
	}
	return nil
}

func (f *Forwarder) UploadTraces(ctx context.Context, scopeSpans *tracepb.ScopeSpans) error {
	spans := scopeSpans.GetSpans()
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
