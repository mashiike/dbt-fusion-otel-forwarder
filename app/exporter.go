package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
)

type Exporter interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error
	UploadMetrics(ctx context.Context, protoMetrics []*otlp.ResourceMetrics) error
	UploadTraces(ctx context.Context, protoSpans []*otlp.ResourceSpans) error
}

var _ Exporter = (*otlp.Client)(nil)

type OtlpExporterConfig struct {
	Endpoint      string            `yaml:"endpoint"`
	Protocol      string            `yaml:"protocol,omitempty"`       // "http/protobuf", "http/json", "grpc"
	Gzip          *bool             `yaml:"gzip,omitempty"`           // Enable gzip compression
	Headers       map[string]string `yaml:"headers,omitempty"`        // Custom headers
	ExportTimeout *time.Duration    `yaml:"export_timeout,omitempty"` // Export timeout
	UserAgent     string            `yaml:"user_agent,omitempty"`     // Custom user agent

	// Per-signal configurations
	Traces  *OtlpSignalConfig `yaml:"traces,omitempty"`
	Metrics *OtlpSignalConfig `yaml:"metrics,omitempty"`
	Logs    *OtlpSignalConfig `yaml:"logs,omitempty"`
}

type OtlpSignalConfig struct {
	Endpoint      string            `yaml:"endpoint,omitempty"`
	Protocol      string            `yaml:"protocol,omitempty"`
	Gzip          *bool             `yaml:"gzip,omitempty"`
	Headers       map[string]string `yaml:"headers,omitempty"`
	ExportTimeout *time.Duration    `yaml:"export_timeout,omitempty"`
	UserAgent     string            `yaml:"user_agent,omitempty"`
}

func (cfg *OtlpExporterConfig) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	return nil
}

func (cfg *OtlpExporterConfig) ClientOptions() []otlp.ClientOption {
	var opts []otlp.ClientOption

	// Global options
	if cfg.Protocol != "" {
		opts = append(opts, otlp.WithProtocol(cfg.Protocol))
	}
	if cfg.Gzip != nil {
		opts = append(opts, otlp.WithGzip(*cfg.Gzip))
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlp.WithHeaders(cfg.Headers))
	}
	if cfg.ExportTimeout != nil {
		opts = append(opts, otlp.WithExportTimeout(*cfg.ExportTimeout))
	}
	if cfg.UserAgent != "" {
		opts = append(opts, otlp.WithUserAgent(cfg.UserAgent))
	}

	// Traces-specific options
	if cfg.Traces != nil {
		if cfg.Traces.Endpoint != "" {
			opts = append(opts, otlp.WithTracesEndpoint(cfg.Traces.Endpoint))
		}
		if cfg.Traces.Protocol != "" {
			opts = append(opts, otlp.WithTracesProtocol(cfg.Traces.Protocol))
		}
		if cfg.Traces.Gzip != nil {
			opts = append(opts, otlp.WithTracesGzip(*cfg.Traces.Gzip))
		}
		if len(cfg.Traces.Headers) > 0 {
			opts = append(opts, otlp.WithTracesHeaders(cfg.Traces.Headers))
		}
		if cfg.Traces.ExportTimeout != nil {
			opts = append(opts, otlp.WithTracesExportTimeout(*cfg.Traces.ExportTimeout))
		}
		if cfg.Traces.UserAgent != "" {
			opts = append(opts, otlp.WithTracesUserAgent(cfg.Traces.UserAgent))
		}
	}

	// Metrics-specific options
	if cfg.Metrics != nil {
		if cfg.Metrics.Endpoint != "" {
			opts = append(opts, otlp.WithMetricsEndpoint(cfg.Metrics.Endpoint))
		}
		if cfg.Metrics.Protocol != "" {
			opts = append(opts, otlp.WithMetricsProtocol(cfg.Metrics.Protocol))
		}
		if cfg.Metrics.Gzip != nil {
			opts = append(opts, otlp.WithMetricsGzip(*cfg.Metrics.Gzip))
		}
		if len(cfg.Metrics.Headers) > 0 {
			opts = append(opts, otlp.WithMetricsHeaders(cfg.Metrics.Headers))
		}
		if cfg.Metrics.ExportTimeout != nil {
			opts = append(opts, otlp.WithMetricsExportTimeout(*cfg.Metrics.ExportTimeout))
		}
		if cfg.Metrics.UserAgent != "" {
			opts = append(opts, otlp.WithMetricsUserAgent(cfg.Metrics.UserAgent))
		}
	}

	// Logs-specific options
	if cfg.Logs != nil {
		if cfg.Logs.Endpoint != "" {
			opts = append(opts, otlp.WithLogsEndpoint(cfg.Logs.Endpoint))
		}
		if cfg.Logs.Protocol != "" {
			opts = append(opts, otlp.WithLogsProtocol(cfg.Logs.Protocol))
		}
		if cfg.Logs.Gzip != nil {
			opts = append(opts, otlp.WithLogsGzip(*cfg.Logs.Gzip))
		}
		if len(cfg.Logs.Headers) > 0 {
			opts = append(opts, otlp.WithLogsHeaders(cfg.Logs.Headers))
		}
		if cfg.Logs.ExportTimeout != nil {
			opts = append(opts, otlp.WithLogsExportTimeout(*cfg.Logs.ExportTimeout))
		}
		if cfg.Logs.UserAgent != "" {
			opts = append(opts, otlp.WithLogsUserAgent(cfg.Logs.UserAgent))
		}
	}

	return opts
}

func NewForwarder(ctx context.Context, cfg *Config) Exporter {
	if len(cfg.Exporters) == 0 {
		slog.Warn("no exporters configured, using noop exporter")
		return &NoopExporter{}
	}
	exporters := make(map[string]Exporter)
	for name, expCfg := range cfg.Exporters {
		exp, err := NewExporters(ctx, expCfg)
		if err != nil {
			slog.Error("failed to create exporter", "name", name, "error", err)
			continue
		}
		exporters[name] = &OonceStartExporter{Exporter: exp}
		slog.Info("exporter created", "name", name, "type", expCfg.Type)
	}
	if len(exporters) == 0 {
		slog.Warn("no valid exporters configured, using noop exporter")
		return &NoopExporter{}
	}
	forwarders := make([]Exporter, 0, len(cfg.Forward))
	for name, fwCfg := range cfg.Forward {
		fw, err := newForwerder(name, fwCfg, exporters)
		if err != nil {
			slog.Error("failed to create forwarder", "name", name, "error", err)
			continue
		}
		forwarders = append(forwarders, fw)
		slog.Info("forwarder created", "name", name)
	}
	if len(forwarders) == 0 {
		slog.Warn("no valid forwarders configured, using noop exporter")
		return &NoopExporter{}
	}
	if len(forwarders) == 1 {
		return forwarders[0]
	}
	return NewMultiplexExporter(forwarders...)
}

func NewExporters(ctx context.Context, cfg ExporterConfig) (Exporter, error) {
	if cfg.Type == "otlp" {
		opts := cfg.Otlp.ClientOptions()
		return otlp.NewClient(cfg.Otlp.Endpoint, opts...)
	}
	return nil, errors.New("unsupported exporter type: " + cfg.Type)
}

type Forwerder struct {
	name            string
	cfg             ForwardConfig
	logsExporter    Exporter
	metricsExporter Exporter
	tracesExporter  Exporter
}

func newForwerder(name string, cfg ForwardConfig, exporters map[string]Exporter) (*Forwerder, error) {
	fw := &Forwerder{
		name: name,
		cfg:  cfg,
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

func (f *Forwerder) Start(ctx context.Context) error {
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

func (f *Forwerder) Stop(ctx context.Context) error {
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

func (f *Forwerder) UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error {
	if f.logsExporter != nil {
		slog.Debug("forwarder uploading logs", "forwarder", f.name, "log_count", len(protoLogs))
		return f.logsExporter.UploadLogs(ctx, protoLogs)
	}
	return nil
}

func (f *Forwerder) UploadMetrics(ctx context.Context, protoMetrics []*otlp.ResourceMetrics) error {
	if f.metricsExporter != nil {
		slog.Debug("forwarder uploading metrics", "forwarder", f.name, "metric_count", len(protoMetrics))
		return f.metricsExporter.UploadMetrics(ctx, protoMetrics)
	}
	return nil
}

func (f *Forwerder) UploadTraces(ctx context.Context, protoSpans []*otlp.ResourceSpans) error {
	if f.tracesExporter != nil {
		slog.Debug("forwarder uploading traces", "forwarder", f.name, "span_count", len(protoSpans))
		return f.tracesExporter.UploadTraces(ctx, protoSpans)
	}
	return nil
}

type OonceStartExporter struct {
	Exporter
	startErr error
	once     sync.Once
}

func (e *OonceStartExporter) Start(ctx context.Context) error {
	e.once.Do(func() {
		e.startErr = e.Exporter.Start(ctx)
	})
	return e.startErr
}

type NoopExporter struct{}

func (e *NoopExporter) Start(ctx context.Context) error {
	return nil
}

func (e *NoopExporter) Stop(ctx context.Context) error {
	return nil
}

func (e *NoopExporter) UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error {
	return nil
}

func (e *NoopExporter) UploadMetrics(ctx context.Context, protoMetrics []*otlp.ResourceMetrics) error {
	return nil
}

func (e *NoopExporter) UploadTraces(ctx context.Context, protoSpans []*otlp.ResourceSpans) error {
	return nil
}

type MultiplexExporter struct {
	exporters []Exporter
}

func NewMultiplexExporter(exporters ...Exporter) *MultiplexExporter {
	return &MultiplexExporter{
		exporters: exporters,
	}
}

func (e *MultiplexExporter) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(e.exporters))

	for _, exporter := range e.exporters {
		wg.Add(1)
		go func(exp Exporter) {
			defer wg.Done()
			if err := exp.Start(ctx); err != nil {
				errCh <- err
			}
		}(exporter)
	}

	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *MultiplexExporter) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(e.exporters))

	for _, exporter := range e.exporters {
		wg.Add(1)
		go func(exp Exporter) {
			defer wg.Done()
			if err := exp.Stop(ctx); err != nil {
				errCh <- err
			}
		}(exporter)
	}

	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *MultiplexExporter) UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(e.exporters))

	for _, exporter := range e.exporters {
		wg.Add(1)
		go func(exp Exporter) {
			defer wg.Done()
			if err := exp.UploadLogs(ctx, protoLogs); err != nil {
				errCh <- err
			}
		}(exporter)
	}

	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *MultiplexExporter) UploadMetrics(ctx context.Context, protoMetrics []*otlp.ResourceMetrics) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(e.exporters))

	for _, exporter := range e.exporters {
		wg.Add(1)
		go func(exp Exporter) {
			defer wg.Done()
			if err := exp.UploadMetrics(ctx, protoMetrics); err != nil {
				errCh <- err
			}
		}(exporter)
	}

	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (e *MultiplexExporter) UploadTraces(ctx context.Context, protoSpans []*otlp.ResourceSpans) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(e.exporters))

	for _, exporter := range e.exporters {
		wg.Add(1)
		go func(exp Exporter) {
			defer wg.Done()
			if err := exp.UploadTraces(ctx, protoSpans); err != nil {
				errCh <- err
			}
		}(exporter)
	}

	wg.Wait()
	close(errCh)
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
