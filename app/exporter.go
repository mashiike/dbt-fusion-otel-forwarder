package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"

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

func NewExporters(ctx context.Context, cfgs map[string]ExporterConfig) map[string]Exporter {
	exporters := make(map[string]Exporter)
	for name, cfg := range cfgs {
		exp, err := NewExporter(ctx, cfg)
		if err != nil {
			slog.Error("failed to create exporter", "name", name, "error", err)
			exp = &NoopExporter{}
		}
		exporters[name] = exp
	}
	return exporters
}

func NewExporter(ctx context.Context, cfg ExporterConfig) (Exporter, error) {
	if cfg.Type == "otlp" {
		opts := cfg.Otlp.ClientOptions()
		client, err := otlp.NewClient(cfg.Otlp.Endpoint, opts...)
		if err != nil {
			return nil, err
		}
		return &OonceStartExporter{Exporter: client}, nil
	}
	return nil, errors.New("unsupported exporter type: " + cfg.Type)
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
