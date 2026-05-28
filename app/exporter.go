package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/mashiike/go-otlp-helper/otlp"
)

const (
	defaultMaxAttempts   = 3
	defaultRetryInterval = 5 * time.Second
)

//go:generate go tool mockgen -source=$GOFILE -destination=./exporter_test.go -package=app
type Exporter interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error
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
		var exp Exporter = &OonceStartExporter{Exporter: client}
		attempts := cfg.MaxAttempts
		if attempts == 0 {
			attempts = defaultMaxAttempts
		}
		interval := defaultRetryInterval
		if cfg.RetryInterval != nil {
			interval = *cfg.RetryInterval
		}
		if attempts > 1 {
			exp = &RetryExporter{
				Exporter:      exp,
				MaxAttempts:   attempts,
				RetryInterval: interval,
			}
		}
		return exp, nil
	}
	return nil, errors.New("unsupported exporter type: " + cfg.Type)
}

type RetryExporter struct {
	Exporter
	MaxAttempts   int
	RetryInterval time.Duration
}

func (e *RetryExporter) UploadLogs(ctx context.Context, protoLogs []*otlp.ResourceLogs) error {
	return e.withRetry(ctx, "logs", func(ctx context.Context) error {
		return e.Exporter.UploadLogs(ctx, protoLogs)
	})
}

func (e *RetryExporter) UploadTraces(ctx context.Context, protoSpans []*otlp.ResourceSpans) error {
	return e.withRetry(ctx, "traces", func(ctx context.Context) error {
		return e.Exporter.UploadTraces(ctx, protoSpans)
	})
}

func (e *RetryExporter) withRetry(ctx context.Context, kind string, fn func(context.Context) error) error {
	var lastErr error
	for i := 0; i < e.MaxAttempts; i++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return lastErr
		}
		if i < e.MaxAttempts-1 {
			slog.Warn("upload failed, will retry",
				"kind", kind,
				"attempt", i+1,
				"max_attempts", e.MaxAttempts,
				"retry_interval", e.RetryInterval,
				"error", err,
			)
			select {
			case <-time.After(e.RetryInterval):
			case <-ctx.Done():
				return lastErr
			}
		}
	}
	return lastErr
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
