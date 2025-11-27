package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// RunParams holds user-supplied options for the wrapper.
type RunParams struct {
	LogPath      string
	OtelFile     string
	TargetCmd    []string
	FlushTimeout time.Duration
}

// App owns the application lifecycle for the dbt OTEL forwarder.
type App struct {
	cfg     *Config
	Stdout  io.Writer
	Stderr  io.Writer
	Stdin   io.Reader
	Environ func() []string
	Logger  *slog.Logger
}

// New returns an App with sensible defaults for CLI execution.
func New(ctx context.Context, cfg *Config) (*App, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	return &App{
		cfg:     cfg,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Stdin:   os.Stdin,
		Environ: os.Environ,
		Logger:  slog.Default(),
	}, nil
}

// Run executes the wrapper: invoke dbt, then forward the OTEL log.
func (a *App) Run(ctx context.Context, params RunParams) int {
	if len(params.TargetCmd) == 0 {
		fmt.Fprintln(a.Stderr, "no dbt command specified")
		return 1
	}

	logDir := params.LogPath
	otelFile := params.OtelFile
	otelPath := otelFile
	if !filepath.IsAbs(otelFile) {
		if logDir == "" {
			logDir = "."
		}
		otelPath = filepath.Join(logDir, otelFile)
	}

	env := a.Environ()
	if !hasEnv(env, "DBT_OTEL_FILE_NAME") {
		env = append(env, fmt.Sprintf("DBT_OTEL_FILE_NAME=%s", otelFile))
	}
	if !hasEnv(env, "DBT_LOG_PATH") && logDir != "" {
		env = append(env, fmt.Sprintf("DBT_LOG_PATH=%s", logDir))
	}
	// Record the start time for cutoff (to skip old logs from previous runs)
	startTimeNano := uint64(time.Now().UnixNano())

	// Channel for streaming log lines from tail goroutine to flush goroutine
	lines := make(chan string, 1000)
	var wg sync.WaitGroup
	tailCtx, tailCancel := context.WithCancel(ctx)
	defer tailCancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.tailOTELFile(tailCtx, otelPath, lines)
	}()

	// Start flush and upload goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.flushAndUpload(ctx, lines, otelPath, startTimeNano, params); err != nil {
			a.Logger.Warn("OTEL upload failed", "error", err)
		}
	}()

	// Execute dbt command
	a.Logger.Debug("executing dbt command", "cmd", params.TargetCmd)
	cmd := exec.CommandContext(ctx, params.TargetCmd[0], params.TargetCmd[1:]...)
	cmd.Env = env
	cmd.Stdout = a.Stdout
	cmd.Stderr = a.Stderr
	cmd.Stdin = a.Stdin
	cmdErr := cmd.Run()
	time.Sleep(100 * time.Millisecond) // wait a bit for file writes to settle
	tailCancel()
	// Close lines channel to signal tail completion
	close(lines)
	a.Logger.Debug("dbt command finished, waiting for upload completion")

	// Wait for upload goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		a.Logger.Debug("OTEL upload goroutines completed")
	case <-time.After(params.FlushTimeout):
		a.Logger.Warn("OTEL upload goroutines did not complete within timeout, proceeding anyway")
	}

	if cmdErr != nil {
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		fmt.Fprintf(a.Stderr, "dbt command failed: %v\n", cmdErr)
		return 1
	}

	a.Logger.Debug("OTEL forwarder completed successfully")
	return 0
}

// tailOTELFile monitors the OTEL log file and sends new lines to the channel.
func (a *App) tailOTELFile(ctx context.Context, path string, lines chan<- string) {
	a.Logger.Debug("starting OTEL file tail", "path", path)

	// Wait for file to be created (dbt may not create it immediately)
	var f *os.File
	var err error
	for i := 0; i < 30; i++ {
		f, err = os.Open(path)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			a.Logger.Debug("tail cancelled before file created")
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		a.Logger.Debug("OTEL file not found, skipping tail", "path", path, "error", err)
		return
	}
	defer f.Close()

	a.Logger.Debug("OTEL file opened successfully", "path", path)

	reader := bufio.NewReader(f)
	lineCount := 0

	for {
		select {
		case <-ctx.Done():
			a.Logger.Debug("tail cancelled", "lines_read", lineCount)
			return
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				// EOF reached, wait a bit and retry
				// Don't return the partial line if we got one
				if line != "" {
					// We have a partial line without newline, put it back
					// This shouldn't happen with JSONL, but handle it gracefully
					a.Logger.Debug("partial line at EOF, waiting for more", "partial", line[:min(50, len(line))])
				}
				select {
				case <-ctx.Done():
					a.Logger.Debug("tail completed", "lines_read", lineCount)
					return
				case <-time.After(100 * time.Millisecond):
					// Continue reading
				}
				continue
			}
			// Other error
			a.Logger.Debug("reader error", "error", err, "lines_read", lineCount)
			return
		}

		// Successfully read a complete line (with newline)
		line = strings.TrimSuffix(line, "\n")
		line = strings.TrimSuffix(line, "\r") // Handle CRLF
		if line == "" {
			continue // Skip empty lines
		}

		lineCount++
		select {
		case lines <- line:
			a.Logger.Debug("line sent to channel", "line_number", lineCount)
		case <-ctx.Done():
			a.Logger.Debug("tail cancelled while sending", "lines_read", lineCount)
			return
		}
	}
}

// flushAndUpload reads lines from channel, buffers them, and periodically uploads traces.
func (a *App) flushAndUpload(ctx context.Context, lines <-chan string, srcPath string, cutoffTimeNano uint64, params RunParams) error {
	forwarders := NewForwarders(ctx, a.cfg)
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()
		for _, forwarder := range forwarders {
			if err := forwarder.Stop(stopCtx); err != nil {
				a.Logger.Warn("failed to stop forwarder", "error", err)
			}
		}
	}()

	// Create decoder once and reuse it to maintain state across flushes
	decoder := NewDecoder(cutoffTimeNano)
	buffer := make([]string, 0, 100)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		a.Logger.Debug("flushing buffer", "line_count", len(buffer))

		spans, logs, err := decoder.DecodeLines(buffer)
		if err != nil {
			a.Logger.Debug("failed to decode spans", "error", err)
			// Don't return error for decode failures, just log and skip
			a.Logger.Warn("skipping invalid OTEL log lines", "error", err)
			buffer = buffer[:0]
			return
		}

		a.Logger.Debug("decoded results", "span_count", len(spans), "log_count", len(logs))

		if len(logs) == 0 && len(spans) == 0 {
			a.Logger.Debug("no spans or logs decoded from buffer")
			buffer = buffer[:0]
			return
		}
		var wg sync.WaitGroup
		uploadCtxWithTimeout, uploadCancel := context.WithTimeout(context.Background(), params.FlushTimeout)
		defer uploadCancel()
		if len(logs) > 0 {
			a.Logger.Debug("logs decoded but not yet handled", "count", len(logs))
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, forwarder := range forwarders {
					if err := forwarder.UploadLogs(uploadCtxWithTimeout, &logspb.ScopeLogs{
						Scope: &commonpb.InstrumentationScope{
							Name:    "dbt-fusion-otel-forwarder",
							Version: Version,
						},
						LogRecords: logs,
					}); err != nil {
						a.Logger.Warn("failed to upload logs", "error", err, "log_count", len(logs))
					} else {
						a.Logger.Debug("logs uploaded successfully", "log_count", len(logs))
					}
				}
			}()
		}

		if len(spans) > 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, forwarder := range forwarders {
					if err := forwarder.UploadTraces(uploadCtxWithTimeout, &tracepb.ScopeSpans{
						Scope: &commonpb.InstrumentationScope{
							Name:    "dbt-fusion-otel-forwarder",
							Version: Version,
						},
						Spans: spans,
					}); err != nil {
						a.Logger.Warn("failed to upload traces", "error", err, "span_count", len(spans))
					} else {
						a.Logger.Debug("traces uploaded successfully", "span_count", len(spans))
					}
				}
			}()
		}
		wg.Wait()
		a.Logger.Debug("upload telemetry successfully", "span_count", len(spans), "log_count", len(logs))
		buffer = buffer[:0]
	}

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				// Channel closed, flush remaining buffer and exit
				// Use background context for final flush to avoid cancellation
				a.Logger.Debug("lines channel closed, final flush")
				flush()
				return nil
			}
			buffer = append(buffer, line)
			if len(buffer) >= 100 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			a.Logger.Debug("upload cancelled, final flush")
			// Use background context for final flush to avoid cancellation
			flush()
			return nil
		}
	}
}

func hasEnv(env []string, key string) bool {
	prefix := key + "="
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
