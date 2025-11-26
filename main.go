package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mashiike/dbt-fusion-otel-forwarder/app"
)

var Version = "v0.1.0"

func main() {
	if code := run(); code != 0 {
		os.Exit(code)
	}
}

func run() int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	fs, parse, targetArgs := newFlagSet()
	var (
		logDir       = getenv("DBT_LOG_PATH", "logs")
		otelFile     = getenv("DBT_OTEL_FILE_NAME", "otel.jsonl")
		logFmt       = getenv("LOG_FORMAT", "json")
		logLevel     = getenv("LOG_LEVEL", "info")
		serviceName  = getenv("DBT_OTEL_SERVICE_NAME", "dbt")
		flushTimeout = getenv("DBT_OTEL_FLUSH_TIMEOUT", "5m")
		config       = getenv("DBT_OTEL_FORWARDER_CONFIG", "dbt-fusion-otel-forwarder-config.yml")
	)
	fs.StringVar(&logDir, "log-path", logDir, "Directory where dbt writes logs (defaults to dbt's log path)")
	fs.StringVar(&otelFile, "otel-file", otelFile, "OTEL log file name (relative to log-path unless absolute)")
	fs.StringVar(&config, "config", config, "Path to forward config (JSON)")
	fs.StringVar(&logLevel, "log-level", logLevel, "Log level (debug, info, warn, error). Default from LOG_LEVEL or info")
	fs.StringVar(&logFmt, "log-format", logFmt, "Log format (json or text). Default from LOG_FORMAT or json")
	fs.StringVar(&serviceName, "service-name", serviceName, "Service name for OTEL traces. Default from DBT_OTEL_SERVICE_NAME or dbt")
	fs.StringVar(&flushTimeout, "flush-timeout", flushTimeout, "Maximum time to wait for flushing OTEL data on exit. Default from DBT_OTEL_FLUSH_TIMEOUT or 5m")
	if err := parse(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse flags: %v\n", err)
		return 1
	}
	var minLevel slog.Level
	warnings := []string{}
	if err := minLevel.UnmarshalText([]byte(logLevel)); err != nil {
		minLevel = slog.LevelInfo
		warnings = append(warnings, fmt.Sprintf("invalid log level: %s, fallback to info", logLevel))
	}
	if logFmt != "json" && logFmt != "text" {
		warnings = append(warnings, fmt.Sprintf("invalid log format: %s, fallback to json", logFmt))
		logFmt = "json"
	}
	var logger *slog.Logger
	switch logFmt {
	case "text":
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: minLevel}))
	default:
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: minLevel}))
	}
	logger = logger.With("app", appName, "version", Version)
	for _, warning := range warnings {
		logger.Warn(warning)
	}
	slog.SetDefault(logger)
	flushTimeoutDuration, err := time.ParseDuration(flushTimeout)
	if err != nil {
		logger.Warn("invalid flush timeout, fallback to 5m", "value", flushTimeout)
		flushTimeoutDuration = 5 * time.Minute
	}

	if len(targetArgs) == 0 {
		if fs.NArg() > 0 {
			targetArgs = fs.Args()
		} else {
			fs.Usage()
			return 1
		}
	}

	var cfg *app.Config
	if config != "" {
		loaded, err := app.LoadConfig(config)
		if err != nil {
			slog.Warn("failed to load config", "error", err)
		}
		cfg = loaded
	}
	a, err := app.New(ctx, cfg)
	if err != nil {
		slog.Error("failed to create app", "error", err)
		return 1
	}

	params := app.RunParams{
		LogPath:      logDir,
		OtelFile:     otelFile,
		TargetCmd:    targetArgs,
		ServiceName:  serviceName,
		FlushTimeout: flushTimeoutDuration,
	}

	return a.Run(ctx, params)
}

const (
	appName          = "dbt-fusion-otel-forwarder"
	optionTerminator = "--"
)

func newFlagSet() (*flag.FlagSet, func() error, []string) {
	wrapperArgs := make([]string, 0, len(os.Args))
	targetCmd := make([]string, 0)
	for i, arg := range os.Args {
		if arg == optionTerminator {
			targetCmd = os.Args[i+1:]
			break
		}
		wrapperArgs = append(wrapperArgs, arg)
	}
	fs := flag.NewFlagSet(appName, flag.ExitOnError)
	fs.SetOutput(os.Stderr)
	return fs, func() error {
		return fs.Parse(wrapperArgs[1:])
	}, targetCmd
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
