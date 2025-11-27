package app

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	yaml "github.com/goccy/go-yaml"
	"github.com/mashiike/go-otlp-helper/otlp"
)

type Config struct {
	Exporters map[string]ExporterConfig `yaml:"exporters"`
	Forward   map[string]ForwardConfig  `yaml:"forward"`
}

func (cfg *Config) Validate() error {
	for name, expCfg := range cfg.Exporters {
		if name == "" {
			return fmt.Errorf("exporter name is required")
		}
		if err := expCfg.Validate(); err != nil {
			return fmt.Errorf("exporters[%s].%w", name, err)
		}
	}
	return nil
}

type ExporterConfig struct {
	Type string             `yaml:"type"`
	Otlp OtlpExporterConfig `yaml:",inline"`
}

func (cfg *ExporterConfig) Validate() error {
	if cfg.Type == "otlp" {
		return cfg.Otlp.Validate()
	}
	return fmt.Errorf("type is not supported: %s", cfg.Type)
}

type OtlpExporterConfig struct {
	Endpoint      string            `yaml:"endpoint"`
	Protocol      string            `yaml:"protocol,omitempty"`       // "http/protobuf", "http/json", "grpc"
	Gzip          *bool             `yaml:"gzip,omitempty"`           // Enable gzip compression
	Headers       map[string]string `yaml:"headers,omitempty"`        // Custom headers
	ExportTimeout *time.Duration    `yaml:"export_timeout,omitempty"` // Export timeout
	UserAgent     string            `yaml:"user_agent,omitempty"`     // Custom user agent

	// Per-signal configurations
	Traces *OtlpSignalConfig `yaml:"traces,omitempty"`
	Logs   *OtlpSignalConfig `yaml:"logs,omitempty"`
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

type ForwardConfig struct {
	Resource *ForwardResourceConfig `yaml:"resource,omitempty"`
	Traces   *TracesForwardConfig   `yaml:"traces,omitempty"`
	Logs     *LogsForwardConfig     `yaml:"logs,omitempty"`
}

func (cfg *ForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	if cfg.Traces != nil {
		if err := cfg.Traces.Validate(exporters); err != nil {
			return fmt.Errorf("traces.%w", err)
		}
	}
	if cfg.Logs != nil {
		if err := cfg.Logs.Validate(exporters); err != nil {
			return fmt.Errorf("logs.%w", err)
		}
	}
	return nil
}

type AttributeModifierConfig struct {
	Action    string  `yaml:"action"` // "set", "remove"
	When      *string `yaml:"when"`
	Key       string  `yaml:"key"`
	Value     any     `yaml:"value"`
	ValueExpr string  `yaml:"value_expr,omitempty"`
}

func (cfg *AttributeModifierConfig) Validate() error {
	if cfg.Action == "" {
		cfg.Action = "set"
	}
	if cfg.Action != "set" && cfg.Action != "remove" {
		return fmt.Errorf("action must be one of 'set', 'remove'")
	}
	if cfg.Key == "" {
		return fmt.Errorf("key is required")
	}
	if cfg.Action == "set" {
		if cfg.Value == nil && cfg.ValueExpr == "" {
			return errors.New("either value or value_expr must be set")
		}
		if cfg.ValueExpr != "" && cfg.Value != nil {
			return errors.New("cannnot both value and value_expr be set")
		}
	}
	return nil
}

type ForwardResourceConfig struct {
	Attributes map[string]any `yaml:"attributes"`
}

type TracesForwardConfig struct {
	Attributes []AttributeModifierConfig `yaml:"attributes,omitempty"`
	Exporters  []string                  `yaml:"exporters"`
}

func (cfg *TracesForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	for _, name := range cfg.Exporters {
		if _, ok := exporters[name]; !ok {
			return fmt.Errorf("traces exporter %s is not defined", name)
		}
	}
	for _, attrMod := range cfg.Attributes {
		if err := attrMod.Validate(); err != nil {
			return fmt.Errorf("invalid trace attribute modifier: %w", err)
		}
	}
	return nil
}

type LogsForwardConfig struct {
	Attributes []AttributeModifierConfig `yaml:"attributes,omitempty"`
	Exporters  []string                  `yaml:"exporters"`
}

func (cfg *LogsForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	for _, name := range cfg.Exporters {
		if _, ok := exporters[name]; !ok {
			return fmt.Errorf("logs exporter %s is not defined", name)
		}
	}
	for _, attrMod := range cfg.Attributes {
		if err := attrMod.Validate(); err != nil {
			return fmt.Errorf("invalid log attribute modifier: %w", err)
		}
	}
	return nil
}

// LoadConfig loads configuration from the specified path.
func LoadConfig(path string) (*Config, error) {
	r, err := loadConfig(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := decocdeConfig(r, &cfg); err != nil {
		return nil, err
	}
	return &cfg, cfg.Validate()
}

func loadConfig(path string) (io.Reader, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	expanded, err := expandWithDefaultAndError(string(data))
	if err != nil {
		return nil, fmt.Errorf("expand env vars in config: %w", err)
	}
	return bytes.NewReader([]byte(expanded)), nil
}

var re = regexp.MustCompile(`\$\{([^}]+)\}`)

func expandWithDefaultAndError(s string) (string, error) {
	var firstErr error
	result := re.ReplaceAllStringFunc(s, func(m string) string {
		content := strings.TrimSuffix(strings.TrimPrefix(m, "${"), "}")

		// ?:error
		if parts := strings.SplitN(content, ":?", 2); len(parts) == 2 {
			key := parts[0]
			errMsg := parts[1]
			val, ok := os.LookupEnv(key)
			if !ok || val == "" {
				if firstErr == nil {
					firstErr = fmt.Errorf("%s", errMsg)
				}
				return ""
			}
			return val
		}

		// :-default
		if parts := strings.SplitN(content, ":-", 2); len(parts) == 2 {
			key := parts[0]
			def := parts[1]
			val, ok := os.LookupEnv(key)
			if ok && val != "" {
				return val
			}
			return def
		}
		if v, ok := os.LookupEnv(content); ok {
			return v
		}
		return ""
	})

	return result, firstErr
}

func decocdeConfig(r io.Reader, v interface{}) error {
	strictDecoder := yaml.NewDecoder(r, yaml.Strict())
	err := strictDecoder.Decode(v)
	if err == nil {
		return nil
	}
	slog.Warn("config decode with strict mode failed, retrying with relaxed mode", "error", err)
	relaxedDecoder := yaml.NewDecoder(r)
	err = relaxedDecoder.Decode(v)
	if err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}
