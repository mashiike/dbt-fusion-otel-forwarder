package app

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"

	yaml "github.com/goccy/go-yaml"
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

type ForwardConfig struct {
	Traces  *TracesForwardConfig  `yaml:"traces,omitempty"`
	Metrics *MetricsForwardConfig `yaml:"metrics,omitempty"`
	Logs    *LogsForwardConfig    `yaml:"logs,omitempty"`
}

func (cfg *ForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	if cfg.Traces != nil {
		if err := cfg.Traces.Validate(exporters); err != nil {
			return fmt.Errorf("traces.%w", err)
		}
	}
	if cfg.Metrics != nil {
		if err := cfg.Metrics.Validate(exporters); err != nil {
			return fmt.Errorf("metrics.%w", err)
		}
	}
	if cfg.Logs != nil {
		if err := cfg.Logs.Validate(exporters); err != nil {
			return fmt.Errorf("logs.%w", err)
		}
	}
	return nil
}

type TracesForwardConfig struct {
	Exporters []string `yaml:"exporters"`
}

func (cfg *TracesForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	for _, name := range cfg.Exporters {
		if _, ok := exporters[name]; !ok {
			return fmt.Errorf("traces exporter %s is not defined", name)
		}
	}
	return nil
}

type MetricsForwardConfig struct {
	Exporters []string `yaml:"exporters"`
}

func (cfg *MetricsForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	for _, name := range cfg.Exporters {
		if _, ok := exporters[name]; !ok {
			return fmt.Errorf("metrics exporter %s is not defined", name)
		}
	}
	return nil
}

type LogsForwardConfig struct {
	Exporters []string `yaml:"exporters"`
}

func (cfg *LogsForwardConfig) Validate(exporters map[string]ExporterConfig) error {
	for _, name := range cfg.Exporters {
		if _, ok := exporters[name]; !ok {
			return fmt.Errorf("logs exporter %s is not defined", name)
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
