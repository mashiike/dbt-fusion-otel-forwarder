package app

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"os"
	"text/template"

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
	tmpl := template.New("config").Funcs(
		template.FuncMap{
			"env": func(key string, args ...string) (string, error) {
				if len(args) > 1 {
					return "", fmt.Errorf("env: too many arguments")
				}
				if value, ok := os.LookupEnv(key); ok {
					return value, nil
				}
				defaultValue := ""
				if len(args) == 1 {
					defaultValue = args[0]
				}
				return defaultValue, nil
			},
		},
	).Option("missingkey=zero")
	tmpl, err = tmpl.Parse(string(data))
	if err != nil {
		// return original data if not a template
		return bytes.NewReader(data), nil
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("execute config template: %w", err)
	}
	return &buf, nil
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
