package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Setenv("API_KEY", "test-api-key")
	cases := []struct {
		name         string
		path         string
		envVars      map[string]string
		expectConfig *Config
		expectErr    bool
	}{
		{
			name:      "valid config",
			path:      "testdata/config.yml",
			expectErr: false,
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://localhost:4317",
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name:      "config with env var expansion",
			path:      "testdata/config_with_header.yml",
			expectErr: false,
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://localhost:4317",
							Headers: map[string]string{
								"Authorization": "Bearer test-api-key",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name:    "config with default value - env var not set",
			path:    "testdata/config_with_default.yml",
			envVars: map[string]string{},
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://localhost:4317",
							Headers: map[string]string{
								"Authorization": "Bearer test-api-key",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name: "config with default value - env var set",
			path: "testdata/config_with_default.yml",
			envVars: map[string]string{
				"OTLP_ENDPOINT": "http://custom:4317",
			},
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://custom:4317",
							Headers: map[string]string{
								"Authorization": "Bearer test-api-key",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name: "config with required env - all set",
			path: "testdata/config_with_required_env.yml",
			envVars: map[string]string{
				"OTLP_ENDPOINT": "http://required:4317",
			},
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://required:4317",
							Headers: map[string]string{
								"Authorization": "Bearer test-api-key",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name: "config with required env - missing API_KEY",
			path: "testdata/config_with_required_env.yml",
			envVars: map[string]string{
				"OTLP_ENDPOINT": "http://required:4317",
				"API_KEY":       "",
			},
			expectErr: true,
		},
		{
			name: "config with required env - missing OTLP_ENDPOINT",
			path: "testdata/config_with_required_env.yml",
			envVars: map[string]string{
				"OTLP_ENDPOINT": "",
			},
			expectErr: true,
		},
		{
			name: "config with mixed syntax",
			path: "testdata/config_with_mixed_syntax.yml",
			envVars: map[string]string{
				"OTLP_ENDPOINT": "http://mixed:4317",
			},
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://mixed:4317",
							Headers: map[string]string{
								"Authorization":   "Bearer test-api-key",
								"X-Custom-Header": "default-value",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
		{
			name: "config with mixed syntax - custom header set",
			path: "testdata/config_with_mixed_syntax.yml",
			envVars: map[string]string{
				"CUSTOM_HEADER": "custom-value",
			},
			expectConfig: &Config{
				Exporters: map[string]ExporterConfig{
					"otlp": {
						Type: "otlp",
						Otlp: OtlpExporterConfig{
							Endpoint: "http://localhost:4317",
							Headers: map[string]string{
								"Authorization":   "Bearer test-api-key",
								"X-Custom-Header": "custom-value",
							},
						},
					},
				},
				Forward: map[string]ForwardConfig{
					"default": {
						Traces: &TracesForwardConfig{
							Exporters: []string{"otlp"},
						},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.envVars {
				t.Setenv(k, v)
			}
			cfg, err := LoadConfig(tc.path)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectConfig, cfg)
		})
	}

}

func TestExpandWithDefaultAndError(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		envVars   map[string]string
		expected  string
		expectErr bool
	}{
		{
			name:     "simple env var",
			input:    "Hello ${USER}",
			envVars:  map[string]string{"USER": "alice"},
			expected: "Hello alice",
		},
		{
			name:     "env var not set - empty string",
			input:    "Hello ${NOTSET}",
			envVars:  map[string]string{},
			expected: "Hello ",
		},
		{
			name:     "default value - env var not set",
			input:    "Endpoint: ${ENDPOINT:-http://localhost:8080}",
			envVars:  map[string]string{},
			expected: "Endpoint: http://localhost:8080",
		},
		{
			name:     "default value - env var set",
			input:    "Endpoint: ${ENDPOINT:-http://localhost:8080}",
			envVars:  map[string]string{"ENDPOINT": "http://custom:9090"},
			expected: "Endpoint: http://custom:9090",
		},
		{
			name:     "default value - env var set to empty",
			input:    "Endpoint: ${ENDPOINT:-http://localhost:8080}",
			envVars:  map[string]string{"ENDPOINT": ""},
			expected: "Endpoint: http://localhost:8080",
		},
		{
			name:      "required env var - not set",
			input:     "Key: ${API_KEY:?API_KEY is required}",
			envVars:   map[string]string{},
			expectErr: true,
		},
		{
			name:      "required env var - set to empty",
			input:     "Key: ${API_KEY:?API_KEY is required}",
			envVars:   map[string]string{"API_KEY": ""},
			expectErr: true,
		},
		{
			name:     "required env var - set",
			input:    "Key: ${API_KEY:?API_KEY is required}",
			envVars:  map[string]string{"API_KEY": "secret-key"},
			expected: "Key: secret-key",
		},
		{
			name:     "multiple expansions",
			input:    "${HOST}:${PORT}",
			envVars:  map[string]string{"HOST": "localhost", "PORT": "8080"},
			expected: "localhost:8080",
		},
		{
			name:     "mixed syntax",
			input:    "${HOST:-localhost}:${PORT:?PORT is required}",
			envVars:  map[string]string{"PORT": "8080"},
			expected: "localhost:8080",
		},
		{
			name:      "mixed syntax - missing required",
			input:     "${HOST:-localhost}:${PORT:?PORT is required}",
			envVars:   map[string]string{},
			expectErr: true,
		},
		{
			name:     "default with special chars",
			input:    "${URL:-https://example.com/path?query=value}",
			envVars:  map[string]string{},
			expected: "https://example.com/path?query=value",
		},
		{
			name:     "error message with special chars",
			input:    "${KEY:?Environment variable KEY must be set!}",
			envVars:  map[string]string{"KEY": "value"},
			expected: "value",
		},
		{
			name:     "nested in yaml-like string",
			input:    "endpoint: \"${ENDPOINT:-http://localhost:4317}\"\nheader: \"Bearer ${TOKEN}\"",
			envVars:  map[string]string{"TOKEN": "abc123"},
			expected: "endpoint: \"http://localhost:4317\"\nheader: \"Bearer abc123\"",
		},
		{
			name:     "multiple same var",
			input:    "${VAR} and ${VAR} again",
			envVars:  map[string]string{"VAR": "test"},
			expected: "test and test again",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.envVars {
				t.Setenv(k, v)
			}
			result, err := expandWithDefaultAndError(tc.input)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}
