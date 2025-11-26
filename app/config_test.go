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
			name:      "valid config",
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
