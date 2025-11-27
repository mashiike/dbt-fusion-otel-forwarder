# dbt-fusion-otel-forwarder

This tool forwards OpenTelemetry Protocol (OTLP) telemetry to an OTLP endpoint based on the logs output by [dbt-fusion](https://github.com/dbt-labs/dbt-fusion). It is intended to wrap and run dbt-fusion commands.

**dbt Core does not emit OTEL logs and is not supported.**


## Quickstart

Download the release binary, then wrap your dbt command:

```sh
dbt-fusion-otel-forwarder --config config.yml -- dbt build
```

## Configuration
Write your configuration in YAML. Environment variables can be injected using the following syntax:

- `${VAR}` - Simple variable expansion
- `${VAR:-default}` - Use default value if VAR is unset or empty
- `${VAR:?error message}` - Fail with error message if VAR is unset or empty

```yaml
exporters:
  otlp:
    type: otlp
    endpoint: "${OTLP_ENDPOINT:-http://localhost:4318}"
    protocol: http/protobuf
    headers:
      x-otlp-token: "${OTLP_TOKEN:?OTLP_TOKEN is required}"

forward:
  default:
    resource:
      attributes:
        "service.name": "${DBT_OTEL_SERVICE_NAME:-dbt}"
    traces:
      exporters: [otlp]
      attributes:
        # Add static attribute
        - action: set
          key: "http.request.method"
          value: "POST"

        # Add dynamic attribute using CEL expression
        - action: set
          when: name.contains("Node evaluated")
          key: "url.path"
          value_expr: attributes["dbt.unique_id"]

        # Remove sensitive attribute
        - action: remove
          key: "sensitive_data"
```

- `exporters`: named OTLP exporters with per-signal overrides (protocol, gzip, headers, timeouts, user agent).
- `forward`: routing rules; this project currently emits traces and logs.
  - `attributes`: modify span/log attributes using static values or CEL expressions.
    - `action`: `set` (add/update) or `remove` (delete)
    - `when`: optional CEL condition (only apply modifier if true)
    - `value`: static value (string, number, boolean, etc.)
    - `value_expr`: CEL expression evaluated at runtime

## CLI flags and environment
- `--config`: Path to the forwarder config.
- `--log-path`: Directory where dbt writes logs (defaults to `DBT_LOG_PATH` or `logs`).
- `--otel-file`: OTEL log file name (defaults to `DBT_OTEL_FILE_NAME` or `otel.jsonl`).
- `--service-name`: Resource `service.name` for exported traces (defaults to `DBT_OTEL_SERVICE_NAME` or `dbt`).
- `--flush-timeout`: Max time to wait for flushing uploads when exiting (defaults to `DBT_OTEL_FLUSH_TIMEOUT` or `5m`).
- `--log-level` / `--log-format`: Configure wrapper logging (`json` or `text`).
- Everything after `--` is executed as the dbt command; env vars above are set for dbt if not already present.

## License

MIT License
