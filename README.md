# dbt-fusion-otel-forwarder

This tool forwards OpenTelemetry Protocol (OTLP) telemetry to an OTLP endpoint based on the logs output by [dbt-fusion](https://github.com/dbt-labs/dbt-fusion). It is intended to wrap and run dbt-fusion commands.

**dbt Core does not emit OTEL logs and is not supported.**


## Quickstart

Download the release binary, then wrap your dbt command:

```sh
dbt-fusion-otel-forwarder --config config.yml -- dbt build
```

## Configuration
Write your configuration in YAML. To inject environment variables, use ```{{ env `KEY` }}``.

```yaml
exporters:
  otlp:
    type: otlp
    endpoint: http://localhost:4318
    protocol: http/protobuf
    headers:
      x-otlp-token: "{{ env `OTLP_TOKEN` }}"

forward:
  default:
    traces:
      exporters: [otlp]
```

- `exporters`: named OTLP exporters with per-signal overrides (protocol, gzip, headers, timeouts, user agent).
- `forward`: routing rules; this project currently emits traces, but logs/metrics sections are parsed for future use.

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
