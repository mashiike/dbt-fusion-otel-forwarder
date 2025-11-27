# dbt-fusion-otel-forwarder

このツールは [dbt-fusion](https://github.com/dbt-labs/dbt-fusion) が出力するログを元にして OpenTelemetry Protocol (OTLP) エンドポイントにテレメトリを転送します。
このツールは、dbt-fusionのコマンドをラップして動かすことを想定しています。

**dbt Core はOTELログを出力しないので対応していません。**


## クイックスタート

リリースバイナリをダウンロードしてインストールし、以下のようにdbtのコマンドをラップして実行します。

```sh
# dbt をラップして OTLP に転送
dbt-fusion-otel-forwarder --config config.yml -- dbt build
```

## 設定
YAMLで以下のように記述します。
環境変数経由で値を設定する場合は以下の記法が使えます:

- `${VAR}` - シンプルな変数展開
- `${VAR:-デフォルト値}` - VARが未設定または空の場合にデフォルト値を使用
- `${VAR:?エラーメッセージ}` - VARが未設定または空の場合にエラーメッセージを出してエラー終了

```yaml
exporters:
  otlp:
    type: otlp
    endpoint: "${OTLP_ENDPOINT:-http://localhost:4318}"
    protocol: http/protobuf
    headers:
      x-otlp-token: "${OTLP_TOKEN:?OTLP_TOKEN は必須です}"

forward:
  default:
    traces:
      exporters: [otlp]
```

- `exporters`: OTLP exporter を名前付きで定義（protocol/gzip/headers/timeouts/user agent などの上書き可）。
- `forward`: ルーティング設定。本プロジェクトは現状 trace を送信（logs/metrics セクションも将来のために解釈）。

## CLI フラグと環境変数
- `--config`: フォワーダー設定ファイルへのパス
- `--log-path`: dbt のログディレクトリ（`DBT_LOG_PATH` または `logs`）
- `--otel-file`: OTEL ログファイル名（`DBT_OTEL_FILE_NAME` または `otel.jsonl`）
- `--service-name`: OTLP 送信時の `service.name`（`DBT_OTEL_SERVICE_NAME` または `dbt`）
- `--flush-timeout`: 終了時にアップロードを待つ上限時間（`DBT_OTEL_FLUSH_TIMEOUT` または `5m`）
- `--log-level` / `--log-format`: ラッパー自身のログ設定（`json` or `text`）
- `--` 以降は dbt コマンドとして実行。上記の環境変数が未設定ならラッパーが設定して渡します。

## LICENCE

MIT License
