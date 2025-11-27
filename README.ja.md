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
    resource:
      attributes:
        "service.name": "${DBT_OTEL_SERVICE_NAME:-dbt}"
    traces:
      exporters: [otlp]
      attributes:
        # 静的な属性を追加
        - action: set
          key: "http.request.method"
          value: "POST"

        # CEL式を使った動的な属性追加
        - action: set
          when: name.contains("Node evaluated")
          key: "url.path"
          value_expr: attributes["dbt.unique_id"]

        # 機密情報を削除
        - action: remove
          key: "sensitive_data"
```

- `exporters`: OTLP exporter を名前付きで定義（protocol/gzip/headers/timeouts/user agent などの上書き可）。
- `forward`: ルーティング設定。本プロジェクトは trace と log を送信します。
  - `attributes`: 静的な値またはCEL式を使ってspan/log属性を変更できます。
    - `action`: `set` (追加/更新) または `remove` (削除)
    - `when`: オプショナルなCEL条件式（trueの場合のみ適用）
    - `value`: 静的な値（文字列、数値、真偽値など）
    - `value_expr`: 実行時に評価されるCEL式

## CLI フラグと環境変数
- `--config`: フォワーダー設定ファイルへのパス
- `--log-path`: dbt のログディレクトリ（`DBT_LOG_PATH` または `logs`）
- `--otel-file`: OTEL ログファイル名（`DBT_OTEL_FILE_NAME` または `otel.jsonl`）
- `--flush-timeout`: 終了時にアップロードを待つ上限時間（`DBT_OTEL_FLUSH_TIMEOUT` または `5m`）
- `--log-level` / `--log-format`: ラッパー自身のログ設定（`json` or `text`）
- `--` 以降は dbt コマンドとして実行。上記の環境変数が未設定ならラッパーが設定して渡します。

## LICENCE

MIT License
