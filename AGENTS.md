# AGENTS.md - dbt-fusion-otel-forwarder

## プロジェクト概要

このプロジェクトは **dbt-fusion の OTEL JSONL ログを OTLP エンドポイントへリアルタイムで転送する透過的なラッパーコマンド** です。dbt の終了コード・stdout/stderr をそのまま返し、フォワードに失敗しても Warn ログのみで処理を続行します。

### 設計思想

**「透過的なラッパー」であることが最優先**

- ラップ先のコマンド（dbt）の実行を妨げない
- ラッパーのエラーは Warn/Error で記録するのみで、dbt の実行結果・終了コードに影響させない
- ラップ有無で同じ結果になること

### 基本動作

```bash
# 基本的な使い方
./dbt-fusion-otel-forwarder --config example.yml -- dbt build

# example.yml の例（YAML/JSON 両対応、環境変数展開可）
# 環境変数展開の書式:
#   ${VAR}              - シンプルな変数展開
#   ${VAR:-default}     - VARが未設定または空の場合にデフォルト値を使用
#   ${VAR:?error msg}   - VARが未設定または空の場合にエラー
exporters:
  otlp:
    type: otlp
    endpoint: "${OTLP_ENDPOINT:-http://localhost:4318}"
    protocol: http/protobuf
    headers:
      x-otlp-token: "${OTLP_TOKEN:?OTLP_TOKEN is required}"

forward:
  default:
    traces:
      exporters: [otlp]
      attributes:
        # 静的な値で属性を設定
        - action: set
          key: "http.request.method"
          value: "POST"

        # CEL式を使った動的な属性設定
        - action: set
          when: name.contains("Node evaluated")
          key: "url.path"
          value_expr: attributes["dbt.unique_id"]

        # 条件付きでHTTPステータスコードを設定
        - action: set
          when: name.contains("Node evaluated")
          key: "http.response.status_code"
          value_expr: |
            status.code == "ERROR" ? 500 : 200
```

### 属性変更機能

スパンやログの属性を動的に追加・削除・変更できます：

**静的な値の設定** (`value`):
```yaml
- action: set
  key: "environment"
  value: "production"
```

**CEL式による動的な値** (`value_expr`):
```yaml
- action: set
  key: "computed_field"
  value_expr: '"prefix_" + name'
```

**条件付き変更** (`when`):
```yaml
- action: set
  when: severityText == "ERROR"
  key: "alert"
  value: true
```

**属性の削除**:
```yaml
- action: remove
  key: "sensitive_data"
```

**利用可能なCEL変数**:

Span用:
- `traceId`, `spanId`, `parentSpanId`, `name`, `traceState`
- `startTimeUnixNano`, `endTimeUnixNano`
- `attributes` (map), `status` (map), `kind` (string)
- `events` (list), `links` (list)

Log用:
- `traceId`, `spanId`, `timeUnixNano`, `observedTimeUnixNano`
- `severityNumber` (int), `severityText` (string)
- `body` (any), `attributes` (map)

詳細は [app/cel.go](app/cel.go) を参照。

1. dbt コマンドを実行
2. OTEL ログファイル生成を短時間待機し、存在すればリアルタイムで tail
3. 新しい行をバッファリング（100行または5秒ごと）
4. SpanStart/SpanEnd を突き合わせて OTLP Trace に変換しアップロード
5. dbt コマンド終了後、残りのバッファを最終フラッシュ
6. dbt の終了コードをそのまま返す

## アーキテクチャ

### ファイル構成

```
.
├── main.go           # CLI エントリーポイント
├── app/
│   ├── app.go        # 並列処理（dbt 実行、tail、アップロード）
│   ├── config.go     # 設定ファイル読み込み/テンプレート
│   ├── decoder.go    # OTEL JSONL → OTLP span/log デコード
│   └── exporter.go   # OTLP exporter と multiplex
└── app/testdata/     # サンプル/ゴールデン（デコード用）
```

### 並列処理の構成

```
main goroutine
├── dbt コマンド実行 (exec.CommandContext)
└── goroutine: tailOTELFile (ファイル監視 → 行を channel へ送信)
    └── goroutine: flushAndUpload (100行 or 5秒でバッファ→Decode→Upload)
```

### エラーハンドリング方針

**すべてのエラーは Warning/Error ログとして記録し、処理は継続する**

1. **OTEL ファイル未生成** → 30回×100ms まで待機。見つからなければ Debug で tail を諦める。
2. **OTEL ログのパースエラー** → Warn を出して該当バッファを破棄。
3. **アップロード失敗** → Warn を出して継続（次バッファへ）。
4. **goroutine のタイムアウト** → Warn を出しつつ dbt の終了を優先。

**タイムアウト設定**

- アップロード: `FlushTimeout`（CLI/環境で指定、デフォルト 5m）
- dbt 終了後の待機: `FlushTimeout`（同上）
- forwarder 停止: 3秒

## dbt-fusion OTEL ログの仕様

### コンテキスト

- サンドボックス用の `jaffle_shop` プロジェクトで検証
- dbt-fusion バージョン: 2.0.0-preview.72
- Fusion は Rust 実装の次世代 dbt エンジン（高速化と SQL 解析強化）
- OpenTelemetry に準拠した構造化ログを出力

### OTEL ログの有効化

```bash
# 環境変数で指定
export DBT_OTEL_FILE_NAME=otel.jsonl
export DBT_LOG_PATH=logs

# コマンドラインオプションで指定
dbt build --otel-file-name otel.jsonl --log-path logs
```

- デフォルトのログ出力先: プロジェクト直下の `logs/`（例: `jaffle_shop/logs/dbt.log`）
- OTEL ファイル: `$log_path/$otel_file_name`（デフォルト: `logs/otel.jsonl`）
- Parquet 出力: `--otel-parquet-file-name` で `$target_path/metadata` 配下に出力可能

### OTEL ログの構造

**フォーマット**: JSONL（1行1レコード）

**レコード種別**:
- `SpanStart`: スパン開始（`span_name`, `start_time_unix_nano` を含む）
- `SpanEnd`: スパン終了（`end_time_unix_nano` を含む）
- `LogRecord`: ユーザーログメッセージ

**主要フィールド**:
- `record_type`: レコード種別（`SpanStart` / `SpanEnd` / `LogRecord`）
- `trace_id`: トレースID（16進数文字列）
- `span_id`: スパンID（16進数文字列）
- `parent_span_id`: 親スパンID（16進数文字列、オプショナル）
- `span_name`: スパン名（例: `NodeEvaluated`, `QueryExecuted`）
- `start_time_unix_nano`: 開始時刻（ナノ秒）
- `end_time_unix_nano`: 終了時刻（ナノ秒）
- `event_type`: イベント種別（例: `NodeEvaluated`, `QueryExecuted`, `PhaseExecuted`）

**`testdata/otel.log` の統計**:
- イベント種別: `NodeEvaluated` 276件、`QueryExecuted` 146件、`UserLogMessage` 51件、`PhaseExecuted` 16件、`ArtifactWritten` 4件、`Process` 2件、`Invocation` 2件
- レコード種別: `SpanStart` 223件、`SpanEnd` 223件、`LogRecord` 51件
- 属性: ホスト情報、invocation 引数、フェーズ、生成アーティファクト、実行 SQL、アダプター（BigQuery）、ノードメタデータ（unique_id、materialization、schema/database）、ノード結果

### スパンの再構築

このプロジェクトでは、以下のロジックで SpanStart/SpanEnd を突き合わせてスパンを再構築しています：

1. `span_id` でレコードをグループ化
2. `SpanStart` から `span_name`, `start_time_unix_nano`, 属性を取得
3. `SpanEnd` から `end_time_unix_nano`, 追加属性を取得
4. `trace_id`, `parent_span_id` で親子関係を構築
5. OTLP Trace 形式に変換してアップロード

詳細は [app/app.go:187-241](app/app.go#L187-L241) の `decodeLinesAsSpans()` 関数を参照。

## 開発ガイド

### ビルドとテスト

```bash
# ビルド
go build ./...

# テスト（race detectorを有効にする）
go test -race ./...

# 静的解析
go vet ./...

# 実行例（デバッグログ有効）
go run . --log-level debug --config example.yml -- dbt build
```

**重要**: テストは必ず `-race` オプション付きで実行してください。このプロジェクトは並行処理を多用するため、データ競合の検出が不可欠です。

### デバッグログの確認

```bash
# デバッグログを有効にして実行
LOG_LEVEL=debug ./dbt-fusion-otel-forwarder --config config.json -- dbt build

# ログフォーマットを text に変更（読みやすい）
LOG_FORMAT=text LOG_LEVEL=debug ./dbt-fusion-otel-forwarder --config config.json -- dbt build
```

**主要なデバッグログ**:
- `starting OTEL forwarder`: ラッパー開始
- `starting OTEL file tail`: ファイル監視開始
- `OTEL file opened successfully`: ファイルオープン成功
- `line sent to channel`: 新しい行を検出
- `flushing buffer`: バッファをフラッシュ
- `uploading traces`: OTLP アップロード開始
- `traces uploaded successfully`: アップロード成功
- `dbt command finished`: dbt コマンド終了
- `OTEL forwarder completed successfully`: ラッパー正常終了

### コードスタイル

- Go の標準的なコーディング規約に従う
- `gofmt` で自動整形
- エラーは必ずハンドリング（無視しない）
- 長い関数は避け、適切に分割する
- コメントは「なぜ」を説明（「何を」は避ける）

### 破壊的変更について

このプロジェクトはまだ開発初期段階のため、**既存実装にこだわらず破壊的変更を行っても構いません**。特に：

- バッファリングロジックの簡素化
- エラーハンドリングの改善
- パフォーマンス最適化

ただし、「透過的なラッパー」という設計思想は維持してください。

## トラブルシューティング

### OTLP Collector に接続できない

**症状**: `failed to upload traces: connection refused` などのエラーログ

**対処**:
1. Collector が起動しているか確認
2. エンドポイント URL が正しいか確認（例: `http://localhost:4318`）
3. ネットワーク接続を確認

**注意**: 接続できなくてもdbtコマンドは正常に実行されます（Warning ログのみ）

### OTEL ログファイルが見つからない

**症状**: `OTEL file not found, skipping tail` のデバッグログ

**原因**:
- dbt が OTEL ログを出力しない設定になっている
- `DBT_OTEL_FILE_NAME` 環境変数が設定されていない

**対処**:
```bash
# 環境変数を設定して実行
DBT_OTEL_FILE_NAME=otel.jsonl dbt-fusion-otel-forwarder -- dbt build

# または、ラッパーに引数で指定
dbt-fusion-otel-forwarder --otel-file otel.jsonl -- dbt build
```

### goroutine がタイムアウトする

**症状**: `OTEL upload goroutines did not complete within timeout` の Warning ログ

**原因**:
- OTLP Collector への接続が遅い
- アップロードするスパン数が多すぎる
- ネットワークが不安定

**対処**: タイムアウトは 10 秒に設定されています。必要に応じて [app/app.go:129](app/app.go#L129) の値を調整してください。

**注意**: タイムアウトしてもdbtコマンドの終了コードは正確に返されます。

## 今後の拡張ポイント

- [ ] メトリクスの追加（アップロード成功/失敗数、レイテンシなど）
- [ ] バッファサイズとフラッシュ間隔の設定可能化
- [ ] リトライロジックの追加（アップロード失敗時）
- [ ] gRPC プロトコルのサポート（現在は HTTP のみ）
- [ ] バッチサイズの最適化（ネットワーク状況に応じて動的調整）
- [ ] ユニットテストの追加

## 参考リンク

- [dbt-fusion](https://github.com/dbt-labs/dbt-fusion) - 次世代 dbt エンジン
- [OpenTelemetry Protocol](https://opentelemetry.io/docs/specs/otlp/) - OTLP 仕様
- [mashiike/go-otlp-helper](https://github.com/mashiike/go-otlp-helper) - OTLP クライアントライブラリ
