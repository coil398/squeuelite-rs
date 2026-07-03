<p align="center">
  <img src="assets/logo.png" alt="SqueueLite" width="360">
</p>

<p align="center">
  <a href="https://github.com/coil398/squeuelite-rs/actions/workflows/ci.yml"><img src="https://github.com/coil398/squeuelite-rs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/squeuelite"><img src="https://img.shields.io/crates/v/squeuelite.svg" alt="crates.io"></a>
  <a href="https://docs.rs/squeuelite"><img src="https://img.shields.io/docsrs/squeuelite" alt="docs.rs"></a>
  <a href="#ライセンス"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License: MIT OR Apache-2.0"></a>
</p>

# SqueueLite

[English](README.md) | **日本語**

**SqueueLite はジョブキューではない。**
SQLite への書き込みキューであり、SQLite をバックエンドに持つエージェントシステムのための単一 writer ゲートウェイである。

> 多数のエージェント。ただ一つの SQLite writer。ジョブキューではない。

---

## 何をするものか

SqueueLite は、複数のエージェント（タスク・プロセス・スレッド）からの並行な書き込み要求を、**ただ一つの** `rusqlite::Connection` を通して直列化する。各書き込み要求は一つのアトミックな `BEGIN IMMEDIATE … COMMIT` トランザクションになる。呼び出し側はコミット後に `WriteResponse` を受け取る（あるいはロールバック後にエラーを受け取る）。

**やらないこと**: ジョブのスケジューリング、ワーカーの管理、失敗した操作のリトライ、長時間動作するバックグラウンドタスクの実行。

**どんなときに使うか**: すべての writer が単一の Rust プロセス内にいるなら、[`tokio-rusqlite`](https://crates.io/crates/tokio-rusqlite) のような薄い actor で十分そのケースを賄える。SqueueLite が真価を発揮するのは、writer が **別々のプロセス、あるいは Rust 以外の言語**であり、単一 writer の前段に一つのゲートウェイ（冪等性・バッチング・バックプレッシャー・UDS/HTTP 上の JSON-RPC）を置きたいときである。

---

## モード

SqueueLite は 2 つのモードのいずれかで動作する。どちらも**同一の単一 writer actor** をラップしており、違いは呼び出し側がそこへ*どう到達するか*だけである。

| | **In-process** | **Sidecar** |
|---|---|---|
| 形態 | 1 つの Rust プロセス、多数の非同期タスク | **別プロセス**（`squeuelite-gateway`） |
| トランスポート | インメモリ channel（ソケットなし） | UDS および/または HTTP — **JSON-RPC 2.0** |
| 呼び出し側 | `InProcessGateway` 経由の Rust コード | JSON-RPC 2.0 を話せる**任意の言語** |
| 速度 | 最速 | やや遅い（ソケット/ネットワークのホップ） |
| 使う場面 | すべての writer が 1 つの Rust バイナリ内にいる | writer が別々の OS プロセス（Rust 以外も含む） |

**sidecar** は `squeuelite-gateway` バイナリである。唯一の writer コネクションを所有するスタンドアロンなデーモンで、すべてのトランスポートが **JSON-RPC 2.0** を使う。1 つのプロセスが UDS と HTTP を同時に提供でき、単一の writer スレッドを共有する。

| トランスポート | フラグ | 適する用途 |
|-----------|------|----------|
| **UDS**（Unix Domain Socket） | `--socket <path>` | ローカル限定、ファイルシステム権限による最大限のセキュリティ |
| **HTTP** `POST /rpc` | `--http <addr>` | ホスト間、あるいは非 Unix クライアント。デフォルトは localhost |

### In-process（単一 Rust プロセス、複数の非同期タスク）

`inprocess` フィーチャを有効にする:

```toml
[dependencies]
squeuelite = { version = "0.1", features = ["inprocess"] }
```

```rust
use squeuelite::{InProcessGateway, SqlOperation, WriteRequest};

let gateway = InProcessGateway::open("./app.db")?;
let handle  = gateway.handle();

// 多数のタスクを spawn する — すべてが同じ handle を共有する。
let resp = handle.execute(WriteRequest {
    request_id:     "req-1".into(),
    actor_id:       "agent-a".into(),
    run_id:         None,
    idempotency_key: None,
    operations: vec![SqlOperation {
        sql:    "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
        params: vec!["agent-a".into(), "started".into()],
    }],
}).await?;

println!("commit_seq = {:?}", resp.commit_seq);
gateway.shutdown().await?;
```

### Sidecar（別プロセス、Unix Domain Socket）

`sidecar` フィーチャを有効にする:

```toml
[dependencies]
squeuelite = { version = "0.1", features = ["sidecar"] }
```

#### ゲートウェイバイナリを起動する

```bash
# UDS のみ（JSON Lines 上の JSON-RPC 2.0）
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock

# HTTP のみ（HTTP POST /rpc 上の JSON-RPC 2.0）
squeuelite-gateway --db ./app.db --http 127.0.0.1:8080

# 両トランスポートを同時に — 1 プロセス、1 writer
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock --http 127.0.0.1:8080
```

`--socket` か `--http` のうち少なくとも一方が必須。ゲートウェイは Ctrl-C でクリーンにシャットダウンする（WAL チェックポイントを含む）。1 プロセス、1 つの SQLite writer。

#### Rust クライアントを使う

```rust
use squeuelite::{Client, SqlOperation};

let mut client = Client::connect("agent-a", "./squeuelite.sock").await?;

// 単一操作
let resp = client.execute(SqlOperation {
    sql:    "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
    params: vec!["agent-a".into(), "started".into()],
}).await?;

// アトミックな複数操作トランザクション
let resp = client.transaction(vec![
    SqlOperation {
        sql:    "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
        params: vec!["agent-a".into(), "tool_result".into()],
    },
    SqlOperation {
        sql:    "UPDATE runs SET status = ? WHERE id = ?".into(),
        params: vec!["done".into(), "run-1".into()],
    },
]).await?;
```

#### socat で UDS ソケットを叩く（JSON-RPC 2.0）

```bash
# ヘルスチェック
echo '{"jsonrpc":"2.0","id":1,"method":"health"}' | socat UNIX-CONNECT:./squeuelite.sock -

# 統計スナップショット
echo '{"jsonrpc":"2.0","id":2,"method":"stats"}' | socat UNIX-CONNECT:./squeuelite.sock -

# WAL チェックポイント
echo '{"jsonrpc":"2.0","id":3,"method":"checkpoint"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Execute（テーブル `t` は既存であること — 「スキーマのセットアップ」を参照）
echo '{"jsonrpc":"2.0","id":4,"method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}' \
  | socat UNIX-CONNECT:./squeuelite.sock -
```

#### curl で HTTP エンドポイントを叩く（JSON-RPC 2.0）

```bash
# ヘルスチェック（素の HTTP GET）
curl -s http://127.0.0.1:8080/health

# Execute（JSON-RPC 2.0 POST）
curl -s -XPOST http://127.0.0.1:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"r1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}'
```

---

## スキーマのセットアップ

デフォルトでは、ゲートウェイは**セキュアな書き込み制限**（§23）付きで動作する。スキーマ変更（`CREATE` / `ALTER` / `DROP` / `TRUNCATE`）は**拒否される**。したがって新規データベースにはアプリケーションのテーブルが存在せず、オプトインしない限り**ゲートウェイ経由で `CREATE TABLE` を送ってテーブルを作ることはできない**。次のいずれかを選ぶ:

- **スキーマを事前作成する** — `squeuelite-gateway` バイナリでは推奨。ゲートウェイを起動する*前*にデータベースファイル内でテーブルを構築しておく:

  ```bash
  sqlite3 ./app.db < schema.sql
  squeuelite-gateway --db ./app.db --socket ./squeuelite.sock
  ```

  以降、ゲートウェイは既存のスキーマに対する書き込みを直列化するだけになる。

- **スキーマ書き込みを明示的に許可する** — in-process、あるいは自作のゲートウェイバイナリ向け。config に `allow_schema_write = true` を設定する:

  ```rust
  let mut config = GatewayConfig::new("./app.db");
  config.allow_schema_write = true;          // ゲートウェイ経由の CREATE / ALTER を許可
  let gateway = InProcessGateway::open_with_config(config)?;
  ```

  sidecar の場合は同じフラグを `SidecarConfig` に設定し、**自作のバイナリ**から `SidecarGateway::open(...)` を呼ぶ。同梱の `squeuelite-gateway` バイナリは意図的にロックダウンされたデフォルトを使い、それを緩めるフラグを持たない。

> SqueueLite の内部テーブル（`squeuelite_commits`、`squeuelite_requests`）は、これらのフラグに関わらず起動時に必ず作成される — これらは要求パスではなく起動時マイグレーションパス（§17）を通る。

---

## フィーチャ

| フィーチャ | 追加されるもの |
|--------------|---------------------------------------------------------------------|
| `bundled`    | SQLite をソースからコンパイル（デフォルト。WAL が保証される）        |
| `inprocess`  | `InProcessGateway` + `GatewayHandle`（tokio mpsc actor）             |
| `sidecar`    | `SidecarGateway` + `Client` + UDS JSON-RPC 2.0 トランスポート        |
| `http`       | `HttpGateway` + `HttpConfig` + HTTP JSON-RPC 2.0（`POST /rpc`）      |

---

## 統合

完全な解説 — プロトコルの詳細、**ユースケース別レシピ**（イベントログ、run ライフサイクル、バッチング、多言語エージェント…）、リトライのセマンティクス、systemd デプロイ — については **[`docs/integration.md`](docs/integration.md)** を参照。

コピペで使えるリファレンスクライアント（Python / Node / Go）は **[`clients/`](clients/)** にある。Rust クライアントは組み込み（`sidecar` フィーチャ）。

---

## プロトコル（sidecar、§18）

両トランスポート（UDS と HTTP）は **JSON-RPC 2.0** を使う。ワイヤフォーマットは同一で、違いはトランスポート層だけである。

### JSON-RPC 2.0 リクエスト形式

**UDS**: Unix Domain Socket 上で 1 行につき 1 つの JSON オブジェクト（`\n` 終端）。
**HTTP**: `Content-Type: application/json` の `POST /rpc`。ボディは 1 つの JSON オブジェクト。

バッチ配列はどちらのトランスポートでもサポートされない。

**リクエスト**（`execute`）:
```json
{"jsonrpc":"2.0","id":"<uuid>","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}
```

**リクエスト**（admin）:
```json
{"jsonrpc":"2.0","id":1,"method":"stats"}
{"jsonrpc":"2.0","id":2,"method":"health"}
{"jsonrpc":"2.0","id":3,"method":"checkpoint"}
```

**成功レスポンス**:
```json
{"jsonrpc":"2.0","id":"<uuid>","result":{"status":"committed","commit_seq":42}}
{"jsonrpc":"2.0","id":2,"result":{"status":"ok"}}
```

**エラーレスポンス**:
```json
{"jsonrpc":"2.0","id":"<uuid>","error":{"code":-32000,"message":"NOT NULL constraint failed: t.v"}}
{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error: ..."}}
```

**エラーコード**:

| コード | 意味 | 発生条件 |
|-----:|---------|------|
| `-32700` | パースエラー | ボディ/行が有効な JSON でない |
| `-32600` | 不正なリクエスト | `jsonrpc != "2.0"`、配列、または `method` 欠落 |
| `-32601` | メソッド未検出 | 未知のメソッド名 |
| `-32602` | 不正なパラメータ | `actor_id` または `operations` の欠落/型違い |
| `-32000` | 書き込み失敗 | Execute が `WriteResponse::Failed` を返した |
| `-32001` | ゲートウェイ過負荷 | channel が満杯（`Error::GatewayOverloaded`） |

### HTTP ヘルスエンドポイント

`GET /health` は `{"status":"ok"}` を返す（素の HTTP。JSON-RPC のエンベロープなし）。

### バイナリデータ（BLOB）

パラメータは JSON 値なので、生のバイト列を直接送ることはできない。バイナリデータは `{"$blob": "<base64>"}` センチネルでラップする — これは単一キーのオブジェクトで、その値はバイト列を RFC 4648 標準 base64 でエンコードしたものである。`$blob` センチネルは `operations[].params` の内側に置き、UDS と HTTP のどちらのトランスポートでも同一に動作する:

```json
{"jsonrpc":"2.0","id":"r1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO files(data) VALUES (?)","params":[{"$blob":"aGVsbG8="}]}]}}
```

ゲートウェイは base64 をデコードして `?` プレースホルダに `BLOB` をバインドするので、SQLite はそれをバイナリのストレージクラスとして保存する（テキストではない）。**読み取りはあなたの言語の SQLite ドライバから直接行う**。ドライバはバイト列をネイティブに返す — SqueueLite は設計上、読み取り API を持たない。

---

## セキュリティ（§23）

- **UDS トランスポート**: Unix Domain Socket。ソケットファイルはデフォルトで `0o600`（所有者の読み書きのみ）で作成される。アクセス制御はファイルシステム権限による。別ユーザーや別コンテナで動くエージェントを接続させたい場合は、`SidecarConfig.socket_mode = 0o660` を設定する（かつ全呼び出し側を共有 Unix グループに追加する）か、バイナリに `--socket-mode 660` を渡す。デフォルトの `0o600` はセキュアなベースラインである。
- **HTTP トランスポート**: TCP に公開される。デフォルトのバインドアドレスは `127.0.0.1`（localhost のみ）。TLS と認証を扱うリバースプロキシなしに、これを `0.0.0.0` に変更**してはならない**。外部アクセスと bearer トークン認証は呼び出し側の責務である。
- **信頼モデル**: すべての呼び出し側は信頼されたローカルプロセスと仮定する（MVP）。
- **SQL ガードレール**: `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `RELEASE` / `PRAGMA` は**常に**拒否される — トランザクションのライフサイクルはゲートウェイが所有する。それ以外は `GatewayConfig` で設定可能:

  | フラグ                | デフォルト | 非デフォルト時の効果                                 |
  |----------------------|---------|------------------------------------------------------|
  | `allow_raw_sql`      | `true`  | `false` → すべての操作を拒否                         |
  | `allow_schema_write` | `false` | `true` → `CREATE` / `ALTER` / `DROP` / `TRUNCATE` を許可 |
  | `allow_delete`       | `true`  | `false` → `DELETE` を拒否                           |
  | `allow_drop`         | `false` | `true` → `DROP` を許可                              |

  これらは**先頭トークンのチェック**（先頭の SQL キーワードのみ）である。完全な SQL パーサは MVP では意図的にスコープ外であり、テーブル単位の allowlist は将来項目である。`squeuelite-gateway` バイナリは常にこれらのデフォルトを使う。

---

## その他の設定

`GatewayConfig` は以下も公開する（すべて任意、妥当なデフォルトあり）:

| フィールド        | デフォルト               | 目的                                                 |
|------------------|--------------------------|------------------------------------------------------|
| `journal_mode`   | `Wal`                    | SQLite ジャーナルモード（§16）                       |
| `synchronous`    | `Normal`                 | `synchronous` PRAGMA（§16）                          |
| `busy_timeout_ms`| `5000`                   | SQLite busy timeout（§16）                           |
| `queue_capacity` | `1024`                   | 有界な要求 channel のサイズ（§14）                  |
| `overflow`       | `WaitTimeout{ millis: 5000 }` | キューが満杯のときの挙動: `Wait` / `Reject` / `WaitTimeout`（§14） |
| `track_commits`  | `true`                   | `squeuelite_commits` に `commit_seq` を記録（§12）  |
| `idempotency`    | `true`                   | `idempotency_key` を持つ要求を重複排除（§13）       |
| `batch`          | `None`（無効）           | 単一操作書き込みの日和見的バッチコミット（§15）     |

---

## ライセンス

以下のいずれかの下でライセンスされる:

- Apache License, Version 2.0（[LICENSE-APACHE](LICENSE-APACHE) または
  <http://www.apache.org/licenses/LICENSE-2.0>）
- MIT ライセンス（[LICENSE-MIT](LICENSE-MIT) または
  <http://opensource.org/licenses/MIT>）

お好みの方で。

### コントリビューション

あなたが明示的に別段の意思を表明しない限り、Apache-2.0 ライセンスに定義される通り、あなたによって本作品への包含を意図して提出されたあらゆるコントリビューションは、追加の条項や条件なしに上記のようにデュアルライセンスされる。
