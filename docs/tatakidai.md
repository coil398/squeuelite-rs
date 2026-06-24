# SqueueLite 設計書 v0.2

## 1. 概要

SqueueLite は、複数のエージェントプロセスから発生する SQLite 書き込み要求を、単一の writer プロセスに集約するための軽量 write gateway である。

SqueueLite はジョブキューではない。
SqueueLite はエージェントに仕事を配らない。
SqueueLite はエージェントをスケジューリングしない。
SqueueLite はジョブを claim しない。
SqueueLite は worker pool ではない。

エージェントの生成、割り当て、実行は窓口エージェントまたはアプリケーション本体が行う。

```txt
Client / User
    |
    v
Frontdoor Agent
    |
    | spawn
    v
Agent A / Agent B / Agent C
    |
    | write request
    v
SqueueLite Gateway
    |
    | single SQLite writer
    v
app.db
```

SqueueLite の責務は、各エージェントが SQLite に書き込みたいとき、その書き込みを安全に直列化し、単一の SQLite writer connection から実行することだけである。

## 2. 背景

SQLite は軽量で、エージェントの状態管理、イベントログ、メモリ、成果物メタデータの保存に向いている。

一方で SQLite は同時に複数の writer を持てない。複数エージェントが同じ SQLite ファイルに直接書き込むと、各エージェント側で以下を考える必要が出る。

```txt
- SQLITE_BUSY
- retry
- transaction 管理
- write ordering
- WAL 設定
- checkpoint
- schema migration
- shutdown 時の書き込み flush
```

これを各エージェントに持たせるのは汚い。

そこで、書き込みだけを SqueueLite Gateway に集約する。

```txt
悪い構成:

Agent A ---> app.db
Agent B ---> app.db
Agent C ---> app.db

各agentがSQLite write競合を個別に処理する。


良い構成:

Agent A --\
Agent B ----> SqueueLite Gateway ---> app.db
Agent C --/

SQLite write競合をgatewayだけが処理する。
```

## 3. 非目標

SqueueLite は以下を目指さない。

```txt
- ジョブキュー
- タスクキュー
- エージェントの仕事配布
- ワーカー管理
- スケジューラ
- workflow engine
- distributed queue
- Redis / RabbitMQ / SQS の代替
- 複数ホストからの同一SQLiteファイル共有
- 任意の長時間処理の実行
```

SqueueLite は、あくまで SQLite write gateway である。

## 4. 想定アーキテクチャ

### 4.1 窓口エージェント

窓口エージェントはユーザーや外部イベントから仕事を受け取る。

窓口エージェントは、その仕事ごとにエージェントを spawn する。

```txt
Frontdoor Agent:
  - request を受け取る
  - run_id を発行する
  - 必要なら run 作成を gateway に書き込む
  - Agent を spawn する
  - Agent に run_id / agent_id / gateway endpoint を渡す
```

SqueueLite はこの spawn には関与しない。

### 4.2 実行エージェント

spawn されたエージェントは、自分の仕事だけを行う。

```txt
Agent:
  - LLM を呼ぶ
  - tool を叩く
  - file を読む
  - 計算する
  - 外部APIを呼ぶ
  - 必要なタイミングで gateway に書き込みを依頼する
```

Agent は SQLite に直接 write しない。

### 4.3 SqueueLite Gateway

SqueueLite Gateway は単一の SQLite writer connection を所有する。

```txt
SqueueLite Gateway:
  - write request を受け取る
  - validation する
  - 順番に SQLite transaction として実行する
  - commit 後に response を返す
```

### 4.4 SQLite

SQLite はアプリケーション状態を保存する。

```txt
app.db
  agents
  runs
  events
  memories
  artifacts
  tool_results
  sqlite internal tables
  squeuelite optional metadata
```

## 5. 基本方針

```txt
- write は必ず gateway 経由
- read は直接 SQLite でも gateway 経由でもよい
- gateway は単一 writer connection を持つ
- gateway は長時間処理をしない
- gateway は SQL / write operation を短い transaction で実行する
- gateway は受け付けた write request に対して commit 後に ack を返す
- gateway は job execution をしない
```

## 6. Read / Write 分離

基本は以下。

```txt
Read:
  Agent -> app.db read-only connection

Write:
  Agent -> SqueueLite Gateway -> app.db
```

WAL mode を使えば、reader は writer と共存しやすい。

各エージェントは読み取りについては SQLite を直接開いてよい。
ただし書き込みは必ず gateway に投げる。

```txt
Agent A -- read  --> app.db
Agent A -- write --> SqueueLite Gateway --> app.db
```

read も統制したい場合は gateway 経由にできるが、MVPでは write gateway に集中する。

## 7. Gateway の内部構造

### 7.1 プロセス構成

SqueueLite Gateway は独立プロセスとして起動する。

```bash
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock
```

各エージェントは Unix Domain Socket 経由で gateway に書き込み要求を送る。

```txt
Agent Process A --\
Agent Process B ----> ./squeuelite.sock ---> SqueueLite Gateway ---> app.db
Agent Process C --/
```

### 7.2 内部スレッド

gateway 内部では、SQLite connection を持つ writer thread を1本だけ立てる。

```txt
Socket acceptor
    |
    v
Request parser
    |
    v
bounded channel
    |
    v
Writer thread
    |
    v
rusqlite::Connection
    |
    v
app.db
```

writer thread 以外は SQLite write connection を持たない。

## 8. Write Request

エージェントは gateway に write request を送る。

最小 request は以下。

```json
{
  "request_id": "01J...",
  "actor_id": "agent-123",
  "run_id": "run-456",
  "mode": "transaction",
  "operations": [
    {
      "sql": "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?, ?, ?, ?)",
      "params": ["agent-123", "run-456", "tool_result", "{\"ok\":true}"]
    }
  ]
}
```

gateway はこれを1つの SQLite transaction として実行する。

```sql
BEGIN IMMEDIATE;
-- operations
COMMIT;
```

成功したら response を返す。

```json
{
  "request_id": "01J...",
  "status": "committed",
  "commit_seq": 1024
}
```

失敗したら rollback して error を返す。

```json
{
  "request_id": "01J...",
  "status": "failed",
  "error": "constraint failed"
}
```

## 9. Operation Model

MVPでは SQL operation を中心にする。

```rust
pub struct WriteRequest {
    pub request_id: String,
    pub actor_id: String,
    pub run_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub operations: Vec<SqlOperation>,
}

pub struct SqlOperation {
    pub sql: String,
    pub params: Vec<Value>,
}
```

gateway は `operations` を1つの transaction 内で順番に実行する。

```rust
fn apply_request(conn: &mut Connection, req: WriteRequest) -> Result<WriteResponse> {
    let tx = conn.transaction()?;

    for op in req.operations {
        tx.execute(&op.sql, params_from(op.params))?;
    }

    tx.commit()?;

    Ok(WriteResponse::committed())
}
```

## 10. 任意SQLを許すか

MVPでは任意の prepared SQL を許す。

ただし制約を設ける。

```txt
- params は必ず bind parameter
- 複数 statement を1つの sql string に詰め込まない
- BEGIN / COMMIT / ROLLBACK は禁止
- PRAGMA は原則禁止
- schema migration は専用 endpoint のみ
- destructive operation は設定で禁止可能にする
```

将来的には typed operation も提供できる。

```rust
pub enum Operation {
    AppendEvent(AppendEvent),
    UpsertMemory(UpsertMemory),
    InsertArtifact(InsertArtifact),
    RawSql(SqlOperation),
}
```

ただし最初から domain schema に依存するとOSSとして使いにくい。
MVPは raw prepared SQL を中核にする。

## 11. Transaction Semantics

1つの write request は1つの transaction として扱う。

```txt
request accepted
  ↓
BEGIN IMMEDIATE
  ↓
operation 1
operation 2
operation 3
  ↓
COMMIT
  ↓
ack
```

途中で失敗した場合は rollback する。

```txt
operation 2 failed
  ↓
ROLLBACK
  ↓
error response
```

これにより、エージェントは複数の書き込みを atomic に扱える。

例。

```json
{
  "operations": [
    {
      "sql": "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?, ?, ?, ?)",
      "params": ["agent-1", "run-1", "started", "{}"]
    },
    {
      "sql": "UPDATE runs SET status = ? WHERE id = ?",
      "params": ["running", "run-1"]
    }
  ]
}
```

この2つは一緒に成功するか、一緒に失敗する。

## 12. Ordering

gateway は writer thread で request を直列処理する。

同じ gateway に到達した request は、writer thread が受け取った順に commit される。

ただし、複数エージェント間の「現実時間上の発生順」は保証しない。
保証するのは gateway 内部での commit order である。

commit order を観測可能にするため、gateway は `commit_seq` を返す。

```txt
commit_seq = monotonically increasing integer
```

内部テーブルを使う場合。

```sql
CREATE TABLE IF NOT EXISTS squeuelite_commits (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  run_id TEXT,
  committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

このテーブルは optional にする。
高速性重視なら無効化できる。

## 13. Idempotency

エージェントは timeout や接続断で同じ write request を再送する可能性がある。

そのため、任意で `idempotency_key` を使えるようにする。

```json
{
  "request_id": "01J...",
  "idempotency_key": "run-1:event-17"
}
```

gateway は同じ `idempotency_key` の request を二重に commit しない。

内部テーブル。

```sql
CREATE TABLE IF NOT EXISTS squeuelite_requests (
  idempotency_key TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  response_json TEXT,
  commit_seq INTEGER,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

挙動。

```txt
初回:
  execute
  commit
  response保存
  response返却

再送:
  request_hash が同じなら保存済み response を返す
  request_hash が違うなら conflict error
```

MVPでは idempotency は optional feature でもよい。

## 14. Backpressure

gateway は bounded queue を持つ。

```rust
bounded_channel_size = 1024
```

queue が満杯の場合、gateway は request を無限に溜めない。

挙動は設定可能。

```txt
- wait
- reject with overloaded
- timeout
```

デフォルトは timeout 付き wait。

```json
{
  "status": "failed",
  "error": "gateway overloaded"
}
```

エージェント側は backoff して再送する。

## 15. Batching

高速化のため、gateway は batch commit をサポートする。

ただし、MVPでは各 request を独立 transaction として処理する。

v0.2以降で以下を追加する。

```txt
- 複数の単一INSERT requestを短時間だけ集める
- 1つのtransactionでまとめてcommitする
- 各requestへ個別responseを返す
```

設定例。

```txt
max_batch_size = 64
max_batch_delay_micros = 500
```

注意点。

```txt
- 明示transaction requestは他requestと混ぜない
- idempotency付きrequestは順序とresponse保存に注意する
- latency優先ならbatchを無効化する
```

## 16. WAL / SQLite 設定

gateway 起動時に SQLite connection に対して推奨設定を適用する。

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;
```

`sync_mode` は設定可能にする。

```txt
NORMAL:
  速い。通常のアプリ用途向け。

FULL:
  より安全。クラッシュ耐性を重視する用途向け。
```

checkpoint は gateway が管理する。

```txt
- 起動時 checkpoint
- shutdown 時 checkpoint
- WALサイズが閾値を超えたら checkpoint
```

## 17. Migration

schema migration は gateway 経由で行う。

理由は、migration も write であり、他のエージェント書き込みと競合させたくないため。

MVPでは以下のどちらかにする。

### 案A: 起動時のみ migration

```txt
gateway start
  ↓
exclusive migration
  ↓
listen start
```

単純で安全。

### 案B: admin endpoint

```txt
POST /admin/migrate
```

運用中 migration も可能だが複雑になる。

MVPでは案Aを採用する。

## 18. Protocol

### 18.1 MVP

Unix Domain Socket + JSON Lines。

```txt
request:
  one JSON per line

response:
  one JSON per line
```

理由。

```txt
- 実装が小さい
- デバッグしやすい
- Rust以外のagentからも叩ける
- curlではなくsocat等で確認できる
```

### 18.2 将来

高速化が必要なら以下を追加する。

```txt
- MessagePack
- bincode
- Cap'n Proto
- gRPC
```

ただしMVPでは不要。

## 19. Rust Crate 構成

```txt
squeuelite/
  Cargo.toml
  src/
    lib.rs
    config.rs
    error.rs
    protocol.rs
    gateway.rs
    writer.rs
    sqlite.rs
    client.rs
    idempotency.rs
  bin/
    squeuelite-gateway.rs
  examples/
    spawn_agents.rs
    append_events.rs
    sidecar_client.rs
```

依存候補。

```toml
[dependencies]
rusqlite = { version = "*", features = ["bundled"] }
serde = { version = "*", features = ["derive"] }
serde_json = "*"
thiserror = "*"
uuid = { version = "*", features = ["v7"] }

tokio = { version = "*", features = ["net", "rt-multi-thread", "macros", "io-util", "sync"], optional = true }
```

`rusqlite/bundled` は feature にしてもよい。

```toml
[features]
default = ["bundled", "tokio"]
bundled = ["rusqlite/bundled"]
tokio = ["dep:tokio"]
```

## 20. Public API

### 20.1 Gateway 起動

```rust
let gateway = Gateway::open(GatewayConfig {
    db_path: "./app.db".into(),
    socket_path: "./squeuelite.sock".into(),
    journal_mode: JournalMode::Wal,
    synchronous: SyncMode::Normal,
    queue_capacity: 1024,
})?;

gateway.run().await?;
```

### 20.2 Client

```rust
let client = Client::connect("./squeuelite.sock").await?;

client.execute(SqlOperation {
    sql: "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?, ?, ?, ?)".into(),
    params: vec![
        agent_id.into(),
        run_id.into(),
        "tool_result".into(),
        payload_json.into(),
    ],
}).await?;
```

### 20.3 Transaction

```rust
client.transaction(vec![
    SqlOperation {
        sql: "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?, ?, ?, ?)".into(),
        params: vec![agent_id.into(), run_id.into(), "started".into(), "{}".into()],
    },
    SqlOperation {
        sql: "UPDATE runs SET status = ? WHERE id = ?".into(),
        params: vec!["running".into(), run_id.into()],
    },
]).await?;
```

## 21. In-process Mode

同一Rustプロセス内で複数agent taskをspawnする場合、socketを使わず in-process gateway を使える。

```txt
Frontdoor Agent
  |
  | tokio spawn
  v
Agent Tasks
  |
  | mpsc
  v
Writer Actor
  |
  v
app.db
```

API例。

```rust
let gateway = InProcessGateway::open("./app.db")?;

let handle = gateway.handle();

tokio::spawn(async move {
    handle.execute(...).await?;
});
```

この mode は最速。
ただし、別プロセスのagentからは使えない。

MVPの実装順は以下。

```txt
1. in-process writer actor
2. Unix socket sidecar
```

または、最初から sidecar を主にしてもよい。

## 22. Failure Semantics

### 22.1 Agent が死んだ場合

agent が死んでも、gateway が既に commit した書き込みは残る。

gateway に送る前の書き込みは失われる。

これは正常。

SqueueLite は agent の仕事を再実行しない。
再実行責任は窓口エージェントまたは上位ランタイムにある。

### 22.2 Gateway が死んだ場合

commit 済みの request は SQLite に残る。

commit 前の request は失敗する可能性がある。

agent 側は response を受け取れなかった request を idempotency_key 付きで再送できる。

### 22.3 SQLite transaction 中にクラッシュした場合

SQLite の transaction に従い、commit 前なら rollback される。

### 22.4 Response 送信前に gateway が死んだ場合

DBにはcommit済みだが、agentはresponseを受け取っていない可能性がある。

この問題は idempotency_key で吸収する。

```txt
agent retries same idempotency_key
  ↓
gateway sees already committed
  ↓
same response returned
```

## 23. Security / Safety

MVPではローカル専用を前提にする。

```txt
- Unix Domain Socket
- file permission でアクセス制御
- TCP listen はしない
```

任意SQLを受け付ける場合、呼び出し元は信頼済みプロセスである必要がある。

設定で以下を制御できるようにする。

```txt
allow_raw_sql = true / false
allow_schema_write = false
allow_delete = true / false
allow_drop = false
```

将来的には operation allowlist を入れる。

```toml
[allowlist]
tables = ["events", "memories", "artifacts", "runs"]
```

## 24. Observability

gateway は最低限の stats を持つ。

```txt
- accepted_requests
- committed_requests
- failed_requests
- rejected_requests
- avg_commit_latency
- p95_commit_latency
- queue_depth
- current_wal_size
```

admin endpoint。

```txt
GET /health
GET /stats
POST /checkpoint
```

JSON Lines protocol なら admin command として実装する。

```json
{ "type": "stats" }
```

## 25. Performance 方針

速くするための方針。

```txt
- SQLite writer connection は1つだけ
- prepared statement cache を使う
- transaction は短くする
- params はbindする
- JSON parse回数を減らす
- socket protocol は後でbinary化可能にする
- batch commitを後から追加できる構造にする
```

最小実装では、性能より正しい直列化を優先する。

ただし設計上、single writer thread はSQLiteの性質に合うため、十分速いはずである。

## 26. Naming

`SqueueLite` は名前として使えるが、queue という語が job queue と誤解される可能性がある。

README の冒頭で必ず明記する。

```txt
SqueueLite is not a job queue.
It is a SQLite write queue: a single-writer gateway for SQLite-backed agent systems.
```

日本語では以下。

```txt
SqueueLite はジョブキューではない。
SQLiteへの書き込み要求を単一writerに集約する write gateway である。
```

## 27. MVP

v0.1.0 の範囲。

```txt
- single writer gateway
- in-process mode
- Unix Domain Socket sidecar
- JSON Lines protocol
- execute
- transaction
- commit response
- error response
- WAL setup
- graceful shutdown
- basic stats
```

入れないもの。

```txt
- job queue
- worker pool
- scheduler
- retry job
- dead letter
- workflow
- distributed mode
- TCP server
- dashboard
- complex permissions
```

## 28. 最小実装スケッチ

```rust
pub struct Gateway {
    sender: mpsc::Sender<Command>,
}

pub enum Command {
    Write {
        request: WriteRequest,
        respond_to: oneshot::Sender<WriteResponse>,
    },
    Shutdown,
}

pub struct Writer {
    conn: rusqlite::Connection,
    receiver: mpsc::Receiver<Command>,
}

impl Writer {
    pub fn run(mut self) -> Result<()> {
        while let Some(cmd) = self.receiver.blocking_recv() {
            match cmd {
                Command::Write { request, respond_to } => {
                    let response = self.apply(request);
                    let _ = respond_to.send(response);
                }
                Command::Shutdown => break,
            }
        }

        Ok(())
    }

    fn apply(&mut self, request: WriteRequest) -> WriteResponse {
        let tx = match self.conn.transaction() {
            Ok(tx) => tx,
            Err(err) => return WriteResponse::failed(err),
        };

        for op in request.operations {
            if let Err(err) = tx.execute(&op.sql, params_from(op.params)) {
                return WriteResponse::failed(err);
            }
        }

        match tx.commit() {
            Ok(_) => WriteResponse::committed(),
            Err(err) => WriteResponse::failed(err),
        }
    }
}
```

## 29. 結論

SqueueLite は、エージェントに仕事を配るものではない。

仕事の受け付けとエージェント spawn は、窓口エージェントが行う。

SqueueLite が解く問題はただ1つ。

```txt
複数のspawn済みエージェントが同じSQLiteに書き込みたいとき、
SQLiteの単一writer制約をどう扱うか。
```

答えは、write gateway である。

```txt
Many agents.
One SQLite writer.
No job queue.
```

