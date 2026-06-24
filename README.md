# SqueueLite

**SqueueLite is not a job queue.**
It is a SQLite write queue: a single-writer gateway for SQLite-backed agent systems.

**SqueueLite はジョブキューではない。**
SQLite への書き込み要求を単一 writer に集約する write gateway である。

> Many agents. One SQLite writer. No job queue.

---

## What it does

SqueueLite serialises concurrent write requests from multiple agents (tasks,
processes, threads) through **one** `rusqlite::Connection`. Each write request
becomes one atomic `BEGIN IMMEDIATE … COMMIT` transaction. Callers get a
`WriteResponse` after the commit (or an error after a rollback).

What it does **not** do: schedule jobs, manage workers, retry failed operations,
or run long-lived background tasks.

---

## Modes

### In-process (single Rust process, multiple async tasks)

Enable the `inprocess` feature:

```toml
[dependencies]
squeuelite = { version = "0.1", features = ["inprocess"] }
```

```rust
use squeuelite::{InProcessGateway, SqlOperation, WriteRequest};

let gateway = InProcessGateway::open("./app.db")?;
let handle  = gateway.handle();

// Spawn many tasks — all share the same handle.
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

### Sidecar (separate process, Unix Domain Socket)

Enable the `sidecar` feature:

```toml
[dependencies]
squeuelite = { version = "0.1", features = ["sidecar"] }
```

#### Start the gateway binary

```bash
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock
```

The gateway listens on `./squeuelite.sock` and shuts down cleanly on Ctrl-C
(WAL checkpoint included).

#### Use the Rust client

```rust
use squeuelite::{Client, SqlOperation};

let mut client = Client::connect("agent-a", "./squeuelite.sock").await?;

// Single operation
let resp = client.execute(SqlOperation {
    sql:    "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
    params: vec!["agent-a".into(), "started".into()],
}).await?;

// Atomic multi-operation transaction
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

#### Poke at the socket with socat

```bash
# Health check
echo '{"type":"health"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Stats snapshot
echo '{"type":"stats"}' | socat UNIX-CONNECT:./squeuelite.sock -

# WAL checkpoint
echo '{"type":"checkpoint"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Write request
echo '{"request_id":"r1","actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}' \
  | socat UNIX-CONNECT:./squeuelite.sock -
```

---

## Features

| Feature      | What it adds                                               |
|--------------|------------------------------------------------------------|
| `bundled`    | Compile SQLite from source (default; WAL guaranteed)        |
| `inprocess`  | `InProcessGateway` + `GatewayHandle` (tokio mpsc actor)    |
| `sidecar`    | `SidecarGateway` + `Client` + UDS JSON Lines protocol      |

---

## Protocol (sidecar, §18)

One JSON object per line (`\n` terminated) over a Unix Domain Socket.

**Client → Gateway**: A [`WriteRequest`] JSON or an admin command:

```json
{ "request_id": "01J...", "actor_id": "agent-a", "operations": [...] }
{ "type": "stats" }
{ "type": "health" }
{ "type": "checkpoint" }
```

**Gateway → Client**: A [`WriteResponse`] JSON or admin response:

```json
{ "request_id": "01J...", "status": "committed", "commit_seq": 42 }
{ "request_id": "01J...", "status": "failed",    "error": "constraint failed" }
{ "accepted": 10, "committed": 9, "failed": 1, "rejected": 0, ... }
{ "status": "ok" }
```

---

## Security (§23)

SqueueLite only listens on Unix Domain Sockets (no TCP). Access control is via
filesystem permissions on the socket file. All callers are assumed to be trusted
local processes in the MVP.

---

## License

MIT OR Apache-2.0
