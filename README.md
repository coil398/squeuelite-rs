<p align="center">
  <img src="assets/logo.svg" alt="SqueueLite" width="360">
</p>

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

SqueueLite runs in one of two modes. Both wrap the **same single-writer actor**;
they differ only in *how* callers reach it.

| | **In-process** | **Sidecar** |
|---|---|---|
| Shape | One Rust process, many async tasks | A **separate process** (`squeuelite-gateway`) |
| Transport | In-memory channel (no socket) | Unix Domain Socket + JSON Lines |
| Callers | Rust code via `InProcessGateway` | **Any language** that can write to the socket |
| Speed | Fastest | Slightly slower (socket hop) |
| Use when | All writers live in one Rust binary | Writers are separate OS processes (incl. non-Rust) |

The **sidecar** is the `squeuelite-gateway` binary: a standalone daemon that owns
the one and only writer connection. Your agent processes don't open the database
themselves — they send write requests to the daemon's socket, and it serialises
them into SQLite. This is what lets non-Rust agents (Python, Go, …) write safely.

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

# Write request (table `t` must already exist — see "Setting up your schema")
echo '{"request_id":"r1","actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}' \
  | socat UNIX-CONNECT:./squeuelite.sock -
```

---

## Setting up your schema

By default the gateway runs with **secure write restrictions** (§23): schema
changes (`CREATE` / `ALTER` / `DROP` / `TRUNCATE`) are **rejected**. A fresh
database therefore has no application tables, and you **cannot create them by
sending `CREATE TABLE` through the gateway** unless you opt in. Choose one:

- **Pre-create the schema** — recommended for the `squeuelite-gateway` binary.
  Build your tables in the database file *before* starting the gateway:

  ```bash
  sqlite3 ./app.db < schema.sql
  squeuelite-gateway --db ./app.db --socket ./squeuelite.sock
  ```

  The gateway then only serialises writes against the existing schema.

- **Allow schema writes explicitly** — for in-process, or your own gateway binary.
  Set `allow_schema_write = true` on the config:

  ```rust
  let mut config = GatewayConfig::new("./app.db");
  config.allow_schema_write = true;          // permit CREATE / ALTER over the gateway
  let gateway = InProcessGateway::open_with_config(config)?;
  ```

  For the sidecar, set the same flag on `SidecarConfig` and call
  `SidecarGateway::open(...)` from **your own binary**. The shipped
  `squeuelite-gateway` binary intentionally uses the locked-down defaults and has
  no flag to relax them.

> SqueueLite's internal tables (`squeuelite_commits`, `squeuelite_requests`) are
> always created at startup regardless of these flags — they go through the
> startup migration path (§17), not the request path.

---

## Features

| Feature      | What it adds                                               |
|--------------|------------------------------------------------------------|
| `bundled`    | Compile SQLite from source (default; WAL guaranteed)        |
| `inprocess`  | `InProcessGateway` + `GatewayHandle` (tokio mpsc actor)    |
| `sidecar`    | `SidecarGateway` + `Client` + UDS JSON Lines protocol      |

---

## Integration

For a full walkthrough — protocol details, **per-use-case recipes** (event log,
run lifecycle, batching, multi-language agents…), retry semantics, and a systemd
deployment — see **[`docs/integration.md`](docs/integration.md)**.

Copy-paste reference clients (Python / Node / Go) live in
**[`clients/`](clients/)**; the Rust client is built in (`sidecar` feature).

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

- **Transport**: Unix Domain Sockets only — no TCP. The socket file is created
  with `0o600` (owner read/write only) by default. Access control is via
  filesystem permissions. To allow agents running as a different user or in a
  separate container to connect, set `SidecarConfig.socket_mode = 0o660` (and
  add all callers to a shared Unix group), or pass `--socket-mode 660` to the
  `squeuelite-gateway` binary. The default `0o600` is kept as the secure-by-
  default baseline; relaxing it requires an explicit opt-in.
- **Trust model**: all callers are assumed to be trusted local processes (MVP).
- **SQL guard rails**: `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `RELEASE` /
  `PRAGMA` are **always** rejected — the gateway owns the transaction lifecycle.
  The rest is configurable on `GatewayConfig`:

  | Flag                 | Default | Non-default effect                                   |
  |----------------------|---------|------------------------------------------------------|
  | `allow_raw_sql`      | `true`  | `false` → reject every operation                     |
  | `allow_schema_write` | `false` | `true` → permit `CREATE` / `ALTER` / `DROP` / `TRUNCATE` |
  | `allow_delete`       | `true`  | `false` → reject `DELETE`                            |
  | `allow_drop`         | `false` | `true` → permit `DROP`                               |

  These are **first-token checks** (the leading SQL keyword only); a full SQL
  parser is intentionally out of scope for the MVP, and a table-level allowlist is
  a future item. The `squeuelite-gateway` binary always uses these defaults.

---

## Other configuration

`GatewayConfig` also exposes (all optional, sensible defaults):

| Field            | Default                  | Purpose                                              |
|------------------|--------------------------|------------------------------------------------------|
| `journal_mode`   | `Wal`                    | SQLite journal mode (§16)                            |
| `synchronous`    | `Normal`                 | `synchronous` PRAGMA (§16)                           |
| `busy_timeout_ms`| `5000`                   | SQLite busy timeout (§16)                            |
| `queue_capacity` | `1024`                   | Bounded request channel size (§14)                  |
| `overflow`       | `WaitTimeout{ millis: 5000 }` | Behaviour when the queue is full: `Wait` / `Reject` / `WaitTimeout` (§14) |
| `track_commits`  | `true`                   | Record `commit_seq` in `squeuelite_commits` (§12)   |
| `idempotency`    | `true`                   | Dedup requests carrying an `idempotency_key` (§13)  |
| `batch`          | `None` (disabled)        | Opportunistic batch-commit of single-op writes (§15)|

---

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
