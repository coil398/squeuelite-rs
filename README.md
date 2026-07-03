<p align="center">
  <img src="assets/logo.png" alt="SqueueLite" width="360">
</p>

<p align="center">
  <a href="https://github.com/coil398/squeuelite-rs/actions/workflows/ci.yml"><img src="https://github.com/coil398/squeuelite-rs/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/squeuelite"><img src="https://img.shields.io/crates/v/squeuelite.svg" alt="crates.io"></a>
  <a href="https://docs.rs/squeuelite"><img src="https://img.shields.io/docsrs/squeuelite" alt="docs.rs"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg" alt="License: MIT OR Apache-2.0"></a>
</p>

# SqueueLite

**English** | [日本語](README.ja.md)

**SqueueLite is not a job queue.**
It is a SQLite write queue: a single-writer gateway for SQLite-backed agent systems.

> Many agents. One SQLite writer. No job queue.

---

## What it does

SqueueLite serialises concurrent write requests from multiple agents (tasks,
processes, threads) through **one** `rusqlite::Connection`. Each write request
becomes one atomic `BEGIN IMMEDIATE … COMMIT` transaction. Callers get a
`WriteResponse` after the commit (or an error after a rollback).

What it does **not** do: schedule jobs, manage workers, retry failed operations,
or run long-lived background tasks.

**When to reach for it:** if all your writers live in one Rust process, a thin
actor such as [`tokio-rusqlite`](https://crates.io/crates/tokio-rusqlite) already
covers that case. SqueueLite earns its keep when writers are **separate processes
or non-Rust languages** and you want one gateway — idempotency, batching,
backpressure, JSON-RPC over UDS/HTTP — in front of the single writer.

---

## Modes

SqueueLite runs in one of two modes. Both wrap the **same single-writer actor**;
they differ only in *how* callers reach it.

| | **In-process** | **Sidecar** |
|---|---|---|
| Shape | One Rust process, many async tasks | A **separate process** (`squeuelite-gateway`) |
| Transport | In-memory channel (no socket) | UDS and/or HTTP — **JSON-RPC 2.0** |
| Callers | Rust code via `InProcessGateway` | **Any language** that can speak JSON-RPC 2.0 |
| Speed | Fastest | Slightly slower (socket/network hop) |
| Use when | All writers live in one Rust binary | Writers are separate OS processes (incl. non-Rust) |

The **sidecar** is the `squeuelite-gateway` binary: a standalone daemon that owns
the one and only writer connection. All transports use **JSON-RPC 2.0**. One process
can serve both UDS and HTTP simultaneously, sharing a single writer thread.

| Transport | Flag | Best for |
|-----------|------|----------|
| **UDS** (Unix Domain Socket) | `--socket <path>` | Local-only, maximum security via filesystem perms |
| **HTTP** `POST /rpc` | `--http <addr>` | Cross-host or non-Unix clients; default localhost |

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
# UDS only (JSON-RPC 2.0 over JSON Lines)
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock

# HTTP only (JSON-RPC 2.0 over HTTP POST /rpc)
squeuelite-gateway --db ./app.db --http 127.0.0.1:8080

# Both transports simultaneously — one process, one writer
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock --http 127.0.0.1:8080
```

At least one of `--socket` or `--http` is required. The gateway shuts down
cleanly on Ctrl-C (WAL checkpoint included). One process, one SQLite writer.

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

#### Poke at the UDS socket with socat (JSON-RPC 2.0)

```bash
# Health check
echo '{"jsonrpc":"2.0","id":1,"method":"health"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Stats snapshot
echo '{"jsonrpc":"2.0","id":2,"method":"stats"}' | socat UNIX-CONNECT:./squeuelite.sock -

# WAL checkpoint
echo '{"jsonrpc":"2.0","id":3,"method":"checkpoint"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Execute (table `t` must already exist — see "Setting up your schema")
echo '{"jsonrpc":"2.0","id":4,"method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}' \
  | socat UNIX-CONNECT:./squeuelite.sock -
```

#### Poke at the HTTP endpoint with curl (JSON-RPC 2.0)

```bash
# Health check (plain HTTP GET)
curl -s http://127.0.0.1:8080/health

# Execute (JSON-RPC 2.0 POST)
curl -s -XPOST http://127.0.0.1:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"r1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}'
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

| Feature      | What it adds                                                        |
|--------------|---------------------------------------------------------------------|
| `bundled`    | Compile SQLite from source (default; WAL guaranteed)                 |
| `inprocess`  | `InProcessGateway` + `GatewayHandle` (tokio mpsc actor)             |
| `sidecar`    | `SidecarGateway` + `Client` + UDS JSON-RPC 2.0 transport            |
| `http`       | `HttpGateway` + `HttpConfig` + HTTP JSON-RPC 2.0 (`POST /rpc`)     |

---

## Integration

For a full walkthrough — protocol details, **per-use-case recipes** (event log,
run lifecycle, batching, multi-language agents…), retry semantics, and a systemd
deployment — see **[`docs/integration.md`](docs/integration.md)**.

Copy-paste reference clients (Python / Node / Go) live in
**[`clients/`](clients/)**; the Rust client is built in (`sidecar` feature).

---

## Protocol (sidecar, §18)

Both transports (UDS and HTTP) use **JSON-RPC 2.0**. The wire format is identical;
only the transport layer differs.

### JSON-RPC 2.0 request format

**UDS**: one JSON object per line (`\n`-terminated) over a Unix Domain Socket.
**HTTP**: `POST /rpc` with `Content-Type: application/json`; body is one JSON object.

Batch arrays are not supported on either transport.

**Request** (`execute`):
```json
{"jsonrpc":"2.0","id":"<uuid>","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}
```

**Request** (admin):
```json
{"jsonrpc":"2.0","id":1,"method":"stats"}
{"jsonrpc":"2.0","id":2,"method":"health"}
{"jsonrpc":"2.0","id":3,"method":"checkpoint"}
```

**Success response**:
```json
{"jsonrpc":"2.0","id":"<uuid>","result":{"status":"committed","commit_seq":42}}
{"jsonrpc":"2.0","id":2,"result":{"status":"ok"}}
```

**Error response**:
```json
{"jsonrpc":"2.0","id":"<uuid>","error":{"code":-32000,"message":"NOT NULL constraint failed: t.v"}}
{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error: ..."}}
```

**Error codes**:

| Code | Meaning | When |
|-----:|---------|------|
| `-32700` | Parse error | Body / line is not valid JSON |
| `-32600` | Invalid Request | `jsonrpc != "2.0"`, array, or `method` missing |
| `-32601` | Method not found | Unknown method name |
| `-32602` | Invalid params | `actor_id` or `operations` missing / wrong type |
| `-32000` | write failed | Execute returned `WriteResponse::Failed` |
| `-32001` | gateway overloaded | Channel full (`Error::GatewayOverloaded`) |

### HTTP health endpoint

`GET /health` returns `{"status":"ok"}` (plain HTTP, no JSON-RPC envelope).

### Binary data (BLOB)

Params are JSON values, so raw bytes cannot be sent directly. Wrap binary data in a
`{"$blob": "<base64>"}` sentinel — a single-key object whose value is the
RFC 4648 standard base64 encoding of the bytes. The `$blob` sentinel lives inside
`operations[].params` and works identically on both UDS and HTTP transports:

```json
{"jsonrpc":"2.0","id":"r1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO files(data) VALUES (?)","params":[{"$blob":"aGVsbG8="}]}]}}
```

The gateway decodes the base64 and binds a `BLOB` to the `?` placeholder, so SQLite stores
it as the binary storage class (not as text). **Reads are done directly through your
language's SQLite driver**, which returns the bytes natively — SqueueLite has no read API
by design.

---

## Security (§23)

- **UDS transport**: Unix Domain Sockets. The socket file is created with
  `0o600` (owner read/write only) by default. Access control is via filesystem
  permissions. To allow agents running as a different user or in a separate
  container to connect, set `SidecarConfig.socket_mode = 0o660` (and add all
  callers to a shared Unix group), or pass `--socket-mode 660` to the binary.
  The default `0o600` is the secure-by-default baseline.
- **HTTP transport**: TCP-exposed. The default bind address is `127.0.0.1`
  (localhost only). Do **not** change this to `0.0.0.0` without a reverse proxy
  that handles TLS and authentication. External access and bearer-token
  authentication are the caller's responsibility.
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
