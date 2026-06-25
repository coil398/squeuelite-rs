# Integration guide

How to wire SqueueLite into a real application — protocol, per-use-case recipes,
and deployment notes.

> **One rule to remember**
>
> ```txt
> WRITE → always through the gateway
> READ  → open SQLite yourself (read-only; WAL lets readers run beside the writer)
> ```
>
> There is exactly **one** gateway per database file, and it is the **only**
> writer. SqueueLite has no read API by design (§6).

---

## 1. Choosing a mode

| | **In-process** | **Sidecar** |
|---|---|---|
| Shape | One Rust process, many async tasks | A separate daemon (`squeuelite-gateway`) |
| Transport | In-memory channel | Unix Domain Socket + JSON Lines |
| Callers | Rust only (`InProcessGateway`) | **Any language** that can write to the socket |
| Set `idempotency_key` / `run_id`? | Yes (full `WriteRequest`) | Yes via raw JSON / reference clients (the built-in Rust `Client` leaves them `None`) |
| Use when | All writers live in one Rust binary | Writers are separate OS processes, often mixed languages |

If your agents are **separate processes in several languages** → use the
**sidecar**. If everything lives in one Rust binary → **in-process** is simplest
and fastest.

---

## 2. The wire protocol (sidecar)

One JSON object per line (`\n`-terminated), request → response, over the socket.

### Request (client → gateway)

```json
{"request_id":"<uuid>","actor_id":"agent-a","run_id":"run-7","idempotency_key":"run-7:step-3","operations":[{"sql":"INSERT INTO events(agent_id,kind) VALUES (?,?)","params":["agent-a","started"]}]}
```

| Field             | Required | Notes |
|-------------------|----------|-------|
| `request_id`      | yes      | Unique per request (UUIDv7 recommended). Echoed back. |
| `actor_id`        | yes      | Who is writing. |
| `run_id`          | no       | Group related writes. |
| `idempotency_key` | no       | If present, a re-sent request with the **same key + same operations** returns the stored response instead of executing twice (§13). |
| `operations`      | yes      | Array of `{sql, params}`. Multiple = one atomic transaction. |
| `operations[].params` | —    | Positional, matched to `?` placeholders. JSON values; arrays/objects are stored as JSON text. |

### Response (gateway → client)

```json
{"request_id":"<uuid>","status":"committed","commit_seq":42}
{"request_id":"<uuid>","status":"failed","error":"constraint failed"}
```

### Admin commands

```json
{"type":"stats"}        → {"accepted":10,"committed":9,"failed":1,"rejected":0,"queue_depth":0,...,"avg_commit_latency_micros":83.2,"p95_commit_latency_micros":140}
{"type":"health"}       → {"status":"ok"}
{"type":"checkpoint"}   → {"status":"ok"}     (runs PRAGMA wal_checkpoint(TRUNCATE))
```

Ready-made clients live in [`../clients/`](../clients/) (Python, Node, Go, Rust).

---

## 3. Reads

SqueueLite never reads for you. Each process opens the SQLite file **read-only**
with its own driver. WAL mode lets readers run concurrently with the single
writer.

```python
import sqlite3
con = sqlite3.connect("file:./app.db?mode=ro", uri=True)
con.execute("PRAGMA busy_timeout=5000")
rows = con.execute("SELECT kind, payload FROM events WHERE run_id = ?", ["run-7"]).fetchall()
```

```rust
use rusqlite::{Connection, OpenFlags};
let read = Connection::open_with_flags("./app.db", OpenFlags::SQLITE_OPEN_READ_ONLY)?;
let n: i64 = read.query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
```

---

## 4. Use-case recipes

The examples assume a schema like the one the design sketches (§4.4): `agents`,
`runs`, `events`, `memories`, `artifacts`, `tool_results`. Create it **before**
starting the gateway (see §6).

### 4.1 Multi-process AI agents, mixed languages (the common case)

A frontdoor process spawns agents (Python / Node / Go / …). Each agent writes
its progress through the one gateway.

```bash
# 1. create the schema once (gateway rejects DDL by default, §23)
sqlite3 ./app.db < schema.sql

# 2. run the daemon (one per DB file)
squeuelite-gateway --db ./app.db --socket /run/app/squeuelite.sock --socket-mode 660
```

```python
# each agent, in its own process
from squeue import Squeue   # clients/python/squeue.py
db = Squeue("/run/app/squeuelite.sock", actor_id="agent-7")
db.execute("INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?,?,?,?)",
           ["agent-7", "run-7", "tool_result", '{"ok":true}'],
           idempotency_key="run-7:event-1", run_id="run-7")
```

- **Different OS users / containers?** Start the gateway with `--socket-mode 660`
  and put every agent's user in a shared group. (Default `0o600` is owner-only.)
- **Same host only** — UDS does not cross machines (§3 non-goal). Share the socket
  via a mounted volume if agents are in separate containers on one host.

### 4.2 Append-only event log (idempotent)

Agents stream events; retries must not duplicate them. Use a deterministic
`idempotency_key`.

```python
db.execute(
    "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?,?,?,?)",
    ["agent-7", "run-7", "llm_call", payload_json],
    idempotency_key=f"run-7:{step_id}",   # same key on retry → no double insert
)
```

If the agent crashes after the write but before recording success, replaying the
same `(idempotency_key, operations)` returns the original `commit_seq` instead of
inserting again (§13).

### 4.3 Agent run lifecycle (atomic multi-write)

Record an event **and** advance run state together — all-or-nothing (§11). Put
both in one `transaction`/`operations` array.

```python
db.transaction([
    ("INSERT INTO events(agent_id, run_id, kind) VALUES (?,?,?)", ["agent-7", "run-7", "completed"]),
    ("UPDATE runs SET status = ?, finished_at = CURRENT_TIMESTAMP WHERE id = ?", ["done", "run-7"]),
], idempotency_key="run-7:finish")
```

If either statement fails, neither is applied.

### 4.4 Memory / artifact upsert

```python
db.execute(
    "INSERT INTO memories(agent_id, key, value) VALUES (?,?,?) "
    "ON CONFLICT(agent_id, key) DO UPDATE SET value = excluded.value",
    ["agent-7", "scratchpad", value_json],
)
```

`DELETE` is allowed by default; `DROP` and schema changes are not (§23). Flip
`allow_delete` / `allow_drop` / `allow_schema_write` on the config if you need them.

### 4.5 High-throughput ingestion (batching)

When many agents fire small single-row inserts, enable batching so the writer
commits them together (one outer transaction, per-request isolation via
SAVEPOINTs, §15). Build a gateway with batching on:

```rust
let mut cfg = GatewayConfig::new("./app.db");
cfg.batch = Some(BatchConfig { max_size: 64, max_delay_micros: 500 });
let gateway = InProcessGateway::open_with_config(cfg)?; // or wrap in SidecarConfig
```

Each request still gets its own response and `commit_seq`; a failure in one
batched request rolls back only that request. Leave `batch = None` (default) for
latency-sensitive workloads.

### 4.6 Single Rust service (in-process)

```rust
// at startup — keep the gateway alive for the whole process
let gateway = InProcessGateway::open("./app.db")?;
let handle  = gateway.handle();            // GatewayHandle is Clone; share it freely

// in a handler / task
handle.execute(WriteRequest {
    request_id:      uuid::Uuid::now_v7().to_string(),
    actor_id:        "agent-a".into(),
    run_id:          Some("run-7".into()),
    idempotency_key: Some("run-7:event-1".into()),
    operations: vec![SqlOperation {
        sql:    "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
        params: vec!["agent-a".into(), "started".into()],
    }],
}).await?;

gateway.shutdown().await?;   // on shutdown — flushes the WAL checkpoint
```

### 4.7 Observability

```python
print(db.stats())   # counters + queue depth + commit latency (avg / p95)
db.health()         # liveness
db.checkpoint()     # force a WAL checkpoint (e.g. before backup)
```

---

## 5. Retry & failure semantics

- **Timeout / disconnect** → resend the *same* `idempotency_key`; the gateway
  returns the stored result rather than re-applying (§13, §22.4).
- **Queue full** → you get `{"status":"failed","error":"gateway overloaded"}`
  (default policy waits up to 5 s, then errors). Back off and retry.
- **Agent dies mid-work** → already-committed writes stay; uncommitted work is
  lost. SqueueLite does not re-run agent work — that is the caller's job (§22.1).

---

## 6. Deployment notes

- **Schema first.** The gateway rejects `CREATE`/`ALTER`/`DROP`/`TRUNCATE` by
  default (§23). Create your tables before starting it (`sqlite3 app.db < schema.sql`),
  or run a gateway built with `allow_schema_write = true`.
- **One writer.** Nothing else may write to the DB file while the gateway runs.
- **Socket permissions.** Default `0o600` (owner only). For multi-user / multi-
  container access use `--socket-mode 660` + a shared group.
- **Run it under a supervisor.** Example systemd unit:

  ```ini
  [Unit]
  Description=SqueueLite gateway
  After=network.target

  [Service]
  ExecStart=/usr/local/bin/squeuelite-gateway --db /var/lib/app/app.db --socket /run/app/squeuelite.sock --socket-mode 660
  Restart=on-failure
  User=app
  Group=app

  [Install]
  WantedBy=multi-user.target
  ```

  Ctrl-C / `SIGINT` triggers a graceful shutdown (WAL checkpoint + socket unlink).

---

## 7. Gotchas at a glance

| Symptom | Cause | Fix |
|---------|-------|-----|
| `sql rejected` on `CREATE TABLE` | `allow_schema_write=false` (default) | Pre-create schema, or set the flag |
| Agent can't connect to socket | Socket is `0o600`, agent runs as another user | `--socket-mode 660` + shared group |
| `no such table` | Schema never created | Create it before starting the gateway |
| Duplicate rows after a retry | No `idempotency_key` | Add a deterministic key (`run:step`) |
| `gateway overloaded` | Queue full under load | Back off & retry; raise `queue_capacity` |
| Stale reads | Reader opened before commit | WAL is read-committed; re-query; set `busy_timeout` |
| Works locally, not across hosts | UDS is single-host (§3 non-goal) | Keep agents + gateway on one host |
