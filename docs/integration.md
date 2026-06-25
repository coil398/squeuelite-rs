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

| | **In-process** | **Sidecar / HTTP** |
|---|---|---|
| Shape | One Rust process, many async tasks | A separate daemon (`squeuelite-gateway`) |
| Transport | In-memory channel | UDS (JSON-RPC 2.0) and/or HTTP (JSON-RPC 2.0) |
| Callers | Rust only (`InProcessGateway`) | **Any language** that can speak JSON-RPC 2.0 |
| Set `idempotency_key` / `run_id`? | Yes (full `WriteRequest`) | Yes via JSON-RPC params |
| Use when | All writers live in one Rust binary | Writers are separate OS processes, often mixed languages |

The sidecar binary (`squeuelite-gateway`) exposes **one wire protocol on two transports**:

| Transport | Flag | Best for |
|-----------|------|---------|
| **UDS** (Unix Domain Socket) | `--socket <path>` | Local-only, maximum security via filesystem perms |
| **HTTP** | `--http <addr>` | Cross-host or non-Unix clients; default localhost |

**Both transports use JSON-RPC 2.0.** One process runs both simultaneously and shares
**one** `InProcessGateway` (single writer thread). This is the single-writer invariant:
no matter how many callers connect via UDS or HTTP, all writes are serialised through
one SQLite connection.

If your agents are **separate processes in several languages** → use the **sidecar**.
If everything lives in one Rust binary → **in-process** is simplest and fastest.

---

## 2. The wire protocol (JSON-RPC 2.0)

Both transports use **JSON-RPC 2.0**. The envelope is always:

```json
{"jsonrpc":"2.0","id":"<id>","method":"<method>","params":{...}}
```

`id` may be a string, number, or `null`. It is echoed verbatim in every response.

---

### UDS transport (JSON-RPC 2.0 over JSON Lines)

Start: `squeuelite-gateway --db ./app.db --socket ./squeuelite.sock`

One JSON-RPC 2.0 request object per line (`\n`-terminated).
**Batch arrays are not supported** (send one request per line).

#### Execute request (UDS — using socat)

```bash
echo '{"jsonrpc":"2.0","id":"req-1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO events(kind) VALUES (?)","params":["started"]}]}}' \
  | socat UNIX-CONNECT:./squeuelite.sock -
```

#### Admin examples (UDS)

```bash
# Health check
echo '{"jsonrpc":"2.0","id":1,"method":"health"}' | socat UNIX-CONNECT:./squeuelite.sock -

# Stats snapshot
echo '{"jsonrpc":"2.0","id":2,"method":"stats"}' | socat UNIX-CONNECT:./squeuelite.sock -

# WAL checkpoint
echo '{"jsonrpc":"2.0","id":3,"method":"checkpoint"}' | socat UNIX-CONNECT:./squeuelite.sock -
```

---

### HTTP transport (JSON-RPC 2.0 over HTTP POST /rpc)

Start: `squeuelite-gateway --db ./app.db --http 127.0.0.1:8080`

> **Security**: HTTP is TCP-exposed. The default bind address is `127.0.0.1`
> (localhost only). Do **not** use `0.0.0.0` without a reverse proxy that
> handles TLS and authentication. External access and authentication are the
> caller's responsibility. Bearer token auth can be added via a tower layer
> in a future version.

#### Execute request (HTTP — using curl)

```bash
curl -s -XPOST http://127.0.0.1:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"req-1","method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO events(kind) VALUES (?)","params":["started"]}]}}'
```

#### Admin examples (HTTP)

```bash
# Health check (plain HTTP GET, no JSON-RPC envelope)
curl -s http://127.0.0.1:8080/health

# Stats snapshot (JSON-RPC)
curl -s -XPOST http://127.0.0.1:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"stats"}'

# WAL checkpoint (JSON-RPC)
curl -s -XPOST http://127.0.0.1:8080/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"checkpoint"}'
```

---

### Running both transports simultaneously (single writer)

```bash
squeuelite-gateway --db ./app.db --socket ./squeuelite.sock --http 127.0.0.1:8080
```

One process, one SQLite writer thread shared between UDS and HTTP. Ctrl-C
triggers graceful shutdown of both transports and the WAL checkpoint.

---

### JSON-RPC 2.0 request fields

| Field (inside `params`) | Required | Notes |
|-------------------------|----------|-------|
| `actor_id`              | yes      | Who is writing. |
| `run_id`                | no       | Group related writes. |
| `idempotency_key`       | no       | If present, a re-sent request with the **same key + same operations** returns the stored response instead of executing twice (§13). |
| `operations`            | yes      | Array of `{sql, params}`. Multiple = one atomic transaction. |
| `operations[].params`   | —        | Positional, matched to `?` placeholders. JSON values; arrays/objects are stored as JSON text. BLOB encoding: `{"$blob": "<base64>"}`. |

### Success response

```json
{"jsonrpc":"2.0","id":"req-1","result":{"status":"committed","commit_seq":42}}
{"jsonrpc":"2.0","id":2,"result":{"status":"ok"}}
```

### Error response

```json
{"jsonrpc":"2.0","id":"req-1","error":{"code":-32000,"message":"NOT NULL constraint failed: events.val","data":"NOT NULL constraint failed: events.val"}}
{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error: ..."}}
```

### Error codes

| Code | Meaning | When |
|-----:|---------|------|
| `-32700` | Parse error | Body / line is not valid JSON |
| `-32600` | Invalid Request | `jsonrpc != "2.0"`, batch array, or `method` missing |
| `-32601` | Method not found | Unknown method name |
| `-32602` | Invalid params | `actor_id` or `operations` missing / wrong type |
| `-32000` | write failed | Execute returned a failed response (constraint / SQL rejected / invalid param) |
| `-32001` | gateway overloaded | Channel full; back off and retry |

### Methods

| Method | Description |
|--------|-------------|
| `execute` | Execute one or more SQL operations as one atomic transaction |
| `stats` | Return a `StatsSnapshot` (counters, queue depth, latency) |
| `health` | Liveness probe — returns `{"status":"ok"}` |
| `checkpoint` | Run `PRAGMA wal_checkpoint(TRUNCATE)` |

---

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

# 2. run the daemon (one per DB file) — both UDS and HTTP
squeuelite-gateway --db ./app.db --socket /run/app/squeuelite.sock --socket-mode 660 --http 127.0.0.1:8080
```

```python
# each agent, in its own process — using the HTTP transport
import httpx, uuid

def gateway_execute(actor_id, sql, params=None, idempotency_key=None):
    resp = httpx.post("http://127.0.0.1:8080/rpc", json={
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "execute",
        "params": {
            "actor_id": actor_id,
            "idempotency_key": idempotency_key,
            "operations": [{"sql": sql, "params": params or []}],
        }
    })
    return resp.json()

gateway_execute("agent-7", "INSERT INTO events(agent_id, run_id, kind) VALUES (?,?,?)",
                ["agent-7", "run-7", "started"])
```

- **Different OS users / containers?** For UDS: `--socket-mode 660` and a shared group.
  For HTTP: use the HTTP transport instead — TCP crosses user / container boundaries.
- **Same host only** — UDS does not cross machines. HTTP can bind `0.0.0.0` behind a
  reverse proxy for cross-host access (add TLS + auth first).

### 4.2 Append-only event log (idempotent)

Agents stream events; retries must not duplicate them. Use a deterministic
`idempotency_key`.

```python
gateway_execute(
    "agent-7",
    "INSERT INTO events(agent_id, run_id, kind, payload) VALUES (?,?,?,?)",
    params=["agent-7", "run-7", "llm_call", payload_json],
    idempotency_key="run-7:step-1",   # same key on retry → no double insert
)
```

If the agent crashes after the write but before recording success, replaying the
same `(idempotency_key, operations)` returns the original `commit_seq` instead of
inserting again (§13).

### 4.3 Agent run lifecycle (atomic multi-write)

Record an event **and** advance run state together — all-or-nothing (§11). Put
both in one `operations` array.

```python
httpx.post("http://127.0.0.1:8080/rpc", json={
    "jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": "execute",
    "params": {
        "actor_id": "agent-7",
        "idempotency_key": "run-7:finish",
        "operations": [
            {"sql": "INSERT INTO events(agent_id, run_id, kind) VALUES (?,?,?)",
             "params": ["agent-7", "run-7", "completed"]},
            {"sql": "UPDATE runs SET status = ?, finished_at = CURRENT_TIMESTAMP WHERE id = ?",
             "params": ["done", "run-7"]},
        ]
    }
})
```

If either statement fails, neither is applied.

### 4.4 Memory / artifact upsert

```python
gateway_execute(
    "agent-7",
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

### 4.8 Binary / BLOB

Because the wire protocol is JSON, raw bytes cannot be sent as-is. Encode binary data
as RFC 4648 standard base64 and wrap it in a `{"$blob": "<base64>"}` sentinel object
inside `operations[].params`. The `$blob` convention works identically on both UDS
and HTTP transports (same JSON-RPC 2.0 params format).

The gateway detects the single-key sentinel, decodes the base64, and binds a native
BLOB to the SQL placeholder — so SQLite stores the data as the `blob` storage class,
not as text.

```python
import base64, httpx, uuid

data = b"\x89PNG\r\n..."          # arbitrary bytes
encoded = base64.b64encode(data).decode()   # RFC 4648 standard base64

httpx.post("http://127.0.0.1:8080/rpc", json={
    "jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": "execute",
    "params": {
        "actor_id": "agent-a",
        "operations": [
            {"sql": "INSERT INTO files(name, data) VALUES (?, ?)",
             "params": ["avatar.png", {"$blob": encoded}]}
        ]
    }
})

# --- Read directly with your SQLite driver (gateway has no read API) ---
import sqlite3
con = sqlite3.connect("file:./app.db?mode=ro", uri=True)
row = con.execute("SELECT data FROM files WHERE name = ?", ["avatar.png"]).fetchone()
recovered: bytes = row[0]   # the driver returns bytes natively
assert recovered == data
```

Rules for the sentinel:
- The object must have **exactly one key** named `"$blob"` with a **string** value.
- Any other shape (multiple keys, non-string value, or a different key name) is treated as a
  plain JSON object and serialised to text — no BLOB decoding occurs.
- An invalid base64 string returns an error response with an `invalid parameter` message
  (code `-32000` in JSON-RPC 2.0 mode).

### 4.7 Observability

```python
import httpx, uuid

def rpc(method, params=None):
    r = httpx.post("http://127.0.0.1:8080/rpc", json={
        "jsonrpc": "2.0", "id": str(uuid.uuid4()), "method": method,
        **({"params": params} if params else {}),
    })
    return r.json().get("result")

print(rpc("stats"))      # counters + queue depth + commit latency (avg / p95)
print(rpc("health"))     # liveness
print(rpc("checkpoint")) # force a WAL checkpoint (e.g. before backup)
```

---

## 5. Retry & failure semantics

- **Timeout / disconnect** → resend the *same* `idempotency_key`; the gateway
  returns the stored result rather than re-applying (§13, §22.4).
- **Queue full** → JSON-RPC error code `-32001` (`gateway overloaded`).
  The default policy waits up to 5 s, then errors. Back off and retry.
- **Agent dies mid-work** → already-committed writes stay; uncommitted work is
  lost. SqueueLite does not re-run agent work — that is the caller's job (§22.1).

---

## 6. Deployment notes

- **Schema first.** The gateway rejects `CREATE`/`ALTER`/`DROP`/`TRUNCATE` by
  default (§23). Create your tables before starting it (`sqlite3 app.db < schema.sql`),
  or run a gateway built with `allow_schema_write = true`.
- **One writer.** Nothing else may write to the DB file while the gateway runs.
- **UDS socket permissions.** Default `0o600` (owner only). For multi-user / multi-
  container access use `--socket-mode 660` + a shared group.
- **HTTP security.** Default `127.0.0.1` (localhost). Changing to `0.0.0.0` requires
  a reverse proxy with TLS and authentication. Bearer token auth can be added via
  a tower layer (future version).
- **Run it under a supervisor.** Example systemd unit (both transports):

  ```ini
  [Unit]
  Description=SqueueLite gateway
  After=network.target

  [Service]
  ExecStart=/usr/local/bin/squeuelite-gateway \
    --db /var/lib/app/app.db \
    --socket /run/app/squeuelite.sock \
    --socket-mode 660 \
    --http 127.0.0.1:8080
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
| `gateway overloaded` (-32001) | Queue full under load | Back off & retry; raise `queue_capacity` |
| Stale reads | Reader opened before commit | WAL is read-committed; re-query; set `busy_timeout` |
| Works locally, not across hosts via UDS | UDS is single-host (§3 non-goal) | Use HTTP transport with appropriate firewall rules |
