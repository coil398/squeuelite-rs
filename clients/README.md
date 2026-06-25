# SqueueLite reference clients

Tiny, dependency-light clients that speak the SqueueLite **JSON-RPC 2.0 over
Unix Domain Socket** protocol. Copy the one for your language into your agent
and adapt it — they are intentionally ~100 lines each.

> **Standard JSON-RPC 2.0 compatible**: any JSON-RPC 2.0 library in your
> language works out of the box. The bundled thin clients are minimal UDS
> examples; they are not required. For the HTTP transport, `POST /rpc` with a
> JSON-RPC 2.0 body — any HTTP + JSON-RPC library applies.

| Language | File | Deps | Run the demo |
|----------|------|------|--------------|
| Python   | [`python/squeue.py`](python/squeue.py) | stdlib only | `python3 python/squeue.py ./squeuelite.sock` |
| Node     | [`node/squeue.mjs`](node/squeue.mjs)   | Node ≥ 16   | `node node/squeue.mjs ./squeuelite.sock` |
| Deno     | [`deno/squeue.ts`](deno/squeue.ts)     | Deno        | `deno run --allow-read --allow-write deno/squeue.ts ./squeuelite.sock` |
| Bun      | [`bun/squeue.ts`](bun/squeue.ts)       | Bun         | `bun bun/squeue.ts ./squeuelite.sock` |
| Go       | [`go/squeue.go`](go/squeue.go)         | stdlib only | import the package |
| JVM (Java) | [`jvm/Squeue.java`](jvm/Squeue.java) | JDK ≥ 16    | `java jvm/Squeue.java ./squeuelite.sock` |
| Rust     | [`rust/squeue.rs`](rust/squeue.rs) (standalone) · or built-in `Client` (`sidecar` feature) | tokio · serde\_json · base64 | copy the module into your crate |

> **Rust note**: the crate ships a built-in `Client`, but it leaves
> `idempotency_key` / `run_id` as `None`. Use the standalone
> [`rust/squeue.rs`](rust/squeue.rs) when you need those fields.
>
> **JVM note**: `Squeue.java` is dependency-free and returns the raw JSON-RPC
> 2.0 response line — parse it with Jackson/Gson or your own. Kotlin/Scala/Clojure
> can call it directly.

## Wire protocol — JSON-RPC 2.0

One JSON object per line over a Unix Domain Socket (or HTTP `POST /rpc`).

### Request — `execute`

```json
{"jsonrpc":"2.0","id":1,"method":"execute","params":{"actor_id":"<actor>","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}
```

Optional fields inside `params`: `"run_id"` and `"idempotency_key"`.

### Request — admin methods

```json
{"jsonrpc":"2.0","id":2,"method":"stats"}
{"jsonrpc":"2.0","id":3,"method":"health"}
{"jsonrpc":"2.0","id":4,"method":"checkpoint"}
```

No `params` needed for admin methods.

### Success response

```json
{"jsonrpc":"2.0","id":1,"result":{"status":"committed","commit_seq":42}}
```

### Error response

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"write failed: UNIQUE constraint failed"}}
```

### Error codes

| Code | Meaning |
|-----:|---------|
| `-32700` | Parse error — line is not valid JSON |
| `-32600` | Invalid Request — `jsonrpc != "2.0"`, batch array, or `method` missing |
| `-32601` | Method not found — unknown method name |
| `-32602` | Invalid params — `actor_id` or `operations` missing / wrong type |
| `-32000` | Write failed — SQL error, constraint violation, etc. |
| `-32001` | Gateway overloaded — bounded channel full |

## All clients implement the same contract

- **Write**: build a JSON-RPC 2.0 `execute` request, read one JSON-RPC 2.0 response per line.
- **Multiple ops in one call** = one atomic transaction (all-or-nothing).
- **`idempotency_key`** makes a write safe to retry (the gateway dedups it).
- **Admin**: `stats` / `health` / `checkpoint` use method names with no `params`.
- **Response shape**: every method returns the full JSON-RPC 2.0 response object.
  Check `.result` for success and `.error` for failure in your application code.
- **Binary / BLOB**: JSON can't carry raw bytes, so wrap them with each client's
  `blob(...)` helper — it produces a `{"$blob": "<base64>"}` value that the gateway
  decodes into a real BLOB. Reads come back as native bytes from your own driver.

> SqueueLite handles **writes only**. For reads, open the SQLite file directly
> with your language's driver in **read-only** mode (WAL lets readers run
> alongside the single writer).

See [`../docs/integration.md`](../docs/integration.md) for the full protocol,
per-use-case recipes, and deployment notes.
