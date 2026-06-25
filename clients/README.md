# SqueueLite reference clients

Tiny, dependency-light clients that speak the SqueueLite **JSON Lines over Unix
Domain Socket** protocol. Copy the one for your language into your agent and
adapt it — they are intentionally ~100 lines each.

| Language | File | Deps | Run the demo |
|----------|------|------|--------------|
| Python   | [`python/squeue.py`](python/squeue.py) | stdlib only | `python3 python/squeue.py ./squeuelite.sock` |
| Node     | [`node/squeue.mjs`](node/squeue.mjs)   | Node ≥ 16   | `node node/squeue.mjs ./squeuelite.sock` |
| Deno     | [`deno/squeue.ts`](deno/squeue.ts)     | Deno        | `deno run --allow-read --allow-write deno/squeue.ts ./squeuelite.sock` |
| Bun      | [`bun/squeue.ts`](bun/squeue.ts)       | Bun         | `bun bun/squeue.ts ./squeuelite.sock` |
| Go       | [`go/squeue.go`](go/squeue.go)         | `github.com/google/uuid` | import the package |
| JVM (Java) | [`jvm/Squeue.java`](jvm/Squeue.java) | JDK ≥ 16    | `java jvm/Squeue.java ./squeuelite.sock` |
| Rust     | [`rust/squeue.rs`](rust/squeue.rs) (standalone) · or built-in `Client` (`sidecar` feature) | tokio · serde_json · uuid | copy the module into your crate |

> **Rust note**: the crate ships a built-in `Client`, but it leaves
> `idempotency_key` / `run_id` as `None`. Use the standalone
> [`rust/squeue.rs`](rust/squeue.rs) when you need those fields.
>
> **JVM note**: `Squeue.java` is dependency-free and returns the raw JSON
> response line — parse it with Jackson/Gson or your own. Kotlin/Scala/Clojure
> can call it directly.

All clients implement the same contract:

- **Write**: send one `WriteRequest` JSON per line, read one `WriteResponse` per line.
- **Multiple ops in one call** = one atomic transaction (all-or-nothing).
- **`idempotency_key`** makes a write safe to retry (the gateway dedups it).
- **Admin**: `stats` / `health` / `checkpoint`.
- **Binary / BLOB**: JSON can't carry raw bytes, so wrap them with each client's
  `blob(...)` helper — it produces a `{"$blob": "<base64>"}` value that the gateway
  decodes into a real BLOB. Reads come back as native bytes from your own driver.

> SqueueLite handles **writes only**. For reads, open the SQLite file directly
> with your language's driver in **read-only** mode (WAL lets readers run
> alongside the single writer).

See [`../docs/integration.md`](../docs/integration.md) for the full protocol,
per-use-case recipes, and deployment notes.
