# SqueueLite reference clients

Tiny, dependency-light clients that speak the SqueueLite **JSON Lines over Unix
Domain Socket** protocol. Copy the one for your language into your agent and
adapt it — they are intentionally ~100 lines each.

| Language | File | Deps | Run the demo |
|----------|------|------|--------------|
| Python   | [`python/squeue.py`](python/squeue.py) | stdlib only | `python3 python/squeue.py ./squeuelite.sock` |
| Node     | [`node/squeue.mjs`](node/squeue.mjs)   | Node ≥ 16   | `node node/squeue.mjs ./squeuelite.sock` |
| Go       | [`go/squeue.go`](go/squeue.go)         | `github.com/google/uuid` | import the package |
| Rust     | built in (`Client`, `sidecar` feature) | —           | see top-level README |

All clients implement the same contract:

- **Write**: send one `WriteRequest` JSON per line, read one `WriteResponse` per line.
- **Multiple ops in one call** = one atomic transaction (all-or-nothing).
- **`idempotency_key`** makes a write safe to retry (the gateway dedups it).
- **Admin**: `stats` / `health` / `checkpoint`.

> SqueueLite handles **writes only**. For reads, open the SQLite file directly
> with your language's driver in **read-only** mode (WAL lets readers run
> alongside the single writer).

See [`../docs/integration.md`](../docs/integration.md) for the full protocol,
per-use-case recipes, and deployment notes.
