# Contributing to SqueueLite

Thanks for your interest! SqueueLite is a single-writer SQLite **write gateway**
(not a job queue) — please keep changes aligned with that focus.

## Build & test

Use a **current stable** Rust toolchain — the bundled SQLite build and the
dependency tree track recent Rust, so there is no fixed MSRV.

Everything below is enforced by CI; run it before pushing:

```bash
cargo build  --features sidecar,http
cargo test   --features sidecar,http
cargo clippy --features sidecar,http --tests -- -D warnings
cargo fmt --all --check
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --features sidecar,http
```

Also build the core without optional transports to keep feature gating honest:

```bash
cargo build                       # core (no transports)
cargo build --features inprocess  # in-process gateway only
```

## Features

| Feature     | Adds |
|-------------|------|
| `bundled`   | Compile SQLite from source (default) |
| `inprocess` | In-process gateway (`InProcessGateway`, tokio actor) |
| `sidecar`   | JSON-RPC 2.0 over Unix Domain Socket + the `Client` |
| `http`      | JSON-RPC 2.0 over HTTP (axum) |

## Conventions

- The wire protocol is **JSON-RPC 2.0** (see [`docs/integration.md`](docs/integration.md)).
  If you change it, update the reference clients in [`clients/`](clients/) too.
- One write request = one SQLite transaction. The **single writer** invariant
  (one `rusqlite::Connection`, moved into the writer thread) must be preserved.
- Add tests for behaviour changes. Doc comments must build clean under
  `-D warnings`.
- Keep PRs focused; explain the "why" in the description.

## License

Unless you explicitly state otherwise, any contribution you submit is dual
licensed under **MIT OR Apache-2.0**, with no additional terms (see the README's
Contribution note).
