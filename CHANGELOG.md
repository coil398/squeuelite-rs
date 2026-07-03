# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); the project aims to follow
[Semantic Versioning](https://semver.org/) once it reaches 1.0.

## [0.1.0] - 2026-07-03

Initial release. SqueueLite is a **single-writer SQLite write gateway** (not a job
queue): it serialises concurrent writes from many agents through one
`rusqlite::Connection`.

### Added

- **In-process gateway** (`InProcessGateway` / `GatewayHandle`) — a tokio mpsc +
  dedicated writer-thread actor (`inprocess` feature).
- **JSON-RPC 2.0** wire protocol over two transports, served by one process that
  shares a single writer:
  - Unix Domain Socket (`sidecar` feature), with configurable socket
    permissions (`--socket-mode`).
  - HTTP via axum (`http` feature): `POST /rpc` + `GET /health`, binds
    `127.0.0.1` by default, with optional bearer-token auth
    (`SQUEUELITE_HTTP_TOKEN`).
- One request = one `BEGIN IMMEDIATE` transaction; atomic multi-op transactions.
- **Idempotency** (`idempotency_key`) with the dedup record written atomically
  in the same transaction as the write.
- **Batching** with per-request `SAVEPOINT` isolation.
- Configurable **backpressure** (`OverflowPolicy`), monotonic commit sequence
  numbers, and basic **stats** (counters, queue depth, avg/p95 commit latency).
- SQL guard rails and `allow_*` security flags; WAL/PRAGMA setup; startup
  migration.
- Binary **BLOB** parameters via a `{"$blob":"<base64>"}` value.
- **Reference clients** in 7 languages: Python, Node, Deno, Bun, Go, JVM, Rust.
- Docs: `docs/integration.md`, per-language `clients/`, `SECURITY.md`,
  `CONTRIBUTING.md`.

[0.1.0]: https://github.com/coil398/squeuelite-rs/releases/tag/v0.1.0
