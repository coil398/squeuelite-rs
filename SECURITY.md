# Security Policy

## Supported versions

SqueueLite is pre-1.0. Only the latest `main` (and the latest published release)
receives security fixes.

## Reporting a vulnerability

Please report security issues **privately** via GitHub's
[private vulnerability reporting](https://github.com/coil398/squeuelite-rs/security/advisories/new)
(repository → **Security** → **Report a vulnerability**).

Do **not** open a public issue for security problems.

## Security model

SqueueLite assumes **trusted local callers** — this is the MVP threat model
(design §23). It is a write gateway, **not** a SQL sandbox.

| Surface | Posture |
|---|---|
| **Unix Domain Socket** | Local only. Access is controlled by socket-file permissions — `0o600` (owner-only) by default; relax with `--socket-mode` (e.g. `660`) + a shared Unix group. |
| **HTTP** | TCP. **Binds `127.0.0.1` by default** (not externally reachable). Optional bearer-token auth via the `SQUEUELITE_HTTP_TOKEN` environment variable (`POST /rpc` then requires `Authorization: Bearer <token>`; `GET /health` stays open). There is **no built-in TLS** — if you bind to a non-loopback address, enable the token **and** front it with TLS via a reverse proxy. |
| **SQL** | `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`/`PRAGMA` are always rejected. `CREATE`/`ALTER`/`DROP`/`TRUNCATE`, `DELETE`, `DROP`, and raw SQL are gated by `GatewayConfig` flags (`allow_schema_write` / `allow_delete` / `allow_drop` / `allow_raw_sql`). All parameters are bound — no string interpolation. Callers are still trusted; arbitrary accepted SQL runs as-is. |
| **Scope** | Single host only. No clustering / multi-host. |

If your deployment exposes the HTTP endpoint beyond localhost or runs untrusted
callers, that is **outside** the current threat model — please open a discussion
before relying on it.
