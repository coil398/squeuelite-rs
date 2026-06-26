//! HTTP JSON-RPC 2.0 gateway (§ http transport).
//!
//! This module is only compiled when the `http` feature is enabled.
//!
//! ## Design
//!
//! [`HttpGateway`] exposes the SqueueLite write gateway over HTTP using axum 0.8.
//! The single endpoint `POST /rpc` accepts a JSON-RPC 2.0 request body and
//! returns a JSON-RPC 2.0 response. A `GET /health` endpoint returns
//! `{"status":"ok"}` for liveness probes.
//!
//! Dispatch is handled by [`crate::jsonrpc::dispatch`], the same
//! transport-agnostic function used by the UDS sidecar. This ensures both
//! transports produce identical responses for identical JSON-RPC requests.
//!
//! ## Security (§23)
//!
//! HTTP is TCP-exposed. The default bind address is `127.0.0.1` (localhost);
//! **do not** change this to `0.0.0.0` unless you are behind a reverse proxy
//! that handles TLS and authentication.
//!
//! ### Optional Bearer-Token Authentication
//!
//! Set [`HttpConfig::auth_token`] to `Some("my-secret-token".into())` to require
//! `Authorization: Bearer <token>` on every `POST /rpc` request. When a token is
//! configured, requests without the header or with a wrong token receive
//! `401 Unauthorized`. `GET /health` is always allowed (liveness probes must
//! not require credentials).
//!
//! The token is best supplied via the `SQUEUELITE_HTTP_TOKEN` environment variable
//! rather than `--http-token` CLI flag, because CLI arguments are visible to
//! other processes via `ps`.
//!
//! For production deployments exposed beyond localhost, pair bearer-token auth
//! with TLS termination at a reverse proxy.
//!
//! ## Single-process, single-writer
//!
//! The HTTP server and the UDS sidecar may run simultaneously in **one** process
//! sharing **one** `InProcessGateway` (and therefore one SQLite writer thread).
//! See `src/bin/squeuelite-gateway.rs` for the combined binary.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::Value;
use tokio::net::TcpListener;

use crate::{inprocess::GatewayHandle, stats::Stats};

// ---------------------------------------------------------------------------
// HttpConfig
// ---------------------------------------------------------------------------

/// Configuration for an [`HttpGateway`].
///
/// The only required setting is `addr`, which controls the TCP bind address.
/// Default: `127.0.0.1:8080` (localhost only).
///
/// ## Security note
///
/// Keep the default `127.0.0.1` for local-only deployments. Changing to
/// `0.0.0.0` exposes the endpoint on all interfaces without authentication.
/// Set [`HttpConfig::auth_token`] to require a bearer token on `POST /rpc`,
/// and put the service behind a TLS-terminating proxy before exposing it
/// externally.
///
/// ## Optional Bearer-Token Authentication
///
/// ```no_run
/// use squeuelite::HttpConfig;
/// let config = HttpConfig {
///     addr: "127.0.0.1:8080".parse().unwrap(),
///     auth_token: Some("my-secret-token".into()),
/// };
/// ```
///
/// When `auth_token` is `Some`, every `POST /rpc` must include:
/// ```text
/// Authorization: Bearer my-secret-token
/// ```
/// Missing or incorrect tokens receive `401 Unauthorized`. `GET /health` is
/// always allowed (no token required) so liveness probes continue to work.
///
/// Supply the token via the `SQUEUELITE_HTTP_TOKEN` environment variable rather
/// than a CLI flag to avoid leaking it in `ps` output.
#[derive(Clone)]
pub struct HttpConfig {
    /// TCP address to bind (default: `127.0.0.1:8080`).
    pub addr: SocketAddr,
    /// Optional bearer token required on `POST /rpc`.
    ///
    /// `None` (default) disables authentication — all requests are accepted.
    /// `Some(token)` enforces `Authorization: Bearer <token>` on `/rpc`.
    /// `GET /health` is always allowed regardless of this setting.
    pub auth_token: Option<String>,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the token value (avoid leaking it via Debug output / logs).
        f.debug_struct("HttpConfig")
            .field("addr", &self.addr)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".parse().expect("valid default addr"),
            auth_token: None,
        }
    }
}

impl HttpConfig {
    /// Create a config binding to the given address with no authentication.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            auth_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared axum state
// ---------------------------------------------------------------------------

/// Shared state injected into every axum request handler.
#[derive(Clone)]
struct AppState {
    handle: GatewayHandle,
    stats: Arc<Stats>,
    db_path: PathBuf,
    /// Optional bearer token for `POST /rpc` authentication.
    /// `None` means no authentication is required.
    auth_token: Option<String>,
}

// ---------------------------------------------------------------------------
// axum handlers
// ---------------------------------------------------------------------------

/// `POST /rpc` — JSON-RPC 2.0 handler.
///
/// The request body must be a JSON-RPC 2.0 object (not a batch array).
/// The response is always `200 OK` with a JSON-RPC 2.0 result or error body,
/// unless bearer-token authentication is configured and the request fails it
/// (in which case `401 Unauthorized` is returned before dispatch).
///
/// Note: HTTP 200 is returned even for JSON-RPC application errors (e.g. write
/// failed, method not found). This follows the JSON-RPC 2.0 specification,
/// which uses the JSON envelope for error signalling rather than HTTP status
/// codes.
async fn rpc_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Bearer-token authentication (§23, optional).
    //
    // When `auth_token` is configured, the `Authorization` header must be
    // present and match `Bearer <token>`. Missing or wrong tokens return 401.
    // `GET /health` bypasses this handler entirely, so no token is needed there.
    if let Some(ref expected) = state.auth_token {
        let authorized = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .map(|token| token == expected.as_str())
            .unwrap_or(false);

        if !authorized {
            let body = Json(serde_json::json!({"error": "unauthorized"}));
            return (StatusCode::UNAUTHORIZED, body).into_response();
        }
    }

    // Parse raw bytes to string (accept any valid UTF-8).
    let line = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => {
            let err = serde_json::json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32700,
                    "message": "request body is not valid UTF-8"
                }
            });
            return (StatusCode::OK, Json(err)).into_response();
        }
    };

    let reply_str =
        crate::jsonrpc::dispatch(line, &state.handle, &state.stats, state.db_path.as_path()).await;

    // The dispatch function returns a JSON string; parse it back to Value so
    // axum can re-serialise it with the correct Content-Type header.
    let reply_value: Value = serde_json::from_str(&reply_str).unwrap_or_else(|_| {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": { "code": -32603, "message": "internal error" }
        })
    });

    (StatusCode::OK, Json(reply_value)).into_response()
}

/// `GET /health` — liveness probe.
///
/// Returns `{"status":"ok"}` unconditionally. No JSON-RPC envelope — this is
/// a plain HTTP health endpoint for load balancers and process monitors.
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

// ---------------------------------------------------------------------------
// HttpGateway
// ---------------------------------------------------------------------------

/// An HTTP server that exposes the SqueueLite gateway over JSON-RPC 2.0.
///
/// Use [`HttpGateway::serve`] to bind and run. Pass a shutdown future (e.g.
/// from `tokio::signal::ctrl_c()`) to enable graceful shutdown.
///
/// ## Endpoints
///
/// | Method | Path | Description |
/// |--------|------|-------------|
/// | `POST` | `/rpc` | JSON-RPC 2.0 dispatch (`execute`, `stats`, `health`, `checkpoint`) |
/// | `GET` | `/health` | Liveness probe — always `{"status":"ok"}` |
pub struct HttpGateway {
    handle: GatewayHandle,
    stats: Arc<Stats>,
    db_path: PathBuf,
}

impl HttpGateway {
    /// Create an [`HttpGateway`] from a [`GatewayHandle`] and a `db_path`.
    ///
    /// The `db_path` is used only by the `"stats"` JSON-RPC method to read
    /// the WAL file size. Pass an empty path for `:memory:` databases.
    pub fn new(handle: GatewayHandle, db_path: impl Into<PathBuf>) -> Self {
        Self {
            handle,
            stats: Stats::new_arc(),
            db_path: db_path.into(),
        }
    }

    /// Create an [`HttpGateway`] with a shared [`Stats`] counter (e.g. when
    /// sharing stats with a concurrent UDS sidecar in the same process).
    pub fn with_stats(
        handle: GatewayHandle,
        stats: Arc<Stats>,
        db_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            handle,
            stats,
            db_path: db_path.into(),
        }
    }

    /// Bind to `addr` and serve until `shutdown` resolves.
    ///
    /// Uses `axum::serve(...).with_graceful_shutdown(shutdown)` so in-flight
    /// requests complete before the server stops.
    pub async fn serve(
        self,
        config: HttpConfig,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> crate::error::Result<()> {
        let state = AppState {
            handle: self.handle,
            stats: self.stats,
            db_path: self.db_path,
            auth_token: config.auth_token,
        };

        // Explicitly cap the request body at MAX_LINE_BYTES (shared with the UDS
        // transport) so that both transports enforce the same 1 MiB ceiling.
        // Without this layer axum 0.8 would apply its own 2 MiB default, which
        // is both larger than the UDS limit and an implicit dependency on an
        // axum version default that could change silently.
        let app = Router::new()
            .route("/rpc", post(rpc_handler))
            .route("/health", get(health_handler))
            .layer(DefaultBodyLimit::max(crate::jsonrpc::MAX_LINE_BYTES))
            .with_state(state);

        let listener = TcpListener::bind(config.addr)
            .await
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        Ok(())
    }
}
