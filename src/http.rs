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
//! that handles TLS and authentication. External access and authentication are
//! the caller's responsibility. A bearer-token layer can be added via:
//!
//! ```text
//! tower_http::validate_request::ValidateRequestHeaderLayer::bearer("my-token")
//! ```
//!
//! applied to the router with `.layer(...)`.
//!
//! ## Single-process, single-writer
//!
//! The HTTP server and the UDS sidecar may run simultaneously in **one** process
//! sharing **one** `InProcessGateway` (and therefore one SQLite writer thread).
//! See `src/bin/squeuelite-gateway.rs` for the combined binary.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    Json,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde_json::Value;
use tokio::net::TcpListener;

use crate::{
    inprocess::GatewayHandle,
    stats::Stats,
};

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
/// Add a bearer-token tower layer or put this behind a TLS-terminating proxy
/// before exposing it externally.
#[derive(Debug, Clone)]
pub struct HttpConfig {
    /// TCP address to bind (default: `127.0.0.1:8080`).
    pub addr: SocketAddr,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".parse().expect("valid default addr"),
        }
    }
}

impl HttpConfig {
    /// Create a config binding to the given address.
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
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
}

// ---------------------------------------------------------------------------
// axum handlers
// ---------------------------------------------------------------------------

/// `POST /rpc` — JSON-RPC 2.0 handler.
///
/// The request body must be a JSON-RPC 2.0 object (not a batch array).
/// The response is always `200 OK` with a JSON-RPC 2.0 result or error body.
///
/// Note: HTTP 200 is returned even for JSON-RPC application errors (e.g. write
/// failed, method not found). This follows the JSON-RPC 2.0 specification,
/// which uses the JSON envelope for error signalling rather than HTTP status
/// codes.
async fn rpc_handler(
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
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

    let reply_str = crate::jsonrpc::dispatch(
        line,
        &state.handle,
        &state.stats,
        state.db_path.as_path(),
    )
    .await;

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
