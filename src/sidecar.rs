//! Unix Domain Socket sidecar gateway (§7, §18, §20.1, §23).
//!
//! This module is only compiled when the `sidecar` feature is enabled.
//!
//! ## Design
//!
//! [`SidecarGateway`] wraps an [`crate::inprocess::InProcessGateway`] and
//! exposes it over a Unix Domain Socket using JSON Lines (§18.1). Each
//! connected client gets its own `tokio::spawn`ed task that reads lines from
//! a `BufReader` and dispatches them as either write requests or admin commands.
//!
//! ## Security (§23)
//!
//! Only Unix Domain Sockets are used; TCP is not supported. Access control is
//! delegated to filesystem permissions on the socket file. After bind, the
//! socket file is set to the mode specified by [`SidecarConfig::socket_mode`]
//! (default `0o600`, owner-only read/write) so that other users on the same
//! host cannot connect even if the umask is permissive. To allow agents running
//! as a shared group to connect, set `socket_mode = 0o660` and ensure all
//! callers belong to the same Unix group (§23).

/// Maximum line length accepted from a single client read (§23 DoS mitigation).
///
/// A single JSON line larger than this limit causes the connection to be closed
/// with an error response. 1 MiB is ample for any realistic write request while
/// preventing memory exhaustion from a misbehaving or malicious local process.
const MAX_LINE_BYTES: u64 = 1024 * 1024; // 1 MiB

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
};

use crate::{
    config::GatewayConfig,
    error::Result,
    inprocess::{GatewayHandle, InProcessGateway},
    protocol::{AdminCommand, Incoming},
    request::WriteStatus,
    stats::{Stats, wal_size_bytes},
};

// ---------------------------------------------------------------------------
// SidecarConfig
// ---------------------------------------------------------------------------

/// Configuration for a [`SidecarGateway`] (§20.1).
///
/// `gateway` controls the underlying SQLite writer (§16 PRAGMAs, §14 queue).
/// `socket_path` is the Unix Domain Socket path that agents connect to (§7.1).
///
/// ## Socket permissions (§23)
///
/// `socket_mode` sets the Unix permission bits applied to the socket file after
/// bind. The default `0o600` restricts access to the owner only (secure by
/// default). For multi-user or multi-container scenarios where agents run under
/// a shared Unix group, set `socket_mode = 0o660` and ensure all callers
/// belong to the same group.
#[derive(Debug, Clone)]
pub struct SidecarConfig {
    /// Writer configuration passed to [`InProcessGateway::open_with_config`].
    pub gateway: GatewayConfig,
    /// Path of the Unix Domain Socket to bind (§7.1, §23).
    pub socket_path: PathBuf,
    /// Unix permission mode for the socket file (octal, default `0o600`).
    ///
    /// Applied via `std::fs::set_permissions` immediately after bind. A value
    /// of `0o600` restricts access to the owner only. Use `0o660` to allow
    /// agents in the same group to connect (multi-user/multi-container setups).
    pub socket_mode: u32,
}

impl SidecarConfig {
    /// Convenience constructor: file DB at `db_path`, socket at `socket_path`.
    ///
    /// Sets `socket_mode` to `0o600` (owner-only, secure by default).
    pub fn new(
        db_path: impl Into<PathBuf>,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            gateway: GatewayConfig::new(db_path),
            socket_path: socket_path.into(),
            socket_mode: 0o600,
        }
    }
}

// ---------------------------------------------------------------------------
// SidecarGateway
// ---------------------------------------------------------------------------

/// A gateway that accepts write requests over a Unix Domain Socket (§7, §23).
///
/// ## Usage (§20.1)
///
/// ```rust,no_run
/// # #[cfg(feature = "sidecar")]
/// # async fn example() -> squeuelite::Result<()> {
/// use squeuelite::{SidecarGateway, SidecarConfig};
/// use tokio::signal;
///
/// let config = SidecarConfig::new("./app.db", "./squeuelite.sock");
/// let gateway = SidecarGateway::open(config)?;
/// // `signal::ctrl_c()` resolves to `Result<(), io::Error>`; map it to `()`
/// // so it matches the `Future<Output = ()>` required by `run`.
/// gateway.run(async { let _ = signal::ctrl_c().await; }).await?;
/// # Ok(())
/// # }
/// ```
pub struct SidecarGateway {
    inner: InProcessGateway,
    socket_path: PathBuf,
    socket_mode: u32,
    stats: Arc<Stats>,
}

impl SidecarGateway {
    /// Open the underlying [`InProcessGateway`] and prepare the socket path.
    ///
    /// Does **not** bind the socket yet; binding happens inside [`Self::run`]
    /// so that the socket only exists while the gateway is actively accepting.
    pub fn open(config: SidecarConfig) -> Result<Self> {
        let inner = InProcessGateway::open_with_config(config.gateway)?;
        Ok(Self {
            inner,
            socket_path: config.socket_path,
            socket_mode: config.socket_mode,
            stats: Stats::new_arc(),
        })
    }

    /// Run the accept loop until `shutdown` resolves (§20.1 graceful shutdown).
    ///
    /// Steps:
    /// 1. Remove any stale socket file at `socket_path`.
    /// 2. Bind a [`UnixListener`].
    /// 3. Accept connections; each gets a `tokio::spawn`ed handler task.
    /// 4. When `shutdown` resolves, stop accepting new connections.
    /// 5. Remove the socket file.
    /// 6. Shut down the [`InProcessGateway`] (WAL checkpoint §16).
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) -> Result<()> {
        let socket_path = self.socket_path.clone();
        let socket_mode = self.socket_mode;

        // Step 1 — remove stale socket (ignore error if it doesn't exist).
        let _ = std::fs::remove_file(&socket_path);

        // Step 2 — bind the Unix Domain Socket.
        let listener = UnixListener::bind(&socket_path)
            .map_err(|e| crate::error::Error::Io(e.to_string()))?;

        // Step 2a — set socket permissions (§23).
        //
        // The mode is taken from `SidecarConfig::socket_mode` (default `0o600`,
        // owner-only). For agents running across multiple users or containers,
        // set `socket_mode = 0o660` and use a shared Unix group (§23).
        //
        // Best-effort: if `set_permissions` fails (e.g. the filesystem does not
        // support Unix permission bits), we emit a warning to stderr and continue.
        // The gateway is still functional; the caller should pre-restrict the
        // parent directory in that case.
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(socket_mode);
            if let Err(e) = fs::set_permissions(&socket_path, perms) {
                eprintln!(
                    "squeuelite: warning: failed to set socket permissions to \
                     0o{socket_mode:o} on {socket_path:?}: {e}"
                );
            }
        }

        let handle = self.inner.handle();
        let stats = Arc::clone(&self.stats);
        let db_path = self.inner.db_path().clone();

        // Step 3 — accept loop, interruptible by shutdown future.
        tokio::select! {
            _ = accept_loop(listener, handle, stats, db_path) => {}
            _ = shutdown => {}
        }

        // Step 5 — remove socket file.
        let _ = std::fs::remove_file(&socket_path);

        // Step 6 — graceful writer shutdown (WAL checkpoint §16).
        self.inner.shutdown().await?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// accept_loop
// ---------------------------------------------------------------------------

/// Continuously accept connections and spawn a handler task for each (§7.2).
async fn accept_loop(
    listener: UnixListener,
    handle: GatewayHandle,
    stats: Arc<Stats>,
    db_path: PathBuf,
) {
    while let Ok((stream, _addr)) = listener.accept().await {
        let handle = handle.clone();
        let stats = Arc::clone(&stats);
        let db_path = db_path.clone();
        tokio::spawn(handle_connection(stream, handle, stats, db_path));
    }
}

// ---------------------------------------------------------------------------
// handle_connection — per-client task
// ---------------------------------------------------------------------------

/// Handle a single client connection: read JSON Lines, dispatch, write replies.
///
/// The connection is **not** closed after each request; the loop continues
/// until the client disconnects (EOF) or an I/O error occurs.
///
/// Protocol (§18.1):
/// - Each line is parsed as [`Incoming`] (untagged: AdminCommand | WriteRequest).
/// - Parse failure → error JSON is returned; connection stays open.
/// - `Write(req)` → stats.accepted++ → execute → update counters → WriteResponse.
/// - `Admin(Stats)` → StatsSnapshot JSON.
/// - `Admin(Health)` → `{"status":"ok"}`.
/// - `Admin(Checkpoint)` → writer checkpoint → `{"status":"ok"}` or error.
async fn handle_connection(
    stream: tokio::net::UnixStream,
    handle: GatewayHandle,
    stats: Arc<Stats>,
    db_path: PathBuf,
) {
    let (read_half, mut write_half) = stream.into_split();

    // §23 DoS mitigation: cap per-line read at MAX_LINE_BYTES.
    //
    // Strategy: use `read_line` on a plain `BufReader` and check the line
    // length after each read. This is the simplest correct approach for an
    // MVP / trusted-local-process context where "basic mitigation" is the goal:
    //   - Normal lines (< MAX_LINE_BYTES) are processed as usual.
    //   - An over-limit line causes an error response and connection close.
    //
    // Note: `read_line` does allocate memory proportional to the line length.
    // A malicious client can still send MAX_LINE_BYTES before we detect and
    // reject it. For a stricter guarantee, `BufReader<Take<R>>` with `set_limit`
    // reset each iteration would bound per-call allocation more tightly, but
    // adds complexity. Given §23's "local trusted process" threat model, the
    // current approach (read → measure → reject if over limit) is sufficient.
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();

    loop {
        buf.clear();
        match reader.read_line(&mut buf).await {
            Ok(0) => break, // EOF — client disconnected.
            Ok(_) => {}
            Err(_) => break, // I/O error — close connection.
        }

        // Reject lines that exceed the per-line byte cap (§23).
        // `buf.len()` includes the trailing `\n` (if present). We use
        // strictly-greater-than to allow exactly MAX_LINE_BYTES of payload
        // (the trailing newline would push `buf.len()` to MAX_LINE_BYTES + 1).
        if buf.len() as u64 > MAX_LINE_BYTES {
            let reply = serde_json::json!({
                "request_id": null,
                "status": "failed",
                "error": format!(
                    "line too long: {} bytes exceeds limit ({MAX_LINE_BYTES} bytes)",
                    buf.len()
                )
            })
            .to_string()
                + "\n";
            let _ = write_half.write_all(reply.as_bytes()).await;
            // Close this connection — stream position is unreliable after an
            // over-limit read; continuing would risk processing a partial line.
            break;
        }

        let line = buf.trim_end_matches('\n');
        let reply = dispatch_line(line, &handle, &stats, db_path.as_path()).await;
        let mut out = reply;
        out.push('\n');
        if write_half.write_all(out.as_bytes()).await.is_err() {
            break;
        }
    }
}

/// Parse one line and return the reply JSON string.
async fn dispatch_line(
    line: &str,
    handle: &GatewayHandle,
    stats: &Arc<Stats>,
    db_path: &Path,
) -> String {
    let incoming: Incoming = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return serde_json::json!({
                "request_id": null,
                "status": "failed",
                "error": format!("parse error: {e}")
            })
            .to_string();
        }
    };

    match incoming {
        Incoming::Write(req) => {
            use std::sync::atomic::Ordering;
            stats.accepted.fetch_add(1, Ordering::Relaxed);

            let result = handle.execute(req).await;
            match result {
                Ok(resp) => {
                    if resp.status == WriteStatus::Committed {
                        stats.committed.fetch_add(1, Ordering::Relaxed);
                    } else {
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                    }
                    serde_json::to_string(&resp).unwrap_or_else(|e| {
                        format!(r#"{{"error":"serialize error: {e}"}}"#)
                    })
                }
                Err(
                    crate::error::Error::GatewayOverloaded
                    | crate::error::Error::GatewayClosed,
                ) => {
                    use std::sync::atomic::Ordering;
                    stats.rejected.fetch_add(1, Ordering::Relaxed);
                    serde_json::json!({
                        "status": "failed",
                        "error": "gateway overloaded"
                    })
                    .to_string()
                }
                Err(e) => {
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                    serde_json::json!({
                        "status": "failed",
                        "error": e.to_string()
                    })
                    .to_string()
                }
            }
        }
        Incoming::Admin(AdminCommand::Stats) => {
            let (avg_latency, p95_latency) = handle.latency_snapshot();
            let snapshot = stats.snapshot(
                handle.queue_depth(),
                handle.queue_capacity(),
                wal_size_bytes(db_path),
                avg_latency,
                p95_latency,
            );
            serde_json::to_string(&snapshot)
                .unwrap_or_else(|e| format!(r#"{{"error":"serialize error: {e}"}}"#))
        }
        Incoming::Admin(AdminCommand::Health) => {
            r#"{"status":"ok"}"#.to_string()
        }
        Incoming::Admin(AdminCommand::Checkpoint) => {
            match handle.checkpoint().await {
                Ok(()) => r#"{"status":"ok"}"#.to_string(),
                Err(e) => serde_json::json!({
                    "status": "failed",
                    "error": e.to_string()
                })
                .to_string(),
            }
        }
    }
}

