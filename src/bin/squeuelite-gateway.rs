//! SqueueLite sidecar gateway binary (§7.1).
//!
//! ## Usage
//!
//! ```text
//! squeuelite-gateway --db <path> [--socket <path>] [--http <addr>] [--socket-mode <octal>]
//! ```
//!
//! `--db` is required. At least one of `--socket` or `--http` must be given.
//!
//! - `--socket <path>` — Unix Domain Socket path (JSON-RPC 2.0 over JSON Lines).
//! - `--http <addr>` — HTTP bind address (JSON-RPC 2.0 over HTTP POST /rpc).
//!   Example: `127.0.0.1:8080`. Default bind is localhost; do not use 0.0.0.0
//!   without a reverse proxy that handles TLS and authentication.
//! - `--socket-mode <octal>` — optional UDS permission bits (default `600`).
//!   Pass `660` to allow a shared Unix group.
//!
//! Both `--socket` and `--http` may be specified simultaneously. One
//! `InProcessGateway` (single writer thread) is shared between both transports.
//! Ctrl-C triggers graceful shutdown of both servers and the WAL checkpoint (§16).
//!
//! ## Security
//!
//! UDS: access control via filesystem permissions (`socket_mode`).
//! HTTP: TCP-exposed; default localhost. External access and authentication are
//! the caller's responsibility. A bearer-token layer can be added via tower.

use squeuelite::{GatewayConfig, SidecarConfig, SidecarGateway};

#[cfg(feature = "http")]
use std::sync::Arc;

#[cfg(feature = "http")]
use squeuelite::{InProcessGateway, stats::Stats};

#[tokio::main]
async fn main() {
    let args = parse_args();

    eprintln!(
        "[squeuelite-gateway] db={}{}{}{}",
        args.db_path,
        args.socket_path
            .as_deref()
            .map(|s| format!(" socket={s}"))
            .unwrap_or_default(),
        args.socket_mode
            .map(|m| format!(" socket_mode=0o{m:o}"))
            .unwrap_or_default(),
        args.http_addr
            .as_deref()
            .map(|a| format!(" http={a}"))
            .unwrap_or_default(),
    );

    // ----- CASE 1: sidecar only (no --http) -----
    // Delegate entirely to SidecarGateway, which owns its own InProcessGateway.
    if let (Some(socket_path), true) = (args.socket_path.as_deref(), args.http_addr.is_none()) {
        let socket_path = socket_path.to_owned();
        let socket_mode = args.socket_mode.unwrap_or(0o600);

        eprintln!("[squeuelite-gateway] UDS JSON-RPC 2.0 listening on {socket_path}");

        let sidecar_config = SidecarConfig {
            gateway: GatewayConfig::new(&args.db_path),
            socket_path: socket_path.into(),
            socket_mode,
        };

        let sidecar_gw = match SidecarGateway::open(sidecar_config) {
            Ok(gw) => gw,
            Err(e) => {
                eprintln!("[squeuelite-gateway] failed to open sidecar: {e}");
                std::process::exit(1);
            }
        };

        let shutdown = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                eprintln!("[squeuelite-gateway] signal error: {e}");
            }
            eprintln!("[squeuelite-gateway] shutting down…");
        };

        if let Err(e) = sidecar_gw.run(shutdown).await {
            eprintln!("[squeuelite-gateway] error during run: {e}");
            std::process::exit(1);
        }

        eprintln!("[squeuelite-gateway] shutdown complete");
        return;
    }

    // ----- CASE 2: HTTP (alone) or both UDS + HTTP -----
    // Open one InProcessGateway and share its handle across both transports.
    #[cfg(not(feature = "http"))]
    {
        eprintln!(
            "[squeuelite-gateway] --http was specified but this binary was built without \
             the `http` feature. Rebuild with `--features sidecar,http` to enable HTTP."
        );
        std::process::exit(2);
    }

    #[cfg(feature = "http")]
    {
        use squeuelite::{HttpConfig, HttpGateway};

        // Open the shared writer.
        let inner = match InProcessGateway::open_with_config(GatewayConfig::new(&args.db_path)) {
            Ok(gw) => gw,
            Err(e) => {
                eprintln!("[squeuelite-gateway] failed to open gateway: {e}");
                std::process::exit(1);
            }
        };
        let handle = inner.handle();
        let db_path = inner.db_path().clone();

        // Shared stats counter: both transports update the same counters so
        // `stats` / `health` responses reflect the combined activity.
        let stats = Stats::new_arc();

        // Broadcast channel for Ctrl-C → both servers.
        let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(2);

        // Ctrl-C handler.
        let tx_ctrlc = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                eprintln!("[squeuelite-gateway] signal error: {e}");
            }
            eprintln!("[squeuelite-gateway] shutting down…");
            let _ = tx_ctrlc.send(());
        });

        let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // ---- UDS (inline, shares the handle) ----
        if let Some(socket_path) = args.socket_path {
            let socket_mode = args.socket_mode.unwrap_or(0o600);
            eprintln!("[squeuelite-gateway] UDS JSON-RPC 2.0 listening on {socket_path}");

            let handle_uds = handle.clone();
            let stats_uds = Arc::clone(&stats);
            let db_path_uds = db_path.clone();
            let mut rx = shutdown_tx.subscribe();

            tasks.push(tokio::spawn(async move {
                let res = run_uds_inline(
                    socket_path,
                    socket_mode,
                    handle_uds,
                    stats_uds,
                    db_path_uds,
                    async move { let _ = rx.recv().await; },
                )
                .await;
                if let Err(e) = res {
                    eprintln!("[squeuelite-gateway] UDS error: {e}");
                }
            }));
        }

        // ---- HTTP ----
        {
            let http_addr_str = args.http_addr.expect("http_addr must be Some in CASE 2");
            let addr: std::net::SocketAddr = match http_addr_str.parse() {
                Ok(a) => a,
                Err(e) => {
                    eprintln!(
                        "[squeuelite-gateway] invalid --http address {http_addr_str:?}: {e}"
                    );
                    std::process::exit(2);
                }
            };
            eprintln!("[squeuelite-gateway] HTTP JSON-RPC 2.0 listening on {http_addr_str}");

            let gw = HttpGateway::with_stats(handle.clone(), Arc::clone(&stats), db_path.clone());
            let http_config = HttpConfig::new(addr);
            let mut rx = shutdown_tx.subscribe();

            tasks.push(tokio::spawn(async move {
                let res = gw
                    .serve(http_config, async move { let _ = rx.recv().await; })
                    .await;
                if let Err(e) = res {
                    eprintln!("[squeuelite-gateway] HTTP error: {e}");
                }
            }));
        }

        // Wait for all server tasks.
        for task in tasks {
            let _ = task.await;
        }

        // Graceful writer shutdown (WAL checkpoint §16).
        if let Err(e) = inner.shutdown().await {
            eprintln!("[squeuelite-gateway] gateway shutdown error: {e}");
        }

        eprintln!("[squeuelite-gateway] shutdown complete");
    }
}

// ---------------------------------------------------------------------------
// Inline UDS accept loop — used when UDS + HTTP share one InProcessGateway
// ---------------------------------------------------------------------------

/// Run a UDS accept loop using a pre-existing `GatewayHandle`.
///
/// Delegates to [`squeuelite::sidecar::accept_loop`] so that the per-line
/// DoS cap (`MAX_LINE_BYTES`) and JSON-Lines dispatch logic live in a single
/// place (`sidecar.rs`), shared by both this binary and [`squeuelite::SidecarGateway`].
#[cfg(feature = "http")]
async fn run_uds_inline(
    socket_path: String,
    socket_mode: u32,
    handle: squeuelite::GatewayHandle,
    stats: Arc<Stats>,
    db_path: std::path::PathBuf,
    shutdown: impl std::future::Future<Output = ()>,
) -> squeuelite::Result<()> {
    use std::fs;
    use tokio::net::UnixListener;

    let path = std::path::Path::new(&socket_path);

    // Remove stale socket.
    let _ = std::fs::remove_file(path);

    let listener = UnixListener::bind(path)
        .map_err(|e| squeuelite::Error::Io(e.to_string()))?;

    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(socket_mode);
        if let Err(e) = fs::set_permissions(path, perms) {
            eprintln!(
                "squeuelite: warning: failed to set socket permissions to \
                 0o{socket_mode:o} on {socket_path}: {e}"
            );
        }
    }

    tokio::select! {
        _ = squeuelite::sidecar::accept_loop(listener, handle, stats, db_path) => {}
        _ = shutdown => {}
    }

    let _ = std::fs::remove_file(path);
    Ok(())
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

struct Args {
    db_path: String,
    socket_path: Option<String>,
    socket_mode: Option<u32>,
    http_addr: Option<String>,
}

/// Parse `--db <path> [--socket <path>] [--http <addr>] [--socket-mode <octal>]`.
fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut db = None;
    let mut socket = None;
    let mut socket_mode = None;
    let mut http_addr = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--db" => {
                i += 1;
                if i < args.len() {
                    db = Some(args[i].clone());
                }
            }
            "--socket" => {
                i += 1;
                if i < args.len() {
                    socket = Some(args[i].clone());
                }
            }
            "--socket-mode" => {
                i += 1;
                if i < args.len() {
                    match u32::from_str_radix(&args[i], 8) {
                        Ok(mode) => socket_mode = Some(mode),
                        Err(_) => {
                            eprintln!(
                                "squeuelite-gateway: invalid --socket-mode value {:?}: \
                                 expected an octal integer (e.g. 600, 660)",
                                args[i]
                            );
                            eprintln!("{}", USAGE);
                            std::process::exit(2);
                        }
                    }
                }
            }
            "--http" => {
                i += 1;
                if i < args.len() {
                    http_addr = Some(args[i].clone());
                }
            }
            _ => {}
        }
        i += 1;
    }

    let db_path = match db {
        Some(d) => d,
        None => {
            eprintln!("squeuelite-gateway: --db is required");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };

    if socket.is_none() && http_addr.is_none() {
        eprintln!("squeuelite-gateway: at least one of --socket or --http is required");
        eprintln!("{USAGE}");
        std::process::exit(2);
    }

    Args {
        db_path,
        socket_path: socket,
        socket_mode,
        http_addr,
    }
}

const USAGE: &str = "Usage: squeuelite-gateway --db <path> [--socket <path>] [--http <addr>] \
                     [--socket-mode <octal>]\nAt least one of --socket or --http is required.";
