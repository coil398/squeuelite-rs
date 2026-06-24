//! SqueueLite sidecar gateway binary (§7.1).
//!
//! ## Usage
//!
//! ```bash
//! squeuelite-gateway --db ./app.db --socket ./squeuelite.sock
//! ```
//!
//! Both `--db` and `--socket` are required. Ctrl-C triggers graceful shutdown
//! (WAL checkpoint §16 + socket unlink §20.1).

use squeuelite::{SidecarConfig, SidecarGateway};

#[tokio::main]
async fn main() {
    let (db_path, socket_path) = parse_args();

    eprintln!("[squeuelite-gateway] db={db_path} socket={socket_path}");
    eprintln!("[squeuelite-gateway] listening on {socket_path}");

    let config = SidecarConfig::new(&db_path, &socket_path);

    let gateway = match SidecarGateway::open(config) {
        Ok(gw) => gw,
        Err(e) => {
            eprintln!("[squeuelite-gateway] failed to open gateway: {e}");
            std::process::exit(1);
        }
    };

    // Shutdown on Ctrl-C (§7.1 graceful shutdown).
    let shutdown = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("[squeuelite-gateway] signal error: {e}");
        }
        eprintln!("[squeuelite-gateway] shutting down…");
    };

    if let Err(e) = gateway.run(shutdown).await {
        eprintln!("[squeuelite-gateway] error during run: {e}");
        std::process::exit(1);
    }

    eprintln!("[squeuelite-gateway] shutdown complete");
}

/// Parse `--db <path> --socket <path>` from `std::env::args`.
///
/// Exits with code 2 and prints usage to stderr if either argument is missing.
fn parse_args() -> (String, String) {
    let args: Vec<String> = std::env::args().collect();
    let mut db = None;
    let mut socket = None;

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
            _ => {}
        }
        i += 1;
    }

    match (db, socket) {
        (Some(d), Some(s)) => (d, s),
        _ => {
            eprintln!("Usage: squeuelite-gateway --db <path> --socket <path>");
            std::process::exit(2);
        }
    }
}
