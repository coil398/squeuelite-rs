//! SqueueLite sidecar gateway binary (§7.1).
//!
//! ## Usage
//!
//! ```bash
//! squeuelite-gateway --db ./app.db --socket ./squeuelite.sock [--socket-mode <octal>]
//! ```
//!
//! Both `--db` and `--socket` are required. `--socket-mode` is optional and
//! defaults to `600` (owner-only). Pass `660` to allow a shared Unix group.
//! Ctrl-C triggers graceful shutdown (WAL checkpoint §16 + socket unlink §20.1).

use squeuelite::{SidecarConfig, SidecarGateway};

#[tokio::main]
async fn main() {
    let (db_path, socket_path, socket_mode) = parse_args();

    eprintln!(
        "[squeuelite-gateway] db={db_path} socket={socket_path} socket_mode=0o{socket_mode:o}"
    );
    eprintln!("[squeuelite-gateway] listening on {socket_path}");

    let mut config = SidecarConfig::new(&db_path, &socket_path);
    config.socket_mode = socket_mode;

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

/// Parse `--db <path> --socket <path> [--socket-mode <octal>]` from
/// `std::env::args`.
///
/// `--socket-mode` is optional and defaults to `600` (0o600, owner-only).
/// The value is parsed as an octal integer (e.g. `660` → `0o660`).
///
/// Exits with code 2 and prints usage to stderr if a required argument is
/// missing or if `--socket-mode` contains an invalid octal value.
fn parse_args() -> (String, String, u32) {
    let args: Vec<String> = std::env::args().collect();
    let mut db = None;
    let mut socket = None;
    let mut socket_mode: Option<u32> = None;

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
                            eprintln!(
                                "Usage: squeuelite-gateway --db <path> --socket <path> \
                                 [--socket-mode <octal>]"
                            );
                            std::process::exit(2);
                        }
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }

    match (db, socket) {
        (Some(d), Some(s)) => (d, s, socket_mode.unwrap_or(0o600)),
        _ => {
            eprintln!(
                "Usage: squeuelite-gateway --db <path> --socket <path> \
                 [--socket-mode <octal>]"
            );
            std::process::exit(2);
        }
    }
}
