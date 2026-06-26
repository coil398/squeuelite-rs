//! Integration tests for the HTTP JSON-RPC 2.0 gateway (§ http transport).
//!
//! Tests bind to 127.0.0.1:0 (OS-assigned port) so they can run in parallel
//! without port conflicts. Raw HTTP/1.1 is sent via `tokio::net::TcpStream`
//! to avoid adding a heavy HTTP client dependency (reqwest etc.).
//!
//! This module is only compiled when the `http` feature is enabled.

#![cfg(feature = "http")]

use std::{net::SocketAddr, time::Duration};

use squeuelite::{
    GatewayConfig, HttpConfig, HttpGateway, InProcessGateway, SqlOperation, WriteStatus,
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Start an `HttpGateway` bound to `127.0.0.1:0` (OS-assigned port).
///
/// Returns `(addr, gateway_handle, shutdown_tx)`. Send `()` to `shutdown_tx`
/// to trigger graceful shutdown.
async fn start_http_gateway() -> (
    SocketAddr,
    squeuelite::GatewayHandle,
    tokio::sync::oneshot::Sender<()>,
) {
    // OS assigns a free port via TcpListener::bind("127.0.0.1:0").
    // We bind a throwaway listener first, read the port, then close it.
    // This is a race in principle, but works reliably in test isolation.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe");
    let addr = probe.local_addr().expect("local addr");
    drop(probe); // Release the port so axum can rebind it.

    // Build InProcessGateway (in-memory DB for tests).
    let mut config = GatewayConfig::new(":memory:");
    config.allow_schema_write = true; // allow CREATE TABLE in tests
    let gw = InProcessGateway::open_with_config(config).expect("open gateway");
    let handle = gw.handle();

    // Build HttpGateway.
    let http_gw = HttpGateway::new(handle.clone(), ":memory:");
    let http_config = HttpConfig::new(addr);

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        let shutdown = async move {
            let _ = rx.await;
        };
        if let Err(e) = http_gw.serve(http_config, shutdown).await {
            eprintln!("[test] http gateway error: {e}");
        }
        // Shut down the underlying gateway on exit.
        gw.shutdown().await.ok();
    });

    // Wait briefly for the server to start accepting connections.
    tokio::time::sleep(Duration::from_millis(30)).await;

    (addr, handle, tx)
}

/// Send a raw HTTP/1.1 POST to `addr` at `/rpc` with `body` and return the
/// response body as a `serde_json::Value`.
///
/// This avoids adding reqwest or another HTTP client as a dev-dependency.
async fn post_rpc(addr: SocketAddr, body: serde_json::Value) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let body_str = serde_json::to_string(&body).expect("serialize body");
    let request = format!(
        "POST /rpc HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_str}",
        body_str.len()
    );

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");

    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read response");

    // Extract the response body (after the blank line separating headers).
    let body_start = response
        .find("\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(response.len());
    let body_slice = &response[body_start..];

    serde_json::from_str(body_slice).unwrap_or_else(|_| {
        serde_json::json!({"error": {"message": format!("failed to parse response: {body_slice}")}})
    })
}

/// Send a raw HTTP/1.1 GET to `addr` at `/health` and return the response body.
async fn get_health(addr: SocketAddr) -> serde_json::Value {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let request = format!("GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");

    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect health");

    stream
        .write_all(request.as_bytes())
        .await
        .expect("write health request");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read health response");

    let body_start = response
        .find("\r\n\r\n")
        .map(|i| i + 4)
        .unwrap_or(response.len());
    let body_slice = &response[body_start..];

    serde_json::from_str(body_slice).unwrap_or_else(|_| serde_json::json!({"raw": body_slice}))
}

// ---------------------------------------------------------------------------
// HTTP-1: execute committed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_execute_committed() {
    let (addr, handle, shutdown_tx) = start_http_gateway().await;

    // First, CREATE the table via the in-process handle directly (setup).
    let create_resp = handle
        .execute(squeuelite::WriteRequest {
            request_id: "setup-create".into(),
            actor_id: "test".into(),
            run_id: None,
            idempotency_key: None,
            operations: vec![SqlOperation {
                sql: "CREATE TABLE IF NOT EXISTS http_events (id INTEGER PRIMARY KEY, val TEXT)"
                    .into(),
                params: vec![],
            }],
        })
        .await
        .expect("create via handle");
    assert_eq!(create_resp.status, WriteStatus::Committed);

    // Now INSERT via HTTP JSON-RPC.
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "test-insert-1",
        "method": "execute",
        "params": {
            "actor_id": "agent-http",
            "operations": [
                {
                    "sql": "INSERT INTO http_events(val) VALUES (?)",
                    "params": ["hello-http"]
                }
            ]
        }
    });

    let resp = post_rpc(addr, req).await;
    assert_eq!(resp["jsonrpc"], "2.0", "must be JSON-RPC 2.0");
    assert_eq!(resp["id"], "test-insert-1", "id must be echoed");
    assert_eq!(
        resp["result"]["status"], "committed",
        "insert must commit; got: {resp}"
    );
    assert!(
        !resp["result"]["commit_seq"].is_null(),
        "commit_seq must be present"
    );

    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------------------
// HTTP-2: health endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_health() {
    let (addr, _handle, shutdown_tx) = start_http_gateway().await;

    let resp = get_health(addr).await;
    assert_eq!(
        resp["status"], "ok",
        "health endpoint must return {{\"status\":\"ok\"}}; got: {resp}"
    );

    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------------------
// HTTP-3: invalid params (missing actor_id) → -32602
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_invalid_params() {
    let (addr, _handle, shutdown_tx) = start_http_gateway().await;

    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 99,
        "method": "execute",
        "params": {
            // actor_id intentionally missing
            "operations": [
                { "sql": "SELECT 1", "params": [] }
            ]
        }
    });

    let resp = post_rpc(addr, req).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 99, "numeric id must be echoed");
    assert_eq!(
        resp["error"]["code"], -32602,
        "missing actor_id must return -32602; got: {resp}"
    );

    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ---------------------------------------------------------------------------
// HTTP-4: idempotency via HTTP JSON-RPC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_http_idempotency_dedup() {
    let (addr, handle, shutdown_tx) = start_http_gateway().await;

    // Setup: CREATE table via handle (HTTP gateway uses in-memory DB).
    let _ = handle
        .execute(squeuelite::WriteRequest {
            request_id: "http-idem-setup".into(),
            actor_id: "test".into(),
            run_id: None,
            idempotency_key: None,
            operations: vec![SqlOperation {
                sql: "CREATE TABLE IF NOT EXISTS http_idem_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)".into(),
                params: vec![],
            }],
        })
        .await
        .expect("create table via handle");

    // First execute with idempotency_key.
    let req1 = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "http-idem-1",
        "method": "execute",
        "params": {
            "actor_id": "agent-http-idem",
            "idempotency_key": "http-idem-key",
            "operations": [
                {
                    "sql": "INSERT INTO http_idem_tbl(val) VALUES (?)",
                    "params": ["idempotent-value"]
                }
            ]
        }
    });
    let resp1 = post_rpc(addr, req1).await;
    assert_eq!(
        resp1["result"]["status"], "committed",
        "first HTTP execute must commit; got: {resp1}"
    );
    let commit_seq_1 = resp1["result"]["commit_seq"].clone();
    assert!(!commit_seq_1.is_null(), "commit_seq must be present");

    // Second execute: same idempotency_key + same ops → dedup, same commit_seq.
    let req2 = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "http-idem-2",
        "method": "execute",
        "params": {
            "actor_id": "agent-http-idem",
            "idempotency_key": "http-idem-key",
            "operations": [
                {
                    "sql": "INSERT INTO http_idem_tbl(val) VALUES (?)",
                    "params": ["idempotent-value"]
                }
            ]
        }
    });
    let resp2 = post_rpc(addr, req2).await;
    assert_eq!(
        resp2["result"]["status"], "committed",
        "second HTTP execute (dedup) must also report committed; got: {resp2}"
    );
    assert_eq!(
        resp2["result"]["commit_seq"], commit_seq_1,
        "second HTTP execute must return the same commit_seq"
    );

    let _ = shutdown_tx.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;
}
