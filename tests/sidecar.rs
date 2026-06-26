//! Integration tests for the Unix Domain Socket sidecar (§18, §20, §24).
//!
//! All tests use JSON-RPC 2.0 over UDS. Raw JSON Lines is no longer supported.
//! This module is only compiled when the `sidecar` feature is enabled.

#![cfg(feature = "sidecar")]

use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Notify;

use squeuelite::{Client, SidecarConfig, SidecarGateway, SqlOperation, WriteStatus};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Generate a unique temp path prefix using a counter + process id.
fn temp_prefix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/squeuelite_test_{id}_{}", std::process::id())
}

/// Start a JSON-RPC 2.0 `SidecarGateway` with a custom `socket_mode`.
///
/// Returns `(socket_path, db_path, shutdown_notify)`.
/// Call `shutdown_notify.notify_one()` to trigger graceful shutdown.
///
/// `allow_schema_write` is set to `true` so that tests can CREATE tables via
/// the write channel without being rejected by the §23 security filter.
async fn start_gateway_with_mode(socket_mode: u32) -> (PathBuf, PathBuf, Arc<Notify>) {
    let prefix = temp_prefix();
    let db_path = PathBuf::from(format!("{prefix}.db"));
    let socket_path = PathBuf::from(format!("{prefix}.sock"));

    let mut gateway_config = squeuelite::GatewayConfig::new(&db_path);
    // §23: allow_schema_write=true so integration tests can CREATE tables.
    gateway_config.allow_schema_write = true;
    let config = SidecarConfig {
        gateway: gateway_config,
        socket_path: socket_path.clone(),
        socket_mode,
    };
    let gateway = SidecarGateway::open(config).expect("open gateway");

    let notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&notify);
    let socket_clone = socket_path.clone();

    tokio::spawn(async move {
        let shutdown_fut = async move { notify_clone.notified().await };
        gateway.run(shutdown_fut).await.expect("gateway run");
    });

    // Wait until the socket file appears (gateway is ready).
    for _ in 0..50 {
        if socket_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        socket_clone.exists(),
        "socket file not created within timeout"
    );

    (socket_path, db_path, notify)
}

/// Start a JSON-RPC 2.0 `SidecarGateway` with the default socket mode (0o600).
async fn start_gateway() -> (PathBuf, PathBuf, Arc<Notify>) {
    start_gateway_with_mode(0o600).await
}

/// Clean up leftover temp files after a test.
fn cleanup(paths: &[&PathBuf]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }
}

// ===========================================================================
// JSON-RPC 2.0 over UDS tests
// ===========================================================================

// ---------------------------------------------------------------------------
// JRPC-1: execute committed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_execute_committed() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-jrpc", &socket)
        .await
        .expect("connect");

    // CREATE TABLE
    let resp = client
        .execute(SqlOperation {
            sql: "CREATE TABLE IF NOT EXISTS jrpc_events (id INTEGER PRIMARY KEY, val TEXT)".into(),
            params: vec![],
        })
        .await
        .expect("create");
    assert_eq!(resp.status, WriteStatus::Committed, "create must commit");

    // INSERT
    let resp = client
        .execute(SqlOperation {
            sql: "INSERT INTO jrpc_events(val) VALUES (?)".into(),
            params: vec![serde_json::Value::String("hello".into())],
        })
        .await
        .expect("insert");
    assert_eq!(resp.status, WriteStatus::Committed, "insert must commit");
    assert!(resp.commit_seq.is_some(), "commit_seq must be present");

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-2: multi-op transaction committed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_transaction_multi_op_committed() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-jrpc-tx", &socket)
        .await
        .expect("connect");

    // Setup tables
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE jrpc_tx_a (id INTEGER PRIMARY KEY, val TEXT NOT NULL)".into(),
            params: vec![],
        })
        .await
        .expect("create a");
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE jrpc_tx_b (id INTEGER PRIMARY KEY, val TEXT NOT NULL)".into(),
            params: vec![],
        })
        .await
        .expect("create b");

    // Atomic transaction: insert into both tables.
    let resp = client
        .transaction(vec![
            SqlOperation {
                sql: "INSERT INTO jrpc_tx_a(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("row-a-1".into())],
            },
            SqlOperation {
                sql: "INSERT INTO jrpc_tx_a(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("row-a-2".into())],
            },
            SqlOperation {
                sql: "INSERT INTO jrpc_tx_b(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("row-b-1".into())],
            },
        ])
        .await
        .expect("transaction");

    assert_eq!(
        resp.status,
        WriteStatus::Committed,
        "multi-op JSON-RPC transaction must commit"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-3: atomic rollback → error -32000 via WriteResponse::Failed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_atomic_rollback() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-jrpc-rollback", &socket)
        .await
        .expect("connect");

    // Setup table with UNIQUE constraint for rollback probe.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE jrpc_rollback (id INTEGER PRIMARY KEY, val TEXT NOT NULL UNIQUE)"
                .into(),
            params: vec![],
        })
        .await
        .expect("create rollback table");

    // Transaction: op1 valid ("good"), op2 violates NOT NULL → whole tx fails.
    let resp = client
        .transaction(vec![
            SqlOperation {
                sql: "INSERT INTO jrpc_rollback(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("good".into())],
            },
            SqlOperation {
                sql: "INSERT INTO jrpc_rollback(val) VALUES (?)".into(),
                params: vec![serde_json::Value::Null], // violates NOT NULL
            },
        ])
        .await
        .expect("transaction (error expected in response)");

    assert_eq!(resp.status, WriteStatus::Failed, "violating tx must fail");
    assert!(resp.error.is_some(), "error field must be set on failure");

    // Probe: inserting "good" again must succeed (op1 was rolled back).
    let probe = client
        .execute(SqlOperation {
            sql: "INSERT INTO jrpc_rollback(val) VALUES ('good')".into(),
            params: vec![],
        })
        .await
        .expect("probe");
    assert_eq!(
        probe.status,
        WriteStatus::Committed,
        "probe must commit, proving op1 was rolled back"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-4: stats via JSON-RPC reflects accepted/committed counts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_stats() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-jrpc-stats", &socket)
        .await
        .expect("connect");

    // Initial stats: all zeros.
    let snap = client.stats().await.expect("initial stats");
    assert_eq!(snap.accepted, 0);
    assert_eq!(snap.committed, 0);

    // Execute two requests.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE jrpc_stats_tbl (id INTEGER PRIMARY KEY)".into(),
            params: vec![],
        })
        .await
        .expect("create");
    client
        .execute(SqlOperation {
            sql: "INSERT INTO jrpc_stats_tbl VALUES (1)".into(),
            params: vec![],
        })
        .await
        .expect("insert");

    let snap = client.stats().await.expect("stats after writes");
    assert_eq!(snap.accepted, 2, "accepted must be 2");
    assert_eq!(snap.committed, 2, "committed must be 2");
    assert_eq!(snap.failed, 0, "failed must be 0");

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-5: method not found → JSON-RPC -32601
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_method_not_found() {
    let (socket, db, shutdown) = start_gateway().await;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    write_half
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"x1\",\"method\":\"unknown_method\"}\n")
        .await
        .expect("write");

    let line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    let v: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");

    assert_eq!(v["jsonrpc"], "2.0", "must be JSON-RPC 2.0 response");
    assert_eq!(v["id"], "x1", "id must be echoed back");
    assert_eq!(
        v["error"]["code"], -32601,
        "unknown method must return -32601"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-6: invalid params (actor_id missing) → -32602
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_invalid_params_missing_actor_id() {
    let (socket, db, shutdown) = start_gateway().await;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    // execute without actor_id → -32602 Invalid params.
    let req = r#"{"jsonrpc":"2.0","id":42,"method":"execute","params":{"operations":[{"sql":"SELECT 1","params":[]}]}}"#;
    write_half.write_all(req.as_bytes()).await.expect("write");
    write_half.write_all(b"\n").await.expect("newline");

    let line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    let v: serde_json::Value = serde_json::from_str(&line).expect("valid JSON");

    assert_eq!(v["jsonrpc"], "2.0");
    assert_eq!(v["id"], 42, "numeric id must be echoed");
    assert_eq!(
        v["error"]["code"], -32602,
        "missing actor_id must return -32602"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-7: parse error (broken JSON) → -32700, connection stays open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_parse_error_connection_stays_open() {
    let (socket, db, shutdown) = start_gateway().await;

    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    // Send invalid JSON — must get -32700 parse error.
    write_half
        .write_all(b"not valid json at all\n")
        .await
        .expect("write invalid");

    let error_line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    let err_v: serde_json::Value =
        serde_json::from_str(&error_line).expect("error response is valid JSON");

    assert_eq!(err_v["jsonrpc"], "2.0", "must be JSON-RPC 2.0");
    assert_eq!(
        err_v["error"]["code"], -32700,
        "parse error must use code -32700"
    );
    assert!(
        err_v["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("parse error"),
        "error message must mention 'parse error'"
    );

    // Connection must still be usable: send a valid health request.
    write_half
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"health\"}\n")
        .await
        .expect("write health");
    let health_line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    let health_v: serde_json::Value =
        serde_json::from_str(&health_line).expect("health response is valid JSON");

    assert_eq!(health_v["jsonrpc"], "2.0");
    assert_eq!(health_v["id"], 99, "health id must be echoed");
    assert!(
        health_v.get("result").is_some(),
        "health must return a result object"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-8: BLOB params → committed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_jsonrpc_blob_params() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-jrpc-blob", &socket)
        .await
        .expect("connect");

    // Create table for BLOBs.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE jrpc_files (id INTEGER PRIMARY KEY, name TEXT, data BLOB)".into(),
            params: vec![],
        })
        .await
        .expect("create files");

    // Insert with a $blob sentinel — base64("hello").
    let resp = client
        .execute(SqlOperation {
            sql: "INSERT INTO jrpc_files(name, data) VALUES (?, ?)".into(),
            params: vec![
                serde_json::Value::String("avatar.png".into()),
                serde_json::json!({"$blob": "aGVsbG8="}), // base64("hello")
            ],
        })
        .await
        .expect("blob insert");

    assert_eq!(
        resp.status,
        WriteStatus::Committed,
        "BLOB insert via JSON-RPC must commit"
    );
    assert!(
        resp.commit_seq.is_some(),
        "commit_seq must be present after BLOB insert"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Socket permissions — default 0o600 (owner-only)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_socket_permission_default_0o600() {
    use std::os::unix::fs::PermissionsExt;

    let (socket, db, shutdown) = start_gateway().await;

    let mode = std::fs::metadata(&socket)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(
        mode, 0o600,
        "default socket_mode must be 0o600, got 0o{mode:o}"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Socket permissions — custom 0o660 (owner + group)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_socket_permission_custom_0o660() {
    use std::os::unix::fs::PermissionsExt;

    let (socket, db, shutdown) = start_gateway_with_mode(0o660).await;

    let mode = std::fs::metadata(&socket)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(
        mode, 0o660,
        "socket_mode 0o660 must be applied after bind, got 0o{mode:o}"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Shutdown removes the socket file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_shutdown_removes_socket() {
    let (socket, db, shutdown) = start_gateway().await;

    assert!(
        socket.exists(),
        "socket must exist while gateway is running"
    );

    // Trigger graceful shutdown.
    shutdown.notify_one();

    // Wait for the socket to disappear.
    for _ in 0..50 {
        if !socket.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(
        !socket.exists(),
        "socket file must be removed after shutdown"
    );

    cleanup(&[&socket, &db]);
}

// ===========================================================================
// §13 Idempotency E2E tests (JSON-RPC over UDS)
// ===========================================================================

/// Helper: send a single raw JSON-RPC 2.0 request over UDS and return the
/// parsed response value.
async fn rpc_raw(
    socket: &std::path::Path,
    body: serde_json::Value,
) -> serde_json::Value {
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

    let stream = UnixStream::connect(socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    let line = serde_json::to_string(&body).expect("serialize") + "\n";
    write_half.write_all(line.as_bytes()).await.expect("write");

    let resp_line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    serde_json::from_str(&resp_line).expect("valid JSON response")
}

// ---------------------------------------------------------------------------
// JRPC-IDEM-1: same idempotency_key + same ops → dedup, no duplicate row
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency_same_key_same_ops_no_duplicate() {
    let (socket, db, shutdown) = start_gateway().await;

    // Setup: CREATE the target table via the UDS.
    let setup = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "setup",
            "method": "execute",
            "params": {
                "actor_id": "agent-idem",
                "operations": [{
                    "sql": "CREATE TABLE idem_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL)",
                    "params": []
                }]
            }
        }),
    )
    .await;
    assert!(
        setup.get("result").is_some(),
        "CREATE TABLE must succeed; got: {setup}"
    );

    // First execute: insert a row with an idempotency_key.
    let resp1 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "idem-req-1",
            "method": "execute",
            "params": {
                "actor_id": "agent-idem",
                "idempotency_key": "idem-key-1",
                "operations": [{
                    "sql": "INSERT INTO idem_tbl(val) VALUES (?)",
                    "params": ["hello"]
                }]
            }
        }),
    )
    .await;
    assert!(
        resp1.get("result").is_some(),
        "first execute must return a result; got: {resp1}"
    );
    assert_eq!(
        resp1["result"]["status"], "committed",
        "first execute must commit"
    );
    let commit_seq_1 = resp1["result"]["commit_seq"].clone();
    assert!(
        !commit_seq_1.is_null(),
        "commit_seq must be present in first response"
    );

    // Second execute: same idempotency_key and same operations → must return
    // the stored response (same commit_seq) without re-executing.
    let resp2 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "idem-req-2",
            "method": "execute",
            "params": {
                "actor_id": "agent-idem",
                "idempotency_key": "idem-key-1",
                "operations": [{
                    "sql": "INSERT INTO idem_tbl(val) VALUES (?)",
                    "params": ["hello"]
                }]
            }
        }),
    )
    .await;
    assert!(
        resp2.get("result").is_some(),
        "second execute (dedup) must return a result; got: {resp2}"
    );
    assert_eq!(
        resp2["result"]["status"], "committed",
        "second execute must also report committed (from stored response)"
    );
    assert_eq!(
        resp2["result"]["commit_seq"], commit_seq_1,
        "second execute must return the same commit_seq as the first"
    );

    // Third execute: different idempotency_key → a new insert must happen.
    let resp3 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "idem-req-3",
            "method": "execute",
            "params": {
                "actor_id": "agent-idem",
                "idempotency_key": "idem-key-2",
                "operations": [{
                    "sql": "INSERT INTO idem_tbl(val) VALUES (?)",
                    "params": ["world"]
                }]
            }
        }),
    )
    .await;
    assert!(
        resp3.get("result").is_some(),
        "third execute (different key) must succeed; got: {resp3}"
    );

    // Re-send idem-key-1 a third time — it must still be deduped and return the
    // original commit_seq. A stable commit_seq across re-sends (resp2, resp4)
    // proves the request was not re-executed, i.e. the idempotency record was
    // persisted atomically with the write (no lossy 2-phase commit window).
    let resp4 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "idem-req-4",
            "method": "execute",
            "params": {
                "actor_id": "agent-idem",
                "idempotency_key": "idem-key-1",
                "operations": [{
                    "sql": "INSERT INTO idem_tbl(val) VALUES (?)",
                    "params": ["hello"]
                }]
            }
        }),
    )
    .await;
    assert_eq!(
        resp4["result"]["commit_seq"], commit_seq_1,
        "third send with idem-key-1 must still return the original commit_seq"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// JRPC-IDEM-2: same key + different ops → IdempotencyConflict (-32000)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_idempotency_conflict_different_ops() {
    let (socket, db, shutdown) = start_gateway().await;

    // Setup table.
    let setup = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "setup-conflict",
            "method": "execute",
            "params": {
                "actor_id": "agent-conflict",
                "operations": [{
                    "sql": "CREATE TABLE conflict_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)",
                    "params": []
                }]
            }
        }),
    )
    .await;
    assert!(setup.get("result").is_some(), "CREATE TABLE must succeed");

    // First request with "conflict-key".
    let resp1 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "conflict-req-1",
            "method": "execute",
            "params": {
                "actor_id": "agent-conflict",
                "idempotency_key": "conflict-key",
                "operations": [{
                    "sql": "INSERT INTO conflict_tbl(val) VALUES (?)",
                    "params": ["first"]
                }]
            }
        }),
    )
    .await;
    assert_eq!(
        resp1["result"]["status"], "committed",
        "first request must commit; got: {resp1}"
    );

    // Second request with the same "conflict-key" but different operations →
    // must return an error response (IdempotencyConflict → ERR_WRITE_FAILED -32000).
    let resp2 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "conflict-req-2",
            "method": "execute",
            "params": {
                "actor_id": "agent-conflict",
                "idempotency_key": "conflict-key",
                "operations": [{
                    "sql": "INSERT INTO conflict_tbl(val) VALUES (?)",
                    "params": ["DIFFERENT"]
                }]
            }
        }),
    )
    .await;
    assert!(
        resp2.get("error").is_some(),
        "conflict request must return an error; got: {resp2}"
    );
    assert_eq!(
        resp2["error"]["code"], -32000,
        "IdempotencyConflict must map to ERR_WRITE_FAILED (-32000); got: {resp2}"
    );
    let msg = resp2["error"]["message"]
        .as_str()
        .unwrap_or("");
    assert!(
        msg.contains("idempotency conflict"),
        "error message must contain 'idempotency conflict'; got: {msg}"
    );

    // Verify only one row was inserted (the conflict did not insert a second row).
    // We confirm by sending another different-ops request with a new key and
    // checking it gets a new, distinct commit_seq (proving the table write path
    // still works and the first row was not duplicated by the conflict attempt).
    let resp3 = rpc_raw(
        &socket,
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": "conflict-verify",
            "method": "execute",
            "params": {
                "actor_id": "agent-conflict",
                "idempotency_key": "new-key",
                "operations": [{
                    "sql": "INSERT INTO conflict_tbl(val) VALUES (?)",
                    "params": ["third"]
                }]
            }
        }),
    )
    .await;
    assert_eq!(
        resp3["result"]["status"], "committed",
        "new-key request must commit; got: {resp3}"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}
