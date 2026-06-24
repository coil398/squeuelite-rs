//! Integration tests for the Unix Domain Socket sidecar (§18, §20, §24, §27).
//!
//! These tests are only compiled when the `sidecar` feature is enabled.

#![cfg(feature = "sidecar")]

use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Notify;

use squeuelite::{Client, SidecarConfig, SidecarGateway, SqlOperation, WriteStatus};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Generate a unique temp path prefix using the thread ID + a counter.
fn temp_prefix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("/tmp/squeuelite_test_{id}_{}", std::process::id())
}

/// Start a `SidecarGateway` in a background task.
///
/// Returns `(socket_path, db_path, shutdown_notify)`.
/// Call `shutdown_notify.notify_one()` to trigger graceful shutdown.
///
/// `allow_schema_write` is set to `true` so that tests can CREATE tables via
/// the write channel without being rejected by the §23 security filter.
async fn start_gateway() -> (PathBuf, PathBuf, Arc<Notify>) {
    let prefix = temp_prefix();
    let db_path = PathBuf::from(format!("{prefix}.db"));
    let socket_path = PathBuf::from(format!("{prefix}.sock"));

    let mut gateway_config = squeuelite::GatewayConfig::new(&db_path);
    // §23: allow_schema_write=true so integration tests can CREATE tables.
    gateway_config.allow_schema_write = true;
    let config = SidecarConfig {
        gateway: gateway_config,
        socket_path: socket_path.clone(),
    };
    let gateway = SidecarGateway::open(config).expect("open gateway");

    let notify = Arc::new(Notify::new());
    let notify_clone = Arc::clone(&notify);
    let socket_clone = socket_path.clone();

    tokio::spawn(async move {
        // Wait for the shutdown signal.
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

/// Clean up leftover temp files after a test.
fn cleanup(paths: &[&PathBuf]) {
    for p in paths {
        let _ = std::fs::remove_file(p);
        // Also try WAL / SHM files.
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }
}

// ---------------------------------------------------------------------------
// Test 1: CREATE TABLE + INSERT → Committed + commit_seq present
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_execute_insert_committed() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-test", &socket)
        .await
        .expect("connect");

    // CREATE TABLE
    let resp = client
        .execute(SqlOperation {
            sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, val TEXT)".into(),
            params: vec![],
        })
        .await
        .expect("execute create");
    assert_eq!(resp.status, WriteStatus::Committed);

    // INSERT
    let resp = client
        .execute(SqlOperation {
            sql: "INSERT INTO events(val) VALUES (?)".into(),
            params: vec![serde_json::Value::String("hello".into())],
        })
        .await
        .expect("execute insert");
    assert_eq!(resp.status, WriteStatus::Committed, "insert must commit");
    assert!(
        resp.commit_seq.is_some(),
        "commit_seq must be set (track_commits=true by default)"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Test 2a: transaction (multiple ops) — all ops commit (happy path)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transaction_multi_op_committed() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-tx", &socket)
        .await
        .expect("connect");

    // Setup: two tables
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE tx_events (id INTEGER PRIMARY KEY, val TEXT NOT NULL)".into(),
            params: vec![],
        })
        .await
        .expect("create tx_events");

    client
        .execute(SqlOperation {
            sql: "CREATE TABLE tx_runs (id INTEGER PRIMARY KEY, status TEXT NOT NULL)".into(),
            params: vec![],
        })
        .await
        .expect("create tx_runs");

    // Atomic transaction: insert into both tables at once — all ops must succeed.
    let resp = client
        .transaction(vec![
            SqlOperation {
                sql: "INSERT INTO tx_events(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("event-1".into())],
            },
            SqlOperation {
                sql: "INSERT INTO tx_events(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("event-2".into())],
            },
            SqlOperation {
                sql: "INSERT INTO tx_runs(status) VALUES (?)".into(),
                params: vec![serde_json::Value::String("running".into())],
            },
        ])
        .await
        .expect("transaction");

    assert_eq!(
        resp.status,
        WriteStatus::Committed,
        "multi-op transaction must commit"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Test 2b: transaction atomic rollback — partial failure rolls back all ops
// ---------------------------------------------------------------------------
//
// This test proves that if op2 in a multi-op transaction fails with a
// constraint violation, op1 is also rolled back (no partial commit).
// Strategy mirrors `src/inprocess.rs::test_atomic_rollback_on_constraint_violation`:
// we insert "good" as op1, then violate NOT NULL as op2. After the Failed
// response we send a probe INSERT of "good"; if op1 were *not* rolled back the
// UNIQUE constraint would reject the probe, so a Committed probe status proves
// the table was empty (op1's row was rolled back).

#[tokio::test]
async fn test_transaction_atomic_rollback() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-tx-rollback", &socket)
        .await
        .expect("connect");

    // Setup: table with NOT NULL + UNIQUE constraints so we can probe rollback.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE rollback_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL UNIQUE)"
                .into(),
            params: vec![],
        })
        .await
        .expect("create rollback_test");

    // Transaction: op1 is valid ("good"), op2 violates NOT NULL → whole tx fails.
    let resp = client
        .transaction(vec![
            SqlOperation {
                sql: "INSERT INTO rollback_test(val) VALUES (?)".into(),
                params: vec![serde_json::Value::String("good".into())],
            },
            SqlOperation {
                sql: "INSERT INTO rollback_test(val) VALUES (?)".into(),
                params: vec![serde_json::Value::Null], // violates NOT NULL
            },
        ])
        .await
        .expect("transaction (error expected in response)");

    assert_eq!(
        resp.status,
        WriteStatus::Failed,
        "transaction with a constraint violation must fail"
    );
    assert!(resp.error.is_some(), "error field must be set on failure");

    // Probe: attempt to INSERT "good" again.
    // If op1 was NOT rolled back, the UNIQUE constraint would reject this probe
    // (status = Failed). A Committed result proves op1's row was rolled back.
    let probe = client
        .execute(SqlOperation {
            sql: "INSERT INTO rollback_test(val) VALUES ('good')".into(),
            params: vec![],
        })
        .await
        .expect("probe execute");

    assert_eq!(
        probe.status,
        WriteStatus::Committed,
        "probe INSERT of 'good' must succeed via socket, proving op1 was rolled back \
         (UNIQUE constraint would have rejected it if the row still existed)"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Test 3: admin stats reflects accepted/committed counts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_admin_stats_counts() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-stats", &socket)
        .await
        .expect("connect");

    // Initial stats: all zeros.
    let snap = client.stats().await.expect("stats");
    assert_eq!(snap.accepted, 0);
    assert_eq!(snap.committed, 0);

    // Execute two requests.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE stats_test (id INTEGER PRIMARY KEY)".into(),
            params: vec![],
        })
        .await
        .expect("create");

    client
        .execute(SqlOperation {
            sql: "INSERT INTO stats_test VALUES (1)".into(),
            params: vec![],
        })
        .await
        .expect("insert");

    // Check stats.
    let snap = client.stats().await.expect("stats after writes");
    assert_eq!(snap.accepted, 2, "accepted must be 2");
    assert_eq!(snap.committed, 2, "committed must be 2");
    assert_eq!(snap.failed, 0);
    assert_eq!(snap.queue_capacity, 1024);

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Test 4: invalid JSON line → error response, connection stays open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_invalid_json_connection_stays_open() {
    let (socket, db, shutdown) = start_gateway().await;

    // Use raw socket to send a malformed line, then a valid write.
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
    };

    let stream = UnixStream::connect(&socket).await.expect("connect");
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half).lines();

    // Send invalid JSON.
    write_half
        .write_all(b"not valid json\n")
        .await
        .expect("write");

    let error_line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    let error_v: serde_json::Value =
        serde_json::from_str(&error_line).expect("error response is valid JSON");
    assert_eq!(
        error_v["status"], "failed",
        "parse error must return status=failed"
    );
    assert!(
        error_v["error"].as_str().unwrap_or("").contains("parse error"),
        "error must mention 'parse error'"
    );

    // Connection must still be usable: send a valid admin health command.
    write_half
        .write_all(b"{\"type\":\"health\"}\n")
        .await
        .expect("write health");
    let health_line = reader
        .next_line()
        .await
        .expect("read")
        .expect("line present");
    assert!(
        health_line.contains("ok"),
        "health response after error must be ok"
    );

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}

// ---------------------------------------------------------------------------
// Test 5: shutdown removes the socket file
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_shutdown_removes_socket() {
    let (socket, db, shutdown) = start_gateway().await;

    assert!(socket.exists(), "socket must exist while gateway is running");

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

// ---------------------------------------------------------------------------
// Test 6: failed SQL (constraint violation) → Failed status + failed counter
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_failed_sql_increments_failed_counter() {
    let (socket, db, shutdown) = start_gateway().await;

    let mut client = Client::connect("agent-fail", &socket)
        .await
        .expect("connect");

    // Create a table with a NOT NULL constraint.
    client
        .execute(SqlOperation {
            sql: "CREATE TABLE nn_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)".into(),
            params: vec![],
        })
        .await
        .expect("create");

    // Intentionally violate the NOT NULL constraint.
    let resp = client
        .execute(SqlOperation {
            sql: "INSERT INTO nn_test(val) VALUES (?)".into(),
            params: vec![serde_json::Value::Null],
        })
        .await
        .expect("execute (error expected in response)");

    assert_eq!(resp.status, WriteStatus::Failed);
    assert!(resp.error.is_some(), "error message must be present");

    // Stats must reflect: 2 accepted (create + insert), 1 committed, 1 failed.
    let snap = client.stats().await.expect("stats");
    assert_eq!(snap.accepted, 2);
    assert_eq!(snap.committed, 1, "only CREATE committed");
    assert_eq!(snap.failed, 1, "the NULL insert must be counted as failed");

    shutdown.notify_one();
    tokio::time::sleep(Duration::from_millis(100)).await;
    cleanup(&[&socket, &db]);
}
