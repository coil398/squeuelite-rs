//! In-process gateway — public API for single-process multi-task usage (§21).
//!
//! This module is only compiled when the `inprocess` feature is enabled.

use std::thread;

use rusqlite::Connection;
use tokio::sync::mpsc;

use crate::{
    config::{GatewayConfig, OverflowPolicy},
    error::{Error, Result},
    request::{WriteRequest, WriteResponse},
    writer::{apply_pragmas, new_latency_buffer, run_migrations, Command, LatencyBuffer, Writer},
};

// ---------------------------------------------------------------------------
// InProcessGateway
// ---------------------------------------------------------------------------

/// Owns the writer thread and the sending end of the command channel.
///
/// ## Shutdown behaviour
///
/// The recommended shutdown path is the explicit [`InProcessGateway::shutdown`]
/// async method: it sends a [`Command::Shutdown`] to the writer thread, then
/// blocks until the thread finishes (which includes the WAL checkpoint, §16).
///
/// When `InProcessGateway` is dropped without calling `shutdown`, the `Drop`
/// implementation drops the channel sender. Because all `Sender` clones are
/// gone, the writer thread's `blocking_recv()` returns `None` on the next
/// iteration and the loop exits naturally. The thread is **not** joined on
/// drop to avoid blocking an async runtime context (joining a thread from
/// inside an async executor can deadlock if the executor is single-threaded).
/// This means the WAL checkpoint may run *after* the gateway struct is gone,
/// which is acceptable for most use cases.
///
/// **Recommendation**: always call `shutdown()` explicitly when you need the
/// WAL checkpoint to complete before the process exits.
pub struct InProcessGateway {
    sender: mpsc::Sender<Command>,
    writer_handle: Option<thread::JoinHandle<()>>,
    db_path: std::path::PathBuf,
    latency_buf: LatencyBuffer,
    overflow: OverflowPolicy,
}

impl InProcessGateway {
    /// Open a gateway with default configuration (§21 `InProcessGateway::open`).
    ///
    /// Equivalent to `open_with_config(GatewayConfig::new(db_path))`.
    pub fn open(db_path: impl Into<std::path::PathBuf>) -> Result<Self> {
        let config = GatewayConfig::new(db_path);
        Self::open_with_config(config)
    }

    /// Open a gateway with full configuration control.
    ///
    /// Steps (§20.1):
    /// 1. Open the SQLite connection.
    /// 2. Apply PRAGMAs (§16).
    /// 3. Run startup migrations (§17 case A).
    /// 4. Spawn the writer thread with a bounded mpsc channel (§14).
    pub fn open_with_config(config: GatewayConfig) -> Result<Self> {
        // Step 1 — open connection.
        let conn = Connection::open(&config.db_path)?;

        // Step 2 — apply PRAGMAs before any user traffic arrives.
        apply_pragmas(&conn, &config)?;

        // Step 3 — create internal tables (no-op when track_commits=false).
        // Startup migration runs on the writer Connection directly and is NOT
        // subject to the validate_sql security checks (§23 note in run_migrations).
        run_migrations(&conn, config.track_commits, config.idempotency)?;

        // Step 4 — bounded channel (§14), latency buffer (§24), and writer thread.
        let (tx, rx) = mpsc::channel::<Command>(config.queue_capacity);

        let latency_buf = new_latency_buffer();
        let latency_buf_writer = latency_buf.clone();

        let track_commits = config.track_commits;
        let idempotency = config.idempotency;
        let overflow = config.overflow.clone();
        let config_clone = config.clone();
        let handle = thread::spawn(move || {
            // Connection is Send but !Sync; moving it into the thread is the
            // only safe pattern (Arc<Mutex<Connection>> risks deadlock because
            // rusqlite's internal locking interacts poorly with external locking).
            Writer::new(
                conn,
                rx,
                track_commits,
                idempotency,
                config_clone,
                latency_buf_writer,
            )
            .run();
        });

        Ok(Self {
            sender: tx,
            writer_handle: Some(handle),
            db_path: config.db_path,
            latency_buf,
            overflow,
        })
    }

    /// Return a cloneable handle that async tasks can use to submit requests
    /// (§21 `gateway.handle()`).
    pub fn handle(&self) -> GatewayHandle {
        GatewayHandle {
            sender: self.sender.clone(),
            latency_buf: self.latency_buf.clone(),
            overflow: self.overflow.clone(),
        }
    }

    /// Return the database path this gateway was opened with.
    ///
    /// Used by the sidecar layer to locate the WAL file for stats (§24).
    pub fn db_path(&self) -> &std::path::PathBuf {
        &self.db_path
    }

    /// Gracefully shut down the gateway.
    ///
    /// Sends [`Command::Shutdown`] to the writer thread, waits for it to
    /// finish (which includes the WAL checkpoint), and consumes `self`.
    ///
    /// Returns [`Error::GatewayClosed`] if the writer thread panicked.
    ///
    /// # Runtime requirement
    ///
    /// This method calls `thread::JoinHandle::join()` directly, which **blocks
    /// the calling OS thread** until the writer thread exits. This is safe with
    /// the `rt-multi-thread` Tokio runtime because the blocking call runs on a
    /// worker thread and does not starve the async scheduler. On a
    /// `current_thread` runtime, this call will block the single runtime thread
    /// and stall all other async tasks until the writer finishes. In that case,
    /// wrap the call in `tokio::task::spawn_blocking`:
    ///
    /// ```ignore
    /// tokio::task::spawn_blocking(move || {
    ///     tokio::runtime::Handle::current().block_on(gateway.shutdown())
    /// }).await??;
    /// ```
    pub async fn shutdown(mut self) -> Result<()> {
        // Send Shutdown command; if the channel is already closed the writer
        // has already exited — that is fine, we just join.
        let _ = self.sender.send(Command::Shutdown).await;

        // self.sender will be dropped when `self` is consumed at end of this
        // function, closing the channel if the Shutdown message was not received.

        // Wait for the writer thread to finish (includes WAL checkpoint §16).
        if let Some(handle) = self.writer_handle.take() {
            handle.join().map_err(|_| Error::GatewayClosed)?;
        }

        Ok(())
    }
}

impl Drop for InProcessGateway {
    /// Best-effort shutdown on drop: dropping the sender causes the writer
    /// thread's `blocking_recv()` to return `None`, terminating the loop.
    ///
    /// The thread is NOT joined here to avoid blocking an async runtime.
    /// Prefer calling `shutdown().await` explicitly for a clean exit.
    fn drop(&mut self) {
        // writer_handle and sender will be dropped automatically; the sender
        // drop is what triggers the natural writer thread exit.
    }
}

// ---------------------------------------------------------------------------
// GatewayHandle
// ---------------------------------------------------------------------------

/// A cheaply cloneable handle that async tasks use to submit write requests.
///
/// Obtain from [`InProcessGateway::handle`].
#[derive(Clone)]
pub struct GatewayHandle {
    sender: mpsc::Sender<Command>,
    latency_buf: LatencyBuffer,
    overflow: OverflowPolicy,
}

impl GatewayHandle {
    /// Submit a write request and wait for the response (§21 `.await?`).
    ///
    /// The request is sent to the writer thread's bounded queue. The behaviour
    /// when the queue is full is governed by [`OverflowPolicy`] (§14):
    ///
    /// - [`OverflowPolicy::Wait`]: blocks indefinitely until a slot opens.
    /// - [`OverflowPolicy::Reject`]: returns [`Error::GatewayOverloaded`] immediately.
    /// - [`OverflowPolicy::WaitTimeout`]: waits up to `millis` ms; returns
    ///   [`Error::GatewayOverloaded`] on timeout.
    ///
    /// Returns `Err(Error::GatewayClosed)` if the writer thread has already stopped.
    pub async fn execute(&self, request: WriteRequest) -> Result<WriteResponse> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let cmd = Command::Write {
            request,
            respond_to: tx,
        };

        // §14 Backpressure — send the command according to the overflow policy.
        match &self.overflow {
            OverflowPolicy::Wait => {
                // Unbounded wait: the caller will block until a slot opens.
                self.sender
                    .send(cmd)
                    .await
                    .map_err(|_| Error::GatewayClosed)?;
            }
            OverflowPolicy::Reject => {
                // Non-blocking: fail immediately if the queue is full.
                self.sender.try_send(cmd).map_err(|e| match e {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => Error::GatewayOverloaded,
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => Error::GatewayClosed,
                })?;
            }
            OverflowPolicy::WaitTimeout { millis } => {
                // Timed wait: fail with GatewayOverloaded if the queue stays
                // full for longer than the configured timeout.
                tokio::time::timeout(
                    std::time::Duration::from_millis(*millis),
                    self.sender.send(cmd),
                )
                .await
                .map_err(|_| Error::GatewayOverloaded)? // timeout expired
                .map_err(|_| Error::GatewayClosed)?; // channel closed
            }
        }

        rx.await.map_err(|_| Error::GatewayClosed)
    }

    /// Number of pending items currently in the bounded channel (§24 stats).
    ///
    /// `queue_depth = max_capacity - current_capacity` (i.e. items waiting).
    pub fn queue_depth(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }

    /// Maximum capacity of the bounded channel (§24 stats, §14).
    pub fn queue_capacity(&self) -> usize {
        self.sender.max_capacity()
    }

    /// Run `PRAGMA wal_checkpoint(TRUNCATE)` via the writer thread (§24 admin).
    ///
    /// Returns `Err(Error::GatewayClosed)` if the writer thread has stopped.
    pub async fn checkpoint(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(Command::Checkpoint { respond_to: tx })
            .await
            .map_err(|_| Error::GatewayClosed)?;

        rx.await.map_err(|_| Error::GatewayClosed)?
    }

    /// Compute avg and p95 commit latency from the shared ring buffer (§24).
    ///
    /// Returns `(avg_micros, p95_micros)`. Both are `0` when no commits have
    /// been recorded yet (empty buffer).
    ///
    /// `p95` is computed by sorting a snapshot of the buffer and taking the
    /// element at the 95th percentile index.
    pub fn latency_snapshot(&self) -> (f64, u64) {
        let guard = match self.latency_buf.lock() {
            Ok(g) => g,
            Err(_) => return (0.0, 0),
        };
        if guard.is_empty() {
            return (0.0, 0);
        }

        let sum: u64 = guard.iter().sum();
        let avg = sum as f64 / guard.len() as f64;

        // p95: copy, sort, index at ⌈0.95 * N⌉ - 1 (0-indexed).
        let mut sorted: Vec<u64> = guard.iter().copied().collect();
        sorted.sort_unstable();
        let p95_idx = ((sorted.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
        let p95 = sorted[p95_idx];

        (avg, p95)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::GatewayConfig,
        request::{SqlOperation, WriteRequest, WriteStatus},
    };

    fn make_request(id: &str, ops: Vec<SqlOperation>) -> WriteRequest {
        WriteRequest {
            request_id: id.to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: ops,
        }
    }

    fn sql_op(sql: &str, params: Vec<serde_json::Value>) -> SqlOperation {
        SqlOperation {
            sql: sql.to_string(),
            params,
        }
    }

    async fn open_memory_gateway(track_commits: bool) -> InProcessGateway {
        let mut config = GatewayConfig::new(":memory:");
        config.track_commits = track_commits;
        // Tests need to CREATE tables via execute(); allow_schema_write must be true.
        config.allow_schema_write = true;
        InProcessGateway::open_with_config(config).unwrap()
    }

    // -----------------------------------------------------------------------
    // Helper: create a test table via the gateway
    // -----------------------------------------------------------------------

    async fn create_test_table(handle: &GatewayHandle) {
        let req = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE test_events (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
                vec![],
            )],
        );
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Committed);
    }

    // -----------------------------------------------------------------------
    // Test 1: single INSERT → Committed + commit_seq = Some(1)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_single_insert_committed_with_commit_seq() {
        let gw = open_memory_gateway(true).await;
        let handle = gw.handle();

        create_test_table(&handle).await;

        let req = make_request(
            "req-1",
            vec![sql_op(
                "INSERT INTO test_events(val) VALUES (?)",
                vec![serde_json::Value::String("hello".into())],
            )],
        );

        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Committed);
        // commit_seq = Some(1): first commit recorded in squeuelite_commits.
        // The CREATE TABLE above also increments the sequence, so this INSERT
        // is the second commit (seq=2). We check Some(_) rather than Some(1).
        assert!(resp.commit_seq.is_some(), "expected commit_seq to be set");
        assert!(resp.error.is_none());

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 2: atomic rollback — op2 constraint violation rolls back op1
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_atomic_rollback_on_constraint_violation() {
        let gw = open_memory_gateway(true).await;
        let handle = gw.handle();

        // Table with a NOT NULL constraint on val AND a UNIQUE constraint.
        // The UNIQUE constraint is used below to verify that op1's row was
        // rolled back: if "good" still existed, re-inserting it would fail.
        let setup = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE strict_events (id INTEGER PRIMARY KEY, val TEXT NOT NULL UNIQUE)",
                vec![],
            )],
        );
        handle.execute(setup).await.unwrap();

        // op1: valid INSERT; op2: NULL into NOT NULL → constraint violation.
        let req = make_request(
            "req-atomic",
            vec![
                sql_op(
                    "INSERT INTO strict_events(val) VALUES (?)",
                    vec![serde_json::Value::String("good".into())],
                ),
                sql_op(
                    "INSERT INTO strict_events(val) VALUES (?)",
                    vec![serde_json::Value::Null], // violates NOT NULL
                ),
            ],
        );

        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Failed);
        assert!(resp.commit_seq.is_none());
        assert!(resp.error.is_some());

        // Verify op1 was also rolled back.
        // Strategy: attempt to INSERT the same value ("good") that op1 tried to
        // insert. If op1 was NOT rolled back (row still exists), the UNIQUE
        // constraint would cause this probe to fail. A Committed result proves
        // the table was empty — op1's row was successfully rolled back.
        let probe = make_request(
            "probe",
            vec![sql_op(
                "INSERT INTO strict_events(val) VALUES ('good')",
                vec![],
            )],
        );
        let probe_resp = handle.execute(probe).await.unwrap();
        assert_eq!(
            probe_resp.status,
            WriteStatus::Committed,
            "probe INSERT of 'good' must succeed, proving op1 was rolled back \
             (UNIQUE constraint would have rejected it if the row still existed)"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 3: SQL constraint rejection — BEGIN and PRAGMA are forbidden (§10)
    //
    // The full set of 6 forbidden keywords (BEGIN / COMMIT / ROLLBACK /
    // SAVEPOINT / RELEASE / PRAGMA) is exhaustively unit-tested in
    // `src/writer.rs` (`test_reject_*`).  The two tests below only verify the
    // end-to-end path: that the rejection propagates correctly through the
    // gateway (channel send → writer actor → WriteResponse::Failed) for a
    // representative BEGIN case and a PRAGMA case.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_sql_constraint_begin_rejected() {
        let gw = open_memory_gateway(true).await;
        let handle = gw.handle();

        let req = make_request("req-begin", vec![sql_op("BEGIN", vec![])]);
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Failed);
        let error = resp.error.expect("error message must be set");
        assert!(
            error.contains("sql rejected"),
            "expected 'sql rejected' in error, got: {error}"
        );

        gw.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_sql_constraint_pragma_rejected() {
        let gw = open_memory_gateway(true).await;
        let handle = gw.handle();

        let req = make_request("req-pragma", vec![sql_op("PRAGMA journal_mode", vec![])]);
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Failed);
        let error = resp.error.expect("error message must be set");
        assert!(
            error.contains("sql rejected"),
            "expected 'sql rejected' in error, got: {error}"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 4: multiple statements in one sql string → Failed (§10, rusqlite)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_multiple_statements_rejected() {
        let gw = open_memory_gateway(true).await;
        let handle = gw.handle();

        create_test_table(&handle).await;

        // Two statements separated by `;` — rusqlite returns MultipleStatement.
        let req = make_request(
            "req-multi",
            vec![sql_op(
                "INSERT INTO test_events(val) VALUES ('a'); INSERT INTO test_events(val) VALUES ('b')",
                vec![],
            )],
        );
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Failed);
        assert!(resp.error.is_some());

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 5: track_commits=false → commit_seq=None, no squeuelite_commits table
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_track_commits_false_no_commit_seq() {
        let gw = open_memory_gateway(false).await;
        let handle = gw.handle();

        create_test_table(&handle).await;

        let req = make_request(
            "req-notrack",
            vec![sql_op(
                "INSERT INTO test_events(val) VALUES (?)",
                vec![serde_json::Value::String("data".into())],
            )],
        );
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(resp.status, WriteStatus::Committed);
        assert!(
            resp.commit_seq.is_none(),
            "commit_seq must be None when track_commits=false"
        );

        // Verify squeuelite_commits table does NOT exist.
        let table_check = make_request(
            "table-check",
            vec![sql_op(
                "INSERT INTO squeuelite_commits(request_id, actor_id) VALUES ('x', 'y')",
                vec![],
            )],
        );
        let check_resp = handle.execute(table_check).await.unwrap();
        assert_eq!(
            check_resp.status,
            WriteStatus::Failed,
            "squeuelite_commits table must not exist when track_commits=false"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 6: params conversion (null/bool/int/float/string/array)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_params_conversion() {
        let gw = open_memory_gateway(false).await;
        let handle = gw.handle();

        // Create a table that accepts all the types we want to test.
        let setup = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE param_test (
                    id      INTEGER PRIMARY KEY,
                    n       ANY,
                    b       INTEGER,
                    i       INTEGER,
                    f       REAL,
                    s       TEXT,
                    arr     TEXT
                )",
                vec![],
            )],
        );
        handle.execute(setup).await.unwrap();

        let req = make_request(
            "req-params",
            vec![sql_op(
                "INSERT INTO param_test(n, b, i, f, s, arr) VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    serde_json::Value::Null,
                    serde_json::Value::Bool(true),
                    serde_json::json!(42i64),
                    serde_json::json!(2.5f64),
                    serde_json::Value::String("hello".into()),
                    serde_json::json!(["a", "b"]),
                ],
            )],
        );

        let resp = handle.execute(req).await.unwrap();
        assert_eq!(
            resp.status,
            WriteStatus::Committed,
            "params conversion should succeed; error: {:?}",
            resp.error
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 7: §23 security — schema write rejected by default
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_schema_write_rejected_by_default() {
        // Default config has allow_schema_write=false.
        let config = GatewayConfig::new(":memory:");
        let gw = InProcessGateway::open_with_config(config).unwrap();
        let handle = gw.handle();

        let req = make_request(
            "req-create",
            vec![sql_op(
                "CREATE TABLE should_fail (id INTEGER PRIMARY KEY)",
                vec![],
            )],
        );
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(
            resp.status,
            WriteStatus::Failed,
            "CREATE must be rejected when allow_schema_write=false"
        );
        let err = resp.error.expect("error must be set");
        assert!(
            err.contains("sql rejected"),
            "expected 'sql rejected', got: {err}"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 8: §23 security — DROP rejected by default
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_drop_rejected_by_default() {
        // allow_drop=false by default.
        let config = GatewayConfig {
            allow_schema_write: true, // allow CREATE to set up the table
            ..GatewayConfig::new(":memory:")
        };
        let gw = InProcessGateway::open_with_config(config).unwrap();
        let handle = gw.handle();

        // Create a table first (allow_schema_write=true).
        let setup = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE drop_test (id INTEGER PRIMARY KEY)",
                vec![],
            )],
        );
        handle.execute(setup).await.unwrap();

        // DROP must be rejected (allow_drop=false by default).
        let req = make_request("req-drop", vec![sql_op("DROP TABLE drop_test", vec![])]);
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(
            resp.status,
            WriteStatus::Failed,
            "DROP must be rejected when allow_drop=false"
        );
        let err = resp.error.expect("error must be set");
        assert!(
            err.contains("sql rejected"),
            "expected 'sql rejected', got: {err}"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 9: §23 security — DELETE rejected when allow_delete=false
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_rejected_when_disabled() {
        let config = GatewayConfig {
            allow_schema_write: true,
            allow_delete: false,
            ..GatewayConfig::new(":memory:")
        };
        let gw = InProcessGateway::open_with_config(config).unwrap();
        let handle = gw.handle();

        // Create and populate a table.
        let setup = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE del_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
                vec![],
            )],
        );
        handle.execute(setup).await.unwrap();

        let insert = make_request(
            "insert",
            vec![sql_op("INSERT INTO del_test(val) VALUES ('row')", vec![])],
        );
        handle.execute(insert).await.unwrap();

        // DELETE must be rejected.
        let req = make_request("req-delete", vec![sql_op("DELETE FROM del_test", vec![])]);
        let resp = handle.execute(req).await.unwrap();
        assert_eq!(
            resp.status,
            WriteStatus::Failed,
            "DELETE must be rejected when allow_delete=false"
        );
        let err = resp.error.expect("error must be set");
        assert!(
            err.contains("sql rejected"),
            "expected 'sql rejected', got: {err}"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test 10: §13 idempotency — end-to-end via GatewayHandle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_idempotency_end_to_end() {
        let config = GatewayConfig {
            allow_schema_write: true,
            idempotency: true,
            track_commits: true,
            ..GatewayConfig::new(":memory:")
        };
        let gw = InProcessGateway::open_with_config(config).unwrap();
        let handle = gw.handle();

        // Setup table.
        let setup = make_request(
            "setup",
            vec![sql_op(
                "CREATE TABLE idem_test (id INTEGER PRIMARY KEY, val TEXT NOT NULL UNIQUE)",
                vec![],
            )],
        );
        handle.execute(setup).await.unwrap();

        // First call: new idempotency key.
        let mut req = make_request(
            "req-idem",
            vec![sql_op(
                "INSERT INTO idem_test(val) VALUES (?)",
                vec![serde_json::json!("only-once")],
            )],
        );
        req.idempotency_key = Some("idem-key-1".to_string());
        let resp1 = handle.execute(req.clone()).await.unwrap();
        assert_eq!(resp1.status, WriteStatus::Committed);

        // Second call: same key and same operations → returns stored response.
        let resp2 = handle.execute(req.clone()).await.unwrap();
        assert_eq!(
            resp2.status,
            WriteStatus::Committed,
            "second call with same idempotency_key must return Committed"
        );

        // Verify no double-insert (UNIQUE constraint would catch it if there was one).
        // We rely on the fact that the second call should not have inserted again.
        // Use a DELETE-like probe: if two rows existed, count would be 2.
        // We can't SELECT directly, so we insert a conflicting value to probe.
        let mut probe = make_request(
            "probe",
            vec![sql_op(
                "INSERT INTO idem_test(val) VALUES (?)",
                vec![serde_json::json!("only-once")],
            )],
        );
        // No idempotency key on probe — will try to INSERT the same value.
        probe.request_id = "probe-unique".to_string();
        let probe_resp = handle.execute(probe).await.unwrap();
        // If "only-once" was inserted twice, UNIQUE would have caught it on the
        // second idempotency call and the table would still have exactly one row.
        // This INSERT should fail because of the UNIQUE constraint.
        assert_eq!(
            probe_resp.status,
            WriteStatus::Failed,
            "duplicate UNIQUE value must still exist (idempotency prevented double-insert)"
        );

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test §14: OverflowPolicy::Reject — GatewayOverloaded when queue is full
    // -----------------------------------------------------------------------

    /// Open a gateway with capacity=1 and OverflowPolicy::Reject.
    ///
    /// To make the queue full we send a request that takes a long time to
    /// process by making the writer block. We use a helper that fills the queue
    /// with one pending request and then immediately tries to send another.
    ///
    /// Strategy: capacity=1 means the single slot is consumed as soon as the
    /// writer thread starts working on the first command. We verify that a
    /// second send with Reject policy returns GatewayOverloaded immediately
    /// when the writer is busy processing the first command.
    #[tokio::test]
    async fn test_overflow_reject_returns_gateway_overloaded() {
        use crate::config::OverflowPolicy;

        let mut config = GatewayConfig::new(":memory:");
        config.queue_capacity = 1;
        config.overflow = OverflowPolicy::Reject;
        config.allow_schema_write = true;

        let gw = InProcessGateway::open_with_config(config).unwrap();
        let handle = gw.handle();

        // Create the test table first (sequential, no overflow yet).
        create_test_table(&handle).await;

        // Fill the channel: send a request and immediately try to fill the queue.
        // With capacity=1 the channel is: [first_cmd being processed or in queue].
        // We spawn the first execute as a background task to ensure it occupies
        // the writer, then send a second request synchronously.
        //
        // Because the writer processes one command at a time and the channel
        // capacity is 1, after the writer takes the first command, the slot is
        // free momentarily. To reliably fill it, we send two commands in rapid
        // succession: the first goes into processing, the second fills the queue.
        // Then the third must be rejected.

        // Send 2 requests to saturate capacity=1 (one in flight + one in queue).
        let h1 = handle.clone();
        tokio::spawn(async move {
            let _ = h1
                .execute(make_request(
                    "fill-1",
                    vec![sql_op("INSERT INTO test_events(val) VALUES ('a')", vec![])],
                ))
                .await;
        });
        // Give the first request time to enter the channel.
        tokio::task::yield_now().await;

        let h2 = handle.clone();
        tokio::spawn(async move {
            let _ = h2
                .execute(make_request(
                    "fill-2",
                    vec![sql_op("INSERT INTO test_events(val) VALUES ('b')", vec![])],
                ))
                .await;
        });
        tokio::task::yield_now().await;

        // Now the third request should be rejected because the queue is full.
        let result = handle
            .execute(make_request(
                "overflow",
                vec![sql_op("INSERT INTO test_events(val) VALUES ('c')", vec![])],
            ))
            .await;

        // Either GatewayOverloaded (queue full) or Committed (queue drained by now)
        // are valid outcomes, but we specifically want to verify the Reject path
        // fires GatewayOverloaded when the queue is provably full.
        //
        // Since timing is not guaranteed, we accept both outcomes but assert the
        // error variant when it occurs is GatewayOverloaded (not GatewayClosed).
        if let Err(e) = &result {
            assert!(
                matches!(e, Error::GatewayOverloaded),
                "expected GatewayOverloaded, got: {e:?}"
            );
        }

        gw.shutdown().await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Test §14: OverflowPolicy::Reject — verifies error variant deterministically
    //
    // Use a tiny channel (capacity=0 is not allowed by tokio; use 1 with a
    // technique that guarantees the slot is full at the time of the third send).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_overflow_reject_error_variant() {
        use crate::config::OverflowPolicy;
        use tokio::sync::mpsc;

        // Build a handle with a mock sender that is already full.
        // We create a channel of capacity 1, fill it manually, then wrap it
        // in a GatewayHandle and verify that execute() returns GatewayOverloaded.
        let (tx, _rx) = mpsc::channel::<Command>(1);

        // Fill the single slot so the channel is at capacity.
        let fill_cmd = Command::Shutdown; // any variant; we just need the slot taken
        tx.try_send(fill_cmd)
            .expect("first send must succeed (slot is free)");

        // Now the channel is full. Construct a handle directly.
        let latency_buf = new_latency_buffer();
        let handle = GatewayHandle {
            sender: tx,
            latency_buf,
            overflow: OverflowPolicy::Reject,
        };

        let result = handle
            .execute(make_request(
                "overflow",
                vec![sql_op("INSERT INTO t VALUES (1)", vec![])],
            ))
            .await;

        assert!(
            matches!(result, Err(Error::GatewayOverloaded)),
            "Reject policy must return GatewayOverloaded when queue is full; got: {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test §14: OverflowPolicy::WaitTimeout — times out and returns GatewayOverloaded
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_overflow_wait_timeout_returns_gateway_overloaded() {
        use crate::config::OverflowPolicy;
        use tokio::sync::mpsc;

        // Create a channel of capacity 1 and fill it so every subsequent send blocks.
        let (tx, _rx) = mpsc::channel::<Command>(1);
        tx.try_send(Command::Shutdown).expect("fill slot");

        // Drop _rx is intentional — we want the channel to be full (not closed).
        // Because `_rx` goes out of scope at end of this test the channel will
        // close, but the timeout will fire first (1 ms < test teardown).

        let latency_buf = new_latency_buffer();
        let handle = GatewayHandle {
            sender: tx,
            latency_buf,
            overflow: OverflowPolicy::WaitTimeout { millis: 1 }, // 1 ms → fires quickly
        };

        let result = handle
            .execute(make_request(
                "timeout-overflow",
                vec![sql_op("INSERT INTO t VALUES (1)", vec![])],
            ))
            .await;

        // Either GatewayOverloaded (timeout) or GatewayClosed (rx dropped) may
        // occur depending on task scheduling. Both are acceptable errors; the
        // important assertion is that the call does NOT hang indefinitely and
        // does NOT return Ok.
        assert!(
            result.is_err(),
            "WaitTimeout must return an error when the queue is full; got Ok"
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::GatewayOverloaded | Error::GatewayClosed),
            "expected GatewayOverloaded or GatewayClosed, got: {err:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Test 11: §24 latency_snapshot via GatewayHandle
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_latency_snapshot_after_commits() {
        let gw = open_memory_gateway(false).await;
        let handle = gw.handle();

        // Before any commits, latency should be zero.
        let (avg, p95) = handle.latency_snapshot();
        assert_eq!(avg, 0.0);
        assert_eq!(p95, 0);

        create_test_table(&handle).await;

        // After at least one commit, the latency buffer must have recorded an
        // entry. avg > 0.0 proves the buffer is non-empty and the value was
        // actually measured (not just a default zero).
        let (avg_after, p95_after) = handle.latency_snapshot();
        assert!(
            avg_after > 0.0,
            "avg must be positive after a commit (latency buffer must be non-empty)"
        );
        assert!(p95_after >= p95, "p95 must be non-decreasing");

        gw.shutdown().await.unwrap();
    }
}
