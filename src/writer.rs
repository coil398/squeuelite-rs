//! In-process writer actor (§21, §28 of the design specification).
//!
//! This module is only compiled when the `inprocess` feature is enabled.

use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior, types::Value};
use tokio::sync::{mpsc, oneshot};

use crate::{
    config::GatewayConfig,
    error::{Error, Result},
    request::{WriteRequest, WriteResponse},
};

// ---------------------------------------------------------------------------
// Command enum (§28)
// ---------------------------------------------------------------------------

/// Messages sent from [`crate::inprocess::GatewayHandle`] to the writer thread.
pub(crate) enum Command {
    /// Execute a write request atomically and reply on the oneshot channel.
    Write {
        request: WriteRequest,
        respond_to: oneshot::Sender<WriteResponse>,
    },
    /// Ask the writer thread to finish processing, run the shutdown WAL
    /// checkpoint, and then exit its loop.
    Shutdown,
    /// Run `PRAGMA wal_checkpoint(TRUNCATE)` and reply with the result (§24 admin).
    Checkpoint {
        respond_to: oneshot::Sender<Result<()>>,
    },
}

// ---------------------------------------------------------------------------
// Writer struct (§28)
// ---------------------------------------------------------------------------

/// The actor that owns the single `rusqlite::Connection` and processes
/// `Command` messages from the bounded mpsc channel.
///
/// `rusqlite::Connection` is `Send` but `!Sync`, so it must live entirely
/// inside this struct, which runs on its own OS thread.
pub(crate) struct Writer {
    conn: Connection,
    receiver: mpsc::Receiver<Command>,
    track_commits: bool,
}

impl Writer {
    /// Create a new `Writer`.
    pub(crate) fn new(
        conn: Connection,
        receiver: mpsc::Receiver<Command>,
        track_commits: bool,
    ) -> Self {
        Self {
            conn,
            receiver,
            track_commits,
        }
    }

    /// Run the writer loop (§28).
    ///
    /// Blocks the calling thread until either:
    /// - a [`Command::Shutdown`] is received, or
    /// - all senders are dropped (i.e. `blocking_recv()` returns `None`).
    ///
    /// After the loop exits, a WAL checkpoint is attempted to minimise the
    /// amount of WAL data left on disk (§16 shutdown checkpoint).
    pub(crate) fn run(mut self) {
        while let Some(cmd) = self.receiver.blocking_recv() {
            match cmd {
                Command::Write {
                    request,
                    respond_to,
                } => {
                    let response = self.apply(request);
                    // If the caller dropped its oneshot receiver we simply
                    // discard the response; the writer loop must continue.
                    let _ = respond_to.send(response);
                }
                Command::Shutdown => break,
                Command::Checkpoint { respond_to } => {
                    let result = self
                        .conn
                        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                        .map_err(Error::Sqlite);
                    let _ = respond_to.send(result);
                }
            }
        }

        // §16 shutdown checkpoint — best effort; ignore errors.
        let _ = self
            .conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    }

    // -----------------------------------------------------------------------
    // apply — §11 transaction semantics + §12 commit_seq
    // -----------------------------------------------------------------------

    /// Execute one [`WriteRequest`] as a single `BEGIN IMMEDIATE` transaction
    /// (§11) and return a [`WriteResponse`].
    ///
    /// Flow:
    /// 1. Validate each operation's SQL prefix (§10).
    /// 2. `BEGIN IMMEDIATE` (§11).
    /// 3. Execute each operation with bound params.
    /// 4. If `track_commits` is set, INSERT into `squeuelite_commits` (§12).
    /// 5. `COMMIT`.
    fn apply(&mut self, request: WriteRequest) -> WriteResponse {
        let req_id = request.request_id.clone();

        // Step 1 — SQL constraint checks (§10, transaction not yet open).
        for op in &request.operations {
            if let Err(e) = reject_forbidden_sql(&op.sql) {
                return WriteResponse::failed(req_id, e.to_string());
            }
        }

        // Step 2 — BEGIN IMMEDIATE (§11, tech-validation confirmed API).
        let tx = match self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
        {
            Ok(tx) => tx,
            Err(e) => return WriteResponse::failed(req_id, e.to_string()),
        };

        // Step 3 — execute each operation.
        for op in &request.operations {
            let params = match params_from(&op.params) {
                Ok(p) => p,
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            };
            // rusqlite::execute() rejects multiple statements automatically
            // (returns Error::MultipleStatement), fulfilling §10's "no multiple
            // statements" constraint without additional application-level checks.
            if let Err(e) = tx.execute(&op.sql, rusqlite::params_from_iter(params.iter())) {
                return WriteResponse::failed(req_id, e.to_string());
            }
        }

        // Step 4 — record commit (§12).
        let commit_seq: Option<i64> = if self.track_commits {
            let insert_result = tx.execute(
                "INSERT INTO squeuelite_commits(request_id, actor_id, run_id) VALUES (?, ?, ?)",
                rusqlite::params![
                    request.request_id,
                    request.actor_id,
                    request.run_id,
                ],
            );
            match insert_result {
                Ok(_) => Some(tx.last_insert_rowid()),
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            }
        } else {
            None
        };

        // Step 5 — COMMIT.
        match tx.commit() {
            Ok(_) => WriteResponse::committed(req_id, commit_seq),
            Err(e) => WriteResponse::failed(req_id, e.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// SQL constraint enforcement (§10)
// ---------------------------------------------------------------------------

/// Reject SQL statements whose first token is a forbidden keyword (§10).
///
/// The gateway owns the transaction lifecycle; allowing callers to issue
/// `BEGIN`, `COMMIT`, `ROLLBACK`, or `SAVEPOINT` would break the 1-request =
/// 1-transaction guarantee. `PRAGMA` is similarly forbidden because gateway
/// configuration is managed exclusively at startup.
///
/// **`DELETE` and `DROP` are intentionally not in the forbidden list** (§23
/// MVP decision). The current MVP assumes trusted in-process agents, so
/// destructive operations are permitted. Future versions may add
/// `allow_delete` / `allow_drop` flags to [`crate::config::GatewayConfig`]
/// to give callers explicit control (§10 future extension).
///
/// **Implementation note**: only the *first token* of the (trimmed) SQL string
/// is checked. Comments at the very start (`-- ...` or `/* */`) and string
/// literals that happen to contain a forbidden keyword are *not* detected. This
/// is an intentional trade-off: a complete SQL parser would add significant
/// complexity for marginal security benefit given that callers are trusted
/// in-process agents (§23).
fn reject_forbidden_sql(sql: &str) -> Result<()> {
    let first_token = sql
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("");

    let forbidden = matches!(
        first_token.to_ascii_uppercase().as_str(),
        "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "PRAGMA"
    );

    if forbidden {
        Err(Error::SqlRejected(format!(
            "statement starts with forbidden keyword '{first_token}'"
        )))
    } else {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// params_from — serde_json::Value → rusqlite::types::Value (§6 / plan §6)
// ---------------------------------------------------------------------------

/// Convert a slice of [`serde_json::Value`] into [`rusqlite::types::Value`]
/// for use with `params_from_iter` (tech-validation §6 conversion table).
///
/// - `Null` → `Value::Null`
/// - `Bool(b)` → `Value::Integer(b as i64)`
/// - `Number` that fits in i64 → `Value::Integer`
/// - `Number` otherwise → `Value::Real(f64)`
/// - `String(s)` → `Value::Text(s)`
/// - `Array` / `Object` → `Value::Text(serde_json::to_string(v)?)` (JSON
///   serialised — SQLite has no native array/object type; §8's payload
///   `"{\"ok\":true}"` example shows the expected pattern)
pub(crate) fn params_from(values: &[serde_json::Value]) -> Result<Vec<Value>> {
    values.iter().map(json_to_sqlite).collect()
}

fn json_to_sqlite(v: &serde_json::Value) -> Result<Value> {
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Real(f)
            } else {
                // Extremely unlikely (only NaN / Inf, which serde_json rejects),
                // but fall back to text to avoid panicking.
                Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Value::Text(s.clone()),
        other => Value::Text(serde_json::to_string(other)?),
    })
}

// ---------------------------------------------------------------------------
// Migration (§17 case A — startup migration only)
// ---------------------------------------------------------------------------

/// Create internal tables required by the gateway at startup (§17 case A).
///
/// When `track_commits` is `false` the `squeuelite_commits` table is **not**
/// created, saving the overhead for high-throughput use cases.
///
/// The `squeuelite_requests` idempotency table (§13) is intentionally omitted
/// in the in-process MVP; it will be added in the sidecar phase.
pub(crate) fn run_migrations(conn: &Connection, track_commits: bool) -> Result<()> {
    if track_commits {
        // §12 DDL — verbatim from the design specification.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS squeuelite_commits (
                seq          INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id   TEXT NOT NULL,
                actor_id     TEXT NOT NULL,
                run_id       TEXT,
                committed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );",
        )
        .map_err(|e| Error::Migration(e.to_string()))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// PRAGMA application (§16)
// ---------------------------------------------------------------------------

/// Apply the gateway's SQLite PRAGMA settings in the recommended order (§16).
///
/// Order: journal_mode → busy_timeout → synchronous → foreign_keys.
/// `journal_mode` is applied with `pragma_update_and_check` so that the actual
/// resulting mode can be inspected (in-memory DBs do not support WAL and will
/// return `"memory"` instead; tests should tolerate this).
pub(crate) fn apply_pragmas(conn: &Connection, config: &GatewayConfig) -> Result<()> {
    // 1. journal_mode — check the applied value (may differ for in-memory DBs).
    conn.pragma_update_and_check(None, "journal_mode", config.journal_mode.as_pragma_value(), |row| {
        let _actual_mode: String = row.get(0)?;
        // We intentionally do not error on WAL non-application (e.g. ":memory:"
        // returns "memory"). Callers that need strict WAL enforcement should use
        // a file-backed database. See tech-validation §4 for details.
        Ok(())
    })?;

    // 2. busy_timeout — use the dedicated Connection method (tech-validation §2).
    conn.busy_timeout(Duration::from_millis(config.busy_timeout_ms))?;

    // 3. synchronous
    conn.pragma_update(None, "synchronous", config.synchronous.as_pragma_value())?;

    // 4. foreign_keys — use integer 1/0 (SQLite treats PRAGMA foreign_keys = 1
    //    identically to = ON; i64 aligns with rusqlite's ToSql integer path).
    conn.pragma_update(None, "foreign_keys", if config.foreign_keys { 1i64 } else { 0i64 })?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests for writer-internal functions
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{SqlOperation, WriteRequest};
    use serde_json::json;

    fn make_write_request(ops: Vec<(&str, Vec<serde_json::Value>)>) -> WriteRequest {
        WriteRequest {
            request_id: "test-req".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: ops
                .into_iter()
                .map(|(sql, params)| SqlOperation {
                    sql: sql.to_string(),
                    params,
                })
                .collect(),
        }
    }

    // -----------------------------------------------------------------------
    // reject_forbidden_sql — keyword blocking
    // -----------------------------------------------------------------------

    #[test]
    fn test_reject_begin() {
        assert!(reject_forbidden_sql("BEGIN").is_err());
        assert!(reject_forbidden_sql("  begin TRANSACTION").is_err());
        assert!(reject_forbidden_sql("BEGIN IMMEDIATE").is_err());
    }

    #[test]
    fn test_reject_commit() {
        assert!(reject_forbidden_sql("COMMIT").is_err());
        assert!(reject_forbidden_sql("commit").is_err());
    }

    #[test]
    fn test_reject_rollback() {
        assert!(reject_forbidden_sql("ROLLBACK").is_err());
    }

    #[test]
    fn test_reject_savepoint() {
        assert!(reject_forbidden_sql("SAVEPOINT sp1").is_err());
    }

    #[test]
    fn test_reject_release() {
        assert!(reject_forbidden_sql("RELEASE sp1").is_err());
    }

    #[test]
    fn test_reject_pragma() {
        assert!(reject_forbidden_sql("PRAGMA journal_mode").is_err());
        assert!(reject_forbidden_sql("pragma journal_mode").is_err());
    }

    #[test]
    fn test_allow_insert() {
        assert!(reject_forbidden_sql("INSERT INTO t(v) VALUES (1)").is_ok());
    }

    #[test]
    fn test_allow_update() {
        assert!(reject_forbidden_sql("UPDATE t SET v = 1").is_ok());
    }

    #[test]
    fn test_allow_delete() {
        // DELETE is intentionally NOT blocked in the MVP (§23, trusted agents).
        assert!(reject_forbidden_sql("DELETE FROM t").is_ok());
    }

    #[test]
    fn test_allow_drop() {
        // DROP is intentionally NOT blocked in the MVP (§23, trusted agents).
        assert!(reject_forbidden_sql("DROP TABLE t").is_ok());
    }

    #[test]
    fn test_allow_create_table() {
        assert!(reject_forbidden_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)").is_ok());
    }

    #[test]
    fn test_mixed_case_begin() {
        // bEgIn should still be rejected (case-insensitive check).
        assert!(reject_forbidden_sql("bEgIn TRANSACTION").is_err());
    }

    // -----------------------------------------------------------------------
    // json_to_sqlite — type conversion
    // -----------------------------------------------------------------------

    #[test]
    fn test_json_null_converts_to_sqlite_null() {
        let result = json_to_sqlite(&json!(null)).unwrap();
        assert!(matches!(result, Value::Null));
    }

    #[test]
    fn test_json_bool_true_converts_to_integer_1() {
        let result = json_to_sqlite(&json!(true)).unwrap();
        assert_eq!(result, Value::Integer(1));
    }

    #[test]
    fn test_json_bool_false_converts_to_integer_0() {
        let result = json_to_sqlite(&json!(false)).unwrap();
        assert_eq!(result, Value::Integer(0));
    }

    #[test]
    fn test_json_integer_converts_to_sqlite_integer() {
        let result = json_to_sqlite(&json!(42i64)).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_json_float_converts_to_sqlite_real() {
        // Use a value that is not close to any well-known constant (e.g. π).
        let result = json_to_sqlite(&json!(2.5f64)).unwrap();
        assert_eq!(result, Value::Real(2.5));
    }

    #[test]
    fn test_json_string_converts_to_sqlite_text() {
        let result = json_to_sqlite(&json!("hello")).unwrap();
        assert_eq!(result, Value::Text("hello".to_string()));
    }

    #[test]
    fn test_json_array_converts_to_sqlite_text_json() {
        let result = json_to_sqlite(&json!(["a", "b"])).unwrap();
        assert_eq!(result, Value::Text(r#"["a","b"]"#.to_string()));
    }

    #[test]
    fn test_json_object_converts_to_sqlite_text_json() {
        let result = json_to_sqlite(&json!({"key": "val"})).unwrap();
        assert_eq!(result, Value::Text(r#"{"key":"val"}"#.to_string()));
    }

    // -----------------------------------------------------------------------
    // apply — atomic rollback (unit test via direct Writer call)
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_atomic_rollback() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL);",
        )
        .unwrap();

        let (_, rx) = mpsc::channel(1);
        let mut writer = Writer::new(conn, rx, false);

        // op1 succeeds (valid), op2 fails (NULL into NOT NULL col) → rollback.
        let req = make_write_request(vec![
            ("INSERT INTO t(val) VALUES (?)", vec![json!("good")]),
            ("INSERT INTO t(val) VALUES (?)", vec![json!(null)]),
        ]);

        let resp = writer.apply(req);
        assert_eq!(
            resp.status,
            crate::request::WriteStatus::Failed,
            "transaction must fail on constraint violation"
        );

        // Verify op1 was also rolled back: table must be empty.
        let count: i64 = writer
            .conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rollback must leave the table empty");
    }
}
