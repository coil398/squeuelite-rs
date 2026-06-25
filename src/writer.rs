//! In-process writer actor (§21, §28 of the design specification).
//!
//! This module is only compiled when the `inprocess` feature is enabled.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use base64::prelude::{Engine as _, BASE64_STANDARD};
use rusqlite::{types::Value, Connection, TransactionBehavior};
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
// LatencyBuffer — shared ring buffer for commit latency (§24)
// ---------------------------------------------------------------------------

/// Ring buffer storing per-commit latency measurements in microseconds (§24).
///
/// Capped at `LATENCY_BUF_CAP` entries; oldest entries are evicted when full.
pub(crate) const LATENCY_BUF_CAP: usize = 1024;

/// Shared ring buffer for commit latency (§24 avg_commit_latency / p95_commit_latency).
///
/// The writer pushes one entry per successful commit; `GatewayHandle` reads a
/// snapshot to compute avg / p95 on demand.
pub(crate) type LatencyBuffer = Arc<Mutex<VecDeque<u64>>>;

/// Create a new, empty [`LatencyBuffer`].
pub(crate) fn new_latency_buffer() -> LatencyBuffer {
    Arc::new(Mutex::new(VecDeque::with_capacity(LATENCY_BUF_CAP)))
}

/// Push a latency measurement into the ring buffer.
///
/// When the buffer is full, the oldest entry is popped first (FIFO eviction).
fn push_latency(buf: &LatencyBuffer, micros: u64) {
    if let Ok(mut guard) = buf.lock() {
        if guard.len() >= LATENCY_BUF_CAP {
            guard.pop_front();
        }
        guard.push_back(micros);
    }
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
    idempotency: bool,
    config: GatewayConfig,
    latency_buf: LatencyBuffer,
}

impl Writer {
    /// Create a new `Writer`.
    pub(crate) fn new(
        conn: Connection,
        receiver: mpsc::Receiver<Command>,
        track_commits: bool,
        idempotency: bool,
        config: GatewayConfig,
        latency_buf: LatencyBuffer,
    ) -> Self {
        Self {
            conn,
            receiver,
            track_commits,
            idempotency,
            config,
            latency_buf,
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
    ///
    /// ### Batching (§15)
    ///
    /// When `config.batch` is `Some(_)`, after receiving the first `Write`
    /// command the writer drains additional `Write` commands from the queue
    /// using `try_recv` (non-blocking, greedy). Only requests with a single
    /// operation are batched ("single-INSERT" class, §15). Multi-op requests,
    /// `Shutdown`, and `Checkpoint` commands encountered during drain terminate
    /// the drain phase; the drained multi-op request is applied individually
    /// after the batch commits, and control commands are handled in the next
    /// iteration.
    ///
    /// This greedy approach approximates §15's "collect for a short time" intent
    /// without a real timer: requests that arrive within the same OS scheduling
    /// quantum (typically a few hundred microseconds) are bundled together,
    /// which is the common case under high concurrency.
    pub(crate) fn run(mut self) {
        while let Some(cmd) = self.receiver.blocking_recv() {
            match cmd {
                Command::Write {
                    request,
                    respond_to,
                } => {
                    if self.config.batch.is_some() {
                        // §15 — batch mode: only single-op requests are batchable.
                        // A multi-op request represents an explicit transaction (§15)
                        // and must be processed individually to preserve semantics.
                        if request.operations.len() != 1 {
                            let response = self.apply(request);
                            let _ = respond_to.send(response);
                        } else {
                            // §15 — greedily collect additional single-op Write commands
                            // from the queue, then commit them together in one outer tx.
                            let mut batch: Vec<(WriteRequest, oneshot::Sender<WriteResponse>)> =
                                vec![(request, respond_to)];

                            let max_size =
                                self.config.batch.as_ref().map(|b| b.max_size).unwrap_or(64);

                            // `deferred_cmd` holds a non-batchable command that was
                            // popped from the queue during drain and must be processed
                            // after the batch commits.
                            let mut deferred_cmd: Option<Command> = None;

                            while batch.len() < max_size {
                                match self.receiver.try_recv() {
                                    Ok(Command::Write {
                                        request: r2,
                                        respond_to: rt2,
                                    }) => {
                                        if r2.operations.len() == 1 {
                                            // Single-op: batchable (§15 "single INSERT").
                                            batch.push((r2, rt2));
                                        } else {
                                            // Multi-op = explicit transaction (§15
                                            // "don't mix explicit transaction requests").
                                            // Apply this request individually after the
                                            // batch commits.
                                            deferred_cmd = Some(Command::Write {
                                                request: r2,
                                                respond_to: rt2,
                                            });
                                            break;
                                        }
                                    }
                                    Ok(ctrl @ (Command::Shutdown | Command::Checkpoint { .. })) => {
                                        // Control command: defer, finish batch first.
                                        deferred_cmd = Some(ctrl);
                                        break;
                                    }
                                    Err(_) => break, // Queue empty.
                                }
                            }

                            if batch.len() == 1 {
                                // Only one request collected — skip batch overhead,
                                // apply as a single transaction.
                                let (req, rt) = batch.pop().unwrap();
                                let response = self.apply(req);
                                let _ = rt.send(response);
                            } else {
                                self.apply_batch(batch);
                            }

                            // Process the deferred command if any.
                            if let Some(cmd) = deferred_cmd {
                                match cmd {
                                    Command::Write {
                                        request,
                                        respond_to,
                                    } => {
                                        let response = self.apply(request);
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
                        }
                    } else {
                        // Default (no batching): 1 request = 1 transaction.
                        let response = self.apply(request);
                        let _ = respond_to.send(response);
                    }
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
        let _ = self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
    }

    // -----------------------------------------------------------------------
    // apply — §11 transaction semantics + §12 commit_seq + §13 idempotency
    // -----------------------------------------------------------------------

    /// Execute one [`WriteRequest`] as a single `BEGIN IMMEDIATE` transaction
    /// (§11) and return a [`WriteResponse`].
    ///
    /// Flow:
    /// 1. Validate each operation's SQL (§10, §23).
    /// 2. If `idempotency_key` is set and `config.idempotency=true`, check
    ///    `squeuelite_requests` for a prior result (§13).
    /// 3. `BEGIN IMMEDIATE` (§11).
    /// 4. Execute each operation with bound params.
    /// 5. If `track_commits` is set, INSERT into `squeuelite_commits` (§12).
    /// 6. If idempotency is active, INSERT into `squeuelite_requests` (§13).
    /// 7. `COMMIT`. Record latency (§24).
    pub(crate) fn apply(&mut self, request: WriteRequest) -> WriteResponse {
        let req_id = request.request_id.clone();

        // Step 1 — SQL constraint checks (§10, §23, transaction not yet open).
        for op in &request.operations {
            if let Err(e) = validate_sql(&op.sql, &self.config) {
                return WriteResponse::failed(req_id, e.to_string());
            }
        }

        // Step 2 — Idempotency check (§13).
        // If `idempotency_key` is present and idempotency is enabled, query
        // the squeuelite_requests table before opening the write transaction.
        if self.idempotency {
            if let Some(ref key) = request.idempotency_key {
                let request_hash = match serde_json::to_string(&request.operations) {
                    Ok(s) => s,
                    Err(e) => return WriteResponse::failed(req_id, e.to_string()),
                };

                // Read-only query — no transaction needed.
                // §25 — prepare_cached: this SELECT runs once per request that
                // carries an idempotency_key, so caching the prepared plan is
                // worthwhile for high-throughput idempotency scenarios.
                let idem_lookup = self
                    .conn
                    .prepare_cached(
                        "SELECT request_hash, response_json \
                         FROM squeuelite_requests WHERE idempotency_key = ?1",
                    )
                    .and_then(|mut stmt| {
                        stmt.query_row(rusqlite::params![key], |row| {
                            let stored_hash: String = row.get(0)?;
                            let response_json: Option<String> = row.get(1)?;
                            Ok((stored_hash, response_json))
                        })
                    });
                match idem_lookup {
                    Ok((stored_hash, response_json)) => {
                        // Row exists.
                        if stored_hash == request_hash {
                            // §13 re-send: same hash → return stored response.
                            if let Some(json) = response_json {
                                if let Ok(resp) = serde_json::from_str::<WriteResponse>(&json) {
                                    return resp;
                                }
                            }
                            // Fallback: stored response missing or unparseable —
                            // treat as idempotent committed (conservative).
                            return WriteResponse::committed(req_id, None);
                        } else {
                            // §13 conflict: same key, different operations.
                            return WriteResponse::failed(
                                req_id,
                                Error::IdempotencyConflict(key.clone()).to_string(),
                            );
                        }
                    }
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        // First time we see this key — proceed with normal execution.
                    }
                    Err(e) => return WriteResponse::failed(req_id, e.to_string()),
                }

                // Normal execution path for a new idempotency key.
                return self.apply_with_idempotency(request, &request_hash);
            }
        }

        // No idempotency key (or idempotency disabled) — standard path.
        self.apply_inner(request)
    }

    /// Execute a request and record its result in `squeuelite_requests` (§13).
    ///
    /// Called only when `idempotency=true` and the key is new.
    fn apply_with_idempotency(
        &mut self,
        request: WriteRequest,
        request_hash: &str,
    ) -> WriteResponse {
        let key = request.idempotency_key.clone().unwrap_or_default();

        let response = self.apply_inner(request);

        // Only persist a successful commit in the idempotency table.
        // On failure the transaction was rolled back, so the key row must NOT
        // be inserted — a failed request is always retryable (§13: "失敗は再実行可能").
        if response.status == crate::request::WriteStatus::Committed {
            let response_json = serde_json::to_string(&response).unwrap_or_default();
            let commit_seq = response.commit_seq;
            // Insert outside the (already committed) transaction.
            // A separate write is fine: if this fails the client gets a Committed
            // response but the idempotency row is absent, meaning the next retry
            // will re-execute. This is a mild safety trade-off (at-most-once vs.
            // at-least-once); it favours correctness (never silently ignoring a
            // new request) over deduplication guarantees.
            //
            // A production implementation would include this INSERT inside the same
            // transaction as the write ops. For the MVP the simple two-phase approach
            // is sufficient.
            // §25 — prepare_cached for the fixed idempotency INSERT (called on
            // every successful commit with a new idempotency key).
            let _ = self
                .conn
                .prepare_cached(
                    "INSERT OR IGNORE INTO squeuelite_requests \
                     (idempotency_key, request_hash, status, response_json, commit_seq) \
                     VALUES (?1, ?2, 'committed', ?3, ?4)",
                )
                .and_then(|mut stmt| {
                    stmt.execute(rusqlite::params![
                        key,
                        request_hash,
                        response_json,
                        commit_seq
                    ])
                });
        }

        response
    }

    /// Core transaction execution logic for single-request paths.
    ///
    /// Opens a `BEGIN IMMEDIATE` transaction, executes all ops, and commits.
    /// Savepoint-based execution for batches is handled separately by
    /// [`Self::apply_savepoint`].
    fn apply_inner(&mut self, request: WriteRequest) -> WriteResponse {
        let req_id = request.request_id.clone();
        let start = Instant::now();

        // BEGIN IMMEDIATE (§11).
        let tx = match self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
        {
            Ok(tx) => tx,
            Err(e) => return WriteResponse::failed(req_id, e.to_string()),
        };

        // Execute each operation.
        // §25 — use `prepare_cached` so the compiled statement plan is reused
        // across transactions on the same Connection. The statement cache lives
        // on the Connection (not on the Transaction), so it survives COMMIT /
        // ROLLBACK and is effective for high-frequency repeated SQL patterns.
        for op in &request.operations {
            let params = match params_from(&op.params) {
                Ok(p) => p,
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            };
            let mut stmt = match tx.prepare_cached(&op.sql) {
                Ok(s) => s,
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            };
            if let Err(e) = stmt.execute(rusqlite::params_from_iter(params.iter())) {
                return WriteResponse::failed(req_id, e.to_string());
            }
        }

        // §12 — record commit.
        // §25 — prepare_cached for the fixed internal INSERT (high call frequency).
        let commit_seq: Option<i64> = if self.track_commits {
            let mut stmt = match tx.prepare_cached(
                "INSERT INTO squeuelite_commits(request_id, actor_id, run_id) VALUES (?, ?, ?)",
            ) {
                Ok(s) => s,
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            };
            let insert_result = stmt.execute(rusqlite::params![
                request.request_id,
                request.actor_id,
                request.run_id,
            ]);
            match insert_result {
                Ok(_) => Some(tx.last_insert_rowid()),
                Err(e) => return WriteResponse::failed(req_id, e.to_string()),
            }
        } else {
            None
        };

        // COMMIT.
        match tx.commit() {
            Ok(_) => {
                // §24 — record commit latency.
                let micros = start.elapsed().as_micros() as u64;
                push_latency(&self.latency_buf, micros);
                WriteResponse::committed(req_id, commit_seq)
            }
            Err(e) => WriteResponse::failed(req_id, e.to_string()),
        }
    }

    // -----------------------------------------------------------------------
    // apply_batch — §15 batch commit with per-request SAVEPOINTs
    // -----------------------------------------------------------------------

    /// Commit a batch of single-op requests inside one outer `BEGIN IMMEDIATE`
    /// transaction, using SAVEPOINTs for per-request isolation (§15).
    ///
    /// Design:
    /// - One outer `transaction_with_behavior(Immediate)` wraps all requests.
    /// - Each request gets its own savepoint (`SAVEPOINT sp_N`).
    /// - On success: `RELEASE sp_N` (commits the savepoint).
    /// - On failure: `ROLLBACK TO sp_N` then `RELEASE sp_N` (drops that
    ///   savepoint without rolling back the outer tx), so other requests are
    ///   unaffected (§15 "各requestを他に巻き込まない").
    /// - Idempotency-key requests in the batch follow §13 logic per savepoint;
    ///   the idempotency INSERT is included inside the same savepoint scope
    ///   (§15 "idempotency付きrequestは順序とresponse保存に注意").
    /// - After all requests are processed, the outer tx is COMMITted.
    /// - Each `respond_to` sender is notified with its individual result
    ///   (§15 "各requestへ個別responseを返す").
    ///
    /// Edge-case notes (§15 implementation comments):
    /// - A failed savepoint leaves a `commit_seq` gap (the AUTOINCREMENT counter
    ///   was not advanced for that request). This is expected and benign — gaps
    ///   in commit_seq are documented as possible by design.
    /// - An idempotency hit inside a batch (duplicate key, same hash) returns
    ///   the stored response immediately without opening a savepoint. A conflict
    ///   (same key, different hash) returns a Failed response the same way.
    fn apply_batch(&mut self, batch: Vec<(WriteRequest, oneshot::Sender<WriteResponse>)>) {
        let start = Instant::now();

        // Open the outer transaction.
        let tx = match self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
        {
            Ok(tx) => tx,
            Err(e) => {
                // If we can't open the outer tx, fail all requests in the batch.
                let err = e.to_string();
                for (req, rt) in batch {
                    let _ = rt.send(WriteResponse::failed(req.request_id.clone(), err.clone()));
                }
                return;
            }
        };

        let mut responses: Vec<(oneshot::Sender<WriteResponse>, WriteResponse)> = Vec::new();

        for (idx, (request, respond_to)) in batch.into_iter().enumerate() {
            let req_id = request.request_id.clone();

            // §10/§23 — SQL constraint checks (must match apply()'s check).
            // Validate before opening a savepoint so that a rejected request
            // results in a Failed response without touching the database.
            {
                let mut validation_err: Option<String> = None;
                for op in &request.operations {
                    if let Err(e) = validate_sql(&op.sql, &self.config) {
                        validation_err = Some(e.to_string());
                        break;
                    }
                }
                if let Some(err) = validation_err {
                    responses.push((respond_to, WriteResponse::failed(req_id, err)));
                    continue;
                }
            }

            // §13 — idempotency check within the batch.
            if self.idempotency {
                if let Some(ref key) = request.idempotency_key {
                    let request_hash = match serde_json::to_string(&request.operations) {
                        Ok(s) => s,
                        Err(e) => {
                            responses
                                .push((respond_to, WriteResponse::failed(req_id, e.to_string())));
                            continue;
                        }
                    };

                    // §25 — use the prepared-statement cache for the idempotency
                    // lookup, consistent with the non-batch write paths.
                    let lookup = tx
                        .prepare_cached(
                            "SELECT request_hash, response_json FROM squeuelite_requests WHERE idempotency_key = ?1",
                        )
                        .and_then(|mut stmt| {
                            stmt.query_row(rusqlite::params![key], |row| {
                                let h: String = row.get(0)?;
                                let r: Option<String> = row.get(1)?;
                                Ok((h, r))
                            })
                        });
                    match lookup {
                        Ok((stored_hash, response_json)) => {
                            if stored_hash == request_hash {
                                let resp = response_json
                                    .and_then(|j| serde_json::from_str::<WriteResponse>(&j).ok())
                                    .unwrap_or_else(|| {
                                        WriteResponse::committed(req_id.clone(), None)
                                    });
                                responses.push((respond_to, resp));
                                continue;
                            } else {
                                let resp = WriteResponse::failed(
                                    req_id,
                                    Error::IdempotencyConflict(key.clone()).to_string(),
                                );
                                responses.push((respond_to, resp));
                                continue;
                            }
                        }
                        Err(rusqlite::Error::QueryReturnedNoRows) => {
                            // New key — proceed.
                        }
                        Err(e) => {
                            responses
                                .push((respond_to, WriteResponse::failed(req_id, e.to_string())));
                            continue;
                        }
                    }

                    // Execute inside a savepoint with idempotency INSERT.
                    let sp_name = format!("sp_{idx}");
                    let sp_resp = Self::apply_savepoint(
                        &tx,
                        &request,
                        &sp_name,
                        self.track_commits,
                        Some((key, &request_hash)),
                    );
                    responses.push((respond_to, sp_resp));
                    continue;
                }
            }

            // No idempotency key — plain savepoint execution.
            let sp_name = format!("sp_{idx}");
            let sp_resp = Self::apply_savepoint(&tx, &request, &sp_name, self.track_commits, None);
            responses.push((respond_to, sp_resp));
        }

        // Commit the outer transaction.
        match tx.commit() {
            Ok(_) => {
                // §24 — record batch latency as one entry (entire batch).
                let micros = start.elapsed().as_micros() as u64;
                push_latency(&self.latency_buf, micros);
                // Send all individual responses.
                for (rt, resp) in responses {
                    let _ = rt.send(resp);
                }
            }
            Err(e) => {
                // Outer commit failed — fail all requests.
                let err = e.to_string();
                for (rt, resp) in responses {
                    // Failed savepoints already carry their own error; only
                    // override Committed responses (the outer commit rolled them back).
                    let final_resp = if resp.status == crate::request::WriteStatus::Committed {
                        WriteResponse::failed(resp.request_id, err.clone())
                    } else {
                        resp
                    };
                    let _ = rt.send(final_resp);
                }
            }
        }
    }

    /// Execute a single request inside a named SAVEPOINT within an existing
    /// outer transaction.
    ///
    /// `idempotency_record`: `Some((key, hash))` to also INSERT into
    /// `squeuelite_requests` inside this savepoint (§13 + §15 interaction).
    ///
    /// Returns the [`WriteResponse`] for this individual request.
    fn apply_savepoint(
        tx: &rusqlite::Transaction<'_>,
        request: &WriteRequest,
        sp_name: &str,
        track_commits: bool,
        idempotency_record: Option<(&str, &str)>,
    ) -> WriteResponse {
        let req_id = request.request_id.clone();

        // Open savepoint.
        if let Err(e) = tx.execute_batch(&format!("SAVEPOINT {sp_name};")) {
            return WriteResponse::failed(req_id, e.to_string());
        }

        // Helper macro to rollback-then-release on error.
        macro_rules! sp_fail {
            ($err:expr) => {{
                let _ = tx.execute_batch(&format!("ROLLBACK TO {sp_name}; RELEASE {sp_name};"));
                return WriteResponse::failed(req_id, $err.to_string());
            }};
        }

        // Execute each operation (batch only admits single-op requests, but
        // the function is general to handle future changes).
        // §25 — prepare_cached reuses compiled statement plans across savepoints.
        for op in &request.operations {
            let params = match params_from(&op.params) {
                Ok(p) => p,
                Err(e) => sp_fail!(e),
            };
            let mut stmt = match tx.prepare_cached(&op.sql) {
                Ok(s) => s,
                Err(e) => sp_fail!(e),
            };
            if let Err(e) = stmt.execute(rusqlite::params_from_iter(params.iter())) {
                sp_fail!(e);
            }
        }

        // §12 — commit tracking.
        // §25 — prepare_cached for the fixed internal INSERT.
        let commit_seq: Option<i64> = if track_commits {
            let mut stmt = match tx.prepare_cached(
                "INSERT INTO squeuelite_commits(request_id, actor_id, run_id) VALUES (?, ?, ?)",
            ) {
                Ok(s) => s,
                Err(e) => sp_fail!(e),
            };
            match stmt.execute(rusqlite::params![
                request.request_id,
                request.actor_id,
                request.run_id
            ]) {
                Ok(_) => Some(tx.last_insert_rowid()),
                Err(e) => sp_fail!(e),
            }
        } else {
            None
        };

        // §13 — idempotency record (if requested).
        // §25 — prepare_cached for the fixed internal INSERT OR IGNORE.
        if let Some((key, hash)) = idempotency_record {
            let resp_preview = WriteResponse::committed(req_id.clone(), commit_seq);
            let response_json = serde_json::to_string(&resp_preview).unwrap_or_default();
            let mut stmt = match tx.prepare_cached(
                "INSERT OR IGNORE INTO squeuelite_requests \
                 (idempotency_key, request_hash, status, response_json, commit_seq) \
                 VALUES (?1, ?2, 'committed', ?3, ?4)",
            ) {
                Ok(s) => s,
                Err(e) => sp_fail!(e),
            };
            if let Err(e) = stmt.execute(rusqlite::params![key, hash, response_json, commit_seq]) {
                sp_fail!(e);
            }
        }

        // Release (commit) the savepoint.
        if let Err(e) = tx.execute_batch(&format!("RELEASE {sp_name};")) {
            sp_fail!(e);
        }

        WriteResponse::committed(req_id, commit_seq)
    }
}

// ---------------------------------------------------------------------------
// SQL constraint enforcement (§10, §23)
// ---------------------------------------------------------------------------

/// Validate a SQL statement against gateway constraints (§10) and the
/// security / safety flags from [`GatewayConfig`] (§23).
///
/// Checks performed (in order):
///
/// 1. **`allow_raw_sql`** (§23): when `false`, all SQL is rejected because the
///    MVP only supports raw SQL. Future typed operations (§10) would bypass this.
/// 2. **Transaction control keywords** (§10): `BEGIN`, `COMMIT`, `ROLLBACK`,
///    `SAVEPOINT`, `RELEASE`, and `PRAGMA` are always forbidden regardless of
///    flags. The gateway owns the transaction lifecycle; callers must not alter it.
/// 3. **`allow_drop`** (§23): when `false`, statements starting with `DROP`
///    are rejected.
/// 4. **`allow_delete`** (§23): when `false`, statements starting with `DELETE`
///    are rejected.
/// 5. **`allow_schema_write`** (§23): when `false`, schema-mutating statements
///    (`CREATE`, `ALTER`, `DROP`, `TRUNCATE`) are rejected. Note that `DROP` may
///    also be caught by rule 3; both rules apply independently.
///
/// **Future work** (§23): operation / table allowlist (`allowlist.tables = …`)
/// is marked as a future item in the design spec and is intentionally not
/// implemented here. A comment below marks the insertion point.
///
/// **Implementation note**: only the *first token* of the trimmed SQL string is
/// checked. Comments at the very start (`-- …` / `/* … */`) and string literals
/// that contain a keyword are not detected — an intentional trade-off (a full
/// SQL parser is overkill given trusted in-process callers, §23).
pub(crate) fn validate_sql(sql: &str, config: &GatewayConfig) -> Result<()> {
    // Rule 1 — §23 allow_raw_sql: when false, reject all SQL (MVP: raw SQL is
    // the only operation type; future typed operations would bypass this flag,
    // §10 future extension).
    if !config.allow_raw_sql {
        return Err(Error::SqlRejected(
            "raw SQL is disabled (allow_raw_sql = false)".to_string(),
        ));
    }

    let first_token = sql
        .trim_start()
        .split_ascii_whitespace()
        .next()
        .unwrap_or("");
    let upper = first_token.to_ascii_uppercase();
    let token = upper.as_str();

    // Rule 2 — §10 transaction control keywords (always forbidden).
    if matches!(
        token,
        "BEGIN" | "COMMIT" | "ROLLBACK" | "SAVEPOINT" | "RELEASE" | "PRAGMA"
    ) {
        return Err(Error::SqlRejected(format!(
            "statement starts with forbidden keyword '{first_token}'"
        )));
    }

    // Rule 3 — §23 allow_drop.
    if token == "DROP" && !config.allow_drop {
        return Err(Error::SqlRejected(
            "DROP statements are disabled (allow_drop = false)".to_string(),
        ));
    }

    // Rule 4 — §23 allow_delete.
    if token == "DELETE" && !config.allow_delete {
        return Err(Error::SqlRejected(
            "DELETE statements are disabled (allow_delete = false)".to_string(),
        ));
    }

    // Rule 5 — §23 allow_schema_write (CREATE / ALTER / DROP / TRUNCATE).
    if matches!(token, "CREATE" | "ALTER" | "DROP" | "TRUNCATE") && !config.allow_schema_write {
        return Err(Error::SqlRejected(format!(
            "schema-write statements are disabled (allow_schema_write = false); \
             statement starts with '{first_token}'"
        )));
    }

    // Future §23 — operation / table allowlist (`allowlist.tables = [...]`).
    // The design spec marks this as a future item; no implementation yet.

    Ok(())
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
        // `{"$blob": "<base64>"}` sentinel: single-key object with "$blob" string value
        // → decode base64 and store as SQLite BLOB.
        // Any other object (multiple keys, "$blob" with non-string value, or no "$blob" key)
        // → fall through to JSON text serialisation (existing behaviour).
        serde_json::Value::Object(map)
            if map.len() == 1 && map.get("$blob").and_then(|b| b.as_str()).is_some() =>
        {
            let encoded = map["$blob"].as_str().unwrap();
            let bytes = BASE64_STANDARD
                .decode(encoded)
                .map_err(|e| Error::InvalidParam(format!("$blob base64 decode failed: {e}")))?;
            Value::Blob(bytes)
        }
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
/// When `idempotency` is `true` (the default, §13), the `squeuelite_requests`
/// table is created with the DDL from §13 of the design specification.
/// When `false`, no idempotency table is created and idempotency keys in
/// requests are silently ignored.
///
/// **Note**: startup migration runs on the writer's `Connection` directly,
/// before the writer thread starts accepting requests. It is NOT subject to
/// the `validate_sql` security checks (§23) — those apply only to user-supplied
/// SQL arriving via the write channel. Internal DDL is always permitted.
pub(crate) fn run_migrations(
    conn: &Connection,
    track_commits: bool,
    idempotency: bool,
) -> Result<()> {
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

    if idempotency {
        // §13 DDL — verbatim from the design specification.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS squeuelite_requests (
                idempotency_key TEXT PRIMARY KEY,
                request_hash    TEXT NOT NULL,
                status          TEXT NOT NULL,
                response_json   TEXT,
                commit_seq      INTEGER,
                created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    conn.pragma_update_and_check(
        None,
        "journal_mode",
        config.journal_mode.as_pragma_value(),
        |row| {
            let _actual_mode: String = row.get(0)?;
            // We intentionally do not error on WAL non-application (e.g. ":memory:"
            // returns "memory"). Callers that need strict WAL enforcement should use
            // a file-backed database. See tech-validation §4 for details.
            Ok(())
        },
    )?;

    // 2. busy_timeout — use the dedicated Connection method (tech-validation §2).
    conn.busy_timeout(Duration::from_millis(config.busy_timeout_ms))?;

    // 3. synchronous
    conn.pragma_update(None, "synchronous", config.synchronous.as_pragma_value())?;

    // 4. foreign_keys — use integer 1/0 (SQLite treats PRAGMA foreign_keys = 1
    //    identically to = ON; i64 aligns with rusqlite's ToSql integer path).
    conn.pragma_update(
        None,
        "foreign_keys",
        if config.foreign_keys { 1i64 } else { 0i64 },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests for writer-internal functions
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::GatewayConfig,
        request::{SqlOperation, WriteRequest},
    };
    use serde_json::json;

    fn default_config() -> GatewayConfig {
        GatewayConfig {
            allow_schema_write: true, // tests need to CREATE tables via validate_sql
            ..GatewayConfig::default()
        }
    }

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
    // validate_sql — keyword blocking (§10 forbidden keywords)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reject_begin() {
        let cfg = default_config();
        assert!(validate_sql("BEGIN", &cfg).is_err());
        assert!(validate_sql("  begin TRANSACTION", &cfg).is_err());
        assert!(validate_sql("BEGIN IMMEDIATE", &cfg).is_err());
    }

    #[test]
    fn test_reject_commit() {
        let cfg = default_config();
        assert!(validate_sql("COMMIT", &cfg).is_err());
        assert!(validate_sql("commit", &cfg).is_err());
    }

    #[test]
    fn test_reject_rollback() {
        let cfg = default_config();
        assert!(validate_sql("ROLLBACK", &cfg).is_err());
    }

    #[test]
    fn test_reject_savepoint() {
        let cfg = default_config();
        assert!(validate_sql("SAVEPOINT sp1", &cfg).is_err());
    }

    #[test]
    fn test_reject_release() {
        let cfg = default_config();
        assert!(validate_sql("RELEASE sp1", &cfg).is_err());
    }

    #[test]
    fn test_reject_pragma() {
        let cfg = default_config();
        assert!(validate_sql("PRAGMA journal_mode", &cfg).is_err());
        assert!(validate_sql("pragma journal_mode", &cfg).is_err());
    }

    #[test]
    fn test_allow_insert() {
        let cfg = default_config();
        assert!(validate_sql("INSERT INTO t(v) VALUES (1)", &cfg).is_ok());
    }

    #[test]
    fn test_allow_update() {
        let cfg = default_config();
        assert!(validate_sql("UPDATE t SET v = 1", &cfg).is_ok());
    }

    #[test]
    fn test_allow_delete_default() {
        // DELETE is allowed when allow_delete=true (default).
        let cfg = default_config();
        assert!(validate_sql("DELETE FROM t", &cfg).is_ok());
    }

    #[test]
    fn test_reject_delete_when_disabled() {
        // §23 allow_delete=false → DELETE must be rejected.
        let cfg = GatewayConfig {
            allow_delete: false,
            allow_schema_write: true,
            ..GatewayConfig::default()
        };
        assert!(validate_sql("DELETE FROM t", &cfg).is_err());
    }

    #[test]
    fn test_allow_drop_when_enabled() {
        // DROP allowed when both allow_drop=true and allow_schema_write=true.
        let cfg = GatewayConfig {
            allow_drop: true,
            allow_schema_write: true,
            ..GatewayConfig::default()
        };
        assert!(validate_sql("DROP TABLE t", &cfg).is_ok());
    }

    #[test]
    fn test_reject_drop_default() {
        // §23 allow_drop=false (default) → DROP must be rejected.
        let cfg = GatewayConfig {
            allow_schema_write: true, // allow_drop is false by default
            ..GatewayConfig::default()
        };
        assert!(validate_sql("DROP TABLE t", &cfg).is_err());
    }

    #[test]
    fn test_reject_create_when_schema_write_disabled() {
        // §23 allow_schema_write=false (default) → CREATE must be rejected.
        let cfg = GatewayConfig::default(); // allow_schema_write=false
        assert!(validate_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)", &cfg).is_err());
    }

    #[test]
    fn test_allow_create_when_schema_write_enabled() {
        let cfg = default_config(); // allow_schema_write=true
        assert!(validate_sql("CREATE TABLE t (id INTEGER PRIMARY KEY)", &cfg).is_ok());
    }

    #[test]
    fn test_reject_all_when_raw_sql_disabled() {
        // §23 allow_raw_sql=false → all SQL rejected.
        let cfg = GatewayConfig {
            allow_raw_sql: false,
            allow_schema_write: true,
            ..GatewayConfig::default()
        };
        assert!(validate_sql("INSERT INTO t VALUES (1)", &cfg).is_err());
        assert!(validate_sql("SELECT 1", &cfg).is_err());
    }

    #[test]
    fn test_mixed_case_begin() {
        // bEgIn should still be rejected (case-insensitive check).
        let cfg = default_config();
        assert!(validate_sql("bEgIn TRANSACTION", &cfg).is_err());
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
    // json_to_sqlite — $blob sentinel
    // -----------------------------------------------------------------------

    #[test]
    fn test_blob_sentinel_decodes_base64_to_blob() {
        // "aGVsbG8=" is base64 for b"hello"
        let result = json_to_sqlite(&json!({"$blob": "aGVsbG8="})).unwrap();
        assert_eq!(result, Value::Blob(b"hello".to_vec()));
    }

    #[test]
    fn test_blob_sentinel_invalid_base64_returns_err() {
        let result = json_to_sqlite(&json!({"$blob": "!!!notbase64!!!"}));
        assert!(result.is_err(), "invalid base64 must return Err");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid parameter"),
            "error must mention 'invalid parameter', got: {err_msg}"
        );
    }

    #[test]
    fn test_blob_sentinel_non_string_value_falls_through_to_text() {
        // {"$blob": 123} — value is not a string → treat as plain object (JSON text)
        let result = json_to_sqlite(&json!({"$blob": 123})).unwrap();
        assert_eq!(result, Value::Text(r#"{"$blob":123}"#.to_string()));
    }

    #[test]
    fn test_blob_sentinel_multiple_keys_falls_through_to_text() {
        // {"$blob": "...", "x": 1} — more than one key → not a sentinel
        let result = json_to_sqlite(&json!({"$blob": "aGVsbG8=", "x": 1})).unwrap();
        // Must be JSON text (key order from serde_json is insertion order for maps)
        assert!(
            matches!(result, Value::Text(_)),
            "multi-key object must be stored as Text(JSON)"
        );
        // Must NOT be a Blob
        assert!(
            !matches!(result, Value::Blob(_)),
            "multi-key object must not be decoded as Blob"
        );
    }

    // -----------------------------------------------------------------------
    // BLOB round-trip: write via gateway → read back as bytes
    // -----------------------------------------------------------------------

    #[test]
    fn test_blob_roundtrip_stored_as_blob_not_text() {
        let mut writer = make_test_writer();

        // Create a BLOB column table.
        writer
            .conn
            .execute_batch("CREATE TABLE blob_t (b BLOB);")
            .unwrap();

        // INSERT using the $blob sentinel (base64 of b"hello").
        let req = make_write_request(vec![(
            "INSERT INTO blob_t(b) VALUES (?)",
            vec![json!({"$blob": "aGVsbG8="})],
        )]);
        let resp = writer.apply(req);
        assert_eq!(
            resp.status,
            crate::request::WriteStatus::Committed,
            "blob INSERT must commit"
        );

        // Read back the raw bytes and the SQLite storage class.
        let (bytes, type_of): (Vec<u8>, String) = writer
            .conn
            .query_row("SELECT b, typeof(b) FROM blob_t", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();

        assert_eq!(bytes, b"hello", "stored bytes must match original data");
        assert_eq!(
            type_of, "blob",
            "SQLite typeof() must be 'blob', not 'text' — data was not stored as JSON string"
        );
    }

    // -----------------------------------------------------------------------
    // apply — atomic rollback (unit test via direct Writer call)
    // -----------------------------------------------------------------------

    fn make_test_writer() -> Writer {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL);")
            .unwrap();

        let (_, rx) = mpsc::channel(1);
        let config = GatewayConfig {
            allow_schema_write: true,
            ..GatewayConfig::default()
        };
        let latency_buf = new_latency_buffer();
        Writer::new(conn, rx, false, true, config, latency_buf)
    }

    #[test]
    fn test_apply_atomic_rollback() {
        let mut writer = make_test_writer();

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

    // -----------------------------------------------------------------------
    // §13 Idempotency tests
    // -----------------------------------------------------------------------

    fn make_idempotency_writer() -> Writer {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn, true, true).unwrap();
        conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT NOT NULL);")
            .unwrap();

        let (_, rx) = mpsc::channel(1);
        let config = GatewayConfig {
            allow_schema_write: true,
            idempotency: true,
            track_commits: true,
            ..GatewayConfig::default()
        };
        let latency_buf = new_latency_buffer();
        Writer::new(conn, rx, true, true, config, latency_buf)
    }

    #[test]
    fn test_idempotency_second_call_returns_same_response() {
        let mut writer = make_idempotency_writer();

        let ops = vec![("INSERT INTO t(val) VALUES (?)", vec![json!("hello")])];
        let mut req = make_write_request(ops);
        req.request_id = "req-idem-1".to_string();
        req.idempotency_key = Some("key-1".to_string());

        // First call: executes and commits.
        let resp1 = writer.apply(req.clone());
        assert_eq!(resp1.status, crate::request::WriteStatus::Committed);

        // Second call with same idempotency_key and same operations: must return
        // the stored response without re-executing.
        let resp2 = writer.apply(req.clone());
        assert_eq!(resp2.status, crate::request::WriteStatus::Committed);

        // The row must only appear once (idempotency prevented double-insert).
        let count: i64 = writer
            .conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "idempotency must prevent double-insert");
    }

    #[test]
    fn test_idempotency_conflict_different_operations() {
        let mut writer = make_idempotency_writer();

        let mut req1 = make_write_request(vec![(
            "INSERT INTO t(val) VALUES (?)",
            vec![json!("first")],
        )]);
        req1.request_id = "req-conflict-1".to_string();
        req1.idempotency_key = Some("conflict-key".to_string());

        let resp1 = writer.apply(req1);
        assert_eq!(resp1.status, crate::request::WriteStatus::Committed);

        // Second request with the same key but different operations → conflict.
        let mut req2 = make_write_request(vec![(
            "INSERT INTO t(val) VALUES (?)",
            vec![json!("different")],
        )]);
        req2.request_id = "req-conflict-2".to_string();
        req2.idempotency_key = Some("conflict-key".to_string());

        let resp2 = writer.apply(req2);
        assert_eq!(resp2.status, crate::request::WriteStatus::Failed);
        let err = resp2.error.expect("error must be set on conflict");
        assert!(
            err.contains("idempotency conflict"),
            "expected 'idempotency conflict' in error, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // §24 latency buffer
    // -----------------------------------------------------------------------

    #[test]
    fn test_latency_buffer_records_commits() {
        let mut writer = make_test_writer();
        assert_eq!(writer.latency_buf.lock().unwrap().len(), 0);

        let req = make_write_request(vec![(
            "INSERT INTO t(val) VALUES (?)",
            vec![json!("lat-test")],
        )]);
        let resp = writer.apply(req);
        assert_eq!(resp.status, crate::request::WriteStatus::Committed);

        assert_eq!(
            writer.latency_buf.lock().unwrap().len(),
            1,
            "one latency entry must be recorded per successful commit"
        );
    }

    // -----------------------------------------------------------------------
    // §15 batching — SAVEPOINT isolation unit tests
    // -----------------------------------------------------------------------

    /// Build a Writer with batch mode enabled (max_size=64) and a test table.
    /// This allows direct invocation of `apply_batch` without going through
    /// `Writer::run`, avoiding timing-dependent batch collection.
    fn make_batch_writer() -> Writer {
        let conn = Connection::open_in_memory().unwrap();
        // `run_migrations` is not called here to keep idempotency=false and
        // avoid squeuelite_commits table creation (track_commits=false).
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT NOT NULL UNIQUE);",
        )
        .unwrap();

        let (_, rx) = mpsc::channel(1);
        let config = GatewayConfig {
            allow_schema_write: true,
            batch: Some(crate::config::BatchConfig {
                max_size: 64,
                max_delay_micros: 500,
            }),
            ..GatewayConfig::default()
        };
        let latency_buf = new_latency_buffer();
        Writer::new(conn, rx, false, false, config, latency_buf)
    }

    /// §15 core guarantee: when one request in a batch fails (UNIQUE constraint
    /// violation), its SAVEPOINT is rolled back while the others are committed.
    #[test]
    fn test_batch_savepoint_isolation_one_failure_others_committed() {
        let mut writer = make_batch_writer();

        // Build a batch directly: 3 requests, where req-2 is a duplicate of
        // req-1 (UNIQUE constraint on `val`), causing req-2 to fail while
        // req-1 and req-3 must remain committed.
        //
        // We call `apply_batch` directly to bypass the run-loop's greedy drain
        // (which is timing-dependent). This is the correct unit-test approach
        // for verifying SAVEPOINT isolation without flakiness.
        let (tx1, mut rx1) = oneshot::channel::<WriteResponse>();
        let req1 = WriteRequest {
            request_id: "req-1".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: vec![crate::request::SqlOperation {
                sql: "INSERT INTO t(val) VALUES (?)".to_string(),
                params: vec![json!("apple")],
            }],
        };

        let (tx2, mut rx2) = oneshot::channel::<WriteResponse>();
        let req2 = WriteRequest {
            request_id: "req-2".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: vec![crate::request::SqlOperation {
                // Duplicate value — triggers UNIQUE constraint failure.
                sql: "INSERT INTO t(val) VALUES (?)".to_string(),
                params: vec![json!("apple")],
            }],
        };

        let (tx3, mut rx3) = oneshot::channel::<WriteResponse>();
        let req3 = WriteRequest {
            request_id: "req-3".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: vec![crate::request::SqlOperation {
                sql: "INSERT INTO t(val) VALUES (?)".to_string(),
                params: vec![json!("banana")],
            }],
        };

        writer.apply_batch(vec![(req1, tx1), (req2, tx2), (req3, tx3)]);

        // Collect responses (non-blocking — apply_batch is synchronous and
        // sends before returning).
        let resp1 = rx1.try_recv().expect("response for req-1 must be sent");
        let resp2 = rx2.try_recv().expect("response for req-2 must be sent");
        let resp3 = rx3.try_recv().expect("response for req-3 must be sent");

        assert_eq!(
            resp1.status,
            crate::request::WriteStatus::Committed,
            "req-1 (first INSERT) must be Committed"
        );
        assert_eq!(
            resp2.status,
            crate::request::WriteStatus::Failed,
            "req-2 (duplicate UNIQUE) must be Failed"
        );
        assert_eq!(
            resp3.status,
            crate::request::WriteStatus::Committed,
            "req-3 must be Committed — req-2's failure must not affect it (§15 SAVEPOINT isolation)"
        );

        // Verify that the database reflects the expected state:
        // req-1 and req-3 committed; req-2 rolled back.
        let count: i64 = writer
            .conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 2,
            "exactly 2 rows must exist after the batch (req-1 and req-3)"
        );

        let vals: Vec<String> = {
            let mut stmt = writer
                .conn
                .prepare("SELECT val FROM t ORDER BY val")
                .unwrap();
            stmt.query_map([], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            vals,
            vec!["apple", "banana"],
            "only 'apple' and 'banana' must be present"
        );
    }

    /// §10/§23 — validate_sql must be applied in the batch path as well.
    /// A batch containing a forbidden SQL statement (BEGIN) must fail that
    /// individual request while committing others.
    #[test]
    fn test_batch_validate_sql_applied_for_each_request() {
        let mut writer = make_batch_writer();

        let (tx1, mut rx1) = oneshot::channel::<WriteResponse>();
        let req1 = WriteRequest {
            request_id: "req-valid".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: vec![crate::request::SqlOperation {
                sql: "INSERT INTO t(val) VALUES (?)".to_string(),
                params: vec![json!("ok")],
            }],
        };

        let (tx2, mut rx2) = oneshot::channel::<WriteResponse>();
        let req2 = WriteRequest {
            request_id: "req-forbidden".to_string(),
            actor_id: "test-actor".to_string(),
            run_id: None,
            idempotency_key: None,
            operations: vec![crate::request::SqlOperation {
                // BEGIN is forbidden (§10 transaction control keywords).
                sql: "BEGIN".to_string(),
                params: vec![],
            }],
        };

        writer.apply_batch(vec![(req1, tx1), (req2, tx2)]);

        let resp1 = rx1.try_recv().expect("response for req-valid must be sent");
        let resp2 = rx2
            .try_recv()
            .expect("response for req-forbidden must be sent");

        assert_eq!(
            resp1.status,
            crate::request::WriteStatus::Committed,
            "req-valid must be Committed"
        );
        assert_eq!(
            resp2.status,
            crate::request::WriteStatus::Failed,
            "req-forbidden (BEGIN keyword) must be Failed due to validate_sql (§10/§23)"
        );
        assert!(
            resp2
                .error
                .as_deref()
                .unwrap_or("")
                .contains("forbidden keyword"),
            "error message must mention the forbidden keyword"
        );

        // The valid request's row must be committed.
        let count: i64 = writer
            .conn
            .query_row("SELECT COUNT(*) FROM t", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "only the valid request must have inserted a row");
    }
}
