use std::path::PathBuf;

/// Configuration for a SqueueLite gateway instance (§16, §20.1).
///
/// Build via [`GatewayConfig::new`] for defaults, or construct directly for
/// full control. `socket_path` is intentionally absent — the in-process mode
/// does not use a Unix socket (that belongs to the sidecar phase, §18).
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Path to the SQLite database file. Use `":memory:"` for in-memory
    /// databases (useful in tests).
    pub db_path: PathBuf,
    /// SQLite journal mode (§16). Defaults to WAL.
    pub journal_mode: JournalMode,
    /// SQLite `synchronous` PRAGMA (§16). Defaults to `Normal`.
    pub synchronous: SyncMode,
    /// Capacity of the bounded mpsc channel between callers and the writer
    /// thread (§14). Defaults to `1024`.
    pub queue_capacity: usize,
    /// When `true` (the default), the gateway creates and writes to the
    /// `squeuelite_commits` internal table and returns a monotonically
    /// increasing `commit_seq` with each response (§12).
    ///
    /// Set to `false` to skip table creation and commit-seq tracking for
    /// maximum throughput.
    pub track_commits: bool,
    /// Milliseconds passed to `rusqlite::Connection::busy_timeout` (§16).
    /// Defaults to `5000`.
    pub busy_timeout_ms: u64,
    /// Whether to enforce `PRAGMA foreign_keys = ON` (§16). Defaults to
    /// `true`.
    pub foreign_keys: bool,

    // -----------------------------------------------------------------------
    // §13 Idempotency
    // -----------------------------------------------------------------------
    /// When `true` (the default), the gateway creates the `squeuelite_requests`
    /// table at startup and deduplicates writes by `idempotency_key` (§13).
    ///
    /// Set to `false` to skip idempotency tracking (e.g. for maximum-throughput
    /// scenarios where callers never retry). When `false`, any `idempotency_key`
    /// in a [`crate::request::WriteRequest`] is silently ignored.
    pub idempotency: bool,

    // -----------------------------------------------------------------------
    // §23 Security / Safety
    // -----------------------------------------------------------------------
    /// When `true` (the default), raw SQL via [`crate::request::SqlOperation`]
    /// is allowed. When `false`, **all** write operations are rejected because
    /// the MVP only supports raw SQL (§10). Future versions will add typed
    /// operations that can bypass this flag (§10 future extension). A comment
    /// is placed at the rejection site in `validate_sql`.
    pub allow_raw_sql: bool,

    /// When `true`, `CREATE`, `ALTER`, `DROP`, and `TRUNCATE` statements are
    /// allowed. Defaults to `false` (§23). Startup migration uses a direct
    /// connection that bypasses this flag; user-supplied SQL is subject to it.
    pub allow_schema_write: bool,

    /// When `true` (the default), `DELETE` statements are allowed (§23).
    /// Set to `false` to prevent callers from deleting rows.
    pub allow_delete: bool,

    /// When `true`, `DROP` statements are allowed. Defaults to `false` (§23).
    /// Note: `DROP` is also a schema-write operation; both `allow_schema_write`
    /// and `allow_drop` must be `true` to issue `DROP TABLE` etc.
    pub allow_drop: bool,

    // -----------------------------------------------------------------------
    // §14 Backpressure
    // -----------------------------------------------------------------------
    /// Behaviour when the bounded mpsc channel is full (§14).
    ///
    /// Defaults to [`OverflowPolicy::WaitTimeout`] with a 5000 ms timeout,
    /// matching the `busy_timeout_ms` default so that the caller gets a prompt
    /// `GatewayOverloaded` error rather than hanging indefinitely.
    pub overflow: OverflowPolicy,

    // -----------------------------------------------------------------------
    // §15 Batching
    // -----------------------------------------------------------------------
    /// Optional batch commit configuration (§15). Defaults to `None` (disabled).
    ///
    /// When `None`, each request is processed as an independent transaction
    /// (the MVP default, preferred for latency-sensitive workloads).
    /// When `Some(batch_config)`, the writer collects multiple single-op
    /// requests arriving in the same scheduling quantum and commits them in
    /// one outer transaction using SAVEPOINTs for per-request isolation.
    pub batch: Option<BatchConfig>,
}

/// Behaviour of `GatewayHandle::execute` when the bounded mpsc channel is
/// full (§14 Backpressure).
///
/// The design spec offers three options:
/// - **wait** — block until space is available (no timeout).
/// - **reject with overloaded** — return immediately with
///   [`crate::error::Error::GatewayOverloaded`].
/// - **timeout** — wait up to a deadline; if the queue is still full, return
///   [`crate::error::Error::GatewayOverloaded`].
///
/// The default is [`OverflowPolicy::WaitTimeout`] with 5000 ms, which matches
/// the `busy_timeout_ms` default and ensures callers get a deterministic error
/// rather than blocking forever under load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Block until a slot opens in the queue (no timeout).
    ///
    /// Equivalent to `tokio::sync::mpsc::Sender::send().await` with no timeout.
    /// Use when callers can tolerate indefinite backpressure.
    Wait,

    /// Return [`crate::error::Error::GatewayOverloaded`] immediately if the
    /// queue is full.
    ///
    /// Equivalent to `tokio::sync::mpsc::Sender::try_send()` (non-blocking).
    Reject,

    /// Wait up to `millis` milliseconds for a slot to open; return
    /// [`crate::error::Error::GatewayOverloaded`] if the timeout expires.
    WaitTimeout {
        /// Timeout in milliseconds.
        millis: u64,
    },
}

/// Configuration for the optional batch-commit optimisation (§15).
///
/// The writer drains the queue greedily after receiving the first request.
/// At most `max_size` requests are bundled per batch. Because the writer
/// uses `try_recv` (non-blocking), the effective delay is bounded by the
/// OS scheduler's wakeup latency rather than `max_delay_micros`; the field
/// is retained for API symmetry with the design spec but is not enforced
/// as a hard timer in the MVP batch implementation (§15 notes this is
/// acceptable for "greedy short-duration collection").
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of requests to bundle into one outer transaction (§15).
    /// Defaults to `64` (§15 `max_batch_size = 64`).
    pub max_size: usize,
    /// Maximum delay target in microseconds before committing a partial batch
    /// (§15 `max_batch_delay_micros = 500`). See struct-level note on MVP
    /// simplification.
    pub max_delay_micros: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 64,
            max_delay_micros: 500,
        }
    }
}

impl GatewayConfig {
    /// Create a configuration for the given database path with all other
    /// fields set to their defaults.
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            ..Default::default()
        }
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            // db_path has no sensible universal default; callers must supply one.
            // Using an empty string here makes the missing-path mistake obvious at
            // open time rather than silently creating an unexpected file.
            db_path: PathBuf::new(),
            journal_mode: JournalMode::Wal,
            synchronous: SyncMode::Normal,
            queue_capacity: 1024,
            track_commits: true,
            busy_timeout_ms: 5000,
            foreign_keys: true,
            // §13 — idempotency enabled by default.
            idempotency: true,
            // §14 — timeout-based backpressure by default (5000 ms matches busy_timeout_ms).
            overflow: OverflowPolicy::WaitTimeout { millis: 5000 },
            // §23 — default values from the design specification.
            allow_raw_sql: true,
            allow_schema_write: false,
            allow_delete: true,
            allow_drop: false,
            // §15 — batching disabled by default (latency-first default).
            batch: None,
        }
    }
}

/// SQLite journal mode (§16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// Write-Ahead Log — recommended for concurrent read / write workloads.
    Wal,
    /// Classic rollback journal. Use only when WAL is unavailable (e.g. on
    /// network filesystems).
    Delete,
}

impl JournalMode {
    /// Return the string value expected by the `PRAGMA journal_mode` statement.
    pub fn as_pragma_value(self) -> &'static str {
        match self {
            JournalMode::Wal => "WAL",
            JournalMode::Delete => "DELETE",
        }
    }
}

/// SQLite `synchronous` PRAGMA value (§16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// `NORMAL` — fast, safe for most workloads.
    Normal,
    /// `FULL` — stronger durability guarantee at the cost of throughput.
    Full,
}

impl SyncMode {
    /// Return the string value expected by the `PRAGMA synchronous` statement.
    pub fn as_pragma_value(self) -> &'static str {
        match self {
            SyncMode::Normal => "NORMAL",
            SyncMode::Full => "FULL",
        }
    }
}
