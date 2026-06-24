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
