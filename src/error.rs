use thiserror::Error;

/// Errors returned by the public SqueueLite API.
///
/// The library never leaks `anyhow` into its public surface; callers always get
/// a concrete [`Error`]. Examples and CLIs are free to use `anyhow` on top.
#[derive(Debug, Error)]
pub enum Error {
    /// An error originating from the underlying SQLite driver.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// A JSON (de)serialization error while handling a request payload.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Schema creation / migration failed.
    #[error("migration error: {0}")]
    Migration(String),

    /// A SQL statement was rejected before execution because it violates
    /// gateway constraints (e.g. BEGIN/COMMIT/ROLLBACK/PRAGMA are forbidden;
    /// §10 of the design specification).
    #[error("sql rejected: {0}")]
    SqlRejected(String),

    /// The gateway writer thread has stopped (channel closed or shutdown
    /// completed). Returned when a send or receive on the internal channel
    /// fails after the writer actor exits.
    #[error("gateway is closed")]
    GatewayClosed,

    /// An I/O error when reading from or writing to a Unix Domain Socket
    /// (sidecar mode, §18).
    #[error("io error: {0}")]
    Io(String),

    /// A protocol-level error in the JSON Lines exchange with the gateway
    /// (e.g. an unexpected error response from an admin command).
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Convenience alias for `Result<T, squeuelite::Error>`.
pub type Result<T> = std::result::Result<T, Error>;
