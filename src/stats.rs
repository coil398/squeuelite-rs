//! Basic gateway statistics (§24 Observability).
//!
//! This module is only compiled when the `sidecar` feature is enabled.
//!
//! Tracks accepted/committed/failed/rejected request counts using atomic
//! counters that are safe to share across async tasks. A `StatsSnapshot`
//! can be requested at any time via the admin `{ "type": "stats" }` command
//! (§24 JSON Lines admin interface).
//!
//! **Not included (future work)**: avg/p95 commit latency. These require
//! a sliding-window histogram and go beyond "basic" stats for the MVP (§27).

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Stats — shared atomic counters
// ---------------------------------------------------------------------------

/// Shared atomic counters for gateway-level statistics (§24).
///
/// Clone an `Arc<Stats>` to share across the accept loop and per-connection
/// tasks.
#[derive(Debug, Default)]
pub(crate) struct Stats {
    /// Requests received from clients (before channel send).
    pub accepted: AtomicU64,
    /// Requests that committed successfully.
    pub committed: AtomicU64,
    /// Requests that were executed but failed (SQLite error / constraint).
    pub failed: AtomicU64,
    /// Requests dropped because the gateway channel was full or closed.
    pub rejected: AtomicU64,
}

impl Stats {
    /// Create a new, zero-initialised `Stats` wrapped in an `Arc`.
    pub(crate) fn new_arc() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Capture a point-in-time snapshot (§24 `StatsSnapshot`).
    ///
    /// `queue_depth` and `queue_capacity` come from the `GatewayHandle`
    /// channel introspection (§14 backpressure). `current_wal_size_bytes`
    /// is fetched best-effort from the filesystem (None for `:memory:` or
    /// when WAL is disabled).
    pub(crate) fn snapshot(
        &self,
        queue_depth: usize,
        queue_capacity: usize,
        current_wal_size_bytes: Option<u64>,
    ) -> StatsSnapshot {
        StatsSnapshot {
            accepted: self.accepted.load(Ordering::Relaxed),
            committed: self.committed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            queue_depth,
            queue_capacity,
            current_wal_size_bytes,
        }
    }
}

// ---------------------------------------------------------------------------
// StatsSnapshot — serialisable point-in-time view
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of gateway statistics (§24).
///
/// Returned by the admin `{ "type": "stats" }` command over the JSON Lines
/// protocol (§18) and exposed on [`crate::Client::stats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    /// Total write requests accepted (incremented before channel send).
    pub accepted: u64,
    /// Write requests that committed successfully.
    pub committed: u64,
    /// Write requests that failed (SQLite error, constraint violation, etc.).
    pub failed: u64,
    /// Write requests rejected due to channel full / gateway closed.
    pub rejected: u64,
    /// Current number of items waiting in the bounded write queue (§14).
    pub queue_depth: usize,
    /// Maximum capacity of the bounded write queue (§14).
    pub queue_capacity: usize,
    /// Size of the WAL file in bytes, or `None` for `:memory:` / no WAL (§16).
    pub current_wal_size_bytes: Option<u64>,
}

// ---------------------------------------------------------------------------
// WAL size helper
// ---------------------------------------------------------------------------

/// Try to read the size of the WAL file adjacent to `db_path`.
///
/// Returns `None` for `:memory:` databases, empty paths, or if the WAL file
/// does not exist (WAL mode not enabled / after a full checkpoint).
pub(crate) fn wal_size_bytes(db_path: &std::path::Path) -> Option<u64> {
    let path_str = db_path.to_str()?;
    if path_str.is_empty() || path_str == ":memory:" {
        return None;
    }
    let wal_path = format!("{path_str}-wal");
    std::fs::metadata(&wal_path).ok().map(|m| m.len())
}
