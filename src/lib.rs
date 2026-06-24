//! # SqueueLite
//!
//! **SqueueLite is not a job queue.**
//! It is a SQLite write gateway: a single-writer gateway for SQLite-backed
//! agent systems that serialises concurrent writes from multiple in-process
//! tasks through one `rusqlite::Connection`.
//!
//! (§26 of the design specification)
//!
//! ## What SqueueLite does
//!
//! - Accepts [`WriteRequest`]s from concurrent callers.
//! - Executes each request as one atomic `BEGIN IMMEDIATE … COMMIT` transaction
//!   on a single SQLite connection.
//! - Returns a [`WriteResponse`] with an optional monotonically increasing
//!   [`WriteResponse::commit_seq`].
//!
//! ## What SqueueLite does NOT do
//!
//! - Schedule or dispatch jobs.
//! - Manage worker pools.
//! - Retry failed operations.
//! - Run long-lived background tasks.
//!
//! ## Quick start (in-process mode, requires the `inprocess` feature)
//!
//! ```rust,no_run
//! # #[cfg(feature = "inprocess")]
//! # async fn example() -> squeuelite::Result<()> {
//! use squeuelite::{InProcessGateway, WriteRequest, SqlOperation};
//!
//! let gateway = InProcessGateway::open(":memory:")?;
//! let handle = gateway.handle();
//!
//! let response = handle.execute(WriteRequest {
//!     request_id: "req-1".into(),
//!     actor_id: "agent-a".into(),
//!     run_id: None,
//!     idempotency_key: None,
//!     operations: vec![SqlOperation {
//!         sql: "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, data TEXT)".into(),
//!         params: vec![],
//!     }],
//! }).await?;
//!
//! println!("commit_seq = {:?}", response.commit_seq);
//! gateway.shutdown().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod request;

#[cfg(feature = "inprocess")]
pub(crate) mod writer;

#[cfg(feature = "inprocess")]
pub mod inprocess;

#[cfg(feature = "sidecar")]
pub(crate) mod protocol;

#[cfg(feature = "sidecar")]
pub mod stats;

#[cfg(feature = "sidecar")]
pub mod sidecar;

#[cfg(feature = "sidecar")]
pub mod client;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use config::{GatewayConfig, JournalMode, OverflowPolicy, SyncMode};
pub use error::{Error, Result};
pub use request::{SqlOperation, WriteRequest, WriteResponse, WriteStatus};

#[cfg(feature = "inprocess")]
pub use inprocess::{GatewayHandle, InProcessGateway};

#[cfg(feature = "sidecar")]
pub use client::Client;

#[cfg(feature = "sidecar")]
pub use sidecar::{SidecarConfig, SidecarGateway};

#[cfg(feature = "sidecar")]
pub use stats::StatsSnapshot;
