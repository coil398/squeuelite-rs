//! JSON Lines protocol for the Unix Domain Socket sidecar (§18).
//!
//! This module is only compiled when the `sidecar` feature is enabled.
//!
//! ## Protocol overview (§18.1 MVP)
//!
//! Communication uses Unix Domain Socket + JSON Lines: one JSON object per
//! line (`\n` terminated). The design chose this over binary protocols because:
//!
//! - Implementation is small — no framing, no length prefix.
//! - Easy to debug with `socat` or any line-oriented tool.
//! - Agents written in any language can speak the protocol.
//! - `socat UNIX-CONNECT:./squeuelite.sock -` is all you need to poke at it.
//!
//! ### Client → Server
//!
//! Each line is deserialised as [`Incoming`]. The `untagged` enum tries
//! [`AdminCommand`] first (discriminated by the `"type"` field), then falls
//! back to [`WriteRequest`].
//!
//! ### Server → Client
//!
//! - A [`WriteRequest`] line gets a [`WriteResponse`] JSON line back.
//! - An [`AdminCommand::Stats`] gets a [`StatsSnapshot`] JSON line.
//! - An [`AdminCommand::Health`] gets `{"status":"ok"}`.
//! - An [`AdminCommand::Checkpoint`] gets `{"status":"ok"}` or an error JSON.
//!
//! The connection is **not** closed after each exchange; the client may send
//! multiple requests over a single connection (§20.2).

use serde::Deserialize;

use crate::request::WriteRequest;

// ---------------------------------------------------------------------------
// Incoming — untagged enum dispatching admin vs write
// ---------------------------------------------------------------------------

/// A single line received from a connected client (§18.1).
///
/// Deserialised as an untagged enum: `AdminCommand` is tried first (it has
/// a required `"type"` field that `WriteRequest` lacks), then `WriteRequest`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Incoming {
    /// An admin command identified by `"type"` (§24 admin interface).
    Admin(AdminCommand),
    /// A write request to be executed as a SQLite transaction (§8).
    Write(WriteRequest),
}

// ---------------------------------------------------------------------------
// AdminCommand — internally-tagged by "type"
// ---------------------------------------------------------------------------

/// Admin commands sent as JSON Lines with a `"type"` discriminant (§24).
///
/// Examples:
/// ```json
/// { "type": "stats" }
/// { "type": "health" }
/// { "type": "checkpoint" }
/// ```
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum AdminCommand {
    /// Return a [`crate::stats::StatsSnapshot`] (§24 `GET /stats`).
    Stats,
    /// Return `{"status":"ok"}` (§24 `GET /health`).
    Health,
    /// Run `PRAGMA wal_checkpoint(TRUNCATE)` (§24 `POST /checkpoint`).
    Checkpoint,
}
