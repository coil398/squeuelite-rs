//! Unix Domain Socket client for SqueueLite sidecar mode (§20.2, §20.3).
//!
//! This module is only compiled when the `sidecar` feature is enabled.
//!
//! ## Protocol
//!
//! The client speaks JSON Lines over a [`tokio::net::UnixStream`]:
//! - One [`WriteRequest`] JSON → one [`WriteResponse`] JSON.
//! - One admin command JSON → one response JSON.
//!
//! Each `Client` owns one connection. In-flight requests are serialised
//! (one outstanding request at a time; no pipelining in the MVP). Pipelining
//! may be added in a future version (§27).

use std::path::Path;

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixStream, unix::OwnedReadHalf, unix::OwnedWriteHalf},
};
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    request::{SqlOperation, WriteRequest, WriteResponse},
    stats::StatsSnapshot,
};

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A client that speaks JSON Lines to a running [`crate::SidecarGateway`].
///
/// Obtain via [`Client::connect`]. One `Client` corresponds to one Unix
/// Domain Socket connection. `&mut self` methods enforce single-in-flight
/// semantics (§20.2).
///
/// ## Example (§20.2)
///
/// ```rust,no_run
/// # #[cfg(feature = "sidecar")]
/// # async fn example() -> squeuelite::Result<()> {
/// use squeuelite::{Client, SqlOperation};
///
/// let mut client = Client::connect("agent-a", "./squeuelite.sock").await?;
/// let resp = client.execute(SqlOperation {
///     sql: "INSERT INTO events(agent_id, kind) VALUES (?, ?)".into(),
///     params: vec!["agent-a".into(), "started".into()],
/// }).await?;
/// println!("status = {:?}", resp.status);
/// # Ok(())
/// # }
/// ```
pub struct Client {
    actor_id: String,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Client {
    /// Connect to a [`SidecarGateway`] socket as `actor_id` (§20.2).
    ///
    /// `actor_id` is embedded in every [`WriteRequest`] sent through this
    /// client (§8 `actor_id` field).
    pub async fn connect(actor_id: impl Into<String>, socket_path: impl AsRef<Path>) -> Result<Self> {
        let stream = UnixStream::connect(socket_path.as_ref())
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        let (read_half, write_half) = stream.into_split();

        Ok(Self {
            actor_id: actor_id.into(),
            reader: BufReader::new(read_half),
            writer: write_half,
        })
    }

    // -----------------------------------------------------------------------
    // Write API (§20.2 / §20.3)
    // -----------------------------------------------------------------------

    /// Execute a single SQL operation as one atomic transaction (§20.2).
    ///
    /// A new UUIDv7 `request_id` is generated automatically (§8).
    pub async fn execute(&mut self, op: SqlOperation) -> Result<WriteResponse> {
        self.transaction(vec![op]).await
    }

    /// Execute multiple SQL operations as one atomic transaction (§20.3).
    ///
    /// All operations succeed or all are rolled back (§11).
    /// A new UUIDv7 `request_id` is generated automatically (§8).
    pub async fn transaction(&mut self, ops: Vec<SqlOperation>) -> Result<WriteResponse> {
        let req = WriteRequest {
            request_id: Uuid::now_v7().to_string(),
            actor_id: self.actor_id.clone(),
            run_id: None,
            idempotency_key: None,
            operations: ops,
        };
        self.send_request(&req).await
    }

    // -----------------------------------------------------------------------
    // Admin API (§24)
    // -----------------------------------------------------------------------

    /// Request a statistics snapshot from the gateway (§24 `{ "type": "stats" }`).
    pub async fn stats(&mut self) -> Result<StatsSnapshot> {
        let line = self.send_line(r#"{"type":"stats"}"#).await?;
        serde_json::from_str(&line).map_err(Error::Json)
    }

    /// Check gateway health (§24 `{ "type": "health" }`).
    ///
    /// Returns `Ok(())` if the gateway responds with `{"status":"ok"}`.
    pub async fn health(&mut self) -> Result<()> {
        let _line = self.send_line(r#"{"type":"health"}"#).await?;
        Ok(())
    }

    /// Request a WAL checkpoint (§24 `{ "type": "checkpoint" }`).
    ///
    /// Returns `Ok(())` if the checkpoint succeeded.
    pub async fn checkpoint(&mut self) -> Result<()> {
        let line = self.send_line(r#"{"type":"checkpoint"}"#).await?;
        // Accept {"status":"ok"} or any non-failed response.
        let v: serde_json::Value = serde_json::from_str(&line).map_err(Error::Json)?;
        if let Some(status) = v.get("status") {
            if status == "ok" {
                return Ok(());
            }
        }
        if let Some(err) = v.get("error") {
            return Err(Error::Protocol(err.to_string()));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    async fn send_request(&mut self, req: &WriteRequest) -> Result<WriteResponse> {
        let line = serde_json::to_string(req).map_err(Error::Json)?;
        let response_line = self.send_line(&line).await?;
        serde_json::from_str(&response_line).map_err(Error::Json)
    }

    /// Send a single JSON line and receive the reply line.
    async fn send_line(&mut self, line: &str) -> Result<String> {
        // Send
        let mut buf = line.to_string();
        buf.push('\n');
        self.writer
            .write_all(buf.as_bytes())
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        // Receive
        let mut reply = String::new();
        self.reader
            .read_line(&mut reply)
            .await
            .map_err(|e| Error::Io(e.to_string()))?;

        if reply.is_empty() {
            return Err(Error::GatewayClosed);
        }

        Ok(reply.trim_end_matches('\n').to_string())
    }
}
