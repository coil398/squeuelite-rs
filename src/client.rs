//! Unix Domain Socket client for SqueueLite sidecar mode (§20.2, §20.3).
//!
//! This module is only compiled when the `sidecar` feature is enabled.
//!
//! ## Protocol
//!
//! The client speaks **JSON-RPC 2.0** over JSON Lines on a
//! [`tokio::net::UnixStream`]. Each request is a single JSON-RPC 2.0 object;
//! each response is a single JSON-RPC 2.0 object (result or error).
//!
//! Each `Client` owns one connection. In-flight requests are serialised
//! (one outstanding request at a time; no pipelining in the MVP). Pipelining
//! may be added in a future version (§27).

use std::path::Path;

use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{unix::OwnedReadHalf, unix::OwnedWriteHalf, UnixStream},
};
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    request::{SqlOperation, WriteRequest, WriteResponse, WriteStatus},
    stats::StatsSnapshot,
};

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A client that speaks JSON-RPC 2.0 to a running [`crate::SidecarGateway`].
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
    pub async fn connect(
        actor_id: impl Into<String>,
        socket_path: impl AsRef<Path>,
    ) -> Result<Self> {
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

    /// Request a statistics snapshot from the gateway (§24).
    ///
    /// Sends `{"jsonrpc":"2.0","id":"...","method":"stats"}` and parses the
    /// result into a [`StatsSnapshot`].
    pub async fn stats(&mut self) -> Result<StatsSnapshot> {
        let id = Uuid::now_v7().to_string();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "stats"
        });
        let req_str = serde_json::to_string(&req).map_err(Error::Json)?;
        let reply = self.send_line(&req_str).await?;
        let v: Value = serde_json::from_str(&reply).map_err(Error::Json)?;
        if let Some(result) = v.get("result") {
            serde_json::from_value(result.clone()).map_err(Error::Json)
        } else if let Some(err) = v.get("error") {
            Err(Error::Protocol(err.to_string()))
        } else {
            Err(Error::Protocol(format!(
                "unexpected stats response: {reply}"
            )))
        }
    }

    /// Check gateway health (§24).
    ///
    /// Sends `{"jsonrpc":"2.0","id":"...","method":"health"}` and returns
    /// `Ok(())` if the gateway responds with `{"status":"ok"}`.
    pub async fn health(&mut self) -> Result<()> {
        let id = Uuid::now_v7().to_string();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "health"
        });
        let req_str = serde_json::to_string(&req).map_err(Error::Json)?;
        let reply = self.send_line(&req_str).await?;
        let v: Value = serde_json::from_str(&reply).map_err(Error::Json)?;
        if v.get("result").is_some() {
            Ok(())
        } else if let Some(err) = v.get("error") {
            Err(Error::Protocol(err.to_string()))
        } else {
            Ok(())
        }
    }

    /// Request a WAL checkpoint (§24).
    ///
    /// Sends `{"jsonrpc":"2.0","id":"...","method":"checkpoint"}` and returns
    /// `Ok(())` if the checkpoint succeeded.
    pub async fn checkpoint(&mut self) -> Result<()> {
        let id = Uuid::now_v7().to_string();
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "checkpoint"
        });
        let req_str = serde_json::to_string(&req).map_err(Error::Json)?;
        let reply = self.send_line(&req_str).await?;
        let v: Value = serde_json::from_str(&reply).map_err(Error::Json)?;
        if v.get("result").is_some() {
            Ok(())
        } else if let Some(err) = v.get("error") {
            Err(Error::Protocol(err.to_string()))
        } else {
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Send a JSON-RPC 2.0 execute request and parse the response into a
    /// [`WriteResponse`].
    async fn send_request(&mut self, req: &WriteRequest) -> Result<WriteResponse> {
        // Build the JSON-RPC 2.0 request envelope.
        // The internal `WriteRequest` maps to the JSON-RPC `execute` method.
        let rpc_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": req.request_id,
            "method": "execute",
            "params": {
                "actor_id": req.actor_id,
                "run_id": req.run_id,
                "idempotency_key": req.idempotency_key,
                "operations": req.operations.iter().map(|op| {
                    serde_json::json!({
                        "sql": op.sql,
                        "params": op.params
                    })
                }).collect::<Vec<_>>()
            }
        });

        let line = serde_json::to_string(&rpc_req).map_err(Error::Json)?;
        let response_line = self.send_line(&line).await?;

        // Parse the JSON-RPC 2.0 response and convert to WriteResponse.
        let v: Value = serde_json::from_str(&response_line).map_err(Error::Json)?;

        if let Some(result) = v.get("result") {
            // Success response: {"jsonrpc":"2.0","id":"...","result":{"status":"committed","commit_seq":42}}
            let status_str = result
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("failed");
            let commit_seq = result.get("commit_seq").and_then(|s| s.as_i64());
            let status = if status_str == "committed" {
                WriteStatus::Committed
            } else {
                WriteStatus::Failed
            };
            Ok(WriteResponse {
                request_id: req.request_id.clone(),
                status,
                commit_seq,
                error: None,
            })
        } else if let Some(err) = v.get("error") {
            // Error response: {"jsonrpc":"2.0","id":"...","error":{"code":-32000,"message":"..."}}
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string)
                .or_else(|| {
                    // Fall back to the data field if present.
                    err.get("data").and_then(|d| d.as_str()).map(str::to_string)
                });
            Ok(WriteResponse {
                request_id: req.request_id.clone(),
                status: WriteStatus::Failed,
                commit_seq: None,
                error: message,
            })
        } else {
            Err(Error::Protocol(format!(
                "unexpected JSON-RPC response: {response_line}"
            )))
        }
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
