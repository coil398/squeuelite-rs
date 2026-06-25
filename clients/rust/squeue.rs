//! Minimal SqueueLite client (raw JSON over UDS) — copy into your project.
//!
//! The crate ships a built-in [`squeuelite::Client`] (enable the `sidecar`
//! feature). Prefer it for most cases. Use *this* standalone module when you
//! want to set `idempotency_key` / `run_id`, which the built-in client leaves
//! as `None`.
//!
//! SqueueLite is *write-only*; read the SQLite file directly (read-only, WAL).
//!
//! Dependencies (add to your `Cargo.toml`):
//! ```toml
//! tokio = { version = "1", features = ["net", "io-util", "rt-multi-thread", "macros"] }
//! serde_json = "1"
//! uuid = { version = "1", features = ["v7"] }
//! ```
//!
//! Example:
//! ```ignore
//! let mut db = Squeue::connect("/run/app/squeuelite.sock", "agent-rs").await?;
//! let resp = db.execute(
//!     "INSERT INTO events(agent_id, kind) VALUES (?, ?)",
//!     serde_json::json!(["agent-rs", "started"]),
//!     Some("run-7:step-1"),   // idempotency_key
//!     Some("run-7"),          // run_id
//! ).await?;
//! // resp == {"request_id": "...", "status": "committed", "commit_seq": 12}
//! ```

use std::io;

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};

pub struct Squeue {
    actor_id: String,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Squeue {
    pub async fn connect(socket_path: &str, actor_id: &str) -> io::Result<Self> {
        let (read_half, write_half) = UnixStream::connect(socket_path).await?.into_split();
        Ok(Self {
            actor_id: actor_id.to_string(),
            reader: BufReader::new(read_half),
            writer: write_half,
        })
    }

    /// Run one statement as a single atomic transaction.
    ///
    /// `params` is a JSON array, e.g. `serde_json::json!(["a", 1, true])`.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: Value,
        idempotency_key: Option<&str>,
        run_id: Option<&str>,
    ) -> io::Result<Value> {
        self.transaction(vec![(sql.to_string(), params)], idempotency_key, run_id)
            .await
    }

    /// Run several `(sql, params)` ops as ONE transaction (all-or-nothing).
    pub async fn transaction(
        &mut self,
        ops: Vec<(String, Value)>,
        idempotency_key: Option<&str>,
        run_id: Option<&str>,
    ) -> io::Result<Value> {
        let operations: Vec<Value> = ops
            .into_iter()
            .map(|(sql, params)| json!({ "sql": sql, "params": params }))
            .collect();

        let mut req = json!({
            "request_id": uuid::Uuid::now_v7().to_string(),
            "actor_id": self.actor_id,
            "operations": operations,
        });
        if let Some(k) = idempotency_key {
            req["idempotency_key"] = Value::from(k);
        }
        if let Some(r) = run_id {
            req["run_id"] = Value::from(r);
        }

        self.roundtrip(&req).await
    }

    pub async fn stats(&mut self) -> io::Result<Value> {
        self.roundtrip(&json!({ "type": "stats" })).await
    }

    async fn roundtrip(&mut self, req: &Value) -> io::Result<Value> {
        let mut line = serde_json::to_string(req)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await?;

        let mut reply = String::new();
        self.reader.read_line(&mut reply).await?;
        if reply.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "gateway closed the connection",
            ));
        }
        serde_json::from_str(&reply).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}
