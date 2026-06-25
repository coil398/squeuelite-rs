//! JSON-RPC 2.0 wire types, helpers, and transport-agnostic dispatcher (§18).
//!
//! This module is compiled when the `sidecar` **or** `http` feature is enabled.
//!
//! ## Protocol overview
//!
//! Transport-independent: each caller (UDS sidecar or HTTP) passes a single
//! JSON line (or HTTP body) to [`dispatch`], which returns a JSON string reply.
//! JSON-RPC **batch arrays** are **not** supported; a single object per request.
//! Arrays are rejected with error code `-32600` (Invalid Request).
//!
//! ## Error codes
//!
//! ### Standard JSON-RPC 2.0 errors
//!
//! | Code     | Meaning            | When                                      |
//! |---------:|--------------------|-------------------------------------------|
//! | `-32700` | Parse error        | Line is not valid JSON                    |
//! | `-32600` | Invalid Request    | `jsonrpc != "2.0"`, array, or no `method` |
//! | `-32601` | Method not found   | Unknown method name                       |
//! | `-32602` | Invalid params     | Missing `actor_id` or `operations`        |
//!
//! ### Application errors (server-error range `-32000`..`-32099`)
//!
//! | Code     | Meaning              | When                                    |
//! |---------:|----------------------|-----------------------------------------|
//! | `-32000` | write failed         | Execute returned `WriteResponse::Failed` (constraint / SQL rejected / invalid param) |
//! | `-32001` | gateway overloaded   | Channel full (`Error::GatewayOverloaded`)|
//!
//! ## socat examples
//!
//! ```bash
//! # Health check
//! echo '{"jsonrpc":"2.0","id":1,"method":"health"}' | socat UNIX-CONNECT:./squeuelite.sock -
//!
//! # Stats snapshot
//! echo '{"jsonrpc":"2.0","id":2,"method":"stats"}' | socat UNIX-CONNECT:./squeuelite.sock -
//!
//! # WAL checkpoint
//! echo '{"jsonrpc":"2.0","id":3,"method":"checkpoint"}' | socat UNIX-CONNECT:./squeuelite.sock -
//!
//! # Execute (table must exist)
//! echo '{"jsonrpc":"2.0","id":4,"method":"execute","params":{"actor_id":"agent-a","operations":[{"sql":"INSERT INTO t(v) VALUES (?)","params":["hello"]}]}}' \
//!   | socat UNIX-CONNECT:./squeuelite.sock -
//! ```

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    inprocess::GatewayHandle,
    request::{SqlOperation, WriteRequest, WriteStatus},
    stats::{Stats, wal_size_bytes},
};

// ---------------------------------------------------------------------------
// Error code constants
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 standard: line was not valid JSON.
pub const ERR_PARSE_ERROR: i64 = -32700;
/// JSON-RPC 2.0 standard: `jsonrpc` != "2.0", array body, or `method` absent.
pub const ERR_INVALID_REQUEST: i64 = -32600;
/// JSON-RPC 2.0 standard: unknown method name.
pub const ERR_METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC 2.0 standard: `actor_id` or `operations` missing / wrong type.
pub const ERR_INVALID_PARAMS: i64 = -32602;

/// Application error: `execute` produced a `WriteResponse::Failed`.
/// `message` = error description, `data` = detail string from `WriteResponse.error`.
pub const ERR_WRITE_FAILED: i64 = -32000;
/// Application error: bounded channel was full (`Error::GatewayOverloaded`).
pub const ERR_GATEWAY_OVERLOADED: i64 = -32001;

// ---------------------------------------------------------------------------
// Transport-shared constants
// ---------------------------------------------------------------------------

/// Maximum body / line size accepted by **both** transports (UDS and HTTP).
///
/// A single JSON line (UDS) or HTTP request body larger than this limit causes
/// the connection to be closed / a 413 response to be returned.  The same
/// constant is used by [`crate::sidecar`] for per-line capping and by
/// [`crate::http`] via `axum::extract::DefaultBodyLimit::max` so that the
/// two transports enforce an identical ceiling.
///
/// 1 MiB is ample for any realistic write request while preventing memory
/// exhaustion from a misbehaving or malicious local process (§23).
pub const MAX_LINE_BYTES: usize = 1024 * 1024; // 1 MiB

// ---------------------------------------------------------------------------
// Incoming JSON-RPC request (raw, before method dispatch)
// ---------------------------------------------------------------------------

/// A single line received from a client, parsed as a JSON-RPC 2.0 request.
///
/// `id`, `method`, and `params` are kept as raw [`Value`] so we can validate
/// them after deserialization and echo `id` verbatim in every response.
///
/// Per spec, `id` may be a string, a number, or `null`; we accept all three.
/// Absent `id` (notification) is represented as `Value::Null` here — the
/// sidecar always sends a response regardless (not strictly spec-compliant for
/// notifications, but safe for a local trusted channel).
#[derive(Debug, Deserialize)]
pub struct RawRequest {
    /// Must be the string `"2.0"`.
    #[serde(default)]
    pub jsonrpc: Option<String>,
    /// Caller-supplied request identifier echoed in the response.
    #[serde(default)]
    pub id: Value,
    /// Method name (`"execute"` / `"stats"` / `"health"` / `"checkpoint"`).
    pub method: Option<String>,
    /// Method-specific parameters; absent for admin methods.
    #[serde(default)]
    pub params: Value,
}

// ---------------------------------------------------------------------------
// Outgoing JSON-RPC response types
// ---------------------------------------------------------------------------

/// A successful JSON-RPC 2.0 response.
///
/// ```json
/// {"jsonrpc":"2.0","id":1,"result":{...}}
/// ```
#[derive(Debug, Serialize)]
pub struct JsonRpcSuccess {
    pub jsonrpc: &'static str,
    pub id: Value,
    pub result: Value,
}

impl JsonRpcSuccess {
    pub fn new(id: Value, result: Value) -> Self {
        Self { jsonrpc: "2.0", id, result }
    }
}

/// A JSON-RPC 2.0 error response.
///
/// ```json
/// {"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}
/// ```
#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub jsonrpc: &'static str,
    pub id: Value,
    pub error: RpcErrorObject,
}

impl JsonRpcError {
    /// Build an error response, setting `id` to `null` when the request id
    /// could not be determined (e.g. parse failure).
    pub fn new(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            error: RpcErrorObject { code, message: message.into(), data: None },
        }
    }

    pub fn with_data(
        id: Value,
        code: i64,
        message: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            error: RpcErrorObject { code, message: message.into(), data: Some(data) },
        }
    }
}

/// The `error` object inside a [`JsonRpcError`] response.
#[derive(Debug, Serialize)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialise a [`JsonRpcSuccess`] to a JSON string (infallible).
pub fn ok_json(id: Value, result: Value) -> String {
    serde_json::to_string(&JsonRpcSuccess::new(id, result))
        .unwrap_or_else(|e| format!(r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":-32603,"message":"internal serialize error: {e}"}}}}"#))
}

/// Serialise a [`JsonRpcError`] to a JSON string (infallible).
pub fn err_json(id: Value, code: i64, message: impl Into<String>) -> String {
    serde_json::to_string(&JsonRpcError::new(id, code, message))
        .unwrap_or_else(|e| format!(r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":-32603,"message":"internal serialize error: {e}"}}}}"#))
}

/// Serialise a [`JsonRpcError`] with extra `data` to a JSON string (infallible).
pub fn err_json_data(
    id: Value,
    code: i64,
    message: impl Into<String>,
    data: Value,
) -> String {
    serde_json::to_string(&JsonRpcError::with_data(id, code, message, data))
        .unwrap_or_else(|e| format!(r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":-32603,"message":"internal serialize error: {e}"}}}}"#))
}

// ---------------------------------------------------------------------------
// Transport-agnostic dispatcher
// ---------------------------------------------------------------------------

/// Parse one JSON-RPC 2.0 request (as a string) and return the reply JSON string.
///
/// This function is **transport-agnostic**: callers may be the UDS sidecar
/// (which passes a JSON Lines line) or the HTTP handler (which passes the
/// HTTP request body as a string). Both get the same dispatch logic.
///
/// `db_path` is used only by the `"stats"` method to read the WAL file size;
/// pass `Path::new("")` for in-memory databases or when stats are unused.
pub async fn dispatch(
    line: &str,
    handle: &GatewayHandle,
    stats: &Arc<Stats>,
    db_path: &std::path::Path,
) -> String {
    // --- Step 1: raw JSON parse ---
    let raw_value: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return err_json(Value::Null, ERR_PARSE_ERROR, format!("parse error: {e}"));
        }
    };

    // --- Step 2: reject arrays (batch not supported) ---
    if raw_value.is_array() {
        return err_json(
            Value::Null,
            ERR_INVALID_REQUEST,
            "JSON-RPC batch arrays are not supported; send one request per line",
        );
    }

    // --- Step 3: deserialise into RawRequest ---
    let req: RawRequest = match serde_json::from_value(raw_value) {
        Ok(r) => r,
        Err(e) => {
            return err_json(
                Value::Null,
                ERR_INVALID_REQUEST,
                format!("invalid request structure: {e}"),
            );
        }
    };

    let id = req.id.clone();

    // --- Step 4: validate `jsonrpc == "2.0"` ---
    match req.jsonrpc.as_deref() {
        Some("2.0") => {}
        _ => {
            return err_json(
                id,
                ERR_INVALID_REQUEST,
                r#"missing or invalid "jsonrpc" field; must be "2.0""#,
            );
        }
    }

    // --- Step 5: require `method` ---
    let method = match req.method.as_deref() {
        Some(m) => m,
        None => {
            return err_json(id, ERR_INVALID_REQUEST, r#"missing "method" field"#);
        }
    };

    // --- Step 6: dispatch by method ---
    match method {
        "execute" => dispatch_execute(id, req.params, handle, stats).await,
        "stats" => dispatch_stats(id, handle, stats, db_path).await,
        "health" => dispatch_health(id),
        "checkpoint" => dispatch_checkpoint(id, handle).await,
        other => err_json(id, ERR_METHOD_NOT_FOUND, format!("method not found: {other}")),
    }
}

/// Handle `"execute"` — parse params and forward to the in-process writer.
async fn dispatch_execute(
    id: Value,
    params: Value,
    handle: &GatewayHandle,
    stats: &Arc<Stats>,
) -> String {
    use std::sync::atomic::Ordering;

    let params_obj = match params.as_object() {
        Some(o) => o,
        None => {
            return err_json(
                id,
                ERR_INVALID_PARAMS,
                "params must be an object with actor_id and operations",
            );
        }
    };

    // actor_id (required, string)
    let actor_id = match params_obj.get("actor_id").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => {
            return err_json(
                id,
                ERR_INVALID_PARAMS,
                "missing or non-string actor_id in params",
            );
        }
    };

    // operations (required, non-empty array)
    let operations_value = match params_obj.get("operations") {
        Some(v) => v,
        None => {
            return err_json(id, ERR_INVALID_PARAMS, "missing operations in params");
        }
    };
    let operations_arr = match operations_value.as_array() {
        Some(a) if !a.is_empty() => a,
        Some(_) => {
            return err_json(id, ERR_INVALID_PARAMS, "operations must be a non-empty array");
        }
        None => {
            return err_json(id, ERR_INVALID_PARAMS, "operations must be an array");
        }
    };

    // Parse each operation.
    let mut ops: Vec<SqlOperation> = Vec::with_capacity(operations_arr.len());
    for (i, op_val) in operations_arr.iter().enumerate() {
        let op_obj = match op_val.as_object() {
            Some(o) => o,
            None => {
                return err_json(
                    id,
                    ERR_INVALID_PARAMS,
                    format!("operations[{i}] must be an object"),
                );
            }
        };
        let sql = match op_obj.get("sql").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                return err_json(
                    id,
                    ERR_INVALID_PARAMS,
                    format!("operations[{i}].sql must be a string"),
                );
            }
        };
        let params_arr: Vec<Value> = match op_obj.get("params") {
            Some(Value::Array(a)) => a.clone(),
            Some(Value::Null) | None => vec![],
            Some(_) => {
                return err_json(
                    id,
                    ERR_INVALID_PARAMS,
                    format!("operations[{i}].params must be an array or absent"),
                );
            }
        };
        ops.push(SqlOperation { sql, params: params_arr });
    }

    // Optional fields.
    let run_id = params_obj
        .get("run_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let idempotency_key = params_obj
        .get("idempotency_key")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    // Convert the JSON-RPC `id` to a `request_id` string for the internal type.
    // If id is null (or missing), generate a UUID to ensure request tracking works.
    let request_id = match &id {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => uuid::Uuid::now_v7().to_string(),
    };

    let write_req = WriteRequest {
        request_id,
        actor_id,
        run_id,
        idempotency_key,
        operations: ops,
    };

    stats.accepted.fetch_add(1, Ordering::Relaxed);

    let result = handle.execute(write_req).await;
    match result {
        Ok(resp) => {
            if resp.status == WriteStatus::Committed {
                stats.committed.fetch_add(1, Ordering::Relaxed);
                ok_json(
                    id,
                    json!({
                        "status": "committed",
                        "commit_seq": resp.commit_seq
                    }),
                )
            } else {
                stats.failed.fetch_add(1, Ordering::Relaxed);
                let msg = resp.error.clone().unwrap_or_else(|| "write failed".into());
                let data = resp.error.map(Value::String).unwrap_or(Value::Null);
                err_json_data(id, ERR_WRITE_FAILED, msg, data)
            }
        }
        Err(
            crate::error::Error::GatewayOverloaded
            | crate::error::Error::GatewayClosed,
        ) => {
            stats.rejected.fetch_add(1, Ordering::Relaxed);
            err_json(id, ERR_GATEWAY_OVERLOADED, "gateway overloaded")
        }
        Err(e) => {
            stats.failed.fetch_add(1, Ordering::Relaxed);
            err_json(id, ERR_WRITE_FAILED, e.to_string())
        }
    }
}

/// Handle `"stats"` — return a [`crate::stats::StatsSnapshot`].
async fn dispatch_stats(
    id: Value,
    handle: &GatewayHandle,
    stats: &Arc<Stats>,
    db_path: &std::path::Path,
) -> String {
    let (avg_latency, p95_latency) = handle.latency_snapshot();
    let snapshot = stats.snapshot(
        handle.queue_depth(),
        handle.queue_capacity(),
        wal_size_bytes(db_path),
        avg_latency,
        p95_latency,
    );
    match serde_json::to_value(&snapshot) {
        Ok(v) => ok_json(id, v),
        Err(e) => err_json(id, -32603, format!("internal serialize error: {e}")),
    }
}

/// Handle `"health"` — return `{"status":"ok"}`.
fn dispatch_health(id: Value) -> String {
    ok_json(id, json!({"status": "ok"}))
}

/// Handle `"checkpoint"` — run WAL checkpoint and return `{"status":"ok"}`.
async fn dispatch_checkpoint(id: Value, handle: &GatewayHandle) -> String {
    match handle.checkpoint().await {
        Ok(()) => ok_json(id, json!({"status": "ok"})),
        Err(e) => err_json(id, ERR_WRITE_FAILED, e.to_string()),
    }
}
