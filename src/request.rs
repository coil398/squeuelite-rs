use serde::{Deserialize, Serialize};

/// A write request sent to the gateway. One request corresponds to one
/// SQLite transaction (§9 of the design specification).
///
/// `idempotency_key` is carried as a field so that the sidecar phase (§13)
/// can implement deduplication without a breaking API change. In the current
/// in-process MVP, the field is not acted upon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    /// Caller-supplied unique identifier for this request (e.g. a UUIDv7).
    pub request_id: String,
    /// Identifier of the agent or actor issuing the write.
    pub actor_id: String,
    /// Optional run / job context that groups related requests.
    pub run_id: Option<String>,
    /// Optional key for idempotent re-delivery (§13). Not enforced by the
    /// in-process gateway in the current MVP; reserved for future use.
    pub idempotency_key: Option<String>,
    /// Ordered list of SQL operations to execute within one transaction.
    pub operations: Vec<SqlOperation>,
}

/// A single parameterised SQL statement that forms part of a [`WriteRequest`].
///
/// `params` uses [`serde_json::Value`] so that callers can pass typed JSON
/// values; the gateway converts them to the appropriate SQLite types before
/// binding (§9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlOperation {
    /// A prepared SQL statement with `?` placeholders.
    pub sql: String,
    /// Bind parameters in positional order.
    pub params: Vec<serde_json::Value>,
}

/// The outcome of a [`WriteRequest`] processed by the gateway (§8).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    /// Echoed back from [`WriteRequest::request_id`].
    pub request_id: String,
    /// Whether the transaction committed or failed.
    pub status: WriteStatus,
    /// Monotonically increasing commit sequence number assigned by the gateway
    /// internal `squeuelite_commits` table (§12). `None` when
    /// `track_commits = false` or when the transaction failed.
    pub commit_seq: Option<i64>,
    /// Human-readable error description when `status == Failed`.
    pub error: Option<String>,
}

impl WriteResponse {
    /// Construct a successful (committed) response.
    ///
    /// `commit_seq` is `Some(seq)` when commit tracking is enabled, `None`
    /// otherwise (§12, §4 of the design specification).
    pub fn committed(request_id: String, commit_seq: Option<i64>) -> Self {
        Self {
            request_id,
            status: WriteStatus::Committed,
            commit_seq,
            error: None,
        }
    }

    /// Construct a failed response after a rollback.
    pub fn failed(request_id: String, error: String) -> Self {
        Self {
            request_id,
            status: WriteStatus::Failed,
            commit_seq: None,
            error: Some(error),
        }
    }
}

/// Outcome status of a write request, serialised as a lowercase string (§8).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteStatus {
    /// The transaction was committed successfully.
    Committed,
    /// The transaction was rolled back.
    Failed,
}
