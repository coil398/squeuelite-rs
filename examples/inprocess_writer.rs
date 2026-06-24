//! Minimal demonstration of the in-process writer actor (§21).
//!
//! Run with:
//!   cargo run --example inprocess_writer --features inprocess
//!
//! Uses `:memory:` so no database file is left on disk after the process exits.

#[tokio::main]
async fn main() -> squeuelite::Result<()> {
    // Open an in-memory gateway (no file left on disk). §21
    let gateway = squeuelite::InProcessGateway::open(":memory:")?;
    let handle = gateway.handle();

    // --- Setup: create a simple events table via the gateway ---
    let setup_resp = handle
        .execute(squeuelite::WriteRequest {
            request_id: "setup-1".into(),
            actor_id: "example-actor".into(),
            run_id: Some("run-demo".into()),
            idempotency_key: None,
            operations: vec![squeuelite::SqlOperation {
                sql: "CREATE TABLE IF NOT EXISTS events \
                      (id INTEGER PRIMARY KEY, agent_id TEXT, kind TEXT, payload TEXT)"
                    .into(),
                params: vec![],
            }],
        })
        .await?;

    println!(
        "[setup]  status={:?}  commit_seq={:?}",
        setup_resp.status, setup_resp.commit_seq
    );

    // --- Request 1: insert one event ---
    let resp1 = handle
        .execute(squeuelite::WriteRequest {
            request_id: "req-1".into(),
            actor_id: "agent-a".into(),
            run_id: Some("run-demo".into()),
            idempotency_key: None,
            operations: vec![squeuelite::SqlOperation {
                sql: "INSERT INTO events(agent_id, kind, payload) VALUES (?, ?, ?)".into(),
                params: vec![
                    serde_json::json!("agent-a"),
                    serde_json::json!("tool_result"),
                    serde_json::json!("{\"ok\":true}"),
                ],
            }],
        })
        .await?;

    println!(
        "[req-1]  status={:?}  commit_seq={:?}",
        resp1.status, resp1.commit_seq
    );

    // --- Request 2: two operations in a single atomic transaction (§11) ---
    let resp2 = handle
        .execute(squeuelite::WriteRequest {
            request_id: "req-2".into(),
            actor_id: "agent-b".into(),
            run_id: Some("run-demo".into()),
            idempotency_key: None,
            operations: vec![
                squeuelite::SqlOperation {
                    sql: "INSERT INTO events(agent_id, kind, payload) VALUES (?, ?, ?)".into(),
                    params: vec![
                        serde_json::json!("agent-b"),
                        serde_json::json!("started"),
                        serde_json::json!("{}"),
                    ],
                },
                squeuelite::SqlOperation {
                    sql: "INSERT INTO events(agent_id, kind, payload) VALUES (?, ?, ?)".into(),
                    params: vec![
                        serde_json::json!("agent-b"),
                        serde_json::json!("completed"),
                        serde_json::json!("{\"rows\":42}"),
                    ],
                },
            ],
        })
        .await?;

    println!(
        "[req-2]  status={:?}  commit_seq={:?}",
        resp2.status, resp2.commit_seq
    );

    // --- Graceful shutdown (runs WAL checkpoint, §16) ---
    gateway.shutdown().await?;
    println!("[done]   gateway shut down cleanly.");

    Ok(())
}
