//! Concurrent stress tests — verify the single-writer invariant under real load.
//!
//! These tests exist to confirm that the gateway's core property holds: no matter
//! how many concurrent callers submit writes simultaneously, all writes are
//! serialised through one SQLite writer thread with no gaps, no duplicates, and
//! a continuous `commit_seq` sequence.
//!
//! Two variants are provided:
//!
//! - **In-process** (`#[cfg(feature = "inprocess")]`): 16 tasks × 100 INSERTs
//!   each (1 600 total) sharing one `GatewayHandle`.  After shutdown, a
//!   read-only `rusqlite` connection opens the **file** DB and counts rows.
//!
//! - **Sidecar / UDS** (`#[cfg(feature = "sidecar")]`): 8 `Client`s (one per
//!   task) × 50 INSERTs each (400 total).  Validates row count and commit_seq
//!   continuity.

// ---------------------------------------------------------------------------
// In-process concurrent stress test
// ---------------------------------------------------------------------------

#[cfg(feature = "inprocess")]
mod inprocess_stress {
    use std::collections::BTreeSet;

    use squeuelite::{GatewayConfig, InProcessGateway, SqlOperation, WriteRequest, WriteStatus};

    /// Generate a unique temporary DB file path.
    fn temp_db_path(tag: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::path::PathBuf::from(format!(
            "/tmp/squeuelite_stress_{tag}_{id}_{}.db",
            std::process::id()
        ))
    }

    /// Remove a DB file and its WAL / SHM siblings.
    fn cleanup_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_inserts_single_writer() {
        const TASKS: usize = 16;
        const INSERTS_PER_TASK: usize = 100;
        const TOTAL: usize = TASKS * INSERTS_PER_TASK;

        let db_path = temp_db_path("inprocess");

        // Open a file-based gateway (not :memory:) so we can read it back
        // with a separate rusqlite connection after shutdown.
        let mut config = GatewayConfig::new(&db_path);
        config.track_commits = true;
        config.allow_schema_write = true;
        let gw = InProcessGateway::open_with_config(config).expect("open gateway");
        let handle = gw.handle();

        // CREATE TABLE via the writer.
        let create_resp = handle
            .execute(WriteRequest {
                request_id: "stress-create".into(),
                actor_id: "stress-test".into(),
                run_id: None,
                idempotency_key: None,
                operations: vec![SqlOperation {
                    sql: "CREATE TABLE stress_events (id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL, seq INTEGER NOT NULL)".into(),
                    params: vec![],
                }],
            })
            .await
            .expect("CREATE TABLE");
        assert_eq!(
            create_resp.status,
            WriteStatus::Committed,
            "CREATE must commit"
        );

        // Spawn TASKS concurrent tasks, each doing INSERTS_PER_TASK INSERTs.
        let mut join_handles = Vec::with_capacity(TASKS);
        for task_id in 0..TASKS {
            let h = handle.clone();
            join_handles.push(tokio::spawn(async move {
                let mut commit_seqs = Vec::with_capacity(INSERTS_PER_TASK);
                for seq in 0..INSERTS_PER_TASK {
                    let resp = h
                        .execute(WriteRequest {
                            request_id: format!("stress-{task_id}-{seq}"),
                            actor_id: "stress-test".into(),
                            run_id: None,
                            idempotency_key: None,
                            operations: vec![SqlOperation {
                                sql: "INSERT INTO stress_events(task_id, seq) VALUES (?, ?)".into(),
                                params: vec![
                                    serde_json::json!(task_id as i64),
                                    serde_json::json!(seq as i64),
                                ],
                            }],
                        })
                        .await
                        .expect("execute INSERT");

                    assert_eq!(
                        resp.status,
                        WriteStatus::Committed,
                        "task={task_id} seq={seq} must commit; error={:?}",
                        resp.error
                    );
                    assert!(
                        resp.commit_seq.is_some(),
                        "task={task_id} seq={seq}: commit_seq must be present"
                    );
                    commit_seqs.push(resp.commit_seq.unwrap());
                }
                commit_seqs
            }));
        }

        // Collect all commit_seqs from all tasks.
        let mut all_commit_seqs: Vec<i64> = Vec::with_capacity(TOTAL);
        for jh in join_handles {
            let seqs = jh.await.expect("task panicked");
            all_commit_seqs.extend(seqs);
        }

        // Verify: all TOTAL writes committed.
        assert_eq!(
            all_commit_seqs.len(),
            TOTAL,
            "must have exactly {TOTAL} commit_seqs"
        );

        // Verify: no duplicates — each commit_seq is unique.
        let unique: BTreeSet<i64> = all_commit_seqs.iter().copied().collect();
        assert_eq!(
            unique.len(),
            TOTAL,
            "commit_seqs must all be unique (no duplicates); \
             got {} unique out of {TOTAL}",
            unique.len()
        );

        // Verify: commit_seq forms a contiguous range (no gaps).
        // The CREATE TABLE also consumes one commit_seq before the inserts, so
        // the insert seqs occupy a contiguous block somewhere in [2..=TOTAL+1].
        // We just verify min..=max covers exactly TOTAL consecutive integers.
        let min_seq = *unique.iter().next().unwrap();
        let max_seq = *unique.iter().next_back().unwrap();
        assert_eq!(
            (max_seq - min_seq + 1) as usize,
            TOTAL,
            "commit_seqs must form a contiguous range [{min_seq}..={max_seq}] \
             of length {TOTAL}; got {} unique values",
            unique.len()
        );

        // Graceful shutdown (includes WAL checkpoint).
        gw.shutdown().await.expect("shutdown");

        // Open a read-only rusqlite connection and verify row count.
        {
            use rusqlite::{Connection, OpenFlags};
            let read_conn = Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .expect("open read-only");
            let row_count: i64 = read_conn
                .query_row("SELECT COUNT(*) FROM stress_events", [], |r| r.get(0))
                .expect("COUNT(*)");
            assert_eq!(
                row_count, TOTAL as i64,
                "read-only connection must see {TOTAL} rows after shutdown"
            );
        }

        cleanup_db(&db_path);
    }
}

// ---------------------------------------------------------------------------
// Sidecar / UDS concurrent stress test
// ---------------------------------------------------------------------------

#[cfg(feature = "sidecar")]
mod sidecar_stress {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

    use tokio::sync::Notify;

    use squeuelite::{Client, GatewayConfig, SidecarConfig, SidecarGateway, SqlOperation};

    /// Generate a unique temp path prefix.
    fn temp_prefix() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        format!("/tmp/squeuelite_sidecar_stress_{id}_{}", std::process::id())
    }

    fn cleanup(paths: &[&PathBuf]) {
        for p in paths {
            let _ = std::fs::remove_file(p);
            let _ = std::fs::remove_file(format!("{}-wal", p.display()));
            let _ = std::fs::remove_file(format!("{}-shm", p.display()));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sidecar_concurrent_inserts_single_writer() {
        const CLIENTS: usize = 8;
        const INSERTS_PER_CLIENT: usize = 50;
        const TOTAL: usize = CLIENTS * INSERTS_PER_CLIENT;

        let prefix = temp_prefix();
        let db_path = PathBuf::from(format!("{prefix}.db"));
        let socket_path = PathBuf::from(format!("{prefix}.sock"));

        // Start the gateway.
        let mut gateway_config = GatewayConfig::new(&db_path);
        gateway_config.allow_schema_write = true;
        gateway_config.track_commits = true;
        let config = SidecarConfig {
            gateway: gateway_config,
            socket_path: socket_path.clone(),
            socket_mode: 0o600,
        };
        let gateway = SidecarGateway::open(config).expect("open gateway");

        let notify = Arc::new(Notify::new());
        let notify_clone = Arc::clone(&notify);
        let socket_clone = socket_path.clone();

        tokio::spawn(async move {
            let shutdown_fut = async move { notify_clone.notified().await };
            gateway.run(shutdown_fut).await.expect("gateway run");
        });

        // Wait for socket to appear.
        for _ in 0..100 {
            if socket_path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(socket_clone.exists(), "socket must be created");

        // CREATE TABLE via the first client.
        {
            let mut setup_client = Client::connect("stress-setup", &socket_path)
                .await
                .expect("connect setup");
            let resp = setup_client
                .execute(SqlOperation {
                    sql: "CREATE TABLE sidecar_stress (id INTEGER PRIMARY KEY AUTOINCREMENT, client_id INTEGER NOT NULL, seq INTEGER NOT NULL)".into(),
                    params: vec![],
                })
                .await
                .expect("CREATE TABLE");
            assert_eq!(
                resp.status,
                squeuelite::WriteStatus::Committed,
                "CREATE must commit"
            );
        }

        // Spawn CLIENTS concurrent tasks.  Each task opens its own Client
        // (one connection = one in-flight request at a time).
        let mut join_handles = Vec::with_capacity(CLIENTS);
        for client_id in 0..CLIENTS {
            let sock = socket_path.clone();
            join_handles.push(tokio::spawn(async move {
                let mut client = Client::connect(&format!("stress-client-{client_id}"), &sock)
                    .await
                    .expect("connect");

                let mut commit_seqs = Vec::with_capacity(INSERTS_PER_CLIENT);
                for seq in 0..INSERTS_PER_CLIENT {
                    let resp = client
                        .execute(SqlOperation {
                            sql: "INSERT INTO sidecar_stress(client_id, seq) VALUES (?, ?)".into(),
                            params: vec![
                                serde_json::json!(client_id as i64),
                                serde_json::json!(seq as i64),
                            ],
                        })
                        .await
                        .expect("execute INSERT");

                    assert_eq!(
                        resp.status,
                        squeuelite::WriteStatus::Committed,
                        "client={client_id} seq={seq} must commit; error={:?}",
                        resp.error
                    );
                    if let Some(cs) = resp.commit_seq {
                        commit_seqs.push(cs);
                    }
                }
                commit_seqs
            }));
        }

        // Collect all commit_seqs.
        let mut all_commit_seqs: Vec<i64> = Vec::new();
        for jh in join_handles {
            let seqs = jh.await.expect("task panicked");
            all_commit_seqs.extend(seqs);
        }

        // Verify: all TOTAL responses returned a commit_seq.
        assert_eq!(
            all_commit_seqs.len(),
            TOTAL,
            "must collect {TOTAL} commit_seqs (one per INSERT)"
        );

        // Verify: no duplicates.
        let unique: BTreeSet<i64> = all_commit_seqs.iter().copied().collect();
        assert_eq!(
            unique.len(),
            TOTAL,
            "commit_seqs must be unique; got {} unique out of {TOTAL}",
            unique.len()
        );

        // Verify: contiguous range among the INSERT commit_seqs.
        let min_seq = *unique.iter().next().unwrap();
        let max_seq = *unique.iter().next_back().unwrap();
        assert_eq!(
            (max_seq - min_seq + 1) as usize,
            TOTAL,
            "commit_seqs must form a contiguous range [{min_seq}..={max_seq}]"
        );

        // Trigger graceful shutdown.
        notify.notify_one();
        // Give the gateway time to finish the WAL checkpoint.
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Verify row count via read-only rusqlite connection.
        {
            use rusqlite::{Connection, OpenFlags};
            let read_conn = Connection::open_with_flags(
                &db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .expect("open read-only");
            let row_count: i64 = read_conn
                .query_row("SELECT COUNT(*) FROM sidecar_stress", [], |r| r.get(0))
                .expect("COUNT(*)");
            assert_eq!(
                row_count, TOTAL as i64,
                "read-only connection must see {TOTAL} rows after shutdown"
            );
        }

        cleanup(&[&socket_path, &db_path]);
    }
}
