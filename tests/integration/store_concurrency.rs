//! Multi-writer Store concurrency on a shared local-FS data dir (design.md
//! 2.4): Lance's local commit lock plus pond's retry layer must serialize
//! concurrent writers without surfacing a spurious `Conflict`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Utc;
use pond::{
    sessions::Store,
    wire::{ProviderOptions, Session},
};
use tempfile::TempDir;

fn make_session(id: usize) -> Session {
    Session {
        id: format!("01HXY{id:08}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": format!("/tmp/p/{id}")}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_on_same_data_dir_serialize_without_conflict() -> anyhow::Result<()> {
    // Two pond Stores share one local data dir; Lance's local commit lock
    // plus pond's retry layer must turn the contention into successful
    // serialized commits, not surfaced `Conflict`. This is the v1 invariant
    // (design.md#invariants-concurrency): "Local filesystem uses Lance's internal commit lock."
    let temp = TempDir::new()?;
    let store_a = Arc::new(Store::open_local(temp.path()).await?);
    let store_b = Arc::new(Store::open_local(temp.path()).await?);

    let sessions_a: Vec<Session> = (0..10).map(make_session).collect();
    let sessions_b: Vec<Session> = (10..20).map(make_session).collect();

    let a = tokio::spawn({
        let store = Arc::clone(&store_a);
        async move { store.upsert_sessions(&sessions_a).await }
    });
    let b = tokio::spawn({
        let store = Arc::clone(&store_b);
        async move { store.upsert_sessions(&sessions_b).await }
    });
    let (out_a, out_b) = tokio::join!(a, b);
    // The primary invariant: neither writer surfaces a spurious `Conflict`.
    // Local-FS commits serialize at Lance's commit lock; pond's retry layer
    // converts transient lock contention into ordered commits, not failures.
    out_a??;
    out_b??;

    // Either existing store handle sees the converged state. Local-FS
    // refresh window is zero (design.md#inv-4), so a long-lived reader
    // picks up another writer's commit on the next read without waiting.
    let (sessions, _, _, _) = store_a.row_counts().await?;
    assert_eq!(
        sessions, 20,
        "concurrent writers must produce union of rows"
    );
    Ok(())
}
