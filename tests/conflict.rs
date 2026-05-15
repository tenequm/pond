//! OCC commit-conflict classification (design.md 3.6.1 `conflict` row).
//!
//! pond's retry layer classifies an exhausted commit-conflict failure as a
//! typed [`Error::Conflict`] rather than the generic `storage_unavailable`
//! bucket. Three coverage levels:
//!
//! 1. Wire mapping: `Error::Conflict { attempts }` -> envelope code + details.
//! 2. Sentinel chain: `ConflictExhausted` attached via `anyhow::Error::context`
//!    survives downcast through the chain.
//! 3. Concurrent writers on the same local-FS data dir succeed without
//!    surfacing a spurious `Conflict` (Lance's commit lock + pond's retry
//!    serialize commits at the manifest layer).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Utc;
use pond::{
    Error,
    sessions::Store,
    substrate::{ConflictExhausted, is_commit_conflict},
    wire::{ErrorCode, ErrorEnvelope, ProviderOptions, Session},
};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn wire_envelope_carries_conflict_code_and_attempts_detail() {
    let envelope: ErrorEnvelope = Error::Conflict { attempts: 3 }.into();
    assert_eq!(envelope.error.code, ErrorCode::Conflict);
    assert_eq!(envelope.error.details, json!({ "attempts": 3 }));
    assert!(!envelope.request_id.is_empty(), "request_id must be set");
}

#[test]
fn conflict_exhausted_sentinel_round_trips_through_anyhow_chain() {
    // Mirrors what `substrate::retry_lance` does at the exhaustion arm: attach
    // `ConflictExhausted` as the outer context on the underlying Lance error.
    let underlying = anyhow::anyhow!("simulated lance commit-conflict source");
    let attached = underlying.context(ConflictExhausted { attempts: 7 });

    let conflict = attached
        .downcast_ref::<ConflictExhausted>()
        .expect("ConflictExhausted must be reachable via downcast");
    assert_eq!(conflict.attempts, 7);

    // A non-conflict error never matches the classifier.
    let plain = anyhow::anyhow!("generic io failure");
    assert!(
        !is_commit_conflict(&plain),
        "plain anyhow strings must not be classified as conflicts",
    );
}

fn make_session(id: usize) -> Session {
    Session {
        id: format!("01HXY{id:08}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: Some(format!("/tmp/p/{id}")),
        options: ProviderOptions::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_on_same_data_dir_serialize_without_conflict() -> anyhow::Result<()> {
    // Two pond Stores share one local data dir; Lance's local commit lock
    // plus pond's retry layer must turn the contention into successful
    // serialized commits, not surfaced `Conflict`. This is the v1 invariant
    // (design.md 2.4): "Local filesystem uses Lance's internal commit lock."
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
    // refresh window is zero (design.md 2.3 inv 4), so a long-lived reader
    // picks up another writer's commit on the next read without waiting.
    let (sessions, _, _, _) = store_a.row_counts().await?;
    assert_eq!(sessions, 20, "concurrent writers must produce union of rows");
    Ok(())
}
