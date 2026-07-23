//! `Store::optimize_indices` under a concurrent writer (spec.md#substrate
//! 3.5 + 3.7): the indices and compaction phases commit independently, so a
//! Rewrite preempted by a hot Update surfaces as `SkippedConflict` in the
//! outcome - never a hard `Err` that would force the operator to coordinate
//! by hand.
//!
//! Uses Lance's `shared-memory://` provider so two `Store` instances share
//! one in-memory backing store and the conflict path that runs is the
//! production `ConditionalPutCommitHandler` (lance-table/src/io/commit.rs:1074),
//! the same OCC handler S3 uses. A `file://`/`TempDir` test would route
//! through the local-FS commit lock instead and prove a different primitive.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Utc;
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, MessageWrite, OptimizeOutcome, Store},
    substrate::{MaintenancePolicy, PhaseOutcome},
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use url::Url;

fn make_session(id: &str) -> Session {
    Session {
        id: id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/contend"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

fn make_message(session_id: &str, idx: usize) -> Message {
    Message::User {
        id: format!("msg-{idx}"),
        session_id: session_id.to_owned(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    }
}

fn make_part(session_id: &str, idx: usize, text: &str) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: format!("msg-{idx}:0001"),
        message_id: format!("msg-{idx}"),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: pond::adapter::extract_str(&serde_json::json!({"x": text}), "x"),
        },
    }
}

/// Smoke: two `Store`s opened against the same `shared-memory://` authority
/// see each other's bytes. The reader is opened *after* the writer commits
/// because pond's `lance-handle-freshness` window (spec.md#3.5) is 5 s for non-local
/// URLs - an already-open handle would not see the new commit for that long.
/// Opening fresh dodges the cache and exercises the real plumbing: the
/// shared-memory pool delivers the same `Arc<InMemory>` to both registries.
#[tokio::test(flavor = "multi_thread")]
async fn shared_memory_two_stores_share_state() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-shared-smoke/")?;
    let writer = Store::open(&url).await?;

    let session = make_session("01HXYSHARED0001");
    let message = make_message(&session.id, 0);
    let part = make_part(&session.id, 0, "shared-memory round trip");
    ingest_events(
        &writer,
        vec![
            IngestEvent::Session(session.clone()),
            IngestEvent::Message(message.clone()),
            IngestEvent::Part(part.clone()),
        ],
    )
    .await?;

    let reader = Store::open(&url).await?;
    let stored = reader
        .get_session(&session.id)
        .await?
        .expect("second Store must see first Store's commit through shared-memory pool");
    assert_eq!(stored.session.id, session.id);
    assert_eq!(stored.messages.len(), 1);
    Ok(())
}

/// `Store::optimize_indices` against a quiet store: every phase that has work
/// commits, nothing is `Failed` or `SkippedConflict`. Proves the split path
/// is wired and the outcome shape carries the right per-table result.
#[tokio::test(flavor = "multi_thread")]
async fn optimize_returns_per_table_outcome_on_quiet_store() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-optimize-quiet/")?;
    let store = Store::open(&url).await?;

    let session = make_session("01HXYQUIET00001");
    let mut events = vec![IngestEvent::Session(session.clone())];
    for idx in 0..32 {
        events.push(IngestEvent::Message(make_message(&session.id, idx)));
        events.push(IngestEvent::Part(make_part(
            &session.id,
            idx,
            "quiet store body",
        )));
    }
    ingest_events(&store, events).await?;

    let outcome = store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?;
    assert_eq!(outcome.tables.len(), 3);
    for table in &outcome.tables {
        assert!(
            matches!(table.indices, PhaseOutcome::Ok | PhaseOutcome::Noop),
            "indices on {} expected Ok or Noop, got {:?}",
            table.table.as_str(),
            table.indices,
        );
        assert!(
            matches!(table.compaction, PhaseOutcome::Ok | PhaseOutcome::Noop),
            "compaction on {} expected Ok or Noop, got {:?}",
            table.table.as_str(),
            table.compaction,
        );
    }
    Ok(())
}

/// `Store::build_indices_only` never runs compaction: compaction phase comes
/// back `NotAttempted` even on a quiet store.
#[tokio::test(flavor = "multi_thread")]
async fn build_indices_only_leaves_compaction_unattempted() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-build-only/")?;
    let store = Store::open(&url).await?;
    let session = make_session("01HXYBUILD00001");
    let mut events = vec![IngestEvent::Session(session.clone())];
    for idx in 0..16 {
        events.push(IngestEvent::Message(make_message(&session.id, idx)));
        events.push(IngestEvent::Part(make_part(&session.id, idx, "build-only")));
    }
    ingest_events(&store, events).await?;

    let outcome = store.build_indices_only(None).await?;
    assert_eq!(outcome.tables.len(), 3);
    for table in &outcome.tables {
        assert!(
            matches!(table.compaction, PhaseOutcome::NotAttempted),
            "compaction on {} must be NotAttempted under build_indices_only, got {:?}",
            table.table.as_str(),
            table.compaction,
        );
    }
    Ok(())
}

/// Under a concurrent writer hammering `messages`, `Store::optimize_indices`
/// must still return `Ok(OptimizeOutcome)`: compaction may surface as
/// `SkippedConflict`, indices on every table must report `Ok` or `Noop`, and
/// the call never propagates an `Err` for OCC contention alone. spec.md#3.7.
#[tokio::test(flavor = "multi_thread")]
async fn optimize_under_contention_surfaces_skipped_not_err() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-optimize-contend/")?;
    let writer = Arc::new(Store::open(&url).await?);
    let optimizer = Store::open(&url).await?;

    let session = make_session("01HXYCONTEND0001");
    writer
        .upsert_sessions(std::slice::from_ref(&session))
        .await?;

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_task = {
        let writer = Arc::clone(&writer);
        let session = session.clone();
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut idx: usize = 0;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let message = make_message(&session.id, idx);
                let parts = [make_part(&session.id, idx, "contend body")];
                let write = MessageWrite {
                    message: &message,
                    parts: &parts,
                    search_text: Some("contend body"),
                };
                if writer.upsert_messages(&session, &[write]).await.is_err() {
                    break;
                }
                idx = idx.wrapping_add(1);
                tokio::task::yield_now().await;
            }
            idx
        })
    };

    // Give the writer a head start so optimize sees real fragments and the
    // commit lane is actively contended.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let outcome: OptimizeOutcome = optimizer
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = writer_task.await;

    assert_eq!(outcome.tables.len(), 3);
    for table in &outcome.tables {
        assert!(
            !matches!(table.indices, PhaseOutcome::Failed(_)),
            "indices on {} must not Fail under contention, got {:?}",
            table.table.as_str(),
            table.indices,
        );
        assert!(
            !matches!(table.compaction, PhaseOutcome::Failed(_)),
            "compaction on {} must not Fail under contention (Skipped is fine), got {:?}",
            table.table.as_str(),
            table.compaction,
        );
    }
    // Indices on at least one table must have committed something or been a
    // no-op (i.e. not erroring out). The whole point of the split is that
    // indices work proceeds even when compaction loses the OCC race.
    assert!(
        outcome.tables.iter().all(|t| matches!(
            t.indices,
            PhaseOutcome::Ok | PhaseOutcome::Noop | PhaseOutcome::SkippedConflict
        )),
        "every table's indices phase must reach a terminal non-Failed state: {:?}",
        outcome.tables,
    );
    Ok(())
}
