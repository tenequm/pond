//! Durable-copy story (spec.md#session-durable-copy): `pond copy --to <file>` produces a
//! portable snapshot of canonical session rows that can be ingested into a fresh store.
//! This test proves the loop round-trips identical row counts and identical
//! `pond copy --to <file>` output.
//!
//! Plus: the JSONL wire stream produces `IngestEvent`s that round-trip back
//! through `ingest_events`, so `copy --to - | ingest` is a portable backup.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::{ingest_adapter, ingest_events, pond_export},
    sessions::{IngestEvent, Store},
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

async fn full_export(store: &Store) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    pond_export(store, None, &mut buffer).await?;
    Ok(buffer)
}

fn parse_events(jsonl: &[u8]) -> anyhow::Result<Vec<IngestEvent>> {
    let text = std::str::from_utf8(jsonl)?;
    let mut events = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        events.push(serde_json::from_str::<IngestEvent>(line)?);
    }
    Ok(events)
}

#[tokio::test(flavor = "multi_thread")]
async fn rm_and_resync_round_trips_to_identical_state() -> anyhow::Result<()> {
    // First ingest: build the canonical state.
    let original = TempDir::new()?;
    let store = Arc::new(Store::open_local(original.path()).await?);
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let original_counts = store.row_counts().await?;
    let original_export = full_export(&store).await?;
    drop(store);

    // Simulate "rm -rf data_dir && pond sync": fresh data dir, same adapter
    // pointing at the same source. The recovery contract is that the new
    // state is byte-identical to the old state.
    let recovered = TempDir::new()?;
    let store = Store::open_local(recovered.path()).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let recovered_counts = store.row_counts().await?;
    let recovered_export = full_export(&store).await?;

    assert_eq!(
        original_counts, recovered_counts,
        "rm-and-resync must produce identical row counts",
    );
    assert_eq!(
        original_export, recovered_export,
        "rm-and-resync must produce identical canonical event streams",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn export_then_ingest_round_trips_canonical_events() -> anyhow::Result<()> {
    // Build the source state from a real adapter.
    let source = TempDir::new()?;
    let source_store = Store::open_local(source.path()).await?;
    ingest_adapter(
        &source_store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let source_export = full_export(&source_store).await?;
    let source_counts = source_store.row_counts().await?;

    // Round-trip the export back into a fresh store via `ingest_events`
    // (the wire-level path, same one HTTP `/v1/ingest` drives). Identical
    // row counts and identical re-export prove the format is lossless.
    let destination = TempDir::new()?;
    let dest_store = Store::open_local(destination.path()).await?;
    let events = parse_events(&source_export)?;
    assert!(!events.is_empty(), "fixture export must yield events");
    let outcomes = ingest_events(&dest_store, events).await?;
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.status != pond::sessions::OutcomeStatus::Error),
        "re-import must not produce any error outcomes",
    );

    let dest_counts = dest_store.row_counts().await?;
    let dest_export = full_export(&dest_store).await?;
    assert_eq!(source_counts, dest_counts);
    assert_eq!(source_export, dest_export);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_bypasses_the_freshness_skip_and_re_reads_every_session() -> anyhow::Result<()> {
    // `pond sync --verify` drives ingest with a `NoopOracle` instead of the
    // per-session watermark map, so the freshness gate never fires and every
    // source body is re-decoded. This is the only path that heals historical
    // M1 damage: a session partially flushed before the commit-row-last fix
    // keeps a frozen watermark mtime can never re-read past
    // (spec.md#session-movement-complete).
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let first = ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    assert!(first.sessions_inserted > 0, "fixtures must yield sessions");

    // Skip-everything oracle: claim pond wrote every session in the far
    // future, newer than any source mtime, so a normal sync skips them all.
    let future = chrono::Utc::now() + chrono::Duration::days(3650);
    let skip_all: std::collections::HashMap<String, chrono::DateTime<chrono::Utc>> = store
        .session_ids()
        .await?
        .into_iter()
        .map(|id| (id, future))
        .collect();
    let skipped =
        ingest_adapter(&store, &ClaudeCodeAdapter::new(FIXTURES), &skip_all, |_| {}).await?;
    assert_eq!(
        skipped.sessions_inserted, 0,
        "a future watermark must insert nothing"
    );
    assert!(
        skipped.skipped_fresh > 0,
        "a normal sync must skip the fresh sessions"
    );

    // `--verify` (NoopOracle): no session is skipped; the idempotent merge
    // re-reads every body and inserts nothing new on already-complete data.
    let verified = ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    assert_eq!(
        verified.skipped_fresh, 0,
        "--verify must not skip any session"
    );
    assert_eq!(
        verified.sessions_inserted, 0,
        "re-reading complete sessions is an idempotent no-op, not a duplicate insert"
    );
    assert_eq!(
        verified.storage_errors, 0,
        "--verify re-ingest must not error"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn export_filtered_to_one_session_carries_only_that_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;

    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("at least one session");

    let mut buffer = Vec::new();
    let summary = pond_export(&store, Some(&session_id), &mut buffer).await?;
    assert_eq!(
        summary.sessions, 1,
        "filter must restrict to exactly one session"
    );
    let events = parse_events(&buffer)?;
    for event in &events {
        let event_session = match event {
            IngestEvent::Session(session) => session.id.clone(),
            IngestEvent::Message(message) => message.session_id().to_owned(),
            IngestEvent::Part(part) => part.session_id.clone(),
        };
        assert_eq!(
            event_session, session_id,
            "no event from a different session should appear in the filtered export",
        );
    }
    Ok(())
}
