//! Recovery story (design.md#inv-10): every byte in pond's datasets is
//! derivable from the registered adapters' source data. The recovery path
//! for any corruption is `rm -rf $POND_DATA_DIR && pond sync`; this test
//! proves the loop actually round-trips identical row counts and identical
//! `pond export` output.
//!
//! Plus: `pond export` produces JSONL `IngestEvent`s that round-trip back
//! through `ingest_events`, so `export | ingest` is a portable backup.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::{ingest_adapter, ingest_events, pond_export},
    sessions::{IngestEvent, Store},
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/claude-code/projects";

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
    // Build the message_id -> session_id map from the Session and Message
    // events in the export, then assert every Part's message_id resolves
    // to the requested session. Parts don't carry session_id directly, so
    // the map is the only way to verify the filter reaches them too.
    let mut message_to_session: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for event in &events {
        if let IngestEvent::Message(message) = event {
            message_to_session.insert(message.id().to_owned(), message.session_id().to_owned());
        }
    }
    for event in &events {
        let event_session = match event {
            IngestEvent::Session(session) => session.id.clone(),
            IngestEvent::Message(message) => message.session_id().to_owned(),
            IngestEvent::Part(part) => message_to_session
                .get(part.message_id.as_str())
                .cloned()
                .expect("every exported Part must reference a Message in the same export"),
        };
        assert_eq!(
            event_session, session_id,
            "no event from a different session should appear in the filtered export",
        );
    }
    Ok(())
}
