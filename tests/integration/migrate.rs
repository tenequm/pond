//! `pond migrate` data path (spec.md#substrate): export the source's clean
//! datasets, merge-import into the destination. The properties under test
//! are the plan's contract: round-trip, rerun-is-a-no-op, and union onto a
//! populated destination - all consequences of `lance-deterministic-pk` +
//! merge-insert, asserted here rather than promised.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, OutcomeStatus, Store},
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use tempfile::TempDir;
use url::Url;

fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

fn make_events(session_id: &str) -> Vec<IngestEvent> {
    let session = Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/migrate"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    };
    let message = Message::User {
        id: format!("{session_id}-msg-1"),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let part = Part {
        session_id: session.id.clone(),
        id: format!("{session_id}-msg-1:0001"),
        message_id: message.id().to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: s("migrate me"),
        },
    };
    vec![
        IngestEvent::Session(session),
        IngestEvent::Message(message),
        IngestEvent::Part(part),
    ]
}

async fn seed(store: &Store, session_id: &str) -> anyhow::Result<()> {
    let outcomes = ingest_events(store, make_events(session_id)).await?;
    assert!(
        outcomes.iter().all(|o| o.status != OutcomeStatus::Error),
        "seed ingest must not error: {outcomes:?}",
    );
    Ok(())
}

/// Run the migrate composition once: source -> staging -> destination.
async fn migrate(from: &Store, to: &Store) -> anyhow::Result<pond::sessions::LanceArchiveImport> {
    let staging = TempDir::new()?;
    let data_dir = staging.path().join("data");
    from.export_clean_lance_datasets(&data_dir).await?;
    to.import_clean_lance_datasets(&data_dir).await
}

#[tokio::test(flavor = "multi_thread")]
async fn migrate_round_trips_reruns_as_noop_and_unions() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-migrate-src/")?).await?;
    seed(&source, "01HXYMIGRATE0001").await?;
    seed(&source, "01HXYMIGRATE0002").await?;

    // Round trip into an empty destination: everything inserts.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-migrate-dst/")?).await?;
    let first = migrate(&source, &dest).await?;
    assert_eq!(first.inserted.sessions, 2);
    assert_eq!(first.inserted.messages, 2);
    assert_eq!(first.inserted.parts, 2);
    let stored = dest
        .get_session("01HXYMIGRATE0001")
        .await?
        .expect("migrated session readable on destination");
    assert_eq!(stored.messages.len(), 1);
    assert_eq!(stored.messages[0].parts.len(), 1);

    // Immediate rerun is a no-op: deterministic PKs make merge-insert skip
    // every row that already landed.
    let rerun = migrate(&source, &dest).await?;
    assert_eq!(rerun.inserted.sessions, 0, "rerun must insert nothing");
    assert_eq!(rerun.inserted.messages, 0);
    assert_eq!(rerun.inserted.parts, 0);

    // Union onto a populated destination: pre-existing rows survive, the
    // archive's rows merge in, nothing is deleted.
    let populated = Store::open(&Url::parse("shared-memory://pond-test-migrate-union/")?).await?;
    seed(&populated, "01HXYMIGRATELOCAL").await?;
    let union = migrate(&source, &populated).await?;
    assert_eq!(union.inserted.sessions, 2);
    let (sessions, messages, parts) = populated.row_counts().await?;
    assert_eq!(sessions, 3, "union must keep the destination's own rows");
    assert_eq!(messages, 3);
    assert_eq!(parts, 3);
    // And the source is untouched.
    let (src_sessions, _, _) = source.row_counts().await?;
    assert_eq!(src_sessions, 2);
    Ok(())
}
