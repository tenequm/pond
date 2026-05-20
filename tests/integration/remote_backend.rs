//! Object-store / remote backend integration (spec.md#handle-freshness,
//! spec.md#substrate).
//!
//! Lance's default `ObjectStoreRegistry` ships `memory://`, which we use as
//! the cheap stand-in for an S3 backend: no daemon, no credentials, no
//! network. This proves pond's NON-local code paths actually exercise the
//! object-store path through `open_or_create_via_ns`, the 90-day retention
//! window, and the shared `lance::Session` routing all four datasets through
//! one ObjectStoreRegistry. The classifier helpers (`is_local`, `local_path`)
//! live in `src/config.rs::tests`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    config::parse_data_dir,
    handlers::ingest_events,
    sessions::{IngestEvent, OutcomeStatus, Store},
    wire::{Message, Part, PartKind, ProviderOptions, Session},
};

use url::Url;

/// Build an `Option<Extracted<String>>` for test fixtures. Integration tests
/// can't see `Extracted::from_test_value` (cfg-test-gated inside the pond
/// crate), so we go through the public `extract_str` producer on a
/// synthetic JSON source.
fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

fn memory_url() -> Url {
    parse_data_dir("memory:///pond-remote-test").expect("memory uri parses")
}

#[tokio::test(flavor = "multi_thread")]
async fn store_open_against_memory_uri_round_trips_a_session() -> anyhow::Result<()> {
    let url = memory_url();
    let store = Store::open(&url).await?;

    let session = Session {
        id: "01HXY00000000001".to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/remote-test"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    };
    let message = Message::User {
        id: "msg-1".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let part = Part {
        id: "msg-1:0001".to_owned(),
        message_id: message.id().to_owned(),
        ordinal: 0,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: s("hello from a remote-backed pond"),
        },
    };

    let outcomes = ingest_events(
        &store,
        vec![
            IngestEvent::Session(session.clone()),
            IngestEvent::Message(message.clone()),
            IngestEvent::Part(part.clone()),
        ],
    )
    .await?;
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.status != OutcomeStatus::Error),
        "remote-backed ingest must not produce errors: {outcomes:?}",
    );

    let stored = store
        .get_session(&session.id)
        .await?
        .expect("session round-trips from memory backend");
    assert_eq!(stored.session.id, session.id);
    assert_eq!(stored.messages.len(), 1);
    assert_eq!(stored.messages[0].parts.len(), 1);
    assert_eq!(stored.messages[0].parts[0], part);
    Ok(())
}
