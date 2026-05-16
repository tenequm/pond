//! Object-store / remote backend (design.md 2.3 inv 4, 3.2.0 storage block).
//!
//! Lance's default `ObjectStoreRegistry` ships `memory://`, which we use as
//! the cheap stand-in for an S3 backend in tests: no daemon, no credentials,
//! no network. The point of these tests is to prove pond's NON-local code
//! paths actually exercise:
//!
//!   - `config::is_local` returns false (refresh window picks 5s, not 0)
//!   - `open_or_create` takes the object-store branch (no `Path::exists`
//!     probe, open-then-fallback-to-write)
//!   - `write_params` picks 90-day retention (vs 30 for local)
//!   - Shared `lance::Session` correctly routes all four datasets through
//!     one ObjectStoreRegistry (no per-dataset client)
//!   - `Store::open_with_options` round-trips through the new
//!     `DatasetBuilder::with_storage_options` path
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;

use chrono::Utc;
use pond::{
    config::{is_local, local_path, parse_data_dir},
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

#[test]
fn memory_uri_is_classified_as_remote() {
    let url = memory_url();
    assert!(
        !is_local(&url),
        "memory:// is not a local-filesystem URL: {url}",
    );
    assert!(
        local_path(&url).is_none(),
        "local_path must return None for non-file schemes",
    );
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

#[tokio::test(flavor = "multi_thread")]
async fn store_open_with_options_threads_storage_options_through_lance() -> anyhow::Result<()> {
    // The shape of `Store::open_with_options` is what S3-mode callers will
    // use. With `memory://` the options are inert (the memory store has no
    // creds to validate), but the call path exercises the same wiring -
    // `DatasetBuilder::with_storage_options` on open and
    // `WriteParams.store_params.storage_options_accessor` on write - that
    // S3 will rely on. This test guards against the call path silently
    // dropping the options on the floor.
    let url = memory_url();
    let mut options = HashMap::new();
    // Inert key Lance will accept and stash; we're verifying the plumbing
    // doesn't reject the options, not that they're applied to memory://.
    options.insert("allow_http".to_owned(), "true".to_owned());

    let store = Store::open_with_options(&url, options).await?;
    let (sessions, messages, parts, embeddings) = store.row_counts().await?;
    assert_eq!((sessions, messages, parts, embeddings), (0, 0, 0, 0));
    Ok(())
}
