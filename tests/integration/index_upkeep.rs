#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Write-path index upkeep (spec.md#index-upkeep): every ingest route builds
//! and folds the FTS index, and retrieval finds a message by a verbatim
//! multi-word phrase it contains.

use chrono::Utc;
use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::{ingest_adapter, ingest_events, pond_search},
    sessions::{IngestEvent, Store},
    wire::{
        Message, Part, PartKind, Provenance, ProviderOptions, SearchEnvelope, SearchRequest,
        SearchResultBody, Session,
    },
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

fn extracted(value: &str) -> pond::adapter::Extracted<String> {
    pond::adapter::extract_str(&serde_json::json!({ "x": value }), "x").unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn adapter_ingest_builds_the_fts_index() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);

    // Ingest builds and folds the index on the write path - no separate verb.
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let backlog = store.unindexed_message_backlog().await?;
    assert_eq!(
        backlog, 0,
        "the FTS index must exist and cover every message after ingest"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn handler_ingest_indexes_and_a_phrase_is_retrievable() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let session = Session {
        id: "index-session".to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: extracted("/tmp/index-test"),
        options: ProviderOptions::new(),
    };
    let message = Message::User {
        id: "index-message".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let phrase = "the quick brown fox jumps over the lazy dog";
    let part = Part {
        session_id: session.id.clone(),
        id: "index-message:0000".to_owned(),
        message_id: message.id().to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: Some(extracted(phrase)),
        },
    };

    // The handler ingest path - the same one HTTP and MCP dispatch into -
    // must build the index, not just `pond sync`.
    ingest_events(
        &store,
        vec![
            IngestEvent::Session(session),
            IngestEvent::Message(message),
            IngestEvent::Part(part),
        ],
    )
    .await?;

    assert_eq!(
        store.unindexed_message_backlog().await?,
        0,
        "handler ingest must fold the FTS index"
    );

    let request = SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: None,
        query: phrase.to_owned(),
        rrf_k: 60,
        filters: Default::default(),
        boost_recent: true,
        group_by_conversation: false,
        limit: 10,
    };
    let envelope = pond_search(&store, None, request).await;
    let SearchEnvelope::Success(response) = envelope else {
        panic!("search must succeed");
    };
    let SearchResultBody::Hits { hits } = response.result else {
        panic!("ungrouped search returns hits");
    };
    assert!(
        hits.iter().any(|hit| hit.message_id == "index-message"),
        "the verbatim phrase must retrieve the message that contains it"
    );
    Ok(())
}
