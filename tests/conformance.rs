#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    adapter::ClaudeCodeAdapter,
    get::pond_get,
    ingest::{IngestEvent, IngestValidator, ingest_adapter},
    substrate::PondStore,
    types::{FileData, Message, Part, PartKind, ProviderOptions, Session},
    wire::{GetEnvelope, GetRequest},
};
use tempfile::TempDir;

#[tokio::test]
async fn claude_code_fixtures_round_trip_and_get() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");

    let summary = ingest_adapter(&store, &adapter).await?;
    assert_eq!(summary.errors, 0);

    // Read the ingested session ids back from the store rather than re-parsing
    // the fixture files that ingest already decoded.
    let session_ids = store.session_ids().await?;
    assert!(!session_ids.is_empty());

    for session_id in &session_ids {
        let envelope = pond_get(
            &store,
            GetRequest {
                protocol_version: pond::PROTOCOL_VERSION,
                namespace: "local".to_owned(),
                session_id: Some(session_id.clone()),
                message_id: None,
                up_to: None,
                context_depth: 0,
                max_messages: 1000,
                include_thinking: true,
                include_tool_results: true,
            },
        )
        .await;
        let GetEnvelope::Success(response) = envelope else {
            panic!("expected successful pond_get for {session_id}");
        };
        let pond::wire::GetResult::Session(stored) = response.result else {
            panic!("expected session result");
        };
        assert_eq!(stored.session.id, *session_id);
        assert!(!stored.messages.is_empty());

        let target = stored.messages[0].message.id().to_owned();
        let envelope = pond_get(
            &store,
            GetRequest {
                protocol_version: pond::PROTOCOL_VERSION,
                namespace: "local".to_owned(),
                session_id: None,
                message_id: Some(target),
                up_to: None,
                context_depth: 1,
                max_messages: 100,
                include_thinking: false,
                include_tool_results: false,
            },
        )
        .await;
        assert!(matches!(envelope, GetEnvelope::Success(_)));
    }

    Ok(())
}

#[tokio::test]
async fn ingest_is_idempotent_for_same_adapter() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");

    ingest_adapter(&store, &adapter).await?;
    let first_counts = store.row_counts().await?;
    let second = ingest_adapter(&store, &adapter).await?;
    let second_counts = store.row_counts().await?;

    assert_eq!(first_counts, second_counts);
    assert_eq!(second.errors, 0);
    assert_eq!(second.inserted, 0);
    assert!(second.matched > 0);

    Ok(())
}

#[tokio::test]
async fn ordering_contract_rejects_part_before_message() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let session = synthetic_session("ordering");
    let part = Part {
        id: "part-1".to_owned(),
        message_id: "missing-message".to_owned(),
        ordinal: 0,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: "orphan".to_owned(),
        },
    };

    let mut validator = IngestValidator::default();
    validator
        .push(&store, IngestEvent::Session(session))
        .await
        .expect("session writes");
    let error = validator
        .push(&store, IngestEvent::Part(part))
        .await
        .expect_err("part before message must fail");
    assert!(
        error
            .to_string()
            .contains("part event appeared before a message")
    );

    Ok(())
}

#[tokio::test]
async fn duplicate_message_id_aborts_session_before_write() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let session = synthetic_session("duplicate-message");
    let first = Message::User {
        id: "message-1".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let second = Message::Assistant {
        id: "message-1".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };

    let mut validator = IngestValidator::default();
    validator
        .push(&store, IngestEvent::Session(session.clone()))
        .await?;
    validator.push(&store, IngestEvent::Message(first)).await?;
    validator.push(&store, IngestEvent::Message(second)).await?;

    let error = validator
        .finish(&store)
        .await
        .expect_err("duplicate message id must fail the session");
    assert!(error.to_string().contains("duplicate message id message-1"));
    assert_eq!(store.row_counts().await?, (0, 0, 0, 0));

    Ok(())
}

#[tokio::test]
async fn file_part_blob_v2_round_trips_through_get() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let session = synthetic_session("blob");
    let message = Message::User {
        id: "message-1".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let part = Part {
        id: "part-1".to_owned(),
        message_id: message.id().to_owned(),
        ordinal: 0,
        options: ProviderOptions::new(),
        kind: PartKind::File {
            media_type: "text/plain".to_owned(),
            file_name: Some("payload.txt".to_owned()),
            data: FileData::Bytes(b"pond".to_vec()),
        },
    };

    let mut validator = IngestValidator::default();
    validator
        .push(&store, IngestEvent::Session(session.clone()))
        .await?;
    validator
        .push(&store, IngestEvent::Message(message.clone()))
        .await?;
    validator
        .push(&store, IngestEvent::Part(part.clone()))
        .await?;
    validator.finish(&store).await?;

    let stored = store
        .get_session(&session.id)
        .await?
        .expect("session should exist");
    let stored_part = &stored.messages[0].parts[0];
    assert_eq!(stored_part, &part);

    Ok(())
}

fn synthetic_session(id: &str) -> Session {
    Session {
        id: id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: Some("/tmp/pond".to_owned()),
        options: ProviderOptions::new(),
    }
}
