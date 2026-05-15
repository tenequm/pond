#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::pond_get,
    handlers::{IngestEvent, IngestValidator, SyncEvent, SyncStatus, ingest_adapter},
    sessions::{OutcomeStatus, Store},
    wire::{FileData, Message, Part, PartKind, ProviderOptions, Session},
    wire::{GetEnvelope, GetRequest},
};
use tempfile::TempDir;

#[tokio::test]
async fn claude_code_fixtures_round_trip_and_get() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");

    let summary = ingest_adapter(&store, &adapter, |_| {}).await?;
    assert_eq!(summary.dropped_events, 0);
    assert_eq!(summary.dropped_sessions, 0);
    assert_eq!(summary.skipped_files, 0);

    // Read the ingested session ids back from the store rather than re-parsing
    // the fixture files that ingest already decoded.
    let session_ids = store.session_ids().await?;
    assert!(!session_ids.is_empty());

    for session_id in &session_ids {
        let envelope = pond_get(
            &store,
            GetRequest {
                protocol_version: pond::PROTOCOL_VERSION,
                namespace: Some("local".to_owned()),
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
        let pond::wire::GetResult::Session {
            session, messages, ..
        } = response.result
        else {
            panic!("expected session result");
        };
        assert_eq!(session.id, *session_id);
        assert!(!messages.is_empty());

        let target = messages[0].id().to_owned();
        let envelope = pond_get(
            &store,
            GetRequest {
                protocol_version: pond::PROTOCOL_VERSION,
                namespace: Some("local".to_owned()),
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
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");

    ingest_adapter(&store, &adapter, |_| {}).await?;
    let first_counts = store.row_counts().await?;
    let second = ingest_adapter(&store, &adapter, |_| {}).await?;
    let second_counts = store.row_counts().await?;

    assert_eq!(first_counts, second_counts);
    assert_eq!(second.dropped_events, 0);
    assert_eq!(second.dropped_sessions, 0);
    assert_eq!(second.inserted, 0);
    assert!(second.matched > 0);

    Ok(())
}

#[tokio::test]
async fn ordering_violation_drops_only_the_offending_event() -> anyhow::Result<()> {
    // Per-event drop semantics (design.md 3.4): a Part with no preceding
    // Message is dropped on the spot, with one Error outcome surfaced. The
    // rest of the substream continues normally - subsequent valid messages
    // and parts get written.
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let session = synthetic_session("ordering");
    let orphan_part = Part {
        id: "orphan-part".to_owned(),
        message_id: "missing-message".to_owned(),
        ordinal: 0,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: "orphan".to_owned(),
        },
    };
    let valid_message = Message::User {
        id: "valid-message".to_owned(),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let valid_part = Part {
        id: "valid-part".to_owned(),
        message_id: valid_message.id().to_owned(),
        ordinal: 0,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: "kept".to_owned(),
        },
    };

    let mut validator = IngestValidator::default();
    validator
        .push(&store, 0, IngestEvent::Session(session.clone()))
        .await?;
    let part_outcomes = validator
        .push(&store, 1, IngestEvent::Part(orphan_part))
        .await?;
    assert_eq!(part_outcomes.len(), 1);
    assert_eq!(part_outcomes[0].kind, "part");
    assert_eq!(part_outcomes[0].status, OutcomeStatus::Error);
    assert!(
        part_outcomes[0]
            .error
            .as_ref()
            .map(|e| e.message.contains("part event appeared before a message"))
            .unwrap_or(false),
        "error message must explain the ordering violation: {part_outcomes:?}"
    );
    validator
        .push(&store, 2, IngestEvent::Message(valid_message))
        .await?;
    validator
        .push(&store, 3, IngestEvent::Part(valid_part))
        .await?;
    validator.finish(&store).await?;

    let (sessions, messages, parts, _) = store.row_counts().await?;
    assert_eq!(sessions, 1, "session committed despite the orphan part");
    assert_eq!(messages, 1, "valid message committed");
    assert_eq!(parts, 1, "valid part committed; the orphan was dropped");

    Ok(())
}

#[tokio::test]
async fn duplicate_message_id_drops_the_second_keeps_the_first() -> anyhow::Result<()> {
    // Per-event drop: a duplicate message id within a substream drops the
    // *duplicate* and surfaces an Error outcome for it. The first wins; the
    // session still commits.
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
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
        .push(&store, 0, IngestEvent::Session(session.clone()))
        .await?;
    validator
        .push(&store, 1, IngestEvent::Message(first))
        .await?;
    let dup_outcomes = validator
        .push(&store, 2, IngestEvent::Message(second))
        .await?;
    assert_eq!(dup_outcomes.len(), 1);
    assert_eq!(dup_outcomes[0].status, OutcomeStatus::Error);
    assert!(
        dup_outcomes[0]
            .error
            .as_ref()
            .map(|e| e.message.contains("duplicate message id message-1"))
            .unwrap_or(false),
        "duplicate-id rejection must name the offending id: {dup_outcomes:?}"
    );

    validator.finish(&store).await?;
    let (sessions, messages, _, _) = store.row_counts().await?;
    assert_eq!(sessions, 1, "session committed");
    assert_eq!(messages, 1, "only the first message committed");

    Ok(())
}

#[tokio::test]
async fn file_part_blob_v2_round_trips_through_get() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
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
        .push(&store, 0, IngestEvent::Session(session.clone()))
        .await?;
    validator
        .push(&store, 1, IngestEvent::Message(message.clone()))
        .await?;
    validator
        .push(&store, 2, IngestEvent::Part(part.clone()))
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

#[tokio::test]
async fn ingest_adapter_emits_discovered_then_session_done_for_each_session() -> anyhow::Result<()>
{
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");

    let mut events: Vec<SyncEvent> = Vec::new();
    ingest_adapter(&store, &adapter, |event| events.push(event)).await?;

    let first = events.first().expect("at least one progress event");
    let discovered_total = match first {
        SyncEvent::Discovered { total } => total.expect("discovery total available on a local dir"),
        SyncEvent::SessionDone(_) => panic!("first event must be Discovered, got SessionDone"),
    };

    let done_count = events
        .iter()
        .filter(|e| matches!(e, SyncEvent::SessionDone(_)))
        .count();
    assert!(
        done_count >= discovered_total,
        "every discovered file must produce one SessionDone (discovered={discovered_total}, \
         done={done_count}). Re-ingest failures may legitimately add extras.",
    );
    for event in &events {
        if let SyncEvent::SessionDone(outcome) = event {
            assert!(
                matches!(outcome.status, SyncStatus::Ok | SyncStatus::Skipped { .. }),
                "fixture corpus should not produce validator errors: {outcome:?}",
            );
        }
    }

    Ok(())
}

#[tokio::test]
async fn corpus_stats_groups_by_adapter_and_project() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/session-samples/claude-code/projects");
    ingest_adapter(&store, &adapter, |_| {}).await?;

    let stats = store.corpus_stats().await?;
    assert!(stats.totals.sessions > 0);
    assert!(stats.totals.messages > 0);

    let claude = stats
        .adapters
        .iter()
        .find(|stat| stat.adapter == "claude-code")
        .expect("claude-code section present");
    assert!(!claude.projects.is_empty());
    let project_sessions: u64 = claude.projects.iter().map(|p| p.sessions).sum();
    let project_messages: u64 = claude.projects.iter().map(|p| p.messages).sum();
    assert_eq!(claude.sessions, project_sessions);
    assert_eq!(claude.messages, project_messages);
    assert_eq!(claude.messages, stats.totals.messages);

    // Projects sort by message count desc.
    for pair in claude.projects.windows(2) {
        assert!(
            pair[0].messages >= pair[1].messages,
            "projects must be ordered by message count desc",
        );
    }

    Ok(())
}
