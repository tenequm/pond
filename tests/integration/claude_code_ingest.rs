#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::pond_get,
    handlers::{SyncEvent, SyncStatus, ingest_adapter},
    sessions::Store,
    wire::{GetEnvelope, GetRequest},
};
use tempfile::TempDir;

#[tokio::test]
async fn claude_code_fixtures_round_trip_and_get() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");

    let summary = ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
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
                limit: 1000,
                include_parts: true,
                cursor: None,
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

        let target = messages
            .iter()
            .find(|m| !m.text.is_empty())
            .map(|m| m.id.clone())
            .unwrap_or_else(|| {
                panic!("session {session_id} has no conversational message in the fixture corpus")
            });
        let envelope = pond_get(
            &store,
            GetRequest {
                protocol_version: pond::PROTOCOL_VERSION,
                namespace: Some("local".to_owned()),
                session_id: None,
                message_id: Some(target),
                up_to: None,
                context_depth: 1,
                limit: 100,
                include_parts: false,
                cursor: None,
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
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");

    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
    let first_counts = store.row_counts().await?;
    let second = ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
    let second_counts = store.row_counts().await?;

    assert_eq!(first_counts, second_counts);
    assert_eq!(second.dropped_events, 0);
    assert_eq!(second.dropped_sessions, 0);
    assert_eq!(second.inserted, 0);
    assert!(second.matched > 0);

    Ok(())
}

#[tokio::test]
async fn ingest_adapter_emits_discovered_then_session_done_for_each_session() -> anyhow::Result<()>
{
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");

    let mut events: Vec<SyncEvent> = Vec::new();
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |event| {
        events.push(event);
    })
    .await?;

    let first = events.first().expect("at least one progress event");
    let discovered_total = match first {
        SyncEvent::Discovered { total } => total.expect("discovery total available on a local dir"),
        SyncEvent::SessionDone(_) => panic!("first event must be Discovered, got SessionDone"),
    };

    let done_count = events
        .iter()
        .filter(|e| matches!(e, SyncEvent::SessionDone(_)))
        .count();
    assert_eq!(
        done_count, discovered_total,
        "every discovered file must produce exactly one SessionDone on a fresh ingest \
         (discovered={discovered_total}, done={done_count})",
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
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let stats = store.corpus_stats(false).await?;
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
    // `include_subagents=false` (the CLI default): sub-branded sessions
    // (`source_agent` with a `/`) are filtered out of the breakdown, so no
    // adapter row carries a `/` and the breakdown sums to strictly less than
    // `totals` - the fixture corpus has one subagent session.
    assert!(stats.adapters.iter().all(|s| !s.adapter.contains('/')));
    let filtered_messages: u64 = stats.adapters.iter().map(|s| s.messages).sum();
    assert!(filtered_messages < stats.totals.messages);

    // Projects sort by message count desc.
    for pair in claude.projects.windows(2) {
        assert!(
            pair[0].messages >= pair[1].messages,
            "projects must be ordered by message count desc",
        );
    }

    // `include_subagents=true`: every `source_agent` gets its own row,
    // including `claude-code/<type>`, and the breakdown reconciles exactly
    // with `totals`.
    let full = store.corpus_stats(true).await?;
    let full_messages: u64 = full.adapters.iter().map(|s| s.messages).sum();
    assert_eq!(full_messages, full.totals.messages);
    assert!(
        full.adapters.iter().any(|s| s.adapter.contains('/')),
        "a subagent session must surface as its own claude-code/<type> row",
    );

    Ok(())
}
