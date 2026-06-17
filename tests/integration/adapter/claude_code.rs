#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{AdapterFactory, ClaudeCodeAdapter, ClaudeCodeFactory, RestoreFidelity},
    handlers::pond_get,
    handlers::{SyncEvent, SyncStatus, ingest_adapter},
    sessions::Store,
    wire::{GetEnvelope, GetRequest, ResponseMode},
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
                context_depth: 0,
                limit: 1000,
                response_mode: ResponseMode::Conversational,
                session_from: Default::default(),
                after_id: None,
            },
        )
        .await;
        let GetEnvelope::Success(response) = envelope else {
            panic!("expected successful pond_get for {session_id}");
        };
        assert_eq!(response.session.id, *session_id);
        let pond::wire::GetResult::Session { messages, .. } = response.result else {
            panic!("expected session result");
        };
        assert!(!messages.is_empty());

        let target = messages
            .iter()
            .find(|m| m.text.as_deref().is_some_and(|text| !text.is_empty()))
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
                context_depth: 1,
                limit: 100,
                response_mode: ResponseMode::Conversational,
                session_from: Default::default(),
                after_id: None,
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
async fn adapter_names_filters_subagents_by_default() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    // `include_subagents=false` (the CLI default): sub-branded sessions
    // (`source_agent` with a `/`) are dropped, so only the bare `claude-code`
    // name survives - the cheap adapter count `pond status` renders.
    let names = store.adapter_names(false).await?;
    assert_eq!(names, vec!["claude-code".to_owned()]);

    // `include_subagents=true`: every distinct `source_agent` surfaces,
    // including the `claude-code/<type>` subagent rows the fixture carries.
    let full = store.adapter_names(true).await?;
    assert!(full.contains(&"claude-code".to_owned()));
    assert!(
        full.iter().any(|name| name.contains('/')),
        "a subagent session must surface as its own claude-code/<type> name",
    );
    // Sorted, distinct.
    assert!(full.windows(2).all(|pair| pair[0] < pair[1]));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_writes_parent_and_direct_subagent_child() -> anyhow::Result<()> {
    let source = TempDir::new()?;
    write_claude_parent_child(source.path())?;
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = ClaudeCodeAdapter::new(source.path());
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let parent = store
        .get_session("parent-session")
        .await?
        .expect("parent ingested");
    let child = store
        .get_session("parent-session/agent-abc123")
        .await?
        .expect("child ingested");
    let mut files = Vec::new();
    for session in [&parent, &child] {
        files.extend(ClaudeCodeFactory.serialize(session, RestoreFidelity::Native)?);
    }
    assert!(
        files
            .iter()
            .any(|f| f.relative_path.ends_with("parent-session.jsonl"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.relative_path.ends_with("subagents/agent-abc123.jsonl"))
    );
    assert!(files.iter().any(|f| {
        f.relative_path
            .ends_with("subagents/agent-abc123.meta.json")
    }));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_writes_parent_and_workflow_nested_subagent_child() -> anyhow::Result<()> {
    let source = TempDir::new()?;
    write_claude_parent_workflow_child(source.path())?;
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = ClaudeCodeAdapter::new(source.path());
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let parent = store
        .get_session("parent-session")
        .await?
        .expect("parent ingested");
    let child = store
        .get_session("parent-session/workflows/wf_test01/agent-xyz789")
        .await?
        .expect("nested workflow child ingested under the full path-derived id");
    let mut files = Vec::new();
    for session in [&parent, &child] {
        files.extend(ClaudeCodeFactory.serialize(session, RestoreFidelity::Native)?);
    }
    assert!(
        files
            .iter()
            .any(|f| f.relative_path.ends_with("parent-session.jsonl"))
    );
    // The nested workflow path round-trips verbatim, not collapsed to the flat
    // `subagents/agent-<hash>.jsonl` shape.
    assert!(files.iter().any(|f| {
        f.relative_path
            .ends_with("subagents/workflows/wf_test01/agent-xyz789.jsonl")
    }));
    assert!(files.iter().any(|f| {
        f.relative_path
            .ends_with("subagents/workflows/wf_test01/agent-xyz789.meta.json")
    }));
    Ok(())
}

fn write_claude_parent_child(root: &Path) -> anyhow::Result<()> {
    let project = root.join("-tmp-restore");
    let subagents = project.join("parent-session").join("subagents");
    std::fs::create_dir_all(&subagents)?;
    std::fs::write(
        project.join("parent-session.jsonl"),
        r#"{"type":"user","uuid":"parent-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp/restore","message":{"role":"user","content":"hi"}}"#,
    )?;
    std::fs::write(
        subagents.join("agent-abc123.jsonl"),
        r#"{"type":"assistant","uuid":"child-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:01.000Z","cwd":"/tmp/restore","message":{"role":"assistant","content":[{"type":"text","text":"child"}]}}"#,
    )?;
    std::fs::write(
        subagents.join("agent-abc123.meta.json"),
        r#"{"agentType":"general-purpose","description":"fixture child"}"#,
    )?;
    Ok(())
}

fn write_claude_parent_workflow_child(root: &Path) -> anyhow::Result<()> {
    let project = root.join("-tmp-restore");
    let wf = project
        .join("parent-session")
        .join("subagents")
        .join("workflows")
        .join("wf_test01");
    std::fs::create_dir_all(&wf)?;
    std::fs::write(
        project.join("parent-session.jsonl"),
        r#"{"type":"user","uuid":"parent-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp/restore","message":{"role":"user","content":"hi"}}"#,
    )?;
    std::fs::write(
        wf.join("agent-xyz789.jsonl"),
        r#"{"type":"assistant","uuid":"wf-child-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:02.000Z","cwd":"/tmp/restore/sub","message":{"role":"assistant","content":[{"type":"text","text":"workflow child"}]}}"#,
    )?;
    std::fs::write(
        wf.join("agent-xyz789.meta.json"),
        r#"{"agentType":"general-purpose","description":"workflow fixture child"}"#,
    )?;
    Ok(())
}
