#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{AdapterFactory, ClaudeCodeAdapter, ClaudeCodeFactory, RestoreFidelity},
    handlers::{SyncEvent, SyncStatus, ingest_adapter},
    sessions::Store,
};
use tempfile::TempDir;

use super::{Conformance, RoundTrip, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/claude_code/projects"
);

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &ClaudeCodeFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        // 10 top-level sessions plus the `pond` session's 3 subagent sidecars
        // (two direct, one workflow-nested), each a `claude-code/<type>` session.
        expected_sessions: 13,
        round_trip: RoundTrip::Reingest { downgraded: 0 },
        config: path_config,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance().assert_ingest_counts_and_searchable().await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    conformance().assert_resync_is_noop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn native_restore_round_trips_parents_and_subagents_through_reingest() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}

/// Real native-Windows capture; see the fixture-gate test below.
const WINDOWS_FIXTURES: &str = "tests/fixtures/adapter/claude_code/windows-projects";

/// The directory name Claude Code chose for the capture's `cwd`.
const WINDOWS_SLUG: &str = "C--dev-pond-fixture-demo-v2";

/// The capture's plain session, its two-subagent parent, and that parent's
/// first child.
const WINDOWS_PLAIN_SESSION: &str = "68f6e765-7552-44c4-8cf9-d88aba05bdb0";
const WINDOWS_PARENT_SESSION: &str = "95602a8e-b311-49b6-a95c-69c12cd105f8";
const WINDOWS_CHILD_SUFFIX: &str = "agent-a44fd74de879ec6e2";

/// The adapter ingests the whole fixture corpus without dropping anything, and
/// every session it produced carries retrievable conversational content.
/// Asserts adapter output at the Store layer - `pond_get_session`/
/// `pond_get_message`/`pond_search` render behavior is covered by their own
/// tests, not re-litigated here.
#[tokio::test]
async fn claude_code_fixtures_ingest_cleanly() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects");

    let summary = ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
    assert_eq!(summary.dropped_events, 0);
    assert_eq!(summary.dropped_sessions, 0);
    assert_eq!(summary.skipped_files, 0);

    let session_ids = store.session_ids().await?;
    assert!(!session_ids.is_empty());

    let mut conversational_total = 0;
    for session_id in &session_ids {
        conversational_total += store.scan_conversational_messages(session_id).await?.len();
    }
    assert!(
        conversational_total > 0,
        "the adapter must produce retrievable conversational messages"
    );

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
        SyncEvent::SessionDone(_) | SyncEvent::SkippedBulk { .. } | SyncEvent::Flushing { .. } => {
            panic!("first event must be Discovered, got {first:?}")
        }
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

/// The Windows fixture gate (plan 2608-13 section 3.5). Captured on native
/// Windows 11 from a `cwd` of `C:\dev\pond fixture_demo.v2`, one path carrying a
/// drive colon, backslashes, a space, an underscore and a dot - so a single
/// capture pins every character class the slug rule collapses.
///
/// Both routes to the slug are asserted, because the restore has two: the
/// captured `source.project_dir` hint, which it prefers, and `encode_project`
/// from the `cwd`, which it falls back to. Asserting only the first would pass
/// with a broken encoder.
#[tokio::test(flavor = "multi_thread")]
async fn windows_capture_ingests_its_native_cwd_and_restores_to_the_same_slug() -> anyhow::Result<()>
{
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(WINDOWS_FIXTURES);

    let summary = ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
    assert_eq!(summary.dropped_events, 0);
    assert_eq!(summary.dropped_sessions, 0);
    assert_eq!(summary.skipped_files, 0);

    // The backslash-bearing `cwd` survives ingest verbatim - it is a value, not
    // a path pond walks.
    for id in store.session_ids().await? {
        let session = store.get_session(&id).await?.expect("ingested");
        assert_eq!(
            &*session.session.project, r"C:\dev\pond fixture_demo.v2",
            "session {id} lost its Windows cwd",
        );
    }

    // Round trip: the slug pond writes back is the slug Claude Code wrote.
    let parent = store
        .get_session(WINDOWS_PARENT_SESSION)
        .await?
        .expect("subagent parent ingested");
    let files = ClaudeCodeFactory.serialize(&parent, RestoreFidelity::Native)?;
    assert!(
        files
            .iter()
            .all(|f| f.relative_path.starts_with(WINDOWS_SLUG)),
        "restore must target the captured slug, got {:?}",
        files.iter().map(|f| &f.relative_path).collect::<Vec<_>>(),
    );

    // That rode the placement hint, which the restore prefers. Strip it and the
    // same slug has to come back out of `encode_project`, derived from the
    // Windows `cwd` alone - the half the capture exists to pin.
    let mut derived = parent;
    let source = derived
        .session
        .options
        .get_mut("source")
        .and_then(serde_json::Value::as_object_mut)
        .expect("ingest records options.source");
    assert!(
        source.remove("project_dir").is_some(),
        "the hint must be there to strip, or the fallback below proves nothing",
    );
    let derived_files = ClaudeCodeFactory.serialize(&derived, RestoreFidelity::Native)?;
    assert!(
        derived_files
            .iter()
            .all(|f| f.relative_path.starts_with(WINDOWS_SLUG)),
        "encode_project must derive the captured slug from the Windows cwd, got {:?}",
        derived_files
            .iter()
            .map(|f| &f.relative_path)
            .collect::<Vec<_>>(),
    );

    // The subagent layout is the same on Windows as on posix.
    let child = store
        .get_session(&format!("{WINDOWS_PARENT_SESSION}/{WINDOWS_CHILD_SUFFIX}"))
        .await?
        .expect("subagent ingested");
    let child_files = ClaudeCodeFactory.serialize(&child, RestoreFidelity::Native)?;
    assert!(
        child_files.iter().any(|f| f
            .relative_path
            .ends_with(format!("subagents/{WINDOWS_CHILD_SUFFIX}.jsonl"))),
        "subagent restore path drifted: {:?}",
        child_files
            .iter()
            .map(|f| &f.relative_path)
            .collect::<Vec<_>>(),
    );

    Ok(())
}

/// The other half of the gate: the slug-decode fallback, which only fires when
/// no row in a transcript carries `cwd`. Derived from the same real capture by
/// stripping that one field, because no natural corpus contains a `cwd`-less
/// transcript. The recovered path is deliberately lossy - the encoding turned
/// the space, underscore and dot into the same `-` a separator became, so they
/// all come back as separators. Recovering the `C:\` prefix is the part that
/// matters; before this, the slug decoded to `C//dev/pond/fixture/demo/v2`.
#[tokio::test(flavor = "multi_thread")]
async fn windows_capture_without_cwd_falls_back_to_decoding_the_slug() -> anyhow::Result<()> {
    let source = TempDir::new()?;
    let project = source.path().join(WINDOWS_SLUG);
    std::fs::create_dir_all(&project)?;
    let captured = Path::new(WINDOWS_FIXTURES)
        .join(WINDOWS_SLUG)
        .join(format!("{WINDOWS_PLAIN_SESSION}.jsonl"));
    let stripped: String = std::fs::read_to_string(&captured)?
        .lines()
        .map(|line| {
            let mut row: serde_json::Value = serde_json::from_str(line).expect("fixture is json");
            if let Some(object) = row.as_object_mut() {
                object.remove("cwd");
            }
            format!("{row}\n")
        })
        .collect();
    std::fs::write(
        project.join(format!("{WINDOWS_PLAIN_SESSION}.jsonl")),
        stripped,
    )?;

    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(source.path());
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let session = store
        .get_session(WINDOWS_PLAIN_SESSION)
        .await?
        .expect("ingested");
    assert_eq!(&*session.session.project, r"C:\dev\pond\fixture\demo\v2");

    Ok(())
}
