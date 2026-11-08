//! Goose adapter integration suite: SQLite `sessions.db` ingest (user, sub-agent,
//! scheduled, thinking sessions) plus legacy JSONL, where the same session id in
//! both generations means DB wins and the legacy copy is counted Superseded.
//! Modeled on `hermes.rs`: cross-module paths (store -> parts/lineage/search),
//! not the seam unit tests that live in `src/adapter/goose.rs`. The fixture is
//! synthetic, captured under a throwaway HOME with a local mock LLM server -
//! no credentials.
//!
//! #![expect(clippy::expect_used, reason = "tests fail by panicking")]

use std::path::Path;

use pond::{
    adapter::{AdapterFactory, GooseAdapter, GooseFactory, NoopOracle, SkipOracle},
    config::SearchConfig,
    embed::LazyEmbedder,
    handlers::{ingest_adapter, pond_search},
    sessions::{RowmapOracle, Store},
    substrate::{MaintenancePolicy, Predicate, ScalarValue},
    wire::{PartKind, Role, SearchEnvelope, SearchFilters, SearchModeWire, SearchRequest, SortBy},
};
use tempfile::TempDir;

const FIXTURE_ROOT: &str = "tests/fixtures/adapter/goose/data";

// 4 DB sessions (ses-user-001, ses-sub-002, ses-think-003, ses-sched-004)
// + 2 legacy JSONL sessions (legacy-001, legacy-002), disjoint ids from the DB.
const FIXTURE_SESSIONS: usize = 6;

async fn ingest(root: &Path) -> anyhow::Result<(Store, TempDir)> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = GooseAdapter::new(root);
    ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    Ok((store, store_dir))
}

/// Full-corpus ingest: 6 sessions (4 DB + 2 legacy), the taxonomy census over
/// all of them, and default-mode search is reachable end to end.
#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let ids = store.session_ids().await?;
    anyhow::ensure!(
        ids.len() == FIXTURE_SESSIONS,
        "goose: expected {FIXTURE_SESSIONS} ingested sessions, got {}: {ids:?}",
        ids.len(),
    );

    // Taxonomy census: every session brands as `goose` or a `goose/<kind>`
    // subpath, never outside the taxonomy (spec 8.3).
    for id in &ids {
        let session = store
            .get_session(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("goose: session {id} missing from store"))?;
        anyhow::ensure!(
            !session.messages.is_empty(),
            "goose: session {id} carries no messages",
        );
        anyhow::ensure!(
            session.session.source_agent.starts_with("goose"),
            "goose: {id} branded outside the goose taxonomy: {}",
            session.session.source_agent,
        );
    }

    // Default-mode search for a word from a root session's conversation.
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;
    let request = SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: "france".to_owned(),
        mode: SearchModeWire::Fts,
        sort_by: SortBy::Relevance,
        filters: SearchFilters::default(),
        limit: 5,
    };
    let response = match pond_search(
        &store,
        &LazyEmbedder::candle(),
        request,
        &SearchConfig::default(),
    )
    .await
    {
        SearchEnvelope::Success(response) => response,
        SearchEnvelope::Error(error) => anyhow::bail!("goose: default search failed: {error:?}"),
    };
    anyhow::ensure!(
        response.matched_total > 0,
        "goose: default search for 'france' found nothing"
    );
    Ok(())
}

/// The taxonomy census over every session: `source_agent` matches the
/// `session_type` column, including kind subpaths (sub-agent, scheduled).
#[tokio::test(flavor = "multi_thread")]
async fn taxonomy_census_over_all_sessions() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let expected: &[(&str, &str)] = &[
        ("ses-user-001", "goose"),
        ("ses-sub-002", "goose/sub-agent"),
        ("ses-think-003", "goose"),
        ("ses-sched-004", "goose"), // fixture's scheduled session carries session_type = user
        ("legacy-001", "goose"),
        ("legacy-002", "goose"),
    ];
    for (id, brand) in expected {
        let session = store
            .get_session(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("goose: session {id} not in store"))?;
        anyhow::ensure!(
            session.session.source_agent == *brand,
            "goose: {id} branded {}, want {}",
            session.session.source_agent,
            brand,
        );
    }
    Ok(())
}

/// Lineage for each kind: `parent_session_id` surfaces verbatim on the child,
/// root sessions carry none, and `options.goose.relation` is `child`/`root`.
#[tokio::test(flavor = "multi_thread")]
async fn lineage_for_each_kind() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let sub = store
        .get_session("ses-sub-002")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-sub-002 not in store"))?;
    anyhow::ensure!(
        sub.session.parent_session_id.as_deref() == Some("ses-user-001"),
        "goose: sub-agent lineage wrong: {:?}",
        sub.session.parent_session_id,
    );
    anyhow::ensure!(
        sub.session
            .options
            .get("goose")
            .and_then(|v| v.get("relation"))
            .and_then(|v| v.as_str())
            == Some("child"),
        "goose: sub-agent relation not child: {:?}",
        sub.session.options.get("goose"),
    );

    let root = store
        .get_session("ses-user-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-user-001 not in store"))?;
    anyhow::ensure!(
        root.session.parent_session_id.is_none(),
        "goose: root session carries a parent: {:?}",
        root.session.parent_session_id,
    );
    anyhow::ensure!(
        root.session
            .options
            .get("goose")
            .and_then(|v| v.get("relation"))
            .and_then(|v| v.as_str())
            == Some("root"),
        "goose: root session relation not root: {:?}",
        root.session.options.get("goose"),
    );
    Ok(())
}

/// Project fallback chain: `working_dir` wins, then `project_id`, then the
/// compact-repr of the session id.
#[tokio::test(flavor = "multi_thread")]
async fn project_fallback_chain() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    // ses-user-001 has working_dir = /tmp/goose-project, project_id = proj-001.
    let user = store
        .get_session("ses-user-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-user-001 not in store"))?;
    anyhow::ensure!(
        &*user.session.project == "/tmp/goose-project",
        "goose: working_dir project wrong: {}",
        *user.session.project,
    );

    // legacy-001 has working_dir = /tmp/goose-legacy, no project_id.
    let legacy = store
        .get_session("legacy-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: legacy-001 not in store"))?;
    anyhow::ensure!(
        &*legacy.session.project == "/tmp/goose-legacy",
        "goose: legacy project wrong: {}",
        *legacy.session.project,
    );
    Ok(())
}

/// Tool/multimodal parts surviving the store: image (File), tool call, and
/// tool result parts round-trip through get_session on the scheduled session.
#[tokio::test(flavor = "multi_thread")]
async fn tool_and_multimodal_parts_survive() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let sched = store
        .get_session("ses-sched-004")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-sched-004 not in store"))?;

    let kinds: Vec<&str> = sched
        .messages
        .iter()
        .flat_map(|m| m.parts.iter())
        .map(|p| p.kind.type_name())
        .collect();
    anyhow::ensure!(
        kinds.contains(&"file"),
        "goose: scheduled session has no File part: {kinds:?}",
    );
    anyhow::ensure!(
        kinds.contains(&"tool_call"),
        "goose: scheduled session has no tool_call part: {kinds:?}",
    );
    anyhow::ensure!(
        kinds.contains(&"tool_result"),
        "goose: scheduled session has no tool_result part: {kinds:?}",
    );

    // The tool result carries `is_failure` and a result body.
    let tool_result = sched
        .messages
        .iter()
        .flat_map(|m| m.parts.iter())
        .find(|p| matches!(p.kind, PartKind::ToolResult { .. }))
        .ok_or_else(|| anyhow::anyhow!("goose: no ToolResult part in ses-sched-004"))?;
    if let PartKind::ToolResult {
        is_failure, result, ..
    } = &tool_result.kind
    {
        anyhow::ensure!(*is_failure, "goose: error tool result not flagged failure");
        anyhow::ensure!(
            !result.is_null(),
            "goose: error tool result body missing: {result:?}",
        );
    }
    Ok(())
}

/// Tool call/result pair survival: the call and its matching result keep
/// call_id correlation across the store on the user session.
#[tokio::test(flavor = "multi_thread")]
async fn tool_call_and_result_pair_survive() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let user = store
        .get_session("ses-user-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-user-001 not in store"))?;

    let want_call_id: Option<String> =
        user.messages
            .iter()
            .flat_map(|m| m.parts.iter())
            .find_map(|p| match &p.kind {
                PartKind::ToolCall { call_id, .. } => call_id.as_ref().map(|e| e.as_ref().clone()),
                _ => None,
            });
    let want_call_id =
        want_call_id.ok_or_else(|| anyhow::anyhow!("goose: no ToolCall in ses-user-001"))?;

    let result = user
        .messages
        .iter()
        .flat_map(|m| m.parts.iter())
        .find(|p| {
            matches!(
                p.kind,
                PartKind::ToolResult { ref call_id, .. }
                    if call_id.as_ref().map(|e| &**e) == Some(&want_call_id)
            )
        })
        .ok_or_else(|| anyhow::anyhow!("goose: no matching ToolResult for {want_call_id}"))?;
    if let PartKind::ToolResult {
        call_id: result_call_id,
        name,
        ..
    } = &result.kind
    {
        anyhow::ensure!(
            result_call_id.as_ref().map(|e| &**e) == Some(&want_call_id),
            "goose: result call_id mismatch: {result_call_id:?} vs {want_call_id}",
        );
        anyhow::ensure!(
            name.is_some(),
            "goose: tool result name not resolved via the call map",
        );
    }
    Ok(())
}

/// A thinking part survives as a Reasoning part with its signature in options.
#[tokio::test(flavor = "multi_thread")]
async fn thinking_part_survives() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let think = store
        .get_session("ses-think-003")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-think-003 not in store"))?;

    let reasoning = think
        .messages
        .iter()
        .flat_map(|m| m.parts.iter())
        .find(|p| matches!(p.kind, PartKind::Reasoning { .. }))
        .ok_or_else(|| anyhow::anyhow!("goose: no Reasoning part in ses-think-003"))?;
    if let PartKind::Reasoning { text } = &reasoning.kind {
        anyhow::ensure!(
            text.as_ref().is_some_and(|t| !t.is_empty()),
            "goose: reasoning text missing",
        );
    }
    anyhow::ensure!(
        reasoning
            .options
            .get("goose")
            .and_then(|v| v.get("signature"))
            .and_then(|v| v.as_str())
            == Some("sig123"),
        "goose: thinking signature not carried: {:?}",
        reasoning.options.get("goose"),
    );
    Ok(())
}

/// DB-vs-legacy coexistence: the legacy ids are disjoint from the DB ids, so
/// each legacy session ingests as a standalone `goose` session. No id collides,
/// so no Superseded skip occurs here - that path is covered by
/// `db_wins_when_id_collides`.
#[tokio::test(flavor = "multi_thread")]
async fn db_vs_legacy_coexistence() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    for id in ["legacy-001", "legacy-002"] {
        let session = store
            .get_session(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("goose: legacy {id} not in store"))?;
        anyhow::ensure!(
            session.session.source_agent == "goose",
            "goose: legacy {id} branded {}",
            session.session.source_agent,
        );
        anyhow::ensure!(
            !session.messages.is_empty(),
            "goose: legacy {id} ingested no messages",
        );
    }

    // The legacy-001 tool request lands as an injected ToolCall part whose name
    // was resolved from the tool call block (status success -> name present).
    let legacy = store
        .get_session("legacy-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: legacy-001 not in store"))?;
    let has_tool_call = legacy
        .messages
        .iter()
        .flat_map(|m| m.parts.iter())
        .any(|p| matches!(p.kind, PartKind::ToolCall { name: Some(_), .. }));
    anyhow::ensure!(
        has_tool_call,
        "goose: legacy-001 has no resolved ToolCall part",
    );
    Ok(())
}

/// DB-vs-legacy coexistence with a colliding id: seed a legacy file under a DB
/// session id, ingest, and assert the DB copy wins and the legacy copy is not
/// added as a session.
#[tokio::test(flavor = "multi_thread")]
async fn db_wins_when_id_collides() -> anyhow::Result<()> {
    let tmp = tempfile::TempDir::new()?;
    let root = tmp.path().join("data");
    std::fs::create_dir_all(root.join("sessions"))?;

    // Copy the DB fixture and the disjoint legacy sessions.
    std::fs::copy(
        Path::new(FIXTURE_ROOT).join("sessions").join("sessions.db"),
        root.join("sessions").join("sessions.db"),
    )?;
    for legacy in ["legacy-001.jsonl", "legacy-002.jsonl"] {
        std::fs::copy(
            Path::new(FIXTURE_ROOT).join("sessions").join(legacy),
            root.join("sessions").join(legacy),
        )?;
    }

    // Legacy file colliding with a DB id.
    let legacy_path = root.join("sessions").join("ses-user-001.jsonl");
    std::fs::write(
        &legacy_path,
        r#"{"id": "ses-user-001", "description": "legacy colliding copy", "working_dir": "/tmp/goose-legacy-collision", "created_at": "2026-01-14T09:00:00Z", "updated_at": "2026-01-14T09:00:00Z", "extension_data": {{}}, "message_count": 1}
{"id": "msg-legacy-collide-1", "role": "user", "created": 0, "content": [{"type": "text", "text": "legacy should not win"}]}"#,
    )?;

    let (store, _guard) = ingest(&root).await?;

    // The DB copy wins: ses-user-001 keeps its DB project, not the legacy one.
    let session = store
        .get_session("ses-user-001")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-user-001 not in store"))?;
    anyhow::ensure!(
        &*session.session.project == "/tmp/goose-project",
        "goose: DB copy did not win collision, project is {}",
        *session.session.project,
    );

    // The legacy copy is counted Superseded: 6 sessions total, the colliding
    // legacy file is skipped, not added.
    let ids = store.session_ids().await?;
    anyhow::ensure!(
        ids.len() == FIXTURE_SESSIONS,
        "goose: collision legacy added a session: {ids:?}",
    );
    Ok(())
}

/// Freshness: a second sync of the unchanged fixture skips every session and
/// writes nothing (goose declares no re-read sessions).
#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path().join("store")).await?;
    let adapter = GooseAdapter::new(Path::new(FIXTURE_ROOT));
    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    anyhow::ensure!(
        first.sessions_inserted > 0,
        "goose: first sync ingested nothing: {first:?}",
    );
    let before = store.session_ids().await?.len();

    store.ensure_rowmap(&store_dir.path().join("cache")).await?;
    let snapshot = store.rowmap_snapshot();
    let oracle = RowmapOracle(snapshot);
    anyhow::ensure!(
        !SkipOracle::is_empty(&oracle),
        "goose: resident rowmap empty after first sync"
    );

    let second = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    anyhow::ensure!(
        second.sessions_inserted == 0,
        "goose: unchanged re-sync re-inserted sessions: {second:?}",
    );
    anyhow::ensure!(
        second.inserted == 0,
        "goose: unchanged re-sync wrote rows: {second:?}",
    );
    anyhow::ensure!(
        second.skipped_fresh > 0,
        "goose: nothing skipped fresh: {second:?}",
    );
    let after = store.session_ids().await?;
    anyhow::ensure!(
        after.len() == before,
        "goose: session set changed on re-sync: {before} -> {}",
        after.len(),
    );
    Ok(())
}

/// Restore is declared unsupported (ingest-only adapter).
#[tokio::test(flavor = "multi_thread")]
async fn restore_is_declared_unsupported() -> anyhow::Result<()> {
    anyhow::ensure!(
        GooseFactory.restore_unsupported().is_some(),
        "goose: restore should be declared unsupported",
    );
    Ok(())
}

/// The sub-agent session is branded `goose/sub-agent`, never bare `goose`.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_branding_is_not_plain_goose() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest(Path::new(FIXTURE_ROOT)).await?;

    // The specific sub_agent session is branded goose/sub-agent.
    let sub = store
        .get_session("ses-sub-002")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: sub session"))?;
    assert_eq!(sub.session.source_agent, "goose/sub-agent");

    // No session carries a malformed `source_agent` (an unknown session_type
    // never brands itself as a plain user session, spec 8.3).
    for id in store.session_ids().await? {
        let session = store.get_session(&id).await?.expect("session");
        anyhow::ensure!(
            session.session.source_agent.starts_with("goose"),
            "goose: {id} branded outside the goose taxonomy: {}",
            session.session.source_agent,
        );
    }
    Ok(())
}

/// The scheduled session carries a system notification (system role message)
/// as a rule-3 carrier.
#[tokio::test(flavor = "multi_thread")]
async fn scheduled_session_carries_system_notification() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let sched = store
        .get_session("ses-sched-004")
        .await?
        .ok_or_else(|| anyhow::anyhow!("goose: ses-sched-004 not in store"))?;

    let system_count = sched
        .messages
        .iter()
        .filter(|m| matches!(m.message.role(), Role::System))
        .count();
    anyhow::ensure!(
        system_count >= 1,
        "goose: scheduled session has no system notification carrier: {system_count}",
    );
    Ok(())
}

/// The DB `source_agent` scope (root brand `goose` only) is searchable via
/// the store's `searchable_in_scope` oracle.
#[tokio::test(flavor = "multi_thread")]
async fn goose_root_sessions_are_searchable_in_scope() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let searchable = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("goose".to_owned()),
        ))
        .await?;
    anyhow::ensure!(
        searchable > 0,
        "goose: root sessions must be searchable in scope after ingest",
    );
    Ok(())
}
