//! nanoclaw adapter integration suite: ingest -> Store -> get/search over the
//! committed fixtures (real `agentgroup-anon-001` + synthetic
//! `agentgroup-synthetic-001`) and synthetic metadata/provider DBs. Single-module
//! mapping behavior stays in `src/adapter/nanoclaw.rs` unit tests; this suite
//! covers the cross-module paths: the opencode-provider composition (the
//! `opencode` reader driven through nanoclaw), the codex visible skip, and
//! additive re-sync freshness through the store's rowmap oracle. All non-fixture
//! data is synthetic - the committed `agentgroup-anon-001` capture is never
//! mutated.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{NanoclawAdapter, NoopOracle, SkipOracle},
    handlers::{SyncEvent, SyncStatus, ingest_adapter},
    sessions::{RowmapOracle, Store},
    substrate::{Predicate, ScalarValue},
};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/nanoclaw"
);
const OPENCODE_DB_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/opencode/opencode.db"
);

// The committed fixture corpus: 3 top-level transcripts (anon main + 2 synthetic
// mains) and 5 subagent sidecars (anon 1, synthetic 503d... 1, synthetic bebe...
// 3), each its own session.
const FIXTURE_SESSIONS: usize = 8;

// Facts about the opencode DB fixture reused for the composition case.
const OPENCODE_DB_SESSIONS: usize = 10;
const OPENCODE_CHILD_ID: &str = "ses_09fbc7fc1ffeUaBz77QiNdJbXa";
const OPENCODE_PARENT_ID: &str = "ses_09fbc87f2ffeyYCZq51oN2HfGa";

async fn ingest(root: &Path) -> anyhow::Result<(Store, TempDir)> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = NanoclawAdapter::new(root);
    ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    Ok((store, store_dir))
}

/// Full-fixture ingest through the Store: session count matches the committed
/// corpus, every session round-trips via get, and the nanoclaw scope is
/// searchable (the FTS/index path ran end to end).
#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let ids = store.session_ids().await?;
    assert_eq!(
        ids.len(),
        FIXTURE_SESSIONS,
        "every committed transcript and subagent sidecar becomes its own session"
    );

    let mut total_messages = 0usize;
    let mut main_sessions = 0usize;
    let mut subagent_sessions = 0usize;
    for id in &ids {
        let session = store
            .get_session(id)
            .await?
            .expect("every enumerated session round-trips through get");
        assert!(
            !session.messages.is_empty(),
            "session {id} carries at least one message"
        );
        total_messages += session.messages.len();
        match session.session.source_agent.as_str() {
            "nanoclaw" => main_sessions += 1,
            "nanoclaw/subagent" => subagent_sessions += 1,
            other => panic!("unexpected source_agent {other} for {id}"),
        }
    }
    assert_eq!(main_sessions, 3, "3 top-level transcripts");
    assert_eq!(subagent_sessions, 5, "5 subagent sidecars");
    assert!(total_messages > 0, "the corpus carries messages");

    // The search index folded these sessions: the nanoclaw main scope is
    // non-empty (excludes subagents by design, per spec search scope).
    let searchable = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("nanoclaw".to_owned()),
        ))
        .await?;
    assert!(
        searchable > 0,
        "nanoclaw main sessions must be searchable after ingest"
    );
    Ok(())
}

/// Seed a minimal `v2.db` covering the join tables the metadata enrichment reads.
/// `sessions` provider columns drive both codex classification and opencode
/// enrichment; the shapes are a subset of the real nanoclaw schema.
fn seed_v2_db(v2_db: &Path, statements: &str) -> anyhow::Result<()> {
    let conn = Connection::open(v2_db)?;
    conn.execute_batch(
        "CREATE TABLE agent_groups (id TEXT PRIMARY KEY, name TEXT NOT NULL, folder TEXT NOT NULL, agent_provider TEXT, created_at TEXT NOT NULL);
         CREATE TABLE messaging_groups (id TEXT PRIMARY KEY, channel_type TEXT NOT NULL, platform_id TEXT NOT NULL, instance TEXT NOT NULL, name TEXT, is_group INTEGER DEFAULT 0, created_at TEXT NOT NULL);
         CREATE TABLE container_configs (agent_group_id TEXT PRIMARY KEY, provider TEXT, model TEXT, assistant_name TEXT, updated_at TEXT NOT NULL);
         CREATE TABLE sessions (id TEXT PRIMARY KEY, agent_group_id TEXT NOT NULL, messaging_group_id TEXT, thread_id TEXT, agent_provider TEXT, status TEXT, created_at TEXT NOT NULL);",
    )?;
    conn.execute_batch(statements)?;
    Ok(())
}

/// opencode-provider composition: a nanoclaw session folder holding an
/// `opencode-xdg/` store is read by the opencode reader and re-attributed -
/// `source_agent` becomes `nanoclaw` (main) / `nanoclaw/subagent` (opencode's own
/// subagent taxonomy), `project` becomes the nanoclaw group, and the nanoclaw
/// metadata lands in `options.nanoclaw` - while opencode's own session/message ids
/// stay canonical.
#[tokio::test(flavor = "multi_thread")]
async fn opencode_provider_sessions_are_composed_and_reattributed() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let group = "agentgroup-oc-001";
    let container_session = "sess-1777000000000-oc";

    // The opencode store lives at <session>/opencode-xdg/opencode/ (OpenCode's XDG
    // data dir, app subdir). Reuse the committed opencode DB fixture verbatim.
    let xdg = root
        .path()
        .join("data")
        .join("v2-sessions")
        .join(group)
        .join(container_session)
        .join("opencode-xdg")
        .join("opencode");
    std::fs::create_dir_all(&xdg)?;
    std::fs::copy(OPENCODE_DB_FIXTURE, xdg.join("opencode.db"))?;

    // v2.db: the container session is an opencode-provider session; its join
    // metadata (channel, model, provider) enriches options.nanoclaw.
    let v2_db = root.path().join("data").join("v2.db");
    seed_v2_db(
        &v2_db,
        "INSERT INTO agent_groups VALUES ('ag-oc', 'OpenCode Group', 'oc', 'opencode', '2026-04-01T00:00:00Z');
         INSERT INTO messaging_groups VALUES ('mg-oc', 'telegram', 'tg-oc', 'telegram', 'OC Channel', 1, '2026-04-01T00:00:00Z');
         INSERT INTO container_configs VALUES ('ag-oc', 'opencode', 'gpt-5', 'Codey', '2026-04-01T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-1777000000000-oc', 'ag-oc', 'mg-oc', 'thread-oc', 'opencode', 'active', '2026-04-27T00:00:00Z');",
    )?;

    let (store, _guard) = ingest(root.path()).await?;

    let ids = store.session_ids().await?;
    assert_eq!(
        ids.len(),
        OPENCODE_DB_SESSIONS,
        "the whole opencode store composes; only opencode sessions exist in this root"
    );

    // A main opencode session: re-attributed to nanoclaw with the group project
    // and enriched nanoclaw options; the opencode session id is canonical.
    let parent = store
        .get_session(OPENCODE_PARENT_ID)
        .await?
        .expect("opencode parent session composes under its canonical id");
    assert_eq!(parent.session.source_agent, "nanoclaw");
    assert_eq!(&*parent.session.project, group);
    let nanoclaw = parent
        .session
        .options
        .get("nanoclaw")
        .expect("nanoclaw metadata mirrored onto the composed session");
    assert_eq!(
        nanoclaw.get("nanoclaw_session_id").and_then(Value::as_str),
        Some(container_session),
        "the nanoclaw container session id, not opencode's",
    );
    assert_eq!(
        nanoclaw.get("channel_type").and_then(Value::as_str),
        Some("telegram"),
    );
    assert_eq!(
        nanoclaw.get("provider").and_then(Value::as_str),
        Some("opencode"),
    );
    // The source record stays opencode's (honest provenance for restore routing).
    assert_eq!(
        parent
            .session
            .options
            .get("source")
            .and_then(|source| source.get("adapter"))
            .and_then(Value::as_str),
        Some("opencode"),
    );
    assert!(
        parent
            .messages
            .iter()
            .any(|stored| stored.message.id().starts_with("msg_")),
        "opencode message ids stay canonical",
    );

    // opencode's own subagent (parentID set) maps to nanoclaw/subagent; the
    // opencode parent link is preserved verbatim.
    let child = store
        .get_session(OPENCODE_CHILD_ID)
        .await?
        .expect("opencode child session composes");
    assert_eq!(child.session.source_agent, "nanoclaw/subagent");
    assert_eq!(
        child.session.parent_session_id.as_deref(),
        Some(OPENCODE_PARENT_ID),
        "opencode's parent link is carried through composition",
    );
    assert_eq!(&*child.session.project, group);
    Ok(())
}

/// codex-provider sessions keep history server-side (no on-disk transcript), so
/// they surface as visible `Unsupported` skips enumerated from `v2.db` metadata -
/// both a direct `agent_provider='codex'` and a NULL provider falling back to a
/// codex group default. A claude session is never skipped.
#[tokio::test(flavor = "multi_thread")]
async fn codex_provider_sessions_are_visibly_skipped() -> anyhow::Result<()> {
    let root = TempDir::new()?;
    let v2_db = root.path().join("data").join("v2.db");
    std::fs::create_dir_all(v2_db.parent().unwrap())?;
    seed_v2_db(
        &v2_db,
        "INSERT INTO agent_groups VALUES ('ag-codex', 'Codex Group', 'codex', 'codex', '2026-04-01T00:00:00Z');
         INSERT INTO agent_groups VALUES ('ag-claude', 'Claude Group', 'claude', 'claude', '2026-04-01T00:00:00Z');
         INSERT INTO container_configs VALUES ('ag-codex', 'codex', 'gpt-5', 'Cod', '2026-04-01T00:00:00Z');
         INSERT INTO container_configs VALUES ('ag-claude', 'claude', 'claude-opus-4', 'Nyx', '2026-04-01T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-codex-direct', 'ag-claude', 'mg', 'thread', 'codex', 'active', '2026-04-27T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-codex-fallback', 'ag-codex', 'mg', 'thread', NULL, 'active', '2026-04-27T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-claude', 'ag-claude', 'mg', 'thread', 'claude', 'active', '2026-04-27T00:00:00Z');",
    )?;

    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = NanoclawAdapter::new(root.path());
    let mut skipped: Vec<(String, String)> = Vec::new();
    let summary = ingest_adapter(&store, &adapter, &NoopOracle, |event| {
        if let SyncEvent::SessionDone(outcome) = event
            && let SyncStatus::Skipped { reason } = outcome.status
        {
            skipped.push((outcome.session_id.unwrap_or_default(), reason));
        }
    })
    .await?;

    assert_eq!(
        summary.skipped_files, 2,
        "both codex sessions surface as counted Unsupported skips"
    );
    let skipped_ids: Vec<&str> = skipped.iter().map(|(id, _)| id.as_str()).collect();
    assert!(skipped_ids.contains(&"sess-codex-direct"));
    assert!(skipped_ids.contains(&"sess-codex-fallback"));
    assert!(
        !skipped_ids.contains(&"sess-claude"),
        "the claude session is not a codex skip"
    );
    assert!(
        skipped
            .iter()
            .all(|(_, reason)| reason.contains("codex provider keeps history server-side")),
        "the skip reason names why: {skipped:?}",
    );
    assert!(
        store.session_ids().await?.is_empty(),
        "codex sessions ingest nothing - they are skipped, not stored",
    );
    Ok(())
}

/// A second sync of unchanged sources skips fresh via the store's rowmap oracle
/// and stays additive: no session or message is re-inserted or duplicated.
#[tokio::test(flavor = "multi_thread")]
async fn re_sync_skips_fresh_and_is_additive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path().join("store")).await?;
    let adapter = NanoclawAdapter::new(FIXTURE_ROOT);

    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert!(first.sessions_inserted > 0, "first sync ingests the corpus");
    let first_ids = store.session_ids().await?;

    let cache = temp.path().join("cache");
    store.ensure_rowmap(&cache).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    assert!(
        !oracle.is_empty(),
        "resident map populated after first sync"
    );

    let second = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    assert_eq!(
        second.sessions_inserted, 0,
        "an unchanged re-sync inserts no session"
    );
    assert_eq!(second.inserted, 0, "an unchanged re-sync writes nothing");
    assert!(
        second.skipped_fresh > 0,
        "unchanged sessions skip fresh via the resident watermark: {second:?}"
    );
    assert_eq!(
        store.session_ids().await?.len(),
        first_ids.len(),
        "re-sync adds no duplicate session",
    );
    Ok(())
}
