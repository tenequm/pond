//! hermes adapter integration suite: ingest -> Store -> get/search over the
//! committed synthetic `state.db` corpus (default profile + a `profiles/coder`
//! profile DB), plus additive re-sync freshness through the store's rowmap
//! oracle. Single-module mapping behavior (content sentinel decode, lineage
//! classification, watermark math, serialize) stays in the
//! `src/adapter/hermes.rs` unit tests; this suite covers the cross-module paths.
//! The whole corpus is synthetic - no real `~/.hermes` data was copied in.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{HermesAdapter, NoopOracle, SkipOracle},
    handlers::ingest_adapter,
    sessions::Store,
    substrate::{Predicate, ScalarValue},
    wire::PartKind,
};
use tempfile::TempDir;

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/adapter/hermes");

// 6 sessions in the default `state.db` + 1 in `profiles/coder/state.db`.
const FIXTURE_SESSIONS: usize = 7;

async fn ingest(root: &Path) -> anyhow::Result<(Store, TempDir)> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = HermesAdapter::new(root);
    ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    Ok((store, store_dir))
}

/// Full-corpus ingest through the Store: every session across the default and
/// profile DBs round-trips via get, the source_agent taxonomy is correct, and
/// the hermes main scope is searchable (the FTS/index path ran end to end).
#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let ids = store.session_ids().await?;
    assert_eq!(
        ids.len(),
        FIXTURE_SESSIONS,
        "every session across the default and profile DBs is ingested",
    );

    let mut main = 0usize;
    let mut subagent = 0usize;
    let mut cron = 0usize;
    for id in &ids {
        let session = store
            .get_session(id)
            .await?
            .expect("every enumerated session round-trips through get");
        assert!(
            !session.messages.is_empty(),
            "session {id} carries at least one message",
        );
        match session.session.source_agent.as_str() {
            "hermes" => main += 1,
            "hermes/subagent" => subagent += 1,
            "hermes/cron" => cron += 1,
            other => panic!("unexpected source_agent {other} for {id}"),
        }
    }
    assert_eq!(
        main, 5,
        "5 plain-hermes sessions (incl. branch + compaction)"
    );
    assert_eq!(subagent, 1, "the delegate spawn is hermes/subagent");
    assert_eq!(cron, 1, "the source='cron' session is hermes/cron");

    let searchable = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("hermes".to_owned()),
        ))
        .await?;
    assert!(
        searchable > 0,
        "hermes main sessions must be searchable after ingest",
    );
    Ok(())
}

/// Lineage: `parent_session_id` is carried verbatim and the `relation` tag in
/// `options.hermes` distinguishes the three hermes edge kinds.
#[tokio::test(flavor = "multi_thread")]
async fn lineage_relations_and_parents_are_preserved() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let relation = |session: &pond::sessions::SessionWithMessages| {
        session
            .session
            .options
            .get("hermes")
            .and_then(|hermes| hermes.get("relation"))
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
    };

    let comp = store.get_session("sess-comp").await?.expect("comp session");
    assert_eq!(comp.session.parent_session_id.as_deref(), Some("sess-root"));
    assert_eq!(relation(&comp).as_deref(), Some("compaction_successor"));
    assert_eq!(comp.session.source_agent, "hermes");

    let branch = store.get_session("sess-branch").await?.expect("branch");
    assert_eq!(
        branch.session.parent_session_id.as_deref(),
        Some("sess-root")
    );
    assert_eq!(relation(&branch).as_deref(), Some("branch"));

    let sub = store.get_session("sess-sub").await?.expect("sub");
    assert_eq!(
        sub.session.parent_session_id.as_deref(),
        Some("sess-delegate-parent"),
    );
    assert_eq!(relation(&sub).as_deref(), Some("spawn"));
    assert_eq!(sub.session.source_agent, "hermes/subagent");
    Ok(())
}

/// Project derivation: `session_key` wins when present, `cwd` is the fallback
/// for a non-gateway (cli) session that has neither key nor chat_id.
#[tokio::test(flavor = "multi_thread")]
async fn project_uses_session_key_then_cwd() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let root = store.get_session("sess-root").await?.expect("root");
    assert_eq!(&*root.session.project, "telegram:100:main");

    // The profile DB was enumerated and its cli session's project fell back to cwd.
    let coder = store.get_session("sess-coder-001").await?.expect("coder");
    assert_eq!(&*coder.session.project, "/home/user/projects/demo");
    Ok(())
}

/// A `\x00json:` multimodal user message decodes to a text Part plus an image
/// File Part, and the tool call/result pair links through the store.
#[tokio::test(flavor = "multi_thread")]
async fn multimodal_and_tool_parts_survive_the_store() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;
    let root = store.get_session("sess-root").await?.expect("root");

    let has_image = root.messages.iter().any(|stored| {
        stored
            .parts
            .iter()
            .any(|part| matches!(&part.kind, PartKind::File { data, .. } if matches!(data, pond::wire::FileData::Url(url) if url.contains("photo-a.png"))))
    });
    assert!(
        has_image,
        "the multimodal image_url part decodes to a File part"
    );

    let has_tool_call = root.messages.iter().any(|stored| {
        stored
            .parts
            .iter()
            .any(|part| matches!(&part.kind, PartKind::ToolCall { .. }))
    });
    let has_tool_result = root.messages.iter().any(|stored| {
        stored
            .parts
            .iter()
            .any(|part| matches!(&part.kind, PartKind::ToolResult { .. }))
    });
    assert!(
        has_tool_call && has_tool_result,
        "tool call + result both ingest"
    );
    Ok(())
}

/// A second sync of the unchanged corpus skips fresh via the store's rowmap
/// oracle and stays additive: no session or message is re-inserted.
#[tokio::test(flavor = "multi_thread")]
async fn re_sync_skips_fresh_and_is_additive() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path().join("store")).await?;
    let adapter = HermesAdapter::new(FIXTURE_ROOT);

    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert!(first.sessions_inserted > 0, "first sync ingests the corpus");
    let first_ids = store.session_ids().await?;

    let cache = temp.path().join("cache");
    store.ensure_rowmap(&cache).await?;
    let oracle = store.sync_oracle().await?;
    assert!(
        !oracle.is_empty(),
        "resident map populated after first sync"
    );

    let second = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    assert_eq!(
        second.sessions_inserted, 0,
        "an unchanged re-sync inserts no session",
    );
    assert_eq!(second.inserted, 0, "an unchanged re-sync writes nothing");
    assert!(
        second.skipped_fresh > 0,
        "unchanged sessions skip fresh via the resident watermark: {second:?}",
    );
    assert_eq!(
        store.session_ids().await?.len(),
        first_ids.len(),
        "re-sync adds no duplicate session",
    );
    Ok(())
}
