//! devin adapter integration suite: the shared conformance checks over both
//! committed data roots (macOS and Windows captures), the subagent and fork
//! lineage the store must hold after a real ingest, and the same messages per
//! session (and the same child row) whether the mid-run pair syncs once or
//! twice. Single-module mapping
//! behavior (the forest partition, provenance, tool outcomes, the watermark)
//! stays in the `src/adapter/devin.rs` unit tests.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use pond::{
    adapter::{DevinAdapter, DevinFactory, NoopOracle},
    handlers::ingest_adapter,
    sessions::{RowmapOracle, Store},
    wire::Session,
};
use serde_json::Value;
use tempfile::TempDir;

use super::{Conformance, RoundTrip, ensure_clean_ingest, ingest_into_temp_store, path_config};

const MACOS_ROOT: &str = "tests/fixtures/adapter/devin/macos/cli";
const WINDOWS_ROOT: &str = "tests/fixtures/adapter/devin/windows/cli";

// 5 sessions rows plus 3 subagent children (one in amplified-color, two in
// chalk-twig); the fork power-almandine copies a subagent link but no
// subagent nodes, so it yields no child.
const MACOS_SESSIONS: usize = 8;
// 2 sessions rows plus the explore subagent of viridian-bear.
const WINDOWS_SESSIONS: usize = 3;

const SUBAGENT_PARENT: &str = "amplified-color";
const SUBAGENT_CHILD: &str = "amplified-color/agent-51d1dc31-ee2a-44d7-b6f8-3be0f0cf5d6e";
const FORK: &str = "power-almandine";

// One session captured twice: `before` while its background subagent ran (no
// link row yet), `after` once it reported back, two compactions and a
// whole-forest re-save later. `after` holds 69 distinct messages, each placed
// in either the root's trees or the subagent's, so 69 messages are stored.
const MIDRUN_ROOT: &str = "tests/fixtures/adapter/devin/midrun";
const MIDRUN_PARENT: &str = "gilded-orca";
const MIDRUN_CHILD: &str = "gilded-orca/agent-e9b73e40-5526-42b0-acae-45389ecfe004";
const MIDRUN_MESSAGES: usize = 69;

fn conformance(root: &'static str, sessions: usize) -> Conformance<'static> {
    Conformance {
        factory: &DevinFactory,
        fixture_root: Path::new(root),
        expected_sessions: sessions,
        resync_rereads: &[],
        round_trip: RoundTrip::IngestOnly,
        config: path_config,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn macos_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_ingest_counts_and_searchable()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn windows_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance(WINDOWS_ROOT, WINDOWS_SESSIONS)
        .assert_ingest_counts_and_searchable()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_resync_is_noop()
        .await?;
    conformance(WINDOWS_ROOT, WINDOWS_SESSIONS)
        .assert_resync_is_noop()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_is_declared_ingest_only() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_round_trip()
        .await
}

/// The subagent child points at its parent under the kind subpath and holds
/// its own messages, none of the parent's; the fork stays a root, because the
/// writer records no link for it.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_lineage_resolves_and_forks_stay_roots() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&DevinAdapter::new(MACOS_ROOT)).await?;

    let child = store
        .get_session(SUBAGENT_CHILD)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent child not stored"))?;
    anyhow::ensure!(child.session.source_agent == "devin/subagent");
    anyhow::ensure!(child.session.parent_session_id.as_deref() == Some(SUBAGENT_PARENT));
    anyhow::ensure!(child.session.parent_message_id.is_none());
    let parent = store
        .get_session(SUBAGENT_PARENT)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent parent not stored"))?;
    let suffixes = |session: &pond::sessions::SessionWithMessages| -> HashSet<String> {
        session
            .messages
            .iter()
            .filter_map(|message| {
                message
                    .message
                    .id()
                    .rsplit_once(':')
                    .map(|(_, id)| id.to_owned())
            })
            .collect()
    };
    anyhow::ensure!(
        suffixes(&parent).is_disjoint(&suffixes(&child)),
        "a subagent message is also stored on the parent",
    );

    let fork = store
        .get_session(FORK)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fork not stored"))?;
    anyhow::ensure!(fork.session.source_agent == "devin");
    anyhow::ensure!(fork.session.parent_session_id.is_none());
    Ok(())
}

/// A sync that lands while a subagent runs, then one after it reports back,
/// store the same messages in every session, and the same child row, as one
/// sync after the report would. Additive sync never rewrites a stored row, so
/// this holds only because nothing the adapter emits for a message or a child
/// depends on when it read.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_during_a_subagent_run_stores_what_a_later_sync_would() -> anyhow::Result<()> {
    let stage = |name: &str| DevinAdapter::new(Path::new(MIDRUN_ROOT).join(name).join("cli"));

    let staged_dir = TempDir::new()?;
    let staged = Store::open_local(staged_dir.path().join("store")).await?;
    let first = ingest_adapter(&staged, &stage("before"), &NoopOracle, |_| {}).await?;
    ensure_clean_ingest("devin", &first)?;
    staged
        .ensure_rowmap(&staged_dir.path().join("cache"))
        .await?;
    let oracle = RowmapOracle(staged.rowmap_snapshot());
    let second = ingest_adapter(&staged, &stage("after"), &oracle, |_| {}).await?;
    ensure_clean_ingest("devin", &second)?;

    let (once, _guard) = ingest_into_temp_store(&stage("after")).await?;
    let (staged, once) = (contents(&staged).await?, contents(&once).await?);
    let messages = |store: &Contents| -> Vec<(String, BTreeMap<String, Value>)> {
        store
            .iter()
            .map(|(id, (_, messages))| (id.clone(), messages.clone()))
            .collect()
    };
    anyhow::ensure!(messages(&staged) == messages(&once));
    anyhow::ensure!(staged[MIDRUN_CHILD].0 == once[MIDRUN_CHILD].0);

    let (child, messages) = &once[MIDRUN_CHILD];
    anyhow::ensure!(child.parent_session_id.as_deref() == Some(MIDRUN_PARENT));
    anyhow::ensure!(messages.len() > 1);
    anyhow::ensure!(
        once.values()
            .map(|(_, messages)| messages.len())
            .sum::<usize>()
            == MIDRUN_MESSAGES
    );
    Ok(())
}

type Contents = BTreeMap<String, (Session, BTreeMap<String, Value>)>;

/// Every stored session row with each message's tool state.
async fn contents(store: &Store) -> anyhow::Result<Contents> {
    let mut out = BTreeMap::new();
    for id in store.session_ids().await? {
        let stored = store
            .get_session(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{id} listed but not stored"))?;
        let messages = stored
            .messages
            .iter()
            .map(|message| {
                (
                    message.message.id().to_owned(),
                    message.message.options()["devin"]["tool_call_state"].clone(),
                )
            })
            .collect();
        out.insert(id, (stored.session, messages));
    }
    Ok(out)
}
