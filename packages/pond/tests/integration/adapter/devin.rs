//! devin adapter integration suite: the shared conformance checks over both
//! committed data roots (macOS and Windows captures), plus the subagent and
//! fork lineage the store must hold after a real ingest. Single-module mapping
//! behavior (the forest partition, provenance, tool outcomes, the watermark)
//! stays in the `src/adapter/devin.rs` unit tests.

use std::collections::HashSet;
use std::path::Path;

use pond::adapter::{DevinAdapter, DevinFactory};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const MACOS_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/devin/macos/cli"
);
const WINDOWS_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/devin/windows/cli"
);

// 5 sessions rows plus 3 subagent children (one in amplified-color, two in
// chalk-twig); the fork power-almandine copies a subagent link but no
// subagent nodes, so it yields no child.
const MACOS_SESSIONS: usize = 8;
// 2 sessions rows plus the explore subagent of viridian-bear.
const WINDOWS_SESSIONS: usize = 3;

const SUBAGENT_PARENT: &str = "amplified-color";
const SUBAGENT_CHILD: &str = "amplified-color/agent-398e395d";
const FORK: &str = "power-almandine";

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

/// The subagent child points at its parent under the kind subpath, and its
/// `parent_message_id` names the `run_subagent` result the parent holds; the
/// fork stays a root, because the writer records no link for it.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_lineage_resolves_and_forks_stay_roots() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&DevinAdapter::new(MACOS_ROOT)).await?;

    let child = store
        .get_session(SUBAGENT_CHILD)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent child not stored"))?;
    anyhow::ensure!(child.session.source_agent == "devin/subagent");
    anyhow::ensure!(child.session.parent_session_id.as_deref() == Some(SUBAGENT_PARENT));
    let link = child
        .session
        .parent_message_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("subagent child has no link message"))?;
    let parent = store
        .get_session(SUBAGENT_PARENT)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent parent not stored"))?;
    anyhow::ensure!(
        parent
            .messages
            .iter()
            .any(|message| message.message.id() == link),
        "link message {link} is not a message of the parent",
    );
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
