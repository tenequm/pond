//! agy adapter integration suite: the shared conformance checks over the
//! committed Gemini-home fixture (both lanes), plus the lineage and taxonomy
//! the store must hold after a real ingest. Single-module mapping behavior
//! (protobuf decode, project chain, tool pairing, in-flight carriers, the
//! freshness watermark) stays in the `src/adapter/agy.rs` unit tests.

use std::path::Path;

use pond::adapter::{AgyAdapter, AgyFactory};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/adapter/agy");

// 13 conversation databases (10 CLI, 3 ACP); the never-prompted ACP one holds
// no steps and ingests nothing.
const FIXTURE_SESSIONS: usize = 12;
const SUBAGENT_PARENT: &str = "6479151c-9891-477a-b60d-df51a4e1dd07";
const SUBAGENT_CHILD: &str = "11b9a01c-2233-415d-b4be-a0204e8ab2ff";
const FORK_PARENT: &str = "07439cf3-11de-46cd-ae53-730229b38bac";
const FORK_CHILD: &str = "3abd71a7-c181-48a7-a473-16f564089f7a";

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &AgyFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        expected_sessions: FIXTURE_SESSIONS,
        resync_rereads: &[],
        round_trip: RoundTrip::IngestOnly,
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
async fn restore_is_declared_ingest_only() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}

/// Lineage survives the store: the subagent child points at its parent under
/// the kind subpath, and the fork's cut point names a message the parent
/// session actually holds.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_and_fork_lineage_resolve_in_the_store() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&AgyAdapter::new(FIXTURE_ROOT)).await?;

    let child = store
        .get_session(SUBAGENT_CHILD)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent child not stored"))?;
    anyhow::ensure!(child.session.source_agent == "agy/subagent");
    anyhow::ensure!(child.session.parent_session_id.as_deref() == Some(SUBAGENT_PARENT));

    let fork = store
        .get_session(FORK_CHILD)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fork not stored"))?;
    anyhow::ensure!(fork.session.source_agent == "agy");
    anyhow::ensure!(fork.session.parent_session_id.as_deref() == Some(FORK_PARENT));
    let cut = fork
        .session
        .parent_message_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("fork has no cut point"))?;
    let parent = store
        .get_session(FORK_PARENT)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fork parent not stored"))?;
    anyhow::ensure!(
        parent
            .messages
            .iter()
            .any(|message| message.message.id() == cut),
        "fork cut point {cut} is not a message of {FORK_PARENT}",
    );
    // The fork copies the parent's steps verbatim, so it holds the cut-point
    // message too, under its own session id.
    anyhow::ensure!(
        fork.messages
            .iter()
            .any(|message| message.message.id() == cut)
    );
    Ok(())
}
