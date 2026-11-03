use std::path::Path;

use pond::adapter::{GooseAdapter, GooseFactory};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/goose/data"
);

// 4 DB sessions + 2 legacy JSONL sessions (disjoint ids from DB).
const EXPECTED_SESSIONS: usize = 6;

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &GooseFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        expected_sessions: EXPECTED_SESSIONS,
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
async fn restore_is_declared_unsupported() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}

/// sub_agent sessions are branded goose/sub-agent, never bare goose.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_branding_is_not_plain_goose() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&GooseAdapter::new(FIXTURE_ROOT)).await?;

    // The specific sub_agent session is branded goose/sub-agent.
    let sub = store
        .get_session("ses-sub-002")
        .await?
        .expect("sub session");
    assert_eq!(sub.session.source_agent, "goose/sub-agent");

    // No ses-sub* session is branded bare goose.
    let ids = store.session_ids().await?;
    for id in &ids {
        if !id.starts_with("ses-sub") {
            continue;
        }
        let session = store.get_session(id).await?.expect("session exists");
        assert_ne!(
            session.session.source_agent, "goose",
            "{id} is a sub_agent session and must be branded goose/sub-agent"
        );
    }
    Ok(())
}

/// subagent session carries parent_session_id from the DB row.
#[tokio::test(flavor = "multi_thread")]
async fn lineage_lands_for_subagent() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&GooseAdapter::new(FIXTURE_ROOT)).await?;

    let sub = store
        .get_session("ses-sub-002")
        .await?
        .expect("sub session");
    assert_eq!(
        sub.session.parent_session_id.as_deref(),
        Some("ses-user-001"),
        "parent_session_id from the DB row is preserved",
    );
    Ok(())
}
