//! oh-my-pi adapter integration suite: the shared conformance checks over the
//! committed omp fixture corpus, plus the two omp-specific cross-module
//! hazards - brand borrowing from the pi codec it shares, and freshness keyed
//! through the title slot. Single-module mapping behavior (title-slot folding,
//! carrier taxonomy, watermark math, the ingest-only restore refusal) stays in
//! the `src/adapter/oh_my_pi.rs` unit tests.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{OhMyPiAdapter, OhMyPiFactory, SkipOracle},
    sessions::RowmapOracle,
    substrate::{Predicate, ScalarValue},
};

use super::{Conformance, RoundTrip, ingest_into_temp_store};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/oh-my-pi/sessions"
);

// 2 slot-fronted sessions + 1 legacy slot-less file, plus the artifacts
// directory omp 17.3.4 writes: a `task` subagent and its own nested child.
const FIXTURE_SESSIONS: usize = 5;
const SLOT_FRONTED_SESSION: &str = "0a1b2c3d4e5f6071";

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &OhMyPiFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        expected_sessions: FIXTURE_SESSIONS,
        round_trip: RoundTrip::IngestOnly,
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

/// The brand is omp's own, never borrowed from the pi codec it shares.
#[tokio::test(flavor = "multi_thread")]
async fn no_omp_row_brands_itself_as_pi() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&OhMyPiAdapter::new(FIXTURE_ROOT)).await?;

    let as_pi = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("pi-coding-agent".to_owned()),
        ))
        .await?;
    assert_eq!(as_pi, 0, "no omp row may brand itself as pi");
    Ok(())
}

/// The freshness peek must read the session id from behind the title slot: a
/// regression there is silent - the rowmap key would be the slot line, every
/// re-sync would re-read the file, and `assert_resync_is_noop` alone could not
/// say WHY.
#[tokio::test(flavor = "multi_thread")]
async fn the_rowmap_keys_the_slot_fronted_session_by_its_header_id() -> anyhow::Result<()> {
    let (store, store_dir) = ingest_into_temp_store(&OhMyPiAdapter::new(FIXTURE_ROOT)).await?;

    store.ensure_rowmap(&store_dir.path().join("cache")).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    assert!(
        oracle.session_max_ts(SLOT_FRONTED_SESSION).is_some(),
        "the slot-fronted session is keyed by its header id, not skipped as unreadable",
    );
    Ok(())
}
