//! oh-my-pi adapter integration suite: ingest -> Store -> searchable scope over
//! the committed omp fixture corpus, plus additive re-sync freshness through the
//! store's rowmap oracle. Single-module mapping behavior (title-slot folding,
//! carrier taxonomy, watermark math, the ingest-only restore refusal) stays in
//! the `src/adapter/oh_my_pi.rs` unit tests; this suite covers the cross-module
//! paths - the ones a slot-fronted file could break without any unit test
//! noticing.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{NoopOracle, OhMyPiAdapter, SkipOracle},
    handlers::ingest_adapter,
    sessions::{RowmapOracle, Store},
    substrate::{Predicate, ScalarValue},
};
use tempfile::TempDir;

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/oh-my-pi/sessions"
);

// 2 slot-fronted sessions + 1 legacy slot-less file.
const FIXTURE_SESSIONS: usize = 3;
const SLOT_FRONTED_SESSION: &str = "0a1b2c3d4e5f6071";

async fn ingest(root: &Path) -> anyhow::Result<(Store, TempDir)> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    ingest_adapter(&store, &OhMyPiAdapter::new(root), &NoopOracle, |_| {}).await?;
    Ok((store, store_dir))
}

/// Full-corpus ingest through the Store: every session lands under the omp
/// brand and its conversational text reaches the searchable scope - the proof
/// the whole pipeline ran, index fold included, on files whose first line is
/// container framing rather than a record.
#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    let (store, _guard) = ingest(Path::new(FIXTURE_ROOT)).await?;

    let ids = store.session_ids().await?;
    assert_eq!(
        ids.len(),
        FIXTURE_SESSIONS,
        "every omp session is ingested, slot-fronted and legacy alike",
    );

    let searchable = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("oh-my-pi".to_owned()),
        ))
        .await?;
    assert!(
        searchable > 0,
        "omp sessions must be searchable after ingest",
    );

    // The brand is omp's own, never borrowed from the pi codec it shares.
    let as_pi = store
        .searchable_in_scope(&Predicate::Eq(
            "source_agent",
            ScalarValue::String("pi-coding-agent".to_owned()),
        ))
        .await?;
    assert_eq!(as_pi, 0, "no omp row may brand itself as pi");
    Ok(())
}

/// Re-sync is additive and skips what is already stored. This only works if the
/// freshness peek reads the session id from behind the title slot: a regression
/// there is silent - it looks like a working sync that re-reads the whole corpus
/// on every run.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = OhMyPiAdapter::new(FIXTURE_ROOT);

    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert!(first.sessions_inserted > 0, "first sync ingests the corpus");

    store.ensure_rowmap(&store_dir.path().join("cache")).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    assert!(
        oracle.session_max_ts(SLOT_FRONTED_SESSION).is_some(),
        "the slot-fronted session is keyed by its header id, not skipped as unreadable",
    );

    let second = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    assert_eq!(
        second.sessions_inserted, 0,
        "an unchanged omp corpus re-syncs no session",
    );
    assert_eq!(second.inserted, 0, "and writes nothing");
    assert_eq!(
        store.session_ids().await?.len(),
        FIXTURE_SESSIONS,
        "and stores no duplicates",
    );
    Ok(())
}
