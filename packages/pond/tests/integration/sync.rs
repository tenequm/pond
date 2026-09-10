//! The `pond sync` freshness gate over the real resident-map oracle
//! (spec.md#adapters): `ensure_rowmap` builds the per-session `max_ts` watermark
//! from the store, [`RowmapOracle`] reads it, and unchanged sources skip without
//! re-decoding or re-writing. `--verify` (a [`NoopOracle`]) bypasses the gate and
//! re-reads everything. This exercises the production wiring end to end, not a
//! hand-built oracle.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::{AgyAdapter, ClaudeCodeAdapter, NoopOracle, SkipOracle},
    handlers::ingest_adapter,
    sessions::{RowmapOracle, Store},
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";
const AGY_FIXTURES: &str = "tests/fixtures/adapter/agy";

#[tokio::test(flavor = "multi_thread")]
async fn rowmap_oracle_skips_unchanged_then_verify_re_reads() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path().join("store")).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);

    // First ingest: a NoopOracle re-reads every source.
    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert!(first.sessions_inserted > 0, "fixtures must yield sessions");

    // Build the resident map and read it as the freshness oracle - the exact
    // path `pond sync` takes (no per-manifest version-resolution storm).
    let cache = temp.path().join("cache");
    store.ensure_rowmap(&cache).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    assert!(!oracle.is_empty(), "resident map is populated after ingest");

    // Re-sync unchanged sources: each session's source timestamp matches the
    // stored watermark, so nothing is re-decoded or re-written.
    let resync = ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    assert_eq!(
        resync.sessions_inserted, 0,
        "an unchanged re-sync inserts no sessions"
    );
    assert_eq!(resync.inserted, 0, "an unchanged re-sync writes nothing");
    assert!(
        resync.skipped_fresh > 0,
        "unchanged sessions skip fresh via the resident watermark, got {resync:?}"
    );

    // `--verify` (NoopOracle) bypasses the gate: every source re-read, but the
    // idempotent merge still writes nothing on already-complete data.
    let verify = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert_eq!(verify.skipped_fresh, 0, "verify skips nothing");
    assert_eq!(
        verify.inserted, 0,
        "re-reading complete data inserts nothing"
    );
    Ok(())
}

/// A store deleted and re-created at the same path must not inherit the old
/// store's rowmap. The cache is keyed by a hash of the storage URL, and nothing
/// in Lance survives a rebuild to tell the two apart - version numbers, row ids
/// and fragment ids all restart - so the stale chain used to be installed as
/// current and gate every session `Fresh` against rows that no longer existed:
/// `pond sync` reported "up to date" into an empty store, exit 0, no warning.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_rebuilt_at_the_same_path_does_not_inherit_the_old_rowmap() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let cache = temp.path().join("cache");
    let store_dir = temp.path().join("store");
    let adapter = ClaudeCodeAdapter::new(FIXTURES);

    // A store with history, and the rowmap that describes it.
    let store = Store::open_local(&store_dir).await?;
    let first = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
    assert!(first.sessions_inserted > 0);
    store.ensure_rowmap(&cache).await?;
    assert!(!RowmapOracle(store.rowmap_snapshot()).is_empty());
    // Drop the mapping before unlinking: Windows refuses to remove a mapped file.
    drop(store);

    // What a user does: wipe the store, keep the path, sync again. The cache
    // directory is untouched, exactly as it survives in ~/.cache/pond.
    std::fs::remove_dir_all(&store_dir)?;
    assert!(
        std::fs::read_dir(&cache)?
            .flatten()
            .any(|entry| entry.path().extension().is_some_and(|ext| ext == "rmm")),
        "the old store's segments must still be on disk for this to be a test"
    );

    let rebuilt = Store::open_local(&store_dir).await?;
    rebuilt.ensure_rowmap(&cache).await?;
    let oracle = RowmapOracle(rebuilt.rowmap_snapshot());
    assert!(
        oracle.is_empty(),
        "an empty store's oracle must be empty; a stale map would gate every source fresh",
    );

    let after = ingest_adapter(&rebuilt, &adapter, &oracle, |_| {}).await?;
    assert_eq!(
        after.sessions_inserted, first.sessions_inserted,
        "every session must land in the rebuilt store, not skip as fresh",
    );
    assert_eq!(after.skipped_fresh, 0, "nothing in an empty store is fresh");
    Ok(())
}

/// The read commands take a different door to the same map: `pond search`,
/// `get-message` and `get-session` call `load_rowmap_if_present`, which
/// installs a discovered chain without building one. Left unguarded, a chain
/// from a previous store at this path is what search hydrates message ids out
/// of - answering with rows attributed to sessions they do not belong to,
/// which is worse than the empty-sync symptom.
///
/// The rebuilt store is populated with a DIFFERENT corpus, for two reasons: an
/// empty store never reaches the version the chain claims, so nothing would
/// install and the test would pass vacuously; and when both stores hold the
/// same rows a stale map is right by coincidence, so only disjoint data can
/// show the hazard.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_path_ignores_a_rowmap_from_a_previous_store() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let cache = temp.path().join("cache");
    let store_dir = temp.path().join("store");

    let store = Store::open_local(&store_dir).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &NoopOracle,
        |_| {},
    )
    .await?;
    store.ensure_rowmap(&cache).await?;
    assert!(!RowmapOracle(store.rowmap_snapshot()).is_empty());
    drop(store);

    std::fs::remove_dir_all(&store_dir)?;
    let rebuilt = Store::open_local(&store_dir).await?;
    ingest_adapter(
        &rebuilt,
        &AgyAdapter::new(AGY_FIXTURES),
        &NoopOracle,
        |_| {},
    )
    .await?;

    // The read path installs a chain, it never builds one - so with the guard
    // holding, this store has no resident map at all rather than another
    // store's.
    rebuilt.load_rowmap_if_present(&cache).await?;
    assert!(
        RowmapOracle(rebuilt.rowmap_snapshot()).is_empty(),
        "a read command must not hydrate from the previous store's map",
    );
    Ok(())
}

/// An on-disk map left by an older pond (incompatible MAGIC) or a corrupt
/// segment must be purged and rebuilt, not error every sync. Regression for the
/// MAGIC bump that introduced the freshness watermark.
#[tokio::test(flavor = "multi_thread")]
async fn ensure_rowmap_rebuilds_an_unreadable_map() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let cache = temp.path().join("cache");
    let store_dir = temp.path().join("store");
    let store = Store::open_local(&store_dir).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;

    // Build a valid map, then overwrite every segment with bytes no current
    // pond can open (simulating an older MAGIC / corruption).
    store.ensure_rowmap(&cache).await?;
    // Release our mapping before rewriting the segment files: Windows forbids
    // writing a file that has a live memory map (POSIX allows it), so the store
    // holding the freshly built mmap must drop it first. A fresh store is opened
    // below to prove the rebuild-on-unreadable path.
    drop(store);
    let mut corrupted = 0;
    for entry in std::fs::read_dir(&cache)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "rmm") {
            std::fs::write(&path, b"PONDRMM0 not a valid map")?;
            corrupted += 1;
        }
    }
    assert!(corrupted > 0, "a segment must exist to corrupt");

    // A fresh Store has no in-memory map, so it must read the corrupt file,
    // purge it, and rebuild - returning Ok, not erroring.
    let reopened = Store::open_local(&store_dir).await?;
    reopened.ensure_rowmap(&cache).await?;
    let oracle = RowmapOracle(reopened.rowmap_snapshot());
    assert!(
        !oracle.is_empty(),
        "map must be rebuilt after purging the unreadable one"
    );
    Ok(())
}
