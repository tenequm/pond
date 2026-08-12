//! The `pond sync` freshness gate over the real resident-map oracle
//! (spec.md#adapters): `ensure_rowmap` builds the per-session `max_ts` watermark
//! from the store, [`RowmapOracle`] reads it, and unchanged sources skip without
//! re-decoding or re-writing. `--verify` (a [`NoopOracle`]) bypasses the gate and
//! re-reads everything. This exercises the production wiring end to end, not a
//! hand-built oracle.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::{ClaudeCodeAdapter, NoopOracle, SkipOracle},
    handlers::ingest_adapter,
    sessions::{RowmapOracle, Store},
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

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
