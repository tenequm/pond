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
    sessions::{MessageWrite, SessionWithMessages, Store, search_text},
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
    let oracle = store.sync_oracle().await?;
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
    let oracle = reopened.sync_oracle().await?;
    assert!(
        !oracle.is_empty(),
        "map must be rebuilt after purging the unreadable one"
    );
    Ok(())
}

/// A session whose messages and parts committed but whose row did not - the
/// window `upsert_session_batch`'s write order leaves, which a crash, a SIGTERM
/// to a running sync, or the runtime drop under `serve --with-sync` can land
/// in - must be re-ingested by the next sync, not latched fresh
/// (spec.md#session-movement-complete). The messages-only map calls it fresh:
/// its max_ts already equals the source's last message.
#[tokio::test(flavor = "multi_thread")]
async fn half_flushed_session_is_re_ingested_not_skipped() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);

    // Borrow one fully ingested session's rows from a reference store.
    let reference = Store::open_local(temp.path().join("reference")).await?;
    ingest_adapter(&reference, &adapter, &NoopOracle, |_| {}).await?;
    // `session_ids` is an unordered scan and the corpus also holds subagent and
    // fork sessions (`parent/child` ids), so sort and take the first main one.
    let mut ids = reference.session_ids().await?;
    ids.sort();
    let session_id = ids
        .into_iter()
        .find(|id| !id.contains('/'))
        .expect("fixtures yield a top-level session");
    let SessionWithMessages { session, messages } = reference
        .get_session(&session_id)
        .await?
        .expect("reference holds the session");

    // Fabricate the half flush: messages and parts durable, no session row.
    let store = Store::open_local(temp.path().join("store")).await?;
    let texts: Vec<Option<String>> = messages
        .iter()
        .map(|with_parts| search_text(&with_parts.message, &with_parts.parts))
        .collect();
    let writes: Vec<MessageWrite<'_>> = messages
        .iter()
        .zip(&texts)
        .map(|(with_parts, text)| MessageWrite {
            message: &with_parts.message,
            parts: &with_parts.parts,
            search_text: text.as_deref(),
        })
        .collect();
    store.upsert_messages(&session, &writes).await?;
    let parts: Vec<_> = messages
        .iter()
        .flat_map(|with_parts| with_parts.parts.iter().cloned())
        .collect();
    store.upsert_parts(&parts).await?;
    assert!(
        store.get_session(&session_id).await?.is_none(),
        "fabricated state: the session row is absent"
    );

    let cache = temp.path().join("cache");
    store.ensure_rowmap(&cache).await?;
    // Control: the map alone would call it fresh - this is what the sessions
    // intersection has to override.
    assert!(
        store
            .rowmap_snapshot()
            .expect("map built")
            .lookup_max_ts(&session_id)
            .is_some(),
        "the resident map knows the half-flushed session's messages"
    );
    let oracle = store.sync_oracle().await?;
    assert_eq!(
        oracle.session_max_ts(&session_id),
        None,
        "no durable session row, no watermark"
    );

    ingest_adapter(&store, &adapter, &oracle, |_| {}).await?;
    let healed = store
        .get_session(&session_id)
        .await?
        .expect("the next sync writes the missing session row");
    assert_eq!(
        healed.messages.len(),
        messages.len(),
        "the re-ingest heals the row without duplicating the messages"
    );
    assert_eq!(
        healed.messages.iter().map(|m| m.parts.len()).sum::<usize>(),
        parts.len(),
        "the re-ingest heals the row without duplicating the parts"
    );
    Ok(())
}
