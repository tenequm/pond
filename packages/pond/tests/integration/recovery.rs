//! Durable-copy story (spec.md#session-durable-copy): `pond copy --to <file>` produces a
//! portable snapshot of canonical session rows that can be ingested into a fresh store.
//! This test proves the loop round-trips identical row counts and identical
//! `pond copy --to <file>` output.
//!
//! Plus: the JSONL wire stream produces `IngestEvent`s that round-trip back
//! through `ingest_events`, so `copy --to - | ingest` is a portable backup.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::{ingest_adapter, ingest_events, pond_export},
    sessions::{IngestEvent, Store},
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

// ---------------------------------------------------------------------------
// Local-store crash-consistency heal (fix #110). These drive the self-heal in
// `open_or_create_via_ns`: a crashed local commit leaves a zero-byte/truncated
// head manifest that poisons a table permanently; opening must roll it back to
// the newest fully readable version by quarantining (never deleting) the bad
// manifests, and the next sync re-ingests the aborted commit.
// ---------------------------------------------------------------------------

async fn sync_fixtures(store: &Store) -> anyhow::Result<()> {
    ingest_adapter(
        store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    Ok(())
}

fn versions_dir(root: &Path, table: &str) -> PathBuf {
    root.join(format!("{table}.lance")).join("_versions")
}

/// Mirrors `substrate::parse_manifest_version` (private): V2 is a 20-digit
/// `u64::MAX - version`, V1 is the plain version; detached/non-`.manifest`
/// files are skipped.
fn parse_manifest_version(name: &str) -> Option<u64> {
    if name.starts_with('d') {
        return None;
    }
    let stem = name.strip_suffix(".manifest")?;
    if stem.len() == 20 {
        stem.parse::<u64>().ok().map(|inv| u64::MAX - inv)
    } else {
        stem.parse::<u64>().ok()
    }
}

/// `(version, path)` for every live manifest under `vdir`, newest first.
fn manifests_desc(vdir: &Path) -> Vec<(u64, PathBuf)> {
    let mut manifests: Vec<(u64, PathBuf)> = std::fs::read_dir(vdir)
        .expect("versions dir exists after a sync")
        .filter_map(|entry| {
            let entry = entry.expect("dir entry");
            parse_manifest_version(&entry.file_name().to_string_lossy())
                .map(|version| (version, entry.path()))
        })
        .collect();
    manifests.sort_by_key(|(version, _)| std::cmp::Reverse(*version));
    manifests
}

fn any_data_file(root: &Path, table: &str) -> PathBuf {
    let data = root.join(format!("{table}.lance")).join("data");
    std::fs::read_dir(&data)
        .expect("data dir exists after a sync")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "lance"))
        .expect("at least one data file")
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_byte_head_manifest_heals() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        sync_fixtures(&store).await?;
        // A second sync guarantees 2+ meaningful versions on messages.
        sync_fixtures(&store).await?;
    }
    let original = {
        let store = Store::open_local(temp.path()).await?;
        let counts = store.row_counts().await?;
        let export = full_export(&store).await?;
        (counts, export)
    };

    let vdir = versions_dir(temp.path(), "messages");
    let head = manifests_desc(&vdir)
        .first()
        .expect("a head manifest")
        .1
        .clone();
    std::fs::write(&head, b"")?;

    // Reopen: heal must roll back to the newest readable version; scans work.
    let store = Store::open_local(temp.path()).await?;
    let healed_counts = store.row_counts().await?;
    let _ = full_export(&store).await?;
    assert!(
        healed_counts.1 <= original.0.1,
        "rollback cannot invent rows: {} > {}",
        healed_counts.1,
        original.0.1,
    );

    // Re-sync restores the full corpus, byte-identical to the pre-corruption state.
    sync_fixtures(&store).await?;
    let restored_counts = store.row_counts().await?;
    let restored_export = full_export(&store).await?;
    assert_eq!(
        restored_counts, original.0,
        "re-sync must restore row counts"
    );
    assert_eq!(
        restored_export, original.1,
        "re-sync must restore canonical stream"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn truncated_head_manifest_heals() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        sync_fixtures(&store).await?;
        sync_fixtures(&store).await?;
    }
    let original = {
        let store = Store::open_local(temp.path()).await?;
        (store.row_counts().await?, full_export(&store).await?)
    };

    let vdir = versions_dir(temp.path(), "messages");
    let head = manifests_desc(&vdir)
        .first()
        .expect("a head manifest")
        .1
        .clone();
    // 8 junk bytes: non-empty but below Lance's 16-byte footer minimum.
    std::fs::write(&head, b"garbage!")?;

    let store = Store::open_local(temp.path()).await?;
    let _ = store.row_counts().await?;
    let _ = full_export(&store).await?;
    sync_fixtures(&store).await?;
    assert_eq!(store.row_counts().await?, original.0);
    assert_eq!(full_export(&store).await?, original.1);
    Ok(())
}

/// Bucket a canonical event stream into `buckets` session-disjoint batches so
/// each batch, ingested separately, produces its own messages append commit -
/// the only way to build 3+ real data versions from an idempotent corpus.
fn bucket_events(events: Vec<IngestEvent>, buckets: usize) -> Vec<Vec<IngestEvent>> {
    use std::collections::HashMap;
    let mut assign: HashMap<String, usize> = HashMap::new();
    let mut next = 0usize;
    let mut out: Vec<Vec<IngestEvent>> = vec![Vec::new(); buckets];
    for event in events {
        let session_id = match &event {
            IngestEvent::Session(session) => session.id.clone(),
            IngestEvent::Message(message) => message.session_id().to_owned(),
            IngestEvent::Part(part) => part.session_id.clone(),
        };
        let bucket = *assign.entry(session_id).or_insert_with(|| {
            let bucket = next % buckets;
            next += 1;
            bucket
        });
        out[bucket].push(event);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn two_consecutive_poisoned_versions_heal() -> anyhow::Result<()> {
    // Source corpus + its canonical export, then rebuild it as three separate
    // messages commits so the heal walk has 3+ readable versions to descend.
    let source = TempDir::new()?;
    let source_store = Store::open_local(source.path()).await?;
    sync_fixtures(&source_store).await?;
    let original_counts = source_store.row_counts().await?;
    let original_export = full_export(&source_store).await?;
    let events = parse_events(&original_export)?;
    drop(source_store);

    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        for batch in bucket_events(events, 3) {
            if !batch.is_empty() {
                ingest_events(&store, batch).await?;
            }
        }
        assert_eq!(
            store.row_counts().await?,
            original_counts,
            "session-disjoint batches must reunite into the full corpus",
        );
    }
    let original = (original_counts, original_export);

    let vdir = versions_dir(temp.path(), "messages");
    let manifests = manifests_desc(&vdir);
    assert!(
        manifests.len() >= 3,
        "need 3+ versions to poison the top two"
    );
    std::fs::write(&manifests[0].1, b"")?;
    std::fs::write(&manifests[1].1, b"")?;

    let store = Store::open_local(temp.path()).await?;
    let _ = store.row_counts().await?;
    let _ = full_export(&store).await?;

    // Both poisoned manifests quarantined, both landed versions gone from _versions.
    for poisoned in &manifests[..2] {
        let name = poisoned.1.file_name().unwrap().to_string_lossy();
        assert!(
            !vdir.join(name.as_ref()).exists(),
            "poisoned manifest {name} must be renamed away",
        );
        let corrupt = {
            let mut os = poisoned.1.clone().into_os_string();
            os.push(".corrupt");
            PathBuf::from(os)
        };
        assert!(corrupt.exists(), "quarantine {corrupt:?} must exist");
    }

    sync_fixtures(&store).await?;
    assert_eq!(store.row_counts().await?, original.0);
    assert_eq!(full_export(&store).await?, original.1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_verify_catches_zeroed_data_file() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        sync_fixtures(&store).await?;
        sync_fixtures(&store).await?;
    }
    let original = {
        let store = Store::open_local(temp.path()).await?;
        (store.row_counts().await?, full_export(&store).await?)
    };

    // Zero both the head manifest AND a referenced data file. A manifest-only
    // heal would stop at N-1 (whose metadata reads clean) and hand back a store
    // that dies on the next real read; the scan-verify probe must reject any
    // version whose data pages no longer read, walking past the poisoned data.
    let vdir = versions_dir(temp.path(), "messages");
    let head = manifests_desc(&vdir)
        .first()
        .expect("a head manifest")
        .1
        .clone();
    let data_file = any_data_file(temp.path(), "messages");
    std::fs::write(&head, b"")?;
    std::fs::write(&data_file, b"")?;

    let store = Store::open_local(temp.path()).await?;
    // The store heal returns is genuinely readable: a full scan (real data-page
    // reads) must not error, whatever version heal landed on.
    let healed = full_export(&store).await;
    assert!(
        healed.is_ok(),
        "heal must not hand back a store whose scan fails: {healed:?}",
    );

    // A subsequent sync rewrites the messages data and restores the full corpus.
    sync_fixtures(&store).await?;
    assert_eq!(store.row_counts().await?, original.0);
    assert_eq!(full_export(&store).await?, original.1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn heal_never_deletes_quarantined_manifests() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        sync_fixtures(&store).await?;
        sync_fixtures(&store).await?;
    }

    let vdir = versions_dir(temp.path(), "messages");
    let before: std::collections::BTreeSet<String> = std::fs::read_dir(&vdir)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let head = manifests_desc(&vdir)
        .first()
        .expect("a head manifest")
        .1
        .clone();
    let head_name = head.file_name().unwrap().to_string_lossy().into_owned();
    let original_bytes = std::fs::read(&head)?;
    std::fs::write(&head, b"")?;

    let store = Store::open_local(temp.path()).await?;
    let _ = store.row_counts().await?;
    drop(store);

    // Every pre-corruption file still exists (possibly renamed to *.corrupt);
    // nothing was removed.
    let after: std::collections::BTreeSet<String> = std::fs::read_dir(&vdir)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    for name in &before {
        let survived = after.contains(name) || after.contains(&format!("{name}.corrupt"));
        assert!(survived, "heal deleted {name}; it must only rename");
    }
    // The quarantined file keeps the (zeroed) content we wrote - the rename is
    // byte-preserving, it does not restore or rewrite.
    let corrupt = vdir.join(format!("{head_name}.corrupt"));
    assert!(corrupt.exists(), "head must be quarantined, not deleted");
    assert!(
        std::fs::read(&corrupt)?.is_empty(),
        "quarantine preserves the corrupt bytes verbatim",
    );
    assert!(
        !original_bytes.is_empty(),
        "sanity: head was non-empty before"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unrecoverable_store_returns_enriched_error_without_quarantine() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    {
        let store = Store::open_local(temp.path()).await?;
        sync_fixtures(&store).await?;
        sync_fixtures(&store).await?;
    }

    // Zero EVERY messages manifest: no version is readable, so heal can find no
    // rollback target. It must touch nothing and return the original error
    // enriched with what it inspected and the concrete recovery (Layer 3).
    let vdir = versions_dir(temp.path(), "messages");
    let before: Vec<String> = std::fs::read_dir(&vdir)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    for (_, path) in manifests_desc(&vdir) {
        std::fs::write(&path, b"")?;
    }

    let opened = Store::open_local(temp.path()).await;
    let error = opened.expect_err("open must fail when no version is readable");
    let rendered = format!("{error:#}");
    assert!(
        rendered.contains("messages") && rendered.contains("nothing was quarantined"),
        "error must name the table and that nothing was quarantined: {rendered}",
    );
    assert!(
        rendered.contains("pond copy") || rendered.contains("pond init"),
        "error must name a concrete recovery: {rendered}",
    );

    // Never quarantines when it cannot repair: _versions is byte-for-byte as we
    // left it, no *.corrupt files.
    let after: Vec<String> = std::fs::read_dir(&vdir)?
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        before, after,
        "no-readable-version heal must not rename anything"
    );
    assert!(
        after.iter().all(|n| !n.ends_with(".corrupt")),
        "no quarantine when heal cannot fix the store",
    );
    Ok(())
}

async fn full_export(store: &Store) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Vec::new();
    pond_export(store, None, &mut buffer).await?;
    Ok(buffer)
}

fn parse_events(jsonl: &[u8]) -> anyhow::Result<Vec<IngestEvent>> {
    let text = std::str::from_utf8(jsonl)?;
    let mut events = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        events.push(serde_json::from_str::<IngestEvent>(line)?);
    }
    Ok(events)
}

#[tokio::test(flavor = "multi_thread")]
async fn rm_and_resync_round_trips_to_identical_state() -> anyhow::Result<()> {
    // First ingest: build the canonical state.
    let original = TempDir::new()?;
    let store = Arc::new(Store::open_local(original.path()).await?);
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let original_counts = store.row_counts().await?;
    let original_export = full_export(&store).await?;
    drop(store);

    // Simulate "rm -rf data_dir && pond sync": fresh data dir, same adapter
    // pointing at the same source. The recovery contract is that the new
    // state is byte-identical to the old state.
    let recovered = TempDir::new()?;
    let store = Store::open_local(recovered.path()).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let recovered_counts = store.row_counts().await?;
    let recovered_export = full_export(&store).await?;

    assert_eq!(
        original_counts, recovered_counts,
        "rm-and-resync must produce identical row counts",
    );
    assert_eq!(
        original_export, recovered_export,
        "rm-and-resync must produce identical canonical event streams",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn export_then_ingest_round_trips_canonical_events() -> anyhow::Result<()> {
    // Build the source state from a real adapter.
    let source = TempDir::new()?;
    let source_store = Store::open_local(source.path()).await?;
    ingest_adapter(
        &source_store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let source_export = full_export(&source_store).await?;
    let source_counts = source_store.row_counts().await?;

    // Round-trip the export back into a fresh store via `ingest_events`
    // (the wire-level path, same one HTTP `/v1/ingest` drives). Identical
    // row counts and identical re-export prove the format is lossless.
    let destination = TempDir::new()?;
    let dest_store = Store::open_local(destination.path()).await?;
    let events = parse_events(&source_export)?;
    assert!(!events.is_empty(), "fixture export must yield events");
    let outcomes = ingest_events(&dest_store, events).await?;
    assert!(
        outcomes
            .iter()
            .all(|outcome| outcome.status != pond::sessions::OutcomeStatus::Error),
        "re-import must not produce any error outcomes",
    );

    let dest_counts = dest_store.row_counts().await?;
    let dest_export = full_export(&dest_store).await?;
    assert_eq!(source_counts, dest_counts);
    assert_eq!(source_export, dest_export);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_bypasses_the_freshness_skip_and_re_reads_every_session() -> anyhow::Result<()> {
    // `pond sync --verify` drives ingest with a `NoopOracle` instead of the
    // per-session watermark map, so the freshness gate never fires and every
    // source body is re-decoded - the full re-read backstop for anything the
    // gate cannot see (spec.md#session-movement-complete).
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let first = ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    assert!(first.sessions_inserted > 0, "fixtures must yield sessions");

    // Skip-everything oracle: a far-future watermark, newer than any source
    // message timestamp, so a normal sync skips every session.
    struct SkipAll;
    impl pond::adapter::SkipOracle for SkipAll {
        fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
            Some(i64::MAX)
        }
    }
    let skipped =
        ingest_adapter(&store, &ClaudeCodeAdapter::new(FIXTURES), &SkipAll, |_| {}).await?;
    assert_eq!(
        skipped.sessions_inserted, 0,
        "a future watermark must insert nothing"
    );
    assert!(
        skipped.skipped_fresh > 0,
        "a normal sync must skip the fresh sessions"
    );

    // `--verify` (NoopOracle): no session is skipped; the idempotent merge
    // re-reads every body and inserts nothing new on already-complete data.
    let verified = ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    assert_eq!(
        verified.skipped_fresh, 0,
        "--verify must not skip any session"
    );
    assert_eq!(
        verified.sessions_inserted, 0,
        "re-reading complete sessions is an idempotent no-op, not a duplicate insert"
    );
    assert_eq!(
        verified.storage_errors, 0,
        "--verify re-ingest must not error"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn export_filtered_to_one_session_carries_only_that_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;

    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("at least one session");

    let mut buffer = Vec::new();
    let summary = pond_export(&store, Some(&session_id), &mut buffer).await?;
    assert_eq!(
        summary.sessions, 1,
        "filter must restrict to exactly one session"
    );
    let events = parse_events(&buffer)?;
    for event in &events {
        let event_session = match event {
            IngestEvent::Session(session) => session.id.clone(),
            IngestEvent::Message(message) => message.session_id().to_owned(),
            IngestEvent::Part(part) => part.session_id.clone(),
        };
        assert_eq!(
            event_session, session_id,
            "no event from a different session should appear in the filtered export",
        );
    }
    Ok(())
}
