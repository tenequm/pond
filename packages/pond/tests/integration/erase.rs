//! Erase enforcement across modules (spec.md#session-append-only-exception):
//! with no erase verb yet, each flow forges the denylist the verb will write
//! and proves sync, copy, archive restore and the read surfaces all honor it.
//! Single-module behavior (the ingest chokepoint, copy planning, the epoch
//! gates) is unit-tested beside its code.
#![expect(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "tests fail by panicking"
)]

use std::collections::BTreeMap;
use std::future::{Ready, ready};
use std::path::Path;

use chrono::DateTime;
use pond::{
    adapter::{Adapter, ClaudeCodeAdapter, NoopOracle},
    embed::LazyEmbedder,
    erase::{ErasedIntent, ErasedOracle},
    handlers::{SyncEvent, SyncStatus, ingest_adapter, pond_get_session, pond_search},
    sessions::{IngestSummary, Store},
    substrate::MaintenancePolicy,
    wire::{GetEnvelope, GetSessionRequest, SearchEnvelope, SearchModeWire, SearchRequest, SortBy},
};
use tempfile::TempDir;

use crate::support::sandboxed_pond;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

fn intent(session_id: &str) -> Ready<anyhow::Result<BTreeMap<String, String>>> {
    ready(Ok(BTreeMap::from([ErasedIntent {
        at: DateTime::from_timestamp(1_790_000_000, 0).unwrap(),
        root: session_id.to_owned(),
    }
    .entry(session_id)])))
}

async fn sync_fixtures(store: &Store) -> anyhow::Result<IngestSummary> {
    ingest_adapter(
        store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &NoopOracle,
        |_| {},
    )
    .await
}

/// The fixture corpus's session ids, sorted, read from a scratch store.
async fn fixture_ids(temp: &TempDir) -> anyhow::Result<Vec<String>> {
    let scratch = Store::open_local(temp.path().join("scratch")).await?;
    sync_fixtures(&scratch).await?;
    let mut ids = scratch.session_ids().await?;
    ids.sort();
    Ok(ids)
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_never_imports_an_erased_session_and_the_plan_counts_it() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let ids = fixture_ids(&temp).await?;
    let erased = ids[0].clone();

    let store = Store::open_local(temp.path().join("store")).await?;
    store.import_erase_intent(intent(&erased)).await?;
    let mut reported_erased = Vec::new();
    let summary = ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &NoopOracle,
        |event| {
            if let SyncEvent::SessionDone(outcome) = event
                && matches!(outcome.status, SyncStatus::Erased)
            {
                reported_erased.extend(outcome.session_id);
            }
        },
    )
    .await?;
    assert!(
        summary.denylisted > 0,
        "the erased source was read and withheld"
    );
    assert_eq!(reported_erased, vec![erased.clone()], "withheld, not ok");
    let stored = store.session_ids().await?;
    assert!(!stored.contains(&erased), "nothing resurrected");
    assert_eq!(stored.len(), ids.len() - 1);

    // The dry-run preview skips the erased source undecoded and counts it.
    let cache = temp.path().join("cache");
    let oracle = ErasedOracle {
        inner: Box::new(store.sync_rowmap_oracle(&cache).await?),
        erased: store.erased_session_ids().await?,
    };
    let plan = ClaudeCodeAdapter::new(FIXTURES)
        .plan(&oracle)
        .await?
        .expect("claude-code previews cheaply");
    assert_eq!(plan.erased, 1);
    assert_eq!(plan.pending, 0);

    // A second sync through that oracle still imports nothing of it.
    let again = ingest_adapter(&store, &ClaudeCodeAdapter::new(FIXTURES), &oracle, |_| {}).await?;
    assert_eq!(again.inserted, 0);
    assert_eq!(again.skipped_erased, 1, "reported as erased, not fresh");
    assert!(!store.session_ids().await?.contains(&erased));
    Ok(())
}

/// A lifted denylist entry stops withholding: the next sync from the
/// still-present source re-ingests the session.
#[tokio::test(flavor = "multi_thread")]
async fn a_lifted_session_re_ingests_on_the_next_sync() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let ids = fixture_ids(&temp).await?;
    let store = Store::open_local(temp.path().join("store")).await?;
    store.import_erase_intent(intent(&ids[0])).await?;
    assert!(sync_fixtures(&store).await?.denylisted > 0);
    assert!(!store.session_ids().await?.contains(&ids[0]));

    assert!(store.lift_erase_intent(&ids[0]).await?.is_some());
    let resync = sync_fixtures(&store).await?;
    assert_eq!(resync.sessions_inserted, 1);
    assert_eq!(resync.denylisted, 0);
    assert_eq!(store.session_ids().await?.len(), ids.len());
    Ok(())
}

fn pond_copy(temp: &TempDir, from: &Path, to: &Path, verify_only: bool) -> std::process::Output {
    let mut command = sandboxed_pond(temp);
    command.args([
        "copy",
        "--from",
        from.to_str().unwrap(),
        "--to",
        to.to_str().unwrap(),
    ]);
    if verify_only {
        command.arg("--verify-only");
    }
    command.output().expect("run pond copy")
}

/// `pond copy` carries the source's intent, withholds the erased session, and
/// its closing verify reports the withheld rows instead of calling them
/// missing. Erased rows already on a destination are reported by session, and
/// neither case fails the copy.
#[tokio::test(flavor = "multi_thread")]
async fn copy_withholds_erased_sessions_both_ways() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let ids = fixture_ids(&temp).await?;
    let (source_path, dest_path) = (temp.path().join("source"), temp.path().join("dest"));
    {
        let source = Store::open_local(&source_path).await?;
        sync_fixtures(&source).await?;
        // The rows stay: a source whose erase never ran its purge.
        source.import_erase_intent(intent(&ids[0])).await?;
    }

    let output = pond_copy(&temp, &source_path, &dest_path, false);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("SYNCED"), "{stdout}");
    assert!(stdout.contains("withheld:"), "{stdout}");
    {
        let dest = Store::open_local(&dest_path).await?;
        assert!(!dest.session_ids().await?.contains(&ids[0]));
        assert!(dest.erased_session_ids().await?.contains(&ids[0]));
        // The destination erases a session it already holds.
        dest.import_erase_intent(intent(&ids[1])).await?;
    }

    let output = pond_copy(&temp, &source_path, &dest_path, true);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains("destination still holds") && stderr.contains(&ids[1]),
        "the erased rows on the destination are named: {stderr}"
    );
    assert!(
        !stderr.contains("pond erase"),
        "no command this binary lacks: {stderr}"
    );
    Ok(())
}

/// A `.pond` archive carries its store's intent, and restoring an archive
/// taken before an erase never brings the erased session back.
#[tokio::test(flavor = "multi_thread")]
async fn archive_round_trip_carries_intent_and_withholds_erased_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let ids = fixture_ids(&temp).await?;
    let origin_path = temp.path().join("origin");
    sync_fixtures(&Store::open_local(&origin_path).await?).await?;
    let before_erase = temp.path().join("before.pond");
    assert!(
        pond_copy(&temp, &origin_path, &before_erase, false)
            .status
            .success()
    );

    let erased_path = temp.path().join("erased");
    Store::open_local(&erased_path)
        .await?
        .import_erase_intent(intent(&ids[0]))
        .await?;
    let output = pond_copy(&temp, &before_erase, &erased_path, false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let erased_store = Store::open_local(&erased_path).await?;
    let restored = erased_store.session_ids().await?;
    assert!(!restored.contains(&ids[0]));
    assert_eq!(restored.len(), ids.len() - 1);
    drop(erased_store);

    let after_erase = temp.path().join("after.pond");
    assert!(
        pond_copy(&temp, &erased_path, &after_erase, false)
            .status
            .success()
    );
    let fresh_path = temp.path().join("fresh");
    assert!(
        pond_copy(&temp, &after_erase, &fresh_path, false)
            .status
            .success()
    );
    let fresh = Store::open_local(&fresh_path).await?;
    assert!(fresh.erased_session_ids().await?.contains(&ids[0]));
    assert!(!fresh.session_ids().await?.contains(&ids[0]));
    Ok(())
}

/// While rows of an erased session still exist (an anomaly window), search
/// hides them and get-session treats the id as absent. (`pond_sql`, the raw
/// escape hatch, is deliberately left unfiltered.)
#[tokio::test(flavor = "multi_thread")]
async fn search_and_get_suppress_an_erased_session_with_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    sync_fixtures(&store).await?;
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;
    // FTS never loads the model, so the unloaded default embedder is enough.
    let embedder = LazyEmbedder::candle();
    let search = |query: String| SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query,
        mode: SearchModeWire::Fts,
        sort_by: SortBy::Relevance,
        filters: Default::default(),
        limit: 20,
    };
    let get = |id: &str| GetSessionRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        id: id.to_owned(),
        limit: 1000,
        from: Default::default(),
        after_message_id: None,
        before_message_id: None,
    };
    let hit_sessions = |envelope: SearchEnvelope| match envelope {
        SearchEnvelope::Success(response) => response
            .sessions
            .into_iter()
            .map(|session| session.session_id)
            .collect::<Vec<_>>(),
        SearchEnvelope::Error(error) => panic!("search failed: {error:?}"),
    };

    // A searchable word from a session's own transcript.
    let (target, word) = {
        let mut found = None;
        for id in store.session_ids().await? {
            if let GetEnvelope::Success(response) = pond_get_session(&store, get(&id)).await
                && let pond::wire::GetResult::Session { messages, .. } = response.result
                && let Some(text) = messages.iter().find_map(|message| message.text.clone())
                && let Some(word) = text.split_whitespace().find(|word| word.len() > 6)
            {
                found = Some((id, word.to_owned()));
                break;
            }
        }
        found.expect("a fixture session with text")
    };
    let search_config = pond::config::SearchConfig::default();
    assert!(
        hit_sessions(pond_search(&store, &embedder, search(word.clone()), &search_config).await)
            .contains(&target)
    );

    store.import_erase_intent(intent(&target)).await?;
    assert!(
        !hit_sessions(pond_search(&store, &embedder, search(word), &search_config).await)
            .contains(&target)
    );
    assert!(matches!(
        pond_get_session(&store, get(&target)).await,
        GetEnvelope::Error(_)
    ));
    Ok(())
}
