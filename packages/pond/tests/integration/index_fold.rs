//! Incremental scalar-index fold correctness (spec.md#lance-index-maintenance,
//! 3.7). Workstream A switched the scalar indexes off the full
//! `create_index(replace=true)` rebuild onto `optimize_indices(append)` - the
//! workaround for Lance v7.0.0-beta.16's `RowAddrTreeMap::from_sorted_iter`
//! panic is no longer needed (7.0.0's `FlatIndex::try_new` sorts by row id
//! before building the bitmap). The load-bearing guarantee these tests prove is
//! that the fold path is byte-identical to a from-scratch rebuild on every
//! field - so the switch cannot change behavior - plus correct BTree
//! (`session_id`) and Bitmap (`source_agent`) pushdown. The last two tests
//! exercise the compaction interaction: row-id-domain indexes must survive a
//! fragment rewrite via stable row ids (and the post-compaction index phase
//! re-folds regardless), while the address-domain zonemap does NOT survive -
//! compaction orphans its payload, and the same-run staleness probe must
//! detect that and recreate it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use chrono::{DateTime, TimeZone, Utc};
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, Store},
    substrate::{
        MaintenancePolicy, OptimizeEvent, OptimizePhase, OptimizeProgressFn, PhaseOutcome,
        Predicate, ScalarValue,
    },
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use url::Url;

const SESSION_ID_BTREE: &str = "messages_session_id_btree";
const SOURCE_AGENT_BITMAP: &str = "messages_source_agent_bitmap";

fn make_session(id: &str, source_agent: &str) -> Session {
    Session {
        id: id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: source_agent.to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/fold"}), "x").unwrap(),
        options: ProviderOptions::new(),
    }
}

fn text_part(session_id: &str, idx: usize) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: format!("msg-{idx}:0001"),
        message_id: format!("msg-{idx}"),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: pond::adapter::extract_str(&serde_json::json!({"x": format!("body {idx}")}), "x"),
        },
    }
}

/// One ingest wave: a session plus `count` user messages whose ids start at
/// `base`, each carrying a conversational text part (so `search_text` is
/// non-null) and the supplied timestamp.
fn wave_events(
    session: &Session,
    base: usize,
    count: usize,
    ts: DateTime<Utc>,
) -> Vec<IngestEvent> {
    let mut events = vec![IngestEvent::Session(session.clone())];
    for offset in 0..count {
        let idx = base + offset;
        events.push(IngestEvent::Message(Message::User {
            id: format!("msg-{idx}"),
            session_id: session.id.clone(),
            timestamp: ts,
            options: ProviderOptions::new(),
        }));
        events.push(IngestEvent::Part(text_part(&session.id, idx)));
    }
    events
}

fn day(year: i32, month: u32, day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, 12, 0, 0).unwrap()
}

fn agent(value: &str) -> Predicate {
    Predicate::Eq("source_agent", ScalarValue::String(value.to_owned()))
}

fn messages_indices(outcome: &pond::sessions::OptimizeOutcome) -> &PhaseOutcome {
    &outcome
        .tables
        .iter()
        .find(|t| t.table.as_str() == "messages")
        .expect("messages table in optimize outcome")
        .indices
}

/// The fold path never errors, its pushdown results are correct, and dropping
/// every scalar index then rebuilding from scratch yields *identical* answers -
/// so `optimize_indices(append)` is behaviorally equivalent to the
/// `create_index(replace=true)` rebuild it replaced. The equivalence is the
/// load-bearing guarantee that switching the scalar indexes off the full
/// rebuild is safe.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_fold_matches_full_rebuild() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-index-fold-equiv/")?;
    let store = Store::open(&url).await?;

    let jan = make_session("01HXYFOLDJAN0001", "claude-code");
    let feb = make_session("01HXYFOLDFEB0001", "codex");

    // Wave 1: build indexes over the January session only.
    ingest_events(&store, wave_events(&jan, 0, 30, day(2026, 1, 15))).await?;
    let built = store.build_indices_only(None).await?;
    assert!(
        matches!(messages_indices(&built), PhaseOutcome::Ok),
        "first build must create the messages indexes, got {:?}",
        messages_indices(&built),
    );

    // Wave 2: new fragments for the February session -> unindexed tail folded
    // by `optimize_indices(append)` (the path this workstream switched to).
    ingest_events(&store, wave_events(&feb, 100, 20, day(2026, 2, 10))).await?;
    let folded = store.build_indices_only(None).await?;
    assert!(
        matches!(messages_indices(&folded), PhaseOutcome::Ok),
        "the append fold must commit work over the unindexed tail (not Noop/Failed), got {:?}",
        messages_indices(&folded),
    );
    let after_fold = corpus_snapshot(&store).await?;

    // Drop every scalar index and rebuild from scratch, then re-measure.
    store.drop_index_by_name(SESSION_ID_BTREE).await?;
    store.drop_index_by_name(SOURCE_AGENT_BITMAP).await?;
    let rebuilt = store.build_indices_only(None).await?;
    assert!(
        matches!(messages_indices(&rebuilt), PhaseOutcome::Ok),
        "rebuild-from-scratch must recreate the dropped indexes, got {:?}",
        messages_indices(&rebuilt),
    );
    let after_rebuild = corpus_snapshot(&store).await?;

    assert_eq!(
        after_fold, after_rebuild,
        "append-fold results must equal full-rebuild results on every field",
    );
    assert_correct(&after_fold, "after fold");
    Ok(())
}

/// Compaction rewrites fragments; stable row ids make the rewrite remap a no-op
/// and the index phase runs after compaction, so the scalar pushdown stays
/// correct - never a Failed outcome, and the answers are unchanged from before
/// the compaction.
#[tokio::test(flavor = "multi_thread")]
async fn fold_survives_compaction() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-index-fold-compact/")?;
    let store = Store::open(&url).await?;

    let jan = make_session("01HXYCMPCTJAN001", "claude-code");
    let feb = make_session("01HXYCMPCTFEB001", "codex");

    // Many small ingest+fold waves so compaction has fragments to merge.
    for batch in 0..6 {
        ingest_events(
            &store,
            wave_events(&jan, batch * 5, 5, day(2026, 1, 1 + batch as u32)),
        )
        .await?;
        store.build_indices_only(None).await?;
    }
    ingest_events(&store, wave_events(&feb, 100, 20, day(2026, 2, 10))).await?;
    store.build_indices_only(None).await?;

    let outcome = store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?;
    for table in &outcome.tables {
        assert!(
            !matches!(table.indices, PhaseOutcome::Failed(_)),
            "indices on {} must not Fail across compaction, got {:?}",
            table.table.as_str(),
            table.indices,
        );
    }
    assert_correct(&corpus_snapshot(&store).await?, "after compaction");
    Ok(())
}

/// Progress callback that records the name of every index an optimize run
/// recreates from scratch (`IndexRebuild` phase starts).
fn rebuild_sink() -> (Arc<Mutex<Vec<String>>>, OptimizeProgressFn) {
    let rebuilds: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&rebuilds);
    let progress: OptimizeProgressFn = Arc::new(move |event| {
        if let OptimizeEvent::PhaseStart {
            phase: OptimizePhase::IndexRebuild,
            detail: Some(detail),
            ..
        } = event
        {
            sink.lock().expect("rebuild sink").push(detail);
        }
    });
    (rebuilds, progress)
}

/// Compaction of zonemap-covered fragments orphans the zonemap's
/// address-domain payload: Lance skips index remapping on stable-row-id
/// datasets and only rewrites the manifest `fragment_bitmap`, so the persisted
/// zones keep pointing at the rewritten (dead) fragment ids and every
/// date-filtered query hard-errors ("fragment N referenced by an
/// address-domain index result was not found"). The indices phase runs after
/// the compaction phase, so the staleness probe must detect the orphaned
/// payload and recreate the index within the same optimize run - date-scoped
/// counts stay exact across it.
#[tokio::test(flavor = "multi_thread")]
async fn zonemap_self_heals_after_compaction() -> anyhow::Result<()> {
    let url = Url::parse("shared-memory://pond-test-zonemap-heal/")?;
    let store = Store::open(&url).await?;

    let jan = make_session("01HXYZMHEALJAN01", "claude-code");
    // Many small ingest+fold waves: each fold extends zonemap coverage over a
    // small fragment, giving the later compaction covered fragments to rewrite.
    for batch in 0..6 {
        ingest_events(
            &store,
            wave_events(&jan, batch * 5, 5, day(2026, 1, 1 + batch as u32)),
        )
        .await?;
        store.build_indices_only(None).await?;
    }

    let jan_window = Predicate::And(vec![
        Predicate::Gte(
            "timestamp",
            ScalarValue::Raw("timestamp '2026-01-01 00:00:00'".to_owned()),
        ),
        Predicate::Lte(
            "timestamp",
            ScalarValue::Raw("timestamp '2026-01-31 23:59:59'".to_owned()),
        ),
    ]);
    assert_eq!(
        store.searchable_in_scope(&jan_window).await?,
        30,
        "date-scoped count before compaction",
    );

    // Collect progress events so the heal is asserted directly, not inferred
    // from the count staying right.
    let (rebuilds, progress) = rebuild_sink();
    let outcome = store
        .optimize_indices(Some(progress), &MaintenancePolicy::always_compact())
        .await?;
    for table in &outcome.tables {
        assert!(
            !matches!(table.indices, PhaseOutcome::Failed(_)),
            "indices on {} must not Fail across compaction, got {:?}",
            table.table.as_str(),
            table.indices,
        );
    }
    assert!(
        rebuilds
            .lock()
            .expect("rebuild sink")
            .iter()
            .any(|detail| detail == "messages_timestamp_zonemap"),
        "the staleness probe must recreate the zonemap in the same optimize run",
    );

    assert_eq!(
        store.searchable_in_scope(&jan_window).await?,
        30,
        "date-scoped count after compaction (stale zonemap must self-heal, not error or undercount)",
    );
    Ok(())
}

/// An FTS index built under another stemmer holds different stems for the
/// same words than the running binary produces at query time, so whole-word
/// search silently misses (lance 11 swapped rust-stemmers for frostem:
/// `added`, `internal`, `paste`, ... drift). Pond stamps the manifest config
/// with the building binary's stemmer fingerprint; an index whose stamp is
/// missing - every index built before the stamp existed - or different must
/// be recreated by the next indices phase and re-stamped, and a current stamp
/// must not trigger a rebuild.
#[tokio::test(flavor = "multi_thread")]
async fn fts_index_self_heals_after_a_stemmer_change() -> anyhow::Result<()> {
    use pond::sessions::MESSAGES_FTS_INDEX;
    use pond::substrate::{FTS_STEMMER_KEY, Table, fts_stemmer_fingerprint};

    async fn stamp(store: &Store) -> anyhow::Result<Option<String>> {
        Ok(store
            .dataset(Table::Messages)
            .await?
            .config()
            .get(FTS_STEMMER_KEY)
            .cloned())
    }
    fn fts_outdated(statuses: &[pond::substrate::IndexStatus]) -> bool {
        statuses
            .iter()
            .any(|status| status.intent_name == MESSAGES_FTS_INDEX && !status.stemmer_current)
    }

    let url = Url::parse("shared-memory://pond-test-fts-stemmer-heal/")?;
    let store = Store::open(&url).await?;
    let session = make_session("01HXYZSTEMHEAL01", "claude-code");
    ingest_events(&store, wave_events(&session, 0, 5, day(2026, 1, 1))).await?;
    store.build_indices_only(None).await?.into_result()?;

    let fingerprint = fts_stemmer_fingerprint()?;
    assert!(
        fingerprint.contains("add") && fingerprint.contains("internal"),
        "the fingerprint must carry the canary stems, got {fingerprint:?}",
    );
    assert_eq!(
        stamp(&store).await?.as_deref(),
        Some(fingerprint),
        "a fresh build must stamp the manifest config",
    );

    let (rebuilds, progress) = rebuild_sink();
    store
        .build_indices_only(Some(progress.clone()))
        .await?
        .into_result()?;
    assert!(
        rebuilds.lock().expect("rebuild sink").is_empty(),
        "a current stamp must not trigger a rebuild",
    );
    assert!(!fts_outdated(&store.index_status().await?));

    // A missing stamp (an index built before the stamp existed) and a foreign
    // one (built by a binary linking another stemmer) must both heal. Each
    // pass reopens the store: the old handle would serve its cached manifest
    // inside the freshness window, exactly as a lance-10 index is first seen
    // by an upgraded binary opening the store afresh.
    for foreign in [
        None,
        Some("ad ad intern past even univers interv organ emerg anthropolog"),
    ] {
        let mut dataset = (*store.dataset(Table::Messages).await?).clone();
        dataset.checkout_latest().await?;
        dataset.update_config([(FTS_STEMMER_KEY, foreign)]).await?;
        let store = Store::open(&url).await?;
        assert!(
            fts_outdated(&store.index_status().await?),
            "stamp {foreign:?} must read as outdated",
        );

        rebuilds.lock().expect("rebuild sink").clear();
        store
            .build_indices_only(Some(progress.clone()))
            .await?
            .into_result()?;
        assert_eq!(
            rebuilds.lock().expect("rebuild sink").as_slice(),
            [MESSAGES_FTS_INDEX],
            "stamp {foreign:?} must recreate exactly the FTS index",
        );
        assert_eq!(
            stamp(&store).await?.as_deref(),
            Some(fingerprint),
            "the rebuild must re-stamp the manifest config",
        );
        assert!(!fts_outdated(&store.index_status().await?));
    }
    Ok(())
}

/// Pin the pushdown families: `session_id` BTree (via `get_session`) and
/// `source_agent` Bitmap. The fold-equals-rebuild and survives-compaction
/// assertions prove this workstream neither introduces nor changes their behavior.
fn assert_correct(snap: &Snapshot, label: &str) {
    assert_eq!(snap.jan_messages, 30, "{label}: january session_id BTree");
    assert_eq!(snap.feb_messages, 20, "{label}: february session_id BTree");
    assert_eq!(
        snap.claude_code, 30,
        "{label}: claude-code source_agent Bitmap"
    );
    assert_eq!(snap.codex, 20, "{label}: codex source_agent Bitmap");
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    jan_messages: usize,
    feb_messages: usize,
    claude_code: usize,
    codex: usize,
}

/// Read every scalar-index-backed query the corpus answers: `session_id` BTree
/// (via `get_session`) and `source_agent` Bitmap.
async fn corpus_snapshot(store: &Store) -> anyhow::Result<Snapshot> {
    let jan = match store.get_session("01HXYFOLDJAN0001").await? {
        Some(session) => session,
        None => store
            .get_session("01HXYCMPCTJAN001")
            .await?
            .expect("january session present"),
    };
    let feb = match store.get_session("01HXYFOLDFEB0001").await? {
        Some(session) => session,
        None => store
            .get_session("01HXYCMPCTFEB001")
            .await?
            .expect("february session present"),
    };
    Ok(Snapshot {
        jan_messages: jan.messages.len(),
        feb_messages: feb.messages.len(),
        claude_code: store.searchable_in_scope(&agent("claude-code")).await?,
        codex: store.searchable_in_scope(&agent("codex")).await?,
    })
}
