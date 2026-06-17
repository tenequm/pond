//! `pond copy` store-to-store data path (spec.md#session-durable-copy): plan an
//! incremental delta (sessions absent or grown on the destination, by the
//! per-session message-count key), then stream only that delta straight into
//! the destination - **appending** the absent sessions (one commit per scan, no
//! merge join) and **merging** the grown ones. The properties under test are the
//! plan's contract: round-trip, rerun-is-a-no-op, union onto a populated
//! destination, that a second copy moves only what actually changed, that the
//! append collapses to one commit per table (not one per scan batch), and that a
//! resumed copy never double-appends - all consequences of
//! `lance-deterministic-pk` + append-only storage, asserted here rather than
//! promised.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{DateTime, Utc};
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, OutcomeStatus, Store},
    substrate::Table,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use url::Url;

fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

fn make_events(session_id: &str) -> Vec<IngestEvent> {
    let session = Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/migrate"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    };
    let message = Message::User {
        id: format!("{session_id}-msg-1"),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    };
    let part = Part {
        session_id: session.id.clone(),
        id: format!("{session_id}-msg-1:0001"),
        message_id: message.id().to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: s("migrate me"),
        },
    };
    vec![
        IngestEvent::Session(session),
        IngestEvent::Message(message),
        IngestEvent::Part(part),
    ]
}

async fn seed(store: &Store, session_id: &str) -> anyhow::Result<()> {
    let outcomes = ingest_events(store, make_events(session_id)).await?;
    assert!(
        outcomes.iter().all(|o| o.status != OutcomeStatus::Error),
        "seed ingest must not error: {outcomes:?}",
    );
    Ok(())
}

/// Run the store-to-store copy once: plan the incremental delta, then stream
/// it from `from` into `to` - the same composition the `pond copy` CLI runs.
async fn copy(from: &Store, to: &Store) -> anyhow::Result<pond::sessions::LanceArchiveImport> {
    let plan = to.plan_incremental_from(from).await?;
    to.copy_delta_from(from, &plan).await
}

fn ts(offset_secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + offset_secs, 0).expect("valid timestamp")
}

/// Build a session's three events with a caller-controlled created-at, message
/// number, and message timestamp - so a test can deterministically order an
/// original ingest before a later append (re-sending the session row with the
/// same `created_at` keeps the immutable-fields check happy).
fn events_at(
    session_id: &str,
    created_at: DateTime<Utc>,
    msg_n: u32,
    msg_ts: DateTime<Utc>,
) -> Vec<IngestEvent> {
    let session = Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at,
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/migrate"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    };
    let message = Message::User {
        id: format!("{session_id}-msg-{msg_n}"),
        session_id: session_id.to_owned(),
        timestamp: msg_ts,
        options: ProviderOptions::new(),
    };
    let part = Part {
        session_id: session_id.to_owned(),
        id: format!("{session_id}-msg-{msg_n}:0001"),
        message_id: message.id().to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text { text: s("delta") },
    };
    vec![
        IngestEvent::Session(session),
        IngestEvent::Message(message),
        IngestEvent::Part(part),
    ]
}

async fn ingest_at(store: &Store, events: Vec<IngestEvent>) -> anyhow::Result<()> {
    let outcomes = ingest_events(store, events).await?;
    assert!(
        outcomes.iter().all(|o| o.status != OutcomeStatus::Error),
        "ingest must not error: {outcomes:?}",
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_round_trips_reruns_as_noop_and_unions() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-migrate-src/")?).await?;
    seed(&source, "01HXYMIGRATE0001").await?;
    seed(&source, "01HXYMIGRATE0002").await?;

    // Round trip into an empty destination: everything inserts.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-migrate-dst/")?).await?;
    let first = copy(&source, &dest).await?;
    assert_eq!(first.inserted.sessions, 2);
    assert_eq!(first.inserted.messages, 2);
    assert_eq!(first.inserted.parts, 2);
    let stored = dest
        .get_session("01HXYMIGRATE0001")
        .await?
        .expect("copied session readable on destination");
    assert_eq!(stored.messages.len(), 1);
    assert_eq!(stored.messages[0].parts.len(), 1);

    // Immediate rerun is a no-op: deterministic PKs make merge-insert skip
    // every row that already landed.
    let rerun = copy(&source, &dest).await?;
    assert_eq!(rerun.inserted.sessions, 0, "rerun must insert nothing");
    assert_eq!(rerun.inserted.messages, 0);
    assert_eq!(rerun.inserted.parts, 0);

    // Union onto a populated destination: pre-existing rows survive, the
    // archive's rows merge in, nothing is deleted.
    let populated = Store::open(&Url::parse("shared-memory://pond-test-migrate-union/")?).await?;
    seed(&populated, "01HXYMIGRATELOCAL").await?;
    let union = copy(&source, &populated).await?;
    assert_eq!(union.inserted.sessions, 2);
    let (sessions, messages, parts) = populated.row_counts().await?;
    assert_eq!(sessions, 3, "union must keep the destination's own rows");
    assert_eq!(messages, 3);
    assert_eq!(parts, 3);
    // And the source is untouched.
    let (src_sessions, _, _) = source.row_counts().await?;
    assert_eq!(src_sessions, 2);
    Ok(())
}

/// The id-set comparison behind `pond copy --verify-only` and copy's closing
/// check: a destination is in sync iff it is missing none of the source's
/// per-table ids. Row counts alone can't prove this (a surplus destination
/// matches no count), so verification keys on the deterministic ids.
#[tokio::test(flavor = "multi_thread")]
async fn collect_ids_proves_destination_is_a_superset() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-verify-src/")?).await?;
    seed(&source, "01HXYVERIFY00001").await?;
    seed(&source, "01HXYVERIFY00002").await?;

    // Before any copy: the fresh destination is missing every source id.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-verify-dst/")?).await?;
    let src_sessions = source.collect_ids(Table::Sessions).await?;
    let dst_sessions = dest.collect_ids(Table::Sessions).await?;
    assert_eq!(src_sessions.len(), 2);
    assert!(dst_sessions.is_empty());
    assert_eq!(src_sessions.difference(&dst_sessions).count(), 2);

    // After copy: the destination contains every source id, every table.
    copy(&source, &dest).await?;
    for table in [Table::Sessions, Table::Messages, Table::Parts] {
        let src = source.collect_ids(table).await?;
        let dst = dest.collect_ids(table).await?;
        assert_eq!(
            src.difference(&dst).count(),
            0,
            "{} not fully contained after copy",
            table.as_str(),
        );
    }

    // A destination carrying extra unrelated rows is still a valid superset -
    // surplus ids are not "missing", so this must verify as synced.
    let populated = Store::open(&Url::parse("shared-memory://pond-test-verify-extra/")?).await?;
    seed(&populated, "01HXYVERIFYLOCAL").await?;
    copy(&source, &populated).await?;
    let src = source.collect_ids(Table::Sessions).await?;
    let dst = populated.collect_ids(Table::Sessions).await?;
    assert_eq!(src.difference(&dst).count(), 0, "source fully contained");
    assert_eq!(
        dst.len(),
        3,
        "destination keeps its own row plus the source's"
    );
    Ok(())
}

/// Incremental contract: after a full copy, a second copy transfers only the
/// sessions that are absent (a brand-new session) or grown (an existing session
/// with more messages) on the destination - never an unchanged session. The
/// per-session message-count key is what distinguishes "grown" from "unchanged"
/// across two stores with independent commit clocks. The grown session's new
/// message reuses the existing message's timestamp on purpose: a count key
/// catches it where a `MAX(messages.timestamp)` key would miss it.
#[tokio::test(flavor = "multi_thread")]
async fn incremental_copy_moves_only_absent_or_grown_sessions() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-incremental-src/")?).await?;
    let unchanged = "01HXYINCREMENT001";
    let grown = "01HXYINCREMENT002";
    let added = "01HXYINCREMENT003";
    ingest_at(&source, events_at(unchanged, ts(0), 1, ts(0))).await?;
    ingest_at(&source, events_at(grown, ts(0), 1, ts(0))).await?;

    // First copy is full: both seeded sessions land on the empty destination.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-incremental-dst/")?).await?;
    let first = copy(&source, &dest).await?;
    assert_eq!(first.inserted.sessions, 2);
    assert_eq!(first.inserted.messages, 2);

    // Grow one session with a second message that REUSES the first message's
    // timestamp (ts(0)) - so only the count, not the max timestamp, reveals the
    // growth - add a wholly new session, leave the third untouched. Source only.
    ingest_at(&source, events_at(grown, ts(0), 2, ts(0))).await?;
    ingest_at(&source, events_at(added, ts(5), 1, ts(5))).await?;

    // The plan routes each session per table: the brand-new session is absent
    // everywhere (append in all three tables); the grown session's row is
    // present (no session-table work) but its messages/parts are partially
    // present, so they merge. The unchanged session appears nowhere.
    let plan = dest.plan_incremental_from(&source).await?;
    assert_eq!(
        plan.total(),
        2,
        "only the grown and added sessions are touched"
    );
    assert_eq!(
        plan.sessions.append,
        vec![added.to_owned()],
        "only the brand-new session row is appended",
    );
    assert!(plan.sessions.merge.is_empty(), "session rows never merge");
    assert_eq!(
        plan.messages.append,
        vec![added.to_owned()],
        "the new session's messages append",
    );
    assert_eq!(
        plan.messages.merge,
        vec![grown.to_owned()],
        "the grown session's messages merge (partially present)",
    );
    assert_eq!(plan.parts.append, vec![added.to_owned()]);
    assert_eq!(plan.parts.merge, vec![grown.to_owned()]);

    // The copy inserts only the new rows: the added session row, the added
    // message, and the grown session's one new message - nothing for the
    // unchanged session, and the already-present rows of the grown session are
    // merge-skipped.
    let delta = dest.copy_delta_from(&source, &plan).await?;
    assert_eq!(delta.inserted.sessions, 1, "only the added session is new");
    assert_eq!(
        delta.inserted.messages, 2,
        "the added message plus the grown session's new message",
    );

    // Destination is now a complete superset of the source, every table.
    for table in [Table::Sessions, Table::Messages, Table::Parts] {
        let src = source.collect_ids(table).await?;
        let dst = dest.collect_ids(table).await?;
        assert_eq!(
            src.difference(&dst).count(),
            0,
            "{} not fully contained after incremental copy",
            table.as_str(),
        );
    }

    // Grown still dedups: the destination row counts equal the source's
    // exactly - the grown session's already-present message was merge-skipped,
    // not re-inserted (an append would have duplicated it).
    assert_eq!(
        dest.row_counts().await?,
        source.row_counts().await?,
        "destination must equal source exactly (grown rows deduped, not doubled)",
    );

    // A third copy with no further source changes is a pure no-op plan.
    let noop = dest.plan_incremental_from(&source).await?;
    assert!(noop.is_empty(), "stable source must plan an empty delta");
    assert_eq!(noop.source_sessions, 3);
    Ok(())
}

/// The append fast path collapses to **one commit per table**, independent of
/// how many scan batches the source produces - the property that makes
/// store-to-store copy bandwidth-bound instead of commit-latency-bound. Read
/// the destination `messages` dataset version before and after a from-empty
/// copy and assert it advanced by exactly one (a single `Append`), not once per
/// batch. In-memory, no S3 needed.
#[tokio::test(flavor = "multi_thread")]
async fn append_collapses_to_one_commit_per_table() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-collapse-src/")?).await?;
    for n in 0..24 {
        seed(&source, &format!("01HXYCOLLAPSE{n:04}")).await?;
    }

    let dest = Store::open(&Url::parse("shared-memory://pond-test-collapse-dst/")?).await?;
    // Force the destination table into existence so the "before" version is the
    // empty table, then measure the bump the copy adds.
    let before = dest.dataset(Table::Messages).await?.version_id();
    let plan = dest.plan_incremental_from(&source).await?;
    assert_eq!(
        plan.messages.append.len(),
        24,
        "from-empty: every session's messages append",
    );
    assert!(plan.messages.merge.is_empty());
    dest.copy_delta_from(&source, &plan).await?;
    let after = dest.dataset(Table::Messages).await?.version_id();

    assert_eq!(
        after - before,
        1,
        "from-empty append must be a single commit, not one per scan batch \
         (before={before}, after={after})",
    );
    Ok(())
}

/// A resumed copy never double-appends. Append does not dedup, so correctness
/// rests entirely on re-planning: once a session has landed it is no longer
/// `absent`, so a re-run skips it. Copy fully, copy again, and assert the
/// destination row counts equal the source's exactly - no phantom duplicates.
#[tokio::test(flavor = "multi_thread")]
async fn resumed_copy_appends_no_duplicates() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-resume-src/")?).await?;
    seed(&source, "01HXYRESUME000001").await?;
    seed(&source, "01HXYRESUME000002").await?;
    seed(&source, "01HXYRESUME000003").await?;

    let dest = Store::open(&Url::parse("shared-memory://pond-test-resume-dst/")?).await?;
    copy(&source, &dest).await?;
    // Re-plan sees a full destination: nothing absent, nothing grown.
    let replan = dest.plan_incremental_from(&source).await?;
    assert!(
        replan.is_empty(),
        "a complete destination plans an empty delta"
    );
    let again = dest.copy_delta_from(&source, &replan).await?;
    assert_eq!(again.inserted.messages, 0, "re-run appends nothing");

    assert_eq!(
        dest.row_counts().await?,
        source.row_counts().await?,
        "resumed copy must not duplicate already-appended rows",
    );
    Ok(())
}

/// The case that made a real resumed copy crawl: the destination already has
/// every session *row* (the small `sessions` table committed) but its
/// `messages`/`parts` are empty (the interrupted append never committed - it
/// commits once at the end). The per-table plan must route those messages/parts
/// to **append**, not merge, even though the session ids are present. Proven by
/// the destination `messages` version advancing by exactly one commit.
#[tokio::test(flavor = "multi_thread")]
async fn session_present_but_messages_empty_appends_not_merges() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-msgempty-src/")?).await?;
    let ids: Vec<String> = (0..16).map(|n| format!("01HXYMSGEMPTY{n:04}")).collect();
    for id in &ids {
        seed(&source, id).await?;
    }

    // Destination carries only the session rows - no messages, no parts - the
    // state an interrupted first copy leaves behind.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-msgempty-dst/")?).await?;
    for id in &ids {
        let session = Session {
            id: id.clone(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc::now(),
            project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/migrate"}), "x")
                .unwrap(),
            options: ProviderOptions::new(),
        };
        ingest_at(&dest, vec![IngestEvent::Session(session)]).await?;
    }

    let plan = dest.plan_incremental_from(&source).await?;
    assert!(
        plan.sessions.append.is_empty(),
        "session rows are already present",
    );
    assert_eq!(
        plan.messages.append.len(),
        ids.len(),
        "messages are absent on the destination -> append, not merge",
    );
    assert!(
        plan.messages.merge.is_empty(),
        "nothing to merge: the destination has no messages",
    );
    assert_eq!(plan.parts.append.len(), ids.len());
    assert!(plan.parts.merge.is_empty());

    // The append is a single commit on the messages table despite the session
    // rows already being present - the resumed-copy fast path.
    let before = dest.dataset(Table::Messages).await?.version_id();
    dest.copy_delta_from(&source, &plan).await?;
    let after = dest.dataset(Table::Messages).await?.version_id();
    assert_eq!(
        after - before,
        1,
        "messages must append in one commit, not per-batch merge (before={before}, after={after})",
    );

    // And the destination is now a complete, non-duplicated superset.
    assert_eq!(
        dest.row_counts().await?,
        source.row_counts().await?,
        "destination equals source after appending the missing messages/parts",
    );
    Ok(())
}
