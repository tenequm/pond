//! `pond copy` store-to-store data path (spec.md#session-durable-copy): plan an
//! incremental delta (sessions absent or grown on the destination, by the
//! per-session message-timestamp key), then stream only that delta straight
//! into the destination merge - no staging copy. The properties under test are
//! the plan's contract: round-trip, rerun-is-a-no-op, union onto a populated
//! destination, and that a second copy moves only what actually changed - all
//! consequences of `lance-deterministic-pk` + merge-insert, asserted here
//! rather than promised.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{DateTime, Utc};
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, OutcomeStatus, Store},
    substrate::Table,
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use std::collections::HashSet;
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
async fn migrate(from: &Store, to: &Store) -> anyhow::Result<pond::sessions::LanceArchiveImport> {
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
async fn migrate_round_trips_reruns_as_noop_and_unions() -> anyhow::Result<()> {
    let source = Store::open(&Url::parse("shared-memory://pond-test-migrate-src/")?).await?;
    seed(&source, "01HXYMIGRATE0001").await?;
    seed(&source, "01HXYMIGRATE0002").await?;

    // Round trip into an empty destination: everything inserts.
    let dest = Store::open(&Url::parse("shared-memory://pond-test-migrate-dst/")?).await?;
    let first = migrate(&source, &dest).await?;
    assert_eq!(first.inserted.sessions, 2);
    assert_eq!(first.inserted.messages, 2);
    assert_eq!(first.inserted.parts, 2);
    let stored = dest
        .get_session("01HXYMIGRATE0001")
        .await?
        .expect("migrated session readable on destination");
    assert_eq!(stored.messages.len(), 1);
    assert_eq!(stored.messages[0].parts.len(), 1);

    // Immediate rerun is a no-op: deterministic PKs make merge-insert skip
    // every row that already landed.
    let rerun = migrate(&source, &dest).await?;
    assert_eq!(rerun.inserted.sessions, 0, "rerun must insert nothing");
    assert_eq!(rerun.inserted.messages, 0);
    assert_eq!(rerun.inserted.parts, 0);

    // Union onto a populated destination: pre-existing rows survive, the
    // archive's rows merge in, nothing is deleted.
    let populated = Store::open(&Url::parse("shared-memory://pond-test-migrate-union/")?).await?;
    seed(&populated, "01HXYMIGRATELOCAL").await?;
    let union = migrate(&source, &populated).await?;
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

    // After migrate: the destination contains every source id, every table.
    migrate(&source, &dest).await?;
    for table in [Table::Sessions, Table::Messages, Table::Parts] {
        let src = source.collect_ids(table).await?;
        let dst = dest.collect_ids(table).await?;
        assert_eq!(
            src.difference(&dst).count(),
            0,
            "{} not fully contained after migrate",
            table.as_str(),
        );
    }

    // A destination carrying extra unrelated rows is still a valid superset -
    // surplus ids are not "missing", so this must verify as synced.
    let populated = Store::open(&Url::parse("shared-memory://pond-test-verify-extra/")?).await?;
    seed(&populated, "01HXYVERIFYLOCAL").await?;
    migrate(&source, &populated).await?;
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
    let first = migrate(&source, &dest).await?;
    assert_eq!(first.inserted.sessions, 2);
    assert_eq!(first.inserted.messages, 2);

    // Grow one session with a second message that REUSES the first message's
    // timestamp (ts(0)) - so only the count, not the max timestamp, reveals the
    // growth - add a wholly new session, leave the third untouched. Source only.
    ingest_at(&source, events_at(grown, ts(0), 2, ts(0))).await?;
    ingest_at(&source, events_at(added, ts(5), 1, ts(5))).await?;

    // The plan names exactly the grown and added sessions, not the unchanged one.
    let plan = dest.plan_incremental_from(&source).await?;
    let planned: HashSet<&str> = plan.sessions.iter().map(String::as_str).collect();
    assert_eq!(
        planned,
        HashSet::from([grown, added]),
        "plan must be {{grown, added}}, was {planned:?}",
    );

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

    // A third copy with no further source changes is a pure no-op plan.
    let noop = dest.plan_incremental_from(&source).await?;
    assert!(noop.is_empty(), "stable source must plan an empty delta");
    assert_eq!(noop.source_sessions, 3);
    Ok(())
}
