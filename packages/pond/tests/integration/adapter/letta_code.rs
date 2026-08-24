//! letta-code adapter integration suite: the shared conformance checks over
//! the committed sandbox capture, plus the cross-module shape of a `tool_call`
//! row once it has been through the store - one source row, two canonical
//! messages, one `call_id`. Single-module mapping behavior (path identity,
//! watermark, carriers, value-equal native restore, foreign reconstruction)
//! stays in the `src/adapter/letta_code.rs` unit tests.

use std::path::Path;

use pond::{
    adapter::{LettaCodeAdapter, LettaCodeFactory},
    wire::{Message, PartKind},
};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/letta-code/transcripts"
);

// Three agents: three conversations under the first (one of them the synthetic
// legacy shape), one under the second, one under the Windows-captured third;
// the zero-byte `local-conv-3` ingests nothing.
const FIXTURE_SESSIONS: usize = 5;
const AGENT_A: &str = "agent-local-0ce90846-9803-4ab1-8d67-31baacdd5148";

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &LettaCodeFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        expected_sessions: FIXTURE_SESSIONS,
        resync_rereads: &[],
        round_trip: RoundTrip::Reingest { downgraded: &[] },
        config: path_config,
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
async fn native_restore_round_trips_through_reingest() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}

/// A `tool_call` row splits into an Assistant call and a Tool result that
/// still share their `call_id` after the store round-trip, and the reasoning
/// row the capture holds survives as a Reasoning Part - the two shapes a
/// reader of this corpus depends on.
#[tokio::test(flavor = "multi_thread")]
async fn tool_rows_and_reasoning_read_back_from_the_store() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&LettaCodeAdapter::new(FIXTURE_ROOT)).await?;
    let session = store
        .get_session(&format!("{AGENT_A}:default"))
        .await?
        .expect("the default conversation ingests");
    assert_eq!(&*session.session.project, AGENT_A);
    assert_eq!(session.session.parent_session_id, None);

    let mut calls = Vec::new();
    let mut results = Vec::new();
    let mut reasoning = 0usize;
    for message in &session.messages {
        for part in &message.parts {
            match (&message.message, &part.kind) {
                (Message::Assistant { .. }, PartKind::ToolCall { call_id, .. }) => {
                    calls.push(call_id.as_deref().map(String::as_str).map(str::to_owned));
                }
                (Message::Tool { .. }, PartKind::ToolResult { call_id, .. }) => {
                    results.push(call_id.as_deref().map(String::as_str).map(str::to_owned));
                }
                (Message::Assistant { .. }, PartKind::Reasoning { .. }) => reasoning += 1,
                _ => {}
            }
        }
    }
    assert_eq!(calls.len(), 3, "three tool rows in the capture");
    assert_eq!(calls, results, "every result keys back to its call");
    assert!(
        calls
            .iter()
            .all(|id| id.as_deref().is_some_and(|id| id.starts_with("toolu_"))),
        "the provider call id is the correlation key: {calls:?}",
    );
    assert_eq!(reasoning, 1, "the reasoning row is a Reasoning Part");
    Ok(())
}
