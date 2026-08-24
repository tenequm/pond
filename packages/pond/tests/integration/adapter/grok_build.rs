//! grok-build adapter integration suite: the shared conformance checks over
//! the committed sandbox capture, plus the cross-module shapes a reader of
//! this corpus depends on - the tool triple through the store, and the
//! parent-side subagent lineage surviving as a stored `parent_session_id`.
//! Single-module mapping behavior (placement, watermark, carriers, value-equal
//! native restore, the peek read budget) stays in the
//! `src/adapter/grok_build.rs` unit tests.

use std::path::Path;

use pond::{
    adapter::{GrokBuildAdapter, GrokBuildFactory},
    wire::{Message, PartKind},
};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/grok-build/sessions"
);

// 15 session dirs in the capture across three buckets (11 macOS, 1 hash-form
// long-cwd, 2 Windows, 1 subagent child); the `no-updates` dir holds no
// `updates.jsonl` (a model-id rejection before any update), so it is
// structurally invisible to the walk and 14 sessions ingest.
const FIXTURE_SESSIONS: usize = 14;
const TOOLS: &str = "01a0355c-4cdd-7c62-9d7b-f0bd11ba9d2d";
const FORK: &str = "01a0355c-8db1-7e50-a5c7-2201f4086118";
const SUBAGENT_PARENT: &str = "01a0355c-9c5a-71e3-8b8a-253db47e0a24";
const SUBAGENT_CHILD: &str = "01a0355c-aead-7641-8548-7eebefb15237";

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &GrokBuildFactory,
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

/// The tool triple through the store: every call's `call_id` keys a result,
/// the `failed` read of a missing file survives as `is_failure`, and the
/// non-zero-exit shell command stays `completed` (its exit code lives in the
/// result body, per row 5 of the decision table).
#[tokio::test(flavor = "multi_thread")]
async fn the_tool_session_reads_back_with_calls_keyed_to_results() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&GrokBuildAdapter::new(FIXTURE_ROOT)).await?;
    let session = store.get_session(TOOLS).await?.expect("tools session");

    let mut calls = Vec::new();
    let mut results = Vec::new();
    let mut failures = 0usize;
    for message in &session.messages {
        for part in &message.parts {
            match (&message.message, &part.kind) {
                (Message::Assistant { .. }, PartKind::ToolCall { call_id, .. }) => {
                    calls.push(call_id.as_deref().cloned());
                }
                (
                    Message::Tool { .. },
                    PartKind::ToolResult {
                        call_id,
                        is_failure,
                        ..
                    },
                ) => {
                    results.push(call_id.as_deref().cloned());
                    failures += usize::from(*is_failure);
                }
                _ => {}
            }
        }
    }
    assert_eq!(calls.len(), 6, "six tool calls in the capture");
    assert_eq!(
        calls, results,
        "every result keys back to its call in order"
    );
    assert_eq!(
        failures, 1,
        "only the read of the missing file failed; the exit-1 command is completed",
    );
    Ok(())
}

/// Lineage through the store: the fork names its parent from its own summary,
/// the subagent child resolves through the parent-side `meta.json` and takes
/// the `grok-build/subagent` subpath (excluded from default search by the
/// harness's brand-scope rules).
#[tokio::test(flavor = "multi_thread")]
async fn lineage_and_subagent_taxonomy_survive_the_store() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&GrokBuildAdapter::new(FIXTURE_ROOT)).await?;

    let fork = store.get_session(FORK).await?.expect("fork");
    assert_eq!(fork.session.parent_session_id.as_deref(), Some(TOOLS));
    assert_eq!(fork.session.source_agent, "grok-build");

    let child = store.get_session(SUBAGENT_CHILD).await?.expect("child");
    assert_eq!(
        child.session.parent_session_id.as_deref(),
        Some(SUBAGENT_PARENT),
    );
    assert_eq!(child.session.source_agent, "grok-build/subagent");
    Ok(())
}
