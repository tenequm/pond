//! claude-ai-export adapter integration suite: the shared conformance checks
//! over the committed synthetic export fixture. Single-module mapping behavior
//! (block -> Part, tool linkage, the `.zip` source form, synthetic ids) stays
//! in the `src/adapter/claude_ai_export.rs` unit tests.

use std::path::Path;

use pond::adapter::ClaudeAiExportFactory;

use super::{Conformance, RoundTrip, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/claude_ai_export"
);

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &ClaudeAiExportFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        // 5 conversations in the fixture export, minus the 0-message one.
        expected_sessions: 4,
        resync_rereads: &[],
        // Per-session restore is a one-conversation export the adapter itself
        // re-reads, so the round trip closes entirely inside pond.
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
