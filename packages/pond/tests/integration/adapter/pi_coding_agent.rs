//! pi-coding-agent adapter integration suite: the shared conformance checks
//! over the committed corpus in all three of pi's on-disk formats at once (v3
//! JSONL, v4 JSONL, the harness-v2 SQLite backend), through the two-field
//! config face `pond sync` uses. Single-module mapping behavior and the
//! byte-level codec replay stay in the `src/adapter/pi_coding_agent.rs` unit
//! tests.

use std::path::Path;

use pond::adapter::PiCodingAgentFactory;
use serde_json::{Value, json};

use super::{Conformance, RoundTrip};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/pi-coding-agent"
);

/// pi's config face: the JSONL sessions root plus an optional SQLite database.
/// The database is declared only where it exists, because a `Reingest` restore
/// root holds just the `sessions/` tree pi writes back.
fn pi_config(root: &Path) -> Value {
    let mut config = json!({ "path": root.join("sessions") });
    let sqlite_path = root.join("sqlite").join("pi-sessions.sqlite");
    if sqlite_path.exists() {
        config["sqlite_path"] = json!(sqlite_path);
    }
    config
}

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &PiCodingAgentFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        // 4 v3 files + 2 v4 files + 2 SQLite sessions.
        expected_sessions: 8,
        // Resume emits v3 for every origin, the only format a released pi
        // opens: the 4 v3-origin sessions replay natively; the 2 v4 and 2
        // SQLite sessions are reconstructed and honestly reported Foreign.
        round_trip: RoundTrip::Reingest { downgraded: 4 },
        config: pi_config,
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
async fn native_restore_replays_v3_and_reconstructs_the_rest() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}
