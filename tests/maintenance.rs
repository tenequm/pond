#![allow(clippy::expect_used, clippy::unwrap_used)]

//! `Store::maintenance` (design.md 3.2.0): a pass runs `cleanup_old_versions`
//! then `optimize_indices` over all four datasets, never removes logical rows,
//! and a per-table failure does not abort the others.

use pond::{adapter::ClaudeCodeAdapter, handlers::ingest_adapter, sessions::Store};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/claude-code/projects";

#[tokio::test]
async fn maintenance_runs_without_removing_logical_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, |_| {}).await?;
    store.ensure_indices().await?;

    let before = store.row_counts().await?;

    // A 30-day retention window keeps every manifest version a fresh ingest
    // just wrote, so cleanup is exercised but finds nothing to remove. The
    // point of the assertion is that maintenance touches manifests, not rows.
    let report = store.maintenance(chrono::Duration::days(30)).await;
    assert_eq!(report.tables_failed, 0, "no table maintenance should fail");
    assert_eq!(report.tables_optimized, 4, "all four datasets maintained");

    let after = store.row_counts().await?;
    assert_eq!(before, after, "maintenance must never remove logical rows");

    Ok(())
}
