#![allow(clippy::expect_used, clippy::unwrap_used)]

//! `PondStore::maintenance` (design.md 3.2.0): a pass runs `cleanup_old_versions`
//! then `optimize_indices` over all four datasets, never removes logical rows,
//! and a per-table failure does not abort the others.

use pond::{adapter::ClaudeCodeAdapter, ingest::ingest_adapter, substrate::PondStore};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/claude-code/projects";

#[tokio::test]
async fn maintenance_runs_without_removing_logical_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter).await?;
    store.ensure_indices().await?;

    let before = store.row_counts().await?;

    // A 30-day retention window keeps every manifest version a fresh ingest
    // just wrote, so cleanup is exercised but finds nothing to remove. The
    // point of the assertion is that maintenance touches manifests, not rows.
    let report = store
        .maintenance(chrono::Duration::days(30), false, false)
        .await;
    assert_eq!(report.tables_failed, 0, "no table maintenance should fail");
    assert_eq!(report.tables_optimized, 4, "all four datasets maintained");

    let after = store.row_counts().await?;
    assert_eq!(before, after, "maintenance must never remove logical rows");

    // The skip flags are honored: a fully-skipped pass still reports every
    // table as handled, and still removes nothing.
    let skipped = store
        .maintenance(chrono::Duration::days(30), true, true)
        .await;
    assert_eq!(skipped.tables_optimized, 4);
    assert_eq!(skipped.tables_failed, 0);
    assert_eq!(store.row_counts().await?, before);

    Ok(())
}
