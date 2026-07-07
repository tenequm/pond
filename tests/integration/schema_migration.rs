#![allow(clippy::expect_used, clippy::unwrap_used)]

//! In-place additive schema migration: a store written before the
//! materialized tool columns (#89) upgrades on first open via the
//! `add_columns` backfill - values derive from stored `variant_data`, no
//! re-ingest (spec.md#session-durable-copy). The pre-#89 store is simulated
//! faithfully by dropping the materialized columns with raw Lance.

use chrono::Utc;
use lance::index::DatasetIndexExt;
use pond::{
    PROTOCOL_VERSION,
    handlers::{IngestEvent, pond_ingest},
    sessions::Store,
    substrate::Table,
    wire::{IngestEnvelope, IngestRequest, Message, Part, PartKind, Provenance, Session},
};
use serde_json::json;
use tempfile::TempDir;

const SESSION_ID: &str = "migration-test-session";

fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&json!({ "x": value }), "x")
}

async fn seed_store(dir: &TempDir) -> anyhow::Result<()> {
    let store = Store::open_local(dir.path()).await?;
    let session = Session {
        id: SESSION_ID.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&json!({ "p": "/tmp/mig" }), "p").unwrap(),
        options: Default::default(),
    };
    let assistant = Message::Assistant {
        id: "m-asst".to_owned(),
        session_id: SESSION_ID.to_owned(),
        timestamp: Utc::now(),
        options: Default::default(),
    };
    let part = |id: &str, ordinal: i32, kind: PartKind| Part {
        session_id: SESSION_ID.to_owned(),
        id: id.to_owned(),
        message_id: "m-asst".to_owned(),
        ordinal,
        provenance: Provenance::Conversational,
        options: Default::default(),
        kind,
    };
    let events = vec![
        IngestEvent::Session(session),
        IngestEvent::Message(assistant),
        IngestEvent::Part(part(
            "p-text",
            0,
            PartKind::Text {
                text: s("running a command"),
            },
        )),
        IngestEvent::Part(part(
            "p-call",
            1,
            PartKind::ToolCall {
                call_id: s("call-1"),
                name: s("Bash"),
                params: json!({ "command": "false" }),
                provider_executed: false,
            },
        )),
        IngestEvent::Part(part(
            "p-result",
            2,
            PartKind::ToolResult {
                call_id: s("call-1"),
                name: s("Bash"),
                is_failure: true,
                result: json!("exit 1"),
            },
        )),
    ];
    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events,
        },
    )
    .await;
    anyhow::ensure!(
        matches!(envelope, IngestEnvelope::Success(_)),
        "seed ingest failed: {envelope:?}",
    );
    Ok(())
}

async fn run_sql(store: &Store, sql: &str) -> anyhow::Result<String> {
    let tables = pond::sql::Tables {
        sessions: None,
        messages: None,
        parts: Some(store.dataset(Table::Parts).await?),
    };
    match pond::sql::run(&tables, sql, pond::sql::Mode::Inline, 100, None).await {
        Ok(pond::sql::Outcome::Inline(text)) => Ok(text),
        Ok(pond::sql::Outcome::Export { .. }) => anyhow::bail!("unexpected export outcome"),
        Err(error) => anyhow::bail!("sql failed: {error:?}"),
    }
}

#[tokio::test]
async fn old_schema_store_backfills_tool_columns_on_open() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    seed_store(&temp).await?;

    // Simulate a pre-#89 store faithfully: drop the materialized columns so
    // `variant_data` is once again the only carrier of tool identity.
    let parts_uri = temp.path().join("parts.lance");
    let mut dataset = lance::dataset::Dataset::open(parts_uri.to_str().unwrap()).await?;
    dataset
        .drop_columns(&["tool_name", "call_id", "is_failure"])
        .await?;
    let stripped = lance::deps::arrow_schema::Schema::from(dataset.schema());
    assert!(
        stripped.field_with_name("tool_name").is_err(),
        "drop_columns must remove tool_name: {stripped:?}",
    );
    drop(dataset);

    // First open of `parts` runs the in-place backfill.
    let store = Store::open_local(temp.path()).await?;
    let migrated = store.dataset(Table::Parts).await?;
    let schema = lance::deps::arrow_schema::Schema::from(migrated.schema());
    for column in ["tool_name", "call_id", "is_failure"] {
        assert!(
            schema.field_with_name(column).is_ok(),
            "backfill must restore {column}: {schema:?}",
        );
    }

    // The backfilled cells match what ingest would have written - derived
    // from the same PartKind decode.
    let text = run_sql(
        &store,
        "SELECT tool_name, call_id FROM parts WHERE type = 'tool_call'",
    )
    .await?;
    assert!(text.contains("Bash"), "tool_name backfilled: {text}");
    assert!(text.contains("call-1"), "call_id backfilled: {text}");

    let text = run_sql(
        &store,
        "SELECT tool_name, is_failure FROM parts WHERE type = 'tool_result'",
    )
    .await?;
    assert!(text.contains("true"), "is_failure backfilled: {text}");

    let text = run_sql(
        &store,
        "SELECT COUNT(*) AS n FROM parts WHERE tool_name IS NULL",
    )
    .await?;
    assert!(
        text.contains("| 1 |"),
        "non-tool parts stay NULL (the text part): {text}",
    );

    // The declared scalar index on the backfilled column builds through the
    // standard missing-index path on the next optimize.
    store
        .optimize_indices(None, &pond::substrate::MaintenancePolicy::always_compact())
        .await?;
    let indexed = store.dataset(Table::Parts).await?;
    let indices = indexed.load_indices().await?;
    assert!(
        indices.iter().any(|i| i.name == "parts_tool_name_btree"),
        "optimize must create the tool_name index on a migrated store: {:?}",
        indices.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
    );

    // Idempotence: a second open classifies as Match and touches nothing.
    drop(store);
    let store = Store::open_local(temp.path()).await?;
    let reopened = store.dataset(Table::Parts).await?;
    let version_before = reopened.version().version;
    drop(store);
    let store = Store::open_local(temp.path()).await?;
    let reopened = store.dataset(Table::Parts).await?;
    assert_eq!(
        reopened.version().version,
        version_before,
        "a migrated store must not commit again on reopen",
    );
    Ok(())
}

#[tokio::test]
async fn old_schema_archive_restores_with_derived_columns() -> anyhow::Result<()> {
    let source = TempDir::new()?;
    seed_store(&source).await?;

    // Export an archive, then strip the materialized columns from the archive
    // datasets - a faithful stand-in for an archive written by a pre-#89 pond.
    let archive = TempDir::new()?;
    {
        let store = Store::open_local(source.path()).await?;
        store.export_clean_lance_datasets(archive.path()).await?;
    }
    let parts_uri = archive.path().join("parts.lance");
    let mut dataset = lance::dataset::Dataset::open(parts_uri.to_str().unwrap()).await?;
    dataset
        .drop_columns(&["tool_name", "call_id", "is_failure"])
        .await?;
    let archive_version = dataset.version().version;
    drop(dataset);

    // Restore into a fresh store: the import derives the missing cells at the
    // read boundary - the archive itself is a snapshot and MUST stay untouched.
    let restored_dir = TempDir::new()?;
    let store = Store::open_local(restored_dir.path()).await?;
    let imported = store.import_clean_lance_datasets(archive.path()).await?;
    assert_eq!(imported.rows.parts, 3, "all archive parts imported");

    let text = run_sql(
        &store,
        "SELECT tool_name, call_id FROM parts WHERE type = 'tool_call'",
    )
    .await?;
    assert!(text.contains("Bash"), "tool_name derived on import: {text}");
    assert!(text.contains("call-1"), "call_id derived on import: {text}");

    let untouched = lance::dataset::Dataset::open(parts_uri.to_str().unwrap()).await?;
    assert_eq!(
        untouched.version().version,
        archive_version,
        "restore must never write into the archive",
    );
    Ok(())
}
