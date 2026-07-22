#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::path::Path;

use chrono::Utc;
use lance::Dataset;
use lance::deps::arrow_array::{Array, Int32Array, StringArray};
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, Store},
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use tempfile::TempDir;
use tokio_stream::StreamExt;

fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

fn session(id: &str) -> Session {
    Session {
        id: id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/pk-test"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

fn replayed_message(session_id: &str, message_id: &str) -> Message {
    Message::Assistant {
        id: message_id.to_owned(),
        session_id: session_id.to_owned(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    }
}

fn replayed_part(session_id: &str, message_id: &str, text: &str) -> Part {
    Part {
        session_id: session_id.to_owned(),
        id: format!("{message_id}:0000"),
        message_id: message_id.to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text { text: s(text) },
    }
}

#[tokio::test]
async fn replayed_part_ids_are_distinct_per_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let parent = session("parent-session");
    let fork = Session {
        id: "fork-session".to_owned(),
        parent_session_id: Some(parent.id.clone()),
        parent_message_id: Some("replayed-message".to_owned()),
        ..session("fork-session")
    };
    let parent_message = replayed_message(&parent.id, "replayed-message");
    let fork_message = replayed_message(&fork.id, "replayed-message");
    let parent_part = replayed_part(&parent.id, parent_message.id(), "parent copy");
    let fork_part = replayed_part(&fork.id, fork_message.id(), "fork copy");

    ingest_events(
        &store,
        vec![
            IngestEvent::Session(parent.clone()),
            IngestEvent::Message(parent_message),
            IngestEvent::Part(parent_part),
            IngestEvent::Session(fork.clone()),
            IngestEvent::Message(fork_message),
            IngestEvent::Part(fork_part.clone()),
        ],
    )
    .await?;

    let (_, messages, parts) = store.row_counts().await?;
    assert_eq!(messages, 2, "both replayed messages persist");
    assert_eq!(parts, 2, "both replayed parts persist");
    assert_no_duplicate_pks(temp.path()).await?;

    let stored = store
        .get_session(&fork.id)
        .await?
        .expect("fork session should be readable");
    let stored_part = &stored.messages[0].parts[0];
    assert_eq!(stored_part.session_id, fork.id);
    assert_eq!(stored_part.message_id, "replayed-message");
    match &stored_part.kind {
        PartKind::Text { text } => {
            assert_eq!(text.as_deref().map(|value| &**value), Some("fork copy"));
        }
        other => panic!("expected text part, got {other:?}"),
    }

    Ok(())
}

async fn assert_no_duplicate_pks(root: &Path) -> anyhow::Result<()> {
    assert_unique(root, "messages", &["session_id", "id"]).await?;
    assert_unique(root, "parts", &["session_id", "message_id", "id"]).await?;
    Ok(())
}

async fn assert_unique(root: &Path, table: &str, columns: &[&str]) -> anyhow::Result<()> {
    // The lance-namespace Directory impl owns the `.lance` directory suffix
    // (spec.md#lance-chokepoints-catalog); pond's table-name constants are bare logical
    // names, so the on-disk dir is a single-suffix `<table>.lance`.
    let uri = root
        .join(format!("{table}.lance"))
        .to_string_lossy()
        .into_owned();
    let dataset = Dataset::open(&uri).await?;
    let mut scanner = dataset.scan();
    scanner.project(columns)?;
    let mut stream = scanner.try_into_stream().await?;
    let mut seen = HashSet::new();
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            rows += 1;
            let key = columns
                .iter()
                .map(|column| pk_value(&batch, column, row))
                .collect::<Vec<_>>()
                .join("\0");
            assert!(seen.insert(key), "{table} contains a duplicate full PK");
        }
    }
    assert_eq!(rows, seen.len(), "{table} duplicate PK check mismatch");
    Ok(())
}

fn pk_value(batch: &lance::deps::arrow_array::RecordBatch, column: &str, row: usize) -> String {
    let array = batch
        .column_by_name(column)
        .expect("projected column exists");
    assert!(!array.is_null(row), "PK column must be non-null");
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        strings.value(row).to_owned()
    } else if let Some(ints) = array.as_any().downcast_ref::<Int32Array>() {
        ints.value(row).to_string()
    } else {
        panic!("unsupported PK column type for {column}")
    }
}
