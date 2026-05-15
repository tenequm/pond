//! Immutable session-level fields (design.md 3.6.4).
//!
//! `Session.source_agent` and `Session.project` are immutable post-first-write
//! because messages/embeddings denormalize them at first ingest; a silent
//! overwrite would desync the denormalized copies. pond core's
//! `IngestValidator` probes the existing session before the merge_insert
//! and emits a per-row `validation_failed` outcome with the typed field name
//! when either changes. Other Session fields (options, parent_session_id,
//! created_at, parent_message_id) re-write idempotently via merge_insert.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    handlers::ingest_events,
    sessions::{IngestEvent, OutcomeStatus, Store},
    wire::{ProviderOptions, Session},
};
use serde_json::json;
use tempfile::TempDir;

fn base_session() -> Session {
    Session {
        id: "01HXY00000000001".to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: Some("/home/me/proj".to_owned()),
        options: ProviderOptions::new(),
    }
}

fn count_status(outcomes: &[pond::sessions::RowOutcome], target: OutcomeStatus) -> usize {
    outcomes
        .iter()
        .filter(|outcome| outcome.status == target)
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn re_ingesting_a_session_with_unchanged_immutable_fields_is_idempotent() -> anyhow::Result<()>
{
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
    assert_eq!(count_status(&first, OutcomeStatus::Inserted), 1);

    let mut again = base_session();
    again.options.insert("title".to_owned(), json!("renamed"));
    let second = ingest_events(&store, vec![IngestEvent::Session(again)]).await?;
    assert_eq!(
        count_status(&second, OutcomeStatus::Error),
        0,
        "options is mutable; the re-ingest must not surface an error: {second:?}",
    );
    assert_eq!(
        count_status(&second, OutcomeStatus::Matched),
        1,
        "unchanged immutable fields must match-insert via merge_insert",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn re_ingesting_with_changed_source_agent_is_rejected() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
    assert_eq!(count_status(&first, OutcomeStatus::Error), 0);

    let mut tampered = base_session();
    tampered.source_agent = "codex-cli".to_owned();
    let second = ingest_events(&store, vec![IngestEvent::Session(tampered)]).await?;
    assert_eq!(count_status(&second, OutcomeStatus::Error), 1);
    let err_row = second
        .iter()
        .find(|outcome| outcome.status == OutcomeStatus::Error)
        .expect("error outcome present");
    let err = err_row.error.as_ref().expect("error body present");
    assert_eq!(err.field, Some("source_agent"));
    assert_eq!(err.reason, Some("immutable"));

    // The stored row stayed on the original adapter - no silent rewrite.
    let stored = store
        .get_session(&base_session().id)
        .await?
        .expect("session row survives the rejected re-ingest");
    assert_eq!(stored.session.source_agent, "claude-code");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn re_ingesting_with_changed_project_is_rejected() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let first = ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;
    assert_eq!(count_status(&first, OutcomeStatus::Error), 0);

    let mut tampered = base_session();
    tampered.project = Some("/somewhere/else".to_owned());
    let second = ingest_events(&store, vec![IngestEvent::Session(tampered)]).await?;
    let err_row = second
        .iter()
        .find(|outcome| outcome.status == OutcomeStatus::Error)
        .expect("project change must surface an error outcome");
    assert_eq!(err_row.error.as_ref().unwrap().field, Some("project"));

    let stored = store
        .get_session(&base_session().id)
        .await?
        .expect("session row survives");
    assert_eq!(
        stored.session.project.as_deref(),
        Some("/home/me/proj"),
        "stored project must remain the original",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn re_ingesting_a_null_project_when_stored_value_exists_is_rejected() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    ingest_events(&store, vec![IngestEvent::Session(base_session())]).await?;

    let mut tampered = base_session();
    tampered.project = None;
    let second = ingest_events(&store, vec![IngestEvent::Session(tampered)]).await?;
    assert_eq!(
        count_status(&second, OutcomeStatus::Error),
        1,
        "NULL-vs-non-NULL project change must also be rejected",
    );
    Ok(())
}
