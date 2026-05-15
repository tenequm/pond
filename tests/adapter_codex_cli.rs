//! End-to-end test for the codex-cli adapter: ingest the committed fixture
//! corpus and assert pond's canonical Session/Message/Part shape comes out
//! the other side. The fixture lives under
//! `tests/fixtures/session-samples/codex-cli/`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{adapter::CodexCliAdapter, handlers::ingest_adapter, sessions::Store, wire::PartKind};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/codex-cli/sessions";

#[tokio::test(flavor = "multi_thread")]
async fn codex_cli_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = CodexCliAdapter::new(FIXTURES);

    let summary = ingest_adapter(&store, &adapter).await?;
    assert!(summary.accepted() > 0, "ingest must accept rows");
    assert_eq!(summary.errors, 0, "no per-row validation errors expected");

    let (sessions, messages, parts, _) = store.row_counts().await?;
    assert!(sessions > 0, "at least one codex-cli session");
    assert!(messages > 0, "at least one codex-cli message");
    assert!(parts > 0, "at least one codex-cli Part");

    let mut saw_text_part = false;
    for session_id in store.session_ids().await? {
        let session = store
            .get_session(&session_id)
            .await?
            .expect("session round-trips");
        assert_eq!(session.session.source_agent, "codex-cli");
        assert!(
            !session.messages.is_empty(),
            "session {session_id} must carry messages",
        );
        for stored in &session.messages {
            for part in &stored.parts {
                if matches!(part.kind, PartKind::Text { .. }) {
                    saw_text_part = true;
                }
            }
        }
    }
    assert!(
        saw_text_part,
        "codex-cli corpus must contain at least one Text Part",
    );
    Ok(())
}
