//! End-to-end test for the codex adapter: ingest the committed fixture
//! corpus and assert pond's canonical Session/Message/Part shape comes out
//! the other side. The fixture lives under `tests/fixtures/session-samples/codex/`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{adapter::CodexAdapter, handlers::ingest_adapter, sessions::Store, wire::PartKind};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/codex/sessions";

#[tokio::test(flavor = "multi_thread")]
async fn codex_adapter_ingests_fixture_corpus_into_canonical_shape() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open(temp.path()).await?;
    let adapter = CodexAdapter::new(FIXTURES);

    let summary = ingest_adapter(&store, &adapter).await?;
    // The fixture set is small but every session must round-trip end-to-end.
    assert!(summary.accepted() > 0, "ingest must accept rows");
    assert_eq!(summary.errors, 0, "no per-row validation errors expected");

    let (sessions, messages, parts, _) = store.row_counts().await?;
    assert!(sessions > 0, "at least one codex session");
    assert!(messages > 0, "at least one codex message");
    assert!(parts > 0, "at least one codex Part");

    // Spot-check the canonical shape: walk every stored session and confirm
    // each one carries `source_agent = "codex"` plus a non-empty messages
    // vector with at least one Text Part.
    let mut saw_text_part = false;
    for session_id in store.session_ids().await? {
        let session = store
            .get_session(&session_id)
            .await?
            .expect("session round-trips");
        assert_eq!(session.session.source_agent, "codex");
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
        "codex corpus must contain at least one Text Part"
    );
    Ok(())
}
