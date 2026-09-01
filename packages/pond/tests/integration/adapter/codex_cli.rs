//! codex-cli adapter integration suite: the shared conformance checks over
//! the committed fixture (two enveloped rollouts, one legacy rollout, one
//! sandbox-captured JS-runtime rollout), plus the cross-module shape a reader
//! of the JS-runtime corpus depends on - the wrapped tool name, the executed
//! command, and the failure verdict all surviving the store. Single-module
//! mapping behavior (snippet parsing, window indexing, legacy rows,
//! value-equal native restore) stays in the `src/adapter/codex_cli.rs` unit
//! tests.

use std::path::Path;

use pond::{
    adapter::{CodexCliAdapter, CodexCliFactory},
    wire::{Message, PartKind},
};

use super::{Conformance, RoundTrip, ingest_into_temp_store, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/codex_cli/sessions"
);

const FIXTURE_SESSIONS: usize = 4;
/// The Codex 0.152 `codex exec` capture (census in the fixture README).
const JS_RUNTIME: &str = "01a05e4f-6011-7b73-b3cf-742c36deb501";

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &CodexCliFactory,
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

/// The JS-runtime session through the store: calls are named after the tool
/// their snippet wraps (a snippet wrapping none stays `exec`), results carry
/// the same name, `$.params.command` is the clean argv when one command ran
/// (`executions[]` only when several did), and `is_failure` is set exactly
/// where a command exited non-zero - including inside a multi-command script.
#[tokio::test(flavor = "multi_thread")]
async fn js_runtime_calls_read_back_named_with_commands_and_verdicts() -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&CodexCliAdapter::new(FIXTURE_ROOT)).await?;
    let session = store
        .get_session(JS_RUNTIME)
        .await?
        .expect("js-runtime session");

    let mut calls = Vec::new();
    let mut results = Vec::new();
    for message in &session.messages {
        for part in &message.parts {
            match (&message.message, &part.kind) {
                (Message::Assistant { .. }, PartKind::ToolCall { name, params, .. }) => {
                    calls.push((
                        name.as_ref().map(|n| n.as_str().to_owned()),
                        params.get("command").cloned(),
                        params
                            .get("executions")
                            .and_then(|runs| runs.as_array())
                            .map_or(0, Vec::len),
                    ));
                }
                (
                    Message::Tool { .. },
                    PartKind::ToolResult {
                        name, is_failure, ..
                    },
                ) => {
                    results.push((name.as_ref().map(|n| n.as_str().to_owned()), *is_failure));
                }
                _ => {}
            }
        }
    }

    let argv = |cmd: &str| Some(serde_json::json!(["/bin/zsh", "-lc", cmd]));
    assert_eq!(
        calls,
        vec![
            (Some("exec".to_owned()), None, 0),
            (Some("exec_command".to_owned()), argv("ls"), 0),
            (Some("exec_command".to_owned()), argv("cat missing.txt"), 0),
            (
                Some("exec_command".to_owned()),
                argv("sed -n '1,120p' notes.md"),
                0
            ),
            (Some("apply_patch".to_owned()), None, 0),
            (Some("exec_command".to_owned()), None, 3),
            (Some("exec_command".to_owned()), argv("sh -c \"exit 2\""), 0),
        ],
        "call names, flattened command, and executions[] length (multi-command only)",
    );
    assert_eq!(
        results,
        vec![
            (Some("exec".to_owned()), false),
            (Some("exec_command".to_owned()), false),
            (Some("exec_command".to_owned()), true),
            (Some("exec_command".to_owned()), false),
            (Some("apply_patch".to_owned()), false),
            (Some("exec_command".to_owned()), true),
            (Some("exec_command".to_owned()), true),
        ],
        "result names and failure verdicts",
    );
    Ok(())
}
