//! Per-adapter integration suites, mirroring `src/adapter/`. One file per
//! adapter (`claude_code.rs`, ...). This module root holds only the
//! cross-adapter interop tests (the foreign-restore matrix) and their shared
//! harness - the seam analog of `src/adapter/mod.rs`, which likewise carries
//! the cross-adapter test support. Single-adapter behavior stays in its
//! per-adapter file.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{
        Adapter, ClaudeCodeAdapter, ClaudeCodeFactory, CodexCliAdapter, CodexCliFactory,
        RestoreFidelity,
    },
    handlers::ingest_adapter,
    sessions::Store,
    wire::Message,
};
use tempfile::TempDir;

mod claude_code;
mod hermes;
mod nanoclaw;
mod oh_my_pi;
mod openclaw;
mod opencode;

#[tokio::test(flavor = "multi_thread")]
async fn foreign_restore_codex_to_claude_reparses() -> anyhow::Result<()> {
    assert_foreign_pair(
        CodexCliAdapter::new("tests/fixtures/adapter/codex_cli/sessions"),
        &ClaudeCodeFactory,
        "codex_to_claude",
        TargetRoot::Claude,
        "019c6c57-e2a9-7373-802e-dfcba907221b",
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_restore_claude_to_codex_reparses() -> anyhow::Result<()> {
    assert_foreign_pair(
        ClaudeCodeAdapter::new("tests/fixtures/adapter/claude_code/projects"),
        &CodexCliFactory,
        "claude_to_codex",
        TargetRoot::Codex,
        "5d1e9ffd-ebbc-4ae6-8d3a-501f5cda6dc9",
    )
    .await
}

enum TargetRoot {
    Claude,
    Codex,
}

async fn assert_foreign_pair(
    origin: impl Adapter,
    target: &dyn pond::adapter::AdapterFactory,
    snapshot_name: &str,
    target_root: TargetRoot,
    session_id: &str,
) -> anyhow::Result<()> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    ingest_adapter(&store, &origin, &pond::adapter::NoopOracle, |_| {}).await?;
    // Pin the session explicitly: selecting by sort order would silently
    // swap which session the golden covers whenever a fixture is added.
    let session = store
        .get_session(session_id)
        .await?
        .unwrap_or_else(|| panic!("foreign-restore fixture must contain session {session_id}"));
    let files = target.serialize(&session, RestoreFidelity::Foreign)?;

    let target_dir = TempDir::new()?;
    write_restored(target_dir.path(), &files)?;
    let target_adapter: Box<dyn Adapter> = match target_root {
        TargetRoot::Claude => Box::new(ClaudeCodeAdapter::new(target_dir.path())),
        TargetRoot::Codex => Box::new(CodexCliAdapter::new(target_dir.path().join("sessions"))),
    };
    let verify_store_dir = TempDir::new()?;
    let verify_store = Store::open_local(verify_store_dir.path()).await?;
    ingest_adapter(
        &verify_store,
        target_adapter.as_ref(),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;

    // Re-parse gate: the foreign output must re-ingest as a real session, not
    // silently collapse to an empty file. Foreign restore drops only System
    // messages, so the round-tripped message count equals the origin's
    // non-System count.
    let origin_non_system = session
        .messages
        .iter()
        .filter(|m| !matches!(m.message, Message::System { .. }))
        .count();
    let restored_ids = verify_store.session_ids().await?;
    assert!(
        !restored_ids.is_empty(),
        "foreign output must re-ingest as at least one session ({snapshot_name})",
    );
    let mut restored_messages = 0usize;
    for id in &restored_ids {
        if let Some(restored) = verify_store.get_session(id).await? {
            restored_messages += restored.messages.len();
        }
    }
    assert_eq!(
        restored_messages, origin_non_system,
        "foreign restore must carry every non-System message ({snapshot_name})",
    );

    insta::assert_snapshot!(snapshot_name, render_files(&files));
    Ok(())
}

fn write_restored(root: &Path, files: &[pond::adapter::RestoredFile]) -> anyhow::Result<()> {
    for file in files {
        let path = root.join(&file.relative_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, &file.bytes)?;
    }
    Ok(())
}

fn render_files(files: &[pond::adapter::RestoredFile]) -> String {
    let mut out = String::new();
    for file in files {
        out.push_str("### ");
        // Normalize to `/` so the golden is one file across platforms; the
        // PathBuf itself keeps OS separators for the on-disk write above.
        out.push_str(&file.relative_path.display().to_string().replace('\\', "/"));
        out.push('\n');
        out.push_str(std::str::from_utf8(&file.bytes).unwrap_or("<non-utf8>"));
        if !out.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}
