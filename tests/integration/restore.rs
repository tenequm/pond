#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use pond::{
    adapter::{
        Adapter, AdapterFactory, ClaudeCodeAdapter, ClaudeCodeFactory, CodexCliAdapter,
        CodexCliFactory, RestoreFidelity,
    },
    handlers::ingest_adapter,
    sessions::Store,
    wire::Message,
};
use tempfile::TempDir;

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

#[tokio::test(flavor = "multi_thread")]
async fn restore_writes_parent_and_direct_subagent_child() -> anyhow::Result<()> {
    let source = TempDir::new()?;
    write_claude_parent_child(source.path())?;
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    let adapter = ClaudeCodeAdapter::new(source.path());
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let parent = store
        .get_session("parent-session")
        .await?
        .expect("parent ingested");
    let child = store
        .get_session("parent-session/agent-abc123")
        .await?
        .expect("child ingested");
    let out = TempDir::new()?;
    let mut files = Vec::new();
    for session in [&parent, &child] {
        files.extend(ClaudeCodeFactory.serialize(session, RestoreFidelity::Native)?);
    }
    write_restored(out.path(), &files)?;
    assert!(
        files
            .iter()
            .any(|f| f.relative_path.ends_with("parent-session.jsonl"))
    );
    assert!(
        files
            .iter()
            .any(|f| f.relative_path.ends_with("subagents/agent-abc123.jsonl"))
    );
    assert!(files.iter().any(|f| {
        f.relative_path
            .ends_with("subagents/agent-abc123.meta.json")
    }));
    Ok(())
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
        out.push_str(&file.relative_path.display().to_string());
        out.push('\n');
        out.push_str(std::str::from_utf8(&file.bytes).unwrap_or("<non-utf8>"));
        if !out.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

fn write_claude_parent_child(root: &Path) -> anyhow::Result<()> {
    let project = root.join("-tmp-restore");
    let subagents = project.join("parent-session").join("subagents");
    std::fs::create_dir_all(&subagents)?;
    std::fs::write(
        project.join("parent-session.jsonl"),
        r#"{"type":"user","uuid":"parent-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:00.000Z","cwd":"/tmp/restore","message":{"role":"user","content":"hi"}}"#,
    )?;
    std::fs::write(
        subagents.join("agent-abc123.jsonl"),
        r#"{"type":"assistant","uuid":"child-message","sessionId":"parent-session","timestamp":"2026-01-01T00:00:01.000Z","cwd":"/tmp/restore","message":{"role":"assistant","content":[{"type":"text","text":"child"}]}}"#,
    )?;
    std::fs::write(
        subagents.join("agent-abc123.meta.json"),
        r#"{"agentType":"general-purpose","description":"fixture child"}"#,
    )?;
    Ok(())
}
