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
        Adapter, AdapterFactory, ClaudeCodeAdapter, ClaudeCodeFactory, CodexCliAdapter,
        CodexCliFactory, NoopOracle, RestoreFidelity, SkipOracle,
    },
    handlers::ingest_adapter,
    sessions::{RowmapOracle, Store},
    substrate::Predicate,
    wire::Message,
};
use tempfile::TempDir;

mod claude_ai_export;
mod claude_code;
mod hermes;
mod nanoclaw;
mod oh_my_pi;
mod openclaw;
mod opencode;

/// How an adapter's fixture proves the round-trip half of spec.md 6.8. The
/// adapter declares its mode; the harness executes it uniformly - capability
/// declarations, never per-adapter branching.
pub(crate) enum RoundTrip {
    /// `serialize(Native)` output, re-opened through the factory's own config
    /// face, re-ingests to canonically equal sessions.
    Reingest,
    /// Native restore targets an external import tool, so its output cannot
    /// re-ingest here; deep value-equality against the source lives in the
    /// named adapter-specific test. The harness still asserts the restore face
    /// serves full-fidelity native output for every fixture session.
    ExternalImport { verified_by: &'static str },
    /// `restore_unsupported`: the declared refusal (with a reason naming the
    /// caller's alternative) IS the conformance statement.
    IngestOnly,
}

/// Shared conformance harness: the checks every adapter suite runs over its
/// committed fixture (spec.md 6.8), driven through `AdapterFactory::open` so
/// each test exercises the same config face `pond sync` uses. Adapter-specific
/// assertions (taxonomy, lineage, project fallbacks) stay in the per-adapter
/// files.
pub(crate) struct Conformance<'a> {
    pub(crate) factory: &'a dyn AdapterFactory,
    pub(crate) fixture_root: &'a Path,
    /// Sessions the store holds after a full-fixture ingest: importable
    /// sessions, not source files - empty sources don't count.
    pub(crate) expected_sessions: usize,
    pub(crate) round_trip: RoundTrip,
    /// The adapter's config face: a source root in, the blob
    /// `AdapterFactory::open` takes out. A function rather than a value
    /// because `Reingest` re-opens the adapter at a fresh restore root.
    pub(crate) config: fn(&Path) -> serde_json::Value,
}

/// The config face every path-backed adapter shares.
pub(crate) fn path_config(root: &Path) -> serde_json::Value {
    serde_json::json!({ "path": root })
}

/// A fresh local store holding one full ingest of `adapter` (no freshness
/// oracle, so every source is read). The `TempDir` is returned alongside
/// because dropping it would pull the directory out from under the store.
pub(crate) async fn ingest_into_temp_store(
    adapter: &dyn Adapter,
) -> anyhow::Result<(Store, TempDir)> {
    let store_dir = TempDir::new()?;
    let store = Store::open_local(store_dir.path()).await?;
    ingest_adapter(&store, adapter, &NoopOracle, |_| {}).await?;
    Ok((store, store_dir))
}

impl Conformance<'_> {
    fn open_at(&self, root: &Path) -> anyhow::Result<Box<dyn Adapter>> {
        self.factory
            .open((self.config)(root))
            .map_err(|error| anyhow::anyhow!("factory refused its own config face: {error}"))
    }

    async fn ingest_fixture(&self) -> anyhow::Result<(Store, TempDir)> {
        ingest_into_temp_store(self.open_at(self.fixture_root)?.as_ref()).await
    }

    /// Full-corpus ingest through the Store: expected session count, every
    /// session readable under the adapter's brand (or a `brand/kind` subpath)
    /// with at least one message, and the brand's scope searchable - the proof
    /// the whole pipeline ran, index fold included.
    pub(crate) async fn assert_ingest_counts_and_searchable(&self) -> anyhow::Result<()> {
        let (store, _guard) = self.ingest_fixture().await?;
        let brand = self.factory.name();

        let ids = store.session_ids().await?;
        anyhow::ensure!(
            ids.len() == self.expected_sessions,
            "{brand}: expected {} ingested sessions, got {}",
            self.expected_sessions,
            ids.len(),
        );
        let kind_prefix = format!("{brand}/");
        for id in &ids {
            let session = store
                .get_session(id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("{brand}: session {id} does not read back"))?;
            anyhow::ensure!(
                !session.messages.is_empty(),
                "{brand}: session {id} ingested without messages",
            );
            let agent = &session.session.source_agent;
            anyhow::ensure!(
                agent == brand || agent.starts_with(&kind_prefix),
                "{brand}: session {id} carries foreign brand {agent}",
            );
        }

        // Exact-or-subpath, the same scope shape the handlers use, so an
        // adapter whose fixture is entirely `brand/kind` sessions still counts.
        let searchable = store
            .searchable_in_scope(&Predicate::Regex("source_agent", format!("^{brand}(/|$)")))
            .await?;
        anyhow::ensure!(
            searchable > 0,
            "{brand}: no searchable rows in the brand scope after ingest",
        );
        Ok(())
    }

    /// Re-sync of the unchanged fixture is additive and skips fresh through
    /// the store's rowmap oracle: zero sessions and rows written the second
    /// time, with the skip visibly counted. A regression here is silent in
    /// production - it looks like a working sync that re-reads the whole
    /// corpus on every run.
    pub(crate) async fn assert_resync_is_noop(&self) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = self.open_at(self.fixture_root)?;
        let brand = self.factory.name();

        let first = ingest_adapter(&store, adapter.as_ref(), &NoopOracle, |_| {}).await?;
        anyhow::ensure!(
            first.sessions_inserted > 0,
            "{brand}: first sync ingested nothing",
        );

        store.ensure_rowmap(&temp.path().join("cache")).await?;
        let oracle = RowmapOracle(store.rowmap_snapshot());
        anyhow::ensure!(
            !oracle.is_empty(),
            "{brand}: resident rowmap empty after first sync",
        );

        let second = ingest_adapter(&store, adapter.as_ref(), &oracle, |_| {}).await?;
        anyhow::ensure!(
            second.sessions_inserted == 0,
            "{brand}: unchanged re-sync re-inserted {} sessions",
            second.sessions_inserted,
        );
        anyhow::ensure!(
            second.inserted == 0,
            "{brand}: unchanged re-sync wrote {} rows",
            second.inserted,
        );
        anyhow::ensure!(
            second.skipped_fresh > 0,
            "{brand}: nothing skipped fresh - the freshness gate never fired: {second:?}",
        );
        anyhow::ensure!(
            store.session_ids().await?.len() == self.expected_sessions,
            "{brand}: re-sync changed the stored session count",
        );
        Ok(())
    }

    /// The round-trip half of spec.md 6.8, in the mode the adapter declared.
    pub(crate) async fn assert_round_trip(&self) -> anyhow::Result<()> {
        let brand = self.factory.name();
        match self.round_trip {
            RoundTrip::IngestOnly => {
                let reason = self.factory.restore_unsupported();
                anyhow::ensure!(
                    reason.is_some_and(|reason| !reason.is_empty()),
                    "{brand}: declared IngestOnly but restore_unsupported gives no reason",
                );
                Ok(())
            }
            RoundTrip::ExternalImport { verified_by } => {
                anyhow::ensure!(
                    self.factory.restore_unsupported().is_none(),
                    "{brand}: declared a restore face but restore_unsupported refuses",
                );
                let (store, _guard) = self.ingest_fixture().await?;
                for id in store.session_ids().await? {
                    let session = store
                        .get_session(&id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("{brand}: session {id} unreadable"))?;
                    let files = self.factory.serialize(&session, RestoreFidelity::Native)?;
                    anyhow::ensure!(
                        !files.is_empty(),
                        "{brand}: native restore of {id} emitted nothing",
                    );
                    for file in &files {
                        anyhow::ensure!(
                            file.actual_fidelity == RestoreFidelity::Native,
                            "{brand}: native restore of {id} downgraded to foreign \
                             (value-equality vs the source is owned by {verified_by})",
                        );
                    }
                }
                Ok(())
            }
            RoundTrip::Reingest => {
                anyhow::ensure!(
                    self.factory.restore_unsupported().is_none(),
                    "{brand}: declared a restore face but restore_unsupported refuses",
                );
                let (store, _guard) = self.ingest_fixture().await?;
                let reingest_store_dir = TempDir::new()?;
                let reingest_store = Store::open_local(reingest_store_dir.path()).await?;
                for id in store.session_ids().await? {
                    let session = store
                        .get_session(&id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("{brand}: session {id} unreadable"))?;
                    let files = self.factory.serialize(&session, RestoreFidelity::Native)?;
                    anyhow::ensure!(
                        !files.is_empty(),
                        "{brand}: native restore of {id} emitted nothing",
                    );
                    for file in &files {
                        anyhow::ensure!(
                            file.actual_fidelity == RestoreFidelity::Native,
                            "{brand}: native restore of {id} downgraded to foreign",
                        );
                    }
                    // Per-session restore root: adapters whose whole corpus is
                    // one file (an export archive) emit the same relative path
                    // for every session.
                    let restore_root = TempDir::new()?;
                    write_restored(restore_root.path(), &files)?;
                    let reopened = self.open_at(restore_root.path())?;
                    ingest_adapter(&reingest_store, reopened.as_ref(), &NoopOracle, |_| {}).await?;
                    let restored = reingest_store.get_session(&id).await?.ok_or_else(|| {
                        anyhow::anyhow!("{brand}: restored output of {id} did not re-ingest")
                    })?;
                    anyhow::ensure!(
                        restored == session,
                        "{brand}: session {id} is not canonically equal after \
                         serialize(Native) -> re-ingest",
                    );
                }
                Ok(())
            }
        }
    }
}

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
    target: &dyn AdapterFactory,
    snapshot_name: &str,
    target_root: TargetRoot,
    session_id: &str,
) -> anyhow::Result<()> {
    let (store, _store_dir) = ingest_into_temp_store(&origin).await?;
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
    let (verify_store, _verify_store_dir) = ingest_into_temp_store(target_adapter.as_ref()).await?;

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

/// Write restored files under `root` through the production path gate
/// (`restore_destinations`), so a `relative_path` the real writer would refuse
/// fails the test instead of passing under a laxer join.
fn write_restored(root: &Path, files: &[pond::adapter::RestoredFile]) -> anyhow::Result<()> {
    let destinations = pond::adapter::restore_destinations(root, files)?;
    for (path, file) in destinations.into_iter().zip(files) {
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
