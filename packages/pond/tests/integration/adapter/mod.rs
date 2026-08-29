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
        CodexCliFactory, NoopOracle, RestoreFidelity, SkipOracle, validate_path_id,
    },
    config::SearchConfig,
    embed::LazyEmbedder,
    handlers::{SyncEvent, SyncStatus, ingest_adapter, pond_search},
    sessions::{IngestSummary, RowmapOracle, SessionWithMessages, Store},
    substrate::{MaintenancePolicy, Predicate},
    wire::{
        Message, PartKind, Provenance, Role, SearchEnvelope, SearchFilters, SearchModeWire,
        SearchRequest, SortBy,
    },
};
use tempfile::TempDir;

mod claude_ai_export;
mod claude_code;
mod grok_build;
mod goose;
mod hermes;
mod letta_code;
mod nanoclaw;
mod oh_my_pi;
mod openclaw;
mod opencode;
mod pi_coding_agent;

/// How an adapter's fixture proves the round-trip half of spec.md 6.8. The
/// adapter declares its mode; the harness executes it uniformly - capability
/// declarations, never per-adapter branching.
///
/// `Reingest` proves the canonical fixed point, parse(serialize(canonical)) ==
/// canonical, which coincides with 6.8's value-equality against the fixture
/// only because every adapter embeds the bounded whole source record in
/// `options.source.raw_record`. The literal byte-level 6.8 check stays a unit
/// test in the adapter file (`test_support::assert_native_restore`, the pi
/// codec replay); do not drop it on the strength of this mode.
pub(crate) enum RoundTrip {
    /// `serialize(Native)` output, re-opened through the factory's own config
    /// face, re-ingests to canonically equal sessions. `downgraded` names the
    /// fixture sessions the adapter serves as `Foreign` on a `Native` request:
    /// a native origin its client cannot load back is reconstructed, not
    /// replayed (spec.md#adapter-native-restore-lossless). Those must still
    /// re-ingest as a session, not equal; naming them makes a change in the
    /// adapter's downgrade policy a visible test change, and an adapter that
    /// downgrades everything cannot pass by declaring so - at least one
    /// session must replay natively.
    Reingest { downgraded: &'static [&'static str] },
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
    /// Fixture sessions the adapter re-reads on an unchanged re-sync because
    /// the source gives them no usable watermark (a trailing mutation with no
    /// timestamp, say). Re-reading is the safe direction, so it is allowed,
    /// but it is a per-sync cost the adapter's freshness row must explain;
    /// naming the sessions keeps every other one held to "skipped fresh".
    pub(crate) resync_rereads: &'static [&'static str],
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

/// The first alphabetic word of at least five letters in a user or assistant
/// Text part of the session - a token BM25 indexes and a default search can
/// be asked for.
fn conversational_word(session: &SessionWithMessages) -> Option<String> {
    session
        .messages
        .iter()
        .filter(|message| matches!(message.message.role(), Role::User | Role::Assistant))
        .flat_map(|message| message.parts.iter())
        .filter(|part| part.provenance == Provenance::Conversational)
        .filter_map(|part| match &part.kind {
            PartKind::Text {
                text: Some(text), ..
            } => Some(text.as_ref().as_str()),
            _ => None,
        })
        .flat_map(|text| text.split(|c: char| !c.is_ascii_alphabetic()))
        .find(|word| word.len() >= 5)
        .map(str::to_ascii_lowercase)
}

/// A fixture ingest is clean or the suite is not measuring the adapter: a
/// dropped event, a rejected session, or a storage error still leaves the
/// session count and the searchable scope intact.
fn ensure_clean_ingest(brand: &str, summary: &IngestSummary) -> anyhow::Result<()> {
    anyhow::ensure!(
        summary.dropped_events == 0 && summary.dropped_sessions == 0 && summary.storage_errors == 0,
        "{brand}: fixture ingest was not clean: {summary:?}",
    );
    Ok(())
}

impl Conformance<'_> {
    fn open_at(&self, root: &Path) -> anyhow::Result<Box<dyn Adapter>> {
        self.factory
            .open((self.config)(root))
            .map_err(|error| anyhow::anyhow!("factory refused its own config face: {error}"))
    }

    async fn ingest_fixture(&self) -> anyhow::Result<(Store, TempDir)> {
        let adapter = self.open_at(self.fixture_root)?;
        let store_dir = TempDir::new()?;
        let store = Store::open_local(store_dir.path()).await?;
        let summary = ingest_adapter(&store, adapter.as_ref(), &NoopOracle, |_| {}).await?;
        ensure_clean_ingest(self.factory.name(), &summary)?;
        Ok((store, store_dir))
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
        let mut probe_word = None;
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
            if session.session.parent_session_id.is_none() {
                // The search layer reads `/` in a session id as the claude-code
                // subagent marker (`handlers::retain_non_subagents`) and drops
                // the hit before ranking, so a root session named that way
                // ingests and reads back yet never surfaces in a default
                // search. Caught live on letta-code's `<agent>/<conversation>`.
                anyhow::ensure!(
                    !id.contains('/'),
                    "{brand}: root session id {id} contains '/', which default search treats as a subagent",
                );
                // And the id must be embeddable in a restore filename: foreign
                // restore targets (claude-code, codex-cli, ...) name their
                // output file after the session id, so an id that fails the
                // path rules (`:` is an NTFS alternate data stream, ...) makes
                // `pond resume --to <them>` a runtime error. Caught live on
                // letta-code's second try, `<agent>:<conversation>`.
                if let Err(error) = validate_path_id("conformance", "session id", id, id.clone()) {
                    anyhow::bail!("{brand}: root session id cannot name a restore file: {error}");
                }
                if probe_word.is_none() {
                    probe_word = conversational_word(&session);
                }
            }
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
        // The session-level brand check above cannot see message rows; the
        // whole store must sit inside the brand scope.
        let all = store.searchable_in_scope(&Predicate::And(vec![])).await?;
        anyhow::ensure!(
            searchable == all,
            "{brand}: {} searchable rows carry a foreign brand",
            all - searchable,
        );

        // The handler path, not just the store: one default-mode search (no
        // source_agent scope - scoping disables the subagent exclusion and would
        // hide exactly the failure above) for a word taken from a root session's
        // own conversation must hit. This is what `pond search` runs.
        let probe_word = probe_word.ok_or_else(|| {
            anyhow::anyhow!("{brand}: no root session carries a conversational word to search for")
        })?;
        store
            .optimize_indices(None, &MaintenancePolicy::always_compact())
            .await?
            .into_result()?;
        let request = SearchRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            query: probe_word.clone(),
            mode: SearchModeWire::Fts,
            sort_by: SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 5,
        };
        let response = match pond_search(
            &store,
            &LazyEmbedder::candle(),
            request,
            &SearchConfig::default(),
        )
        .await
        {
            SearchEnvelope::Success(response) => response,
            SearchEnvelope::Error(error) => {
                anyhow::bail!("{brand}: default search for {probe_word:?} failed: {error:?}")
            }
        };
        anyhow::ensure!(
            response.matched_total > 0,
            "{brand}: default search for {probe_word:?} found nothing across {} searchable rows",
            response.searchable_in_scope,
        );
        Ok(())
    }

    /// Re-sync of the unchanged fixture is additive and skips fresh through
    /// the store's rowmap oracle: zero sessions and rows written the second
    /// time, the skip visibly counted, and no session re-read beyond the ones
    /// the adapter declares. A regression here is silent in production - it
    /// looks like a working sync that re-reads the whole corpus on every run.
    pub(crate) async fn assert_resync_is_noop(&self) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path().join("store")).await?;
        let adapter = self.open_at(self.fixture_root)?;
        let brand = self.factory.name();

        let first = ingest_adapter(&store, adapter.as_ref(), &NoopOracle, |_| {}).await?;
        ensure_clean_ingest(brand, &first)?;
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

        let mut rereads = Vec::new();
        let second = ingest_adapter(&store, adapter.as_ref(), &oracle, |event| {
            if let SyncEvent::SessionDone(outcome) = event
                && matches!(outcome.status, SyncStatus::Ok | SyncStatus::Partial { .. })
                && let Some(id) = outcome.session_id
            {
                rereads.push(id);
            }
        })
        .await?;
        ensure_clean_ingest(brand, &second)?;
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
        // A re-read session writes nothing (its rows merge-match), so
        // `inserted == 0` alone cannot tell "every session skipped" from
        // "one skipped, the rest re-read".
        let mut declared: Vec<&str> = self.resync_rereads.to_vec();
        declared.sort_unstable();
        rereads.sort_unstable();
        anyhow::ensure!(
            rereads == declared,
            "{brand}: unchanged re-sync re-read sessions {rereads:?}, declared {declared:?} \
             ({} rows matched): {second:?}",
            second.matched,
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
                // The capability query and `serialize` are two surfaces; a
                // refusal on one with output on the other is a drift bug.
                let (store, _guard) = self.ingest_fixture().await?;
                let id = store
                    .session_ids()
                    .await?
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("{brand}: fixture ingested no sessions"))?;
                let session = store
                    .get_session(&id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("{brand}: session {id} unreadable"))?;
                anyhow::ensure!(
                    self.factory
                        .serialize(&session, RestoreFidelity::Native)
                        .is_err(),
                    "{brand}: declared IngestOnly but serialize(Native) emitted files",
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
            RoundTrip::Reingest { downgraded } => {
                anyhow::ensure!(
                    self.factory.restore_unsupported().is_none(),
                    "{brand}: declared a restore face but restore_unsupported refuses",
                );
                let (store, _guard) = self.ingest_fixture().await?;
                let reingest_store_dir = TempDir::new()?;
                let reingest_store = Store::open_local(reingest_store_dir.path()).await?;
                let ids = store.session_ids().await?;
                let mut downgrades = Vec::new();
                for id in &ids {
                    let session = store
                        .get_session(id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("{brand}: session {id} unreadable"))?;
                    let files = self.factory.serialize(&session, RestoreFidelity::Native)?;
                    anyhow::ensure!(
                        !files.is_empty(),
                        "{brand}: native restore of {id} emitted nothing",
                    );
                    let native = files
                        .iter()
                        .all(|file| file.actual_fidelity == RestoreFidelity::Native);
                    if !native {
                        downgrades.push(id.as_str());
                    }
                    // Per-session restore root: adapters whose whole corpus is
                    // one file (an export archive) emit the same relative path
                    // for every session.
                    let restore_root = TempDir::new()?;
                    write_restored(restore_root.path(), files)?;
                    let reopened = self.open_at(restore_root.path())?;
                    let summary =
                        ingest_adapter(&reingest_store, reopened.as_ref(), &NoopOracle, |_| {})
                            .await?;
                    ensure_clean_ingest(brand, &summary)?;
                    let restored = reingest_store.get_session(id).await?.ok_or_else(|| {
                        anyhow::anyhow!("{brand}: restored output of {id} did not re-ingest")
                    })?;
                    if native && restored != session {
                        anyhow::bail!(
                            "{brand}: session {id} is not canonically equal after \
                             serialize(Native) -> re-ingest: {}",
                            first_difference(&session, &restored),
                        );
                    }
                }
                let mut declared: Vec<&str> = downgraded.to_vec();
                declared.sort_unstable();
                downgrades.sort_unstable();
                anyhow::ensure!(
                    downgrades == declared,
                    "{brand}: sessions downgraded to foreign on native restore {downgrades:?}, \
                     declared {declared:?}",
                );
                anyhow::ensure!(
                    downgrades.len() < ids.len(),
                    "{brand}: every session downgraded - declare IngestOnly or capture \
                     raw_record so native restore can replay",
                );
                let reingested = reingest_store.session_ids().await?;
                anyhow::ensure!(
                    reingested.len() == ids.len(),
                    "{brand}: restored output re-ingested as {} sessions, the fixture holds {}",
                    reingested.len(),
                    ids.len(),
                );
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
    write_restored(target_dir.path(), files.clone())?;
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

/// Write restored files under `root` through the production writer, so a
/// `relative_path` it would refuse (or two files colliding on one path) fails
/// the test instead of passing under a laxer write.
fn write_restored(root: &Path, files: Vec<pond::adapter::RestoredFile>) -> anyhow::Result<()> {
    pond::adapter::write_restored_files(root, files)?;
    Ok(())
}

/// Where two canonical sessions first diverge, for an equality failure that
/// points at a message instead of dumping two transcripts.
fn first_difference(expected: &SessionWithMessages, actual: &SessionWithMessages) -> String {
    if expected.session != actual.session {
        return format!(
            "session header differs\n  expected: {:?}\n  actual:   {:?}",
            expected.session, actual.session
        );
    }
    if expected.messages.len() != actual.messages.len() {
        return format!(
            "{} messages expected, {} restored",
            expected.messages.len(),
            actual.messages.len()
        );
    }
    expected
        .messages
        .iter()
        .zip(&actual.messages)
        .position(|(expected, actual)| expected != actual)
        .map_or_else(
            || "no field differs (equality and diff disagree)".to_owned(),
            |index| {
                format!(
                    "message {index} differs\n  expected: {:?}\n  actual:   {:?}",
                    expected.messages[index], actual.messages[index]
                )
            },
        )
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
