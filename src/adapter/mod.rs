//! Source-adapter seam.
//!
//! Pond ingests sessions from many runtimes. The seam splits in two:
//!
//! - [`AdapterFactory`] is the stateless face every format publishes once,
//!   collected by [`registry`]. It knows how to construct configured adapters
//!   from an opaque JSON config blob ([`AdapterFactory::open`]) and how to
//!   probe the user's environment for a default config
//!   ([`AdapterFactory::probe_default`]).
//! - [`Adapter`] is the live, configured instance. Its only job is
//!   [`Adapter::events`]: stream canonical [`IngestEvent`]s in append-only
//!   order per session. The "source" is opaque to the seam - a directory
//!   tree, an HTTP endpoint, a database, an archive file.
//!
//! Concrete implementations live in `adapter/<format>.rs` and are tied
//! together by [`registry`]. A new adapter is one file plus one line in the
//! registry; no central dispatch table to edit.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio_stream::{Stream, StreamExt};

use crate::{
    sessions::{IngestEvent, MessageWithParts, SessionWithMessages},
    wire::ProviderOptions,
};

mod claude_ai_export;
mod claude_code;
mod claude_desktop_app;
mod codex_cli;
mod discovery;
pub mod extract;
mod jsonl;
mod opencode;
mod pi_coding_agent;

pub use claude_ai_export::{ClaudeAiExportAdapter, ClaudeAiExportFactory};
pub use claude_code::{ClaudeCodeAdapter, ClaudeCodeFactory};
pub use claude_desktop_app::{ClaudeDesktopAppAdapter, ClaudeDesktopAppFactory};
pub use codex_cli::{CodexCliAdapter, CodexCliFactory};
pub use discovery::{
    Candidate, apply_to_doc, discover, persist_accept, probe_unconfigured, prompt_and_persist,
    set_adapter_enabled,
};
pub use extract::{
    Extracted, Source, extract_bool, extract_compact_repr, extract_raw_record, extract_self_str,
    extract_str, extract_value,
};
pub use opencode::{OpencodeAdapter, OpencodeFactory};
pub use pi_coding_agent::{PiCodingAgentAdapter, PiCodingAgentFactory};

/// Stateless face of an adapter type: how the registry knows about it without
/// instantiating it. One implementation per known format, registered in
/// [`registry`].
pub trait AdapterFactory: Send + Sync {
    /// Stable short name. Used as the `[adapters.<name>]` config key, the
    /// `pond sync <name>` positional arg, and the `Session.source_agent`
    /// value emitted by the corresponding adapter.
    fn name(&self) -> &'static str;

    /// Open a configured adapter from a JSON-shaped config blob. The shape is
    /// owned by each factory: filesystem adapters expect `{ "path": "..." }`,
    /// API-backed adapters expect `{ "endpoint": "...", "auth_token": "..." }`,
    /// etc. The seam doesn't know or care. A factory rejects a bad blob with
    /// [`AdapterErrorKind::Config`].
    fn open(&self, config: Value) -> Result<Box<dyn Adapter>, AdapterError>;

    /// Probe the user's environment for a default config. Returns the JSON
    /// blob that would go into `[adapters.<name>]` if the picker writes it
    /// back. Filesystem adapters check their canonical install path under
    /// `env.home`; adapters with no auto-discovery rule (e.g. API adapters
    /// that need explicit creds) return `None`.
    fn probe_default(&self, env: &Env) -> Option<Value>;

    /// Restore one canonical session into this adapter's native file layout.
    fn serialize(
        &self,
        session: &SessionWithMessages,
        fidelity: RestoreFidelity,
    ) -> Result<Vec<RestoredFile>, AdapterError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreFidelity {
    Native,
    Foreign,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredFile {
    pub relative_path: PathBuf,
    pub bytes: Vec<u8>,
    /// Fidelity actually served when this file was produced. Equal to the
    /// requested fidelity unless the adapter had to downgrade (e.g. caller
    /// asked `Native` but the session lacks a stored `raw_record`, so the
    /// adapter served `Foreign`). spec.md#adapter-native-restore-lossless:
    /// native may be impossible on older logs; the signal lets the CLI warn
    /// rather than silently degrade.
    pub actual_fidelity: RestoreFidelity,
}

impl RestoredFile {
    pub(crate) fn new(
        relative_path: impl Into<PathBuf>,
        bytes: Vec<u8>,
        actual_fidelity: RestoreFidelity,
    ) -> Self {
        Self {
            relative_path: relative_path.into(),
            bytes,
            actual_fidelity,
        }
    }
}

/// Live, configured adapter instance. Holds whatever handle the source needs
/// (an open directory root, an HTTP client + auth, a database connection)
/// for the lifetime of its event stream.
pub trait Adapter: Send + Sync {
    /// Stream every canonical event for every session this adapter knows
    /// about, in append-only order per session. The stream borrows `self`
    /// so callers can pass `&adapter` or hold a `Box<dyn Adapter>` and
    /// invoke this through `as_ref()`.
    fn events(&self) -> EventStream<'_> {
        let stream = self.events_with(&NoopOracle);
        Box::pin(stream.filter_map(|res| match res {
            Ok(AdapterYield::Event(event)) => Some(Ok(event)),
            Ok(AdapterYield::Skipped { .. } | AdapterYield::SkippedBatch { .. }) => None,
            Err(error) => Some(Err(error)),
        }))
    }

    /// Count how many sessions [`Self::events`] will produce, used by the
    /// CLI bar to set its length up front. A filesystem adapter walks its
    /// root and counts `.jsonl` files; an API adapter calls its list
    /// endpoint. Cheap and best-effort: errors here only mean we run with
    /// an unknown total (the bar still ticks per session), so callers
    /// fall back to a rolling counter rather than failing the sync.
    fn discover(&self) -> DiscoverFuture<'_>;

    /// Stream events with a [`SkipOracle`] the adapter MAY consult to
    /// short-circuit per-session re-decoding (spec.md#adapter-integrity-event-ordering). Default impl
    /// ignores the oracle.
    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a>;
}

/// Per-session watermark lookup: when did pond last write this session?
/// Backed by Lance's `_row_last_updated_at_version` joined to the manifest
/// commit timestamp (spec.md#adapters). Adapter compares this to the source
/// file's mtime to decide whether to re-decode.
pub trait SkipOracle: Send + Sync {
    fn last_ingested_at(&self, session_id: &str) -> Option<DateTime<Utc>>;

    /// Fast-path hint: the oracle has no watermarks at all (first ingest or
    /// `NoopOracle`). Lets adapters skip the per-file work needed to ask
    /// `last_ingested_at` - typically the JSONL header peek + driver parse.
    /// Defaults to `false` so existing oracles stay correct without changes.
    fn is_empty(&self) -> bool {
        false
    }
}

/// `SkipOracle` that always returns `None`. Used by tests and benches that
/// don't want skip behavior interfering with their assertions.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopOracle;

impl SkipOracle for NoopOracle {
    fn last_ingested_at(&self, _session_id: &str) -> Option<DateTime<Utc>> {
        None
    }

    fn is_empty(&self) -> bool {
        true
    }
}

/// `pond sync` passes the `Store::session_last_ingested_at` map straight in as
/// the oracle - no wrapper struct, no second representation. The blanket
/// `is_empty` short-circuits the per-file mtime stat on a fresh corpus.
impl SkipOracle for std::collections::HashMap<String, DateTime<Utc>> {
    fn last_ingested_at(&self, session_id: &str) -> Option<DateTime<Utc>> {
        self.get(session_id).copied()
    }
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }
}

#[derive(Debug, Clone)]
pub enum AdapterYield {
    Event(IngestEvent),
    Skipped {
        /// `None` for files that never yield a session id (empty `.jsonl`).
        session_id: Option<String>,
        project: Option<String>,
        reason: SkipReason,
    },
    /// Aggregate skip; one yield per N files (typically `Fresh` recurring
    /// sync) instead of N. Avoids O(N) per-session orchestrator overhead.
    SkippedBatch {
        reason: SkipReason,
        count: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipReason {
    Fresh,
    /// File produced no importable session (empty `.jsonl`, sidecar-only rows,
    /// or an unextractable header). Benign: counted, never an error or a
    /// per-event drop. The underlying cause is logged at `-vv` (debug) verbosity.
    Empty,
    /// File is structurally a known sidecar whose specific shape this adapter
    /// version can't ingest. Surfaced as a visible, counted failure - NOT a
    /// benign skip - so the gap is actionable and the file is never folded into
    /// another session under a borrowed id. The payload is the user-facing
    /// reason naming the file and the fix.
    Unsupported(String),
}

pub type AdapterYieldStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<AdapterYield, AdapterError>> + Send + 'a>>;

/// Boxed future returning the number of sessions an adapter will emit. The
/// shape mirrors [`EventStream`] - one alias per async trait method so the
/// trait stays `dyn`-compatible without per-adapter associated types.
pub type DiscoverFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize, AdapterError>> + Send + 'a>>;

/// Environment slice handed to [`AdapterFactory::probe_default`]. Kept
/// deliberately small - just `home`, because env-var lookups for API creds
/// are unreliable and most adapters with API backends should require
/// explicit config rather than opportunistic env reads.
pub struct Env {
    pub home: PathBuf,
}

impl Env {
    /// Read `home` from the `HOME` env var. Returns `None` when `HOME` is
    /// unset (CI, post-install hooks, sandboxed runs).
    pub fn from_env() -> Option<Self> {
        let home = std::env::var_os("HOME")?;
        Some(Self {
            home: PathBuf::from(home),
        })
    }

    /// Construct an `Env` with an explicit home. Tests use this to inject a
    /// `TempDir`-backed home without touching the process env.
    pub fn with_home(home: impl Into<PathBuf>) -> Self {
        Self { home: home.into() }
    }
}

/// Boxed, `Send`-only stream of [`IngestEvent`]s with one shared error type.
/// The lifetime parameter lets future adapters borrow from their config; for
/// `self: Box<Self>` impls the lifetime collapses to `'static`.
pub type EventStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<IngestEvent, AdapterError>> + Send + 'a>>;

/// One error type for every adapter. Each call site tags the error with the
/// adapter's name (so multi-adapter syncs can attribute failures) and a
/// `location` string the operator can act on (file path, URL, line number,
/// config key, ...). The `kind` carries the underlying class.
#[derive(Debug)]
pub struct AdapterError {
    pub adapter: &'static str,
    pub location: String,
    pub kind: AdapterErrorKind,
}

#[derive(Debug)]
pub enum AdapterErrorKind {
    /// Filesystem / network IO at `location`.
    Io(std::io::Error),
    /// JSON parse error at line `line` inside `location`.
    Parse {
        line: usize,
        source: serde_json::Error,
    },
    /// Format-specific shape error: missing required field, unknown role,
    /// unsupported record type. The `String` is operator-facing.
    Schema(String),
    /// `AdapterFactory::open` rejected its config blob.
    Config(String),
    /// HTTP / RPC / timeout error from an API-backed adapter.
    Transport(String),
    /// Auth failure from an API-backed adapter (bad token, expired creds).
    Auth(String),
}

impl AdapterError {
    pub fn io(adapter: &'static str, location: impl Into<String>, source: std::io::Error) -> Self {
        Self {
            adapter,
            location: location.into(),
            kind: AdapterErrorKind::Io(source),
        }
    }

    pub fn parse(
        adapter: &'static str,
        location: impl Into<String>,
        line: usize,
        source: serde_json::Error,
    ) -> Self {
        Self {
            adapter,
            location: location.into(),
            kind: AdapterErrorKind::Parse { line, source },
        }
    }

    pub fn schema(
        adapter: &'static str,
        location: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            adapter,
            location: location.into(),
            kind: AdapterErrorKind::Schema(message.into()),
        }
    }

    pub fn config(adapter: &'static str, message: impl Into<String>) -> Self {
        Self {
            adapter,
            location: "config".to_owned(),
            kind: AdapterErrorKind::Config(message.into()),
        }
    }
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            AdapterErrorKind::Io(source) => {
                write!(
                    formatter,
                    "{} io error at {}: {source}",
                    self.adapter, self.location
                )
            }
            AdapterErrorKind::Parse { line, source } => write!(
                formatter,
                "{} json parse error at {}:{line}: {source}",
                self.adapter, self.location,
            ),
            AdapterErrorKind::Schema(message) => {
                write!(
                    formatter,
                    "{} schema error at {}: {message}",
                    self.adapter, self.location
                )
            }
            AdapterErrorKind::Config(message) => {
                write!(formatter, "{} config error: {message}", self.adapter)
            }
            AdapterErrorKind::Transport(message) => write!(
                formatter,
                "{} transport error at {}: {message}",
                self.adapter, self.location,
            ),
            AdapterErrorKind::Auth(message) => {
                write!(formatter, "{} auth error: {message}", self.adapter)
            }
        }
    }
}

impl std::error::Error for AdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            AdapterErrorKind::Io(source) => Some(source),
            AdapterErrorKind::Parse { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// The static, ordered registry of every adapter pond knows. A new adapter
/// adds one `&Factory` here plus one file under `src/adapter/`. Order is the
/// order discovery presents to the operator.
pub fn registry() -> &'static [&'static dyn AdapterFactory] {
    &[
        &ClaudeCodeFactory,
        &ClaudeDesktopAppFactory,
        &ClaudeAiExportFactory,
        &CodexCliFactory,
        &OpencodeFactory,
        &PiCodingAgentFactory,
    ]
}

/// Look up a factory by name. Returns `None` for unknown names; callers
/// usually wrap that in a clear error using [`known_names`].
pub fn by_name(name: &str) -> Option<&'static dyn AdapterFactory> {
    registry().iter().copied().find(|f| f.name() == name)
}

/// The names of every registered adapter. Drives error messages
/// ("unknown adapter X; known: ...") and the discovery picker labels.
pub fn known_names() -> Vec<&'static str> {
    registry().iter().map(|f| f.name()).collect()
}

/// Probe every adapter for a default config under `env.home`. Returns
/// `(name, default_config)` pairs in registry order, skipping adapters whose
/// `probe_default` returned `None`. The picker shows these to the operator.
pub fn probe_all(env: &Env) -> Vec<(&'static str, Value)> {
    registry()
        .iter()
        .filter_map(|factory| factory.probe_default(env).map(|cfg| (factory.name(), cfg)))
        .collect()
}

/// Stable Part-row id: `"{message_id}:{ordinal:04}"`. Both JSONL adapters use
/// this shape so the cross-adapter id space stays predictable.
pub(crate) fn part_id(message_id: &str, ordinal: usize) -> String {
    format!("{message_id}:{ordinal:04}")
}

/// Compact (no-whitespace) JSON serialization used as a fallback Part body
/// when a row carries something we don't have a richer canonical shape for.
pub(crate) fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

pub(crate) fn jsonl_bytes(
    adapter: &'static str,
    records: &[Value],
) -> Result<Vec<u8>, AdapterError> {
    let mut bytes = Vec::new();
    for record in records {
        let line = serde_json::to_vec(record).map_err(|err| {
            AdapterError::schema(adapter, "serialize", format!("json encode failed: {err}"))
        })?;
        bytes.extend(line);
        bytes.push(b'\n');
    }
    Ok(bytes)
}

/// Shared `AdapterFactory::open` plumbing: parse the config blob's `path` and
/// expand a leading `~` against `$HOME` once, not per path adapter.
pub(crate) fn config_path(adapter: &'static str, config: Value) -> Result<PathBuf, AdapterError> {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Cfg {
        path: PathBuf,
    }
    let cfg: Cfg = serde_json::from_value(config)
        .map_err(|err| AdapterError::config(adapter, format!("bad config blob: {err}")))?;
    Ok(match std::env::var_os("HOME") {
        Some(home) => crate::config::expand_home_under(&cfg.path, Path::new(&home)),
        None => cfg.path,
    })
}

pub(crate) fn raw_record(options: &ProviderOptions) -> Option<Value> {
    options
        .get("source")
        .and_then(|source| source.get("raw_record"))
        .cloned()
}

/// Standard `options.source = {adapter, raw_record}` shape used by every
/// adapter that captures its source record for native restore. Centralized so
/// the writer side of the raw-record convention lives next to the reader
/// ([`raw_record`]); per-adapter side-fields (e.g. claude-code's `cwd`,
/// codex-cli's `git`) extend this map after construction.
pub(crate) fn source_options(adapter: &'static str, raw: &Value) -> ProviderOptions {
    let mut options = ProviderOptions::new();
    options.insert(
        "source".to_owned(),
        serde_json::json!({
            "adapter": adapter,
            "raw_record": extract_raw_record(raw),
        }),
    );
    options
}

/// `Part.ordinal` is stored as `i32`; ingest counts as `usize`. A session
/// could in principle exceed `i32::MAX` parts, in which case we clamp rather
/// than drop the record.
#[inline]
pub(crate) fn part_ordinal(ordinal: usize) -> i32 {
    i32::try_from(ordinal).unwrap_or(i32::MAX)
}

/// Reject `/`, `\`, `..`, and absolute paths in any segment that will become
/// part of a filesystem path during restore. Centralizing it here keeps every
/// adapter's restore-write path on the same allowlist; the writer
/// ([`write_restored_files`]) re-applies it as a defense-in-depth check on
/// every segment regardless of which adapter built the `RestoredFile`.
pub(crate) fn validate_path_id(
    adapter: &'static str,
    kind: &str,
    id: &str,
    location: impl Into<String>,
) -> Result<(), AdapterError> {
    if id.is_empty()
        || id.contains('/')
        || id.contains('\\')
        || id.contains("..")
        || std::path::Path::new(id).is_absolute()
    {
        return Err(AdapterError::schema(
            adapter,
            location,
            format!("{kind} contains a path separator or traversal marker: {id}"),
        ));
    }
    Ok(())
}

/// Atomically write a batch of `RestoredFile`s under `root`. Every path
/// segment is re-validated and the joined path is required to stay inside
/// `root` (spec.md#adapter-native-restore-lossless: restore writes are
/// adapter-supplied, but the gate lives at the writer so a single audit
/// covers every adapter today and tomorrow). On partial failure the
/// half-written tree is discarded before the error is returned.
///
/// Currently exercised only by adapter tests; the production restore CLI
/// will route through this same helper when it lands.
#[allow(dead_code)]
pub(crate) fn write_restored_files(
    root: &Path,
    files: Vec<RestoredFile>,
) -> Result<(), AdapterError> {
    // Stage under a sibling temp dir and atomically rename so a partial
    // failure cannot leave a half-populated restore in place.
    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    let stem = root
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("restore");
    let staging = parent.join(format!(".{stem}.tmp"));
    let io =
        |location: String, source: std::io::Error| AdapterError::io("restore", location, source);
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging).map_err(|e| io(staging.display().to_string(), e))?;

    let result = (|| -> Result<(), AdapterError> {
        for file in files {
            write_one_into_staging(&staging, &file)?;
        }
        Ok(())
    })();

    if let Err(error) = result {
        let _ = std::fs::remove_dir_all(&staging);
        return Err(error);
    }

    // Replace any existing restore root; the staging dir becomes the new root.
    let _ = std::fs::remove_dir_all(root);
    if let Some(parent) = root.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| io(parent.display().to_string(), e))?;
    }
    std::fs::rename(&staging, root).map_err(|e| {
        let _ = std::fs::remove_dir_all(&staging);
        io(root.display().to_string(), e)
    })?;
    Ok(())
}

#[allow(dead_code)]
fn write_one_into_staging(staging: &Path, file: &RestoredFile) -> Result<(), AdapterError> {
    // Validate every segment of the supplied relative path.
    for component in file.relative_path.components() {
        use std::path::Component;
        let segment = match component {
            Component::Normal(s) => s,
            Component::CurDir => continue,
            // Absolute prefixes, root, and `..` are categorically rejected -
            // a restored file's relative_path is by contract relative + safe.
            _ => {
                return Err(AdapterError::schema(
                    "restore",
                    file.relative_path.display().to_string(),
                    "relative_path component is not a normal name",
                ));
            }
        };
        let Some(text) = segment.to_str() else {
            return Err(AdapterError::schema(
                "restore",
                file.relative_path.display().to_string(),
                "relative_path segment is not UTF-8",
            ));
        };
        validate_path_id(
            "restore",
            "relative_path segment",
            text,
            file.relative_path.display().to_string(),
        )?;
    }

    let dest = staging.join(&file.relative_path);
    // Defense-in-depth: confirm the joined path is still inside the staging
    // dir even if every individual segment passed the syntactic check.
    if !dest.starts_with(staging) {
        return Err(AdapterError::schema(
            "restore",
            file.relative_path.display().to_string(),
            "relative_path escaped the restore root after join",
        ));
    }
    let io =
        |location: String, source: std::io::Error| AdapterError::io("restore", location, source);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| io(parent.display().to_string(), e))?;
    }
    std::fs::write(&dest, &file.bytes).map_err(|e| io(dest.display().to_string(), e))?;
    Ok(())
}

pub(crate) fn extracted_text(value: &Option<Extracted<String>>) -> &str {
    value.as_deref().map(String::as_str).unwrap_or("")
}

/// Deterministic message ordering for restore: timestamp, then id as a
/// tiebreaker so equal-timestamp messages always serialize in a stable order.
pub(crate) fn by_timestamp_then_id(
    left: &MessageWithParts,
    right: &MessageWithParts,
) -> std::cmp::Ordering {
    left.message
        .timestamp()
        .cmp(&right.message.timestamp())
        .then_with(|| left.message.id().cmp(right.message.id()))
}

/// `ProviderOptions::new()` shortcut; both adapters reach for an empty
/// options map often enough that naming the no-op clarifies the call sites.
#[inline]
pub(crate) fn empty_options() -> ProviderOptions {
    ProviderOptions::new()
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    };

    use serde_json::Value;
    use tempfile::TempDir;

    use super::{Adapter, AdapterFactory, Env, NoopOracle, RestoreFidelity};
    use crate::{handlers::ingest_adapter, sessions::Store};

    /// Shared probe_default contract: when the adapter's expected install
    /// subpath exists under an injected `HOME`, `probe_default` returns it;
    /// when the path is removed, it returns `None`. Each adapter owns its
    /// `probe_default_*` test (per the seam-boundaries rule) but the shape
    /// is the same, so the helper takes the factory + its expected subpath.
    pub(crate) fn assert_probe_default(
        factory: &dyn AdapterFactory,
        expected_subpath: &[&str],
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let mut expected = temp.path().to_path_buf();
        for segment in expected_subpath {
            expected.push(segment);
        }
        std::fs::create_dir_all(&expected)?;
        let env = Env::with_home(temp.path());

        let probe = factory.probe_default(&env);
        let got = probe
            .as_ref()
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str);
        anyhow::ensure!(
            got == expected.to_str(),
            "factory must probe its install path: got {got:?}, expected {expected:?}",
        );

        std::fs::remove_dir_all(&expected)?;
        anyhow::ensure!(
            factory.probe_default(&env).is_none(),
            "probe_default must be None once the install path disappears",
        );
        Ok(())
    }

    pub(crate) async fn assert_native_restore(
        factory: &dyn AdapterFactory,
        adapter: &dyn Adapter,
        source_root: &Path,
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        ingest_adapter(&store, adapter, &NoopOracle, |_| {}).await?;
        let session_ids = store.session_ids().await?;
        assert!(
            !session_ids.is_empty(),
            "native restore fixture must ingest at least one session",
        );

        let mut restored_paths = BTreeSet::new();
        for session_id in session_ids {
            let Some(session) = store.get_session(&session_id).await? else {
                anyhow::bail!("session id listed by store was not readable: {session_id}");
            };
            let restored = factory.serialize(&session, RestoreFidelity::Native)?;
            for file in restored {
                let expected = source_root.join(&file.relative_path);
                let expected_bytes = std::fs::read(&expected)
                    .map_err(|err| anyhow::anyhow!("read {}: {err}", expected.display()))?;
                assert_json_file_equal(&expected, &expected_bytes, &file.bytes)?;
                restored_paths.insert(file.relative_path);
            }
        }
        assert_eq!(
            restored_paths,
            source_json_files(source_root)?,
            "native restore must emit exactly the source JSON/JSONL file set",
        );
        Ok(())
    }

    fn source_json_files(root: &Path) -> anyhow::Result<BTreeSet<PathBuf>> {
        let mut out = BTreeSet::new();
        collect_source_json_files(root, root, &mut out)?;
        Ok(out)
    }

    fn collect_source_json_files(
        root: &Path,
        dir: &Path,
        out: &mut BTreeSet<PathBuf>,
    ) -> anyhow::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                collect_source_json_files(root, &path, out)?;
                continue;
            }
            if let Some("json" | "jsonl") = path.extension().and_then(|ext| ext.to_str()) {
                out.insert(path.strip_prefix(root)?.to_path_buf());
            }
        }
        Ok(())
    }

    fn assert_json_file_equal(path: &Path, expected: &[u8], actual: &[u8]) -> anyhow::Result<()> {
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            let expected_lines = json_lines(expected)?;
            let actual_lines = json_lines(actual)?;
            assert_eq!(
                actual_lines,
                expected_lines,
                "jsonl mismatch at {}",
                path.display()
            );
        } else {
            let expected_value: serde_json::Value = serde_json::from_slice(expected)?;
            let actual_value: serde_json::Value = serde_json::from_slice(actual)?;
            assert_eq!(
                actual_value,
                expected_value,
                "json mismatch at {}",
                path.display()
            );
        }
        Ok(())
    }

    fn json_lines(bytes: &[u8]) -> anyhow::Result<Vec<serde_json::Value>> {
        let text = std::str::from_utf8(bytes)?;
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).map_err(Into::into))
            .collect()
    }
}
