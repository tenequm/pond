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
mod hermes;
mod jsonl;
mod letta_code;
mod nanoclaw;
mod oh_my_pi;
mod openclaw;
mod opencode;
mod pi_coding_agent;
mod sqlite;

pub use claude_ai_export::{ClaudeAiExportAdapter, ClaudeAiExportFactory};
pub use claude_code::{ClaudeCodeAdapter, ClaudeCodeFactory};
pub use claude_desktop_app::{ClaudeDesktopAppAdapter, ClaudeDesktopAppFactory};
pub use codex_cli::{CodexCliAdapter, CodexCliFactory};
pub use discovery::{
    Candidate, apply_to_doc, discover, persist_accept, probe_pathless, probe_unconfigured,
    prompt_and_persist, set_adapter_enabled,
};
pub use extract::{
    Extracted, Source, extract_bool, extract_compact_repr, extract_raw_record, extract_self_str,
    extract_str, extract_value,
};
pub use hermes::{HermesAdapter, HermesFactory};
pub use letta_code::{LettaCodeAdapter, LettaCodeFactory};
pub use nanoclaw::{NanoclawAdapter, NanoclawFactory};
pub use oh_my_pi::{OhMyPiAdapter, OhMyPiFactory};
pub use openclaw::{
    EraseTarget, OpenClawAdapter, OpenClawFactory, PreserveNote, ReconciliationReport,
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

    /// `None` when this factory can restore; `Some(reason)` when it is
    /// ingest-only, and the reason names the caller's alternative.
    ///
    /// A capability query, not a runtime failure: `pond resume` asks BEFORE it
    /// plans a lineage, so an ingest-only client is a typed unanswerable request
    /// (exit 2) instead of an error surfacing from [`Self::serialize`] as a
    /// generic exit 1. The reason string lives with the adapter that owns it so
    /// the CLI carries no per-adapter advice.
    fn restore_unsupported(&self) -> Option<&'static str> {
        None
    }

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

    /// Cheap sync preview: classify every discovered source as fresh vs
    /// pending against the oracle's watermarks WITHOUT decoding bodies (the
    /// same gate [`Self::events_with`] applies before its expensive read).
    /// Powers `pond sync --dry-run` and the `pond status` pending count, so it
    /// must stay bounded-read cheap. `Ok(None)` (the default) means this
    /// adapter has no gate cheaper than a full read and callers report the
    /// pending count as unknown rather than paying for it.
    fn plan<'a>(&'a self, _oracle: &'a dyn SkipOracle) -> PlanFuture<'a> {
        Box::pin(async { Ok(None) })
    }
}

/// What the next `pond sync` would do for one adapter, computed from the
/// freshness gate alone: `pending` sessions get read; `fresh` ones are skipped
/// outright - including sessions whose source provably holds nothing ingestible
/// right now ([`SourceWatermark::Empty`]), because "nothing to sync" IS up to
/// date. The unit is the SESSION, not the file: a source may hold many (an
/// export archive) and the gate enumerates and counts what pond stores.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncPlan {
    pub sessions: usize,
    pub fresh: usize,
    pub pending: usize,
}

impl SyncPlan {
    /// The one classifier: fold session heads through [`source_in_sync`].
    /// Callers with an empty oracle must not use this - there is nothing to
    /// compare against, so a first sync's plan is [`SyncPlan::all_pending`]
    /// without paying for peeks.
    pub fn from_heads<'a>(
        oracle: &dyn SkipOracle,
        heads: impl IntoIterator<Item = (Option<&'a str>, SourceWatermark)>,
    ) -> Self {
        let mut plan = Self::default();
        for (session_id, watermark) in heads {
            plan.sessions += 1;
            if source_in_sync(oracle, session_id, watermark) {
                plan.fresh += 1;
            } else {
                plan.pending += 1;
            }
        }
        plan
    }

    /// A first-sync plan: no oracle entries means every session will be read.
    pub fn all_pending(sessions: usize) -> Self {
        Self {
            sessions,
            pending: sessions,
            ..Self::default()
        }
    }
}

/// Source-side verdict of a freshness peek.
///
/// `Empty` MUST be claimed only on PROOF that the source currently holds
/// nothing ingestible (a zero-byte file; a whole-source inspection finding no
/// ingestible record). The proof is re-derived from current source content on
/// every run - never a cached marker - so the moment the source gains real
/// content the peek stops saying `Empty` and the source re-reads
/// (spec.md#session-movement-complete). Without `Empty`, a permanently
/// content-free source can never earn a stored watermark and reports a store
/// that syncs clean as forever out of date.
///
/// `Opaque` is the safe default for anything undeterminable cheaply (an
/// oversized record, a tail window smaller than the file): the source counts
/// pending and re-reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceWatermark {
    /// Latest ingestible-content timestamp (micros) found in the source.
    At(i64),
    /// Provably nothing to ingest right now.
    Empty,
    /// Could not determine cheaply - re-read to be safe.
    Opaque,
}

/// Gate verdict for one source: skip (in sync) or re-read. `Empty` sources are
/// in sync by definition; a watermark compares via [`is_session_fresh`], which
/// needs a session id to look up the stored side; anything else re-reads.
pub fn source_in_sync(
    oracle: &dyn SkipOracle,
    session_id: Option<&str>,
    watermark: SourceWatermark,
) -> bool {
    match watermark {
        SourceWatermark::Empty => true,
        SourceWatermark::At(ts) => {
            session_id.is_some_and(|id| is_session_fresh(oracle, id, Some(ts)))
        }
        SourceWatermark::Opaque => false,
    }
}

/// Boxed future for [`Adapter::plan`], mirroring [`DiscoverFuture`].
pub type PlanFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Option<SyncPlan>, AdapterError>> + Send + 'a>,
>;

/// Store-side freshness watermark: the max message timestamp (micros) pond
/// already holds for a session. Backed by the resident row-meta map (zero S3 -
/// see [`crate::rowmap`]), which is itself rebuilt from the store, so the check
/// is deterministic with no local cursor to desync. `None` means pond has never
/// seen the session, or the resident map is behind the store - either way the
/// caller re-reads.
///
/// The skip is sound because pond and every source are append-only: a session's
/// max message timestamp only advances as it gains messages. The one residual is
/// two messages sharing the exact micros across a sync boundary (negligible at
/// sub-second precision, self-healing once any newer message arrives); `pond sync
/// --verify` (which passes [`NoopOracle`]) is the full-re-read backstop.
pub trait SkipOracle: Send + Sync {
    fn session_max_ts(&self, session_id: &str) -> Option<i64>;

    /// Fast-path hint: the oracle has no entries at all (first ingest or
    /// `NoopOracle`). Lets adapters skip the per-session work needed to read the
    /// source's latest message timestamp. Defaults to `false`.
    fn is_empty(&self) -> bool {
        false
    }
}

/// Seam decision rule - the only place the freshness comparison lives. A session
/// is fresh (skip the re-decode) iff the source's latest message timestamp is no
/// newer than pond's stored watermark. A missing signal on either side is never
/// fresh.
pub fn is_session_fresh(
    oracle: &dyn SkipOracle,
    session_id: &str,
    source_last_ts_micros: Option<i64>,
) -> bool {
    matches!(
        (oracle.session_max_ts(session_id), source_last_ts_micros),
        (Some(stored), Some(source)) if source <= stored
    )
}

/// `SkipOracle` that always returns `None`. Used by `--verify`, tests, and
/// benches that want every source re-read.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopOracle;

impl SkipOracle for NoopOracle {
    fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
        None
    }

    fn is_empty(&self) -> bool {
        true
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
    /// Session present in more than one source form; this copy is superseded by
    /// an authoritative copy the same run ingests (e.g. opencode's legacy tree
    /// copy of a DB-resident session). Content identity is not verified -
    /// supersession is by session id (the source's documented migration
    /// contract). Visible and counted, never folded into `Empty`.
    Superseded,
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
    /// Read `home` from the environment portably ([`crate::config::home_dir`]:
    /// `USERPROFILE` on Windows, `HOME` on Unix). Returns `None` when unset
    /// (CI, post-install hooks, sandboxed runs).
    pub fn from_env() -> Option<Self> {
        crate::config::home_dir().map(|home| Self { home })
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
        &OpenClawFactory,
        &NanoclawFactory,
        &HermesFactory,
        &PiCodingAgentFactory,
        &OhMyPiFactory,
        &LettaCodeFactory,
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
    Ok(expand_home(cfg.path))
}

/// Expand a leading `~` against the user's home directory, or return the path
/// untouched when the env has no home (CI, post-install hooks, sandboxes).
/// Shared because an adapter whose config carries more than one path cannot use
/// [`config_path`]. Home resolution is portable ([`crate::config::home_dir`]:
/// `USERPROFILE` on Windows, `HOME` on Unix).
pub(crate) fn expand_home(path: PathBuf) -> PathBuf {
    match crate::config::home_dir() {
        Some(home) => crate::config::expand_home_under(&path, &home),
        None => path,
    }
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
    if let Some(reason) = windows_hostile(id) {
        return Err(AdapterError::schema(
            adapter,
            location,
            format!("{kind} {reason}: {id}"),
        ));
    }
    Ok(())
}

/// Matched without the extension - `NUL.jsonl` opens the NUL device too.
const WINDOWS_DEVICE_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Why `segment` cannot be a filesystem name on Windows, or `None`. Enforced on
/// every platform because archives are portable, and because each of these
/// fails *silently* on Windows rather than loudly.
fn windows_hostile(segment: &str) -> Option<&'static str> {
    if segment.contains(':') {
        return Some("contains ':', which names an NTFS alternate data stream on Windows");
    }
    if segment.ends_with('.') || segment.ends_with(' ') {
        return Some("ends with a dot or space, which Windows silently strips");
    }
    let stem = segment
        .split_once('.')
        .map_or(segment, |(head, _)| head)
        .trim_end();
    if WINDOWS_DEVICE_NAMES
        .iter()
        .any(|device| stem.eq_ignore_ascii_case(device))
    {
        return Some("is a reserved Windows device name");
    }
    None
}

/// Resolve each `RestoredFile` to its absolute destination under `root`. Every
/// path segment is re-validated and the joined path is required to stay inside
/// `root` (spec.md#adapter-native-restore-lossless: restore writes are
/// adapter-supplied, but the gate lives at the writer so a single audit covers
/// every adapter today and tomorrow). Separate from the write so a caller
/// restoring a whole lineage can refuse a collision across ALL its files before
/// any of them touch disk.
pub fn restore_destinations(
    root: &Path,
    files: &[RestoredFile],
) -> Result<Vec<PathBuf>, AdapterError> {
    files
        .iter()
        .map(|file| restore_destination(root, file))
        .collect()
}

fn restore_destination(root: &Path, file: &RestoredFile) -> Result<PathBuf, AdapterError> {
    let at = || file.relative_path.display().to_string();
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
                    at(),
                    "relative_path component is not a normal name",
                ));
            }
        };
        let Some(text) = segment.to_str() else {
            return Err(AdapterError::schema(
                "restore",
                at(),
                "relative_path segment is not UTF-8",
            ));
        };
        validate_path_id("restore", "relative_path segment", text, at())?;
    }
    let dest = root.join(&file.relative_path);
    // Defense-in-depth: confirm the joined path is still inside `root` even if
    // every individual segment passed the syntactic check.
    if !dest.starts_with(root) {
        return Err(AdapterError::schema(
            "restore",
            at(),
            "relative_path escaped the restore root after join",
        ));
    }
    Ok(dest)
}

/// Write a batch of `RestoredFile`s under `root`, creating parent directories
/// as needed and returning what was written. Restore NEVER overwrites and never
/// removes: `root` is a live client's own data directory, so an existing
/// destination is refused - naming every colliding path - before the first byte
/// is written. A mid-batch io failure unwinds to the tree as it was found: every
/// file this call created is removed (including one only half written), and
/// every directory it created is removed if it is still empty - a directory
/// something else has since populated stays.
///
/// Callers restoring a lineage MUST pass the whole lineage in one call: the
/// no-overwrite refusal and the unwind are both batch-scoped, so splitting a
/// lineage across calls reintroduces the partial write they exist to prevent
/// (spec.md#adapter-lineage-complete-restore).
pub fn write_restored_files(
    root: &Path,
    files: Vec<RestoredFile>,
) -> Result<Vec<PathBuf>, AdapterError> {
    let dests = restore_destinations(root, &files)?;
    let existing: Vec<String> = dests
        .iter()
        .filter(|dest| exists_even_if_dangling(dest))
        .map(|dest| dest.display().to_string())
        .collect();
    if !existing.is_empty() {
        return Err(AdapterError::schema(
            "restore",
            root.display().to_string(),
            format!(
                "refusing to overwrite existing files: {}",
                existing.join(", ")
            ),
        ));
    }

    let io =
        |location: String, source: std::io::Error| AdapterError::io("restore", location, source);
    let mut written = Vec::with_capacity(dests.len());
    let mut created_dirs: Vec<PathBuf> = Vec::new();
    let outcome = (|| -> Result<(), AdapterError> {
        for (dest, file) in dests.into_iter().zip(files) {
            if let Some(parent) = dest.parent() {
                // `create_dir_all` reports nothing about which ancestors it had
                // to bring into being, so name them before the call - that list
                // is the unwind's only record of what this batch added to the
                // tree.
                let created: Vec<PathBuf> = parent
                    .ancestors()
                    .take_while(|dir| !dir.exists())
                    .map(Path::to_path_buf)
                    .collect();
                let result = std::fs::create_dir_all(parent);
                created_dirs.extend(created);
                result.map_err(|error| io(parent.display().to_string(), error))?;
            }
            // `create_new` is the guard, not the pre-check above: it is O_EXCL,
            // so it refuses any existing path - including a symlink, which
            // `fs::write` would instead follow to a target outside `root`,
            // defeating the containment check in `restore_destination`. It also
            // closes the window between that pre-check and this write, and
            // catches two `RestoredFile`s claiming one path. The pre-check
            // survives only to name every collision at once.
            let mut handle = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&dest)
                .map_err(|error| io(dest.display().to_string(), error))?;
            // The path exists from here on, so it joins the unwind BEFORE the
            // write that can still fail: a truncated leftover is
            // indistinguishable from a finished restore on the retry, which
            // would then report "already resumed" over a broken file.
            written.push(dest);
            std::io::Write::write_all(&mut handle, &file.bytes).map_err(|error| {
                // The path just pushed is the one being written.
                let location = written
                    .last()
                    .map_or_else(String::new, |dest| dest.display().to_string());
                io(location, error)
            })?;
        }
        Ok(())
    })();
    let Err(error) = outcome else {
        return Ok(written);
    };
    for path in &written {
        let _ = std::fs::remove_file(path);
    }
    // Deepest first, because `remove_dir` only takes empty directories - and
    // that emptiness check is what keeps the unwind from removing a directory
    // someone else has since put files in.
    created_dirs.sort_unstable_by_key(|dir| std::cmp::Reverse(dir.components().count()));
    for dir in &created_dirs {
        let _ = std::fs::remove_dir(dir);
    }
    Err(error)
}

/// Does anything occupy this path? `Path::exists` follows symlinks, so a
/// DANGLING one reads as absent - and a restore that trusted it would write
/// through the link, outside the root. `symlink_metadata` sees the link itself.
pub fn exists_even_if_dangling(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok()
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
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::{RestoreFidelity, RestoredFile, validate_path_id, write_restored_files};

    #[test]
    fn validate_path_id_refuses_windows_hostile_segments() {
        let ok = |id: &str| validate_path_id("t", "id", id, "loc").is_ok();
        assert!(ok("ses_01HXY"));
        assert!(ok("msg-1.jsonl"));
        assert!(!ok("NUL"));
        assert!(!ok("nul.jsonl"));
        assert!(!ok("CoM9.txt"));
        // A device name only as the whole stem.
        assert!(ok("console.jsonl"));
        assert!(ok("nullable"));
        // Windows strips these, collapsing two ids onto one file.
        assert!(!ok("session."));
        assert!(!ok("session "));
        assert!(!ok("C:session"));
        assert!(!ok("session:stream"));
    }

    /// A failed batch leaves nothing of itself behind - not the files it wrote,
    /// and not the directories it had to create to write them.
    #[test]
    fn a_failed_batch_removes_the_files_and_the_directories_it_created() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("client-home");
        // A regular file where the second destination needs a directory: the
        // batch fails at `create_dir_all`, after the first file has landed.
        std::fs::create_dir_all(&root).expect("root");
        std::fs::write(root.join("blocked"), b"in the way").expect("blocker");

        let error = write_restored_files(
            &root,
            vec![
                RestoredFile::new(
                    "sessions/deep/first.jsonl",
                    b"first".to_vec(),
                    RestoreFidelity::Foreign,
                ),
                RestoredFile::new(
                    "blocked/second.jsonl",
                    b"second".to_vec(),
                    RestoreFidelity::Foreign,
                ),
            ],
        )
        .expect_err("a destination whose parent is a file must fail the batch");
        assert!(error.to_string().contains("io error"), "{error}");

        assert!(
            !root.join("sessions/deep/first.jsonl").exists(),
            "the file written before the failure survived the unwind",
        );
        assert!(
            !root.join("sessions/deep").exists() && !root.join("sessions").exists(),
            "directories this batch created survived the unwind",
        );
        assert!(
            root.join("blocked").is_file() && root.is_dir(),
            "the unwind removed something it did not create",
        );
    }

    /// The unwind is scoped to this batch: a directory it did not create keeps
    /// standing, and everything already inside it stays.
    #[test]
    fn the_unwind_keeps_a_directory_it_did_not_create() {
        let temp = TempDir::new().expect("temp dir");
        let root = temp.path().join("client-home");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::write(root.join("blocked"), b"in the way").expect("blocker");

        write_restored_files(
            &root,
            vec![RestoredFile::new(
                "sessions/first.jsonl",
                b"first".to_vec(),
                RestoreFidelity::Foreign,
            )],
        )
        .expect("the first batch writes cleanly");
        let intruder = root.join("sessions/not-ours.txt");
        std::fs::write(&intruder, b"someone else's").expect("intruder");

        write_restored_files(
            &root,
            vec![
                RestoredFile::new(
                    "sessions/second.jsonl",
                    b"second".to_vec(),
                    RestoreFidelity::Foreign,
                ),
                RestoredFile::new(
                    "blocked/third.jsonl",
                    b"third".to_vec(),
                    RestoreFidelity::Foreign,
                ),
            ],
        )
        .expect_err("the second batch fails on the blocked destination");
        assert!(
            !root.join("sessions/second.jsonl").exists(),
            "the failed batch's own file survived",
        );
        assert!(intruder.is_file(), "the unwind removed a foreign file");
    }

    /// Adapters are import-isolated from the store and query layer: no
    /// production line under `src/adapter/` names the store, the substrate,
    /// the rowmap, the handlers, the Lance/Arrow/DataFusion crates beneath
    /// them, or `Extracted::from_stored`, and every crate-root path an adapter
    /// spells resolves to the seam's own vocabulary (`adapter`, `wire`,
    /// `config`, and the three canonical model types from `sessions`).
    ///
    /// What this proves is mechanical and narrow: adapter code cannot reach the
    /// store's write path, commit discipline, or query plans, so an adapter PR
    /// is reviewed as parse and mapping behavior and the standard checks are
    /// its whole bar. It does not prove performance isolation - an adapter
    /// still decides the volume and shape of what it emits, and
    /// `benches/ingest_bench.rs` drives claude-code end to end. Every
    /// exemption is listed with its reason and its expected line count, so a
    /// second use behind the same name is a visible change.
    #[test]
    fn adapters_never_touch_the_store_or_query_layer() {
        // Matched as whole identifiers or as a `<token>_` prefix: the lance
        // and arrow workspaces are hyphenated sub-crates (`lance_io`,
        // `arrow_select`).
        const FORBIDDEN_IDENTS: [&str; 10] = [
            "Store",
            "substrate",
            "rowmap",
            "handlers",
            "lance",
            "lancedb",
            "arrow",
            "datafusion",
            "object_store",
            "from_stored",
        ];
        // The crate-root modules an adapter may name, and the only `sessions`
        // items; `sql`, `transport`, `embed`, `store`, and every other module
        // fail by omission, grouped imports included.
        const ALLOWED_ROOTS: [&str; 4] = ["adapter", "wire", "config", "sessions"];
        const ALLOWED_SESSIONS_ITEMS: [&str; 3] =
            ["IngestEvent", "SessionWithMessages", "MessageWithParts"];
        // (file, token, expected hits). openclaw's deletion reconciliation is a
        // read-only detection pass over stored sessions
        // (spec.md#session-append-only-exception): the import path, then the
        // identifier on the import line and the `&Store` parameter; read-only
        // is documented there, not checked here. `from_stored` is defined in
        // extract.rs for the store side to call.
        const EXEMPT: [(&str, &str, usize); 3] = [
            ("openclaw.rs", "crate::sessions::Store", 1),
            ("openclaw.rs", "Store", 2),
            ("extract.rs", "from_stored", 1),
        ];
        // A column-0 line opening one of these below the test module is a
        // production item the scan would otherwise never see.
        const ITEM_KEYWORDS: [&str; 13] = [
            "fn",
            "pub",
            "impl",
            "use",
            "const",
            "static",
            "struct",
            "enum",
            "trait",
            "type",
            "async",
            "unsafe",
            "macro_rules!",
        ];

        fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("adapter dir is readable") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    rust_files(&path, out);
                } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
                    out.push(path);
                }
            }
        }
        fn identifiers(text: &str) -> impl Iterator<Item = &str> {
            text.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .filter(|ident| !ident.is_empty())
        }
        fn forbidden(ident: &str) -> Option<&'static str> {
            FORBIDDEN_IDENTS.into_iter().find(|token| {
                ident == *token
                    || ident
                        .strip_prefix(token)
                        .is_some_and(|rest| rest.starts_with('_'))
            })
        }
        // Test modules sit at the bottom of the file (repo convention) and
        // legitimately drive the store end to end: the scan stops at the
        // `#[cfg(test)]` that introduces a `mod` (attributes and doc lines in
        // between allowed), not at one gating one item.
        fn opens_test_module(lines: &[&str], index: usize) -> bool {
            lines[index].trim_start().starts_with("#[cfg(test)]")
                && lines[index + 1..]
                    .iter()
                    .map(|line| line.trim_start())
                    .find(|line| {
                        !line.is_empty() && !line.starts_with("#[") && !line.starts_with("//")
                    })
                    .is_some_and(|line| line.split_whitespace().take(4).any(|word| word == "mod"))
        }
        /// The text between a `{` at `open` and its matching `}`, exclusive.
        fn group_body(text: &str, open: usize) -> Option<&str> {
            let mut depth = 0usize;
            for (offset, c) in text[open..].char_indices() {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(&text[open + 1..open + offset]);
                        }
                    }
                    _ => {}
                }
            }
            None
        }
        fn split_top_level(body: &str) -> Vec<&str> {
            let mut entries = Vec::new();
            let mut depth = 0usize;
            let mut start = 0usize;
            for (offset, c) in body.char_indices() {
                match c {
                    '{' => depth += 1,
                    '}' => depth -= 1,
                    ',' if depth == 0 => {
                        entries.push(body[start..offset].trim());
                        start = offset + 1;
                    }
                    _ => {}
                }
            }
            entries.push(body[start..].trim());
            entries
                .into_iter()
                .filter(|entry| !entry.is_empty())
                .collect()
        }
        /// One crate-root path (`wire::Role::System`, `sessions::{A, B}`,
        /// `sql::run`) against the allowlist; `Some` names the offending path.
        fn check_root_path(entry: &str) -> Option<String> {
            let head = entry.split('{').next().unwrap_or("").trim_end_matches("::");
            let mut segments = head.split("::").map(str::trim).filter(|s| !s.is_empty());
            let root = segments.next()?;
            if !ALLOWED_ROOTS.contains(&root) {
                return Some(format!("crate::{head}"));
            }
            if root != "sessions" {
                return None;
            }
            let group_items = entry
                .find('{')
                .and_then(|open| group_body(entry, open))
                .map(|body| identifiers(body).collect::<Vec<_>>())
                .unwrap_or_default();
            segments
                .chain(group_items)
                .find(|item| !ALLOWED_SESSIONS_ITEMS.contains(item))
                .map(|item| format!("crate::sessions::{item}"))
        }
        /// Every crate-root path spelled inline on `line` (`crate::x::y`,
        /// `crate::{a, b}`, `super::super::x` from a child module).
        fn inline_root_paths<'a>(line: &'a str, root_prefixes: &[&str]) -> Vec<&'a str> {
            let mut entries = Vec::new();
            for prefix in root_prefixes {
                for (start, _) in line.match_indices(prefix) {
                    let rest = &line[start + prefix.len()..];
                    let head_len = rest
                        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
                        .unwrap_or(rest.len());
                    let entry_end = match rest[head_len..].starts_with('{') {
                        true => group_body(rest, head_len)
                            .map_or(rest.len(), |body| head_len + body.len() + 2),
                        false => head_len,
                    };
                    entries.push(&rest[..entry_end]);
                }
            }
            entries
        }
        fn is_item_line(line: &str) -> bool {
            let first = line.split(|c: char| c.is_whitespace() || c == '(').next();
            first.is_some_and(|word| ITEM_KEYWORDS.contains(&word))
                && !line.split_whitespace().take(4).any(|word| word == "mod")
        }

        let adapter_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("adapter");
        let mut files = Vec::new();
        rust_files(&adapter_dir, &mut files);
        files.sort();
        let mut violations = Vec::new();
        let mut exempt_hits: Vec<usize> = vec![0; EXEMPT.len()];
        for path in &files {
            let name = path
                .strip_prefix(&adapter_dir)
                .expect("collected under adapter_dir")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            // `super::` is the crate root from mod.rs and the seam from a child.
            let root_prefixes: &[&str] = match name.as_str() {
                "mod.rs" => &["crate::", "super::"],
                _ => &["crate::", "super::super::"],
            };
            let text = std::fs::read_to_string(path).expect("adapter source is readable");
            let lines: Vec<&str> = text.lines().collect();
            let mut record = |line_no: usize, token: &str, line: &str| {
                let exempt = EXEMPT
                    .iter()
                    .position(|(file, tok, _)| *file == name && *tok == token);
                match exempt {
                    Some(slot) => exempt_hits[slot] += 1,
                    None => violations.push(format!("{name}:{line_no}: `{token}` in: {line}")),
                }
            };
            // A multi-line `use crate::{ ... };` group: depth within it, and
            // which crate-root module a nested multi-line group belongs to.
            let mut group_depth = 0usize;
            let mut nested_root: Option<String> = None;
            let mut in_tests = false;
            for (index, raw) in lines.iter().enumerate() {
                let line_no = index + 1;
                let line = raw.trim_start();
                if in_tests {
                    if raw.len() == line.len() && is_item_line(line) {
                        record(line_no, "production item after the test module", line);
                    }
                    continue;
                }
                if opens_test_module(&lines, index) {
                    in_tests = true;
                    continue;
                }
                if line.starts_with("//") {
                    continue;
                }
                for ident in identifiers(line) {
                    if let Some(token) = forbidden(ident) {
                        record(line_no, token, line);
                    }
                }
                if group_depth > 0 {
                    let entry = line.trim_end_matches(';').trim_end_matches(',');
                    let opens = entry.matches('{').count();
                    let closes = entry.matches('}').count();
                    let is_path = entry.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_');
                    if group_depth == 1 && is_path {
                        if let Some(bad) = check_root_path(entry) {
                            record(line_no, &bad, line);
                        }
                        if opens > closes {
                            nested_root = entry.split("::").next().map(str::to_owned);
                        }
                    } else if group_depth > 1
                        && nested_root.as_deref() == Some("sessions")
                        && let Some(item) =
                            identifiers(entry).find(|item| !ALLOWED_SESSIONS_ITEMS.contains(item))
                    {
                        record(line_no, &format!("crate::sessions::{item}"), line);
                    }
                    group_depth = (group_depth + opens).saturating_sub(closes);
                    continue;
                }
                for entry in inline_root_paths(line, root_prefixes) {
                    if entry.starts_with('{') {
                        match group_body(entry, 0) {
                            Some(body) => {
                                for sub in split_top_level(body) {
                                    if let Some(bad) = check_root_path(sub) {
                                        record(line_no, &bad, line);
                                    }
                                }
                            }
                            // `crate::{` opening a multi-line group.
                            None => {
                                group_depth = 1;
                                nested_root = None;
                            }
                        }
                    } else if let Some(bad) = check_root_path(entry) {
                        record(line_no, &bad, line);
                    }
                }
            }
        }
        for ((file, token, expected), actual) in EXEMPT.iter().zip(exempt_hits) {
            if actual != *expected {
                violations.push(format!(
                    "{file}: `{token}` exempt for {expected} lines, found {actual} - a new use \
                     needs its own reason, a removed one drops the exemption"
                ));
            }
        }
        violations.sort();
        assert!(
            violations.is_empty(),
            "adapter code reached past the seam into the store/query layer:\n{}\n\
             A legitimate read-only use goes into EXEMPT as (file, token, count) with its \
             reason; production items belong above the test module.",
            violations.join("\n"),
        );
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
    };

    use serde_json::Value;
    use tempfile::TempDir;

    use super::{Adapter, AdapterFactory, Env, NoopOracle, RestoreFidelity, SkipOracle};
    use crate::{handlers::ingest_adapter, sessions::Store};

    /// Oracle that makes every session gate as fresh.
    pub(crate) struct MaxWatermarkOracle;
    impl SkipOracle for MaxWatermarkOracle {
        fn session_max_ts(&self, _session_id: &str) -> Option<i64> {
            Some(i64::MAX)
        }
    }

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
