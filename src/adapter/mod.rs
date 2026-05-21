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

mod claude_code;
mod codex_cli;
mod discovery;
pub mod extract;
mod jsonl;

pub use claude_code::{ClaudeCodeAdapter, ClaudeCodeFactory};
pub use codex_cli::{CodexCliAdapter, CodexCliFactory};
pub use discovery::{Candidate, discover, prompt_and_persist};
pub use extract::{
    Extracted, Source, extract_bool, extract_compact_repr, extract_raw_record, extract_self_str,
    extract_str, extract_value,
};

/// Stateless face of an adapter type: how the registry knows about it without
/// instantiating it. One implementation per known format, registered in
/// [`registry`].
pub trait AdapterFactory: Send + Sync {
    /// Stable short name. Used as the `[sources.<name>]` config key, the
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
    /// blob that would go into `[sources.<name>]` if the picker writes it
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
            Ok(AdapterYield::Skipped { .. }) => None,
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
    /// short-circuit per-session re-decoding (spec.md#event-ordering). Default impl
    /// ignores the oracle.
    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a>;
}

/// Per-session watermark lookup: when did pond last write this session?
/// Backed by Lance's `_row_last_updated_at_version` joined to the manifest
/// commit timestamp (spec.md#event-ordering). Adapter compares this to the source
/// file's mtime to decide whether to re-decode.
pub trait SkipOracle: Send + Sync {
    fn last_ingested_at(&self, session_id: &str) -> Option<DateTime<Utc>>;
}

/// `SkipOracle` that always returns `None`. Used by tests and benches that
/// don't want skip behavior interfering with their assertions.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopOracle;

impl SkipOracle for NoopOracle {
    fn last_ingested_at(&self, _session_id: &str) -> Option<DateTime<Utc>> {
        None
    }
}

#[derive(Debug, Clone)]
pub enum AdapterYield {
    Event(IngestEvent),
    Skipped {
        session_id: String,
        project: Option<String>,
        reason: SkipReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    Fresh,
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
    &[&ClaudeCodeFactory, &CodexCliFactory]
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

// -- shared helpers used by file-tree-based adapters ---------------------------

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
    use std::path::Path;

    use tempfile::TempDir;

    use super::{Adapter, AdapterFactory, NoopOracle, RestoreFidelity};
    use crate::{handlers::ingest_adapter, sessions::Store};

    pub(crate) async fn assert_native_restore(
        factory: &dyn AdapterFactory,
        adapter: &dyn Adapter,
        source_root: &Path,
    ) -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        ingest_adapter(&store, adapter, &NoopOracle, |_| {}).await?;
        for session_id in store.session_ids().await? {
            let Some(session) = store.get_session(&session_id).await? else {
                anyhow::bail!("session id listed by store was not readable: {session_id}");
            };
            let restored = factory.serialize(&session, RestoreFidelity::Native)?;
            for file in restored {
                let expected = source_root.join(&file.relative_path);
                let expected_bytes = std::fs::read(&expected)
                    .map_err(|err| anyhow::anyhow!("read {}: {err}", expected.display()))?;
                assert_json_file_equal(&expected, &expected_bytes, &file.bytes)?;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    #[test]
    fn each_factory_probes_its_default_under_an_injected_home() {
        // Per-adapter discovery lives on each factory's `probe_default`, not in
        // a central name->path table. Driving each one with an injected `home`
        // proves the rule lives where the format lives.
        let temp = TempDir::new().unwrap();
        let home = temp.path();
        let claude_dir = home.join(".claude").join("projects");
        let codex_dir = home.join(".codex").join("sessions");
        std::fs::create_dir_all(&claude_dir).unwrap();
        std::fs::create_dir_all(&codex_dir).unwrap();

        let env = Env::with_home(home);

        let claude_probe = ClaudeCodeFactory.probe_default(&env);
        assert_eq!(
            claude_probe
                .as_ref()
                .and_then(|v| v.get("path"))
                .and_then(Value::as_str),
            Some(claude_dir.to_str().unwrap()),
        );

        let codex_probe = CodexCliFactory.probe_default(&env);
        assert_eq!(
            codex_probe
                .as_ref()
                .and_then(|v| v.get("path"))
                .and_then(Value::as_str),
            Some(codex_dir.to_str().unwrap()),
        );

        // Removing the codex marker dir drops just that factory's probe.
        std::fs::remove_dir_all(&codex_dir).unwrap();
        assert!(CodexCliFactory.probe_default(&env).is_none());
        assert!(ClaudeCodeFactory.probe_default(&env).is_some());
    }
}
