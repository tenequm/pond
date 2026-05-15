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

use std::{
    io::IsTerminal,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use dialoguer::{MultiSelect, theme::ColorfulTheme};
use serde_json::Value;
use tokio_stream::Stream;
use toml_edit::{DocumentMut, Item, Table};

use crate::{sessions::IngestEvent, wire::ProviderOptions};

mod claude_code;
mod codex_cli;

pub use claude_code::{ClaudeCodeAdapter, ClaudeCodeFactory};
pub use codex_cli::{CodexCliAdapter, CodexCliFactory};

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
}

/// Live, configured adapter instance. Holds whatever handle the source needs
/// (an open directory root, an HTTP client + auth, a database connection)
/// for the lifetime of its event stream.
pub trait Adapter: Send + Sync {
    /// Stream every canonical event for every session this adapter knows
    /// about, in append-only order per session. The stream borrows `self`
    /// so callers can pass `&adapter` or hold a `Box<dyn Adapter>` and
    /// invoke this through `as_ref()`.
    fn events(&self) -> EventStream<'_>;
}

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

/// Path-bearing io error used internally by [`collect_jsonl_files`]; callers
/// remap it into the right [`AdapterError`] with their adapter name.
pub(crate) struct IoAtPath {
    pub path: String,
    pub source: std::io::Error,
}

/// Walk `root` recursively and collect every `*.jsonl` file under it, sorted
/// for deterministic ingest order. Shared between the file-tree adapters
/// (claude-code, codex-cli, pi, nanoclaw, openclaw); fan-out tree adapters
/// (opencode) and file-pair adapters (claude-app) walk their own shapes.
pub(crate) async fn collect_jsonl_files(root: &Path) -> Result<Vec<PathBuf>, IoAtPath> {
    use std::ffi::OsStr;

    let mut paths = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let dir_display = dir.display().to_string();
        let mut entries = tokio::fs::read_dir(&dir).await.map_err(|source| IoAtPath {
            path: dir_display.clone(),
            source,
        })?;
        while let Some(entry) = entries.next_entry().await.map_err(|source| IoAtPath {
            path: dir_display.clone(),
            source,
        })? {
            let file_type = entry.file_type().await.map_err(|source| IoAtPath {
                path: dir_display.clone(),
                source,
            })?;
            let child = entry.path();
            if file_type.is_dir() {
                stack.push(child);
            } else if child.extension() == Some(OsStr::new("jsonl")) {
                paths.push(child);
            }
        }
    }
    paths.sort();
    Ok(paths)
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

/// `ProviderOptions::new()` shortcut; both adapters reach for an empty
/// options map often enough that naming the no-op clarifies the call sites.
#[inline]
pub(crate) fn empty_options() -> ProviderOptions {
    ProviderOptions::new()
}

/// One discovered adapter: its name, a hint to show the operator (typically
/// the probed path or endpoint), and the JSON config blob that will be
/// persisted under `[sources.<name>]` if the operator confirms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub name: String,
    pub hint: String,
    pub config: Value,
}

/// Probe every registered factory (or just the one named in `focus`) under
/// the current environment and shape results into [`Candidate`]s for the
/// picker. Returns an empty list when no factory's `probe_default` returned
/// anything - the caller surfaces a "configure manually" error in that case.
pub fn discover(focus: Option<&str>) -> Vec<Candidate> {
    let Some(env) = Env::from_env() else {
        return Vec::new();
    };
    let candidates: Vec<Candidate> = match focus {
        None => probe_all(&env)
            .into_iter()
            .map(|(name, config)| Candidate {
                name: name.to_owned(),
                hint: hint_for(&config),
                config,
            })
            .collect(),
        Some(name) => by_name(name)
            .and_then(|factory| factory.probe_default(&env))
            .map(|config| Candidate {
                name: name.to_owned(),
                hint: hint_for(&config),
                config,
            })
            .into_iter()
            .collect(),
    };
    candidates
}

/// Best-effort label for the picker. For filesystem configs that's the
/// `path`; for richer configs we fall back to a compact JSON dump so the
/// operator at least sees what they're confirming.
fn hint_for(config: &Value) -> String {
    if let Some(path) = config.get("path").and_then(Value::as_str) {
        return path.to_owned();
    }
    if let Some(endpoint) = config.get("endpoint").and_then(Value::as_str) {
        return endpoint.to_owned();
    }
    serde_json::to_string(config).unwrap_or_default()
}

/// Prompt the operator to pick which `candidates` to register, then persist
/// the chosen entries to `config.toml` and return them. Pre-checks every
/// candidate (the operator already opted in by running `pond sync`). In a
/// non-tty context we never prompt - we bail with a clear "configure
/// manually" message so CI and post-install scripts get a predictable error
/// instead of a hang.
pub fn prompt_and_persist(
    config_path: &Path,
    candidates: &[Candidate],
) -> anyhow::Result<Vec<Candidate>> {
    if candidates.is_empty() {
        bail!(
            "no adapter sources detected in this environment; known adapters: {}",
            known_names().join(", "),
        );
    }
    if !std::io::stdin().is_terminal() {
        bail!(
            "[sources] is empty and stdin is not a terminal; add a [sources.<adapter>] \
             entry to {} (known adapters: {})",
            config_path.display(),
            known_names().join(", "),
        );
    }
    let labels = candidates
        .iter()
        .map(|c| format!("{} ({})", c.name, c.hint))
        .collect::<Vec<_>>();
    let defaults = vec![true; candidates.len()];
    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select sources to register (space toggles, enter confirms)")
        .items(&labels)
        .defaults(&defaults)
        .interact()
        .context("source picker prompt failed")?;
    if selections.is_empty() {
        bail!("no sources selected; nothing to sync");
    }
    let picks: Vec<Candidate> = selections
        .into_iter()
        .filter_map(|index| candidates.get(index).cloned())
        .collect();
    persist(config_path, &picks)?;
    Ok(picks)
}

/// Write the picked sources back to `config.toml` under `[sources.<name>]`,
/// preserving any existing user comments/formatting via `toml_edit`. Each
/// pick's `config` JSON object is unpacked into TOML key/value pairs.
fn persist(config_path: &Path, picks: &[Candidate]) -> anyhow::Result<()> {
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config dir {}", parent.display()))?;
    }
    let existing = if config_path.exists() {
        std::fs::read_to_string(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?
    } else {
        String::new()
    };
    let mut doc: DocumentMut = existing
        .parse()
        .with_context(|| format!("failed to parse {} as TOML", config_path.display()))?;

    if !doc.contains_key("sources") {
        let mut table = Table::new();
        table.set_implicit(true);
        doc.insert("sources", Item::Table(table));
    }
    let Some(sources) = doc["sources"].as_table_mut() else {
        bail!("config.toml has a `sources` value that is not a table");
    };
    for pick in picks {
        let entry = json_to_toml_table(&pick.config).with_context(|| {
            format!(
                "pick for {:?} did not produce a TOML-shaped table",
                pick.name
            )
        })?;
        sources.insert(&pick.name, Item::Table(entry));
    }

    std::fs::write(config_path, doc.to_string())
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    Ok(())
}

/// Convert a JSON object into a `toml_edit::Table`. Factories produce JSON
/// blobs (the seam contract); the picker persists them as TOML tables. Non-
/// object roots are rejected with an error rather than silently dropped.
fn json_to_toml_table(value: &Value) -> anyhow::Result<Table> {
    let Value::Object(map) = value else {
        bail!("config blob must be a JSON object, got {value}");
    };
    let mut table = Table::new();
    for (key, val) in map {
        table[key] = json_to_toml_item(val)?;
    }
    Ok(table)
}

fn json_to_toml_item(value: &Value) -> anyhow::Result<Item> {
    use toml_edit::{Array, InlineTable, Value as TomlValue, value as tv};
    Ok(match value {
        Value::Null => bail!("null is not representable in TOML"),
        Value::Bool(b) => tv(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                tv(i)
            } else if let Some(f) = n.as_f64() {
                tv(f)
            } else {
                bail!("number {n} is not representable in TOML");
            }
        }
        Value::String(s) => tv(s.clone()),
        Value::Array(values) => {
            let mut array = Array::new();
            for v in values {
                let item = json_to_toml_item(v)?;
                let toml_value: TomlValue = item.into_value().map_err(|_| {
                    anyhow::anyhow!("array element {v} is not a scalar; nested tables in arrays")
                })?;
                array.push(toml_value);
            }
            Item::Value(TomlValue::Array(array))
        }
        Value::Object(_) => {
            let table = json_to_toml_table(value)?;
            let mut inline = InlineTable::new();
            for (key, item) in table.iter() {
                if let Some(v) = item.as_value() {
                    inline.insert(key, v.clone());
                }
            }
            Item::Value(TomlValue::InlineTable(inline))
        }
    })
}
