//! Configuration loading: the `[embeddings]`, `[adapters]`, `[storage]`, and
//! `[creds.*]` blocks.
//!
//! pond ships built-in defaults, so an instance with no `config.toml` still
//! works. `pond config schema` emits [`DEFAULT_CONFIG_TOML`], the
//! fully-annotated example. Loading layers `config.toml` under the `POND_*`
//! env mirror via figment, so every command also works with no config file
//! at all (spec.md#storage-configless) - URLs + env vars are sufficient.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use url::Url;

/// Parse `"128 MiB"`, `"1 GiB"`, `"500 KiB"`, or a bare byte count. Accepts
/// SI (KB/MB/GB) and binary (KiB/MiB/GiB/TiB) suffixes; treats the bare unit
/// `"B"` and unsuffixed numbers as raw bytes. Tolerant of whitespace and
/// case. The result MUST fit in `usize` (Lance's cache APIs take `usize`).
fn parse_byte_size(raw: &str) -> Result<usize, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("byte-size value is empty".to_owned());
    }
    let split = trimmed
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split);
    let number: f64 = number
        .trim()
        .parse()
        .map_err(|_| format!("byte-size value {raw:?} is not a number"))?;
    if !number.is_finite() || number < 0.0 {
        return Err(format!("byte-size value {raw:?} must be non-negative"));
    }
    let multiplier: f64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1_000.0,
        "kib" => 1_024.0,
        "m" | "mb" => 1_000_000.0,
        "mib" => 1_048_576.0,
        "g" | "gb" => 1_000_000_000.0,
        "gib" => 1_073_741_824.0,
        "tib" => 1_099_511_627_776.0,
        other => {
            return Err(format!(
                "byte-size unit {other:?} not recognized (try MiB / GiB)"
            ));
        }
    };
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes > usize::MAX as f64 {
        return Err(format!("byte-size value {raw:?} overflows usize"));
    }
    Ok(bytes as usize)
}

/// Accept string / integer / float / bool and stringify. The env mirror
/// parses values TOML-ishly, so `POND_CREDS_X_SECRET_ACCESS_KEY=12345`
/// arrives as a number; these fields are strings no matter how they scan.
fn lenient_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Text(String),
        Int(i64),
        Float(f64),
        Bool(bool),
    }
    Ok(
        Option::<Repr>::deserialize(deserializer)?.map(|repr| match repr {
            Repr::Text(value) => value,
            Repr::Int(value) => value.to_string(),
            Repr::Float(value) => value.to_string(),
            Repr::Bool(value) => value.to_string(),
        }),
    )
}

fn deserialize_byte_size_opt<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Bytes(u64),
        Text(String),
    }
    let repr: Option<Repr> = Option::deserialize(deserializer)?;
    match repr {
        None => Ok(None),
        Some(Repr::Bytes(value)) => usize::try_from(value).map(Some).map_err(de::Error::custom),
        Some(Repr::Text(value)) => parse_byte_size(&value).map(Some).map_err(de::Error::custom),
    }
}

/// True when the URL is on the local filesystem. Mirrors Lance's
/// `ObjectStore::is_local` (lance-io/src/object_store.rs:541): the `file` and
/// `file+uring` schemes are local; everything else (incl. `memory://`) is not.
pub fn is_local(url: &Url) -> bool {
    matches!(url.scheme(), "file" | "file+uring")
}

/// Extract the filesystem `PathBuf` for local URLs. `None` for remote.
pub fn local_path(url: &Url) -> Option<PathBuf> {
    if is_local(url) {
        url.to_file_path().ok()
    } else {
        None
    }
}

/// URI string for a child of this location (typically one Lance dataset under
/// the data dir). Trims a single trailing slash on the base, then concatenates
/// with a `/` separator. This keeps `Dataset::open` / `Dataset::write` happy
/// on both filesystem and object-store backends - they want the URI form, not
/// a `url::Url`.
pub fn child_uri(base: &Url, suffix: &str) -> String {
    // For local URLs we strip the `file://` prefix so log lines and error
    // messages render as plain paths (`/srv/pond/sessions.lance`), matching
    // what pond used to emit before the URL migration.
    if let Some(path) = local_path(base) {
        return path.join(suffix).display().to_string();
    }
    format!("{}/{suffix}", base.as_str().trim_end_matches('/'))
}

/// Render a `Url` for human-readable log/diagnostic output: local URLs come
/// back as plain paths (no `file://` prefix, `$HOME` contracted to `~`);
/// remote URLs stay verbatim.
pub fn display(url: &Url) -> String {
    if let Some(path) = local_path(url) {
        contract_home(&path).display().to_string()
    } else {
        url.to_string()
    }
}

/// Build a `Url` from a filesystem path. Convenience for tests and for
/// callers that hold a `PathBuf` already. The path must be
/// absolute (`url::Url::from_file_path` is a hard requirement on Unix); a
/// relative path gets canonicalized via `std::path::absolute` first.
pub fn url_for_path(path: impl AsRef<Path>) -> Result<Url> {
    let path = path.as_ref();
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::path::absolute(path)
            .with_context(|| format!("failed to absolutize {}", path.display()))?
    };
    Url::from_file_path(&absolute).map_err(|()| {
        anyhow!(
            "failed to convert path {} into a file:// URL",
            absolute.display()
        )
    })
}

/// Default `config.toml` body emitted by `pond config schema`. Every
/// line is commented: pond ships built-in defaults, so the file is purely a
/// discoverable template and pond still works with no `config.toml` on disk.
pub const DEFAULT_CONFIG_TOML: &str = "\
# pond configuration.
#
# pond ships built-in defaults, so every setting here is optional - delete this
# file and pond still works. Uncomment and edit to override.

# Where pond looks for adapter data to import. One entry per adapter type
# (`claude-code`, `codex-cli`, ...). `pond sync` with no arguments syncs every
# entry; `pond sync <adapter>` syncs just one. With an empty `[adapters]`,
# `pond sync` runs an interactive discovery against the known default paths
# and writes the picks back here.
#
# Future wrap: pond is single-namespace in v1 (spec.md#wire-namespace-resolution); `[adapters]` is
# flat here. When multi-namespace pond lands, adapter registration becomes
# per-tenant under `[namespaces.<ns>.adapters.<adapter>]`. Pre-v1 the schema
# is breakable; the rename is operationally free until a real second tenant
# exists.
#
# [adapters.claude-code]
# enabled = true
# path = \"~/.claude/projects\"
#
# [adapters.codex-cli]
# enabled = true
# path = \"~/.codex/sessions\"
#
# Set `enabled = false` to keep the section but skip it on `pond sync`;
# re-enable via `pond adapters enable <adapter>`.

# Embeddings. Search defaults to the vector arm (matching on meaning) when the
# store has any vectors, falling back to FTS otherwise - the model loads lazily
# on the first vector query, so there's no cost on FTS-only corpora. `model`
# selects the HuggingFace XLM-RoBERTa model; `dim` declares its output width and
# is baked into the messages.vector schema on table creation - it must equal the
# model's hidden_size.
#
# Common pairings:
#   model = \"intfloat/multilingual-e5-small\"   dim = 384   (default)
#   model = \"intfloat/multilingual-e5-base\"    dim = 768
#   model = \"intfloat/multilingual-e5-large\"   dim = 1024
#
# A different-dim model needs a fresh data dir; pond enforces this at the
# schema boundary.
#
# [embeddings]
# model = \"intfloat/multilingual-e5-small\"
# dim = 384

# Search tuning. Leave unset for Lance defaults; set when tuning vector recall
# against a corpus.
#
# [search]
# nprobes = 16

# Storage maintenance. Tunes the compaction + cleanup pass that runs inside
# `pond sync` and `pond optimize`.
#
# - `compaction_fragment_cap` is the per-task fragment-count backstop: a
#   planned compaction task touching at least this many fragments always runs
#   even when the write-amplification veto would skip it. Default 64; 0
#   disables the veto and runs every task Lance plans.
# - `cleanup_older_than` is the manifest-retention window for the safe cleanup
#   pass. Accepts `Ns` / `Nm` / `Nh` / `Nd` (default `1d`, floor `1h` - it is
#   what protects in-flight readers). Versions older than this are reclaimed
#   by Lance's OCC-coordinated GC.
#
# [maintenance]
# compaction_fragment_cap = 64
# cleanup_older_than = \"1d\"

# Long-running process caps. Both accept either a plain byte count or a
# humansize-style suffix (\"128 MiB\", \"1 GiB\"). Both are optional - leave
# unset to let pond pick the backend-aware default:
#   local FS  : index_cache = 256 MiB, metadata_cache = 128 MiB
#   remote    : index_cache = 2 GiB,   metadata_cache = 512 MiB
# Lance's library defaults (6 GiB / 1 GiB) are too generous for a per-session
# `pond mcp` process; tightening them is what keeps RSS under the 500 MiB target
# without measurable latency regressions on typical agent-history corpora.
#
# [runtime]
# index_cache_bytes    = \"256 MiB\"
# metadata_cache_bytes = \"128 MiB\"

# Storage address and credentials (spec.md#storage-url-grammar).
#
# `path` is the default destination used when `--storage-path` (env
# `POND_STORAGE_PATH`) is not passed. Absent = the platform-local data dir.
# Addresses are URLs; the `s3+https` form carries the endpoint, bucket, and
# prefix in one token:
#
#   /abs/path or ~/path                  local filesystem
#   s3://bucket/prefix                   AWS S3 (ambient credential chain)
#   s3+https://host/bucket/prefix        S3-compatible endpoint (Hetzner, R2, B2, MinIO)
#   gs://bucket/prefix                   Google Cloud Storage
#   az://account/container/prefix        Azure Blob
#
# Credentials live in `[creds.<name>]` sets and bind to URLs by `scope`
# prefix - longest match wins (spec.md#creds-scope-match); a set without
# `scope` matches any URL. With no matching set, the standard cloud SDK
# chain applies (AWS_* env, shared credentials file, instance metadata).
# Secrets never go in URLs or CLI flags; besides inline values,
# `access_key_id_file` / `secret_access_key_file` read a file and
# `secret_access_key_command` runs a command (e.g. `op read ...`). `extra`
# holds verbatim `object_store` options pond has not typed.
#
# Every field mirrors to env: `POND_STORAGE_PATH`, `POND_CREDS_<NAME>_<FIELD>`
# (set names are lowercase alphanumeric, so the env grammar is unambiguous).
# Precedence: CLI flag > POND_* env > this file > ambient cloud chain.
# Probe a destination end-to-end with `pond storage check`.
#
# Future wrap: pond is single-namespace in v1 (spec.md#wire-namespace-resolution);
# `[storage]` is flat here on the assumption of one bucket per pond. When
# multi-namespace pond lands this becomes `[namespaces.<ns>.storage]`.
#
# [storage]
# path = \"s3+https://nbg1.your-objectstorage.com/my-pond\"
#
# [creds.default]
# access_key_id     = \"...\"
# secret_access_key = \"...\"

# `pond share <session-id>` publishing destination - a public bucket, distinct
# from `[storage]`. Give it its own `[creds.share]` set scoped to `bucket` so
# publishing credentials stay separate from the private data-store credentials.
# No POND_* env mirror for this section (file-only, unlike [storage]/[creds]).
#
# [share]
# provider        = \"bucket\"
# bucket          = \"s3+https://acct.r2.cloudflarestorage.com/pond-shares\"
# public_base_url = \"https://shares.example.com\"
# viewer          = \"html\"
# max_inline_image_bytes = \"2 MiB\"
#
# [creds.share]
# scope             = \"s3+https://acct.r2.cloudflarestorage.com/pond-shares\"
# access_key_id     = \"...\"
# secret_access_key = \"...\"
";

/// Top-level `config.toml` shape.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    /// `[adapters.<adapter>]` map: per-adapter config blobs the matching
    /// factory deserializes inside its `open()`. The shape is adapter-defined
    /// (filesystem adapters expect `{ path = "..." }`; API-backed adapters
    /// expect endpoint + auth keys), so this layer stays opaque. Empty by
    /// default; `pond sync` runs discovery into this map on first use.
    #[serde(default)]
    pub adapters: BTreeMap<String, Value>,
    /// `[storage]`: the default destination URL (spec.md#storage-url-grammar).
    /// `None` = the platform-local data dir.
    #[serde(default)]
    pub storage: StorageConfig,
    /// `[share]`: `pond share`'s publishing destination. Distinct from
    /// `[storage]` - shares go to a public bucket, not the private data store.
    #[serde(default)]
    pub share: ShareConfig,
    /// `[creds.<name>]`: URL-scoped credential sets. Every storage URL
    /// resolves its own set by longest-prefix `scope` match
    /// (spec.md#creds-scope-match); the resolver lives in `pond::substrate`.
    /// `[share].bucket` resolves against this same map (typically a distinct
    /// `[creds.share]` entry, scoped to the share bucket).
    #[serde(default)]
    pub creds: BTreeMap<String, CredsSet>,
}

/// `[storage]`: the single default destination. Typed so the legacy
/// passthrough map (ENV-style `object_store` keys) fails loudly with the
/// rewrite recipe instead of silently changing meaning.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default)]
    pub path: Option<String>,
}

/// One `[creds.<name>]` set. All fields optional; validation enforces at most
/// one variant per logical secret. `extra` carries verbatim `object_store`
/// options pond has not typed (redaction in `pond config show` still applies
/// to its keys by name).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredsSet {
    /// URL prefix this set binds to. `None` = the catch-all set (at most one).
    #[serde(default)]
    pub scope: Option<String>,
    // Key / region fields are `lenient_string`: the env mirror parses values
    // TOML-ishly, so an all-digit key or region arrives as a number and must
    // still land in these String fields.
    #[serde(default, deserialize_with = "lenient_string")]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub access_key_id_file: Option<PathBuf>,
    #[serde(default, deserialize_with = "lenient_string")]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub secret_access_key_file: Option<PathBuf>,
    #[serde(default)]
    pub secret_access_key_command: Option<String>,
    #[serde(default, deserialize_with = "lenient_string")]
    pub region: Option<String>,
    #[serde(default)]
    pub virtual_hosted_style_request: Option<bool>,
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// `[creds.<name>]` name charset `[a-z][a-z0-9]{0,15}` (spec.md#storage-env-mirror):
/// lowercase-alphanumeric keeps `POND_CREDS_<NAME>_<FIELD>` splittable at the
/// first `_` after the name. Shared by config validation and `pond creds`.
pub fn valid_creds_set_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && name.len() <= 16
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// The rejection message for a name that fails [`valid_creds_set_name`], shared
/// by config validation and `pond creds` so the rule and its wording never drift.
pub fn creds_set_name_error(name: &str) -> String {
    format!(
        "creds set name {name:?} must match [a-z][a-z0-9]{{0,15}} (lowercase alphanumeric, no separators)"
    )
}

/// `[share]`: `pond share`'s publishing destination. All fields optional -
/// `pond share` bails with a clear message if `bucket`/`public_base_url` are
/// unset when it actually needs them (mirrors `[storage]`, which is likewise
/// unvalidated at load time). File-only: unlike `[storage]`/`[creds.*]`, this
/// section has no `POND_*` env mirror (`env_mirror` only recognizes
/// `storage_path` and `creds_*` - see spec.md#storage-env-mirror).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ShareConfig {
    /// Publishing backend. Only `bucket` is implemented; the type exists so
    /// an unimplemented value fails at config-load with serde's own
    /// "unknown variant" error instead of a runtime `bail!`.
    pub provider: Option<ShareProvider>,
    /// Public bucket URL (same grammar as `[storage].path`), resolved against
    /// `[creds.<name>]` exactly like any other storage address.
    pub bucket: Option<String>,
    /// Public origin that serves `bucket`'s contents, e.g.
    /// `https://shares.example.com`. Combined with the generated share id to
    /// form the printed URL: `{public_base_url}/{id}.{ext}`.
    pub public_base_url: Option<String>,
    /// Artifact format. Only `html` is implemented in v1.
    #[serde(default)]
    pub viewer: ShareViewer,
    /// Per-image cap for inline `data:` URIs in the HTML renderer; images
    /// over this size fall back to a placeholder instead of ballooning the
    /// artifact. Accepts the same forms as `[runtime]`'s byte-size fields
    /// (`"2 MiB"` or a bare integer). `None` = a 2 MiB built-in default.
    #[serde(default, deserialize_with = "deserialize_byte_size_opt")]
    pub max_inline_image_bytes: Option<usize>,
}

/// `[share].provider` values. Single-variant today; `GitHubPagesPublisher` /
/// `SelfHostedPublisher` / `HostedPublisher` land as new variants later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareProvider {
    #[default]
    Bucket,
}

/// `[share].viewer` values. Single-variant today; `JsonRenderer` (hosted
/// viewer) lands as a new variant later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShareViewer {
    #[default]
    Html,
}

/// `[runtime]`: long-running process caps. Both knobs accept either a plain
/// byte count or a `humansize`-style suffix (`"128 MiB"`, `"1 GiB"`). Both are
/// optional - `None` lets `pond::substrate` pick the backend-aware default
/// (local FS gets a tight cap; object stores stay near Lance's defaults).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RuntimeConfig {
    #[serde(default, deserialize_with = "deserialize_byte_size_opt")]
    pub index_cache_bytes: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_byte_size_opt")]
    pub metadata_cache_bytes: Option<usize>,
}

/// `[search]`: optional Lance vector-query tuning knobs.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchConfig {
    #[serde(default)]
    pub nprobes: Option<usize>,
}

/// `[maintenance]`: storage-maintenance knobs shared by `pond sync` and
/// `pond optimize`. All optional - omit and pond falls back to the
/// in-process defaults in `pond::substrate` (`DEFAULT_COMPACTION_FRAGMENT_CAP`,
/// `default_cleanup_older_than`).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Sub-target fragment count past which the compaction phase runs (it also
    /// runs once those fragments hold a whole target fragment's worth of rows).
    /// Default 64 stops the automated sync re-compacting the trailing fragment
    /// every pass; 0 compacts every pass.
    #[serde(default)]
    pub compaction_fragment_cap: Option<usize>,
    /// Manifest-retention window for the safe cleanup pass. Accepts
    /// `Ns`/`Nm`/`Nh`/`Nd` (default `1d`). Versions older than this are
    /// reclaimed by Lance's OCC-coordinated GC (`delete_unverified=false`),
    /// which never races a concurrent writer on any backend.
    #[serde(default)]
    pub cleanup_older_than: Option<String>,
}

/// `[embeddings]`: model selector and vector dimension. There is no master
/// switch - a `vector` search degrades to FTS when no vectors exist in the
/// store (`has_embeddings()` is the only gate); the candle/Metal model is
/// `LazyEmbedder`-loaded on the first query that
/// actually needs it. `model` and `dim` are installed into the process at
/// startup via `embed::init_model_id` / `sessions::init_embedding_dim`, so
/// swapping models for a one-off experiment is a temporary config file - no
/// CLI flag and no per-call-site plumbing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EmbeddingsConfig {
    /// The embedding model id (spec.md#search): any XLM-RoBERTa model loadable
    /// by `candle-transformers`. Defaults to `intfloat/multilingual-e5-small`.
    pub model: String,
    /// Output dimension of `model`. Must equal the model's `hidden_size`.
    /// Defaults to 384 (e5-small). Set to 768 for e5-base, 1024 for e5-large.
    pub dim: usize,
    /// Whether `pond sync` embeds each new message inline at ingest. Off by
    /// default: sync is a cheap message backup that writes null vectors, so it
    /// never downloads the ~500 MB model, never touches the GPU, and never
    /// blocks on embedding - it just captures your conversations. `pond
    /// optimize` is the manual embed step that backfills those null-vector rows
    /// later. Until it runs, search still works but degrades to keyword/FTS for
    /// the un-embedded rows (vector search only scores rows that carry a
    /// vector). Set `true` to restore embed-at-ingest, so every synced row is
    /// immediately vector-searchable at the cost of the model download and the
    /// per-message embedding work on the sync's critical path.
    pub embed_on_sync: bool,
}

impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self {
            model: crate::embed::DEFAULT_MODEL_ID.to_owned(),
            dim: crate::sessions::DEFAULT_EMBEDDING_DIM,
            embed_on_sync: false,
        }
    }
}

/// The platform-local default storage path, used when neither
/// `--storage-path` / `POND_STORAGE_PATH` nor `[storage].path` is set:
/// `$XDG_DATA_HOME/pond`, then `$HOME/.local/share/pond`, then `.pond`.
/// `xdg_data_home` is honored only if absolute, per the XDG base-directory
/// spec.
pub fn default_storage_path(xdg_data_home: Option<PathBuf>, home: Option<PathBuf>) -> Result<Url> {
    if let Some(xdg) = xdg_data_home.filter(|path| path.is_absolute()) {
        return url_for_path(xdg.join("pond"));
    }
    if let Some(home) = home {
        return url_for_path(home.join(".local").join("share").join("pond"));
    }
    // No HOME and no usable XDG var - stay usable rather than panic.
    url_for_path(PathBuf::from(".pond"))
}

/// Cache dir for rebuildable artifacts (the search row meta map): the XDG-cache
/// analog of [`default_storage_path`]. Separate root because the contents are
/// regenerated from the store, not durable data.
pub fn default_cache_path(xdg_cache_home: Option<PathBuf>, home: Option<PathBuf>) -> PathBuf {
    if let Some(xdg) = xdg_cache_home.filter(|path| path.is_absolute()) {
        return xdg.join("pond");
    }
    if let Some(home) = home {
        return home.join(".cache").join("pond");
    }
    PathBuf::from(".pond-cache")
}

/// Local default path for `config.toml`. URI-backed data dirs always land
/// here because the config file has to be local (it names the bucket and
/// any creds). XDG hierarchy: `$XDG_CONFIG_HOME/pond/config.toml`, then
/// `$HOME/.config/pond/config.toml`, then `.pond.toml` in cwd.
pub fn default_config_path(xdg_config_home: Option<PathBuf>, home: Option<PathBuf>) -> PathBuf {
    if let Some(xdg) = xdg_config_home.filter(|path| path.is_absolute()) {
        return xdg.join("pond").join("config.toml");
    }
    if let Some(home) = home {
        return home.join(".config").join("pond").join("config.toml");
    }
    PathBuf::from(".pond.toml")
}

impl Config {
    /// Load `config.toml` from `path` (if it exists) layered under the
    /// `POND_*` env mirror, and validate. A missing file yields the built-in
    /// defaults - env vars alone are a complete config
    /// (spec.md#storage-configless). On success the resolved embedding model
    /// id + dim are installed into the process (`OnceLock`-backed; only the
    /// first call per process sticks), so all downstream code paths see a
    /// consistent pair without per-handler plumbing.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::load_with_provenance(path)?.0)
    }

    /// [`Config::load`] over an in-memory TOML body (still layered under the
    /// `POND_*` env mirror). `pond init` uses this to validate and resolve
    /// the config it is composing BEFORE anything touches disk - the wizard
    /// writes exactly once, at the end.
    pub fn load_str(body: &str) -> Result<Self> {
        let figment = Figment::new().merge(Toml::string(body)).merge(env_mirror());
        let config: Self = figment
            .extract_lossy()
            .map_err(|error| anyhow!("failed to load config: {error}"))?;
        config.embeddings.validate()?;
        config.validate_creds()?;
        Ok(config)
    }

    /// [`Config::load`] that also returns the figment, so `pond config show`
    /// can attribute each value to its source layer (file / env / default).
    pub fn load_with_provenance(path: impl AsRef<Path>) -> Result<(Self, Figment)> {
        let path = path.as_ref();
        let figment = Figment::new().merge(Toml::file(path)).merge(env_mirror());
        // `extract_lossy`, not `extract`: env values parse TOML-ishly, so an
        // all-digit secret would arrive as a number and fail the String field;
        // lossy stringifies scalars instead.
        let config: Self = figment.extract_lossy().map_err(|error| {
            if let Some(recipe) = detect_legacy_storage(path) {
                return anyhow!("{recipe}");
            }
            if let Some(recipe) = detect_legacy_sources(path) {
                return anyhow!("{recipe}");
            }
            // Inline figment's message (it already names the failing key and
            // source layer) so single-line error surfaces keep the detail.
            anyhow!("failed to load config {}: {error}", path.display())
        })?;
        config.embeddings.validate()?;
        config.validate_creds()?;
        config.embeddings.install_runtime();
        // Tilde expansion is per-adapter (inside each factory's `open()`):
        // an API-backed adapter has no path to expand, and only the
        // filesystem-shaped adapters need the helper. See `expand_home_under`.
        Ok((config, figment))
    }

    /// `[creds.*]` structural rules (spec.md#creds-scope-match): set-name
    /// charset, at most one variant per logical secret, at most one
    /// scope-less set, no duplicate scopes. All parse-time so a misbinding
    /// dies before any URL resolves against it.
    fn validate_creds(&self) -> Result<()> {
        let mut scopeless: Option<&str> = None;
        let mut scopes: BTreeMap<String, &str> = BTreeMap::new();
        for (name, set) in &self.creds {
            if !valid_creds_set_name(name) {
                bail!(creds_set_name_error(name));
            }
            if set.access_key_id.is_some() && set.access_key_id_file.is_some() {
                bail!("[creds.{name}] sets both access_key_id and access_key_id_file; pick one");
            }
            let secret_variants = [
                set.secret_access_key.is_some(),
                set.secret_access_key_file.is_some(),
                set.secret_access_key_command.is_some(),
            ]
            .iter()
            .filter(|present| **present)
            .count();
            if secret_variants > 1 {
                bail!(
                    "[creds.{name}] sets more than one of secret_access_key / secret_access_key_file / secret_access_key_command; pick one"
                );
            }
            match set.scope.as_deref() {
                None => {
                    if let Some(other) = scopeless {
                        bail!(
                            "[creds.{other}] and [creds.{name}] are both scope-less; at most one catch-all set is allowed - add a `scope` to one"
                        );
                    }
                    scopeless = Some(name);
                }
                Some(scope) => {
                    // Duplicates are checked on the canonical form (incl.
                    // trailing-slash trim, matching scope-match semantics),
                    // so two spellings of one prefix can never tie at
                    // resolve time.
                    let canonical = crate::substrate::parse_scope(scope)
                        .map(|url| url.as_str().trim_end_matches('/').to_owned())
                        .with_context(|| {
                            format!("[creds.{name}] scope {scope:?} is not a valid URL prefix")
                        })?;
                    if let Some(other) = scopes.insert(canonical, name) {
                        bail!(
                            "[creds.{other}] and [creds.{name}] declare the same scope {scope:?}; merge them or narrow one"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Resolve the `[adapters.<adapter>]` entries to drive `pond sync`. Only
    /// sections with `enabled = true` flow through; sections with
    /// `enabled = false` (or absent) are treated as opt-out and the
    /// per-adapter blob (minus `enabled`) is handed to the factory's
    /// `open()`. With `adapter = None` returns every enabled entry; with
    /// `Some(name)` returns just that one - and errors if it's not in
    /// config OR if it's currently disabled (the caller should then
    /// re-prompt or report).
    pub fn resolve_adapters(&self, adapter: Option<&str>) -> Result<Vec<(String, Value)>> {
        match adapter {
            None => Ok(self
                .adapters
                .iter()
                .filter_map(|(name, blob)| take_enabled(name, blob))
                .collect()),
            Some(name) => {
                let blob = self
                    .adapters
                    .get(name)
                    .ok_or_else(|| anyhow!("no [adapters.{name}] entry in config"))?;
                take_enabled(name, blob).map(|entry| vec![entry]).ok_or_else(|| {
                    anyhow!(
                        "adapter [{name}] is disabled (enabled = false); run `pond adapters enable {name}` to re-enable, then `pond sync {name}`"
                    )
                })
            }
        }
    }

    /// Names that are configured but currently `enabled = false`. Used by
    /// `pond sync` post-import to know not to re-probe an adapter the user
    /// already declined (the decline persists; re-prompt only via the
    /// positional override `pond sync <name>`).
    pub fn disabled_adapter_names(&self) -> Vec<&str> {
        self.adapters
            .iter()
            .filter_map(|(name, blob)| {
                let enabled = blob
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if enabled { None } else { Some(name.as_str()) }
            })
            .collect()
    }
}

/// The `POND_*` env mirror (spec.md#storage-env-mirror): `POND_STORAGE_PATH`
/// -> `storage.path`, `POND_CREDS_<NAME>_<FIELD>` -> `creds.<name>.<field>`.
/// Filtered to exactly those two shapes - clap owns its own `POND_*` vars
/// (`POND_CONFIG_FILE`, `POND_HOST`, ...) and an unfiltered prefix would turn each
/// of them into an unknown-field error here.
fn env_mirror() -> Env {
    // Keys reach these closures pre-lowercasing (`CREDS_...`), so compare on
    // an ascii-lowered copy; `str::starts_with` is case-sensitive.
    Env::prefixed("POND_")
        .filter(|key| {
            let key = key.as_str().to_ascii_lowercase();
            // `extra` has no env form (spec.md#storage-env-mirror): the env
            // grammar stays flat strings; structured options belong in the
            // file (or URL query params).
            key == "storage_path" || (key.starts_with("creds_") && !key.ends_with("_extra"))
        })
        .map(|key| {
            // Set names are lowercase alphanumeric (validate_creds), so the
            // first `_` after `creds` and the one after the name are the only
            // separators; field names keep their underscores.
            let key = key.as_str().to_ascii_lowercase();
            let dots = if key.starts_with("creds_") { 2 } else { 1 };
            key.replacen('_', ".", dots).into()
        })
}

/// The pre-redesign `[storage]` passthrough keys, by role (ENV-style
/// `object_store` aliases). Both the load-time error recipe
/// (`detect_legacy_storage`) and the `pond init` rewrite read these, so the
/// legacy vocabulary lives in one place - a new alias must not require
/// editing two detectors in lockstep.
pub const LEGACY_ENDPOINT_KEYS: &[&str] = &["aws_endpoint", "endpoint"];
pub const LEGACY_ACCESS_KEY_KEYS: &[&str] = &["aws_access_key_id", "access_key_id"];
pub const LEGACY_SECRET_KEY_KEYS: &[&str] = &["aws_secret_access_key", "secret_access_key"];
pub const LEGACY_VIRTUAL_HOSTED_KEYS: &[&str] = &[
    "aws_virtual_hosted_style_request",
    "virtual_hosted_style_request",
];

/// Recognize the pre-redesign `[storage]` passthrough map (ENV-style
/// `object_store` keys) and return the exact rewrite onto `[storage].path` +
/// `[creds.default]`. An error with a recipe, not a shim: old configs do not
/// keep working.
fn detect_legacy_storage(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&text).ok()?;
    let storage = value.get("storage")?.as_table()?;
    if storage.is_empty() || storage.keys().all(|key| key == "path") {
        return None;
    }
    let get = |names: &[&str]| {
        storage.iter().find_map(|(key, value)| {
            names
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
                .then(|| value.as_str().unwrap_or_default().to_owned())
        })
    };
    let endpoint = get(LEGACY_ENDPOINT_KEYS);
    let host = endpoint
        .as_deref()
        .and_then(|e| e.split("://").nth(1))
        .unwrap_or("<endpoint-host>");
    // Under the declared virtual-hosted style the endpoint host leads with
    // the bucket; de-fold it, or following the recipe verbatim folds the
    // bucket in twice (the new grammar re-applies virtual hosting).
    let virtual_hosted = storage.iter().any(|(key, value)| {
        LEGACY_VIRTUAL_HOSTED_KEYS
            .iter()
            .any(|name| key.eq_ignore_ascii_case(name))
            && (value.as_bool().unwrap_or(false)
                || value
                    .as_str()
                    .is_some_and(|text| text.eq_ignore_ascii_case("true") || text == "1"))
    });
    let path_recipe = match host.split_once('.') {
        Some((bucket, rest)) if virtual_hosted && rest.contains('.') => {
            format!("s3+https://{rest}/{bucket}/<prefix>")
        }
        _ => format!("s3+https://{host}/<bucket>/<prefix>"),
    };
    // spec.md#storage-redaction: never echo credential values, even back to
    // their owner - stderr lands in logs, scrollback, and pasted bug reports.
    let mut recipe = format!(
        "config {} uses the old [storage] passthrough map; rewrite it as:\n\n[storage]\npath = \"{path_recipe}\"\n\n[creds.default]\n",
        path.display(),
    );
    recipe.push_str("access_key_id     = \"...\"  # copy from the old [storage] section\n");
    recipe.push_str("secret_access_key = \"...\"  # copy from the old [storage] section\n");
    recipe.push_str(
        "\n(the endpoint and bucket fold into the URL; allow_http is scheme-derived; virtual-hosted addressing defaults on; the region is autodetected - append ?region=<x> to the URL only if your store insists. `pond storage check` verifies the result end-to-end, and `pond init` can apply this rewrite for you)",
    );
    Some(recipe)
}

/// Recognize a pre-rename `[sources.<name>]` config block (the adapter map was
/// renamed `sources` -> `adapters`) and return a one-line recipe pointing at
/// `pond init`. An error with a recipe, not a shim: old configs do not silently
/// keep working. Transitional - delete once live configs have migrated.
fn detect_legacy_sources(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&text).ok()?;
    value.get("sources")?.as_table()?;
    Some(format!(
        "config {} uses a [sources.*] block; the adapter map was renamed to [adapters.*]. Run `pond init` to migrate it, or rename each `[sources.<name>]` header to `[adapters.<name>]` by hand.",
        path.display(),
    ))
}

/// Inner helper: return `Some((name, blob))` when the adapter section is
/// enabled, stripping the discriminator from the blob before handing it on;
/// `None` when the section is missing `enabled` or has `enabled = false`.
fn take_enabled(name: &str, blob: &Value) -> Option<(String, Value)> {
    let enabled = blob
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !enabled {
        return None;
    }
    let mut clean = blob.clone();
    if let Some(obj) = clean.as_object_mut() {
        obj.remove("enabled");
    }
    Some((name.to_owned(), clean))
}

/// Expand `~` and `$VAR`/`${VAR}` in `path` against an explicit `home`.
/// Filesystem-shaped adapters call this from inside their factory's `open()`.
/// Tests use it directly to exercise the rule without mutating the
/// process-wide `HOME` env var (`std::env::set_var` is `unsafe` under
/// edition 2024 and pond forbids unsafe code). Unset vars and `~user` forms
/// pass through unchanged - never guess.
pub fn expand_home_under(path: &Path, home: &Path) -> PathBuf {
    let Some(text) = path.to_str() else {
        return path.to_path_buf();
    };
    let home_text = home.to_string_lossy();
    let expanded = shellexpand::full_with_context_no_errors(
        text,
        || Some(home_text.clone()),
        |var| std::env::var(var).ok(),
    );
    PathBuf::from(expanded.as_ref())
}

/// The inverse of [`expand_home_under`] for display and config writes:
/// contract a `home` prefix back to `~` so user-facing surfaces (and the
/// paths `pond init` persists) stay portable and readable. Non-home paths
/// pass through unchanged.
pub fn contract_home_under(path: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix(home) {
        Ok(rest) if rest.as_os_str().is_empty() => PathBuf::from("~"),
        Ok(rest) => Path::new("~").join(rest),
        Err(_) => path.to_path_buf(),
    }
}

/// [`contract_home_under`] against the process `HOME`. Returns the input
/// rendered for humans; machine surfaces (JSON output, the wire) keep
/// absolute paths.
pub fn contract_home(path: &Path) -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => contract_home_under(path, Path::new(&home)),
        None => path.to_path_buf(),
    }
}

impl EmbeddingsConfig {
    /// Surface-level validation: model id non-empty and dim positive. The
    /// dim/model mismatch is the load-time check inside `CandleEmbedder::load`,
    /// which knows the model's `hidden_size`.
    pub fn validate(&self) -> Result<()> {
        if self.model.trim().is_empty() {
            bail!("embeddings.model must be a non-empty HuggingFace model id");
        }
        if self.dim == 0 {
            bail!("embeddings.dim must be positive; got {}", self.dim);
        }
        Ok(())
    }

    /// Install model id + dim into the process. Idempotent: only the first
    /// call sticks (matches `OnceLock` semantics in `embed::init_model_id` and
    /// `sessions::init_embedding_dim`).
    pub fn install_runtime(&self) {
        crate::embed::init_model_id(self.model.clone());
        crate::sessions::init_embedding_dim(self.dim);
    }
}

/// Write `config.toml` with owner-only perms (0600). The file can carry a
/// plaintext `secret_access_key` (inline `[creds.*]`), so it must never be
/// group/world-readable - matching the AWS CLI's 0600 on its credentials file.
/// Unix only; Windows is out of v1 scope. Order is truncate -> chmod -> write,
/// so the secret is only ever written once perms are already 0600, even when
/// repairing a pre-existing 0644 file.
pub fn write_config_file(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("failed to write {}", path.display()))?;
        // `.mode()` applies only on creation; chmod also repairs a pre-existing file.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod 0600 {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)
            .with_context(|| format!("failed to write {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // `result_large_err`: `figment::Jail` closures return `figment::Error`
    // by contract; the size is figment's, not ours.
    #![allow(clippy::expect_used, clippy::unwrap_used, clippy::result_large_err)]

    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    #[cfg(unix)]
    #[test]
    fn write_config_file_is_owner_only_0600() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        // A pre-existing world-readable file must be repaired, not left at 0644.
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_config_file(&path, "[creds.default]\nsecret_access_key = \"x\"\n").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "config with secrets must be owner-only");
        assert!(
            std::fs::read_to_string(&path)
                .unwrap()
                .contains("secret_access_key")
        );
    }

    #[test]
    fn validate_catches_empty_model_and_bad_dim() {
        assert!(EmbeddingsConfig::default().validate().is_ok());
        // Empty / whitespace-only model id is rejected: HuggingFace fetch
        // would fail far away from the config error.
        let bad_model = EmbeddingsConfig {
            model: "   ".to_owned(),
            dim: 768,
            embed_on_sync: false,
        };
        assert!(bad_model.validate().is_err());
        // Non-multiple-of-8 dims are accepted now: IVF_SQ has no subspace
        // stride, so the old `dim % 8` requirement is gone.
        let odd_dim = EmbeddingsConfig {
            model: "intfloat/multilingual-e5-base".to_owned(),
            dim: 100,
            embed_on_sync: false,
        };
        assert!(odd_dim.validate().is_ok());
        // Zero is still rejected.
        let zero_dim = EmbeddingsConfig {
            model: "intfloat/multilingual-e5-base".to_owned(),
            dim: 0,
            embed_on_sync: false,
        };
        assert!(zero_dim.validate().is_err());
    }

    #[test]
    fn config_load_missing_file_falls_back_to_builtin() {
        let config = Config::load("/nonexistent/pond-config-xyz.toml").unwrap();
        assert_eq!(config.embeddings, EmbeddingsConfig::default());
    }

    #[test]
    fn default_config_toml_loads_to_the_builtin_defaults() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, DEFAULT_CONFIG_TOML).unwrap();
        // The shipped template is all comments, so it must load and validate as
        // the built-in defaults - a malformed template fails right here.
        let config = Config::load(&path).unwrap();
        assert_eq!(config.embeddings, EmbeddingsConfig::default());
        assert_eq!(config.embeddings.model, crate::embed::DEFAULT_MODEL_ID);
        assert_eq!(
            config.embeddings.dim,
            crate::sessions::DEFAULT_EMBEDDING_DIM
        );
    }

    #[test]
    fn default_storage_path_follows_xdg_then_home() {
        // An absolute XDG_DATA_HOME wins.
        let resolved =
            default_storage_path(Some(PathBuf::from("/xdg")), Some(PathBuf::from("/home")))
                .unwrap();
        assert!(is_local(&resolved));
        assert_eq!(local_path(&resolved).unwrap(), PathBuf::from("/xdg/pond"));

        // A relative XDG_DATA_HOME is ignored per the XDG spec; HOME is the fallback.
        let resolved = default_storage_path(
            Some(PathBuf::from("relative")),
            Some(PathBuf::from("/home")),
        )
        .unwrap();
        assert_eq!(
            local_path(&resolved).unwrap(),
            PathBuf::from("/home/.local/share/pond"),
        );

        // No XDG and no HOME - stays usable: returns the cwd-anchored `.pond`.
        // The result is absolute (Lance's URL conversion requires it), so we
        // just check that the URL ends with the relative path's components.
        let resolved = default_storage_path(None, None).unwrap();
        assert!(is_local(&resolved));
        assert!(
            local_path(&resolved).unwrap().ends_with(".pond"),
            "fallback path should end with .pond: {resolved}",
        );
    }

    #[test]
    fn expand_home_under_handles_tilde_forms() {
        let home = Path::new("/srv/me");
        assert_eq!(
            expand_home_under(Path::new("~"), home),
            PathBuf::from("/srv/me")
        );
        assert_eq!(
            expand_home_under(Path::new("~/.codex/sessions"), home),
            PathBuf::from("/srv/me/.codex/sessions"),
        );
        // Absolute paths pass through unchanged.
        assert_eq!(
            expand_home_under(Path::new("/etc/passwd"), home),
            PathBuf::from("/etc/passwd"),
        );
        // A leading `~something` (no slash) is not the home form - leave it.
        assert_eq!(
            expand_home_under(Path::new("~user/elsewhere"), home),
            PathBuf::from("~user/elsewhere"),
        );
    }

    #[test]
    fn expand_home_under_handles_env_vars() {
        // Jail serializes env mutation against the other env-touching tests.
        figment::Jail::expect_with(|jail| {
            jail.set_env("POND_TEST_EXPAND_DIR", "/srv/data");
            let home = Path::new("/srv/me");
            assert_eq!(
                expand_home_under(Path::new("$POND_TEST_EXPAND_DIR/pond"), home),
                PathBuf::from("/srv/data/pond"),
            );
            assert_eq!(
                expand_home_under(Path::new("${POND_TEST_EXPAND_DIR}/pond"), home),
                PathBuf::from("/srv/data/pond"),
            );
            // Unset vars pass through unchanged - never guess.
            assert_eq!(
                expand_home_under(Path::new("$POND_TEST_UNSET_VAR/x"), home),
                PathBuf::from("$POND_TEST_UNSET_VAR/x"),
            );
            Ok(())
        });
    }

    #[test]
    fn contract_home_under_inverts_expansion() {
        let home = Path::new("/srv/me");
        assert_eq!(
            contract_home_under(Path::new("/srv/me/.local/share/pond"), home),
            PathBuf::from("~/.local/share/pond"),
        );
        assert_eq!(
            contract_home_under(Path::new("/srv/me"), home),
            PathBuf::from("~")
        );
        // Non-home paths pass through unchanged.
        assert_eq!(
            contract_home_under(Path::new("/etc/passwd"), home),
            PathBuf::from("/etc/passwd"),
        );
    }

    #[test]
    fn resolve_adapters_returns_one_or_all_or_errors() {
        let temp = TempDir::new().unwrap();
        let body = "\
[adapters.claude-code]
enabled = true
path = \"/srv/claude\"

[adapters.codex-cli]
enabled = true
path = \"/srv/codex\"

[adapters.opencode]
enabled = false
";
        let path = temp.path().join("config.toml");
        std::fs::write(&path, body).expect("write config");
        let config = Config::load(&path).unwrap();

        // None -> only enabled entries
        let all = config.resolve_adapters(None).unwrap();
        assert_eq!(all.len(), 2);
        let names: Vec<_> = all.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"claude-code"));
        assert!(names.contains(&"codex-cli"));
        // The `enabled` discriminator never reaches the adapter blob.
        for (_, blob) in &all {
            assert!(blob.get("enabled").is_none(), "enabled should be stripped");
        }

        // Some(name) -> one entry, opaque JSON blob
        let one = config.resolve_adapters(Some("codex-cli")).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].0, "codex-cli");
        assert_eq!(
            one[0].1.get("path").and_then(Value::as_str),
            Some("/srv/codex"),
        );

        // Disabled positional -> errors with the recovery hint baked in.
        let disabled = config.resolve_adapters(Some("opencode"));
        let err = disabled
            .expect_err("disabled adapter must error")
            .to_string();
        assert!(err.contains("enabled = false"), "got: {err}");
        assert!(err.contains("pond sync opencode"), "got: {err}");

        // Unknown -> error
        assert!(config.resolve_adapters(Some("nope")).is_err());

        // disabled_adapter_names lists exactly the off ones.
        assert_eq!(config.disabled_adapter_names(), vec!["opencode"]);
    }

    #[test]
    fn memory_uri_is_classified_as_remote() {
        let url = Url::parse("memory:///pond-remote-test").expect("memory uri parses");
        assert!(
            !is_local(&url),
            "memory:// is not a local-filesystem URL: {url}",
        );
        assert!(
            local_path(&url).is_none(),
            "local_path must return None for non-file schemes",
        );
    }

    // The storage/creds tests run inside `figment::Jail` even when they set
    // no env vars: the Jail-based env-mirror test mutates process-global env
    // mid-flight, and the Jail lock is what serializes them against it.

    #[test]
    fn storage_and_creds_round_trip() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
[storage]
path = "s3+https://nbg1.example.com/my-pond"

[creds.default]
access_key_id     = "AKIA123"
secret_access_key = "shh"

[creds.work]
scope             = "s3+https://fsn1.example.com/work-pond/"
access_key_id     = "AKIA456"
secret_access_key_command = "op read op://vault/pond/secret"
region            = "fsn1"
virtual_hosted_style_request = false
extra = { request_timeout = "60 seconds" }
"#,
            )?;
            let config = Config::load("config.toml").expect("config loads");
            assert_eq!(
                config.storage.path.as_deref(),
                Some("s3+https://nbg1.example.com/my-pond"),
            );
            assert_eq!(config.creds.len(), 2);
            let work = &config.creds["work"];
            assert_eq!(
                work.secret_access_key_command.as_deref(),
                Some("op read op://vault/pond/secret"),
            );
            assert_eq!(work.virtual_hosted_style_request, Some(false));
            assert_eq!(work.extra["request_timeout"], "60 seconds");
            Ok(())
        });
    }

    #[test]
    fn creds_validators_reject_bad_shapes() {
        let cases: &[(&str, &str)] = &[
            // Unknown key dies loudly (typos must not silently no-op).
            ("[creds.a]\nacces_key_id = \"x\"\n", "acces_key_id"),
            // Name charset: separators break the env-mirror grammar.
            ("[creds.my_set]\naccess_key_id = \"x\"\n", "[a-z][a-z0-9]"),
            ("[creds.A1]\naccess_key_id = \"x\"\n", "[a-z][a-z0-9]"),
            // One variant per logical secret.
            (
                "[creds.a]\nsecret_access_key = \"x\"\nsecret_access_key_command = \"cat\"\n",
                "more than one",
            ),
            (
                "[creds.a]\naccess_key_id = \"x\"\naccess_key_id_file = \"/k\"\n",
                "pick one",
            ),
            // At most one scope-less set.
            (
                "[creds.a]\naccess_key_id = \"x\"\n[creds.b]\naccess_key_id = \"y\"\n",
                "scope-less",
            ),
            // Duplicate scopes can never tie-break - checked canonicalized,
            // so two spellings of one prefix still collide.
            (
                "[creds.a]\nscope = \"s3+https://h:443/b/\"\naccess_key_id = \"x\"\n[creds.b]\nscope = \"s3+https://h/b\"\naccess_key_id = \"y\"\n",
                "same scope",
            ),
        ];
        figment::Jail::expect_with(|jail| {
            for (body, needle) in cases {
                jail.create_file("config.toml", body)?;
                let err = Config::load("config.toml").expect_err(body).to_string();
                assert!(
                    err.contains(needle),
                    "want {needle:?} in error for {body:?}, got: {err}",
                );
            }
            Ok(())
        });
    }

    #[test]
    fn valid_creds_set_name_matches_env_mirror_charset() {
        for ok in ["default", "work", "work2", "a", "abcdefghij123456"] {
            assert!(valid_creds_set_name(ok), "{ok:?} should be valid");
        }
        for bad in ["", "Work", "my_set", "2fast", "abcdefghij1234567", "set-1"] {
            assert!(!valid_creds_set_name(bad), "{bad:?} should be invalid");
        }
    }

    #[test]
    fn legacy_storage_map_errors_with_the_rewrite_recipe() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
[storage]
AWS_ACCESS_KEY_ID = "AKIA123"
AWS_SECRET_ACCESS_KEY = "shh"
AWS_REGION = "nbg1"
AWS_ENDPOINT = "https://ttq.nbg1.your-objectstorage.com"
aws_virtual_hosted_style_request = "true"
"#,
            )?;
            let err = Config::load("config.toml")
                .expect_err("legacy map must error")
                .to_string();
            // The error IS the migration: old keys mapped onto the new shape.
            assert!(err.contains("old [storage] passthrough map"), "got: {err}");
            // The declared virtual-hosted style pins the bucket as the leading
            // host label; the recipe must de-fold it, not repeat the folded
            // host (which the new grammar would fold again).
            assert!(
                err.contains("s3+https://nbg1.your-objectstorage.com/ttq/<prefix>"),
                "recipe must de-fold the virtual-hosted endpoint, got: {err}",
            );
            // spec.md#storage-redaction: the recipe must NOT echo the real
            // key values - placeholders plus a "copy from" pointer only.
            assert!(!err.contains("AKIA123"), "got: {err}");
            assert!(!err.contains("\"shh\""), "got: {err}");
            assert!(err.contains("access_key_id     = \"...\""), "got: {err}");
            // Region is autodetected (AWS) or defaulted (S3-compatible
            // endpoints ignore it): the recipe must not carry AWS_REGION
            // forward, only name the ?region= override.
            assert!(!err.contains("region            ="), "got: {err}");
            assert!(err.contains("?region="), "got: {err}");
            assert!(err.contains("pond storage check"), "got: {err}");
            // Without the addressing-style key the split is unknowable; the
            // recipe keeps the host verbatim with a <bucket> placeholder.
            jail.create_file(
                "config.toml",
                r#"
[storage]
AWS_ACCESS_KEY_ID = "AKIA123"
AWS_ENDPOINT = "https://ttq.nbg1.your-objectstorage.com"
"#,
            )?;
            let err = Config::load("config.toml")
                .expect_err("legacy map must error")
                .to_string();
            assert!(
                err.contains("s3+https://ttq.nbg1.your-objectstorage.com/<bucket>/<prefix>"),
                "got: {err}",
            );
            Ok(())
        });
    }

    #[test]
    fn legacy_sources_block_errors_with_the_adapters_recipe() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                "[sources.claude-code]\nenabled = true\npath = \"/srv/claude\"\n",
            )?;
            let err = Config::load("config.toml")
                .expect_err("legacy [sources.*] must error")
                .to_string();
            assert!(err.contains("[adapters.*]"), "names the new key: {err}");
            assert!(err.contains("pond init"), "points at the fix: {err}");
            Ok(())
        });
    }

    #[test]
    fn env_mirror_layers_over_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
[storage]
path = "/from-file"

[creds.work]
scope         = "s3://file-bucket/"
access_key_id = "from-file"
region        = "file-region"
"#,
            )?;
            // Env beats file per field; untouched fields survive the merge.
            jail.set_env("POND_STORAGE_PATH", "/from-env");
            jail.set_env("POND_CREDS_WORK_ACCESS_KEY_ID", "from-env");
            // A purely-numeric env secret must stay a string (extract_lossy).
            jail.set_env("POND_CREDS_WORK_SECRET_ACCESS_KEY", "12345");
            // A set defined only in env is discovered by the prefix scan.
            jail.set_env("POND_CREDS_CI_ACCESS_KEY_ID", "ci-key");
            let config = Config::load("config.toml").expect("env+file config loads");
            assert_eq!(config.storage.path.as_deref(), Some("/from-env"));
            let work = &config.creds["work"];
            assert_eq!(work.access_key_id.as_deref(), Some("from-env"));
            assert_eq!(work.secret_access_key.as_deref(), Some("12345"));
            assert_eq!(work.region.as_deref(), Some("file-region"));
            assert_eq!(work.scope.as_deref(), Some("s3://file-bucket/"));
            assert_eq!(config.creds["ci"].access_key_id.as_deref(), Some("ci-key"));
            Ok(())
        });
    }

    #[test]
    fn share_config_parses_with_distinct_creds_scope() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
[storage]
path = "s3+https://acct.example.com/pond-data"

[creds.default]
access_key_id     = "data-key"
secret_access_key = "data-secret"

[share]
provider        = "bucket"
bucket          = "s3+https://acct.example.com/pond-shares"
public_base_url = "https://shares.example.com"
viewer          = "html"
max_inline_image_bytes = "2 MiB"

[creds.share]
scope             = "s3+https://acct.example.com/pond-shares"
access_key_id     = "share-key"
secret_access_key = "share-secret"
"#,
            )?;
            let config = Config::load("config.toml").expect("config loads");
            assert_eq!(config.share.provider, Some(ShareProvider::Bucket));
            assert_eq!(
                config.share.bucket.as_deref(),
                Some("s3+https://acct.example.com/pond-shares"),
            );
            assert_eq!(
                config.share.public_base_url.as_deref(),
                Some("https://shares.example.com"),
            );
            assert_eq!(config.share.viewer, ShareViewer::Html);
            assert_eq!(config.share.max_inline_image_bytes, Some(2 * 1024 * 1024));

            // The data URL resolves to [creds.default]; the share URL resolves
            // to the separate, more-specifically-scoped [creds.share] - same
            // resolver, same map, two independent scope matches.
            let data_url = crate::substrate::StorageUrl::parse(
                config.storage.path.as_deref().expect("storage path"),
            )
            .expect("data url parses");
            let data_resolved = data_url.resolve(&config.creds).expect("data resolves");
            assert_eq!(
                data_resolved.options.get("access_key_id").map(String::as_str),
                Some("data-key"),
            );

            let share_url =
                crate::substrate::StorageUrl::parse(config.share.bucket.as_deref().unwrap())
                    .expect("share url parses");
            let share_resolved = share_url.resolve(&config.creds).expect("share resolves");
            assert_eq!(
                share_resolved
                    .options
                    .get("access_key_id")
                    .map(String::as_str),
                Some("share-key"),
            );
            Ok(())
        });
    }

    #[test]
    fn share_config_rejects_unknown_fields_and_bad_viewer() {
        figment::Jail::expect_with(|jail| {
            jail.create_file("config.toml", "[share]\nbuckett = \"typo\"\n")?;
            let err = Config::load("config.toml")
                .expect_err("typo'd field must error")
                .to_string();
            assert!(err.contains("buckett"), "got: {err}");

            jail.create_file("config.toml", "[share]\nviewer = \"json\"\n")?;
            let err = Config::load("config.toml")
                .expect_err("unimplemented viewer must error")
                .to_string();
            assert!(err.contains("json"), "got: {err}");
            Ok(())
        });
    }

    #[test]
    fn share_config_has_no_env_mirror() {
        // [share] is file-only in v1 (env_mirror only recognizes storage_path
        // and creds_* - see the doc comment on ShareConfig). A POND_SHARE_*
        // var must be silently ignored, not error and not override the file.
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                "[share]\nbucket = \"s3://from-file/shares\"\n",
            )?;
            jail.set_env("POND_SHARE_BUCKET", "s3://from-env/shares");
            let config = Config::load("config.toml").expect("config loads");
            assert_eq!(
                config.share.bucket.as_deref(),
                Some("s3://from-file/shares"),
                "POND_SHARE_BUCKET must not reach [share] - no env mirror in v1",
            );
            Ok(())
        });
    }

    #[test]
    fn share_creds_still_get_the_existing_creds_env_mirror() {
        // [creds.share] is just another entry in the existing creds map, so
        // it rides the pre-existing POND_CREDS_<NAME>_<FIELD> mirror for free
        // even though [share] itself has none.
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
[creds.share]
scope         = "s3://shares/"
access_key_id = "from-file"
"#,
            )?;
            jail.set_env("POND_CREDS_SHARE_ACCESS_KEY_ID", "from-env");
            let config = Config::load("config.toml").expect("config loads");
            assert_eq!(
                config.creds["share"].access_key_id.as_deref(),
                Some("from-env"),
            );
            Ok(())
        });
    }
}
