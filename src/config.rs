//! Configuration loading: the embedding-model registry and the background
//! maintenance settings.
//!
//! Built-in defaults are shipped in the binary so a pond instance with no
//! `config.toml` still works; user config adds or overrides entries by `id`.
//! `pond config --print-schema` emits [`DEFAULT_CONFIG_TOML`], the
//! fully-annotated example.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::embed::DEFAULT_BATCH_TOKEN_SQ_BUDGET;

/// Where pond keeps its Lance datasets: a local directory or an object-store
/// URI (`s3://...`, `gs://...`, `az://...`, ...). Lance accepts URIs directly
/// for every supported backend; this type keeps the local-vs-remote branches
/// type-checked rather than string-sniffed at each call site. Load-bearing in
/// three places: opening (only local pre-creates the dir), the `config.toml`
/// default (only local inherits the data dir; URI installs fall back to
/// `$XDG_CONFIG_HOME/pond/config.toml` because the config file is always
/// local - it names the bucket), and display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageLocation {
    LocalPath(PathBuf),
    Uri(String),
}

impl StorageLocation {
    /// URI for a child of this location (typically one Lance dataset under
    /// the data dir). Local paths use `Path::join`; URIs concatenate with a
    /// single `/` separator after trimming any trailing slash on the base.
    pub fn child_uri(&self, suffix: &str) -> String {
        match self {
            Self::LocalPath(path) => path.join(suffix).display().to_string(),
            Self::Uri(uri) => format!("{}/{suffix}", uri.trim_end_matches('/')),
        }
    }

    /// `Some(path)` only when this location is local. Used by the local-only
    /// branches (`create_dir_all`, the `<data_dir>/config.toml` default).
    pub fn local_path(&self) -> Option<&Path> {
        match self {
            Self::LocalPath(path) => Some(path),
            Self::Uri(_) => None,
        }
    }
}

impl FromStr for StorageLocation {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Some((scheme, rest)) = s.split_once("://") {
            // file:// reduces to a local path; everything else (s3, gs, az,
            // abfs, https, ...) is a remote URI. Lance validates the scheme
            // on first dataset open, so we don't gatekeep here.
            if scheme.eq_ignore_ascii_case("file") {
                return Ok(Self::LocalPath(PathBuf::from(rest)));
            }
            return Ok(Self::Uri(s.to_owned()));
        }
        Ok(Self::LocalPath(PathBuf::from(s)))
    }
}

impl std::fmt::Display for StorageLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalPath(path) => path.display().fmt(f),
            Self::Uri(uri) => f.write_str(uri),
        }
    }
}

impl From<PathBuf> for StorageLocation {
    fn from(path: PathBuf) -> Self {
        Self::LocalPath(path)
    }
}

impl From<&Path> for StorageLocation {
    fn from(path: &Path) -> Self {
        Self::LocalPath(path.to_path_buf())
    }
}

impl From<&PathBuf> for StorageLocation {
    fn from(path: &PathBuf) -> Self {
        Self::LocalPath(path.clone())
    }
}

impl From<&StorageLocation> for StorageLocation {
    fn from(location: &StorageLocation) -> Self {
        location.clone()
    }
}

/// Default `config.toml` body emitted by `pond config --print-schema`. Every
/// line is commented: pond ships built-in defaults, so the file is purely a
/// discoverable template and pond still works with no `config.toml` on disk.
pub const DEFAULT_CONFIG_TOML: &str = "\
# pond configuration.
#
# pond ships built-in defaults, so every setting here is optional - delete this
# file and pond still works. Uncomment and edit to override.

# Where pond looks for source data to import. One entry per adapter type
# (`claude-code`, `codex`, ...). `pond sync` with no arguments syncs every
# entry; `pond sync <adapter>` syncs just one. With an empty `[sources]`,
# `pond sync` runs an interactive discovery against the known default paths
# and writes the picks back here.
#
# [sources.claude-code]
# path = \"~/.claude/projects\"
#
# [sources.codex]
# path = \"~/.codex/sessions\"

# Register or tune an embedding model. pond validates each entry against its
# known-model set (model id, dimension, distance metric). `pond serve` and
# `pond mcp` probe the embeddings table at boot and load the model only when
# rows exist for the default identity, so embeddings stay fully opt-in: run
# `pond embed` once to fill the backlog.
#
# [[embeddings.models]]
# id = \"Qwen/Qwen3-Embedding-0.6B\"
# dim = 1024
# max_embed_tokens = 1024
# num_sub_vectors = 64
# distance = \"cosine\"
# normalize = true
# default = true
#
# Per-namespace tunable overrides (immutable fields cannot be overridden):
# [embeddings.overrides.local.\"Qwen/Qwen3-Embedding-0.6B\"]
# max_embed_tokens = 2048

# Background maintenance: `cleanup_old_versions` + `optimize_indices`, run by
# `pond serve` on an interval and by the `pond maintenance` one-shot verb.
# `pond maintenance` runs regardless of `enabled`.
#
# [maintenance]
# enabled = true          # run the background task under `pond serve`
# interval_secs = 21600   # background pass interval (default 6h)
# retention_days = 30     # cleanup_old_versions window (default 30 days)
";

/// Top-level `config.toml` shape.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
    #[serde(default)]
    pub maintenance: MaintenanceConfig,
    /// `[sources.<adapter>]` map: per-adapter config blobs the matching
    /// factory deserializes inside its `open()`. The shape is adapter-defined
    /// (filesystem adapters expect `{ path = "..." }`; API-backed adapters
    /// expect endpoint + auth keys), so this layer stays opaque. Empty by
    /// default; `pond sync` runs discovery into this map on first use.
    #[serde(default)]
    pub sources: BTreeMap<String, Value>,
}

/// The `[maintenance]` section: background `cleanup_old_versions` +
/// `optimize_indices` settings (design.md 3.2.0). Durations are plain integers
/// rather than humanized strings - one fewer parser, and `config.toml` stays
/// trivially round-trippable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceConfig {
    /// Whether `pond serve` spawns the background maintenance task. The
    /// `pond maintenance` one-shot verb runs regardless of this flag.
    #[serde(default = "default_maintenance_enabled")]
    pub enabled: bool,
    /// Background pass interval in seconds (default 6h).
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// `cleanup_old_versions` retention window in days (default 30).
    #[serde(default = "default_retention_days")]
    pub retention_days: u64,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            enabled: default_maintenance_enabled(),
            interval_secs: default_interval_secs(),
            retention_days: default_retention_days(),
        }
    }
}

/// The `[embeddings]` section: the model registry plus per-namespace overrides.
/// Embeddings are opt-in by data: `pond serve` / `pond mcp` probe the
/// `embeddings` table at boot and load the model only when rows exist for the
/// default identity. Run `pond embed` once to populate the backlog.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingsConfig {
    /// `[[embeddings.models]]` entries. User entries merge over built-ins by `id`.
    #[serde(default)]
    pub models: Vec<EmbeddingModel>,
    /// `[embeddings.overrides.<namespace>.<model_id>]` tunable overrides.
    #[serde(default)]
    pub overrides: BTreeMap<String, BTreeMap<String, EmbeddingOverride>>,
}

/// One `[[embeddings.models]]` registry entry (design.md 3.2.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingModel {
    /// Registry id and the `model_id` PK component on the `embeddings` table.
    /// Doubles as the HuggingFace repo passed to the loader - see `load_repo`,
    /// which strips any `@revision` suffix used for cache invalidation.
    pub id: String,
    /// Output vector dimension. Must match the known model's actual dim.
    pub dim: u32,
    /// Token cap on the text embedded per message. One message produces one
    /// vector; the model's own ceiling is far higher, but a longer input still
    /// collapses into a single vector, so this caps embed cost for the rare
    /// giant message. Enforced as the tokenizer `max_length` at model load -
    /// the full `search_text` still goes to the BM25 index uncapped.
    pub max_embed_tokens: usize,
    /// IVF_PQ `num_sub_vectors` for this model's vector index.
    pub num_sub_vectors: usize,
    #[serde(default = "default_distance")]
    pub distance: Distance,
    #[serde(default = "default_true")]
    pub normalize: bool,
    #[serde(default)]
    pub default: bool,
}

/// Per-namespace tunable overrides. Immutable fields (`dim`, `distance`,
/// `normalize`) cannot be overridden - they would invalidate stored vectors
/// (design.md 3.2.4).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingOverride {
    pub max_embed_tokens: Option<usize>,
    pub num_sub_vectors: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distance {
    Cosine,
    L2,
    Dot,
}

/// A model pond knows how to load: the validation set for a model's load repo.
struct KnownModel {
    code: &'static str,
    dim: u32,
    distance: Distance,
}

/// v1 ships a single loader path: the Qwen3 candle backend via
/// `Qwen3TextEmbedding::from_hf`. Adding a model pond already knows how to load
/// is config-only; a new loader still requires code (design.md 3.2.4).
const KNOWN_MODELS: &[KnownModel] = &[KnownModel {
    code: "Qwen/Qwen3-Embedding-0.6B",
    dim: 1024,
    distance: Distance::Cosine,
}];

/// Resolve pond's data directory. An explicit `--data-dir` / `POND_DATA_DIR`
/// wins (and may carry an `s3://` / `gs://` / `az://` URI); otherwise the
/// XDG-local fallback (`$XDG_DATA_HOME/pond`, then `$HOME/.local/share/pond`,
/// then `.pond`). `xdg_data_home` is honored only if absolute, per the XDG
/// base-directory spec.
pub fn resolve_data_dir(
    explicit: Option<StorageLocation>,
    xdg_data_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> StorageLocation {
    if let Some(location) = explicit {
        return location;
    }
    if let Some(xdg) = xdg_data_home.filter(|path| path.is_absolute()) {
        return StorageLocation::LocalPath(xdg.join("pond"));
    }
    if let Some(home) = home {
        return StorageLocation::LocalPath(home.join(".local").join("share").join("pond"));
    }
    // No HOME and no usable XDG var - stay usable rather than panic.
    StorageLocation::LocalPath(PathBuf::from(".pond"))
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

fn default_distance() -> Distance {
    Distance::Cosine
}

fn default_true() -> bool {
    true
}

fn default_maintenance_enabled() -> bool {
    true
}

fn default_interval_secs() -> u64 {
    21_600
}

fn default_retention_days() -> u64 {
    30
}

impl MaintenanceConfig {
    /// Reject a config that would spawn a maintenance task that cannot run:
    /// a zero interval would busy-loop, a zero retention would be a no-op
    /// cleanup. Only enforced when the background task is enabled.
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.interval_secs == 0 {
            bail!("[maintenance] interval_secs must be greater than 0 when enabled");
        }
        if self.enabled && self.retention_days == 0 {
            bail!("[maintenance] retention_days must be greater than 0 when enabled");
        }
        Ok(())
    }
}

impl EmbeddingModel {
    /// The built-in v1 default: Qwen3-Embedding-0.6B (design.md 3.2.4).
    pub fn qwen3_default() -> Self {
        Self {
            id: "Qwen/Qwen3-Embedding-0.6B".to_owned(),
            dim: 1024,
            // ~98% of the measured corpus is under 1024 tokens (plan.md Stage
            // 2); the tail is truncated for the vector but kept whole in
            // BM25/FTS and `pond_get`. 1024 also keeps the per-message embed
            // cost tiny - see `DEFAULT_BATCH_TOKEN_SQ_BUDGET`.
            max_embed_tokens: 1024,
            num_sub_vectors: 64,
            distance: Distance::Cosine,
            normalize: true,
            default: true,
        }
    }

    /// The HuggingFace repo id to load: `id` with any `@revision` suffix stripped.
    /// `id` itself stays the logical identity (registry key + `model_id` PK).
    pub fn load_repo(&self) -> &str {
        self.id
            .split_once('@')
            .map_or(self.id.as_str(), |(repo, _)| repo)
    }
}

impl Config {
    /// Load `config.toml` from `path` if it exists, merge user `[[embeddings.models]]`
    /// over the built-in defaults by `id`, expand `~` in source paths, and
    /// validate the resolved registry. A missing file yields the built-in defaults.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut config = if path.exists() {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config {}", path.display()))?;
            toml::from_str::<Self>(&text)
                .with_context(|| format!("failed to parse config {}", path.display()))?
        } else {
            Self::default()
        };
        config.embeddings.apply_builtin_defaults();
        config.embeddings.validate()?;
        config.maintenance.validate()?;
        // Tilde expansion is per-adapter (inside each factory's `open()`):
        // an API-backed adapter has no path to expand, and only the
        // filesystem-shaped adapters need the helper. See `expand_home_under`.
        Ok(config)
    }

    /// The built-in defaults with no `config.toml` on disk.
    pub fn builtin() -> Self {
        let mut config = Self::default();
        config.embeddings.apply_builtin_defaults();
        config
    }

    /// Resolve the `[sources.<adapter>]` entries to drive `pond sync`. With
    /// `adapter = None` returns every entry (the no-arg sync path); with
    /// `Some(name)` returns just that one or errors if it's not in config.
    /// The caller is responsible for the discovery fallback when this returns
    /// an empty list. Each tuple's `Value` is the opaque config blob to hand
    /// to the matching factory's `open()`.
    pub fn resolve_sources(&self, adapter: Option<&str>) -> Result<Vec<(String, Value)>> {
        match adapter {
            None => Ok(self
                .sources
                .iter()
                .map(|(name, blob)| (name.clone(), blob.clone()))
                .collect()),
            Some(name) => {
                let blob = self
                    .sources
                    .get(name)
                    .ok_or_else(|| anyhow!("no [sources.{name}] entry in config"))?;
                Ok(vec![(name.to_owned(), blob.clone())])
            }
        }
    }
}

/// Tilde-expand `path` against an explicit `home`. Filesystem-shaped adapters
/// call this from inside their factory's `open()`. Tests use it directly to
/// exercise the rule without mutating the process-wide `HOME` env var
/// (`std::env::set_var` is `unsafe` under edition 2024 and pond forbids
/// unsafe code).
pub fn expand_home_under(path: &Path, home: &Path) -> PathBuf {
    let Some(text) = path.to_str() else {
        return path.to_path_buf();
    };
    if text == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = text.strip_prefix("~/") {
        return home.join(rest);
    }
    path.to_path_buf()
}

impl EmbeddingsConfig {
    /// Insert built-in registry entries that the user config did not override by `id`.
    fn apply_builtin_defaults(&mut self) {
        for builtin in builtin_models() {
            if !self.models.iter().any(|model| model.id == builtin.id) {
                self.models.push(builtin);
            }
        }
    }

    /// Validate the resolved registry against pond's known-model set: unknown
    /// load repo, dim mismatch, unsupported distance, or not exactly one
    /// `default = true` entry all fail startup with a clear error (design.md 3.2.4).
    pub fn validate(&self) -> Result<()> {
        if self.models.is_empty() {
            bail!("embeddings registry is empty");
        }
        for model in &self.models {
            let known = KNOWN_MODELS
                .iter()
                .find(|known| known.code == model.load_repo())
                .with_context(|| {
                    format!(
                        "embedding model {:?} uses unknown load repo {:?}; known repos: {}",
                        model.id,
                        model.load_repo(),
                        KNOWN_MODELS
                            .iter()
                            .map(|known| known.code)
                            .collect::<Vec<_>>()
                            .join(", "),
                    )
                })?;
            if model.dim != known.dim {
                bail!(
                    "embedding model {:?} declares dim {} but {} is {}",
                    model.id,
                    model.dim,
                    known.code,
                    known.dim,
                );
            }
            if model.distance != known.distance {
                bail!(
                    "embedding model {:?} declares distance {:?} but {} requires {:?}",
                    model.id,
                    model.distance,
                    known.code,
                    known.distance,
                );
            }
            check_max_embed_tokens(model.max_embed_tokens, &model.id)?;
        }
        // Namespace overrides can raise `max_embed_tokens` after the base
        // registry is validated, so they must clear the same ceiling.
        for (namespace, models) in &self.overrides {
            for (model_id, over) in models {
                if let Some(value) = over.max_embed_tokens {
                    check_max_embed_tokens(
                        value,
                        &format!("{model_id} (override for namespace {namespace:?})"),
                    )?;
                }
            }
        }
        match self.models.iter().filter(|model| model.default).count() {
            1 => Ok(()),
            0 => bail!("embeddings registry has no `default = true` model"),
            n => bail!(
                "embeddings registry has {n} `default = true` models; exactly one is required"
            ),
        }
    }

    /// The single `default = true` entry, with any matching namespace override
    /// applied. Assumes [`validate`](Self::validate) has passed.
    pub fn default_model(&self, namespace: &str) -> Result<EmbeddingModel> {
        let base = self
            .models
            .iter()
            .find(|model| model.default)
            .context("embeddings registry has no default model")?
            .clone();
        Ok(self.with_overrides(base, namespace))
    }

    /// Look up a model by `id`, with any matching namespace override applied.
    pub fn model(&self, id: &str, namespace: &str) -> Result<EmbeddingModel> {
        let base = self
            .models
            .iter()
            .find(|model| model.id == id)
            .with_context(|| format!("embedding model {id:?} not found in registry"))?
            .clone();
        Ok(self.with_overrides(base, namespace))
    }

    fn with_overrides(&self, mut model: EmbeddingModel, namespace: &str) -> EmbeddingModel {
        if let Some(over) = self
            .overrides
            .get(namespace)
            .and_then(|models| models.get(&model.id))
        {
            if let Some(value) = over.max_embed_tokens {
                model.max_embed_tokens = value;
            }
            if let Some(value) = over.num_sub_vectors {
                model.num_sub_vectors = value;
            }
        }
        model
    }
}

pub fn resolve_model(
    config: &Config,
    model: Option<&str>,
    namespace: &str,
) -> anyhow::Result<EmbeddingModel> {
    match model {
        Some(id) => config.embeddings.model(id, namespace),
        None => config.embeddings.default_model(namespace),
    }
}

fn builtin_models() -> Vec<EmbeddingModel> {
    vec![EmbeddingModel::qwen3_default()]
}

/// Validate one `max_embed_tokens` value. Beyond a non-zero check, it must be
/// small enough that a single message's attention cost (`max_embed_tokens^2`)
/// fits one embedding batch's budget: cost-aware batching keeps a long message
/// out of an oversized batch, but it cannot split a single message, so a
/// message that does not fit a batch on its own would risk an out-of-memory
/// inference pass. `label` identifies the offending registry entry or override.
fn check_max_embed_tokens(value: usize, label: &str) -> Result<()> {
    if value == 0 {
        bail!("embedding model {label:?} max_embed_tokens must be greater than 0");
    }
    let cost = value.saturating_mul(value);
    if cost > DEFAULT_BATCH_TOKEN_SQ_BUDGET {
        bail!(
            "embedding model {label:?} max_embed_tokens {value} is too large: a single \
             message would cost {cost} token^2, over the {DEFAULT_BATCH_TOKEN_SQ_BUDGET} \
             per-batch budget - a message must fit one embedding batch on its own"
        );
    }
    Ok(())
}
