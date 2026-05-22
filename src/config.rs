//! Configuration loading: the `[embeddings]`, `[sources]`, and `[storage]`
//! blocks.
//!
//! pond ships built-in defaults, so an instance with no `config.toml` still
//! works. `pond config --print-schema` emits [`DEFAULT_CONFIG_TOML`], the
//! fully-annotated example.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use lance_io::object_store::uri_to_url;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

/// Parse a CLI / env `--data-dir` argument into a `Url`. Delegates to Lance's
/// own `uri_to_url`, which handles every form pond cares about:
/// - bare paths like `/srv/pond` -> `file:///srv/pond`
/// - explicit `file://...` URIs
/// - object-store URIs (`s3://`, `gs://`, `az://`, ...)
/// - tilde expansion (`~/...`)
/// - Windows drive letters (we don't ship Windows, but the parser handles it)
///
/// Using Lance's parser keeps pond's CLI parse path identical to what Lance
/// uses internally - no risk of pond accepting a string Lance later rejects.
pub fn parse_data_dir(input: &str) -> Result<Url> {
    uri_to_url(input).with_context(|| format!("invalid --data-dir {input:?}"))
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
/// back as plain paths (no `file://` prefix); remote URLs stay verbatim.
pub fn display(url: &Url) -> String {
    if let Some(path) = local_path(url) {
        path.display().to_string()
    } else {
        url.to_string()
    }
}

/// Build a `Url` from a filesystem path. Convenience for tests and for
/// `resolve_data_dir` callers that hold a `PathBuf` already. The path must be
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

/// Default `config.toml` body emitted by `pond config --print-schema`. Every
/// line is commented: pond ships built-in defaults, so the file is purely a
/// discoverable template and pond still works with no `config.toml` on disk.
pub const DEFAULT_CONFIG_TOML: &str = "\
# pond configuration.
#
# pond ships built-in defaults, so every setting here is optional - delete this
# file and pond still works. Uncomment and edit to override.

# Where pond looks for source data to import. One entry per adapter type
# (`claude-code`, `codex-cli`, ...). `pond sync` with no arguments syncs every
# entry; `pond sync <adapter>` syncs just one. With an empty `[sources]`,
# `pond sync` runs an interactive discovery against the known default paths
# and writes the picks back here.
#
# Future wrap: pond is single-namespace in v1 (spec.md#namespace-resolution); `[sources]` is
# flat here. When multi-namespace pond lands, source registration becomes
# per-tenant under `[namespaces.<ns>.sources.<adapter>]`. Pre-v1 the schema
# is breakable; the rename is operationally free until a real second tenant
# exists.
#
# [sources.claude-code]
# path = \"~/.claude/projects\"
#
# [sources.codex-cli]
# path = \"~/.codex/sessions\"

# Embeddings. Default `enabled = false`: search runs FTS-only and no model is
# loaded. Set `true` for hybrid search. `pond embed` runs regardless, so
# vectors can be pre-populated before flipping the switch. `model` selects the
# embedding model; the engine ships one loader.
#
# [embeddings]
# enabled = false
# model = \"intfloat/multilingual-e5-base\"

# Object-store credentials and tuning, passed verbatim to Lance's
# `DatasetBuilder::with_storage_options`. Required only when `--data-dir` is
# an `s3://` / `gs://` / `az://` URI that needs auth or a non-default region.
# Keys follow the `object_store` crate's standard names. Environment
# variables of the same name are read by `object_store` automatically;
# values in this block override them. pond does not parse these.
#
# Future wrap: pond is single-namespace in v1 (spec.md#namespace-resolution); `[storage]` is
# flat here on the assumption of one bucket per pond. When multi-namespace
# pond lands and tenants need separate buckets/regions, this becomes
# `[namespaces.<ns>.storage]`. Pre-v1 the schema is breakable; the rename is
# operationally free until a real second tenant exists.
#
# [storage]
# AWS_ACCESS_KEY_ID = \"...\"
# AWS_SECRET_ACCESS_KEY = \"...\"
# AWS_REGION = \"us-east-1\"
# AWS_ENDPOINT = \"https://minio.example.com\"  # for self-hosted MinIO
# allow_http = \"true\"                          # only for non-TLS endpoints
";

/// Top-level `config.toml` shape.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
    /// `[sources.<adapter>]` map: per-adapter config blobs the matching
    /// factory deserializes inside its `open()`. The shape is adapter-defined
    /// (filesystem adapters expect `{ path = "..." }`; API-backed adapters
    /// expect endpoint + auth keys), so this layer stays opaque. Empty by
    /// default; `pond sync` runs discovery into this map on first use.
    #[serde(default)]
    pub sources: BTreeMap<String, Value>,
    /// `[storage]` key=value pairs handed verbatim to Lance's
    /// `DatasetBuilder::with_storage_options` and `WriteParams.store_params`.
    /// Keys are the standard `object_store` config names
    /// (`AWS_ACCESS_KEY_ID`, `AWS_REGION`, `AWS_ENDPOINT`, etc.); see Lance's
    /// `DatasetBuilder::with_storage_options` doc for the per-scheme variants
    /// (S3 / GCS / Azure). pond does not parse or validate these; Lance does.
    /// Empty by default; required only when `--data-dir` is an object-store
    /// URI that needs credentials or a non-default region/endpoint. Values
    /// here override any matching environment variables.
    #[serde(default)]
    pub storage: BTreeMap<String, String>,
}

/// `[embeddings]`: the master switch and the model selector. With
/// `enabled = false` (the default) the search path never loads a model;
/// `pond embed` runs regardless.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// The embedding model id (spec.md#search): configuration selects the
    /// model, the engine supplies the loader. v1 ships exactly one loader.
    #[serde(default = "default_model")]
    pub model: String,
}

impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            model: default_model(),
        }
    }
}

fn default_model() -> String {
    crate::embed::MODEL_ID.to_owned()
}

/// Resolve pond's data directory. An explicit `--data-dir` / `POND_DATA_DIR`
/// wins (and may carry an `s3://` / `gs://` / `az://` URI); otherwise the
/// XDG-local fallback (`$XDG_DATA_HOME/pond`, then `$HOME/.local/share/pond`,
/// then `.pond`). `xdg_data_home` is honored only if absolute, per the XDG
/// base-directory spec.
pub fn resolve_data_dir(
    explicit: Option<Url>,
    xdg_data_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Result<Url> {
    if let Some(location) = explicit {
        return Ok(location);
    }
    if let Some(xdg) = xdg_data_home.filter(|path| path.is_absolute()) {
        return url_for_path(xdg.join("pond"));
    }
    if let Some(home) = home {
        return url_for_path(home.join(".local").join("share").join("pond"));
    }
    // No HOME and no usable XDG var - stay usable rather than panic.
    url_for_path(PathBuf::from(".pond"))
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
    /// Load `config.toml` from `path` if it exists and validate it. A missing
    /// file yields the built-in defaults.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let config = if path.exists() {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read config {}", path.display()))?;
            toml::from_str::<Self>(&text)
                .with_context(|| format!("failed to parse config {}", path.display()))?
        } else {
            Self::default()
        };
        config.embeddings.validate()?;
        // Tilde expansion is per-adapter (inside each factory's `open()`):
        // an API-backed adapter has no path to expand, and only the
        // filesystem-shaped adapters need the helper. See `expand_home_under`.
        Ok(config)
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
    /// The configured model must be one the engine has a loader for
    /// (spec.md#search). v1 ships exactly one.
    pub fn validate(&self) -> Result<()> {
        if self.model != crate::embed::MODEL_ID {
            bail!(
                "embeddings.model {:?} is not a model pond can load; the engine \
                 ships one loader: {:?}",
                self.model,
                crate::embed::MODEL_ID,
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::Value;
    use tempfile::TempDir;

    #[test]
    fn validate_rejects_an_unknown_model() {
        let config = EmbeddingsConfig {
            enabled: true,
            model: "bogus/model".to_owned(),
        };
        assert!(config.validate().is_err());
        assert!(EmbeddingsConfig::default().validate().is_ok());
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
        assert!(!config.embeddings.enabled);
        assert_eq!(config.embeddings.model, crate::embed::MODEL_ID);
    }

    #[test]
    fn resolve_data_dir_follows_explicit_then_xdg_then_home() {
        // An explicit `--data-dir` / `POND_DATA_DIR` wins over everything. The
        // explicit value can carry any URI form Lance accepts; here we test the
        // local-path form (parsing is delegated to Lance's `uri_to_url`).
        let explicit = parse_data_dir("/explicit").unwrap();
        let resolved = resolve_data_dir(
            Some(explicit.clone()),
            Some(PathBuf::from("/xdg")),
            Some(PathBuf::from("/home")),
        )
        .unwrap();
        assert_eq!(resolved, explicit);

        // An absolute XDG_DATA_HOME is used next.
        let resolved = resolve_data_dir(
            None,
            Some(PathBuf::from("/xdg")),
            Some(PathBuf::from("/home")),
        )
        .unwrap();
        assert!(is_local(&resolved));
        assert_eq!(local_path(&resolved).unwrap(), PathBuf::from("/xdg/pond"));

        // A relative XDG_DATA_HOME is ignored per the XDG spec; HOME is the fallback.
        let resolved = resolve_data_dir(
            None,
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
        let resolved = resolve_data_dir(None, None, None).unwrap();
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
    fn resolve_sources_returns_one_or_all_or_errors() {
        let temp = TempDir::new().unwrap();
        let body = "\
[sources.claude-code]
path = \"/srv/claude\"

[sources.codex-cli]
path = \"/srv/codex\"
";
        let path = temp.path().join("config.toml");
        std::fs::write(&path, body).expect("write config");
        let config = Config::load(&path).unwrap();

        // None -> everything in [sources.*]
        let all = config.resolve_sources(None).unwrap();
        assert_eq!(all.len(), 2);
        let names: Vec<_> = all.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"claude-code"));
        assert!(names.contains(&"codex-cli"));

        // Some(name) -> one entry, opaque JSON blob
        let one = config.resolve_sources(Some("codex-cli")).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].0, "codex-cli");
        assert_eq!(
            one[0].1.get("path").and_then(Value::as_str),
            Some("/srv/codex"),
        );

        // Unknown -> error
        assert!(config.resolve_sources(Some("nope")).is_err());
    }

    #[test]
    fn memory_uri_is_classified_as_remote() {
        let url = parse_data_dir("memory:///pond-remote-test").expect("memory uri parses");
        assert!(
            !is_local(&url),
            "memory:// is not a local-filesystem URL: {url}",
        );
        assert!(
            local_path(&url).is_none(),
            "local_path must return None for non-file schemes",
        );
    }
}
