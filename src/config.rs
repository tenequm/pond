//! Configuration loading and the embedding-model registry.
//!
//! v1 wires up the `[embeddings]` section only; the remaining `config.toml`
//! schema (`pond config --print-schema` and friends) lands in Stage 3. Built-in
//! defaults are shipped in the binary so a pond instance with no `config.toml`
//! still works; user config adds or overrides entries by `id`.

use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

/// Top-level `config.toml` shape. Only `[embeddings]` is wired in v1.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
}

/// The `[embeddings]` section: the model registry plus per-namespace overrides.
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
    /// Registry id, also the `model_id` PK component on the `embeddings` table.
    pub id: String,
    /// Loader code: the HuggingFace repo id passed to `Qwen3TextEmbedding::from_hf`.
    pub fastembed_code: String,
    /// Output vector dimension. Must match the known model's actual dim.
    pub dim: u32,
    pub chunk_size_tokens: usize,
    pub chunk_overlap_tokens: usize,
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
/// `normalize`, `fastembed_code`) cannot be overridden - they would invalidate
/// stored vectors (design.md 3.2.4).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddingOverride {
    pub chunk_size_tokens: Option<usize>,
    pub chunk_overlap_tokens: Option<usize>,
    pub num_sub_vectors: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distance {
    Cosine,
    L2,
    Dot,
}

/// A model pond knows how to load: the validation set for `fastembed_code`.
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

fn default_distance() -> Distance {
    Distance::Cosine
}

fn default_true() -> bool {
    true
}

impl EmbeddingModel {
    /// The built-in v1 default: Qwen3-Embedding-0.6B (design.md 3.2.4).
    pub fn qwen3_default() -> Self {
        Self {
            id: "Qwen/Qwen3-Embedding-0.6B".to_owned(),
            fastembed_code: "Qwen/Qwen3-Embedding-0.6B".to_owned(),
            dim: 1024,
            chunk_size_tokens: 1024,
            chunk_overlap_tokens: 128,
            num_sub_vectors: 64,
            distance: Distance::Cosine,
            normalize: true,
            default: true,
        }
    }
}

impl Config {
    /// Load `config.toml` from `path` if it exists, merge user `[[embeddings.models]]`
    /// over the built-in defaults by `id`, and validate the resolved registry.
    /// A missing file yields the built-in defaults.
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
        Ok(config)
    }

    /// The built-in defaults with no `config.toml` on disk.
    pub fn builtin() -> Self {
        let mut config = Self::default();
        config.embeddings.apply_builtin_defaults();
        config
    }
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
    /// loader code, dim mismatch, unsupported distance, or not exactly one
    /// `default = true` entry all fail startup with a clear error (design.md 3.2.4).
    pub fn validate(&self) -> Result<()> {
        if self.models.is_empty() {
            bail!("embeddings registry is empty");
        }
        for model in &self.models {
            let known = KNOWN_MODELS
                .iter()
                .find(|known| known.code == model.fastembed_code)
                .with_context(|| {
                    format!(
                        "embedding model {:?} uses unknown loader code {:?}; known codes: {}",
                        model.id,
                        model.fastembed_code,
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
            if model.chunk_overlap_tokens >= model.chunk_size_tokens {
                bail!(
                    "embedding model {:?} chunk_overlap_tokens ({}) must be < chunk_size_tokens ({})",
                    model.id,
                    model.chunk_overlap_tokens,
                    model.chunk_size_tokens,
                );
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
            if let Some(value) = over.chunk_size_tokens {
                model.chunk_size_tokens = value;
            }
            if let Some(value) = over.chunk_overlap_tokens {
                model.chunk_overlap_tokens = value;
            }
            if let Some(value) = over.num_sub_vectors {
                model.num_sub_vectors = value;
            }
        }
        model
    }
}

fn builtin_models() -> Vec<EmbeddingModel> {
    vec![EmbeddingModel::qwen3_default()]
}
