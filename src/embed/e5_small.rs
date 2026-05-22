use std::{path::PathBuf, sync::Mutex};

use anyhow::{Result, anyhow};
use fastembed::{EmbeddingModel as FastEmbedModel, InitOptions, TextEmbedding};

use crate::config::EmbeddingModel;

use super::EmbedBackend;

/// The e5-small backend: `intfloat/multilingual-e5-small` via `fastembed`'s
/// ONNX Runtime path, running on CPU.
pub struct E5SmallEmbedder {
    /// `fastembed::TextEmbedding::embed` takes `&mut self`, but `EmbedBackend`
    /// is shared as `Arc<dyn EmbedBackend>`, so the `Mutex` supplies the
    /// interior mutability the trait's `&self` method needs.
    inner: Mutex<TextEmbedding>,
    dim: usize,
    model_id: String,
    max_embed_tokens: i32,
}

impl E5SmallEmbedder {
    /// Load `intfloat/multilingual-e5-small` from HuggingFace (cached after the
    /// first download) and build a CPU ONNX Runtime session.
    pub fn load(model: &EmbeddingModel) -> Result<Self> {
        // `max_embed_tokens` is the tokenizer `max_length`: input past it is
        // truncated before inference - one message, one vector, bounded cost.
        let mut options = InitOptions::new(FastEmbedModel::MultilingualE5Small)
            .with_max_length(model.max_embed_tokens);
        if let Some(cache_dir) = hf_hub_cache_dir() {
            options = options.with_cache_dir(cache_dir);
        }
        let inner = TextEmbedding::try_new(options)
            .map_err(|error| anyhow!("failed to load embedding model {}: {error}", model.id))?;
        tracing::info!(model = %model.id, "loaded embedding model");
        Ok(Self {
            inner: Mutex::new(inner),
            dim: model.dim as usize,
            model_id: model.id.clone(),
            max_embed_tokens: model.max_embed_tokens as i32,
        })
    }
}

impl EmbedBackend for E5SmallEmbedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        // `EmbedWorker` already batches `texts` within a cost budget, so `None`
        // runs the whole pond-batch in one ORT call rather than re-chunking it.
        self.inner
            .lock()
            .map_err(|error| anyhow!("e5 embedder mutex poisoned: {error}"))?
            .embed(texts, None)
            .map_err(|error| anyhow!("embedding inference failed: {error}"))
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn max_embed_tokens(&self) -> i32 {
        self.max_embed_tokens
    }
}

/// The HuggingFace hub cache directory. fastembed otherwise defaults to a
/// cwd-relative `.fastembed_cache`, which would re-download the model for
/// every working directory; this points it at the shared standard location.
fn hf_hub_cache_dir() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;
    Some(base.join("huggingface").join("hub"))
}
