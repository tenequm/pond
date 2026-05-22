use std::{path::PathBuf, sync::Mutex};

use anyhow::{Result, anyhow};
use fastembed::{EmbeddingModel as FastEmbedModel, InitOptions, TextEmbedding};

use super::{EmbedBackend, MODEL_ID};

/// The e5-small backend: `intfloat/multilingual-e5-small` via `fastembed`'s
/// ONNX Runtime path, running on CPU.
pub struct E5SmallEmbedder {
    /// `fastembed::TextEmbedding::embed` takes `&mut self`, but `EmbedBackend`
    /// is shared as `Arc<dyn EmbedBackend>`, so the `Mutex` supplies the
    /// interior mutability the trait's `&self` method needs.
    inner: Mutex<TextEmbedding>,
}

impl E5SmallEmbedder {
    /// Load `intfloat/multilingual-e5-small` from HuggingFace (cached after the
    /// first download) and build a CPU ONNX Runtime session.
    pub fn load() -> Result<Self> {
        // 512 is e5-small's training context; the tokenizer truncates input
        // past it before inference - one message, one vector, bounded cost.
        let mut options =
            InitOptions::new(FastEmbedModel::MultilingualE5Small).with_max_length(512);
        if let Some(cache_dir) = hf_hub_cache_dir() {
            options = options.with_cache_dir(cache_dir);
        }
        let inner = TextEmbedding::try_new(options)
            .map_err(|error| anyhow!("failed to load embedding model {MODEL_ID}: {error}"))?;
        tracing::info!(model = %MODEL_ID, "loaded embedding model");
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }
}

impl EmbedBackend for E5SmallEmbedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        // `EmbedWorker` already batches `texts` to a small fixed size, so this
        // whole pond-batch runs in one ORT call (`None` re-chunks only past
        // fastembed's 256 default, which a worker batch never reaches).
        self.inner
            .lock()
            .map_err(|error| anyhow!("e5 embedder mutex poisoned: {error}"))?
            .embed(texts, None)
            .map_err(|error| anyhow!("embedding inference failed: {error}"))
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
