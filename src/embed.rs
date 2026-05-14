//! The embedding stage of `pond ingest`: the Qwen3 candle backend and the
//! batch-oriented worker that populates the `embeddings` dataset. One message
//! produces one vector - there is no chunking.
//!
//! Batching is load-bearing (plan.md Stage 2): the worker accumulates messages
//! and calls the model once per batch, never once per message. The same rule
//! applies to the Lance write path - embedding rows are written in batches,
//! never one `merge_insert` per message.

use anyhow::{Result, anyhow};
use tokio_stream::StreamExt;

use crate::{
    config::EmbeddingModel,
    datasets::{self, EmbeddingRow},
    substrate::{PendingMessage, PondStore},
};

/// Default number of messages accumulated before a model-inference + write batch.
pub const DEFAULT_BATCH_SIZE: usize = 32;

/// The retrieval task description baked into the Qwen3 query instruction.
const QUERY_INSTRUCTION_TASK: &str =
    "Given a search query, retrieve relevant messages from prior conversations";

/// Format a search query with the Qwen3-Embedding instruction prefix. The model
/// card prescribes `Instruct: {task}\nQuery: {query}` for the query side; the
/// document side (chunks embedded by the worker) gets no prefix, so queries and
/// documents are deliberately embedded asymmetrically.
pub fn qwen3_query_instruction(query: &str) -> String {
    format!("Instruct: {QUERY_INSTRUCTION_TASK}\nQuery: {query}")
}

/// A pluggable embedding backend. The real backend is [`Qwen3Embedder`];
/// tests substitute an instrumented fake to assert batching behavior.
pub trait EmbedBackend: Send + Sync {
    /// Embed a batch of texts. The returned vectors are L2-normalized and have
    /// length [`dim`](Self::dim).
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// Output vector dimension.
    fn dim(&self) -> usize;
}

/// The Qwen3 candle backend, loaded via `fastembed`'s `Qwen3TextEmbedding`.
pub struct Qwen3Embedder {
    inner: fastembed::Qwen3TextEmbedding,
    dim: usize,
}

impl Qwen3Embedder {
    /// Load the model weights from HuggingFace (cached after first download)
    /// onto the Metal device on macOS, CPU elsewhere. The selected device is
    /// logged at startup.
    pub fn load(model: &EmbeddingModel) -> Result<Self> {
        let device = select_device();
        let label = device_label(&device);
        // The Qwen3-Embedding weights ship as bf16; loading them as bf16 (rather
        // than upconverting to f32) halves resident memory at no quality cost
        // and keeps the full f32 exponent range, so no overflow risk.
        //
        // `max_embed_tokens` is the tokenizer `max_length`: input past it is
        // truncated before inference, which is exactly the per-message cap - one
        // message, one vector, bounded embed cost (plan.md Stage 2).
        let inner = fastembed::Qwen3TextEmbedding::from_hf(
            model.load_repo(),
            &device,
            candle_core::DType::BF16,
            model.max_embed_tokens,
        )
        .map_err(|error| {
            anyhow!(
                "failed to load embedding model {}: {error}",
                model.load_repo()
            )
        })?;
        tracing::info!(model = %model.id, device = label, "loaded embedding model");
        Ok(Self {
            inner,
            dim: model.dim as usize,
        })
    }

    /// The device the weights were loaded onto (`"metal"`, `"cuda"`, or `"cpu"`).
    pub fn device(&self) -> &'static str {
        device_label(self.inner.device())
    }
}

impl EmbedBackend for Qwen3Embedder {
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        self.inner
            .embed(texts)
            .map_err(|error| anyhow!("embedding inference failed: {error}"))
    }

    fn dim(&self) -> usize {
        self.dim
    }
}

/// Whether `repo_id`'s weights and tokenizer are already in the local
/// HuggingFace cache. Lets `pond setup` report "present" vs "downloading"
/// without triggering a fetch.
pub fn model_is_cached(repo_id: &str) -> bool {
    let repo = hf_hub::Cache::from_env().model(repo_id.to_owned());
    ["config.json", "tokenizer.json", "model.safetensors"]
        .iter()
        .all(|file| repo.get(file).is_some())
}

/// Select the embedding device: Metal on macOS, CUDA on a non-macOS build with
/// the `cuda` feature, CPU otherwise. candle's `*_if_available` helpers return
/// `Cpu` when the matching backend feature is not compiled into `candle-core`;
/// `new_metal` / `new_cuda` can still fail at runtime (no GPU or driver), so an
/// `Err` falls back to `Cpu` too. The chosen device is logged in [`Qwen3Embedder::load`].
fn select_device() -> candle_core::Device {
    #[cfg(target_os = "macos")]
    let device = candle_core::Device::metal_if_available(0);
    #[cfg(not(target_os = "macos"))]
    let device = candle_core::Device::cuda_if_available(0);
    device.unwrap_or_else(|error| {
        tracing::warn!(%error, "GPU device unavailable, falling back to CPU");
        candle_core::Device::Cpu
    })
}

fn device_label(device: &candle_core::Device) -> &'static str {
    match device {
        candle_core::Device::Cpu => "cpu",
        candle_core::Device::Cuda(_) => "cuda",
        candle_core::Device::Metal(_) => "metal",
    }
}

/// Outcome of an [`EmbedWorker::run`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmbedSummary {
    /// Messages that had pending (un-embedded) `search_text`; one vector each.
    pub messages: usize,
    /// Model-inference + write batches issued.
    pub batches: usize,
}

/// Populates the `embeddings` dataset for one registry model. Reads
/// `messages.search_text` directly (no second concatenation path), batches
/// messages through the backend one vector each, and writes embedding rows in
/// batches.
pub struct EmbedWorker<'a, B: EmbedBackend> {
    store: &'a PondStore,
    backend: &'a B,
    model_id: String,
    batch_size: usize,
}

impl<'a, B: EmbedBackend> EmbedWorker<'a, B> {
    /// Build a worker for `model`. The backend's [`dim`](EmbedBackend::dim) must
    /// match the model's declared `dim`.
    pub fn new(store: &'a PondStore, backend: &'a B, model: &EmbeddingModel) -> Result<Self> {
        if backend.dim() != model.dim as usize {
            return Err(anyhow!(
                "backend dim {} does not match model {} dim {}",
                backend.dim(),
                model.id,
                model.dim,
            ));
        }
        Ok(Self {
            store,
            backend,
            model_id: model.id.clone(),
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Override the message batch size (default [`DEFAULT_BATCH_SIZE`]).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Embed every message with `search_text` that does not yet have an
    /// embedding row for this model. Idempotent: the PK is `(message_id,
    /// model_id)`, so a re-run over an already-embedded corpus is a no-op.
    ///
    /// Messages are pulled from a streaming scan, so peak memory is one stream
    /// page plus the staged batch - not the whole corpus.
    pub async fn run(&self) -> Result<EmbedSummary> {
        let embedded = self.store.embedded_message_ids(&self.model_id).await?;
        let mut summary = EmbedSummary::default();

        // Accumulate messages *and* across stream pages so the model and the
        // write path always see full batches; `staged` is flushed only when it
        // reaches `batch_size`, never at a page boundary.
        let mut staged: Vec<StagedMessage> = Vec::with_capacity(self.batch_size);
        let mut stream = self.store.pending_messages_stream().await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let mut pending = datasets::pending_message_from_batch(&batch, row)?;
                if embedded.contains(&pending.message_id) {
                    continue;
                }
                summary.messages += 1;
                // Move `search_text` out of the message: it is handed to the
                // backend, and the embedding row never carries it.
                let text = std::mem::take(&mut pending.search_text);
                staged.push(StagedMessage {
                    message: pending,
                    text,
                });
                if staged.len() >= self.batch_size {
                    self.flush(&mut staged, &mut summary).await?;
                }
            }
        }
        self.flush(&mut staged, &mut summary).await?;

        tracing::info!(
            model = %self.model_id,
            messages = summary.messages,
            batches = summary.batches,
            "embed worker finished",
        );
        Ok(summary)
    }

    /// Embed the staged messages in one model call and write the resulting rows
    /// in one Lance batch. Empties `staged`.
    async fn flush(
        &self,
        staged: &mut Vec<StagedMessage>,
        summary: &mut EmbedSummary,
    ) -> Result<()> {
        if staged.is_empty() {
            return Ok(());
        }
        // Move the staged messages out so their text can be handed to the
        // backend without cloning; `staged` is left empty for the next batch.
        let messages = std::mem::take(staged);
        let mut texts = Vec::with_capacity(messages.len());
        let mut metas = Vec::with_capacity(messages.len());
        for staged in messages {
            texts.push(staged.text);
            metas.push(staged.message);
        }
        let vectors = self.backend.embed(&texts)?;
        if vectors.len() != metas.len() {
            return Err(anyhow!(
                "backend returned {} vectors for {} messages",
                vectors.len(),
                metas.len()
            ));
        }

        let mut rows = Vec::with_capacity(metas.len());
        for (message, vector) in metas.into_iter().zip(vectors) {
            rows.push(EmbeddingRow {
                message_id: message.message_id,
                model_id: self.model_id.clone(),
                vector,
                session_id: message.session_id,
                source_agent: message.source_agent,
                project: message.project,
                role: message.role,
                timestamp: message.timestamp,
            });
        }

        self.store.upsert_embeddings(&rows).await?;
        summary.batches += 1;
        Ok(())
    }
}

struct StagedMessage {
    message: PendingMessage,
    text: String,
}

#[cfg(test)]
mod tests {
    use super::{device_label, select_device};

    // plan.md Stage 2 done-when: the embedding worker runs on the Metal device
    // on macOS (real Apple hardware), never the CPU fallback; a default
    // non-macOS build runs on CPU. `select_device` is the device-selection path
    // the worker takes; exercising it needs no model weights. A `--features cuda`
    // build can select a GPU at runtime, so the CPU assertion is scoped to the
    // default (no-`cuda`) build.
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_selects_the_metal_device() {
        assert_eq!(device_label(&select_device()), "metal");
    }

    #[cfg(all(not(target_os = "macos"), not(feature = "cuda")))]
    #[test]
    fn non_macos_selects_cpu() {
        assert_eq!(device_label(&select_device()), "cpu");
    }
}
