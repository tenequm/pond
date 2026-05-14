//! The embedding worker: token-aware chunking, the Qwen3 candle backend, and
//! the batch-oriented worker that populates the `embeddings` dataset.
//!
//! Batching is load-bearing (plan.md Stage 2): the worker accumulates chunks
//! across messages and calls the model once per batch, never once per chunk.
//! The same rule applies to the Lance write path - embedding rows are written
//! in batches, never one `merge_insert` per chunk.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokenizers::Tokenizer;
use tokio_stream::StreamExt;

use crate::{
    config::EmbeddingModel,
    datasets::{self, EmbeddingRow},
    substrate::{PendingMessage, PondStore},
};

/// Default number of chunks accumulated before a model-inference + write batch.
pub const DEFAULT_BATCH_SIZE: usize = 32;

/// Token-aware deterministic chunker. Same `(tokenizer, text)` always produces
/// the same chunks, which keeps the `embeddings` PK stable across retries
/// (design.md 3.2.4).
#[derive(Debug, Clone, Copy)]
pub struct Chunker {
    chunk_size: usize,
    overlap: usize,
}

impl Chunker {
    /// Build a chunker for a registry model. `chunk_overlap_tokens` is required
    /// to be `< chunk_size_tokens` (enforced by config validation).
    pub fn new(model: &EmbeddingModel) -> Self {
        Self {
            chunk_size: model.chunk_size_tokens,
            overlap: model.chunk_overlap_tokens,
        }
    }

    /// Split `text` into overlapping token windows, each decoded back to a
    /// string. Text within a single chunk budget round-trips as-is.
    pub fn chunk(&self, tokenizer: &Tokenizer, text: &str) -> Result<Vec<String>> {
        let encoding = tokenizer
            .encode(text, false)
            .map_err(|error| anyhow!("tokenizer encode failed: {error}"))?;
        let ids = encoding.get_ids();
        if ids.len() <= self.chunk_size {
            return Ok(vec![text.to_owned()]);
        }

        let step = self.chunk_size - self.overlap;
        let mut chunks = Vec::new();
        let mut start = 0;
        while start < ids.len() {
            let end = (start + self.chunk_size).min(ids.len());
            let decoded = tokenizer
                .decode(&ids[start..end], true)
                .map_err(|error| anyhow!("tokenizer decode failed: {error}"))?;
            chunks.push(decoded);
            if end == ids.len() {
                break;
            }
            start += step;
        }
        Ok(chunks)
    }
}

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
        let inner = fastembed::Qwen3TextEmbedding::from_hf(
            &model.fastembed_code,
            &device,
            candle_core::DType::BF16,
            model.chunk_size_tokens,
        )
        .map_err(|error| {
            anyhow!(
                "failed to load embedding model {}: {error}",
                model.fastembed_code
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

/// Load the model's own tokenizer for the chunker. After [`Qwen3Embedder::load`]
/// has run, `tokenizer.json` is already in the HuggingFace cache so this is a
/// cache hit; standalone it downloads just that one small file.
pub fn load_tokenizer(repo_id: &str) -> Result<Tokenizer> {
    let api = hf_hub::api::sync::ApiBuilder::new()
        .with_progress(false)
        .build()
        .map_err(|error| anyhow!("failed to initialize HuggingFace API: {error}"))?;
    let path = api
        .model(repo_id.to_owned())
        .get("tokenizer.json")
        .map_err(|error| anyhow!("failed to fetch tokenizer.json for {repo_id}: {error}"))?;
    Tokenizer::from_file(&path).map_err(|error| anyhow!("failed to load tokenizer: {error}"))
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

#[cfg(target_os = "macos")]
fn select_device() -> candle_core::Device {
    match candle_core::Device::new_metal(0) {
        Ok(device) => device,
        Err(error) => {
            tracing::warn!(%error, "Metal device unavailable, falling back to CPU");
            candle_core::Device::Cpu
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn select_device() -> candle_core::Device {
    candle_core::Device::Cpu
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
    /// Messages that had pending (un-embedded) `search_text`.
    pub messages: usize,
    /// Chunks produced and embedded across those messages.
    pub chunks: usize,
    /// Model-inference + write batches issued.
    pub batches: usize,
}

/// Populates the `embeddings` dataset for one registry model. Reads
/// `messages.search_text` directly (no second concatenation path), chunks it,
/// batches chunks across messages through the backend, and writes embedding
/// rows in batches.
pub struct EmbedWorker<'a, B: EmbedBackend> {
    store: &'a PondStore,
    backend: &'a B,
    tokenizer: &'a Tokenizer,
    chunker: Chunker,
    model_id: String,
    batch_size: usize,
}

impl<'a, B: EmbedBackend> EmbedWorker<'a, B> {
    /// Build a worker for `model`. The backend's [`dim`](EmbedBackend::dim) must
    /// match the model's declared `dim`.
    pub fn new(
        store: &'a PondStore,
        backend: &'a B,
        tokenizer: &'a Tokenizer,
        model: &EmbeddingModel,
    ) -> Result<Self> {
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
            tokenizer,
            chunker: Chunker::new(model),
            model_id: model.id.clone(),
            batch_size: DEFAULT_BATCH_SIZE,
        })
    }

    /// Override the chunk batch size (default [`DEFAULT_BATCH_SIZE`]).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Embed every message with `search_text` that does not yet have embedding
    /// rows for this model. Idempotent: deterministic chunks produce stable PKs,
    /// so a re-run over an already-embedded corpus is a no-op.
    ///
    /// Messages are pulled from a streaming scan, so peak memory is one stream
    /// page plus the staged batch - not the whole corpus.
    pub async fn run(&self) -> Result<EmbedSummary> {
        let embedded = self.store.embedded_message_ids(&self.model_id).await?;
        let mut summary = EmbedSummary::default();

        // Accumulate chunks across messages *and* across stream pages so the
        // model and the write path always see full batches; `staged` is flushed
        // only when it reaches `batch_size`, never at a page boundary.
        let mut staged: Vec<StagedChunk> = Vec::with_capacity(self.batch_size);
        let mut stream = self.store.pending_messages_stream().await?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let pending = datasets::pending_message_from_batch(&batch, row)?;
                if embedded.contains(&pending.message_id) {
                    continue;
                }
                summary.messages += 1;
                let chunks = self.chunker.chunk(self.tokenizer, &pending.search_text)?;
                let pending = Arc::new(pending);
                for (chunk_index, text) in chunks.into_iter().enumerate() {
                    staged.push(StagedChunk {
                        message: Arc::clone(&pending),
                        chunk_index: i32::try_from(chunk_index).unwrap_or(i32::MAX),
                        text,
                    });
                    if staged.len() >= self.batch_size {
                        self.flush(&mut staged, &mut summary).await?;
                    }
                }
            }
        }
        self.flush(&mut staged, &mut summary).await?;

        tracing::info!(
            model = %self.model_id,
            messages = summary.messages,
            chunks = summary.chunks,
            batches = summary.batches,
            "embed worker finished",
        );
        Ok(summary)
    }

    /// Embed the staged chunks in one model call and write the resulting rows in
    /// one Lance batch. Empties `staged`.
    async fn flush(&self, staged: &mut Vec<StagedChunk>, summary: &mut EmbedSummary) -> Result<()> {
        if staged.is_empty() {
            return Ok(());
        }
        // Move the staged chunks out so their text can be handed to the backend
        // without cloning; `staged` is left empty for the next batch.
        let chunks = std::mem::take(staged);
        let mut texts = Vec::with_capacity(chunks.len());
        let mut metas = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            texts.push(chunk.text);
            metas.push((chunk.message, chunk.chunk_index));
        }
        let vectors = self.backend.embed(&texts)?;
        if vectors.len() != metas.len() {
            return Err(anyhow!(
                "backend returned {} vectors for {} chunks",
                vectors.len(),
                metas.len()
            ));
        }

        let mut rows = Vec::with_capacity(metas.len());
        for ((message, chunk_index), vector) in metas.into_iter().zip(vectors) {
            rows.push(EmbeddingRow {
                message_id: message.message_id.clone(),
                model_id: self.model_id.clone(),
                chunk_index,
                vector,
                session_id: message.session_id.clone(),
                source_agent: message.source_agent.clone(),
                project: message.project.clone(),
                role: message.role.clone(),
                timestamp: message.timestamp,
            });
        }

        self.store.upsert_embeddings(&rows).await?;
        summary.chunks += rows.len();
        summary.batches += 1;
        Ok(())
    }
}

struct StagedChunk {
    message: Arc<PendingMessage>,
    chunk_index: i32,
    text: String,
}
