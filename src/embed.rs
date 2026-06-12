//! The embedding stage: candle XLM-RoBERTa FP16 ([`CandleEmbedder`]) plus
//! the batch-oriented [`EmbedWorker`] that fills `messages.vector` /
//! `messages.embedding_model` (spec.md#search). One message produces one
//! vector - there is no chunking.
//!
//! [`LazyEmbedder`] caches a loaded backend for `pond mcp` / `pond serve`
//! and drops it after [`DEFAULT_IDLE_EVICTION`] of no use. The drop is
//! clean under macOS `phys_footprint` (post-drop drops to ~107 MiB
//! regardless of backend), so time-weighted RSS over an interactive MCP
//! session stays well under the per-instance budget despite the macOS
//! Metal buffer pool's `iokit_mapped` retention during active queries.
//!
//! The worker accumulates messages and calls the model once per fixed-size
//! batch, never once per message, and writes each batch's vectors to
//! `messages` in one column-update commit.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use candle_core::{DType, Device, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::xlm_roberta::{Config, XLMRobertaModel};
use tokenizers::Tokenizer;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

use crate::sessions::{EmbeddedMessage, PendingMessage, Store, embedding_dim};

/// e5's training context. The tokenizer truncates input past it before
/// inference - one message, one vector, bounded embed cost.
pub(crate) const MAX_TOKENS: usize = 512;

/// The candle e5 backend: XLM-RoBERTa FP16 weights on the GPU (Metal on
/// macOS, CUDA on a `cuda`-feature non-macOS build, CPU otherwise).
/// `forward` is `&self`, so no interior mutability is needed.
pub struct CandleEmbedder {
    model: XLMRobertaModel,
    tokenizer: Tokenizer,
    device: Device,
}

impl CandleEmbedder {
    /// Load the configured XLM-RoBERTa model from HuggingFace (cached after
    /// the first download) onto the best available device.
    pub fn load() -> Result<Self> {
        let device = select_device();
        let id = model_id();
        let api = hf_hub::api::sync::Api::new().context("init HuggingFace hub client")?;
        let repo = api.model(id.to_owned());
        let fetch = |file: &str| {
            repo.get(file)
                .with_context(|| format!("fetch {file} for {id}"))
        };

        let config: Config =
            serde_json::from_str(&std::fs::read_to_string(fetch("config.json")?)?)?;
        if config.hidden_size != embedding_dim() {
            return Err(anyhow!(
                "[embeddings].dim = {} but model {id:?} reports hidden_size = {}; \
                 set [embeddings].dim to match the model's output width.",
                embedding_dim(),
                config.hidden_size,
            ));
        }
        // mmap the safetensors file: candle's `safetensors::load` path uses
        // `std::fs::read` which retains an owned `Vec<u8>` of the full FP32
        // weights in the system allocator after drop on macOS. mmap avoids
        // the owned-heap path. Note: candle's Metal pool retains FP32->F16
        // cast transients regardless (iokit_mapped contribution to
        // phys_footprint, candle-core/src/metal_backend/device.rs:44-57).
        let model_path = fetch("model.safetensors")?;
        #[allow(unsafe_code)]
        let vb =
            unsafe { VarBuilder::from_mmaped_safetensors(&[model_path], DType::F16, &device)? };
        let model = XLMRobertaModel::new(&config, vb)
            .map_err(|error| anyhow!("load {id} weights: {error}"))?;

        let mut tokenizer = Tokenizer::from_file(fetch("tokenizer.json")?)
            .map_err(|error| anyhow!("load e5 tokenizer: {error}"))?;
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            strategy: tokenizers::PaddingStrategy::BatchLongest,
            pad_id: config.pad_token_id,
            ..Default::default()
        }));
        tokenizer
            .with_truncation(Some(tokenizers::TruncationParams {
                max_length: MAX_TOKENS,
                ..Default::default()
            }))
            .map_err(|error| anyhow!("configure e5 tokenizer: {error}"))?;

        tracing::info!(model = %id, device = device_label(&device), "loaded embedding model");
        Ok(Self {
            model,
            tokenizer,
            device,
        })
    }
}

impl Embedder for CandleEmbedder {
    fn device(&self) -> &str {
        device_label(&self.device)
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|error| anyhow!("tokenize embedding batch: {error}"))?;
        let mut ids = Vec::with_capacity(encodings.len());
        let mut masks = Vec::with_capacity(encodings.len());
        for encoding in &encodings {
            ids.push(Tensor::new(encoding.get_ids(), &self.device)?);
            masks.push(Tensor::new(encoding.get_attention_mask(), &self.device)?);
        }
        let input_ids = Tensor::stack(&ids, 0)?;
        let attention_mask = Tensor::stack(&masks, 0)?;
        let token_type_ids = input_ids.zeros_like()?;
        let hidden = self
            .model
            .forward(
                &input_ids,
                &attention_mask,
                &token_type_ids,
                None,
                None,
                None,
            )?
            .to_dtype(DType::F32)?;
        let mask = attention_mask.to_dtype(DType::F32)?.unsqueeze(2)?;
        let summed = hidden.broadcast_mul(&mask)?.sum(1)?;
        let counts = mask.sum(1)?;
        let mean = summed.broadcast_div(&counts)?;
        let norm = mean.sqr()?.sum_keepdim(1)?.sqrt()?;
        mean.broadcast_div(&norm)?
            .to_vec2::<f32>()
            .map_err(|error| anyhow!("read embedding vectors: {error}"))
    }
}

fn select_device() -> Device {
    #[cfg(target_os = "macos")]
    let device = Device::metal_if_available(0);
    #[cfg(not(target_os = "macos"))]
    let device = Device::cuda_if_available(0);
    device.unwrap_or_else(|error| {
        tracing::warn!(%error, "GPU device unavailable, falling back to CPU");
        Device::Cpu
    })
}

fn device_label(device: &Device) -> &'static str {
    match device {
        Device::Cpu => "cpu",
        Device::Cuda(_) => "cuda",
        Device::Metal(_) => "metal",
    }
}

/// Arc-shared factory used by [`LazyEmbedder`] to build the backend on
/// first call (or on reload after idle eviction). Arc so the loader can be
/// cloned into `spawn_blocking` without consuming `&self`.
type EmbedLoader = Arc<dyn Fn() -> Result<Arc<dyn Embedder>> + Send + Sync>;

/// How long the cached backend can sit unused before [`LazyEmbedder::get`]
/// drops it. Five minutes matches typical interactive-MCP conversational
/// pauses: short enough that a model that's been unused for a turn or two
/// is gone before the next quiet window, long enough that ordinary
/// query bursts never pay the reload cost.
pub const DEFAULT_IDLE_EVICTION: Duration = Duration::from_secs(300);

struct CachedBackend {
    backend: Arc<dyn Embedder>,
    last_use: Instant,
}

/// Lazy holder for an [`Embedder`] with idle eviction. The model isn't
/// loaded until the first hybrid/vector call asks for it - idle `pond mcp`
/// / `pond serve` processes pay nothing while no vector queries land. After
/// `idle_threshold` of inactivity the cached backend is dropped on the
/// next `get` call; under macOS `phys_footprint` the drop reclaims
/// ~365-585 MiB cleanly (the post-drop floor is ~107 MiB regardless of
/// backend). Reload cost is one synchronous model-load (300-500 ms),
/// absorbed inside the human-paced gap between MCP queries.
pub struct LazyEmbedder {
    loader: EmbedLoader,
    state: Mutex<Option<CachedBackend>>,
    idle_threshold: Duration,
}

impl LazyEmbedder {
    /// candle XLM-RoBERTa FP16 (Metal on macOS / CUDA with `--features cuda`
    /// / CPU otherwise). The pond default for every entry point.
    pub fn candle() -> Self {
        Self::with_loader(Arc::new(|| {
            Ok(Arc::new(CandleEmbedder::load()?) as Arc<dyn Embedder>)
        }))
    }

    /// Build a `LazyEmbedder` from an explicit loader. Used by the bench
    /// harness to override the idle threshold; production callers use
    /// [`Self::candle`].
    pub fn with_loader(loader: EmbedLoader) -> Self {
        Self {
            loader,
            state: Mutex::new(None),
            idle_threshold: DEFAULT_IDLE_EVICTION,
        }
    }

    /// Override the idle-eviction threshold. Pass `Duration::MAX` to disable
    /// eviction entirely - useful in benches that want a stable steady-state.
    #[must_use]
    pub fn with_idle_threshold(mut self, threshold: Duration) -> Self {
        self.idle_threshold = threshold;
        self
    }

    /// Pre-seed with an already-constructed backend. Used by integration
    /// tests that want to inject a fake `Embedder` without paying the real
    /// model-load cost. Eviction is disabled so the test fake survives the
    /// whole test even if a test stalls.
    pub fn from_loaded(backend: Arc<dyn Embedder>) -> Self {
        let preloaded = Arc::clone(&backend);
        let loader: EmbedLoader = Arc::new(move || Ok(Arc::clone(&preloaded)));
        Self {
            loader,
            state: Mutex::new(Some(CachedBackend {
                backend,
                last_use: Instant::now(),
            })),
            idle_threshold: Duration::MAX,
        }
    }

    /// Load (on first call or after eviction) or return the cached handle.
    /// The candle load is synchronous and blocking, so it runs on
    /// `spawn_blocking`; the async caller sees a clean `await` point.
    pub async fn get(&self) -> Result<Arc<dyn Embedder>> {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if let Some(cached) = &*state
            && now.duration_since(cached.last_use) > self.idle_threshold
        {
            tracing::info!(
                idle_secs = self.idle_threshold.as_secs(),
                "evicting idle embedder",
            );
            *state = None;
        }
        if let Some(cached) = state.as_mut() {
            cached.last_use = now;
            return Ok(Arc::clone(&cached.backend));
        }
        let loader = Arc::clone(&self.loader);
        let backend = tokio::task::spawn_blocking(move || loader())
            .await
            .map_err(|join_error| anyhow!("embedder load panicked: {join_error}"))??;
        *state = Some(CachedBackend {
            backend: Arc::clone(&backend),
            last_use: now,
        });
        Ok(backend)
    }
}

/// Default embedding model pond ships a loader for (spec.md#search). Used when
/// `[embeddings].model` is absent. `pond embed` stamps the runtime model id
/// (see [`model_id`]) into `messages.embedding_model` with every vector.
/// e5-small (384-dim) is the default; the paraphrase benchmark set showed no
/// statistically-significant quality loss vs e5-base while halving vector
/// storage and ~halving model RSS.
pub const DEFAULT_MODEL_ID: &str = "intfloat/multilingual-e5-small";

/// Process-wide model id, seeded once at startup from `[embeddings].model` via
/// [`init_model_id`]. `OnceLock` (not `const`) so a temporary config file can
/// pick e5-small / e5-large for an experiment without touching every call site.
/// Uninitialized -> [`DEFAULT_MODEL_ID`], keeping unit tests config-free.
static MODEL_ID_RUNTIME: OnceLock<String> = OnceLock::new();

/// The active model id. Returns the value installed by [`init_model_id`] or
/// [`DEFAULT_MODEL_ID`] when nothing has installed one (tests, ad-hoc tooling).
pub fn model_id() -> &'static str {
    MODEL_ID_RUNTIME
        .get()
        .map(String::as_str)
        .unwrap_or(DEFAULT_MODEL_ID)
}

/// Seed [`model_id`] from config. First call wins; later calls with a different
/// id are silently ignored - the process loads its config once.
pub fn init_model_id(id: String) {
    MODEL_ID_RUNTIME.get_or_init(|| id);
}

/// Messages per model-inference + write batch. e5 truncates at 512 tokens, so
/// a 32-row batch's padded attention transient stays bounded.
pub const DEFAULT_BATCH_SIZE: usize = 32;

/// Messages buffered and length-sorted before being cut into model batches.
/// The tokenizer pads every batch to its longest member, so a batch mixing a short
/// and a long message embeds the short one at the long one's length. Sorting a
/// window first clusters similar-length messages, so each batch pads near its
/// own longest, not the corpus worst case. Bounded so peak memory stays one
/// window, not the whole backlog. See [`EmbedWorker::with_sort_window`].
pub const DEFAULT_SORT_WINDOW: usize = 2048;

/// Format a search query for the embedder. e5 is an asymmetric retriever:
/// its model card prescribes `query: ` on the search side, `passage: ` on
/// documents. Used by `pond_search` to prepare the query text before the
/// candle/Metal embed call.
pub fn format_query(query: &str) -> String {
    format!("query: {query}")
}

/// Format a document (one message's `search_text`) for the embedder - the
/// `passage: ` half of the pair documented on [`format_query`]. Used by
/// `EmbedWorker` when batching messages for `pond embed`.
pub fn format_passage(text: &str) -> String {
    format!("passage: {text}")
}

/// The embedding seam (spec.md#search): text in, vectors out. The real
/// backend is [`CandleEmbedder`]; tests substitute an instrumented fake
/// to assert batching behavior. The vector width is checked at the write
/// boundary and the model id is whatever [`model_id`] returns at the
/// time of the write.
pub trait Embedder: Send + Sync {
    /// A short label naming the hardware/runtime: `"metal"`, `"cuda"`,
    /// or `"cpu"`. Used by `pond embed` to surface what backend ran the
    /// inference; benches print it alongside latency.
    fn device(&self) -> &str;

    /// Embed a batch of texts. The returned vectors are L2-normalized and
    /// [`embedding_dim`] long, one per input.
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
}

/// Outcome of an [`EmbedWorker::run`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmbedSummary {
    /// Messages embedded; one vector each.
    pub messages: usize,
    /// Model-inference + write batches issued.
    pub batches: usize,
    /// Set when the run exited via the cancel flag instead of stream end -
    /// the caller uses this to print an interrupted notice and decide whether
    /// to still rebuild downstream indices.
    pub cancelled: bool,
}

/// Per-batch stats handed to a progress callback. Lets `pond embed` drive an
/// `indicatif` bar without leaking the crate into this module's API.
#[derive(Debug, Clone, Copy)]
pub struct BatchProgress {
    /// Messages embedded in this batch.
    pub batch_messages: usize,
    /// Running message total across the run.
    pub total_messages: usize,
    /// Running batch count across the run.
    pub total_batches: usize,
}

type ProgressFn = Box<dyn Fn(BatchProgress) + Send + Sync>;

/// Fills `messages.vector` / `messages.embedding_model` for the backlog of
/// un-embedded messages. Reads `messages.search_text` directly, batches it
/// through the backend one vector each, and writes each batch back to
/// `messages` by primary key.
pub struct EmbedWorker<'a, B: Embedder> {
    store: &'a Store,
    backend: &'a B,
    include_stale: bool,
    /// Optional cap on total messages embedded in one `run` - `None` in
    /// production (embed everything), set by the benchmark harness to a fixed
    /// count so a run is a stable, comparable workload.
    limit: Option<usize>,
    /// Messages buffered and length-sorted per `drain_window` pass
    /// ([`DEFAULT_SORT_WINDOW`]); the benchmark sweeps it through
    /// [`EmbedWorker::with_sort_window`].
    sort_window: usize,
    /// Optional per-batch progress callback. Called once per `flush()` with
    /// the running totals; `pond embed` wires this to an `indicatif` bar.
    progress: Option<ProgressFn>,
    /// Set externally (Ctrl-C handler in `pond embed`): the pull loop drains
    /// the in-memory window before exiting so partial work is committed.
    cancel: Option<Arc<AtomicBool>>,
}

impl<'a, B: Embedder> EmbedWorker<'a, B> {
    /// Build a worker over `store`'s un-embedded backlog. A backend whose
    /// vectors are the wrong width is rejected at the write boundary
    /// (`embedding_update_batch`), so there is nothing to validate here.
    pub fn new(store: &'a Store, backend: &'a B) -> Self {
        Self {
            store,
            backend,
            include_stale: false,
            limit: None,
            sort_window: DEFAULT_SORT_WINDOW,
            progress: None,
            cancel: None,
        }
    }

    /// Honour `flag` as a cooperative cancellation signal. The pull loop checks
    /// it before each new stream message; once set, the worker drains the
    /// current window (committing the embedded slice) and returns with
    /// `EmbedSummary { cancelled: true, .. }`. `pond embed` wires this to a
    /// Ctrl-C handler so an interrupted run doesn't lose its in-memory window.
    pub fn with_cancel(mut self, flag: Arc<AtomicBool>) -> Self {
        self.cancel = Some(flag);
        self
    }

    fn cancelled(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(|f| f.load(Ordering::Relaxed))
    }

    /// Override the length-sort window (default [`DEFAULT_SORT_WINDOW`]). The
    /// benchmark harness sweeps this to size the padding-waste vs. throughput
    /// trade-off; a window of [`DEFAULT_BATCH_SIZE`] disables sorting.
    pub fn with_sort_window(mut self, window: usize) -> Self {
        self.sort_window = window.max(DEFAULT_BATCH_SIZE);
        self
    }

    /// Register a per-batch progress callback. Called once after each
    /// `flush()` with the messages in the just-finished batch and the running
    /// totals. `pond embed` uses this to drive an `indicatif` progress bar.
    pub fn with_progress(
        mut self,
        callback: impl Fn(BatchProgress) + Send + Sync + 'static,
    ) -> Self {
        self.progress = Some(Box::new(callback));
        self
    }

    /// Cap the run at `limit` messages (default: no cap). The benchmark harness
    /// uses this to embed a fixed, comparable slice of a corpus.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit.max(1));
        self
    }

    pub fn include_stale(mut self) -> Self {
        self.include_stale = true;
        self
    }

    /// Embed every message whose `vector` is still null. Idempotent: a re-run
    /// over an already-embedded corpus finds an empty backlog and is a no-op.
    ///
    /// Messages are pulled from a streaming scan, so peak memory is one stream
    /// page plus the staged batch - not the whole corpus.
    pub async fn run(&self) -> Result<EmbedSummary> {
        let mut summary = EmbedSummary::default();
        let mut window: Vec<PendingMessage> = Vec::with_capacity(self.sort_window);
        let mut pulled = 0usize;

        let mut stream = if self.include_stale {
            Box::pin(self.store.pending_or_stale_messages())
                as std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<PendingMessage>> + '_>>
        } else {
            Box::pin(self.store.pending_embedding_messages())
                as std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<PendingMessage>> + '_>>
        };
        while let Some(pending) = stream.next().await {
            // Stop pulling once the message cap is reached or cancellation
            // fires; the staged window is still drained below, so the
            // already-embedded slice commits cleanly.
            if self.limit.is_some_and(|limit| pulled >= limit) || self.cancelled() {
                break;
            }
            window.push(pending?);
            pulled += 1;
            if window.len() >= self.sort_window {
                self.drain_window(&mut window, &mut summary).await?;
            }
        }
        self.drain_window(&mut window, &mut summary).await?;
        summary.cancelled = self.cancelled();

        tracing::info!(
            model = model_id(),
            messages = summary.messages,
            batches = summary.batches,
            cancelled = summary.cancelled,
            "embed worker finished",
        );
        Ok(summary)
    }

    /// One `merge_update` per window, not per 32-row batch: each
    /// `merge_update` streams the target column once, so amortizing it over
    /// a window-sized batch beats issuing it per model batch. The
    /// length-sort clusters similar lengths because the tokenizer pads each
    /// batch to its longest member. Empties `window`.
    async fn drain_window(
        &self,
        window: &mut Vec<PendingMessage>,
        summary: &mut EmbedSummary,
    ) -> Result<()> {
        if window.is_empty() {
            return Ok(());
        }
        window.sort_unstable_by_key(|message| message.search_text.len());
        let mut batch: Vec<PendingMessage> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        let mut accumulator: Vec<EmbeddedMessage> = Vec::with_capacity(window.len());
        for message in window.drain(..) {
            batch.push(message);
            if batch.len() >= DEFAULT_BATCH_SIZE {
                accumulator.extend(self.embed_batch(&mut batch, summary).await?);
            }
        }
        accumulator.extend(self.embed_batch(&mut batch, summary).await?);
        if !accumulator.is_empty() {
            self.store.write_embeddings(&accumulator).await?;
        }
        Ok(())
    }

    /// Run one model batch; return the rows. Store write is batched in
    /// [`drain_window`](Self::drain_window), one `merge_update` per window.
    async fn embed_batch(
        &self,
        batch: &mut Vec<PendingMessage>,
        summary: &mut EmbedSummary,
    ) -> Result<Vec<EmbeddedMessage>> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let pending = std::mem::take(batch);
        // Apply e5's `passage: ` document prefix at the model boundary; the
        // stored `search_text` keeps its uncapped, unprefixed form for FTS.
        let texts = pending
            .iter()
            .map(|message| format_passage(&message.search_text))
            .collect::<Vec<_>>();
        let vectors = self.backend.embed(&texts)?;
        if vectors.len() != pending.len() {
            return Err(anyhow!(
                "backend returned {} vectors for {} messages",
                vectors.len(),
                pending.len(),
            ));
        }
        let rows = pending
            .into_iter()
            .zip(vectors)
            .map(|(message, vector)| EmbeddedMessage {
                session_id: message.session_id,
                id: message.id,
                vector,
            })
            .collect::<Vec<_>>();
        let batch_messages = rows.len();
        summary.messages += batch_messages;
        summary.batches += 1;
        if let Some(progress) = &self.progress {
            progress(BatchProgress {
                batch_messages,
                total_messages: summary.messages,
                total_batches: summary.batches,
            });
        }
        Ok(rows)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[test]
    fn e5_prefixes_apply_the_asymmetric_retrieval_pair() {
        assert_eq!(
            format_query("how does retry backoff work"),
            "query: how does retry backoff work",
        );
        assert_eq!(
            format_passage("retry uses exponential backoff"),
            "passage: retry uses exponential backoff",
        );
    }

    /// Counts how many times `LazyEmbedder` invokes its loader. Lets the
    /// idle-eviction test detect reloads without spinning up a real model.
    struct CountingEmbedder;
    impl Embedder for CountingEmbedder {
        fn device(&self) -> &str {
            "test"
        }
        fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>> {
            Ok(vec![])
        }
    }

    /// `LazyEmbedder` keys eviction on `std::time::Instant`, which isn't
    /// affected by `tokio::time::pause`. The test uses a tiny real
    /// threshold so the suite runs in <100 ms.
    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_embedder_evicts_after_idle_threshold() {
        let loads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&loads);
        let loader: EmbedLoader = Arc::new(move || {
            counter.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Arc::new(CountingEmbedder) as Arc<dyn Embedder>)
        });
        let embedder =
            LazyEmbedder::with_loader(loader).with_idle_threshold(Duration::from_millis(20));

        embedder.get().await.unwrap();
        assert_eq!(
            loads.load(AtomicOrdering::SeqCst),
            1,
            "first get loads once"
        );

        embedder.get().await.unwrap();
        assert_eq!(
            loads.load(AtomicOrdering::SeqCst),
            1,
            "back-to-back get reuses the cached backend",
        );

        tokio::time::sleep(Duration::from_millis(60)).await;
        embedder.get().await.unwrap();
        assert_eq!(
            loads.load(AtomicOrdering::SeqCst),
            2,
            "get after the idle threshold triggers a reload",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_embedder_from_loaded_never_evicts() {
        let preloaded = LazyEmbedder::from_loaded(Arc::new(CountingEmbedder));
        preloaded.get().await.unwrap();
        // Wait past any reasonable threshold; the from_loaded path uses
        // Duration::MAX so the fake stays alive for the whole test.
        tokio::time::sleep(Duration::from_millis(60)).await;
        preloaded.get().await.unwrap();
    }
}
