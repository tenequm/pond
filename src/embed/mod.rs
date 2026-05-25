//! The embedding stage: the e5 candle/Metal backend and the batch-oriented
//! worker that fills `messages.vector` / `messages.embedding_model`
//! (spec.md#search). One message produces one vector - there is no chunking.
//!
//! The worker accumulates messages and calls the model once per fixed-size
//! batch, never once per message, and writes each batch's vectors to
//! `messages` in one column-update commit.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Result, anyhow};
use lance::index::vector::VectorIndexParams;
use lance_linalg::distance::MetricType;
use tokio::sync::OnceCell;
use tokio_stream::StreamExt;

use crate::sessions::{EMBEDDING_DIM, EmbeddedMessage, PendingMessage, Store};

pub mod e5;
pub use e5::E5Embedder;

/// Lazy holder for the embedding backend used by long-running `pond serve` /
/// `pond mcp`: the model isn't loaded until the first hybrid search asks for
/// it. Idle `pond mcp` keeps RSS down to ~50 MB; first search triggers the
/// candle/Metal load (~2-4s) and the loaded handle is cached for the life of
/// the process. `pond embed` and `pond search` (one-shot CLI) load eagerly
/// via [`E5Embedder::load`] directly.
pub struct LazyEmbedder {
    enabled: bool,
    cell: OnceCell<Arc<dyn EmbedBackend>>,
}

impl LazyEmbedder {
    /// Build a lazy handle. `enabled` mirrors `config.embeddings.enabled`;
    /// when false, `get` always returns `None` and never loads a model.
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            cell: OnceCell::new(),
        }
    }

    /// Pre-seed with an already-constructed backend (tests, one-shot eager
    /// paths). The cell is filled; subsequent `get` calls return this handle.
    pub fn from_loaded(backend: Arc<dyn EmbedBackend>) -> Self {
        let cell = OnceCell::new();
        let _ = cell.set(backend);
        Self {
            enabled: true,
            cell,
        }
    }

    /// Cheap, sync: is the embedder configured? `pond search`'s mode-resolution
    /// uses this to decide hybrid-vs-FTS without paying a model-load cost on
    /// FTS-only queries.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Load (on first call) or return the cached handle. Returns `None` when
    /// the config has embeddings disabled. The candle load is synchronous and
    /// blocking, so it runs on `spawn_blocking`; the async caller sees a clean
    /// `await` point.
    pub async fn get(&self) -> Result<Option<Arc<dyn EmbedBackend>>> {
        if !self.enabled {
            return Ok(None);
        }
        let handle = self
            .cell
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(|| {
                    E5Embedder::load().map(|backend| Arc::new(backend) as Arc<dyn EmbedBackend>)
                })
                .await
                .map_err(|join_error| anyhow!("embedder load panicked: {join_error}"))?
            })
            .await?;
        Ok(Some(handle.clone()))
    }
}

/// The one embedding model pond ships a loader for (spec.md#search).
/// `config.embeddings.model` is validated against this; `pond embed` stamps it
/// into `messages.embedding_model` with every vector.
pub const MODEL_ID: &str = "intfloat/multilingual-e5-base";

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

/// IVF_PQ parameters for the `messages.vector` index, sized to the row count.
/// Cosine metric: e5 vectors are L2-normalized. `num_sub_vectors = dim / 8`
/// gives 8-float PQ subspaces (`EMBEDDING_DIM` is divisible by 8).
pub fn index_params(num_rows: usize) -> VectorIndexParams {
    VectorIndexParams::ivf_pq(
        ivf_num_partitions(num_rows),
        8,
        EMBEDDING_DIM / 8,
        MetricType::Cosine,
        15,
    )
}

fn ivf_num_partitions(num_rows: usize) -> usize {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let sqrt = (num_rows as f64).sqrt().round() as usize;
    sqrt.clamp(32, 4096)
}

/// Prefix a search query for e5. e5 is an asymmetric retriever: its model
/// card prescribes `query: ` on the search side, `passage: ` on documents.
pub fn e5_query(query: &str) -> String {
    format!("query: {query}")
}

/// Prefix a document (one message's `search_text`) for e5 - the
/// `passage: ` half of the pair documented on [`e5_query`].
pub fn e5_passage(text: &str) -> String {
    format!("passage: {text}")
}

/// The embedding seam (spec.md#search): text in, vectors out. The real backend
/// is [`E5Embedder`]; tests substitute an instrumented fake to assert
/// batching behavior. v1 has one model ([`MODEL_ID`]), so the seam needs no
/// dimension or model-id accessor - the vector width is checked at the write
/// boundary and the model id is the [`MODEL_ID`] constant.
pub trait EmbedBackend: Send + Sync {
    /// Embed a batch of texts. The returned vectors are L2-normalized and
    /// `EMBEDDING_DIM` long, one per input.
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
pub struct EmbedWorker<'a, B: EmbedBackend> {
    store: &'a Store,
    backend: &'a B,
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

impl<'a, B: EmbedBackend> EmbedWorker<'a, B> {
    /// Build a worker over `store`'s un-embedded backlog. A backend whose
    /// vectors are the wrong width is rejected at the write boundary
    /// (`embedding_update_batch`), so there is nothing to validate here.
    pub fn new(store: &'a Store, backend: &'a B) -> Self {
        Self {
            store,
            backend,
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

    /// Embed every message whose `vector` is still null. Idempotent: a re-run
    /// over an already-embedded corpus finds an empty backlog and is a no-op.
    ///
    /// Messages are pulled from a streaming scan, so peak memory is one stream
    /// page plus the staged batch - not the whole corpus.
    pub async fn run(&self) -> Result<EmbedSummary> {
        let mut summary = EmbedSummary::default();
        let mut window: Vec<PendingMessage> = Vec::with_capacity(self.sort_window);
        let mut pulled = 0usize;

        let stream = self.store.pending_embedding_messages();
        tokio::pin!(stream);
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
            model = MODEL_ID,
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
            .map(|message| e5_passage(&message.search_text))
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
mod tests {
    use super::*;

    #[test]
    fn e5_prefixes_apply_the_asymmetric_retrieval_pair() {
        assert_eq!(
            e5_query("how does retry backoff work"),
            "query: how does retry backoff work",
        );
        assert_eq!(
            e5_passage("retry uses exponential backoff"),
            "passage: retry uses exponential backoff",
        );
    }
}
