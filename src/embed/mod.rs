//! The embedding stage of `pond ingest`: the Qwen3 candle backend and the
//! batch-oriented worker that populates the `embeddings` dataset. One message
//! produces one vector - there is no chunking.
//!
//! Batching is load-bearing (plan.md Stage 2): the worker accumulates messages
//! and calls the model once per batch, never once per message. The same rule
//! applies to the Lance write path - embedding rows are written in batches,
//! never one `merge_insert` per message.

use anyhow::{Result, anyhow};
use lance::index::vector::VectorIndexParams;
use lance_linalg::distance::MetricType;
use tokio_stream::StreamExt;

use crate::{
    config::{Distance, EmbeddingModel},
    sessions::{EmbeddingRow, PendingMessage, Store},
};

pub mod qwen3;
pub use qwen3::Qwen3Embedder;

/// Default ceiling on the number of messages in one model-inference + write
/// batch. The cost budget below is the other, usually-binding, limiter.
pub const DEFAULT_BATCH_SIZE: usize = 32;

/// Default per-batch attention-cost budget, in `token^2` units.
///
/// A padded batch's dominant transient is the `[batch, heads, seq, seq]`
/// attention scores tensor; `seq` is the *longest* member of the batch because
/// the tokenizer pads the batch to its longest sequence. So the tensor scales
/// with `batch_count * max_seq_len^2` - and a fixed *count* batch of length-
/// heterogeneous messages is a memory trap: one long message padded together
/// with 31 short ones allocates tens of GB and wedges the process. Budgeting
/// `count * max_seq_len^2` instead makes a long message fall into its own small
/// batch rather than dragging short ones up to its length.
///
/// Sized so the bf16 attention tensor stays well under ~1.5 GB even for a
/// 32-head model: `budget * heads * 2 bytes` = `24M * 32 * 2` ~= 1.5 GB; for
/// the 16-head Qwen3-0.6B default it is ~0.77 GB. A single message capped at
/// the default `max_embed_tokens` (1024) costs `1024^2` ~= 1.05M, so ~22 such
/// messages still fit one batch; the count ceiling is the usual limiter and
/// this budget is the safety net that catches a long message before it drags
/// a whole count-sized batch up to its length.
///
/// [`crate::config::EmbeddingsConfig::validate`] rejects any configured
/// `max_embed_tokens` whose single-message cost (`max_embed_tokens^2`) exceeds
/// this budget - cost-aware batching cannot split a single message, so a
/// message must always fit one batch on its own.
pub const DEFAULT_BATCH_TOKEN_SQ_BUDGET: usize = 24_000_000;

/// Default number of pending messages buffered and length-sorted before
/// batching.
///
/// fastembed pads every batch to its longest member, so a batch mixing a long
/// message with short ones embeds the short ones at the long one's length -
/// pure wasted compute. The fix is the standard one (see `sentence-transformers`
/// `SentenceTransformer.encode`): sort by length so each batch holds
/// similar-length inputs. `encode` sorts *all* inputs because it holds them in
/// memory; pond streams (peak memory is a window, not the whole corpus), so it
/// sorts within a bounded window instead - large enough to be a representative
/// length sample, small enough to stay in the streaming memory profile (a
/// window is on the order of a few Lance scan pages). Unlike `encode`, pond
/// needs no un-sort: embeddings are keyed by `(message_id, model_id)`, so the
/// order they are produced in does not matter.
pub const DEFAULT_LENGTH_WINDOW: usize = 4096;

/// Default cap on the *bytes* buffered in one length-sort window - the other
/// drain trigger besides [`DEFAULT_LENGTH_WINDOW`]'s message count. `search_text`
/// is buffered untruncated (truncation happens inside the model call), so
/// without a byte cap a window landing on a run of very large messages could
/// spike host RSS far above the streaming memory profile. 64 MiB bounds it
/// regardless of the corpus's message-size distribution.
pub const DEFAULT_WINDOW_BYTE_BUDGET: usize = 64 * 1024 * 1024;

/// Bytes-per-token ratio for the *sort* length estimate. Typical for code and
/// prose (~3-4 bytes/token); this is only a relative ordering proxy, not a
/// safety bound - see [`cost_upper_bound`] for the bound the budget enforces.
const BYTES_PER_TOKEN_ESTIMATE: usize = 3;

pub fn index_params(model: &EmbeddingModel, num_rows: usize) -> VectorIndexParams {
    VectorIndexParams::ivf_pq(
        ivf_num_partitions(num_rows),
        8,
        model.num_sub_vectors,
        metric_type(model.distance),
        15,
    )
}

pub fn metric_type(distance: Distance) -> MetricType {
    match distance {
        Distance::Cosine => MetricType::Cosine,
        Distance::L2 => MetricType::L2,
        Distance::Dot => MetricType::Dot,
    }
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

/// A message's relative length, the key the length-sort window is ordered on.
/// Bytes over the typical bytes-per-token ratio: a cheap monotonic proxy that
/// groups similar-length messages so each batch pads to barely above its own
/// members. Clamped to `max_embed_tokens` since the tokenizer truncates there
/// (past that point messages are all the same effective length). This is *not*
/// a safety bound: it can under-count token-dense input, so it is never used
/// for the cost budget (that is [`cost_upper_bound`]'s job).
fn estimate_tokens(text: &str, max_embed_tokens: usize) -> usize {
    text.len()
        .div_ceil(BYTES_PER_TOKEN_ESTIMATE)
        .clamp(1, max_embed_tokens)
}

/// A *conservative upper bound* on a message's post-truncation token count, for
/// the attention-cost budget. Byte-level BPE emits at least one byte per token,
/// so byte length is always >= the real token count; clamped to
/// `max_embed_tokens` because the tokenizer truncates there. Unlike
/// [`estimate_tokens`] this can never *under*-count, so a batch kept within
/// `count * bound^2` is a true memory bound, not a heuristic one.
fn cost_upper_bound(text: &str, max_embed_tokens: usize) -> usize {
    text.len().clamp(1, max_embed_tokens)
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

    /// The registry id of the model this backend embeds with - the `model_id`
    /// PK component on the `embeddings` table. Vector search scopes its scan to
    /// `(model_id, max_embed_tokens)` so it never mixes in vectors from another
    /// model or another cap (which are distinct rows under the same key).
    fn model_id(&self) -> &str;

    /// The `max_embed_tokens` cap this backend embeds under - the other
    /// `embeddings` PK identity component (see [`model_id`](Self::model_id)).
    fn max_embed_tokens(&self) -> i32;
}

/// Outcome of an [`EmbedWorker::run`] pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmbedSummary {
    /// Messages that had pending (un-embedded) `search_text`; one vector each.
    pub messages: usize,
    /// Model-inference + write batches issued.
    pub batches: usize,
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

/// Populates the `embeddings` dataset for one registry model. Reads
/// `messages.search_text` directly (no second concatenation path), batches
/// messages through the backend one vector each, and writes embedding rows in
/// batches.
pub struct EmbedWorker<'a, B: EmbedBackend> {
    store: &'a Store,
    backend: &'a B,
    model_id: String,
    /// Token cap applied per message before estimating its batch cost; mirrors
    /// the fastembed tokenizer's truncation point.
    max_embed_tokens: usize,
    /// Hard ceiling on messages per batch.
    batch_size: usize,
    /// Co-batching threshold on `batch_count * max_seq_len^2`: a message is
    /// flushed into its own batch rather than added to a staged batch that
    /// would exceed this. Not an absolute ceiling - a single message that on
    /// its own exceeds the budget is still embedded (cost-aware batching cannot
    /// split one message). [`crate::config::EmbeddingsConfig::validate`] keeps
    /// the production path safe by rejecting any `max_embed_tokens` whose
    /// single-message cost exceeds [`DEFAULT_BATCH_TOKEN_SQ_BUDGET`].
    cost_budget: usize,
    /// Pending messages buffered and length-sorted before batching, so each
    /// batch holds similar-length messages and padding waste stays low.
    window_size: usize,
    /// Byte cap on one buffered window before a forced drain - the memory guard
    /// pairing with `window_size`'s count guard, since the window holds
    /// untruncated `search_text`.
    window_byte_budget: usize,
    /// Optional cap on total messages embedded in one `run` - `None` in
    /// production (embed everything), set by the benchmark harness to a fixed
    /// count so a run is a stable, comparable workload.
    limit: Option<usize>,
    /// Optional per-batch progress callback. Called once per `flush()` with
    /// the running totals; `pond embed` wires this to an `indicatif` bar.
    progress: Option<ProgressFn>,
}

impl<'a, B: EmbedBackend> EmbedWorker<'a, B> {
    /// Build a worker for `model`. The backend's [`dim`](EmbedBackend::dim) must
    /// match the model's declared `dim`.
    pub fn new(store: &'a Store, backend: &'a B, model: &EmbeddingModel) -> Result<Self> {
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
            max_embed_tokens: model.max_embed_tokens,
            batch_size: DEFAULT_BATCH_SIZE,
            cost_budget: DEFAULT_BATCH_TOKEN_SQ_BUDGET,
            window_size: DEFAULT_LENGTH_WINDOW,
            window_byte_budget: DEFAULT_WINDOW_BYTE_BUDGET,
            limit: None,
            progress: None,
        })
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

    /// Override the message-count ceiling per batch (default
    /// [`DEFAULT_BATCH_SIZE`]). The cost budget is usually the binding limiter.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Override the per-batch attention-cost co-batching threshold (default
    /// [`DEFAULT_BATCH_TOKEN_SQ_BUDGET`]). Tests use a small value to exercise
    /// the cost-driven split without large inputs. Note this is a co-batching
    /// threshold, not an absolute ceiling: a single message whose own cost
    /// exceeds the budget is still embedded alone (see the `cost_budget` field).
    pub fn with_cost_budget(mut self, cost_budget: usize) -> Self {
        self.cost_budget = cost_budget.max(1);
        self
    }

    /// Override the length-sort window (default [`DEFAULT_LENGTH_WINDOW`]).
    /// Bigger windows give better length-locality at higher buffered memory.
    pub fn with_window_size(mut self, window_size: usize) -> Self {
        self.window_size = window_size.max(1);
        self
    }

    /// Override the per-window byte budget (default
    /// [`DEFAULT_WINDOW_BYTE_BUDGET`]). The window drains on whichever of the
    /// count cap or this byte cap is hit first.
    pub fn with_window_byte_budget(mut self, window_byte_budget: usize) -> Self {
        self.window_byte_budget = window_byte_budget.max(1);
        self
    }

    /// Cap the run at `limit` messages (default: no cap). The benchmark harness
    /// uses this to embed a fixed, comparable slice of a corpus.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit.max(1));
        self
    }

    /// Embed every message with `search_text` that does not yet have an
    /// embedding row for this model. Idempotent: the PK is `(message_id,
    /// model_id)`, so a re-run over an already-embedded corpus is a no-op.
    ///
    /// Messages are pulled from a streaming scan, so peak memory is one stream
    /// page plus the staged batch - not the whole corpus.
    pub async fn run(&self) -> Result<EmbedSummary> {
        let embedded = self
            .store
            .embedded_message_ids(&self.model_id, self.max_embed_tokens as i32)
            .await?;
        let mut summary = EmbedSummary::default();

        // Buffer pending messages into a window; once it is full, length-sort
        // and batch it (see `drain_window`). Sorting within the window keeps
        // each batch's messages similar-length, so fastembed's pad-to-longest
        // does not blow short messages up to a long one's length.
        let mut window: Vec<StagedMessage> = Vec::with_capacity(self.window_size);
        let mut window_bytes = 0usize;
        let stream = self.store.pending_messages_stream();
        tokio::pin!(stream);
        'pull: while let Some(pending) = stream.next().await {
            let mut pending = pending?;
            if embedded.contains(&pending.message_id) {
                continue;
            }
            summary.messages += 1;
            // Move `search_text` out of the message: it is handed to the
            // backend, and the embedding row never carries it.
            let text = std::mem::take(&mut pending.search_text);
            window_bytes += text.len();
            let tokens = estimate_tokens(&text, self.max_embed_tokens);
            let cost_tokens = cost_upper_bound(&text, self.max_embed_tokens);
            window.push(StagedMessage {
                message: pending,
                text,
                tokens,
                cost_tokens,
            });

            // Drain on whichever fills first: the count cap bounds the sort
            // cost, the byte cap bounds host RSS (the window holds
            // untruncated `search_text`, so a run of large messages could
            // otherwise spike memory before any model call).
            if window.len() >= self.window_size || window_bytes >= self.window_byte_budget {
                self.drain_window(&mut window, &mut summary).await?;
                window_bytes = 0;
            }

            // Stop pulling once the message cap is reached; the window is
            // still drained below, so exactly `limit` messages are embedded.
            if self.limit.is_some_and(|limit| summary.messages >= limit) {
                break 'pull;
            }
        }
        self.drain_window(&mut window, &mut summary).await?;

        tracing::info!(
            model = %self.model_id,
            messages = summary.messages,
            batches = summary.batches,
            "embed worker finished",
        );
        Ok(summary)
    }

    /// Length-sort a window of pending messages, then carve it into batches by
    /// the count ceiling and the attention-cost budget. Sorting first - the
    /// `sentence-transformers` `encode` technique - keeps each batch
    /// length-homogeneous, so the model embeds little padding. Empties `window`.
    async fn drain_window(
        &self,
        window: &mut Vec<StagedMessage>,
        summary: &mut EmbedSummary,
    ) -> Result<()> {
        if window.is_empty() {
            return Ok(());
        }
        // Ascending by estimated token length: consecutive messages are
        // similar-length, so each batch carved off below pads to barely above
        // its own members' lengths. pond needs no un-sort (unlike `encode`) -
        // embedding rows are keyed by `(message_id, model_id)`.
        window.sort_unstable_by_key(|message| message.tokens);

        let mut staged: Vec<StagedMessage> = Vec::with_capacity(self.batch_size);
        // Running max of the *conservative* per-message bound across the staged
        // batch. The cost budget is enforced on this, never on the sort estimate
        // - `cost_tokens` cannot under-count, so the batch is a true memory bound.
        let mut staged_max_cost = 0usize;
        for message in window.drain(..) {
            // Flush the staged batch first if adding this message would overflow
            // the count ceiling or the cost budget. A single message always
            // gets staged (the check is skipped when `staged` is empty), so an
            // oversized message simply becomes its own batch.
            if !staged.is_empty() {
                let projected_max = staged_max_cost.max(message.cost_tokens);
                let projected_cost = (staged.len() + 1)
                    .saturating_mul(projected_max)
                    .saturating_mul(projected_max);
                if staged.len() >= self.batch_size || projected_cost > self.cost_budget {
                    self.flush(&mut staged, summary).await?;
                    staged_max_cost = 0;
                }
            }
            staged_max_cost = staged_max_cost.max(message.cost_tokens);
            staged.push(message);
        }
        self.flush(&mut staged, summary).await?;
        Ok(())
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
                max_embed_tokens: self.max_embed_tokens as i32,
                vector,
                session_id: message.session_id,
                source_agent: message.source_agent,
                project: message.project,
                role: message.role,
                timestamp: message.timestamp,
            });
        }

        let batch_messages = rows.len();
        self.store.upsert_embeddings(&rows).await?;
        summary.batches += 1;
        if let Some(progress) = &self.progress {
            progress(BatchProgress {
                batch_messages,
                total_messages: summary.messages,
                total_batches: summary.batches,
            });
        }
        Ok(())
    }
}

struct StagedMessage {
    message: PendingMessage,
    text: String,
    /// Relative length estimate - the key the window is sorted on.
    tokens: usize,
    /// Conservative upper bound on token count - what the cost budget is
    /// enforced against, so the budget can never be under-counted past.
    cost_tokens: usize,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn qwen3_query_instruction_wraps_the_query_in_the_model_card_prefix() {
        let prompt = qwen3_query_instruction("how does retry backoff work");
        // Model-card format: `Instruct: {task}\nQuery: {query}` - the query sits on
        // its own line after the instruction and is never mutated, only prefixed.
        assert!(prompt.starts_with("Instruct: "));
        assert!(prompt.ends_with("\nQuery: how does retry backoff work"));
    }

    #[test]
    fn metric_type_maps_each_registry_distance() {
        assert_eq!(metric_type(Distance::Cosine), MetricType::Cosine);
        assert_eq!(metric_type(Distance::L2), MetricType::L2);
        assert_eq!(metric_type(Distance::Dot), MetricType::Dot);
    }
}
