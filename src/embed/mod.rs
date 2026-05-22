//! The embedding stage: the e5-small ONNX backend and the batch-oriented
//! worker that fills `messages.vector` / `messages.embedding_model`
//! (spec.md#search). One message produces one vector - there is no chunking.
//!
//! The worker accumulates messages and calls the model once per fixed-size
//! batch, never once per message, and writes each batch's vectors to
//! `messages` in one column-update commit.

use anyhow::{Result, anyhow};
use lance::index::vector::VectorIndexParams;
use lance_linalg::distance::MetricType;
use tokio_stream::StreamExt;

use crate::sessions::{EMBEDDING_DIM, EmbeddedMessage, PendingMessage, Store};

pub mod e5_small;
pub use e5_small::E5SmallEmbedder;

/// The one embedding model pond ships a loader for (spec.md#search).
/// `config.embeddings.model` is validated against this; `pond embed` stamps it
/// into `messages.embedding_model` with every vector.
pub const MODEL_ID: &str = "intfloat/multilingual-e5-small";

/// Messages per model-inference + write batch. A small fixed batch keeps the
/// padded attention transient bounded without length-sorting machinery: e5
/// truncates at 512 tokens, so even a worst-case batch stays modest.
pub const DEFAULT_BATCH_SIZE: usize = 32;

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

/// Prefix a search query for e5-small. e5 is an asymmetric retriever: its model
/// card prescribes `query: ` on the search side, `passage: ` on documents.
pub fn e5_query(query: &str) -> String {
    format!("query: {query}")
}

/// Prefix a document (one message's `search_text`) for e5-small - the
/// `passage: ` half of the pair documented on [`e5_query`].
pub fn e5_passage(text: &str) -> String {
    format!("passage: {text}")
}

/// The embedding seam (spec.md#search): text in, vectors out. The real backend
/// is [`E5SmallEmbedder`]; tests substitute an instrumented fake to assert
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
    /// Optional per-batch progress callback. Called once per `flush()` with
    /// the running totals; `pond embed` wires this to an `indicatif` bar.
    progress: Option<ProgressFn>,
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
            progress: None,
        }
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
        let mut batch: Vec<PendingMessage> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
        let mut pulled = 0usize;

        let stream = self.store.pending_embedding_messages();
        tokio::pin!(stream);
        while let Some(pending) = stream.next().await {
            // Stop pulling once the message cap is reached; the staged batch is
            // still flushed below, so exactly `limit` messages are embedded.
            if self.limit.is_some_and(|limit| pulled >= limit) {
                break;
            }
            batch.push(pending?);
            pulled += 1;
            if batch.len() >= DEFAULT_BATCH_SIZE {
                self.flush(&mut batch, &mut summary).await?;
            }
        }
        self.flush(&mut batch, &mut summary).await?;

        tracing::info!(
            model = MODEL_ID,
            messages = summary.messages,
            batches = summary.batches,
            "embed worker finished",
        );
        Ok(summary)
    }

    /// Embed the staged messages in one model call and write the resulting
    /// vectors back to `messages` in one column-update commit. Empties `batch`.
    async fn flush(
        &self,
        batch: &mut Vec<PendingMessage>,
        summary: &mut EmbedSummary,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
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
        self.store.write_embeddings(&rows).await?;
        summary.messages += batch_messages;
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
