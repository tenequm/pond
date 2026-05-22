//! Integration tests for the embedding worker: real `Store`, real fixture
//! ingest, and an instrumented fake backend that records every batch shape.
//! No model weights required. Registry validation, query-instruction format,
//! and the `metric_type` mapping are unit-tested inline in `src/config.rs`
//! and `src/embed/mod.rs`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Mutex;

use chrono::Utc;
use pond::{
    PROTOCOL_VERSION,
    adapter::ClaudeCodeAdapter,
    config::Config,
    embed::{EmbedBackend, EmbedWorker},
    handlers::{IngestEvent, ingest_adapter, pond_ingest},
    sessions::Store,
    wire::{IngestEnvelope, IngestRequest},
    wire::{Message, Part, PartKind, Provenance, Session},
};
use tempfile::TempDir;

/// Build an `Option<Extracted<String>>` for test fixtures. Integration tests
/// can't see `Extracted::from_test_value` (cfg-test-gated inside the pond
/// crate), so we go through the public `extract_str` producer on a
/// synthetic JSON source.
fn s(value: &str) -> Option<pond::adapter::Extracted<String>> {
    pond::adapter::extract_str(&serde_json::json!({"x": value}), "x")
}

/// A single fixture project subdir - enough sessions to fill more than one
/// embedding batch without ingesting the whole fixture corpus.
const FIXTURES: &str =
    "tests/fixtures/adapter/claude_code/projects/-Users-user-Projects-myproject-d";

/// One recorded `embed` call: the batch's message count and its shortest /
/// longest input length in bytes. `min`/`max` together tell whether a batch is
/// length-homogeneous (the point of length-bucketing) or mixes short and long.
#[derive(Clone, Copy, Debug)]
struct Call {
    count: usize,
    min_bytes: usize,
    max_bytes: usize,
}

/// Records the shape of every `embed` call so tests can assert batching
/// happens, a long message is never co-batched, and batches are length-sorted.
struct FakeBackend {
    dim: usize,
    calls: Mutex<Vec<Call>>,
}

impl FakeBackend {
    fn new(dim: usize) -> Self {
        Self {
            dim,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        let lengths = texts.iter().map(String::len);
        self.calls.lock().unwrap().push(Call {
            count: texts.len(),
            min_bytes: lengths.clone().min().unwrap_or(0),
            max_bytes: lengths.max().unwrap_or(0),
        });
        Ok(vec![vec![0.1; self.dim]; texts.len()])
    }

    fn dim(&self) -> usize {
        self.dim
    }

    fn model_id(&self) -> &str {
        "Qwen/Qwen3-Embedding-0.6B"
    }

    fn max_embed_tokens(&self) -> i32 {
        1024
    }
}

#[tokio::test]
async fn embed_worker_batches_inference_and_writes() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let model = Config::builtin().embeddings.default_model("local")?;
    let backend = FakeBackend::new(model.dim as usize);

    let summary = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(4)
        .run()
        .await?;

    assert!(
        summary.messages > 0,
        "fixtures should yield pending messages"
    );

    let calls = backend.calls();
    assert_eq!(
        calls.len(),
        summary.batches,
        "one model call per write batch"
    );
    assert_eq!(
        calls.iter().map(|call| call.count).sum::<usize>(),
        summary.messages,
        "every message is embedded exactly once",
    );
    // Durable invariants - no batch exceeds the count ceiling, and batching
    // actually happens. Exact batch sizes are deliberately not asserted: the
    // window is length-sorted before batching, so sizes depend on the corpus's
    // length distribution, not a fixed `batch_size` fill.
    assert!(
        calls.iter().all(|call| call.count <= 4),
        "no batch exceeds the count ceiling, saw {:?}",
        calls.iter().map(|c| c.count).collect::<Vec<_>>(),
    );
    assert!(
        calls.iter().any(|call| call.count > 1),
        "the worker batches - it does not embed one message per call",
    );

    // Re-run is a no-op: the `(session_id, message_id, model_id)` PK is already populated.
    let backend = FakeBackend::new(model.dim as usize);
    let again = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(4)
        .run()
        .await?;
    assert_eq!(again.messages, 0);
    assert!(backend.calls().is_empty());

    Ok(())
}

/// `Message` + `Part` ingest events for an assistant message carrying a single
/// text part - the text becomes the message's `search_text`.
fn text_message_events(session_id: &str, message_id: &str, text: &str) -> Vec<IngestEvent> {
    vec![
        IngestEvent::Message(Message::Assistant {
            id: message_id.to_owned(),
            session_id: session_id.to_owned(),
            timestamp: Utc::now(),
            options: Default::default(),
        }),
        IngestEvent::Part(Part {
            session_id: session_id.to_owned(),
            id: format!("{message_id}-part"),
            message_id: message_id.to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: Default::default(),
            kind: PartKind::Text { text: s(text) },
        }),
    ]
}

#[tokio::test]
async fn embed_worker_buckets_messages_by_length() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    // 12 short + 12 long messages, strictly interleaved in ingest order. Without
    // length-sorting, batching in stream order would mix short and long in the
    // same batch; the worker sorts each window first, so it must not.
    let session_id = "length-bucket-session";
    let short = "short message"; // ~13 bytes
    let long = "lorem ipsum ".repeat(500); // ~6 KB - far above any short
    let mut events = vec![IngestEvent::Session(Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "pond-length-test"}), "x")
            .unwrap(),
        options: Default::default(),
    })];
    for i in 0..24 {
        let text = if i % 2 == 0 { short } else { &long };
        events.extend(text_message_events(session_id, &format!("msg-{i}"), text));
    }

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events,
        },
    )
    .await;
    assert!(
        matches!(envelope, IngestEnvelope::Success(_)),
        "synthetic ingest should succeed: {envelope:?}",
    );

    let model = Config::builtin().embeddings.default_model("local")?;
    let backend = FakeBackend::new(model.dim as usize);
    // window_size 8 over 24 messages -> 3 windows, so this also exercises the
    // multi-window drain path. A huge cost budget keeps `batch_size` the only
    // limiter, isolating the length-sort as the thing under test.
    let summary = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(4)
        .with_cost_budget(usize::MAX)
        .with_window_size(8)
        .run()
        .await?;
    assert_eq!(summary.messages, 24, "every message embedded");

    let calls = backend.calls();
    assert_eq!(
        calls.iter().map(|call| call.count).sum::<usize>(),
        24,
        "every message embedded exactly once across all windows",
    );
    // Length-bucketing sorts each window before batching, so within a window all
    // short messages precede all long ones - one length transition. The only
    // batch that can still mix the two is the one straddling that transition,
    // when the short count is not a clean multiple of `batch_size`, and there is
    // at most one such batch per window. Unsorted, every batch of the
    // interleaved corpus would mix short and long (6 of 6); bucketing caps the
    // mixed batches at one per window. A mixed batch has a tiny `min_bytes` and
    // a huge `max_bytes` at once.
    let mixed = calls
        .iter()
        .filter(|call| call.min_bytes < 1_000 && call.max_bytes > 1_000)
        .count();
    let windows = 24usize.div_ceil(8);
    assert!(
        mixed <= windows,
        "length-bucketing must leave at most one boundary-straddling batch per \
         window ({windows}), saw {mixed} mixed in {calls:?}",
    );

    Ok(())
}

#[tokio::test]
async fn embed_worker_respects_cost_budget() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    // 16 short messages and 4 long ones. With the budget below, the shorts must
    // co-batch (count > 1) while staying within the budget, and each long
    // message - whose own cost exceeds the budget - must land alone. The test
    // asserts the numeric budget contract directly on every recorded batch.
    let session_id = "cost-invariant-session";
    let short = "word ".repeat(24); // 120 bytes
    let long = "lorem ipsum ".repeat(350); // 4200 bytes - clamps to the token cap
    let mut events = vec![IngestEvent::Session(Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "pond-cost-invariant"}), "x")
            .unwrap(),
        options: Default::default(),
    })];
    for i in 0..20 {
        let text = if i < 16 { &short } else { &long };
        events.extend(text_message_events(session_id, &format!("msg-{i}"), text));
    }

    let envelope = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events,
        },
    )
    .await;
    assert!(
        matches!(envelope, IngestEnvelope::Success(_)),
        "synthetic ingest should succeed: {envelope:?}",
    );

    let model = Config::builtin().embeddings.default_model("local")?;
    let cap = model.max_embed_tokens;
    let backend = FakeBackend::new(model.dim as usize);
    let cost_budget = 200_000;
    let summary = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(32)
        .with_cost_budget(cost_budget)
        .run()
        .await?;
    assert_eq!(summary.messages, 20);

    // The contract: every batch is within `count * max_cost_tokens^2` of the
    // budget, OR it is a lone message (cost-aware batching cannot split one
    // message, so an over-budget singleton is allowed - and only that). The
    // worker's per-message bound is `cost_upper_bound`, i.e.
    // `text.len().clamp(1, max_embed_tokens)`, so the batch's max bound is
    // reconstructable from the recorded `max_bytes`.
    let calls = backend.calls();
    assert!(
        calls.iter().any(|call| call.count > 1),
        "short messages must co-batch, saw {calls:?}",
    );
    assert!(
        calls.iter().any(|call| call.count == 1),
        "each long message must land alone, saw {calls:?}",
    );
    for call in &calls {
        let max_cost = call.max_bytes.clamp(1, cap);
        let batch_cost = call.count * max_cost * max_cost;
        assert!(
            call.count == 1 || batch_cost <= cost_budget,
            "batch {call:?} cost {batch_cost} exceeds budget {cost_budget} with >1 message",
        );
    }

    Ok(())
}
