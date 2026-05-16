//! Stage 2 tests for the embedding registry, worker, and query instruction. No
//! test requires the Qwen3 weights - the worker runs against an instrumented
//! fake backend.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Mutex;

use chrono::Utc;
use pond::{
    PROTOCOL_VERSION,
    adapter::ClaudeCodeAdapter,
    config::{Config, DEFAULT_CONFIG_TOML, EmbeddingModel, EmbeddingsConfig, resolve_data_dir},
    embed::{EmbedBackend, EmbedWorker, qwen3_query_instruction},
    handlers::{IngestEvent, ingest_adapter, pond_ingest},
    sessions::Store,
    wire::{IngestEnvelope, IngestRequest},
    wire::{Message, Part, PartKind, Session},
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
    "tests/fixtures/session-samples/claude-code/projects/-Users-user-Projects-myproject-d";

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

#[test]
fn qwen3_query_instruction_wraps_the_query_in_the_model_card_prefix() {
    let prompt = qwen3_query_instruction("how does retry backoff work");
    // Model-card format: `Instruct: {task}\nQuery: {query}` - the query sits on
    // its own line after the instruction and is never mutated, only prefixed.
    assert!(prompt.starts_with("Instruct: "));
    assert!(prompt.ends_with("\nQuery: how does retry backoff work"));
}

#[test]
fn builtin_registry_validates() {
    let config = Config::builtin();
    config
        .embeddings
        .validate()
        .expect("the built-in registry must be valid");
    let model = config.embeddings.default_model("local").unwrap();
    assert_eq!(model.id, "Qwen/Qwen3-Embedding-0.6B");
    assert_eq!(model.dim, 1024);
}

#[test]
fn registry_rejects_unknown_model() {
    let config = EmbeddingsConfig {
        models: vec![EmbeddingModel {
            id: "bogus/model".to_owned(),
            ..EmbeddingModel::qwen3_default()
        }],
        ..EmbeddingsConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn registry_rejects_dim_mismatch() {
    let config = EmbeddingsConfig {
        models: vec![EmbeddingModel {
            dim: 512,
            ..EmbeddingModel::qwen3_default()
        }],
        ..EmbeddingsConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn registry_rejects_missing_and_duplicate_defaults() {
    let no_default = EmbeddingsConfig {
        models: vec![EmbeddingModel {
            default: false,
            ..EmbeddingModel::qwen3_default()
        }],
        ..EmbeddingsConfig::default()
    };
    assert!(no_default.validate().is_err());

    let two_defaults = EmbeddingsConfig {
        models: vec![
            EmbeddingModel {
                id: "a".to_owned(),
                ..EmbeddingModel::qwen3_default()
            },
            EmbeddingModel {
                id: "b".to_owned(),
                ..EmbeddingModel::qwen3_default()
            },
        ],
        ..EmbeddingsConfig::default()
    };
    assert!(two_defaults.validate().is_err());
}

#[test]
fn config_load_merges_namespace_overrides() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(
        &path,
        "[embeddings.overrides.local.\"Qwen/Qwen3-Embedding-0.6B\"]\nmax_embed_tokens = 2048\n",
    )
    .unwrap();

    let config = Config::load(&path).unwrap();
    assert_eq!(
        config
            .embeddings
            .default_model("local")
            .unwrap()
            .max_embed_tokens,
        2048,
    );
    // The override is scoped to its namespace; others keep the built-in value.
    assert_eq!(
        config
            .embeddings
            .default_model("other")
            .unwrap()
            .max_embed_tokens,
        1024,
    );
}

#[test]
fn registry_rejects_oversized_max_embed_tokens() {
    // The built-in 1024 is well within the per-batch cost budget.
    let ok = EmbeddingsConfig {
        models: vec![EmbeddingModel {
            max_embed_tokens: 1024,
            ..EmbeddingModel::qwen3_default()
        }],
        ..EmbeddingsConfig::default()
    };
    assert!(ok.validate().is_ok());

    // A value whose single-message cost (`max_embed_tokens^2`) exceeds the
    // batch budget is rejected: cost-aware batching cannot split one message,
    // so a message that does not fit a batch alone would risk an OOM pass.
    let oversized = EmbeddingsConfig {
        models: vec![EmbeddingModel {
            max_embed_tokens: 8192,
            ..EmbeddingModel::qwen3_default()
        }],
        ..EmbeddingsConfig::default()
    };
    assert!(oversized.validate().is_err());
}

#[test]
fn config_load_missing_file_falls_back_to_builtin() {
    let config = Config::load("/nonexistent/pond-config-xyz.toml").unwrap();
    assert_eq!(config.embeddings.models.len(), 1);
}

#[test]
fn default_config_toml_loads_to_the_builtin_registry() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, DEFAULT_CONFIG_TOML).unwrap();
    // The shipped template is all comments, so it must load and validate as the
    // built-in registry - a malformed template fails right here.
    let config = Config::load(&path).unwrap();
    config.embeddings.validate().unwrap();
    assert_eq!(
        config.embeddings.default_model("local").unwrap().id,
        "Qwen/Qwen3-Embedding-0.6B",
    );
}

#[test]
fn resolve_data_dir_follows_explicit_then_xdg_then_home() {
    use pond::config::{is_local, local_path, parse_data_dir};
    use std::path::PathBuf;

    // An explicit `--data-dir` / `POND_DATA_DIR` wins over everything. The
    // explicit value can carry any URI form Lance accepts; here we test the
    // local-path form (parsing is delegated to Lance's `uri_to_url`).
    let explicit = parse_data_dir("/explicit").unwrap();
    let resolved = resolve_data_dir(
        Some(explicit.clone()),
        Some(PathBuf::from("/xdg")),
        Some(PathBuf::from("/home")),
    )
    .unwrap();
    assert_eq!(resolved, explicit);

    // An absolute XDG_DATA_HOME is used next.
    let resolved = resolve_data_dir(
        None,
        Some(PathBuf::from("/xdg")),
        Some(PathBuf::from("/home")),
    )
    .unwrap();
    assert!(is_local(&resolved));
    assert_eq!(local_path(&resolved).unwrap(), PathBuf::from("/xdg/pond"));

    // A relative XDG_DATA_HOME is ignored per the XDG spec; HOME is the fallback.
    let resolved = resolve_data_dir(
        None,
        Some(PathBuf::from("relative")),
        Some(PathBuf::from("/home")),
    )
    .unwrap();
    assert_eq!(
        local_path(&resolved).unwrap(),
        PathBuf::from("/home/.local/share/pond"),
    );

    // No XDG and no HOME - stays usable: returns the cwd-anchored `.pond`.
    // The result is absolute (Lance's URL conversion requires it), so we
    // just check that the URL ends with the relative path's components.
    let resolved = resolve_data_dir(None, None, None).unwrap();
    assert!(is_local(&resolved));
    assert!(
        local_path(&resolved).unwrap().ends_with(".pond"),
        "fallback path should end with .pond: {resolved}",
    );
}

#[tokio::test]
async fn embed_worker_batches_inference_and_writes() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, |_| {}).await?;

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

    // Re-run is a no-op: the `(message_id, model_id)` PK is already populated.
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
            id: format!("{message_id}-part"),
            message_id: message_id.to_owned(),
            ordinal: 0,
            options: Default::default(),
            kind: PartKind::Text { text: s(text) },
        }),
    ]
}

#[tokio::test]
async fn embed_worker_caps_batch_cost_for_long_messages() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    // One session: 10 tiny messages and one very long one. The long message's
    // `search_text` is far past the token cap; the rest are a handful of bytes.
    let session_id = "cost-budget-session";
    let mut events = vec![IngestEvent::Session(Session {
        id: session_id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: Some("pond-cost-test".to_owned()),
        options: Default::default(),
    })];
    for i in 0..11 {
        let text = if i == 5 {
            "lorem ipsum ".repeat(2_500) // ~30 KB - token estimate clamps to the cap
        } else {
            format!("short message number {i}")
        };
        events.extend(text_message_events(session_id, &format!("msg-{i}"), &text));
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
    // A high count ceiling so it never binds, and a small cost budget so the
    // long message is forced out of any shared batch. The budget sits between
    // a full batch of tiny messages and a two-message batch at the token cap.
    let summary = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(32)
        .with_cost_budget(1_000_000)
        .run()
        .await?;
    assert_eq!(summary.messages, 11);

    let calls = backend.calls();
    assert_eq!(
        calls.iter().map(|call| call.count).sum::<usize>(),
        11,
        "every message embedded exactly once",
    );
    // The long message (max byte length far above any short one) can never be
    // padded into a batch with other messages - it is always embedded alone.
    for call in &calls {
        if call.max_bytes > 10_000 {
            assert_eq!(
                call.count, 1,
                "a long message must be embedded in its own batch, saw {calls:?}",
            );
        }
    }
    // The budget is a cap, not a ban: the short messages still batch together.
    assert!(
        calls.iter().any(|call| call.count > 1),
        "short messages must still batch, saw {calls:?}",
    );

    Ok(())
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
        project: Some("pond-length-test".to_owned()),
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
        project: Some("pond-cost-invariant".to_owned()),
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
