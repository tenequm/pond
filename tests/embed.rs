//! Stage 2 tests for the embedding registry, worker, and query instruction. No
//! test requires the Qwen3 weights - the worker runs against an instrumented
//! fake backend.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Mutex;

use pond::{
    adapter::ClaudeCodeAdapter,
    config::{Config, DEFAULT_CONFIG_TOML, EmbeddingModel, EmbeddingsConfig, resolve_data_dir},
    embed::{EmbedBackend, EmbedWorker, qwen3_query_instruction},
    ingest::ingest_adapter,
    substrate::PondStore,
};
use tempfile::TempDir;

/// A single fixture project subdir - enough sessions to fill more than one
/// embedding batch without ingesting the whole fixture corpus.
const FIXTURES: &str =
    "tests/fixtures/session-samples/claude-code/projects/-Users-user-Projects-myproject-d";

/// Records the batch size of every `embed` call so tests can assert batching.
struct FakeBackend {
    dim: usize,
    calls: Mutex<Vec<usize>>,
}

impl FakeBackend {
    fn new(dim: usize) -> Self {
        Self {
            dim,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn call_sizes(&self) -> Vec<usize> {
        self.calls.lock().unwrap().clone()
    }
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        self.calls.lock().unwrap().push(texts.len());
        Ok(vec![vec![0.1; self.dim]; texts.len()])
    }

    fn dim(&self) -> usize {
        self.dim
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
        4096,
    );
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
    use std::path::PathBuf;

    // An explicit `--data-dir` / `POND_DATA_DIR` wins over everything.
    assert_eq!(
        resolve_data_dir(
            Some(PathBuf::from("/explicit")),
            Some(PathBuf::from("/xdg")),
            Some(PathBuf::from("/home")),
        ),
        PathBuf::from("/explicit"),
    );
    // An absolute XDG_DATA_HOME is used next.
    assert_eq!(
        resolve_data_dir(
            None,
            Some(PathBuf::from("/xdg")),
            Some(PathBuf::from("/home"))
        ),
        PathBuf::from("/xdg/pond"),
    );
    // A relative XDG_DATA_HOME is ignored per the XDG spec; HOME is the fallback.
    assert_eq!(
        resolve_data_dir(
            None,
            Some(PathBuf::from("relative")),
            Some(PathBuf::from("/home")),
        ),
        PathBuf::from("/home/.local/share/pond"),
    );
    // No XDG and no HOME - stays usable rather than panicking.
    assert_eq!(resolve_data_dir(None, None, None), PathBuf::from(".pond"));
}

#[tokio::test]
async fn embed_worker_batches_inference_and_writes() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter).await?;

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

    let calls = backend.call_sizes();
    assert_eq!(
        calls.len(),
        summary.batches,
        "one model call per write batch"
    );
    assert_eq!(
        calls.iter().sum::<usize>(),
        summary.messages,
        "every message is embedded exactly once",
    );
    // Batching is load-bearing: no singleton call while a full batch was
    // available - every non-final batch is full.
    if calls.len() > 1 {
        for &size in &calls[..calls.len() - 1] {
            assert_eq!(size, 4, "non-final batches must be full, saw {calls:?}");
        }
    }

    // Re-run is a no-op: the `(message_id, model_id)` PK is already populated.
    let backend = FakeBackend::new(model.dim as usize);
    let again = EmbedWorker::new(&store, &backend, &model)?
        .with_batch_size(4)
        .run()
        .await?;
    assert_eq!(again.messages, 0);
    assert!(backend.call_sizes().is_empty());

    Ok(())
}
