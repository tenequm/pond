//! Integration test for the embedding worker: real `Store`, real fixture
//! ingest, and an instrumented fake backend that records every batch shape.
//! No model weights required. This is the only place the `EmbedWorker` run
//! loop is asserted - other suites invoke it only as setup. Query-instruction
//! format is unit-tested in `src/embed/mod.rs`, the vector-on-`messages`
//! schema in `src/sessions.rs`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Mutex;

use pond::{
    adapter::ClaudeCodeAdapter,
    embed::{DEFAULT_BATCH_SIZE, EmbedBackend, EmbedWorker},
    handlers::ingest_adapter,
    sessions::{EMBEDDING_DIM, Store},
};
use tempfile::TempDir;

/// A single fixture project subdir - enough sessions to fill an embedding
/// batch without ingesting the whole fixture corpus.
const FIXTURES: &str =
    "tests/fixtures/adapter/claude_code/projects/-Users-user-Projects-myproject-d";

/// Records the message count and the input texts of every `embed` call so the
/// test can assert batching happens and documents carry the `passage:` prefix.
struct FakeBackend {
    counts: Mutex<Vec<usize>>,
    texts: Mutex<Vec<String>>,
}

impl FakeBackend {
    fn new() -> Self {
        Self {
            counts: Mutex::new(Vec::new()),
            texts: Mutex::new(Vec::new()),
        }
    }

    fn counts(&self) -> Vec<usize> {
        self.counts.lock().unwrap().clone()
    }

    fn texts(&self) -> Vec<String> {
        self.texts.lock().unwrap().clone()
    }
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        self.counts.lock().unwrap().push(texts.len());
        self.texts.lock().unwrap().extend(texts.iter().cloned());
        Ok(vec![vec![0.1; EMBEDDING_DIM]; texts.len()])
    }
}

/// The worker's end-to-end contract: it drains the un-embedded backlog,
/// batches the model calls, prefixes documents with e5's `passage:` marker,
/// fills `vector` + `embedding_model`, and a re-run is a no-op.
#[tokio::test]
async fn embed_worker_drains_the_backlog() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    // Ingest leaves every `vector` null - search would be FTS-only here.
    assert!(
        !store.has_embeddings().await?,
        "ingest must leave every message un-embedded",
    );

    let backend = FakeBackend::new();
    let summary = EmbedWorker::new(&store, &backend).run().await?;
    assert!(
        summary.messages > 0,
        "fixtures should yield pending messages"
    );

    let counts = backend.counts();
    assert_eq!(
        counts.len(),
        summary.batches,
        "one model call per write batch",
    );
    assert_eq!(
        counts.iter().sum::<usize>(),
        summary.messages,
        "every message is embedded exactly once",
    );
    assert!(
        counts.iter().all(|&count| count <= DEFAULT_BATCH_SIZE),
        "no batch exceeds the size ceiling, saw {counts:?}",
    );
    assert!(
        counts.iter().any(|&count| count > 1),
        "the worker batches - it does not embed one message per call",
    );

    let texts = backend.texts();
    assert_eq!(texts.len(), summary.messages, "every message embedded once");
    assert!(
        texts.iter().all(|text| text.starts_with("passage: ")),
        "the worker must prefix documents with e5's `passage: ` marker",
    );

    // `pond embed` filled `vector` + `embedding_model`, so search is now hybrid.
    assert!(
        store.has_embeddings().await?,
        "the worker must fill vector + embedding_model on messages",
    );

    // Re-run is a no-op: every message now has a non-null `vector`, so the
    // backlog (`vector IS NULL`) is empty.
    let backend = FakeBackend::new();
    let again = EmbedWorker::new(&store, &backend).run().await?;
    assert_eq!(again.messages, 0);
    assert!(backend.counts().is_empty());

    Ok(())
}
