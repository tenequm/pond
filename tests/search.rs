//! Handler-level integration tests for `pond_search` over a real fixture
//! corpus with a deterministic fake embedder. Pure-helper tests (RRF math,
//! recency boost, filter predicate construction, planner shape, distance
//! metric mapping) and `Store`-level vector-index tests live inline in
//! `src/handlers.rs::tests`, `src/embed/mod.rs::tests`, and
//! `src/sessions.rs::tests`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::ClaudeCodeAdapter,
    config::Config,
    embed::{EmbedBackend, EmbedWorker},
    handlers::ingest_adapter,
    handlers::pond_get,
    handlers::pond_search,
    sessions::Store,
    wire::PartKind,
    wire::{
        GetEnvelope, GetRequest, GetResult, Hit, ProjectFilter, SearchEnvelope, SearchFilters,
        SearchRequest, SearchResultBody,
    },
};
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/session-samples/claude-code/projects";

/// An instrumented embedding backend: deterministic, content-dependent vectors,
/// no model weights. Enough for the vector retriever to produce a stable,
/// non-degenerate ranking and for the query side to embed.
struct FakeBackend {
    dim: usize,
}

impl FakeBackend {
    fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| pseudo_vector(text, self.dim))
            .collect())
    }

    fn dim(&self) -> usize {
        self.dim
    }

    // The fake stands in for the builtin model: `searchable_corpus` embeds the
    // fixtures with it, so vector search must scope to this identity to see them.
    fn model_id(&self) -> &str {
        "Qwen/Qwen3-Embedding-0.6B"
    }

    fn max_embed_tokens(&self) -> i32 {
        1024
    }
}

/// A deterministic pseudo-random vector seeded by the text's FNV-1a hash.
fn pseudo_vector(text: &str, dim: usize) -> Vec<f32> {
    let mut state = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (0..dim)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            #[allow(clippy::cast_precision_loss)]
            let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
            unit - 1.0
        })
        .collect()
}

/// Ingest the claude-code fixtures, build the indices, and embed every message
/// with the fake backend - the `pond_search` handler then runs end to end
/// without model weights, exactly as `pond ingest` wires it (main.rs).
async fn searchable_corpus(temp: &TempDir) -> anyhow::Result<(Store, FakeBackend)> {
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;
    store.ensure_indices().await?;

    let model = Config::builtin().embeddings.default_model("local")?;
    let backend = FakeBackend::new(model.dim as usize);
    EmbedWorker::new(&store, &backend, &model)?.run().await?;
    store.ensure_embedding_indices(&model).await?;
    Ok((store, backend))
}

fn search_request(query: &str) -> SearchRequest {
    SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        rrf_k: 60,
        filters: SearchFilters::default(),
        boost_recent: true,
        group_by_conversation: false,
        limit: 20,
    }
}

fn get_request(session_id: &str) -> GetRequest {
    GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: Some(session_id.to_owned()),
        message_id: None,
        up_to: None,
        context_depth: 0,
        max_messages: 1000,
        include_thinking: false,
        include_tool_results: false,
    }
}

/// Unwrap a hit-shaped search response, panicking on errors or grouped results.
fn hits_of(envelope: SearchEnvelope) -> Vec<Hit> {
    match envelope {
        SearchEnvelope::Success(response) => match response.result {
            SearchResultBody::Hits { hits } => hits,
            SearchResultBody::Groups { .. } => panic!("expected hits, got groups"),
        },
        SearchEnvelope::Error(error) => panic!("search failed: {error:?}"),
    }
}

/// Unwrap a successful search envelope, panicking on errors.
fn success_of(envelope: SearchEnvelope) -> pond::wire::SearchResponse {
    match envelope {
        SearchEnvelope::Success(response) => response,
        SearchEnvelope::Error(error) => panic!("search failed: {error:?}"),
    }
}

/// Pull the hits body out of a response, panicking on grouped results.
fn body_hits(body: SearchResultBody) -> Vec<Hit> {
    match body {
        SearchResultBody::Hits { hits } => hits,
        SearchResultBody::Groups { .. } => panic!("expected hits, got groups"),
    }
}

/// A phrase taken verbatim from an ingested message - guaranteed to produce FTS
/// hits without hard-coding fixture content.
async fn corpus_phrase(store: &Store) -> anyhow::Result<String> {
    for session_id in store.session_ids().await? {
        let GetEnvelope::Success(response) = pond_get(store, get_request(&session_id)).await else {
            continue;
        };
        let GetResult::Session { parts, .. } = response.result else {
            continue;
        };
        for part in &parts {
            if let PartKind::Text { text: Some(text) } = &part.kind {
                let words = text.split_whitespace().take(8).collect::<Vec<_>>();
                if words.len() >= 4 {
                    return Ok(words.join(" "));
                }
            }
        }
    }
    anyhow::bail!("no usable text part in the fixture corpus")
}

/// The retrieval mode is server-determined: hybrid when the embedder is loaded
/// AND has embeddings for its identity, FTS otherwise. The wire surface no
/// longer carries a `search_mode` field; per-hit `matched_via` is the only
/// retriever-provenance signal. This test exercises all three branches and
/// asserts the right retrievers ranked the hits.
#[tokio::test(flavor = "multi_thread")]
async fn search_picks_hybrid_or_fts_based_on_embedder_state() -> anyhow::Result<()> {
    // Case 1: embedder + embeddings -> hybrid (both retrievers contribute).
    let temp = TempDir::new()?;
    let (store, backend) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let hits = body_hits(
        success_of(pond_search(&store, Some(&backend), search_request(&phrase)).await).result,
    );
    assert!(!hits.is_empty(), "hybrid search must return hits");
    for pair in hits.windows(2) {
        assert!(pair[0].score >= pair[1].score, "hits must be score-ordered");
    }
    let saw_vector = hits
        .iter()
        .any(|hit| hit.matched_via.iter().any(|v| v == "vector"));
    assert!(
        saw_vector,
        "hybrid must surface at least one vector-ranked hit"
    );
    for hit in &hits {
        assert!(!hit.matched_via.is_empty());
        assert!(
            hit.matched_via
                .iter()
                .all(|via| via == "vector" || via == "fts"),
            "hybrid matched_via must be a subset of {{vector, fts}}: {:?}",
            hit.matched_via,
        );
    }

    // Case 2: embedder is `None` -> FTS-only.
    let hits =
        body_hits(success_of(pond_search(&store, None, search_request(&phrase)).await).result);
    assert!(!hits.is_empty(), "fts must still return hits");
    for hit in &hits {
        assert_eq!(hit.matched_via, ["fts"]);
    }

    // Case 3: embedder present but the embeddings table has no rows for its
    // identity -> auto-degrade to FTS. Build a fresh store with messages and
    // the FTS index but no embed pass.
    let temp2 = TempDir::new()?;
    let store2 = Store::open_local(temp2.path()).await?;
    ingest_adapter(
        &store2,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    store2.ensure_indices().await?;
    let hits = body_hits(
        success_of(pond_search(&store2, Some(&backend), search_request(&phrase)).await).result,
    );
    for hit in &hits {
        assert_eq!(hit.matched_via, ["fts"]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_narrow_results_over_the_fixture_corpus() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, backend) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    // role: every hit carries the requested role.
    let mut request = search_request(&phrase);
    request.filters.role = Some("assistant".to_owned());
    let hits = hits_of(pond_search(&store, Some(&backend), request).await);
    assert!(!hits.is_empty(), "the corpus has assistant messages");
    assert!(hits.iter().all(|hit| hit.role == "assistant"));

    // session_id: every hit belongs to the one requested session.
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("the corpus has at least one session");
    let mut request = search_request(&phrase);
    request.filters.session_id = Some(session_id.clone());
    let hits = hits_of(pond_search(&store, Some(&backend), request).await);
    assert!(hits.iter().all(|hit| hit.session_id == session_id));

    // source_agent: the real agent returns hits; an unknown one returns none.
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("claude-code".to_owned());
    assert!(!hits_of(pond_search(&store, Some(&backend), request).await).is_empty());
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("no-such-agent".to_owned());
    assert!(hits_of(pond_search(&store, Some(&backend), request).await).is_empty());

    // date window: a far-future lower bound excludes the whole corpus.
    let mut request = search_request(&phrase);
    request.filters.from_date = Some("2099-01-01".to_owned());
    assert!(hits_of(pond_search(&store, Some(&backend), request).await).is_empty());

    // project (contains): every hit is scoped to the requested project.
    let project = hits_of(pond_search(&store, Some(&backend), search_request(&phrase)).await)
        .into_iter()
        .map(|hit| hit.project)
        .find(|p| !p.is_empty())
        .expect("fixture hits carry a project");
    let mut request = search_request(&phrase);
    request.filters.project = Some(ProjectFilter::Contains(project.clone()));
    let hits = hits_of(pond_search(&store, Some(&backend), request).await);
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|hit| hit.project.contains(project.as_str())),
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_conversation_collapses_to_one_summary_per_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, backend) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let mut request = search_request(&phrase);
    request.group_by_conversation = true;
    let SearchEnvelope::Success(response) = pond_search(&store, Some(&backend), request).await
    else {
        panic!("grouped search must succeed");
    };
    let SearchResultBody::Groups { groups } = response.result else {
        panic!("group_by_conversation returns groups, not hits");
    };

    assert!(!groups.is_empty());
    // One summary per session: session ids are unique across groups.
    let unique = groups
        .iter()
        .map(|group| group.session_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique.len(),
        groups.len(),
        "each session appears at most once",
    );
    for group in &groups {
        // `message_count` is the whole-session size, not the match count.
        assert!(group.message_count > 0);
        assert!(group.best_score > 0.0);
        assert!(group.first_timestamp <= group.last_timestamp);
    }
    assert_eq!(response.total, groups.len());

    Ok(())
}
