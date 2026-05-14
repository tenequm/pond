//! Stage 2 search tests: RRF math, the recency-boost formula, filter-predicate
//! construction, the distance-metric mapping, the `explain_plan` prefilter
//! pushdown assertion, and the synthetic IVF_PQ index-activation check. Every
//! test runs on every `cargo test` - no model weights, no `#[ignore]`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::{Duration, Utc};
use lance_linalg::distance::MetricType;
use pond::{
    adapter::ClaudeCodeAdapter,
    config::{Config, Distance},
    datasets::{EMBEDDING_DIM, EmbeddingRow},
    embed::{EmbedBackend, EmbedWorker},
    get::pond_get,
    ingest::{IngestEvent, ingest_adapter, pond_ingest},
    search::{RankedList, build_filter, make_preview, pond_search, recency_boost, rrf_merge},
    substrate::{PondStore, metric_type},
    types::{Message, Part, PartKind, ProviderOptions, Session},
    wire::{
        GetEnvelope, GetRequest, GetResult, Hit, IngestEnvelope, IngestRequest, ProjectMatch,
        SearchEnvelope, SearchFilters, SearchMode, SearchRequest, SearchResultBody,
    },
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Pure functions
// ---------------------------------------------------------------------------

#[test]
fn rrf_merge_fuses_retrievers_and_reports_provenance() {
    let lists = [
        RankedList {
            retriever: "vector",
            ids: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
        },
        RankedList {
            retriever: "fts",
            ids: vec!["b".to_owned(), "a".to_owned(), "d".to_owned()],
        },
    ];
    let merged = rrf_merge(&lists, 60);

    // "a" (ranks 1,2) and "b" (ranks 2,1) have equal fused scores; the tie
    // breaks on message_id, so "a" sorts first. Both beat the single-retriever
    // "c" and "d".
    assert_eq!(merged[0].message_id, "a");
    assert_eq!(merged[1].message_id, "b");
    assert_eq!(merged[0].matched_via, vec!["vector", "fts"]);
    assert!(merged[0].score > merged[2].score);

    let c = merged.iter().find(|hit| hit.message_id == "c").unwrap();
    assert_eq!(c.matched_via, vec!["vector"]);
    let d = merged.iter().find(|hit| hit.message_id == "d").unwrap();
    assert_eq!(d.matched_via, vec!["fts"]);
}

#[test]
fn recency_boost_matches_the_kb_formula() {
    let now = Utc::now();
    // Caps at +0.2 at age zero.
    assert!((recency_boost(now, now) - 0.2).abs() < 1e-6);
    // One half-life (7 days) decays by exactly 1/e.
    let week = recency_boost(now - Duration::days(7), now);
    assert!((week - 0.2 / std::f64::consts::E).abs() < 1e-3);
    // A year out is effectively zero.
    assert!(recency_boost(now - Duration::days(365), now) < 1e-3);
    // Future timestamps clamp to the cap rather than exceeding it.
    assert!((recency_boost(now + Duration::days(1), now) - 0.2).abs() < 1e-6);
}

#[test]
fn make_preview_truncates_at_code_point_boundary() {
    let short = "a short preview";
    assert_eq!(make_preview(short), short);

    let long = "x".repeat(800);
    let preview = make_preview(&long);
    assert!(preview.ends_with("..."));
    assert_eq!(preview.chars().count(), 503);
}

#[test]
fn build_filter_pushes_down_each_predicate() {
    let filters = SearchFilters {
        project: Some("/Users/me/pond".to_owned()),
        project_match: ProjectMatch::Exact,
        session_id: Some("01HXY".to_owned()),
        source_agent: Some("claude-code".to_owned()),
        role: Some("assistant".to_owned()),
        from_date: Some("2026-01-01".to_owned()),
        to_date: Some("2026-05-01".to_owned()),
        min_score: 0.0,
    };
    let sql = build_filter(&filters).unwrap();
    assert!(sql.contains("project = '/Users/me/pond'"));
    assert!(sql.contains("session_id = '01HXY'"));
    assert!(sql.contains("source_agent = 'claude-code'"));
    assert!(sql.contains("role = 'assistant'"));
    assert!(sql.contains("timestamp >="));
    assert!(sql.contains("timestamp <="));
}

#[test]
fn build_filter_is_null_ignores_the_project_value() {
    let filters = SearchFilters {
        project: Some("ignored".to_owned()),
        project_match: ProjectMatch::IsNull,
        ..SearchFilters::default()
    };
    assert_eq!(build_filter(&filters).unwrap(), "project IS NULL");
}

#[test]
fn build_filter_rejects_bad_role_and_date() {
    let bad_role = SearchFilters {
        role: Some("wizard".to_owned()),
        ..SearchFilters::default()
    };
    assert!(build_filter(&bad_role).is_err());

    let bad_date = SearchFilters {
        from_date: Some("01-01-2026".to_owned()),
        ..SearchFilters::default()
    };
    assert!(build_filter(&bad_date).is_err());
}

#[test]
fn empty_filters_produce_no_predicate() {
    assert_eq!(build_filter(&SearchFilters::default()).unwrap(), "");
}

#[test]
fn build_filter_contains_escapes_like_wildcards() {
    let filters = SearchFilters {
        project: Some("/Users/me/my_project".to_owned()),
        project_match: ProjectMatch::Contains,
        ..SearchFilters::default()
    };
    let sql = build_filter(&filters).unwrap();
    // `_` is a LIKE wildcard and is everywhere in real paths; it must be escaped
    // so `my_project` matches literally, with an ESCAPE clause naming the char.
    assert!(
        sql.contains(r"my\_project"),
        "underscore must be escaped: {sql}"
    );
    assert!(
        sql.contains(r"ESCAPE '\'"),
        "predicate must declare the escape char: {sql}"
    );
}

#[test]
fn metric_type_maps_each_registry_distance() {
    assert_eq!(metric_type(Distance::Cosine), MetricType::Cosine);
    assert_eq!(metric_type(Distance::L2), MetricType::L2);
    assert_eq!(metric_type(Distance::Dot), MetricType::Dot);
}

// ---------------------------------------------------------------------------
// Synthetic datasets (no model, no ingest)
// ---------------------------------------------------------------------------

/// Build `count` synthetic embedding rows with deterministic pseudo-random
/// vectors of the production dimension, spread across a handful of sessions.
fn synthetic_rows(count: usize, model_id: &str) -> Vec<EmbeddingRow> {
    let now = Utc::now();
    (0..count)
        .map(|i| {
            let mut vector = Vec::with_capacity(EMBEDDING_DIM);
            let mut state = (i as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(1);
            for _ in 0..EMBEDDING_DIM {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                #[allow(clippy::cast_precision_loss)]
                let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
                vector.push(unit - 1.0);
            }
            EmbeddingRow {
                message_id: format!("msg-{i}"),
                model_id: model_id.to_owned(),
                vector,
                session_id: format!("session-{}", i % 8),
                source_agent: "claude-code".to_owned(),
                project: Some(format!("/proj/{}", i % 4)),
                role: if i % 2 == 0 { "user" } else { "assistant" }.to_owned(),
                timestamp: now - Duration::seconds(i as i64),
            }
        })
        .collect()
}

#[tokio::test]
async fn filtered_vector_scan_pushes_scalar_predicate_into_the_index() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let model = Config::builtin().embeddings.default_model("local")?;

    // 4 synthetic rows: `synthetic_rows` cycles `session-{i % 8}`, so 4 is the
    // smallest count where `session-3` (the filter value below) is a real
    // partition. Scalar-index pushdown is volume-independent - the planner emits
    // a `ScalarIndexQuery` for an indexed equality whenever the index exists, so
    // a larger corpus produces the identical plan.
    store
        .upsert_embeddings(&synthetic_rows(4, &model.id))
        .await?;
    store.ensure_embedding_indices(&model).await?;

    let query = vec![0.01_f32; EMBEDDING_DIM];
    let plan = store
        .explain_vector_plan(&query, 10, "session_id = 'session-3'")
        .await?;

    // The load-bearing assertion (design.md 3.3): the predicate is served by a
    // scalar-index node, not a postfilter `FilterExec`. (A `FilterExec` for the
    // KNN-internal `_distance IS NOT NULL` is expected and unrelated.)
    assert!(
        plan.contains("ScalarIndexQuery"),
        "expected a ScalarIndexQuery node in the plan:\n{plan}",
    );
    let predicate_postfiltered = plan
        .lines()
        .any(|line| line.contains("FilterExec") && line.contains("session_id"));
    assert!(
        !predicate_postfiltered,
        "the scalar predicate must not fall back to a FilterExec postfilter:\n{plan}",
    );
    Ok(())
}

#[tokio::test]
async fn vector_index_activates_past_the_row_threshold() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let store = PondStore::open(temp.path()).await?;
    let model = Config::builtin().embeddings.default_model("local")?;

    // 256 rows is the hard floor: the IVF_PQ index uses `num_bits = 8`, so its
    // PQ trainer needs one row per code centroid (2^8 = 256) - fewer fails with
    // "Not enough rows to train PQ". The thresholds below straddle that count by
    // exactly one, so the test exercises the `row_count >= threshold` boundary.
    let rows = synthetic_rows(256, &model.id);
    let planted = rows[0].clone();
    store.upsert_embeddings(&rows).await?;

    // Just below threshold (256 < 257): no vector index yet.
    store
        .ensure_embedding_indices_with_threshold(&model, 257)
        .await?;
    assert!(
        !store
            .embedding_index_names()
            .await?
            .iter()
            .any(|name| name == "embeddings_vector_ivfpq"),
        "vector index must not build below the activation threshold",
    );

    // At the threshold (256 >= 256): the IVF_PQ index builds.
    store
        .ensure_embedding_indices_with_threshold(&model, 256)
        .await?;
    let indices = store.embedding_index_names().await?;
    assert!(
        indices.iter().any(|name| name == "embeddings_vector_ivfpq"),
        "IVF_PQ index should build past the activation threshold: {indices:?}",
    );

    // A query whose vector is a planted row returns that row.
    let hits = store.vector_search(&planted.vector, 10, "").await?;
    assert!(
        hits.iter().any(|(id, _)| id == &planted.message_id),
        "planted vector should be retrievable via the index",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// End-to-end `pond_search` handler (fixture corpus, no model weights)
// ---------------------------------------------------------------------------

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
async fn searchable_corpus(temp: &TempDir) -> anyhow::Result<(PondStore, FakeBackend)> {
    let store = PondStore::open(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter).await?;
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
        namespace: "local".to_owned(),
        query: query.to_owned(),
        search_mode: SearchMode::Hybrid,
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
        namespace: "local".to_owned(),
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

/// A phrase taken verbatim from an ingested message - guaranteed to produce FTS
/// hits without hard-coding fixture content.
async fn corpus_phrase(store: &PondStore) -> anyhow::Result<String> {
    for session_id in store.session_ids().await? {
        let GetEnvelope::Success(response) = pond_get(store, get_request(&session_id)).await else {
            continue;
        };
        let GetResult::Session(stored) = response.result else {
            continue;
        };
        for message in &stored.messages {
            for part in &message.parts {
                if let PartKind::Text { text } = &part.kind {
                    let words = text.split_whitespace().take(8).collect::<Vec<_>>();
                    if words.len() >= 4 {
                        return Ok(words.join(" "));
                    }
                }
            }
        }
    }
    anyhow::bail!("no usable text part in the fixture corpus")
}

#[tokio::test(flavor = "multi_thread")]
async fn search_modes_each_execute_and_report_matched_via() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, backend) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    for mode in [SearchMode::Hybrid, SearchMode::Vector, SearchMode::Fts] {
        let mut request = search_request(&phrase);
        request.search_mode = mode;
        let hits = hits_of(pond_search(&store, &backend, request).await);
        assert!(
            !hits.is_empty(),
            "{mode:?} search must return hits over the corpus",
        );
        // Results are score-ordered, descending.
        for pair in hits.windows(2) {
            assert!(
                pair[0].score >= pair[1].score,
                "{mode:?} hits must be score-ordered",
            );
        }
        // `matched_via` names only retrievers valid for the mode.
        for hit in &hits {
            assert!(!hit.matched_via.is_empty());
            match mode {
                SearchMode::Vector => assert_eq!(hit.matched_via, ["vector"]),
                SearchMode::Fts => assert_eq!(hit.matched_via, ["fts"]),
                SearchMode::Hybrid => assert!(
                    hit.matched_via
                        .iter()
                        .all(|via| via == "vector" || via == "fts"),
                    "hybrid matched_via must be a subset of {{vector, fts}}: {:?}",
                    hit.matched_via,
                ),
            }
        }
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
    let hits = hits_of(pond_search(&store, &backend, request).await);
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
    let hits = hits_of(pond_search(&store, &backend, request).await);
    assert!(hits.iter().all(|hit| hit.session_id == session_id));

    // source_agent: the real agent returns hits; an unknown one returns none.
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("claude-code".to_owned());
    assert!(!hits_of(pond_search(&store, &backend, request).await).is_empty());
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("no-such-agent".to_owned());
    assert!(hits_of(pond_search(&store, &backend, request).await).is_empty());

    // date window: a far-future lower bound excludes the whole corpus.
    let mut request = search_request(&phrase);
    request.filters.from_date = Some("2099-01-01".to_owned());
    assert!(hits_of(pond_search(&store, &backend, request).await).is_empty());

    // project (exact): every hit is scoped to the requested project.
    let project = hits_of(pond_search(&store, &backend, search_request(&phrase)).await)
        .into_iter()
        .find_map(|hit| hit.project)
        .expect("fixture hits carry a project");
    let mut request = search_request(&phrase);
    request.filters.project = Some(project.clone());
    let hits = hits_of(pond_search(&store, &backend, request).await);
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|hit| hit.project.as_deref() == Some(project.as_str())),
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn project_match_is_null_pushes_down_and_returns_injected_rows() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    // The fixture corpus is all non-null `project` (Claude Code always derives
    // it from `cwd`); the only null-project rows are the ones injected below.
    let (store, _) = searchable_corpus(&temp).await?;
    let model = Config::builtin().embeddings.default_model("local")?;

    // No v1 adapter produces null-project rows, so inject canonical
    // `Session { project: None, .. }` events straight through the pond_ingest
    // handler - `is_null` lives downstream of any adapter (design.md 3.4).
    let mut events = Vec::new();
    let mut injected_ids = Vec::new();
    for n in 0..3 {
        let session_id = format!("null-project-session-{n}");
        let message_id = format!("null-project-message-{n}");
        injected_ids.push(message_id.clone());
        events.push(IngestEvent::Session(Session {
            id: session_id.clone(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "synthetic".to_owned(),
            created_at: Utc::now(),
            project: None,
            options: ProviderOptions::new(),
        }));
        events.push(IngestEvent::Message(Message::User {
            id: message_id.clone(),
            session_id: session_id.clone(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        }));
        events.push(IngestEvent::Part(Part {
            id: format!("{message_id}:0000"),
            message_id: message_id.clone(),
            ordinal: 0,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: format!("null project sentinel message {n}"),
            },
        }));
    }

    let IngestEnvelope::Success(response) = pond_ingest(
        &store,
        IngestRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: "local".to_owned(),
            events,
        },
    )
    .await
    else {
        panic!("pond_ingest must accept the injected events");
    };
    assert_eq!(response.accepted, 9);
    assert_eq!(response.rejected, 0);

    // Embed the freshly ingested messages and refresh the indices.
    let backend = FakeBackend::new(model.dim as usize);
    EmbedWorker::new(&store, &backend, &model)?.run().await?;
    store.ensure_indices().await?;
    store.ensure_embedding_indices(&model).await?;

    let mut request = search_request("null project sentinel");
    request.filters.project_match = ProjectMatch::IsNull;
    let hits = hits_of(pond_search(&store, &backend, request).await);

    assert_eq!(
        hits.len(),
        injected_ids.len(),
        "is_null returns exactly the null-project rows, not the fixture corpus",
    );
    assert!(hits.iter().all(|hit| hit.project.is_none()));
    let returned = hits
        .iter()
        .map(|hit| hit.message_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    for id in &injected_ids {
        assert!(returned.contains(id.as_str()), "missing injected row {id}");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_conversation_collapses_to_one_summary_per_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, backend) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let mut request = search_request(&phrase);
    request.group_by_conversation = true;
    let SearchEnvelope::Success(response) = pond_search(&store, &backend, request).await else {
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
