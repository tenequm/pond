//! Handler-level integration tests for `pond_search` over a real fixture
//! corpus with a deterministic fake embedder. Pure-helper tests (fusion math,
//! filter predicate construction, planner shape, distance metric mapping) and
//! `Store`-level vector-index tests live inline in
//! `src/handlers.rs::tests`, `src/embed/mod.rs::tests`, and
//! `src/sessions.rs::tests`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::ClaudeCodeAdapter,
    config::SearchConfig,
    embed::{EmbedWorker, Embedder, LazyEmbedder},
    handlers::ingest_adapter,
    handlers::pond_get,
    handlers::pond_search,
    sessions::{Store, embedding_dim},
    substrate::MaintenancePolicy,
    wire::{
        GetEnvelope, GetRequest, GetResult, ProjectFilter, SearchEnvelope, SearchFilters,
        SearchModeWire, SearchRequest, SortBy,
    },
};
use std::sync::Arc;
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

/// An instrumented embedding backend: deterministic, content-dependent vectors,
/// no model weights. Enough for the vector retriever to produce a stable,
/// non-degenerate ranking and for the query side to embed.
struct FakeBackend;

impl Embedder for FakeBackend {
    fn device(&self) -> &str {
        "fake"
    }

    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|text| pseudo_vector(text)).collect())
    }
}

/// A deterministic pseudo-random vector seeded by the text's FNV-1a hash.
fn pseudo_vector(text: &str) -> Vec<f32> {
    let mut state = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (0..embedding_dim())
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
async fn searchable_corpus(temp: &TempDir) -> anyhow::Result<(Store, LazyEmbedder)> {
    let store = Store::open_local(temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(FIXTURES);
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let backend = FakeBackend;
    EmbedWorker::new(&store, &backend).run().await?;
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;
    let embedder = LazyEmbedder::from_loaded(Arc::new(backend) as Arc<dyn Embedder>);
    Ok((store, embedder))
}

fn search_config() -> SearchConfig {
    SearchConfig::default()
}

fn search_request(query: &str) -> SearchRequest {
    SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode: SearchModeWire::Vector,
        sort_by: SortBy::Relevance,
        filters: SearchFilters::default(),
        limit: 20,
    }
}

fn get_request(session_id: &str) -> GetRequest {
    GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: Some(session_id.to_owned()),
        message_id: None,
        session_limit: 1000,
        session_from: Default::default(),
        session_after_message_id: None,
        session_before_message_id: None,
        message_context_before: 3,
        message_context_after: 3,
    }
}

struct HitView {
    session_id: String,
    message_id: String,
    project: String,
    score: f64,
}

/// Unwrap a successful search response and flatten its session matches for assertions.
fn hits_of(envelope: SearchEnvelope) -> Vec<HitView> {
    match envelope {
        SearchEnvelope::Success(response) => response
            .sessions
            .into_iter()
            .flat_map(|session| {
                let session_id = session.session_id;
                let project = session.project;
                session.matches.into_iter().map(move |hit| HitView {
                    session_id: session_id.clone(),
                    message_id: hit.message_id,
                    project: project.clone(),
                    score: hit.score,
                })
            })
            .collect(),
        SearchEnvelope::Error(error) => panic!("search failed: {error:?}"),
    }
}

/// A phrase taken verbatim from an ingested message - guaranteed to produce FTS
/// hits without hard-coding fixture content.
async fn corpus_phrase(store: &Store) -> anyhow::Result<String> {
    for session_id in store.session_ids().await? {
        let GetEnvelope::Success(response) = pond_get(store, get_request(&session_id)).await else {
            continue;
        };
        let GetResult::Session { messages, .. } = response.result else {
            continue;
        };
        // Session scope renders the conversational text (search_text), which is
        // exactly what the arms index - an ideal source of FTS-able phrases.
        for text in messages.iter().filter_map(|m| m.text.as_ref()) {
            let words = text.split_whitespace().take(8).collect::<Vec<_>>();
            if words.len() >= 4 {
                return Ok(words.join(" "));
            }
        }
    }
    anyhow::bail!("no usable text in the fixture corpus")
}

/// The default `vector` arm degrades to FTS-only when the store has no
/// embeddings. Either way the corpus must yield scored, [0,1]-normalized hits.
#[tokio::test(flavor = "multi_thread")]
async fn search_vector_default_degrades_to_fts_without_embeddings() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let hits =
        hits_of(pond_search(&store, &embedder, search_request(&phrase), &search_config()).await);
    assert!(!hits.is_empty(), "vector search must return hits");
    // Display score is clamped cosine; global hit order follows session rank +
    // within-session recency, not a flat score sort, so only the bounds hold.
    for hit in &hits {
        assert!(
            (0.0..=1.0).contains(&hit.score),
            "score must be normalized to [0, 1]: {}",
            hit.score
        );
    }

    let temp2 = TempDir::new()?;
    let store2 = Store::open_local(temp2.path()).await?;
    ingest_adapter(
        &store2,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    let hits = hits_of(
        pond_search(
            &store2,
            &embedder,
            search_request(&phrase),
            &search_config(),
        )
        .await,
    );
    for hit in &hits {
        assert!(
            (0.0..=1.0).contains(&hit.score),
            "FTS-only score must be normalized to [0, 1]: {}",
            hit.score
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_narrow_results_over_the_fixture_corpus() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    // session_id: every hit belongs to the one requested session.
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .find(|id| !id.contains('/'))
        .expect("the corpus has at least one top-level session");
    let mut request = search_request(&phrase);
    request.filters.session_id = Some(session_id.clone());
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(hits.iter().all(|hit| hit.session_id == session_id));

    // date window: a far-future lower bound excludes the whole corpus.
    let mut request = search_request(&phrase);
    request.filters.from_date = Some("2099-01-01".to_owned());
    assert!(hits_of(pond_search(&store, &embedder, request, &search_config()).await).is_empty());

    // #75: a far-past lower bound must keep every hit, not prune the corpus.
    let unfiltered =
        hits_of(pond_search(&store, &embedder, search_request(&phrase), &search_config()).await)
            .len();
    let mut request = search_request(&phrase);
    request.filters.from_date = Some("2000-01-01".to_owned());
    request.filters.to_date = Some("2099-12-31".to_owned());
    assert_eq!(
        hits_of(pond_search(&store, &embedder, request, &search_config()).await).len(),
        unfiltered,
    );

    // project (contains): every hit is scoped to the requested project.
    let project =
        hits_of(pond_search(&store, &embedder, search_request(&phrase), &search_config()).await)
            .into_iter()
            .map(|hit| hit.project)
            .find(|p| !p.is_empty())
            .expect("fixture hits carry a project");
    let mut request = search_request(&phrase);
    request.filters.project = Some(ProjectFilter::Contains(project.clone()));
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(!hits.is_empty());
    assert!(
        hits.iter()
            .all(|hit| hit.project.contains(project.as_str())),
    );

    Ok(())
}

/// spec.md#search: a session-scoped hybrid search fuses per message, not per
/// session root - root keying would collapse the response to exactly one hit
/// no matter how many messages in the session match (the production
/// false-negative pattern this guards against).
#[tokio::test(flavor = "multi_thread")]
async fn session_scoped_search_returns_per_message_hits() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    // Pick the session with the most searchable (embedded) hits in an
    // unscoped search - it has at least two matchable messages.
    let SearchEnvelope::Success(unscoped) =
        pond_search(&store, &embedder, search_request(&phrase), &search_config()).await
    else {
        panic!("unscoped search must succeed");
    };
    let target = unscoped
        .sessions
        .iter()
        .max_by_key(|s| s.session_messages_count)
        .expect("unscoped search returns sessions")
        .session_id
        .clone();

    let mut request = search_request(&phrase);
    request.filters.session_id = Some(target.clone());
    let SearchEnvelope::Success(response) =
        pond_search(&store, &embedder, request, &search_config()).await
    else {
        panic!("session-scoped search must succeed");
    };

    assert_eq!(response.sessions.len(), 1, "one session in scope");
    let session = &response.sessions[0];
    assert_eq!(session.session_id, target);
    assert!(
        response.matched_total > 1,
        "per-message fusion must surface more than the single root-collapsed hit; got {}",
        response.matched_total
    );
    assert!(
        session.matches.len() > 1,
        "the session-scoped match cap widens to the requested limit; got {} matches",
        session.matches.len()
    );
    // spec.md#search-absence-honesty: the scope count reflects the session's
    // searchable messages, not the whole corpus.
    assert!(response.searchable_in_scope >= response.matched_total);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn search_returns_one_session_row_with_top_matches_per_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let SearchEnvelope::Success(response) =
        pond_search(&store, &embedder, search_request(&phrase), &search_config()).await
    else {
        panic!("search must succeed");
    };

    assert!(!response.sessions.is_empty());
    // One summary per session: session ids are unique across groups.
    let unique = response
        .sessions
        .iter()
        .map(|session| session.session_id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        unique.len(),
        response.sessions.len(),
        "each session appears at most once",
    );
    for session in &response.sessions {
        // `session_messages_count` is the whole-session size, not the match count.
        assert!(session.session_messages_count > 0);
        assert!(session.matched_message_count > 0);
        assert!(!session.matches.is_empty());
        // No per-session match cap (the old MAX_MATCHES_PER_SESSION=3 is gone);
        // scores are clamped to [0, 1].
        for hit in &session.matches {
            assert!((0.0..=1.0).contains(&hit.score));
        }
        // Within a session, matches render newest-first (recency supersession).
        for pair in session.matches.windows(2) {
            assert!(pair[0].timestamp >= pair[1].timestamp);
        }
    }
    assert!(response.matched_total >= response.sessions.len());

    Ok(())
}

/// spec.md#model-part-provenance: a harness `<task-notification>` message must be
/// absent from search results yet still returned in full by `pond_get`.
#[tokio::test(flavor = "multi_thread")]
async fn injected_task_notification_is_excluded_from_search_but_kept_for_get() -> anyhow::Result<()>
{
    let corpus = TempDir::new()?;
    let project_dir = corpus.path().join("-tmp-pond-provenance");
    std::fs::create_dir_all(&project_dir)?;
    let session_uuid = "77777777-7777-7777-7777-777777777777";
    let marker = "zzqqx-unique-notification-marker";
    let prompt = serde_json::json!({
        "type": "user",
        "uuid": "u-prompt",
        "sessionId": session_uuid,
        "cwd": "/tmp/pond-provenance",
        "timestamp": "2026-05-16T00:00:00.000Z",
        "message": {"role": "user", "content": "ordinary conversational prompt"},
    });
    let notification = serde_json::json!({
        "type": "user",
        "uuid": "u-notify",
        "sessionId": session_uuid,
        "cwd": "/tmp/pond-provenance",
        "timestamp": "2026-05-16T00:00:01.000Z",
        "message": {
            "role": "user",
            "content": format!("<task-notification>{marker}</task-notification>"),
        },
    });
    std::fs::write(
        project_dir.join(format!("{session_uuid}.jsonl")),
        format!("{prompt}\n{notification}\n"),
    )?;

    let store_temp = TempDir::new()?;
    let store = Store::open_local(store_temp.path()).await?;
    let adapter = ClaudeCodeAdapter::new(corpus.path());
    ingest_adapter(&store, &adapter, &pond::adapter::NoopOracle, |_| {}).await?;

    let request = SearchRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: marker.to_owned(),
        mode: SearchModeWire::Vector,
        sort_by: SortBy::Relevance,
        filters: SearchFilters::default(),
        limit: 50,
    };
    let embedder = LazyEmbedder::candle();
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(
        hits.iter().all(|hit| hit.message_id != "u-notify"),
        "an injected task-notification must never surface as a search hit"
    );

    let GetEnvelope::Success(default_response) = pond_get(&store, get_request(session_uuid)).await
    else {
        panic!("pond_get must succeed");
    };
    let GetResult::Session {
        messages: default_messages,
        ..
    } = default_response.result
    else {
        panic!("session get returns a session result");
    };
    assert!(
        default_messages.iter().all(|m| m.id != "u-notify"),
        "injected message is filtered from the conversational view by default (spec.md#search)"
    );

    // The injected message is filtered from search and the conversational
    // session view, but the data is preserved and reachable by id via message
    // scope (the "give me this exact message" path).
    let mut by_id = get_request(session_uuid);
    by_id.session_id = None;
    by_id.message_id = Some("u-notify".to_owned());
    let GetEnvelope::Success(restore_response) = pond_get(&store, by_id).await else {
        panic!("message-scope pond_get must succeed");
    };
    let GetResult::Message { target, .. } = restore_response.result else {
        panic!("message-scope get returns a message result");
    };
    assert_eq!(
        target.id, "u-notify",
        "injected message is preserved and reachable by id"
    );
    Ok(())
}

/// spec.md#protocol: message-scope context siblings are always the
/// conversational view - in carrier-heavy sessions the system/tool rows never
/// crowd the conversation out of the context window.
#[tokio::test(flavor = "multi_thread")]
async fn message_context_siblings_are_conversational() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, _embedder) = searchable_corpus(&temp).await?;

    // A target with conversational neighbors on both sides: a session with at
    // least three conversational messages, target its middle one.
    let mut target_id = None;
    for session_id in store.session_ids().await? {
        let GetEnvelope::Success(response) = pond_get(&store, get_request(&session_id)).await
        else {
            continue;
        };
        let GetResult::Session { messages, .. } = response.result else {
            continue;
        };
        let conversational: Vec<&String> = messages
            .iter()
            .filter_map(|m| m.text.as_ref().map(|_| &m.id))
            .collect();
        if conversational.len() >= 3 {
            target_id = Some(conversational[conversational.len() / 2].clone());
            break;
        }
    }
    let target_id =
        target_id.expect("fixtures contain a session with >= 3 conversational messages");

    let request = GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: None,
        message_id: Some(target_id),
        session_limit: 20,
        session_from: Default::default(),
        session_after_message_id: None,
        session_before_message_id: None,
        message_context_before: 5,
        message_context_after: 5,
    };
    let GetEnvelope::Success(response) = pond_get(&store, request).await else {
        panic!("message-scope get must succeed");
    };
    let GetResult::Message { siblings, .. } = response.result else {
        panic!("message-scope result expected");
    };
    assert!(
        !siblings.is_empty(),
        "the target has conversational neighbors"
    );
    assert!(
        siblings.iter().all(|m| m.text.is_some()),
        "the context window must hold only conversational siblings"
    );
    Ok(())
}
