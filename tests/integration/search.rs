//! Handler-level integration tests for `pond_search` over a real fixture
//! corpus with a deterministic fake embedder. Pure-helper tests (fusion math,
//! filter predicate construction, planner shape, distance metric mapping) and
//! `Store`-level vector-index tests live inline in
//! `src/handlers.rs::tests`, `src/embed/mod.rs::tests`, and
//! `src/sessions.rs::tests`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use chrono::Utc;
use pond::{
    adapter::ClaudeCodeAdapter,
    config::SearchConfig,
    embed::{EmbedWorker, Embedder, LazyEmbedder},
    handlers::ingest_adapter,
    handlers::pond_get,
    handlers::pond_search,
    sessions::{IngestEvent, Store, embedding_dim},
    substrate::MaintenancePolicy,
    wire::{
        GetEnvelope, GetRequest, GetResult, Message, Part, PartKind, ProjectFilter, Provenance,
        ProviderOptions, SearchEnvelope, SearchFilters, SearchModeWire, SearchRequest, Session,
        SortBy,
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

/// A fresh process searching a remote store must serve `_indices/*` from the
/// on-disk cache, not the store. A second `Store` over the same shared-memory
/// bytes has an empty in-memory index cache, so its search reads the index
/// files through the wrapper and populates the disk cache.
#[tokio::test(flavor = "multi_thread")]
async fn index_disk_cache_populates_from_a_fresh_reader() -> anyhow::Result<()> {
    fn has_cached_index(dir: &std::path::Path) -> bool {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return false;
        };
        entries.flatten().any(|entry| {
            let path = entry.path();
            if path.is_dir() {
                entry.file_name() == "_indices" || has_cached_index(&path)
            } else {
                false
            }
        })
    }

    let cache = TempDir::new()?;
    let url = url::Url::parse("shared-memory://pond-test-index-disk-cache/")?;
    let caps = pond::substrate::RuntimeCaps::default();

    let writer = Store::open_with_options(&url, std::collections::HashMap::new(), caps).await?;
    ingest_adapter(
        &writer,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    EmbedWorker::new(&writer, &FakeBackend).run().await?;
    writer
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;
    let phrase = corpus_phrase(&writer).await?;

    let reader = Store::open_with_options_cached(
        &url,
        std::collections::HashMap::new(),
        caps,
        Some(cache.path().to_path_buf()),
    )
    .await?;
    let embedder = LazyEmbedder::from_loaded(Arc::new(FakeBackend) as Arc<dyn Embedder>);
    let hits = hits_of(
        pond_search(
            &reader,
            &embedder,
            search_request(&phrase),
            &search_config(),
        )
        .await,
    );
    assert!(!hits.is_empty(), "fresh reader must still return hits");
    assert!(
        has_cached_index(cache.path()),
        "search through the wrapper must cache _indices files under {}",
        cache.path().display(),
    );
    Ok(())
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

    // #75: a wide far-past..far-future window must keep every hit, not prune the corpus.
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

/// f3 recall guard: sync batches the FTS/vector fold, so between folds an
/// unindexed tail exists. Recall must stay complete - the retrievers drop
/// `fast_search` when an index has a tail and flat-scan it. A term that lives
/// only in the unfolded tail must still be found, and stays found after the fold.
#[tokio::test(flavor = "multi_thread")]
async fn fts_search_covers_the_unindexed_tail() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?; // fixtures ingested + folded (tail = 0)

    // A distinctive term absent from the fixtures, ingested WITHOUT a fold so it
    // lives only in the unindexed tail.
    let marker = "quokkanaut";
    let session_id = "01HXYTAILRECALL0000000000";
    let events = vec![
        IngestEvent::Session(Session {
            id: session_id.to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: "claude-code".to_owned(),
            created_at: Utc::now(),
            project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/tail"}), "x")
                .unwrap(),
            options: ProviderOptions::new(),
        }),
        IngestEvent::Message(Message::User {
            id: "tail-msg".to_owned(),
            session_id: session_id.to_owned(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        }),
        IngestEvent::Part(Part {
            session_id: session_id.to_owned(),
            id: "tail-msg:0001".to_owned(),
            message_id: "tail-msg".to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: pond::adapter::extract_str(
                    &serde_json::json!({"x": format!("the {marker} appeared at dawn")}),
                    "x",
                ),
            },
        }),
    ];
    pond::handlers::ingest_events(&store, events).await?;

    let fts = |q: &str| SearchRequest {
        mode: SearchModeWire::Fts,
        ..search_request(q)
    };

    // Deferred fold: the marker is only in the tail. `fast_search` would miss it;
    // the retriever must drop `fast_search` and flat-scan the tail.
    let hits = hits_of(pond_search(&store, &embedder, fts(marker), &search_config()).await);
    assert!(
        hits.iter().any(|h| h.session_id == session_id),
        "tail-only term must be found while the fold is deferred (complete recall)",
    );

    // After folding, the same term is served from the index.
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;
    let hits = hits_of(pond_search(&store, &embedder, fts(marker), &search_config()).await);
    assert!(
        hits.iter().any(|h| h.session_id == session_id),
        "tail term still found after the fold",
    );
    Ok(())
}

/// spec.md#search subagent exclusion covers BOTH shapes: claude-code composite
/// ids (`<parent>/agent-x`, caught pre-hydration by `retain_non_subagents`) and
/// openclaw plain-id subagents whose subagent-ness lives only in a `/`-subpath
/// `source_agent` (caught at the hydrated-meta stage). A root `source_agent`
/// keeps the exclusion; naming the subpath opts in.
#[tokio::test(flavor = "multi_thread")]
async fn source_agent_subpath_exclusion_covers_plain_id_openclaw_subagents() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;

    let marker = "narwhalescope";
    // An openclaw main session, its plain-id subagent (subagent-ness only in
    // source_agent), and a claude-code composite-id subagent (subagent-ness in
    // the id). Each carries the distinctive marker in one user message.
    let sessions = [
        ("01OPENCLAWMAIN000000000000", "openclaw"),
        ("01OPENCLAWSUBAGENT00000000", "openclaw/subagent"),
        ("01CCPARENT0000/agent-child", "claude-code/general-purpose"),
    ];
    let mut events = Vec::new();
    for (idx, (sid, agent)) in sessions.iter().enumerate() {
        let mid = format!("m-{idx}");
        events.push(IngestEvent::Session(Session {
            id: (*sid).to_owned(),
            parent_session_id: None,
            parent_message_id: None,
            source_agent: (*agent).to_owned(),
            created_at: Utc::now(),
            project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/oc"}), "x").unwrap(),
            options: ProviderOptions::new(),
        }));
        events.push(IngestEvent::Message(Message::User {
            id: mid.clone(),
            session_id: (*sid).to_owned(),
            timestamp: Utc::now(),
            options: ProviderOptions::new(),
        }));
        events.push(IngestEvent::Part(Part {
            session_id: (*sid).to_owned(),
            id: format!("{mid}:0000"),
            message_id: mid.clone(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: pond::adapter::extract_str(
                    &serde_json::json!({"x": format!("the {marker} surfaced today")}),
                    "x",
                ),
            },
        }));
    }
    pond::handlers::ingest_events(&store, events).await?;

    let fts = |filters: SearchFilters| SearchRequest {
        mode: SearchModeWire::Fts,
        filters,
        ..search_request(marker)
    };
    let success = |envelope: SearchEnvelope| match envelope {
        SearchEnvelope::Success(response) => response,
        SearchEnvelope::Error(error) => panic!("search failed: {error:?}"),
    };
    let session_ids = |envelope: SearchEnvelope| -> Vec<String> {
        hits_of(envelope)
            .into_iter()
            .map(|h| h.session_id)
            .collect()
    };

    // Default: both subagent shapes excluded; only the openclaw main survives.
    // matched_total drops the meta-excluded subagent too (mirror of the id-based
    // retain's drop-before-count), so it is not inflated.
    let default = success(
        pond_search(
            &store,
            &embedder,
            fts(SearchFilters::default()),
            &search_config(),
        )
        .await,
    );
    assert_eq!(
        default
            .sessions
            .iter()
            .map(|s| s.session_id.clone())
            .collect::<Vec<_>>(),
        vec!["01OPENCLAWMAIN000000000000".to_owned()],
        "default excludes both subagent shapes",
    );
    assert_eq!(
        default.matched_total, 1,
        "meta-excluded subagent must not inflate matched_total",
    );

    // Explicit subpath filter returns the plain-id openclaw subagent.
    let got = session_ids(
        pond_search(
            &store,
            &embedder,
            fts(SearchFilters {
                source_agent: Some("openclaw/subagent".to_owned()),
                ..SearchFilters::default()
            }),
            &search_config(),
        )
        .await,
    );
    assert_eq!(
        got,
        vec!["01OPENCLAWSUBAGENT00000000".to_owned()],
        "subpath filter reaches the subagent it names",
    );

    // Root value keeps the exclusion: the SQL subpath arm matches the subagent
    // row, but the meta-stage check drops it - only the main session returns.
    let got = session_ids(
        pond_search(
            &store,
            &embedder,
            fts(SearchFilters {
                source_agent: Some("openclaw".to_owned()),
                ..SearchFilters::default()
            }),
            &search_config(),
        )
        .await,
    );
    assert_eq!(
        got,
        vec!["01OPENCLAWMAIN000000000000".to_owned()],
        "root value stays main-sessions-only",
    );

    Ok(())
}
