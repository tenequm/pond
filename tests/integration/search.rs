//! Handler-level integration tests for `pond_search` over a real fixture
//! corpus with a deterministic fake embedder. Pure-helper tests (RRF math,
//! recency boost, filter predicate construction, planner shape, distance
//! metric mapping) and `Store`-level vector-index tests live inline in
//! `src/handlers.rs::tests`, `src/embed/mod.rs::tests`, and
//! `src/sessions.rs::tests`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use pond::{
    adapter::ClaudeCodeAdapter,
    config::SearchConfig,
    embed::{EmbedBackend, EmbedWorker, LazyEmbedder},
    handlers::ingest_adapter,
    handlers::pond_get,
    handlers::pond_search,
    sessions::{Store, embedding_dim},
    wire::PartKind,
    wire::{
        GetEnvelope, GetRequest, GetResult, Hit, ProjectFilter, SearchEnvelope, SearchFilters,
        SearchRequest, SearchResultBody,
    },
};
use std::sync::Arc;
use tempfile::TempDir;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

/// An instrumented embedding backend: deterministic, content-dependent vectors,
/// no model weights. Enough for the vector retriever to produce a stable,
/// non-degenerate ranking and for the query side to embed.
struct FakeBackend;

impl EmbedBackend for FakeBackend {
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
    store.optimize_indices(None, None).await?.into_result()?;
    let embedder = LazyEmbedder::from_loaded(Arc::new(backend) as Arc<dyn EmbedBackend>);
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
        mode_override: None,
        similar_to: None,
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
        include_parts: true,
        cursor: None,
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

fn get_request_text_only(session_id: &str) -> GetRequest {
    GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: Some(session_id.to_owned()),
        message_id: None,
        up_to: None,
        context_depth: 0,
        max_messages: 1000,
        include_parts: false,
        cursor: None,
    }
}

/// The retrieval mode is server-determined: hybrid when the store has any
/// vectors, FTS-only otherwise. The wire surface does not carry a mode field;
/// the only observable is whether the corpus yields scored hits at all.
#[tokio::test(flavor = "multi_thread")]
async fn search_picks_hybrid_or_fts_based_on_store_state() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let hits = body_hits(
        success_of(pond_search(&store, &embedder, search_request(&phrase), &search_config()).await)
            .result,
    );
    assert!(!hits.is_empty(), "hybrid search must return hits");
    for pair in hits.windows(2) {
        assert!(pair[0].score >= pair[1].score, "hits must be score-ordered");
    }
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
    let hits = body_hits(
        success_of(
            pond_search(
                &store2,
                &embedder,
                search_request(&phrase),
                &search_config(),
            )
            .await,
        )
        .result,
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

    // role: every hit carries the requested role.
    let mut request = search_request(&phrase);
    request.filters.role = Some("assistant".to_owned());
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
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
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(hits.iter().all(|hit| hit.session_id == session_id));

    // source_agent: the real agent returns hits; an unknown one returns none.
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("claude-code".to_owned());
    assert!(!hits_of(pond_search(&store, &embedder, request, &search_config()).await).is_empty());
    let mut request = search_request(&phrase);
    request.filters.source_agent = Some("no-such-agent".to_owned());
    assert!(hits_of(pond_search(&store, &embedder, request, &search_config()).await).is_empty());

    // date window: a far-future lower bound excludes the whole corpus.
    let mut request = search_request(&phrase);
    request.filters.from_date = Some("2099-01-01".to_owned());
    assert!(hits_of(pond_search(&store, &embedder, request, &search_config()).await).is_empty());

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

#[tokio::test(flavor = "multi_thread")]
async fn group_by_conversation_collapses_to_one_summary_per_session() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, embedder) = searchable_corpus(&temp).await?;
    let phrase = corpus_phrase(&store).await?;

    let mut request = search_request(&phrase);
    request.group_by_conversation = true;
    let SearchEnvelope::Success(response) =
        pond_search(&store, &embedder, request, &search_config()).await
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
        // `session_messages_count` is the whole-session size, not the match count.
        assert!(group.session_messages_count > 0);
        assert!(group.best_score > 0.0);
        if let Some(last) = group.last_timestamp {
            assert!(
                group.first_timestamp <= last,
                "first_timestamp must precede last_timestamp when both are present",
            );
        }
    }
    assert_eq!(response.total, groups.len());

    Ok(())
}

/// spec.md#part-provenance: a harness `<task-notification>` message must be
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
        mode_override: None,
        similar_to: None,
        filters: SearchFilters::default(),
        boost_recent: true,
        group_by_conversation: false,
        limit: 50,
    };
    let embedder = LazyEmbedder::new();
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(
        hits.iter().all(|hit| hit.message_id != "u-notify"),
        "an injected task-notification must never surface as a search hit"
    );

    let GetEnvelope::Success(response) =
        pond_get(&store, get_request_text_only(session_uuid)).await
    else {
        panic!("pond_get must succeed");
    };
    let GetResult::Session { messages, .. } = response.result else {
        panic!("session get returns a session result");
    };
    assert!(
        messages.iter().any(|m| m.id == "u-notify"),
        "the injected message is preserved and returned by pond_get"
    );
    Ok(())
}

/// `pond_get` paginates over the 10k-token char budget: a session whose
/// `search_text` totals more than the budget returns `has_more=true` and a
/// continuation cursor; the continuation re-runs without re-supplying the
/// originating session_id.
#[tokio::test(flavor = "multi_thread")]
async fn pond_get_paginates_over_the_char_budget_with_a_self_contained_cursor() -> anyhow::Result<()>
{
    use chrono::{TimeZone, Utc};
    use pond::wire::{IngestRequest, Message, Provenance, ProviderOptions, Session};
    use pond::{adapter, handlers};

    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let session_id = "paginate-session".to_owned();
    let session = Session {
        id: session_id.clone(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        project: adapter::extract_str(&serde_json::json!({"x": "pond-paginate"}), "x").unwrap(),
        options: ProviderOptions::new(),
    };

    let huge_text = "abc def ghi jkl ".repeat(2000);
    let mut events: Vec<pond::handlers::IngestEvent> =
        vec![pond::handlers::IngestEvent::Session(session)];
    for index in 0..3 {
        let message_id = format!("paginate-msg-{index}");
        events.push(pond::handlers::IngestEvent::Message(Message::User {
            id: message_id.clone(),
            session_id: session_id.clone(),
            timestamp: Utc
                .with_ymd_and_hms(2026, 1, 1, 0, index as u32 + 1, 0)
                .unwrap(),
            options: ProviderOptions::new(),
        }));
        events.push(pond::handlers::IngestEvent::Part(pond::wire::Part {
            session_id: session_id.clone(),
            id: format!("paginate-part-{index}"),
            message_id,
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: pond::wire::PartKind::Text {
                text: adapter::extract_str(&serde_json::json!({"x": huge_text}), "x"),
            },
        }));
    }

    let envelope = handlers::pond_ingest(
        &store,
        IngestRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            events,
        },
    )
    .await;
    assert!(
        matches!(envelope, pond::wire::IngestEnvelope::Success(_)),
        "ingest should succeed: {envelope:?}"
    );

    let first = pond_get(
        &store,
        GetRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            session_id: Some(session_id.clone()),
            message_id: None,
            up_to: None,
            context_depth: 0,
            max_messages: 1000,
            include_parts: false,
            cursor: None,
        },
    )
    .await;
    let GetEnvelope::Success(first_response) = first else {
        panic!("first page must succeed");
    };
    assert!(
        first_response.has_more,
        "long corpus must trip the page budget"
    );
    let cursor = first_response
        .next_cursor
        .clone()
        .expect("has_more implies next_cursor");

    let second = pond_get(
        &store,
        GetRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            session_id: None,
            message_id: None,
            up_to: None,
            context_depth: 0,
            max_messages: 1000,
            include_parts: false,
            cursor: Some(cursor),
        },
    )
    .await;
    let GetEnvelope::Success(second_response) = second else {
        panic!("continuation page must succeed");
    };
    let GetResult::Session {
        messages: second_messages,
        ..
    } = second_response.result
    else {
        panic!("session-scope cursor must return a session result");
    };
    let GetResult::Session {
        messages: first_messages,
        ..
    } = first_response.result
    else {
        panic!("first page is session-scope");
    };
    assert!(
        !second_messages.is_empty(),
        "continuation must surface remaining messages"
    );
    let first_ids: std::collections::HashSet<&str> =
        first_messages.iter().map(|m| m.id.as_str()).collect();
    assert!(
        second_messages
            .iter()
            .all(|m| !first_ids.contains(m.id.as_str())),
        "cursor pages must be disjoint"
    );

    Ok(())
}
