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
    wire::PartKind,
    wire::{
        GetEnvelope, GetRequest, GetResult, ProjectFilter, ResponseMode, SearchEnvelope,
        SearchFilters, SearchRequest,
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
        mode_override: None,
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
        context_depth: 0,
        limit: 1000,
        response_mode: ResponseMode::Verbatim,
        session_from: Default::default(),
        after_id: None,
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
        for part in messages.iter().filter_map(|m| m.parts.as_ref()).flatten() {
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
        context_depth: 0,
        limit: 1000,
        response_mode: ResponseMode::Conversational,
        session_from: Default::default(),
        after_id: None,
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

    let hits =
        hits_of(pond_search(&store, &embedder, search_request(&phrase), &search_config()).await);
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
        assert!(session.matches.len() <= 3);
        assert!(session.matches[0].score > 0.0);
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
        mode_override: None,
        filters: SearchFilters::default(),
        limit: 50,
    };
    let embedder = LazyEmbedder::candle();
    let hits = hits_of(pond_search(&store, &embedder, request, &search_config()).await);
    assert!(
        hits.iter().all(|hit| hit.message_id != "u-notify"),
        "an injected task-notification must never surface as a search hit"
    );

    let GetEnvelope::Success(default_response) =
        pond_get(&store, get_request_text_only(session_uuid)).await
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

    let GetEnvelope::Success(restore_response) = pond_get(&store, get_request(session_uuid)).await
    else {
        panic!("pond_get must succeed in verbatim mode");
    };
    let GetResult::Session {
        messages: restore_messages,
        ..
    } = restore_response.result
    else {
        panic!("session get returns a session result");
    };
    assert!(
        restore_messages.iter().any(|m| m.id == "u-notify"),
        "injected message is preserved and reachable in verbatim mode"
    );
    Ok(())
}

/// `pond_get` paginates over the response byte budget: a session whose
/// `search_text` totals more than the budget returns `messages_remaining > 0`,
/// and re-requesting with `after_id` set to the last returned message id
/// surfaces the rest, disjoint from the first page.
#[tokio::test(flavor = "multi_thread")]
async fn pond_get_paginates_over_the_byte_budget_via_after_id() -> anyhow::Result<()> {
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

    // ~80KB per message; three of them exceed the ~200KB page budget so the
    // first page stops mid-session.
    let huge_text = "abc def ghi jkl ".repeat(5000);
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
            context_depth: 0,
            limit: 1000,
            response_mode: ResponseMode::Conversational,
            session_from: Default::default(),
            after_id: None,
        },
    )
    .await;
    let GetEnvelope::Success(first_response) = first else {
        panic!("first page must succeed");
    };
    let GetResult::Session {
        messages: first_messages,
        messages_remaining,
        ..
    } = first_response.result
    else {
        panic!("first page is session-scope");
    };
    assert!(
        messages_remaining > 0,
        "long corpus must trip the page budget"
    );
    let after_id = first_messages
        .last()
        .expect("first page is non-empty")
        .id
        .clone();

    let second = pond_get(
        &store,
        GetRequest {
            protocol_version: pond::PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            session_id: Some(session_id.clone()),
            message_id: None,
            context_depth: 0,
            limit: 1000,
            response_mode: ResponseMode::Conversational,
            session_from: Default::default(),
            after_id: Some(after_id),
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
        panic!("continuation must return a session result");
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
        "after_id pages must be disjoint"
    );

    Ok(())
}

/// spec.md#protocol: message-mode context siblings follow `response_mode` -
/// conversational by default, so system/tool carriers don't crowd the
/// conversation out of the +-depth window; `complete` opts back in.
#[tokio::test(flavor = "multi_thread")]
async fn message_context_siblings_default_to_conversational() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let (store, _embedder) = searchable_corpus(&temp).await?;

    // Find a conversational target within reach of a carrier message.
    let mut target_id = None;
    'sessions: for session_id in store.session_ids().await? {
        let mut request = get_request(&session_id);
        request.response_mode = ResponseMode::Complete;
        let GetEnvelope::Success(response) = pond_get(&store, request).await else {
            continue;
        };
        let GetResult::Session { messages, .. } = response.result else {
            continue;
        };
        for (idx, message) in messages.iter().enumerate() {
            if message.text.is_none() {
                continue; // carrier; need a conversational target
            }
            let lo = idx.saturating_sub(5);
            let hi = (idx + 6).min(messages.len());
            if messages[lo..hi].iter().any(|m| m.text.is_none()) {
                target_id = Some(message.id.clone());
                break 'sessions;
            }
        }
    }
    let target_id = target_id.expect("fixtures contain a carrier near a conversational message");

    let message_request = |response_mode: ResponseMode| GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: None,
        message_id: Some(target_id.clone()),
        context_depth: 5,
        limit: 1000,
        response_mode,
        session_from: Default::default(),
        after_id: None,
    };

    let GetEnvelope::Success(default_response) =
        pond_get(&store, message_request(ResponseMode::Conversational)).await
    else {
        panic!("message-mode get must succeed");
    };
    let GetResult::Message { siblings, .. } = default_response.result else {
        panic!("message-mode result expected");
    };
    assert!(
        siblings.iter().all(|m| m.text.is_some()),
        "default context window must hold only conversational siblings"
    );

    let GetEnvelope::Success(complete_response) =
        pond_get(&store, message_request(ResponseMode::Complete)).await
    else {
        panic!("complete-mode get must succeed");
    };
    let GetResult::Message { siblings, .. } = complete_response.result else {
        panic!("message-mode result expected");
    };
    assert!(
        siblings.iter().any(|m| m.text.is_none()),
        "complete mode opts carriers back into the window"
    );
    Ok(())
}

/// `pond_get(session_from = "end")` returns the newest `limit` messages of a
/// session in chronological order (the compaction-recovery path); `start`
/// returns the oldest. The two are disjoint ends of the same session.
#[tokio::test(flavor = "multi_thread")]
async fn pond_get_session_from_end_returns_the_recent_tail() -> anyhow::Result<()> {
    use chrono::{TimeZone, Utc};
    use pond::wire::{
        GetResult as WireGetResult, IngestRequest, Message, Provenance, ProviderOptions, Session,
        SessionFrom,
    };
    use pond::{adapter, handlers};

    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;

    let session_id = "tail-session".to_owned();
    let session = Session {
        id: session_id.clone(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        project: adapter::extract_str(&serde_json::json!({"x": "pond-tail"}), "x").unwrap(),
        options: ProviderOptions::new(),
    };
    let mut events: Vec<pond::handlers::IngestEvent> =
        vec![pond::handlers::IngestEvent::Session(session)];
    for index in 0..5u32 {
        let message_id = format!("tail-msg-{index}");
        events.push(pond::handlers::IngestEvent::Message(Message::User {
            id: message_id.clone(),
            session_id: session_id.clone(),
            timestamp: Utc.with_ymd_and_hms(2026, 1, 1, 0, index + 1, 0).unwrap(),
            options: ProviderOptions::new(),
        }));
        events.push(pond::handlers::IngestEvent::Part(pond::wire::Part {
            session_id: session_id.clone(),
            id: format!("tail-part-{index}"),
            message_id,
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: pond::wire::PartKind::Text {
                text: adapter::extract_str(
                    &serde_json::json!({ "x": format!("message {index}") }),
                    "x",
                ),
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

    let request = |from: SessionFrom| GetRequest {
        protocol_version: pond::PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: Some(session_id.clone()),
        message_id: None,
        context_depth: 0,
        limit: 2,
        response_mode: ResponseMode::Conversational,
        session_from: from,
        after_id: None,
    };
    let page = |envelope: GetEnvelope| -> (Vec<String>, usize) {
        let GetEnvelope::Success(response) = envelope else {
            panic!("get must succeed");
        };
        let WireGetResult::Session {
            messages,
            messages_remaining,
            ..
        } = response.result
        else {
            panic!("session-scope result expected");
        };
        (
            messages.into_iter().map(|m| m.id).collect(),
            messages_remaining,
        )
    };

    let (end_ids, end_remaining) = page(pond_get(&store, request(SessionFrom::End)).await);
    assert_eq!(
        end_ids,
        ["tail-msg-3", "tail-msg-4"],
        "end returns the newest two, in chronological order"
    );
    assert_eq!(
        end_remaining, 3,
        "three older messages remain before the tail"
    );

    let (start_ids, _) = page(pond_get(&store, request(SessionFrom::Start)).await);
    assert_eq!(
        start_ids,
        ["tail-msg-0", "tail-msg-1"],
        "start returns the oldest two"
    );

    Ok(())
}
