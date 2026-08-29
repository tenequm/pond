#![allow(clippy::expect_used, clippy::unwrap_used)]

//! HTTP+JSON transport (spec.md#protocol, spec.md#protocol):
//! `POST /v1/search`, `POST /v1/get-session`, and `POST /v1/get-message`
//! are thin adapters over the shared wire handlers. The router is driven via
//! `tower::ServiceExt::oneshot` - no socket bind, no HTTP client dependency.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode},
};
use pond::{
    PROTOCOL_VERSION,
    adapter::ClaudeCodeAdapter,
    embed::{EmbedWorker, Embedder},
    handlers::ingest_adapter,
    sessions::{Store, embedding_dim},
    substrate::MaintenancePolicy,
    transport::{AppState, http},
    wire::{ErrorCode, GetEnvelope, SearchEnvelope},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

const FIXTURES: &str = "tests/fixtures/adapter/claude_code/projects";

/// Deterministic, content-dependent vectors - no model weights, exact f32s.
struct FakeBackend;

impl Embedder for FakeBackend {
    fn device(&self) -> &str {
        "fake"
    }

    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|text| fake_vector(text)).collect())
    }
}

fn fake_vector(text: &str) -> Vec<f32> {
    let bytes = text.as_bytes();
    (0..embedding_dim())
        .map(|i| {
            let byte = bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0);
            f32::from(byte) / 255.0
        })
        .collect()
}

/// Ingest the claude-code fixtures, index, embed - exactly the corpus
/// `pond serve` would expose - and wrap it in an `AppState` + router.
async fn router() -> anyhow::Result<(TempDir, Arc<Store>, Router)> {
    // The vector arm is refused unless this instance opted in.
    pond::embed::init_enabled(true);
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(FIXTURES),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;

    let backend = FakeBackend;
    EmbedWorker::new(&store, &backend).run().await?;
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;

    let store = Arc::new(store);
    let state = AppState {
        store: Arc::clone(&store),
        embedder: Arc::new(pond::embed::LazyEmbedder::from_loaded(Arc::new(backend))),
        search: pond::config::SearchConfig::default(),
    };
    Ok((temp, store, http::router(state, &[])))
}

/// The `/mcp` route validates `Host` against an allowlist (the MCP spec's
/// DNS-rebinding defence, carried by rmcp) and that list is loopback-only
/// unless `serve` is told otherwise - so a server reached by its own public
/// name answers `/mcp` with 403 until that name is passed in. `/v1/*` carries
/// no such check, which is why a hosted pond can look healthy on the JSON API
/// while every MCP client is refused.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_route_gates_on_the_host_allowlist() -> anyhow::Result<()> {
    pond::embed::init_enabled(true);
    let temp = TempDir::new()?;
    let store = Arc::new(Store::open_local(temp.path()).await?);
    let state = AppState {
        store,
        embedder: Arc::new(pond::embed::LazyEmbedder::from_loaded(Arc::new(
            FakeBackend,
        ))),
        search: pond::config::SearchConfig::default(),
    };
    let app = http::router(state, &["pond.example.com".to_owned()]);

    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pond-test", "version": "0"},
        },
    });

    // Loopback keeps working: the defaults are extended, not replaced.
    assert_eq!(
        mcp_status(&app, "localhost", &initialize).await,
        StatusCode::OK
    );
    // The name the deployment actually answers to.
    assert_eq!(
        mcp_status(&app, "pond.example.com", &initialize).await,
        StatusCode::OK
    );
    // Anything else is still refused.
    assert_eq!(
        mcp_status(&app, "attacker.example.com", &initialize).await,
        StatusCode::FORBIDDEN
    );

    // Same rejected `Host`, unrelated route: the JSON API is not gated.
    let search = json!({"protocol_version": PROTOCOL_VERSION, "query": "anything"});
    let request = Request::builder()
        .method("POST")
        .uri("/v1/search")
        .header("host", "attacker.example.com")
        .header("content-type", "application/json")
        .body(Body::from(search.to_string()))
        .unwrap();
    let status = app.clone().oneshot(request).await.unwrap().status();
    assert_ne!(status, StatusCode::FORBIDDEN);

    Ok(())
}

/// `POST /mcp` under one `Host`, reporting only the status - the allowlist is
/// checked before the request is parsed, so the body never matters here.
async fn mcp_status(app: &Router, host: &str, body: &Value) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", host)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .body(Body::from(body.to_string()))
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

async fn post(app: &Router, path: &str, body: &Value) -> (StatusCode, HeaderMap, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, headers, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn search_and_get_round_trip() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        // A subagent session id contains a `/`; embedded in a URL path it
        // would not route. These tests address sessions over HTTP, so pick a
        // top-level (path-safe) session id.
        .find(|id| !id.contains('/'))
        .expect("the fixture corpus has at least one session");

    // POST /v1/search round-trips to a success envelope on the vector arm.
    let (status, headers, body) = post(
        &app,
        "/v1/search",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "query": "error handling",
            "mode": "vector",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.contains_key("x-pond-request-id"));
    assert!(
        body.get("request_id").is_none(),
        "Option B keeps request ids in the HTTP header, not the body: {body}",
    );
    let envelope: SearchEnvelope = serde_json::from_value(body)?;
    assert!(
        matches!(envelope, SearchEnvelope::Success(_)),
        "search should succeed over the fixture corpus",
    );

    // An absent `mode` takes the fts arm. The JSON envelope carries no mode
    // field, so the arms are told apart by behavior: BM25 needs real token
    // overlap and finds nothing for a token no message holds, while kNN always
    // returns the nearest rows.
    let gibberish = json!({ "protocol_version": PROTOCOL_VERSION, "query": "zqxjvwkhbrmp" });
    let (status, _headers, body) = post(&app, "/v1/search", &gibberish).await;
    assert_eq!(status, StatusCode::OK);
    let SearchEnvelope::Success(default_arm) = serde_json::from_value(body)? else {
        panic!("expected a successful search");
    };
    assert_eq!(
        default_arm.matched_total, 0,
        "the default arm is fts: an unseen token matches nothing",
    );

    let mut as_vector = gibberish;
    as_vector["mode"] = json!("vector");
    let (status, _headers, body) = post(&app, "/v1/search", &as_vector).await;
    assert_eq!(status, StatusCode::OK);
    let SearchEnvelope::Success(vector_arm) = serde_json::from_value(body)? else {
        panic!("expected a successful search");
    };
    assert!(
        vector_arm.matched_total > 0,
        "the vector arm returns the nearest rows for any query",
    );

    // POST /v1/get-session round-trips a full session by id.
    let (status, _headers, body) = post(
        &app,
        "/v1/get-session",
        &json!({ "protocol_version": PROTOCOL_VERSION, "id": session_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let envelope: GetEnvelope = serde_json::from_value(body)?;
    let GetEnvelope::Success(response) = envelope else {
        panic!("expected a successful get");
    };
    assert_eq!(response.session.id, session_id);
    let pond::wire::GetResult::Session { .. } = response.result else {
        panic!("expected a session result");
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn error_envelopes_carry_typed_codes_and_statuses() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .find(|id| !id.contains('/'))
        .expect("the fixture corpus has at least one session");

    // version_unsupported -> 400.
    let (status, headers, body) = post(
        &app,
        "/v1/search",
        &json!({ "protocol_version": 999, "query": "x" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(headers.contains_key("x-pond-request-id"));
    let SearchEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::VersionUnsupported);

    // validation_failed -> 400 (after_message_id and before_message_id are
    // mutually exclusive pagination anchors).
    let (status, _headers, body) = post(
        &app,
        "/v1/get-session",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "id": session_id,
            "after_message_id": "does-not-exist",
            "before_message_id": "also-does-not-exist",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let GetEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::ValidationFailed);

    // not_found -> 404.
    let (status, _headers, body) = post(
        &app,
        "/v1/get-session",
        &json!({ "protocol_version": PROTOCOL_VERSION, "id": "does-not-exist" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let GetEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::NotFound);

    Ok(())
}
