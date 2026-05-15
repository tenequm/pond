#![allow(clippy::expect_used, clippy::unwrap_used)]

//! HTTP+JSON transport (design.md 3.6, 2.2): `POST /v1/search` and `POST /v1/get`
//! are thin adapters over the shared wire handlers. The router is driven via
//! `tower::ServiceExt::oneshot` - no socket bind, no HTTP client dependency.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use pond::{
    PROTOCOL_VERSION,
    adapter::ClaudeCodeAdapter,
    config::Config,
    embed::{EmbedBackend, EmbedWorker},
    handlers::ingest_adapter,
    sessions::Store,
    transport::{AppState, http},
    wire::{ErrorCode, GetEnvelope, SearchEnvelope},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

const FIXTURES: &str = "tests/fixtures/session-samples/claude-code/projects";

/// Deterministic, content-dependent vectors - no model weights, exact f32s.
struct FakeBackend {
    dim: usize,
}

impl EmbedBackend for FakeBackend {
    fn embed(&self, texts: &[String]) -> anyhow::Result<Vec<Vec<f32>>> {
        Ok(texts
            .iter()
            .map(|text| fake_vector(text, self.dim))
            .collect())
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

fn fake_vector(text: &str, dim: usize) -> Vec<f32> {
    let bytes = text.as_bytes();
    (0..dim)
        .map(|i| {
            let byte = bytes.get(i % bytes.len().max(1)).copied().unwrap_or(0);
            f32::from(byte) / 255.0
        })
        .collect()
}

/// Ingest the claude-code fixtures, index, embed - exactly the corpus
/// `pond serve` would expose - and wrap it in an `AppState` + router.
async fn router() -> anyhow::Result<(TempDir, Arc<Store>, Router)> {
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    ingest_adapter(&store, &ClaudeCodeAdapter::new(FIXTURES), |_| {}).await?;
    store.ensure_indices().await?;

    let model = Config::builtin().embeddings.default_model("local")?;
    let backend = FakeBackend {
        dim: model.dim as usize,
    };
    EmbedWorker::new(&store, &backend, &model)?.run().await?;
    store.ensure_embedding_indices(&model).await?;

    let store = Arc::new(store);
    let state = AppState {
        store: Arc::clone(&store),
        embedder: Some(Arc::new(backend)),
    };
    Ok((temp, store, http::router(state)))
}

async fn post(app: &Router, path: &str, body: &Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn search_and_get_round_trip() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("the fixture corpus has at least one session");

    // POST /v1/search round-trips to a success envelope.
    let (status, body) = post(
        &app,
        "/v1/search",
        &json!({ "protocol_version": PROTOCOL_VERSION, "query": "error handling" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let envelope: SearchEnvelope = serde_json::from_value(body)?;
    assert!(
        matches!(envelope, SearchEnvelope::Success(_)),
        "search should succeed over the fixture corpus",
    );

    // POST /v1/get round-trips a full session by id.
    let (status, body) = post(
        &app,
        "/v1/get",
        &json!({ "protocol_version": PROTOCOL_VERSION, "session_id": session_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let envelope: GetEnvelope = serde_json::from_value(body)?;
    let GetEnvelope::Success(response) = envelope else {
        panic!("expected a successful get");
    };
    let pond::wire::GetResult::Session { session, .. } = response.result else {
        panic!("expected a session result");
    };
    assert_eq!(session.id, session_id);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn error_envelopes_carry_typed_codes_and_statuses() -> anyhow::Result<()> {
    let (_temp, _store, app) = router().await?;

    // version_unsupported -> 400.
    let (status, body) = post(
        &app,
        "/v1/search",
        &json!({ "protocol_version": 999, "query": "x" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let SearchEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::VersionUnsupported);

    // validation_failed -> 400 (get with neither session_id nor message_id).
    let (status, body) = post(
        &app,
        "/v1/get",
        &json!({ "protocol_version": PROTOCOL_VERSION }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let GetEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::ValidationFailed);

    // not_found -> 404.
    let (status, body) = post(
        &app,
        "/v1/get",
        &json!({ "protocol_version": PROTOCOL_VERSION, "session_id": "does-not-exist" }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let GetEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::NotFound);

    Ok(())
}

async fn sse_text(app: &Router, uri: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_emits_session_message_and_end() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("fixture corpus has at least one session");

    let (status, body) = sse_text(&app, &format!("/v1/sessions/{session_id}/events")).await;
    assert_eq!(status, StatusCode::OK);
    // No `since` -> the `session` header is the first event the client sees.
    assert!(
        body.contains(&format!("event: session\nid: session:{session_id}\n")),
        "expected a leading session event: {body}",
    );
    assert!(body.contains("event: message\n"), "expected message events");
    assert!(
        body.contains(&format!("event: end\nid: end:{session_id}\n")),
        "expected a trailing end event",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_with_session_since_skips_header() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("session id");

    let (status, body) = sse_text(
        &app,
        &format!("/v1/sessions/{session_id}/events?since=session:{session_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body.contains("event: session\n"),
        "since=session:<id> must omit the header event: {body}",
    );
    assert!(body.contains("event: message\n"), "messages still flow");
    assert!(body.contains("event: end\n"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_with_end_since_is_idempotent() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("session id");

    let (status, body) = sse_text(
        &app,
        &format!("/v1/sessions/{session_id}/events?since=end:{session_id}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body.contains("event: message\n"),
        "since=end:<id> must not re-emit messages: {body}",
    );
    assert!(body.contains("event: end\n"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_with_unknown_since_message_id_400s() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let session_id = store
        .session_ids()
        .await?
        .into_iter()
        .next()
        .expect("session id");

    let (status, body) = sse_text(
        &app,
        &format!("/v1/sessions/{session_id}/events?since=not-a-real-message-id"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("validation_failed"), "got: {body}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn session_events_for_unknown_session_404s() -> anyhow::Result<()> {
    let (_temp, _store, app) = router().await?;

    let (status, body) = sse_text(&app, "/v1/sessions/does-not-exist/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body.contains("not_found"), "got: {body}");
    Ok(())
}
