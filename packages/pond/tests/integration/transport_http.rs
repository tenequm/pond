#![allow(clippy::expect_used, clippy::unwrap_used)]

//! HTTP+JSON transport (spec.md#protocol, spec.md#protocol):
//! `POST /v1/search`, `POST /v1/get-session`, `POST /v1/get-message`, and
//! `POST /v1/x/sql` are thin adapters over the shared wire handlers. The router
//! is driven via `tower::ServiceExt::oneshot` - no HTTP client dependency. The
//! exceptions bind a socket: `shutdown_completes_while_an_mcp_stream_is_open`,
//! because the hang it covers is in the connection drain and `oneshot` never
//! opens a connection to drain, and `unix_socket_serves_sql_and_is_removed_on_shutdown`,
//! which is about the socket's lifecycle.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

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
    wire::{ErrorCode, GetEnvelope, SearchEnvelope, SqlEnvelope},
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
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
    let state = AppState::new(
        Arc::clone(&store),
        Arc::new(pond::embed::LazyEmbedder::from_loaded(Arc::new(backend))),
        pond::config::SearchConfig::default(),
    );
    Ok((
        temp,
        store,
        http::router(state, &[], tokio_util::sync::CancellationToken::new()),
    ))
}

/// An `AppState` over an empty store, for the tests that never read a row: the
/// fixture corpus [`router`] ingests would only add cost to them.
async fn empty_state(temp: &TempDir) -> anyhow::Result<AppState> {
    // The vector arm is refused unless this instance opted in.
    pond::embed::init_enabled(true);
    Ok(AppState::new(
        Arc::new(Store::open_local(temp.path()).await?),
        Arc::new(pond::embed::LazyEmbedder::from_loaded(Arc::new(
            FakeBackend,
        ))),
        pond::config::SearchConfig::default(),
    ))
}

/// Shutdown has to finish while an MCP client is attached. axum's graceful
/// shutdown waits for every in-flight connection, and a streamable-HTTP client
/// holds its `GET /mcp` stream open for the whole session, so before the
/// session cancel this hung until the supervisor killed the process. Driven
/// over a real socket, because the hang is in the connection drain and a
/// `oneshot` against the `Router` never binds one.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_completes_while_an_mcp_stream_is_open() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = empty_state(&temp).await?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        http::serve_with_shutdown(listener, state, &[], async move {
            let _ = stopped.await;
        })
        .await
    });

    let session = mcp_session(addr).await?;
    // Hold it: an initialized session's `GET /mcp` is the long-lived SSE stream
    // a connected agent keeps open. Reading the head proves it is established;
    // the socket then stays in scope, unread and unclosed, across the shutdown.
    let mut stream = TcpStream::connect(addr).await?;
    let head = request(
        &mut stream,
        &format!(
            "GET /mcp HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\
             Mcp-Session-Id: {session}\r\n\r\n"
        ),
    )
    .await?;
    // Guard against this test quietly going vacuous: if the stream were ever
    // refused, the connection would close and shutdown would finish for the
    // wrong reason, still green.
    assert!(
        head.starts_with("HTTP/1.1 200")
            && head
                .to_ascii_lowercase()
                .contains("content-type: text/event-stream"),
        "the /mcp stream has to be established for this test to mean anything:\n{head}"
    );

    let started = Instant::now();
    stop.send(()).expect("serve task should still be running");

    let served = tokio::time::timeout(Duration::from_secs(30), server)
        .await
        .expect("serve must stop while a client holds its /mcp stream open")?;
    served?;

    // This is what pins the session teardown rather than the backstop. Drop
    // `.with_cancellation_token(..)` in transport.rs and the deadline arm still
    // returns `Ok(())`, just after SHUTDOWN_DRAIN, so every assertion above
    // stays green; bounding the elapsed time is what fails.
    let elapsed = started.elapsed();
    assert!(
        elapsed < http::SHUTDOWN_DRAIN,
        "shutdown took {elapsed:?}, so the drain deadline returned rather than \
         the MCP session teardown letting it finish inside SHUTDOWN_DRAIN ({:?})",
        http::SHUTDOWN_DRAIN
    );
    drop(stream);
    Ok(())
}

/// Initialize an MCP session over a fresh connection and return its id.
async fn mcp_session(addr: SocketAddr) -> anyhow::Result<String> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pond-test", "version": "0"},
        },
    })
    .to_string();
    let mut stream = TcpStream::connect(addr).await?;
    let head = request(
        &mut stream,
        &format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await?;
    head.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("mcp-session-id")
                .then(|| value.trim().to_owned())
        })
        .ok_or_else(|| anyhow::anyhow!("no mcp-session-id in response head:\n{head}"))
}

/// Write one raw HTTP/1.1 request and read back just the response head. Raw
/// rather than through a client crate: the point is to own the socket and
/// decide when it closes, which is what this test is about.
async fn request(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    raw: &str,
) -> anyhow::Result<String> {
    // Deadlined: the read below has no natural end, so a regression that stalls
    // before emitting headers would hang this test until the CI runner's limit
    // instead of failing it.
    tokio::time::timeout(RESPONSE_HEAD_TIMEOUT, async {
        stream.write_all(raw.as_bytes()).await?;
        let mut head = Vec::new();
        let mut byte = [0_u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).await? == 0 {
                anyhow::bail!("connection closed before the response head was complete");
            }
            head.push(byte[0]);
        }
        Ok(String::from_utf8_lossy(&head).into_owned())
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for a response head"))?
}

/// Deadline on one request/response head (see [`request`]).
const RESPONSE_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

/// The `/mcp` route validates `Host` against an allowlist (the MCP spec's
/// DNS-rebinding defence, carried by rmcp) and that list is loopback-only
/// unless `serve` is told otherwise - so a server reached by its own public
/// name answers `/mcp` with 403 until that name is passed in. The `/v1/*`
/// routes other than `/v1/x/sql` carry no such check, which is why a hosted
/// pond can look healthy on the JSON API while every MCP client is refused.
#[tokio::test(flavor = "multi_thread")]
async fn mcp_route_gates_on_the_host_allowlist() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = empty_state(&temp).await?;
    let app = http::router(
        state,
        &["pond.example.com".to_owned()],
        tokio_util::sync::CancellationToken::new(),
    );

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
        .header("host", "127.0.0.1:9797")
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

/// The periodic allocator trim in `spawn_prewarm` only fires when a request
/// completed since the previous interval, so two properties have to hold or
/// the trim silently stops happening (or starts happening on an idle server,
/// paying for a glibc arena walk every 30 s for nothing): every request path
/// arms the flag, including one that answers with an error, and taking it
/// clears it. Neither is visible in RSS, so nothing else would catch a handler
/// that lost its guard.
#[tokio::test(flavor = "multi_thread")]
async fn completed_requests_arm_the_periodic_allocator_trim() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = empty_state(&temp).await?;
    let app = http::router(
        state.clone(),
        &[],
        tokio_util::sync::CancellationToken::new(),
    );

    assert!(
        !state.take_completed_activity(),
        "a process that has served nothing is idle"
    );

    // Not-found is still work done: the handler allocated to answer it.
    let (status, _, _) = post(
        &app,
        "/v1/get-session",
        &json!({"protocol_version": PROTOCOL_VERSION, "session_id": "absent"}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        state.take_completed_activity(),
        "a completed request arms the next trim"
    );
    assert!(
        !state.take_completed_activity(),
        "taking the flag disarms it: an interval with no request must not trim"
    );

    Ok(())
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

/// `POST /v1/x/sql` answers JSON rows keyed by column name, timestamps as
/// microsecond RFC3339 cursors, with the row cap signalled by `truncated`.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_returns_json_rows() -> anyhow::Result<()> {
    let (_temp, store, app) = router().await?;
    let sessions = store.session_ids().await?.len();
    assert!(sessions > 2, "the cap below must cut the fixture corpus");

    let (status, headers, body) = post(
        &app,
        "/v1/x/sql",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "sql": "SELECT session_id, max(timestamp) AS last_ts, count(*) AS n \
                    FROM messages GROUP BY session_id \
                    ORDER BY last_ts DESC, session_id LIMIT 50",
            "limit": 2,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(headers.contains_key("x-pond-request-id"));
    let SqlEnvelope::Success(response) = serde_json::from_value(body)? else {
        panic!("expected a success envelope");
    };
    assert_eq!(response.columns, ["session_id", "last_ts", "n"]);
    assert_eq!(response.row_count, 2);
    assert_eq!(response.rows.len(), 2);
    assert!(response.truncated, "the row cap cut the result");
    for row in &response.rows {
        assert!(row["session_id"].is_string(), "{row}");
        assert!(row["n"].is_u64(), "counts are JSON numbers: {row}");
        let last_ts = row["last_ts"].as_str().expect("timestamps are strings");
        let (_, fraction) = last_ts.split_once('.').expect("fractional seconds");
        assert!(
            fraction.len() == 7 && fraction.ends_with('Z'),
            "exactly six fractional digits then Z: {last_ts}"
        );
        chrono::DateTime::parse_from_rfc3339(last_ts)?;
    }
    Ok(())
}

/// The route is read-only and every query-shaped failure is a 400
/// `validation_failed` carrying the SQL surface's own recovery text.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_rejects_writes_and_bad_requests() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    let sql = |query: &str| json!({"protocol_version": PROTOCOL_VERSION, "query": query});

    for query in [
        "DELETE FROM messages",
        "CREATE TABLE t (x INT)",
        "SELECT 1; SELECT 2",
        "SELEC 1",
    ] {
        let (status, headers, body) = post(&app, "/v1/x/sql", &sql(query)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert!(headers.contains_key("x-pond-request-id"));
        let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
            panic!("expected an error envelope for {query}");
        };
        assert_eq!(error.error.code, ErrorCode::ValidationFailed, "{query}");
        assert!(!error.error.message.is_empty());
    }

    for limit in [0, pond::sql::MAX_INLINE_ROWS + 1] {
        let mut request = sql("SELECT 1");
        request["limit"] = json!(limit);
        let (status, _, body) = post(&app, "/v1/x/sql", &request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "limit {limit}: {body}");
        let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
            panic!("expected an error envelope for limit {limit}");
        };
        assert_eq!(error.error.code, ErrorCode::ValidationFailed);
    }

    let (status, _, body) = post(
        &app,
        "/v1/x/sql",
        &json!({"protocol_version": 999, "query": "SELECT 1"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::VersionUnsupported);
    Ok(())
}

/// `/v1/x/sql` reads arbitrary corpus rows, so it shares the `/mcp` route's
/// DNS-rebinding defence: loopback and the `--allowed-host` names pass, any
/// other `Host` is refused before the body is even parsed.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_gates_on_the_host_allowlist() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &["pond.example.com".to_owned()],
        tokio_util::sync::CancellationToken::new(),
    );
    let body = json!({"protocol_version": PROTOCOL_VERSION, "query": "SELECT 1"}).to_string();
    let status = |host: &'static str| {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/x/sql")
            .header("host", host)
            .header("content-type", "application/json")
            .body(Body::from(body.clone()))
            .unwrap();
        let app = app.clone();
        async move { app.oneshot(request).await.unwrap().status() }
    };
    for host in [
        "127.0.0.1:9797",
        "localhost:9797",
        "[::1]:9797",
        "pond.example.com",
    ] {
        assert_eq!(status(host).await, StatusCode::OK, "{host}");
    }
    for host in ["attacker.example.com", "attacker.example.com:9797"] {
        assert_eq!(status(host).await, StatusCode::FORBIDDEN, "{host}");
    }
    Ok(())
}

/// The read-only gate runs before any dataset open: a write is refused as a
/// 400 even when the store cannot open the table it names, which a read of
/// that same table proves.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_rejects_writes_before_opening_tables() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    for entry in std::fs::read_dir(temp.path())? {
        let path = entry?.path();
        if path.is_dir() {
            std::fs::remove_dir_all(path)?;
        }
    }
    let sql = |query: &str| json!({"protocol_version": PROTOCOL_VERSION, "query": query});

    let (status, _, body) = post(&app, "/v1/x/sql", &sql("SELECT count(*) FROM messages")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");

    let (status, _, body) = post(&app, "/v1/x/sql", &sql("DELETE FROM messages")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::ValidationFailed);
    Ok(())
}

/// Namespace resolution runs before any dataset open, so a table-free query
/// cannot slip an unknown namespace past it.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_rejects_an_unknown_namespace_before_opening_tables() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    for query in ["SELECT 1", "SELECT count(*) FROM parts"] {
        let (status, _, body) = post(
            &app,
            "/v1/x/sql",
            &json!({"protocol_version": PROTOCOL_VERSION, "namespace": "other", "query": query}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
            panic!("expected an error envelope for {query}");
        };
        assert_eq!(error.error.code, ErrorCode::NamespaceUnknown, "{query}");
    }

    let (status, _, body) = post(
        &app,
        "/v1/x/sql",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "namespace": "local",
            "query": "SELECT 1 AS ready",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "columns": ["ready"],
            "rows": [{"ready": 1}],
            "row_count": 1,
            "truncated": false,
            "elapsed_ms": body["elapsed_ms"],
        }),
    );
    Ok(())
}

/// A malformed body is axum's plain-text rejection, not a pond envelope:
/// clients must send `Content-Type: application/json` and handle both shapes.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_rejects_a_malformed_body_before_the_handler() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    let send = |content_type: Option<&str>, body: &str| {
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/x/sql")
            .header("host", "localhost");
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        app.clone()
            .oneshot(request.body(Body::from(body.to_owned())).unwrap())
    };

    let response = send(Some("application/json"), "{not json").await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!response.headers().contains_key("x-pond-request-id"));

    let valid = json!({"protocol_version": PROTOCOL_VERSION, "query": "SELECT 1"}).to_string();
    let response = send(None, &valid).await?;
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    Ok(())
}

/// A query that outruns `timeout_seconds` is a 400 whose text names the HTTP
/// field to raise.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_maps_a_timeout_to_validation_failed() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    let (status, _, body) = post(
        &app,
        "/v1/x/sql",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "query": "SELECT max(value % 7) FROM generate_series(1, 100000000000)",
            "timeout_seconds": 1,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::ValidationFailed);
    assert!(
        error.error.message.contains("exceeded the 1s limit")
            && error.error.message.contains("timeout_seconds")
            && error.error.message.contains("/v1/x/sql"),
        "{}",
        error.error.message
    );
    Ok(())
}

/// A deterministic encoder fault is a 500 `internal`, never a retryable 503
/// `storage_unavailable` a client would retry forever: arrow-json cannot
/// encode a map with non-string keys.
#[tokio::test(flavor = "multi_thread")]
async fn sql_route_maps_an_encoder_failure_to_internal() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let app = http::router(
        empty_state(&temp).await?,
        &[],
        tokio_util::sync::CancellationToken::new(),
    );
    let (status, _, body) = post(
        &app,
        "/v1/x/sql",
        &json!({
            "protocol_version": PROTOCOL_VERSION,
            "query": "SELECT map([1, 2], ['a', 'b']) AS m",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    let SqlEnvelope::Error(error) = serde_json::from_value(body)? else {
        panic!("expected an error envelope");
    };
    assert_eq!(error.error.code, ErrorCode::Internal);
    Ok(())
}

/// `--socket` serves the same router over a Unix socket: a supervisor's first
/// successful connect gets a real answer, and a clean stop removes the socket
/// file so the path is free for the next run.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn unix_socket_serves_sql_and_is_removed_on_shutdown() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let state = empty_state(&temp).await?;
    let sockets = TempDir::new()?;
    let path = sockets.path().join("pond.sock");
    let mut claim = http::SocketClaim::acquire(&path)?;
    let listener = claim.bind()?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        http::serve_unix(listener, claim, state, &[], async move {
            let _ = stopped.await;
        })
        .await
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut stream = loop {
        if let Ok(stream) = tokio::net::UnixStream::connect(&path).await {
            break stream;
        }
        assert!(
            Instant::now() < deadline,
            "socket never accepted a connection"
        );
        assert!(!server.is_finished(), "serve exited before accepting");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let body =
        json!({"protocol_version": PROTOCOL_VERSION, "query": "SELECT 1 AS ready"}).to_string();
    let head = request(
        &mut stream,
        &format!(
            "POST /v1/x/sql HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await?;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    drop(stream);

    stop.send(()).expect("server still running");
    tokio::time::timeout(Duration::from_secs(30), server).await???;
    assert!(!path.exists(), "shutdown removes the socket file");
    Ok(())
}

/// The binary end to end, as a supervisor runs it: `--socket` wins over an
/// inherited POND_HOST/POND_PORT instead of failing the parse as a conflict,
/// the first successful connect is answered, a second server on the same path
/// is refused, and SIGTERM removes the socket.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn pond_serve_socket_ignores_tcp_env_and_cleans_up_on_sigterm() -> anyhow::Result<()> {
    let temp = TempDir::new()?;
    let path = temp.path().join("pond.sock");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home)?;
    let serve = || {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_pond"));
        command
            .arg("serve")
            .arg("--storage-path")
            .arg(temp.path().join("store"))
            .arg("--socket")
            .arg(&path)
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", temp.path().join("config"))
            .env("XDG_DATA_HOME", temp.path().join("data"))
            .env("XDG_CACHE_HOME", temp.path().join("cache"))
            .env("XDG_STATE_HOME", temp.path().join("state"))
            .env("POND_HOST", "0.0.0.0")
            .env("POND_PORT", "1")
            .env_remove("POND_CONFIG_FILE")
            .env("NO_COLOR", "1");
        command
    };
    let mut child = serve()
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut stream = loop {
        if let Ok(stream) = tokio::net::UnixStream::connect(&path).await {
            break stream;
        }
        if let Some(status) = child.try_wait()? {
            panic!("pond serve exited before accepting: {status}");
        }
        if Instant::now() >= deadline {
            child.kill()?;
            panic!("socket never accepted a connection");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let body =
        json!({"protocol_version": PROTOCOL_VERSION, "query": "SELECT 1 AS ready"}).to_string();
    let head = request(
        &mut stream,
        &format!(
            "POST /v1/x/sql HTTP/1.1\r\nHost: localhost\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
    .await?;
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    drop(stream);

    let second = serve().output()?;
    assert!(!second.status.success(), "{:?}", second.status);
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("another pond serve owns"), "{stderr}");
    tokio::net::UnixStream::connect(&path)
        .await
        .expect("the refused server leaves the live socket alone");

    let signalled = std::process::Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    assert!(signalled.success());
    let output = tokio::task::spawn_blocking(move || child.wait_with_output()).await??;
    assert!(output.status.success(), "{:?}", output.status);
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("serve: http listening on unix:{}\n", path.display())
    );
    assert!(!path.exists(), "SIGTERM removes the socket file");
    Ok(())
}
