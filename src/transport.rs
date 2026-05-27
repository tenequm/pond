//! The HTTP+JSON and stdio-MCP transports: thin adapters over the shared wire
//! handlers. Both transports dispatch to the same handler functions - no
//! per-transport behavior divergence.
//!
//! HTTP exposes `POST /v1/search`, `POST /v1/get`, `POST /v1/ingest`, and the
//! SSE `GET /v1/sessions/{id}/events` stream. MCP exposes only `pond_search` /
//! `pond_get` (the kb-parity surface); ingest stays HTTP-only and CLI-only.

use std::sync::Arc;

use crate::{config::SearchConfig, embed::LazyEmbedder, sessions::Store};

/// Shared state handed to both transports. `embedder` holds a lazy handle:
/// the model isn't loaded until the first hybrid search asks for it, so
/// `pond mcp` idles at ~50 MB resident and only pays the ~600 MB load cost on
/// the first query that needs it (spec.md#search opt-in).
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub embedder: Arc<LazyEmbedder>,
    pub search: SearchConfig,
}

pub mod http {
    //! axum HTTP+JSON server: `POST /v1/search`, `POST /v1/get`, and the `/mcp`
    //! route carrying rmcp's streamable-HTTP MCP transport.

    use std::net::{IpAddr, SocketAddr};

    use std::{convert::Infallible, time::Duration};

    use anyhow::Context;
    use axum::{
        Json, Router,
        extract::{DefaultBodyLimit, Path, Query, State},
        http::StatusCode,
        response::{
            IntoResponse, Response,
            sse::{Event, KeepAlive, Sse},
        },
        routing::{get as get_route, post},
    };
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };
    use serde::Deserialize;
    use tokio::net::TcpListener;

    use super::AppState;
    use crate::{
        handlers::{
            parse_since, pond_get, pond_ingest, pond_search, pond_session_events, resolve_namespace,
        },
        wire::{
            ErrorCode, ErrorEnvelope, GetEnvelope, GetRequest, IngestEnvelope, IngestRequest,
            SearchEnvelope, SearchRequest, default_namespace,
        },
    };

    /// HTTP body cap for `POST /v1/*` JSON handlers (spec.md#protocol): 8 MB.
    /// Replaces axum's 2 MB default - that default is more restrictive than the
    /// design's intent and would surface oversize ingests as a generic 413
    /// instead of pond's typed `validation_failed`.
    pub const HTTP_BODY_LIMIT_BYTES: usize = 8 * 1024 * 1024;

    /// Build the axum router: the `/v1/*` JSON handlers plus the nested `/mcp`
    /// streamable-HTTP MCP service. Public so the integration test can drive it
    /// without binding a socket.
    pub fn router(state: AppState) -> Router {
        let mcp_state = state.clone();
        let mcp = StreamableHttpService::new(
            move || Ok(super::mcp::PondMcp::new(mcp_state.clone())),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default(),
        );
        Router::new()
            .route("/v1/search", post(search))
            .route("/v1/get", post(get))
            .route("/v1/ingest", post(ingest))
            .route(
                "/v1/sessions/{session_id}/events",
                get_route(session_events),
            )
            .layer(DefaultBodyLimit::max(HTTP_BODY_LIMIT_BYTES))
            .with_state(state)
            .nest_service("/mcp", mcp)
    }

    /// Bind and serve until ctrl-c. `--port 0` selects an OS-assigned free port;
    /// an unspecified host (`0.0.0.0` / `::`) logs a security notice because the
    /// personal pond is single-user and LAN exposure is opt-in (spec.md#scope).
    pub async fn serve(state: AppState, host: String, port: u16) -> anyhow::Result<()> {
        let ip: IpAddr = host
            .parse()
            .with_context(|| format!("invalid --host {host:?}"))?;
        if ip.is_unspecified() {
            tracing::warn!(
                %host,
                "binding to an unspecified address exposes pond on the LAN; \
                 the personal pond is single-user"
            );
        }
        let listener = TcpListener::bind(SocketAddr::new(ip, port))
            .await
            .with_context(|| format!("failed to bind {host}:{port}"))?;
        let local = listener
            .local_addr()
            .context("failed to read bound address")?;
        tracing::info!(%local, "pond serve listening (HTTP /v1/*, MCP /mcp)");
        axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("axum server error")
    }

    async fn shutdown_signal() {
        let _ = tokio::signal::ctrl_c().await;
    }

    async fn search(
        State(state): State<AppState>,
        Json(mut request): Json<SearchRequest>,
    ) -> (StatusCode, Json<SearchEnvelope>) {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_search(&state.store, &state.embedder, request, &state.search).await;
        let status = match &envelope {
            SearchEnvelope::Success(_) => StatusCode::OK,
            SearchEnvelope::Error(error) => status_for(&error.error.code),
        };
        (status, Json(envelope))
    }

    async fn get(
        State(state): State<AppState>,
        Json(mut request): Json<GetRequest>,
    ) -> (StatusCode, Json<GetEnvelope>) {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_get(&state.store, request).await;
        let status = match &envelope {
            GetEnvelope::Success(_) => StatusCode::OK,
            GetEnvelope::Error(error) => status_for(&error.error.code),
        };
        (status, Json(envelope))
    }

    async fn ingest(
        State(state): State<AppState>,
        Json(mut request): Json<IngestRequest>,
    ) -> (StatusCode, Json<IngestEnvelope>) {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_ingest(&state.store, request).await;
        // Per-row errors in `results[]` are not request-level failures, so
        // the envelope success path always returns 200; only transport-level
        // failures (validation_failed, namespace_unknown, etc.) map to 4xx/5xx.
        let status = match &envelope {
            IngestEnvelope::Success(_) => StatusCode::OK,
            IngestEnvelope::Error(error) => status_for(&error.error.code),
        };
        (status, Json(envelope))
    }

    /// Query string for `GET /v1/sessions/{session_id}/events`
    /// (spec.md#protocol). `since` resumes after a prior event id; the
    /// `Last-Event-ID` HTTP header is honored as a fallback for EventSource
    /// auto-reconnect.
    #[derive(Debug, Deserialize)]
    struct SessionEventsQuery {
        #[serde(default)]
        since: Option<String>,
        #[serde(default)]
        namespace: Option<String>,
    }

    /// `GET /v1/sessions/{session_id}/events` SSE handler. Catch-up only in
    /// v1: emits canonical message stubs strictly after `since` in
    /// `(timestamp, message_id)` order, then `end`. SSE keepalive every 15s.
    async fn session_events(
        State(state): State<AppState>,
        Path(session_id): Path<String>,
        headers: axum::http::HeaderMap,
        Query(params): Query<SessionEventsQuery>,
    ) -> Response {
        if let Err(envelope) = resolve_namespace(params.namespace.as_deref()) {
            let code = envelope.error.code.clone();
            return error_response(code, envelope);
        }

        let since_raw = params.since.clone().or_else(|| {
            headers
                .get("last-event-id")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned)
        });
        let since = parse_since(since_raw.as_deref());

        match pond_session_events(&state.store, &session_id, since).await {
            Ok(events) => {
                let stream = tokio_stream::iter(events.into_iter().map(|sse| {
                    let payload = sse.data.to_string();
                    Ok::<_, Infallible>(Event::default().event(sse.event).id(sse.id).data(payload))
                }));
                Sse::new(stream)
                    .keep_alive(
                        KeepAlive::new()
                            .interval(Duration::from_secs(15))
                            .text("keepalive"),
                    )
                    .into_response()
            }
            Err(envelope) => error_response(envelope.error.code.clone(), envelope),
        }
    }

    fn error_response(code: ErrorCode, envelope: ErrorEnvelope) -> Response {
        (status_for(&code), Json(envelope)).into_response()
    }

    /// Map a wire error code to an HTTP status. The envelope body still carries
    /// the full typed error; the status is the coarse signal.
    fn status_for(code: &ErrorCode) -> StatusCode {
        match code {
            ErrorCode::ValidationFailed
            | ErrorCode::VersionUnsupported
            | ErrorCode::NamespaceUnknown => StatusCode::BAD_REQUEST,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

pub mod mcp {
    //! The rmcp MCP layer: `pond_search` / `pond_get` tools and `schema://pond`
    //! / `stats://pond` resources, transport-agnostic. Mounted on stdio (via
    //! `pond mcp`) and on the `/mcp` HTTP route (via `pond serve`).

    use anyhow::Context;
    use rmcp::{
        ErrorData, RoleServer, ServerHandler, ServiceExt,
        handler::server::{router::tool::ToolRouter, wrapper::Parameters},
        model::{
            AnnotateAble, CallToolResult, Content, ErrorCode as JsonRpcErrorCode,
            ListResourcesResult, PaginatedRequestParams, RawResource, ReadResourceRequestParams,
            ReadResourceResult, ResourceContents, ServerCapabilities, ServerInfo,
        },
        schemars,
        service::RequestContext,
        tool, tool_handler, tool_router,
        transport::stdio,
    };
    use serde::Deserialize;

    use super::AppState;
    use crate::{
        PROTOCOL_VERSION,
        handlers::pond_get as run_get,
        handlers::pond_search as run_search,
        wire::{
            ErrorCode as WireErrorCode, ErrorEnvelope, GetEnvelope, GetRequest, ProjectFilter,
            SearchEnvelope, SearchFilters, SearchRequest, default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource. Detail
    /// agents load on demand; the per-tool descriptions below stay tight.
    const SCHEMA_DOC: &str = "\
pond_search filters: query (semantic - concepts, not project names), limit \
(default 10, max 200), project (path substring), conversation_id (exact \
session match), source_agent, role (user|assistant), from_date / to_date \
(YYYY-MM-DD), boost_recent (default true), group_by_conversation (default \
true; grouped hits carry `best_hit_message_id` for drill-in via `pond_get`).

pond_search response: each hit carries `score` normalized to [0.0, 1.0] \
(comparable within one response). Grouped hits surface `first_timestamp` and, \
when matches span more than one timestamp, `last_timestamp`. The hit `text` \
is a ~600-char window of the indexed message body centered on the query \
term; fetch the full body via `pond_get(message_id)`.

pond_search multilingual: pond's embedder (multilingual-e5-small) is trained \
for cross-lingual retrieval, so a query in language A can match indexed text \
in language B via the vector arm. The FTS arm is character-ngram-based and \
only matches surface tokens, so for cross-lingual queries expect most signal \
to come from the vector arm.

pond_get: message_id (one message + context_depth messages of thread context \
each side) OR conversation_id (full session; up_to truncates, max_messages \
caps at 1000). The default response carries `messages[].text` only; pass \
`include_parts=true` to also receive the per-message parts (reasoning, tool \
calls, tool results, files). Responses are paginated with a `~10000-token \
budget; when `has_more` is true the response carries an opaque `next_cursor` \
- pass it back as `cursor` to fetch the next page (originating session_id / \
message_id / include_parts are re-supplied automatically).";

    /// `pond_search` MCP tool parameters. Field names follow the kb parity
    /// contract: `conversation_id` here maps to the wire `session_id` filter.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpSearchParams {
        /// What to search for: concepts and keywords. Keep it semantic - do
        /// not put project names in the query, use the `project` filter
        /// instead. Optional only when `similar_to` is set (vector-only mode
        /// uses the stored vector and ignores the query text); required in
        /// every other call.
        #[serde(default)]
        query: Option<String>,
        /// Max hits to return. Default 10, server-capped at 200.
        #[serde(default)]
        limit: Option<usize>,
        /// Filter to projects whose path contains this substring.
        #[serde(default)]
        project: Option<String>,
        /// Filter to one session (exact match).
        #[serde(default)]
        conversation_id: Option<String>,
        /// Filter to one source agent (e.g. "claude-code").
        #[serde(default)]
        source_agent: Option<String>,
        /// Filter by message role: "user" or "assistant".
        #[serde(default)]
        role: Option<String>,
        /// Only messages on or after this date (YYYY-MM-DD).
        #[serde(default)]
        from_date: Option<String>,
        /// Only messages on or before this date (YYYY-MM-DD).
        #[serde(default)]
        to_date: Option<String>,
        /// Boost recent messages in ranking. Default true.
        #[serde(default)]
        boost_recent: Option<bool>,
        /// "Find similar messages to this one." When set, pond uses the
        /// stored vector for `similar_to` as the kNN query and ignores the
        /// `query` text; vector-only, no embedder load. Compose with
        /// `pond_search` -> read top hit -> `pond_search(similar_to=<that
        /// message_id>)` to explore neighbors of any returned hit.
        #[serde(default)]
        similar_to: Option<String>,
        /// Collapse hits to one summary per session. Default true: most
        /// searches are "find the conversation about X" and message-level
        /// duplicates from one session corrupt the corpus picture. Pass
        /// `false` to get individual message hits (composes with
        /// `pond_get(message_id, context_depth)`). When grouped, each group
        /// carries `best_hit_message_id` for drill-in.
        #[serde(default)]
        group_by_conversation: Option<bool>,
    }

    /// `pond_get` MCP tool parameters. `conversation_id` maps to the wire
    /// `session_id`; one of `message_id` / `conversation_id` / `cursor`
    /// is required.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpGetParams {
        /// Retrieve this message plus surrounding thread context.
        #[serde(default)]
        message_id: Option<String>,
        /// Retrieve this whole session (mutually exclusive with message_id).
        #[serde(default)]
        conversation_id: Option<String>,
        /// With conversation_id: truncate the session at and including this
        /// message id (restore-to-a-point).
        #[serde(default)]
        up_to: Option<String>,
        /// With message_id: messages of thread context to include on each side.
        #[serde(default)]
        context_depth: Option<usize>,
        /// Cap on returned messages. Default 100, max 1000.
        #[serde(default)]
        max_messages: Option<usize>,
        /// When true, the response also carries each message's parts
        /// (reasoning, tool calls, tool results, files). Default false: the
        /// response carries only `messages[].text` to fit the agent-context
        /// budget; pass true when full parts are needed for restore-style flows.
        #[serde(default)]
        include_parts: Option<bool>,
        /// Opaque continuation token from a prior response's `next_cursor`.
        /// When set, the originating `conversation_id` / `message_id` /
        /// `include_parts` are re-supplied from the cursor automatically.
        #[serde(default)]
        cursor: Option<String>,
    }

    /// The pond MCP server: holds the shared state and the generated tool router.
    #[derive(Clone)]
    pub struct PondMcp {
        state: AppState,
        tool_router: ToolRouter<PondMcp>,
    }

    #[tool_router]
    impl PondMcp {
        pub fn new(state: AppState) -> Self {
            Self {
                state,
                tool_router: Self::tool_router(),
            }
        }

        #[tool(
            description = "Hybrid (vector + BM25) search over stored conversation history. \
                           Returns ranked message hits with `score` normalized to [0, 1]. \
                           Keep `query` semantic; use `project` / `conversation_id` filters \
                           for scope. See `schema://pond` for the full schema and \
                           multilingual notes."
        )]
        async fn pond_search(
            &self,
            Parameters(params): Parameters<McpSearchParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                query: params.query.unwrap_or_default(),
                filters: SearchFilters {
                    project: params.project.map(ProjectFilter::Contains),
                    session_id: params.conversation_id,
                    source_agent: params.source_agent,
                    from_date: params.from_date,
                    to_date: params.to_date,
                    role: params.role,
                    // min_score is intentionally not on the MCP surface; scores
                    // are response-relative, so a server-side threshold is a
                    // footgun for agent callers. CLI / HTTP still exposes it
                    // for the bench harness.
                    min_score: 0.0,
                },
                boost_recent: params.boost_recent.unwrap_or(true),
                group_by_conversation: params.group_by_conversation.unwrap_or(true),
                limit: params.limit.unwrap_or(10),
                mode_override: None,
                similar_to: params.similar_to,
            };
            match run_search(
                &self.state.store,
                &self.state.embedder,
                request,
                &self.state.search,
            )
            .await
            {
                SearchEnvelope::Success(response) => json_result(&response),
                SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Retrieve stored conversation content. With `message_id`: that \
                           message plus `context_depth` messages of thread context each side. \
                           With `conversation_id`: the full session. Default response carries \
                           `messages[].text` only; pass `include_parts=true` to also receive \
                           parts (reasoning, tool calls, tool results, files). Responses are \
                           bounded by a ~10000-token budget - when `has_more` is true, pass \
                           `next_cursor` back as `cursor` to fetch the next page."
        )]
        async fn pond_get(
            &self,
            Parameters(params): Parameters<McpGetParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let request = GetRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                session_id: params.conversation_id,
                message_id: params.message_id,
                up_to: params.up_to,
                context_depth: params.context_depth.unwrap_or(0),
                max_messages: params.max_messages.unwrap_or(100),
                include_parts: params.include_parts.unwrap_or(false),
                cursor: params.cursor,
            };
            match run_get(&self.state.store, request).await {
                GetEnvelope::Success(response) => json_result(&response),
                GetEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }
    }

    // `router = self.tool_router` makes the generated `call_tool` / `list_tools`
    // read the cached router field; the bare-`#[tool_handler]` default rebuilds
    // the router via `Self::tool_router()` on every call instead.
    #[tool_handler(router = self.tool_router)]
    impl ServerHandler for PondMcp {
        fn get_info(&self) -> ServerInfo {
            ServerInfo::new(
                ServerCapabilities::builder()
                    .enable_tools()
                    .enable_resources()
                    .build(),
            )
            .with_instructions(
                "pond: session storage and retrieval. Tools: pond_search (hybrid search \
                 over conversation history), pond_get (retrieve a message with thread \
                 context, or a full session). Resources: schema://pond, stats://pond.",
            )
        }

        async fn list_resources(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListResourcesResult, ErrorData> {
            Ok(ListResourcesResult {
                resources: vec![
                    RawResource::new("schema://pond", "pond search schema").no_annotation(),
                    RawResource::new("stats://pond", "pond corpus stats").no_annotation(),
                ],
                next_cursor: None,
                meta: None,
            })
        }

        async fn read_resource(
            &self,
            request: ReadResourceRequestParams,
            _context: RequestContext<RoleServer>,
        ) -> Result<ReadResourceResult, ErrorData> {
            match request.uri.as_str() {
                "schema://pond" => Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    SCHEMA_DOC,
                    request.uri,
                )])),
                "stats://pond" => {
                    let store = &self.state.store;
                    let map_err = |error: anyhow::Error| {
                        ErrorData::internal_error(format!("stats unavailable: {error}"), None)
                    };
                    let (sessions, messages, parts) = store.row_counts().await.map_err(&map_err)?;
                    let embedding = store.embedding_progress().await.map_err(&map_err)?;
                    let stale = store.stale_embedding_count().await.map_err(&map_err)?;
                    let indices = store.index_status().await.map_err(&map_err)?;

                    let embedded_percent = if embedding.total == 0 {
                        0.0
                    } else {
                        #[allow(clippy::cast_precision_loss)]
                        let pct = (embedding.embedded as f64 / embedding.total as f64) * 100.0;
                        (pct * 10.0).round() / 10.0
                    };
                    let index_rows = indices
                        .iter()
                        .map(|status| {
                            serde_json::json!({
                                "table": status.table.as_str(),
                                "intent": status.intent_name,
                                "exists": status.exists,
                                "fragments_covered": status.fragments_covered,
                                "unindexed_rows": status.unindexed_rows,
                            })
                        })
                        .collect::<Vec<_>>();

                    // spec.md#search: `search_text` is the conversational text
                    // (filtered of harness-injected parts at the adapter seam).
                    // `embedding.total` is the searchable population - that is
                    // the right denominator for "% embedded", not total messages.
                    let stats = serde_json::json!({
                        "corpus": {
                            "sessions": sessions,
                            "messages": messages,
                            "searchable_messages": embedding.total,
                            "parts": parts,
                        },
                        "embeddings": {
                            "model": embedding.model,
                            "embedded": embedding.embedded,
                            "searchable_total": embedding.total,
                            "embedded_percent": embedded_percent,
                            "stale_under_other_model": stale,
                        },
                        "indices": index_rows,
                    });
                    Ok(ReadResourceResult::new(vec![ResourceContents::text(
                        stats.to_string(),
                        request.uri,
                    )]))
                }
                other => Err(ErrorData::resource_not_found(
                    format!("unknown resource: {other}"),
                    None,
                )),
            }
        }
    }

    /// Run the stdio MCP server until the client disconnects. All diagnostics
    /// go to stderr (the shared `tracing` subscriber); stdout carries only
    /// JSON-RPC frames, written by rmcp's stdio transport (spec.md#scope).
    pub async fn serve_stdio(state: AppState) -> anyhow::Result<()> {
        let service = PondMcp::new(state)
            .serve(stdio())
            .await
            .context("failed to start stdio MCP server")?;
        service.waiting().await.context("stdio MCP server error")?;
        Ok(())
    }

    /// Serialize a wire response into an MCP tool result (one JSON text block).
    fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
        let text = serde_json::to_string(value).map_err(|error| {
            ErrorData::internal_error(format!("failed to serialize response: {error}"), None)
        })?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Map a wire error envelope to a JSON-RPC error. rmcp ships no app-level
    /// codes, so pond defines its own `-32000`-family set here. The `data`
    /// payload carries pond's canonical string code and a `retryable` flag
    /// (per spec.md#error-model) so MCP callers can branch on retry semantics
    /// without parsing message strings or knowing the JSON-RPC code mapping.
    fn to_error_data(envelope: &ErrorEnvelope) -> ErrorData {
        let (jsonrpc_code, pond_code, retryable) = match envelope.error.code {
            WireErrorCode::ValidationFailed => (-32010, "validation_failed", false),
            WireErrorCode::VersionUnsupported => (-32011, "version_unsupported", false),
            WireErrorCode::NotFound => (-32012, "not_found", false),
            WireErrorCode::NamespaceUnknown => (-32013, "namespace_unknown", false),
            WireErrorCode::StorageUnavailable => (-32014, "storage_unavailable", true),
            WireErrorCode::Conflict => (-32015, "conflict", true),
            WireErrorCode::Internal => (-32016, "internal", false),
        };
        let mut data = match &envelope.error.details {
            serde_json::Value::Object(map) => map.clone(),
            _ => serde_json::Map::new(),
        };
        data.insert("pond_code".to_owned(), serde_json::json!(pond_code));
        data.insert("retryable".to_owned(), serde_json::json!(retryable));
        data.insert(
            "request_id".to_owned(),
            serde_json::json!(envelope.request_id),
        );
        ErrorData::new(
            JsonRpcErrorCode(jsonrpc_code),
            envelope.error.message.clone(),
            Some(serde_json::Value::Object(data)),
        )
    }
}
