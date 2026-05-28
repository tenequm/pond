//! The HTTP+JSON and stdio-MCP transports: thin adapters over the shared wire
//! handlers. Both transports dispatch to the same handler functions - no
//! per-transport behavior divergence.
//!
//! HTTP exposes `POST /v1/search`, `POST /v1/get`, and `POST /v1/ingest`. MCP
//! exposes only `pond_search` / `pond_get` (the kb-parity surface); ingest
//! stays HTTP-only and CLI-only.

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

    use anyhow::Context;
    use axum::{
        Json, Router,
        extract::{DefaultBodyLimit, State},
        http::{HeaderValue, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };
    use tokio::net::TcpListener;

    use super::AppState;
    use crate::{
        handlers::{pond_get, pond_ingest, pond_search},
        wire::{
            ErrorCode, GetEnvelope, GetRequest, IngestEnvelope, IngestRequest, SearchEnvelope,
            SearchRequest, default_namespace, new_request_id,
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
    ) -> Response {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_search(&state.store, &state.embedder, request, &state.search).await;
        let status = match &envelope {
            SearchEnvelope::Success(_) => StatusCode::OK,
            SearchEnvelope::Error(error) => status_for(&error.error.code),
        };
        with_request_id((status, Json(envelope)).into_response())
    }

    async fn get(State(state): State<AppState>, Json(mut request): Json<GetRequest>) -> Response {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_get(&state.store, request).await;
        let status = match &envelope {
            GetEnvelope::Success(_) => StatusCode::OK,
            GetEnvelope::Error(error) => status_for(&error.error.code),
        };
        with_request_id((status, Json(envelope)).into_response())
    }

    async fn ingest(
        State(state): State<AppState>,
        Json(mut request): Json<IngestRequest>,
    ) -> Response {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_ingest(&state.store, request).await;
        // Per-row errors in `results[]` are not request-level failures, so
        // the envelope success path always returns 200; only transport-level
        // failures (validation_failed, namespace_unknown, etc.) map to 4xx/5xx.
        let status = match &envelope {
            IngestEnvelope::Success(_) => StatusCode::OK,
            IngestEnvelope::Error(error) => status_for(&error.error.code),
        };
        with_request_id((status, Json(envelope)).into_response())
    }

    fn with_request_id(mut response: Response) -> Response {
        if let Ok(value) = HeaderValue::from_str(&new_request_id()) {
            response.headers_mut().insert("x-pond-request-id", value);
        }
        response
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
            ListResourcesResult, ListToolsResult, Meta, PaginatedRequestParams, RawResource,
            ReadResourceRequestParams, ReadResourceResult, ResourceContents, ServerCapabilities,
            ServerInfo,
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
            ResponseMode, SearchEnvelope, SearchFilters, SearchRequest, default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource. Detail
    /// agents load on demand; the per-tool descriptions below stay tight.
    const SCHEMA_DOC: &str = "\
pond_search filters: query (semantic - concepts, not project names), limit \
(default 10, max 200), project (path substring), session_id (exact session \
match), source_agent, role (user|assistant|system|tool), from_date / to_date \
(YYYY-MM-DD), cursor (opaque continuation token).

pond_search response: always grouped by session. `matched_total` is the \
message count before `limit` and byte-budget truncation. Each session carries \
up to 3 top-scoring `matches`, sorted score-desc; each match has `score` \
normalized to [0.0, 1.0] within one response and `text` as a ~600-char indexed \
body window. When `has_more` is true, pass `next_cursor` back as `cursor`; \
rank may shift between cursor pages if the corpus changes.

pond_search multilingual: pond's embedder (multilingual-e5-small) is trained \
for cross-lingual retrieval, so a query in language A can match indexed text \
in language B via the vector arm. The FTS arm is character-ngram-based and \
only matches surface tokens, so for cross-lingual queries expect most signal \
to come from the vector arm.

pond_get: message_id (the target message's full parts, paginated, plus \
context_depth sibling messages each side) OR session_id (the whole session). \
Session mode takes response_mode: \"conversational\" (default - human/model \
text with compact parts_summary per message), \"complete\" (all messages \
including system/tool carriers, with parts_summary), or \"verbatim\" (all \
messages with full parts inline; heaviest). limit defaults to 20, caps at \
1000. Every per-message view carries parts_summary (kind + short label); \
verbatim adds full parts. Responses are bounded by a size budget: when \
messages_remaining > 0 (session mode) or target_parts_remaining > 0 (message \
mode), pass the last returned id back as after_id to fetch the next page. Not \
for bulk export - use `pond export`.";

    /// `pond_search` MCP tool parameters.
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
        session_id: Option<String>,
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
        /// "Find similar messages to this one." When set, pond uses the
        /// stored vector for `similar_to` as the kNN query and ignores the
        /// `query` text; vector-only, no embedder load. Compose with
        /// `pond_search` -> read top hit -> `pond_search(similar_to=<that
        /// message_id>)` to explore neighbors of any returned hit.
        #[serde(default)]
        similar_to: Option<String>,
        /// Opaque continuation token from a prior response's `next_cursor`.
        #[serde(default)]
        cursor: Option<String>,
    }

    /// `pond_get` MCP tool parameters. Exactly one of `message_id` /
    /// `session_id` is required.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpGetParams {
        /// Retrieve this message: its full parts plus `context_depth` sibling
        /// messages each side. `response_mode` is ignored in this mode.
        #[serde(default)]
        message_id: Option<String>,
        /// Retrieve this whole session (mutually exclusive with message_id).
        #[serde(default)]
        session_id: Option<String>,
        /// With message_id: messages of thread context to include on each side.
        #[serde(default)]
        context_depth: Option<usize>,
        /// Cap on returned messages (session mode) or parts (message mode).
        /// Default 20, max 1000.
        #[serde(default)]
        limit: Option<usize>,
        /// Session-mode depth: "conversational" (default; human/model text
        /// only, with part summaries), "complete" (all messages incl. carriers,
        /// with part summaries), or "verbatim" (all messages with full parts
        /// inline). Ignored in message mode.
        #[serde(default)]
        response_mode: Option<String>,
        /// Exclusive continuation anchor from a prior response: the last
        /// `message_id` (session mode) or last `part_id` (message mode).
        #[serde(default)]
        after_id: Option<String>,
    }

    fn parse_response_mode(value: Option<String>) -> ResponseMode {
        match value.as_deref() {
            Some("complete") => ResponseMode::Complete,
            Some("verbatim") => ResponseMode::Verbatim,
            // None or any other value falls back to the conversational default.
            _ => ResponseMode::Conversational,
        }
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
                           Returns sessions with up to 3 top matches each and cursor \
                           pagination when the response reaches the server byte budget. \
                           Keep `query` semantic; use `project` / `session_id` filters \
                           for scope. See `schema://pond` for the full schema."
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
                    session_id: params.session_id,
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
                limit: params.limit.unwrap_or(10),
                cursor: params.cursor,
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
                           message's full parts (paginated) plus `context_depth` sibling \
                           messages each side. With `session_id`: the session at one of three \
                           `response_mode`s - \"conversational\" (default; text + part \
                           summaries), \"complete\" (all messages + part summaries), or \
                           \"verbatim\" (all messages with full parts inline). Responses are \
                           bounded by a size budget; when `messages_remaining` (or \
                           `target_parts_remaining`) is > 0, pass the last id back as `after_id` \
                           to page on. Not for bulk export - use `pond export`."
        )]
        async fn pond_get(
            &self,
            Parameters(params): Parameters<McpGetParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let request = GetRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                session_id: params.session_id,
                message_id: params.message_id,
                context_depth: params.context_depth.unwrap_or(0),
                limit: params.limit.unwrap_or(20),
                response_mode: parse_response_mode(params.response_mode),
                after_id: params.after_id,
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

        async fn list_tools(
            &self,
            request: Option<PaginatedRequestParams>,
            context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            let _ = (request, context);
            let mut result = ListToolsResult {
                tools: self.tool_router.list_all(),
                next_cursor: None,
                meta: None,
            };
            annotate_tool_limits(&mut result);
            Ok(result)
        }
    }

    fn annotate_tool_limits(result: &mut ListToolsResult) {
        for tool in &mut result.tools {
            let chars = match tool.name.as_ref() {
                "pond_search" => 80_000,
                "pond_get" => 200_000,
                _ => continue,
            };
            let mut meta = serde_json::Map::new();
            meta.insert(
                "anthropic/maxResultSizeChars".to_owned(),
                serde_json::json!(chars),
            );
            tool.meta = Some(Meta(meta));
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
        ErrorData::new(
            JsonRpcErrorCode(jsonrpc_code),
            envelope.error.message.clone(),
            Some(serde_json::Value::Object(data)),
        )
    }

    #[cfg(test)]
    mod tests {
        #![allow(clippy::expect_used, clippy::unwrap_used)]

        use std::sync::Arc;

        use rmcp::model::{ErrorCode as JsonRpcErrorCode, Tool};

        use super::*;
        use crate::wire::{ErrorBody, ErrorCode, Role, SearchResponse, SearchResult};

        #[test]
        fn error_data_carries_code_and_retryability() {
            let cases = [
                (
                    ErrorCode::ValidationFailed,
                    -32010,
                    "validation_failed",
                    false,
                ),
                (
                    ErrorCode::VersionUnsupported,
                    -32011,
                    "version_unsupported",
                    false,
                ),
                (ErrorCode::NotFound, -32012, "not_found", false),
                (
                    ErrorCode::NamespaceUnknown,
                    -32013,
                    "namespace_unknown",
                    false,
                ),
                (
                    ErrorCode::StorageUnavailable,
                    -32014,
                    "storage_unavailable",
                    true,
                ),
                (ErrorCode::Conflict, -32015, "conflict", true),
                (ErrorCode::Internal, -32016, "internal", false),
            ];
            for (code, jsonrpc, pond_code, retryable) in cases {
                let error = to_error_data(&ErrorEnvelope {
                    error: ErrorBody {
                        code,
                        message: "boom".to_owned(),
                        details: serde_json::json!({"detail": 1}),
                    },
                });
                assert_eq!(error.code, JsonRpcErrorCode(jsonrpc));
                let data = error.data.unwrap();
                assert_eq!(data["detail"], serde_json::json!(1));
                assert_eq!(data["pond_code"], serde_json::json!(pond_code));
                assert_eq!(data["retryable"], serde_json::json!(retryable));
                assert!(
                    data.get("request_id").is_none(),
                    "MCP errors use JSON-RPC ids for correlation"
                );
            }
        }

        #[test]
        fn annotate_tool_limits_sets_anthropic_meta() {
            let schema = Arc::new(serde_json::Map::new());
            let mut result = ListToolsResult {
                tools: vec![
                    Tool::new("pond_search", "Search", Arc::clone(&schema)),
                    Tool::new("pond_get", "Get", Arc::clone(&schema)),
                ],
                next_cursor: None,
                meta: None,
            };
            annotate_tool_limits(&mut result);
            let value = |name: &str| {
                result
                    .tools
                    .iter()
                    .find(|tool| tool.name == name)
                    .and_then(|tool| tool.meta.as_ref())
                    .and_then(|meta| meta.0.get("anthropic/maxResultSizeChars"))
                    .and_then(serde_json::Value::as_i64)
            };
            assert_eq!(value("pond_search"), Some(80_000));
            assert_eq!(value("pond_get"), Some(200_000));
        }

        #[test]
        fn json_result_serializes_search_response_without_request_id() {
            let response = SearchResponse {
                sessions: vec![crate::wire::SearchSession {
                    session_id: "s1".to_owned(),
                    project: "pond".to_owned(),
                    source_agent: "claude-code".to_owned(),
                    session_messages_count: 2,
                    matched_message_count: 1,
                    matches: vec![SearchResult {
                        message_id: "m1".to_owned(),
                        role: Role::User,
                        timestamp: chrono::DateTime::from_timestamp(0, 0).unwrap(),
                        text: "hello".to_owned(),
                        score: 1.0,
                        parts_summary: Vec::new(),
                    }],
                }],
                matched_total: 1,
                has_more: false,
                next_cursor: None,
            };
            let result = json_result(&response).unwrap();
            let text = result.content[0].raw.as_text().unwrap().text.as_str();
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            assert!(value.get("request_id").is_none());
            assert_eq!(value["sessions"][0]["matches"][0]["message_id"], "m1");
        }
    }
}
