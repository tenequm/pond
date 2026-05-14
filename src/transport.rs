//! The HTTP+JSON and stdio-MCP transports: thin adapters over the shared wire
//! handlers (`pond_search`, `pond_get`). Both transports dispatch to the exact
//! same handler functions - the only intentional divergence is the MCP
//! placeholder rendering in [`mcp::render_placeholders`] (design.md 3.6.3).
//!
//! Ingest has no transport in v1: only the `pond ingest` CLI verb writes to the
//! store, and it does its own batching. There is no `POST /v1/ingest` and no
//! `pond_ingest` MCP tool.

use std::sync::Arc;

use crate::{embed::EmbedBackend, substrate::PondStore};

/// Shared state handed to both transports: the store and the embedding backend
/// (`pond_search` needs the backend to embed the query). `Arc<dyn EmbedBackend>`
/// so tests can inject a fake backend without loading model weights.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<PondStore>,
    pub embedder: Arc<dyn EmbedBackend>,
}

pub mod http {
    //! axum HTTP+JSON server: `POST /v1/search`, `POST /v1/get`, and the `/mcp`
    //! route carrying rmcp's streamable-HTTP MCP transport.

    use std::net::{IpAddr, SocketAddr};

    use anyhow::Context;
    use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
    use rmcp::transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    };
    use tokio::net::TcpListener;

    use super::AppState;
    use crate::{
        get::pond_get,
        search::pond_search,
        wire::{ErrorCode, GetEnvelope, GetRequest, SearchEnvelope, SearchRequest},
    };

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
            .with_state(state)
            .nest_service("/mcp", mcp)
    }

    /// Bind and serve until ctrl-c. `--port 0` selects an OS-assigned free port;
    /// an unspecified host (`0.0.0.0` / `::`) logs a security notice because the
    /// personal pond is single-user and LAN exposure is opt-in (design.md 2.1.1).
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
        Json(request): Json<SearchRequest>,
    ) -> (StatusCode, Json<SearchEnvelope>) {
        let envelope = pond_search(&state.store, state.embedder.as_ref(), request).await;
        let status = match &envelope {
            SearchEnvelope::Success(_) => StatusCode::OK,
            SearchEnvelope::Error(error) => status_for(&error.error.code),
        };
        (status, Json(envelope))
    }

    async fn get(
        State(state): State<AppState>,
        Json(request): Json<GetRequest>,
    ) -> (StatusCode, Json<GetEnvelope>) {
        let envelope = pond_get(&state.store, request).await;
        let status = match &envelope {
            GetEnvelope::Success(_) => StatusCode::OK,
            GetEnvelope::Error(error) => status_for(&error.error.code),
        };
        (status, Json(envelope))
    }

    /// Map a wire error code to an HTTP status. The envelope body still carries
    /// the full typed error (3.6.1); the status is the coarse signal.
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
        get::pond_get as run_get,
        search::pond_search as run_search,
        types::{PartKind, StoredMessage},
        wire::{
            ErrorCode as WireErrorCode, ErrorEnvelope, GetEnvelope, GetRequest, GetResult,
            ProjectMatch, SearchEnvelope, SearchFilters, SearchMode, SearchRequest,
            default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource.
    const SCHEMA_DOC: &str = "\
pond_search filters: query (semantic - concepts, not project names), limit \
(default 10, max 200), project (path substring), conversation_id (exact \
session match), source_agent, role (user|assistant), from_date / to_date \
(YYYY-MM-DD), min_score (default 0.0; pond's RRF score is not on kb's 0-1 \
scale), boost_recent (default true), group_by_conversation (default false).

pond_get: message_id (one message + context_depth messages of thread context \
each side) OR conversation_id (full session; up_to truncates, max_messages \
caps at 1000). include_thinking / include_tool_results default false; excluded \
parts are rendered as [reasoning: N chars] / [tool_result: N chars] placeholders.";

    /// `pond_search` MCP tool parameters. Field names follow the kb parity
    /// contract: `conversation_id` here maps to the wire `session_id` filter.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpSearchParams {
        /// What to search for: concepts and keywords. Keep it semantic - do not
        /// put project names in the query, use the `project` filter instead.
        query: String,
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
        /// Minimum hybrid score. Default 0.0 - pond's RRF score is not on kb's
        /// 0-1 scale, so a 0.5 default would filter everything.
        #[serde(default)]
        min_score: Option<f64>,
        /// Boost recent messages in ranking. Default true.
        #[serde(default)]
        boost_recent: Option<bool>,
        /// Collapse hits to one summary per session. Default false.
        #[serde(default)]
        group_by_conversation: Option<bool>,
    }

    /// `pond_get` MCP tool parameters. `conversation_id` maps to the wire
    /// `session_id`; one of `message_id` / `conversation_id` is required.
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
        /// With conversation_id: cap on returned messages. Default 100, max 1000.
        #[serde(default)]
        max_messages: Option<usize>,
        /// Include reasoning parts in full. Default false (rendered as a
        /// `[reasoning: N chars]` placeholder).
        #[serde(default)]
        include_thinking: Option<bool>,
        /// Include tool-result parts in full. Default false (rendered as a
        /// `[tool_result: N chars]` placeholder).
        #[serde(default)]
        include_tool_results: Option<bool>,
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
            description = "Hybrid search (vector + full-text + RRF) over stored conversation \
                           history. Returns ranked message hits. Keep `query` semantic; use \
                           the `project` / `conversation_id` filters for scope."
        )]
        async fn pond_search(
            &self,
            Parameters(params): Parameters<McpSearchParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: default_namespace(),
                query: params.query,
                search_mode: SearchMode::default(),
                rrf_k: 60,
                filters: SearchFilters {
                    project: params.project,
                    project_match: ProjectMatch::default(),
                    session_id: params.conversation_id,
                    source_agent: params.source_agent,
                    from_date: params.from_date,
                    to_date: params.to_date,
                    role: params.role,
                    min_score: params.min_score.unwrap_or(0.0),
                },
                boost_recent: params.boost_recent.unwrap_or(true),
                group_by_conversation: params.group_by_conversation.unwrap_or(false),
                limit: params.limit.unwrap_or(10),
            };
            match run_search(&self.state.store, self.state.embedder.as_ref(), request).await {
                SearchEnvelope::Success(response) => json_result(&response),
                SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Retrieve stored conversation content. With `message_id`: that \
                           message plus `context_depth` messages of thread context each side. \
                           With `conversation_id`: the full session. Excluded thinking / \
                           tool-result parts are rendered as `[reasoning: N chars]` / \
                           `[tool_result: N chars]` placeholders."
        )]
        async fn pond_get(
            &self,
            Parameters(params): Parameters<McpGetParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let include_thinking = params.include_thinking.unwrap_or(false);
            let include_tool_results = params.include_tool_results.unwrap_or(false);
            // Always fetch full content from the shared handler; the MCP
            // transport renders placeholders for excluded parts (design.md
            // 3.6.3) instead of dropping them - so the divergence lives here,
            // not in the handler.
            let request = GetRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: default_namespace(),
                session_id: params.conversation_id,
                message_id: params.message_id,
                up_to: params.up_to,
                context_depth: params.context_depth.unwrap_or(0),
                max_messages: params.max_messages.unwrap_or(100),
                include_thinking: true,
                include_tool_results: true,
            };
            match run_get(&self.state.store, request).await {
                GetEnvelope::Success(mut response) => {
                    render_placeholders(
                        &mut response.result,
                        include_thinking,
                        include_tool_results,
                    );
                    json_result(&response)
                }
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
                    let (sessions, messages, parts, embeddings) =
                        self.state.store.row_counts().await.map_err(|error| {
                            ErrorData::internal_error(format!("stats unavailable: {error}"), None)
                        })?;
                    let stats = serde_json::json!({
                        "sessions": sessions,
                        "messages": messages,
                        "parts": parts,
                        "embeddings": embeddings,
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
    /// JSON-RPC frames, written by rmcp's stdio transport (design.md 2.1.1).
    pub async fn serve_stdio(state: AppState) -> anyhow::Result<()> {
        let service = PondMcp::new(state)
            .serve(stdio())
            .await
            .context("failed to start stdio MCP server")?;
        service.waiting().await.context("stdio MCP server error")?;
        Ok(())
    }

    /// Replace excluded `Reasoning` / `ToolResult` parts with a compact text
    /// placeholder so a calling agent knows retrievable content exists and can
    /// re-request with the toggle (design.md 3.6.3). This is the one
    /// intentional MCP-vs-HTTP response divergence.
    fn render_placeholders(
        result: &mut GetResult,
        include_thinking: bool,
        include_tool_results: bool,
    ) {
        let messages: &mut Vec<StoredMessage> = match result {
            GetResult::Session(session) => &mut session.messages,
            GetResult::Message { messages, .. } => messages,
        };
        for stored in messages.iter_mut() {
            for part in stored.parts.iter_mut() {
                let placeholder = match &part.kind {
                    PartKind::Reasoning { text } if !include_thinking => {
                        Some(format!("[reasoning: {} chars]", text.chars().count()))
                    }
                    PartKind::ToolResult { result, .. } if !include_tool_results => Some(format!(
                        "[tool_result: {} chars]",
                        result.to_string().chars().count()
                    )),
                    _ => None,
                };
                if let Some(text) = placeholder {
                    part.kind = PartKind::Text { text };
                }
            }
        }
    }

    /// Serialize a wire response into an MCP tool result (one JSON text block).
    fn json_result<T: serde::Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
        let text = serde_json::to_string(value).map_err(|error| {
            ErrorData::internal_error(format!("failed to serialize response: {error}"), None)
        })?;
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }

    /// Map a wire error envelope to a JSON-RPC error. rmcp ships no app-level
    /// codes, so pond defines its own `-32000`-family set here.
    fn to_error_data(envelope: &ErrorEnvelope) -> ErrorData {
        let code = match envelope.error.code {
            WireErrorCode::ValidationFailed => JsonRpcErrorCode(-32010),
            WireErrorCode::VersionUnsupported => JsonRpcErrorCode(-32011),
            WireErrorCode::NotFound => JsonRpcErrorCode(-32012),
            WireErrorCode::NamespaceUnknown => JsonRpcErrorCode(-32013),
            WireErrorCode::StorageUnavailable => JsonRpcErrorCode(-32014),
            WireErrorCode::Conflict => JsonRpcErrorCode(-32015),
            WireErrorCode::Internal => JsonRpcErrorCode(-32016),
        };
        ErrorData::new(
            code,
            envelope.error.message.clone(),
            Some(envelope.error.details.clone()),
        )
    }
}
