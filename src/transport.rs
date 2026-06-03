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
            ErrorCode as WireErrorCode, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse,
            GetResult, MessageView, PartKind, PartSummary, ProjectFilter, ResponseMode,
            ResponsePart, SearchEnvelope, SearchFilters, SearchRequest, SearchResponse,
            default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource. Detail
    /// agents load on demand; the per-tool descriptions below stay tight.
    const SCHEMA_DOC: &str = "\
pond_search filters: query (semantic - concepts, not project names), limit \
(returned sessions; default 10, max 200), project (path substring), session_id \
(exact session match), source_agent, role (user|assistant|system|tool), \
from_date / to_date (YYYY-MM-DD), cursor (opaque continuation token).

pond_search response: a transcript. The first line states totals \
(`matched_total` is the message count before `limit` and byte-budget \
truncation), then results are grouped by session, ordered by each session's \
best hit. Each session lists up to 3 top-scoring hits, score-desc; each hit is \
a `--- [n] score | role | time | message_id | project | agent | session ---` \
rule followed by its matched text (a ~600-char indexed window). `score` is \
normalized to [0.0, 1.0] within one response. When more remain, a `cursor:` \
footer carries the token to pass back as `cursor`; rank may shift between \
pages if the corpus changes.

pond_search multilingual: pond's embedder (multilingual-e5-small) is trained \
for cross-lingual retrieval, so a query in language A can match indexed text \
in language B via the vector arm. The FTS arm is character-ngram-based and \
only matches surface tokens, so for cross-lingual queries expect most signal \
to come from the vector arm.

pond_get: message_id (the target message, marked `>`, plus context_depth \
sibling messages each side) OR session_id (the whole session). Output is a \
transcript - each message is a `--- [n] role | time | message_id ---` rule, \
then its text/content as real lines, then parts (`-> name [call_id]` tool \
call, `<- name [call_id] (ok|failed)` result). Session mode takes \
response_mode: \"conversational\" (default - human/model text only), \
\"complete\" (all messages incl. carriers, tools as one-liners), or \
\"verbatim\" (full part bodies inline; heaviest). limit defaults to 20, caps \
at 1000. Bounded by a size budget: when the footer shows `after_id=`, pass it \
back to page. A whole-session response also lists the session's subagents (each \
stored as its own session) in a footer; pass a listed id back as session_id to \
open it. Not for bulk export - use `pond export`.";

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
        /// Max sessions to return. Default 10, server-capped at 200.
        #[serde(default)]
        limit: Option<usize>,
        /// Filter to projects whose path contains this substring.
        #[serde(default)]
        project: Option<String>,
        /// Filter to one session (exact match).
        #[serde(default)]
        session_id: Option<String>,
        /// Filter to one source agent, e.g. "claude-code" or
        /// "claude-code/general-purpose" (a subagent).
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
                           Returns a readable transcript: a leading `key:` line explains the \
                           format and the first line states totals, then results are grouped by \
                           session, ordered by each session's best hit. Each hit is a `--- [n] \
                           score | role | time | message_id | project | agent | session ---` \
                           delimiter rule followed by the matched text. Pass a returned \
                           `message_id` to `pond_get` for full text. Common args: \
                           query (semantic - concepts, not project names), then project / \
                           from_date / to_date to scope. Advanced: source_agent (e.g. \
                           \"claude-code\", or \"claude-code/general-purpose\" for subagents), \
                           similar_to (vector-only neighbors of a message_id), cursor (paging). \
                           Scores are relative within one response; there is no min_score.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
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
                request.clone(),
                &self.state.search,
            )
            .await
            {
                SearchEnvelope::Success(response) => {
                    Ok(tool_result(render_search_transcript(&response, &request)))
                }
                SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Retrieve stored conversation content as a readable transcript \
                           (a leading `key:` line explains the format). Common: session_id \
                           (whole session; pair with response_mode \
                           conversational|complete|verbatim) OR message_id (that message \
                           marked `>`, plus context_depth sibling messages each side, with \
                           its tool/file parts in full). A session_id response lists the \
                           session's subagents in a footer so you can open each. Advanced: \
                           limit (cap), after_id (paging - pass the value the footer shows). \
                           Tool/result lines render as `-> name [call_id]` / `<- name \
                           [call_id] (ok|failed)`. Not for bulk export - use `pond export`.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
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
            match run_get(&self.state.store, request.clone()).await {
                GetEnvelope::Success(response) => {
                    let mut transcript = render_get_transcript(&response, &request);
                    // Spawn-only subagents are stored as their own sessions
                    // (spec.md#datasets); surface them on the parent's first page
                    // so an agent can open each (otherwise they are undiscoverable
                    // from the MCP surface). Best-effort: a lookup failure just
                    // omits the footer rather than failing the get.
                    if request.message_id.is_none()
                        && request.after_id.is_none()
                        && let Ok(children) =
                            self.state.store.child_sessions(&response.session.id).await
                        && !children.is_empty()
                    {
                        transcript.push_str(&render_subagents_footer(&children));
                    }
                    Ok(tool_result(transcript))
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
                "pond recalls past agent sessions (Claude Code and others) - prior work, \
                 decisions, and context across sessions, not the live conversation. \
                 Workflow: pond_search to find relevant messages, then pond_get to read \
                 full text by message_id or a whole session by session_id; both return \
                 readable transcripts, not JSON. Scope with filters, not the query: project \
                 (path substring), session_id, source_agent, role, from_date / to_date - \
                 keep query semantic (concepts, not project names). Scores are relative \
                 within one response; there is no min_score. Subagents are stored as their \
                 own sessions (source_agent like \"claude-code/general-purpose\"); pond_get \
                 on a parent session lists them in a footer so you can open each. Deeper \
                 reference on demand: resource schema://pond (all filters + response format), \
                 stats://pond (corpus + embedding stats).",
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

    /// Build an MCP tool result from a rendered transcript. Deliberately text
    /// only: Claude Code surfaces `structuredContent` over the text block when
    /// both are present, which would shadow the transcript - the readable view
    /// is the whole point on the MCP surface. Programmatic clients that want the
    /// structured wire shape use the HTTP `/v1/*` JSON API instead.
    fn tool_result(transcript: String) -> CallToolResult {
        CallToolResult::success(vec![Content::text(transcript)])
    }

    /// Footer for a `pond_get` session response listing the session's spawn-only
    /// subagents. Each subagent is its own session (spec.md#datasets) addressable
    /// by the printed id, so the caller can open any with `pond_get(session_id)`;
    /// without this they are invisible from the MCP surface.
    fn render_subagents_footer(children: &[crate::wire::Session]) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "subagents ({}) - pass an id to pond_get(session_id=...):",
            children.len()
        );
        for child in children {
            let _ = writeln!(out, "  {} | {}", child.id, child.source_agent);
        }
        out
    }

    /// `YYYY-MM-DD HH:MM:SSZ` - compact, sortable, timezone-explicit.
    fn fmt_ts(ts: &chrono::DateTime<chrono::Utc>) -> String {
        ts.format("%Y-%m-%d %H:%M:%SZ").to_string()
    }

    /// Inner string of an `Extracted<String>` option, or `?` when the source
    /// carried none (spec.md#model-no-synthesis: absence is real, not a blank).
    fn opt_name(value: &Option<crate::adapter::extract::Extracted<String>>) -> &str {
        value.as_deref().map(String::as_str).unwrap_or("?")
    }

    /// Append each line of `body` to `out`, so escaped `\n` in stored text
    /// renders as real line breaks. A trailing blank line in the source is
    /// dropped (lines() already does this).
    fn push_lines(out: &mut String, body: &str, indent: &str) {
        use std::fmt::Write;
        for line in body.lines() {
            let _ = writeln!(out, "{indent}{line}");
        }
    }

    fn render_search_transcript(response: &SearchResponse, request: &SearchRequest) -> String {
        use std::fmt::Write;
        if response.sessions.is_empty() {
            return match request.similar_to.as_deref() {
                Some(id) => format!("pond_search: no matches similar to {id}.\n"),
                None => format!("pond_search: no matches for {:?}.\n", request.query),
            };
        }
        let shown: usize = response.sessions.iter().map(|s| s.matches.len()).sum();
        let sim = request
            .similar_to
            .as_deref()
            .map(|id| format!(" similar to {id}"))
            .unwrap_or_default();
        let mut out = String::new();
        let _ = writeln!(
            out,
            "pond_search: {} matching messages, showing {} hits from {} sessions{}.",
            response.matched_total,
            shown,
            response.sessions.len(),
            sim,
        );
        let _ = writeln!(
            out,
            "key: session rules group hits by session, ordered by best hit; \"--- [n] score | role | time | message_id | project | agent | session ---\" delimits each hit + matched text. pond_get <message_id> for full; pass cursor to page."
        );
        let mut index = 0;
        for (session_index, session) in response.sessions.iter().enumerate() {
            let best = session
                .matches
                .first()
                .map(|hit| hit.score)
                .unwrap_or_default();
            let _ = writeln!(out);
            let _ = writeln!(
                out,
                "{}",
                rule_line(&format!(
                    "session [{}] best {:.2} | {}/{} matched | {} | {} | {}",
                    session_index + 1,
                    best,
                    session.matched_message_count,
                    session.session_messages_count,
                    session.project,
                    session.source_agent,
                    session.session_id,
                )),
            );
            for hit in &session.matches {
                index += 1;
                let _ = writeln!(out);
                let _ = writeln!(
                    out,
                    "{}",
                    rule_line(&format!(
                        "[{index}] {:.2} | {} | {} | {} | {} | {} | {}",
                        hit.score,
                        hit.role.as_str(),
                        fmt_ts(&hit.timestamp),
                        hit.message_id,
                        session.project,
                        session.source_agent,
                        session.session_id,
                    )),
                );
                push_lines(&mut out, &hit.text, "");
            }
        }
        if let Some(cursor) = &response.next_cursor {
            let _ = writeln!(out);
            let _ = writeln!(out, "cursor: {cursor} (pass as `cursor` to page)");
        }
        out
    }

    fn render_get_transcript(response: &GetResponse, request: &GetRequest) -> String {
        use std::fmt::Write;
        let session = &response.session;
        let mut out = String::new();
        match &response.result {
            GetResult::Session {
                messages,
                messages_remaining,
            } => {
                let mode = match request.response_mode {
                    ResponseMode::Conversational => "conversational",
                    ResponseMode::Complete => "complete",
                    ResponseMode::Verbatim => "verbatim",
                };
                let more = if *messages_remaining > 0 {
                    " (more)"
                } else {
                    ""
                };
                let _ = writeln!(
                    out,
                    "pond_get: session {} ({mode}), {} messages{more}.",
                    session.id,
                    messages.len(),
                );
                let _ = writeln!(
                    out,
                    "key: \"--- [n] role | time | message_id ---\" delimits each message; \"->\" tool call, \"<-\" result. Pass after_id=<id> to page."
                );
                for (idx, message) in messages.iter().enumerate() {
                    let _ = writeln!(out);
                    render_message(
                        &mut out,
                        idx + 1,
                        message,
                        message.parts.as_deref(),
                        &message.parts_summary,
                        false,
                    );
                }
                let _ = writeln!(out);
                let _ = writeln!(
                    out,
                    "session {} | {} | {}",
                    session.id, session.source_agent, session.project,
                );
                if *messages_remaining > 0
                    && let Some(last) = messages.last()
                {
                    let _ = writeln!(
                        out,
                        "... {} more messages; pass after_id={} to pond_get to continue",
                        messages_remaining, last.id,
                    );
                }
            }
            GetResult::Message {
                target,
                target_parts,
                target_parts_remaining,
                siblings,
            } => {
                let _ = writeln!(
                    out,
                    "pond_get: thread around {} in session {} (context +/-{}).",
                    target.id, session.id, request.context_depth,
                );
                let _ = writeln!(
                    out,
                    "key: \"--- [n] role | time | message_id ---\" delimits each message; \">\" = the one you requested; \"->\" tool call, \"<-\" result. pond_get <message_id> to expand any line."
                );
                // Interleave target with siblings, ordered by (timestamp, id) to
                // match storage - codex writes many messages at the same
                // timestamp, so the id is the real tiebreak (a bare timestamp
                // sort scrambles them). Drop context siblings with nothing to
                // render (carrier turns with no text/content/parts); the
                // requested target always stays, even if empty.
                let mut thread: Vec<(&MessageView, bool)> =
                    siblings.iter().map(|view| (view, false)).collect();
                thread.push((target, true));
                thread.sort_by(|a, b| {
                    a.0.timestamp
                        .cmp(&b.0.timestamp)
                        .then_with(|| a.0.id.cmp(&b.0.id))
                });
                thread.retain(|(view, is_target)| *is_target || message_has_content(view));
                for (idx, (view, is_target)) in thread.iter().enumerate() {
                    let _ = writeln!(out);
                    let parts: Option<&[ResponsePart]> = if *is_target {
                        Some(target_parts.as_slice())
                    } else {
                        view.parts.as_deref()
                    };
                    render_message(
                        &mut out,
                        idx + 1,
                        view,
                        parts,
                        &view.parts_summary,
                        *is_target,
                    );
                }
                let _ = writeln!(out);
                let _ = writeln!(
                    out,
                    "session {} | {} | {}",
                    session.id, session.source_agent, session.project,
                );
                if *target_parts_remaining > 0
                    && let Some(last) = target_parts.last()
                {
                    let _ = writeln!(
                        out,
                        "... {} more parts of {}; pass after_id={} to pond_get to continue",
                        target_parts_remaining, target.id, last.id,
                    );
                }
            }
        }
        out
    }

    /// Whether a message view has anything to render below its header: real
    /// text/content, or any parts (full or summarized). Used to drop empty
    /// carrier turns from message-mode context.
    fn message_has_content(view: &MessageView) -> bool {
        view.text.as_deref().is_some_and(|t| !t.trim().is_empty())
            || view
                .content
                .as_deref()
                .is_some_and(|c| !c.trim().is_empty())
            || view.parts.as_deref().is_some_and(|p| !p.is_empty())
            || !view.parts_summary.is_empty()
    }

    /// Target column width for a delimiter-rule header.
    const RULE_WIDTH: usize = 72;

    /// Wrap `inner` as a delimiter rule: `--- {inner} ----...` padded to
    /// [`RULE_WIDTH`] (always at least a 3-dash tail when `inner` is already
    /// wide). Used for both search hits and get message headers.
    fn rule_line(inner: &str) -> String {
        let head = format!("--- {inner} ");
        let pad = RULE_WIDTH.saturating_sub(head.chars().count()).max(3);
        format!("{head}{}", "-".repeat(pad))
    }

    /// One message block: an indexed `--- [n] role | time | id ---` delimiter
    /// rule (unambiguous even when the body has blank lines or `##` headings),
    /// then text/content as real lines, then parts - full bodies when `parts`
    /// is present, else one-line summaries.
    fn render_message(
        out: &mut String,
        index: usize,
        view: &MessageView,
        parts: Option<&[ResponsePart]>,
        summary: &[PartSummary],
        is_target: bool,
    ) {
        use std::fmt::Write;
        let marker = if is_target { "> " } else { "" };
        let _ = writeln!(
            out,
            "{}",
            rule_line(&format!(
                "[{index}] {marker}{} | {} | {}",
                view.role.as_str(),
                fmt_ts(&view.timestamp),
                view.id,
            )),
        );
        if let Some(text) = &view.text {
            push_lines(out, text, "");
        }
        if let Some(content) = &view.content {
            push_lines(out, content, "");
        }
        match parts {
            Some(parts) => {
                for part in parts {
                    render_part_full(out, part);
                }
            }
            None => {
                for part in summary {
                    render_part_summary(out, part);
                }
            }
        }
    }

    fn render_part_full(out: &mut String, part: &ResponsePart) {
        use std::fmt::Write;
        match &part.kind {
            PartKind::Text { text } => {
                if let Some(text) = text {
                    push_lines(out, text, "");
                }
            }
            PartKind::Reasoning { text } => {
                let _ = writeln!(out, "  (reasoning)");
                if let Some(text) = text {
                    push_lines(out, text, "  ");
                }
            }
            PartKind::ToolCall {
                name,
                call_id,
                params,
                ..
            } => {
                let _ = writeln!(out, "  -> {} [{}]", opt_name(name), opt_name(call_id));
                push_lines(out, &value_to_text(params), "     ");
            }
            PartKind::ToolResult {
                name,
                call_id,
                is_failure,
                result,
            } => {
                let status = if *is_failure { "failed" } else { "ok" };
                let _ = writeln!(
                    out,
                    "  <- {} [{}] ({status})",
                    opt_name(name),
                    opt_name(call_id),
                );
                push_lines(out, &value_to_text(result), "     ");
            }
            PartKind::File {
                media_type,
                file_name,
                ..
            } => {
                let _ = writeln!(
                    out,
                    "  [file {}]",
                    file_name.as_deref().unwrap_or(media_type)
                );
            }
            PartKind::ToolApprovalRequest { approval_id, .. } => {
                let _ = writeln!(out, "  [approval request {approval_id}]");
            }
            PartKind::ToolApprovalResponse {
                approval_id,
                approved,
                ..
            } => {
                let verb = if *approved { "approved" } else { "denied" };
                let _ = writeln!(out, "  [approval {approval_id} {verb}]");
            }
        }
    }

    fn render_part_summary(out: &mut String, summary: &PartSummary) {
        use std::fmt::Write;
        let label = summary.label.as_deref().unwrap_or("");
        let call = summary
            .call_id
            .as_deref()
            .map(|id| format!(" [{id}]"))
            .unwrap_or_default();
        match summary.kind.as_str() {
            "tool_call" => {
                let _ = writeln!(out, "  -> {label}{call}");
            }
            "tool_result" => {
                let _ = writeln!(out, "  <- {label}{call}");
            }
            "file" => {
                let _ = writeln!(out, "  [file {label}]");
            }
            other => {
                let _ = writeln!(out, "  [{other} {label}]");
            }
        }
    }

    /// Render a tool param/result `Value` for the transcript: a JSON string
    /// shows as its text; anything else as compact JSON. `null` shows nothing.
    fn value_to_text(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(text) => text.clone(),
            serde_json::Value::Null => String::new(),
            other => serde_json::to_string(other).unwrap_or_default(),
        }
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
        fn get_transcript_marks_target_and_renders_tool_parts() {
            let ts = chrono::DateTime::from_timestamp(0, 0).unwrap();
            let tool_call: ResponsePart = serde_json::from_value(serde_json::json!({
                "id": "p1", "ordinal": 0, "provenance": "conversational",
                "type": "tool_call", "name": "Bash", "call_id": "toolu_x",
                "params": { "command": "ls" }, "provider_executed": false,
            }))
            .unwrap();
            let tool_result: ResponsePart = serde_json::from_value(serde_json::json!({
                "id": "p2", "ordinal": 1, "provenance": "conversational",
                "type": "tool_result", "name": "Bash", "call_id": "toolu_x",
                "is_failure": false, "result": "file.txt",
            }))
            .unwrap();
            let target = MessageView {
                id: "m1".to_owned(),
                role: crate::wire::Role::Assistant,
                timestamp: ts,
                text: Some("Let me list files.".to_owned()),
                content: None,
                parts_summary: Vec::new(),
                parts: None,
            };
            let response = GetResponse {
                session: crate::wire::GetSession {
                    id: "s1".to_owned(),
                    source_agent: "claude-code".to_owned(),
                    project: "/p".to_owned(),
                    created_at: ts,
                },
                result: GetResult::Message {
                    target,
                    target_parts: vec![tool_call, tool_result],
                    target_parts_remaining: 0,
                    siblings: Vec::new(),
                },
            };
            let request = GetRequest {
                protocol_version: crate::PROTOCOL_VERSION,
                namespace: None,
                session_id: None,
                message_id: Some("m1".to_owned()),
                context_depth: 0,
                limit: 20,
                response_mode: ResponseMode::default(),
                after_id: None,
            };

            let transcript = render_get_transcript(&response, &request);
            assert!(transcript.contains("--- [1] > assistant | 1970-01-01 00:00:00Z | m1 ---"));
            assert!(transcript.contains("Let me list files."));
            assert!(transcript.contains("  -> Bash [toolu_x]"));
            assert!(transcript.contains("  <- Bash [toolu_x] (ok)"));
            assert!(transcript.contains("session s1 | claude-code | /p"));
        }

        #[test]
        fn search_transcript_renders_header_and_hits() {
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
                        text: "hello\nworld".to_owned(),
                        score: 1.0,
                        parts_summary: Vec::new(),
                    }],
                }],
                matched_total: 1,
                has_more: false,
                next_cursor: None,
            };
            let request = SearchRequest {
                protocol_version: crate::PROTOCOL_VERSION,
                namespace: None,
                query: "hi".to_owned(),
                mode_override: None,
                similar_to: None,
                filters: SearchFilters::default(),
                limit: 10,
                cursor: None,
            };

            let transcript = render_search_transcript(&response, &request);
            assert!(
                transcript.starts_with(
                    "pond_search: 1 matching messages, showing 1 hits from 1 sessions."
                )
            );
            assert!(
                transcript
                    .contains("key: session rules group hits by session, ordered by best hit")
            );
            assert!(
                transcript
                    .contains("--- session [1] best 1.00 | 1/2 matched | pond | claude-code | s1")
            );
            // Hit lines stay flat and indexed so callers can still extract
            // message_id from the same delimiter shape.
            assert!(transcript.contains(
                "--- [1] 1.00 | user | 1970-01-01 00:00:00Z | m1 | pond | claude-code | s1"
            ));
            // Stored "\n" renders as a real line break, not an escape.
            assert!(transcript.contains("hello\nworld"));

            // The MCP result is transcript-only text (no structuredContent to
            // shadow it on the Claude Code client).
            let result = tool_result(transcript);
            assert!(result.content[0].raw.as_text().is_some());
            assert!(result.structured_content.is_none());
        }
    }
}
