//! The HTTP+JSON and stdio-MCP transports: thin adapters over the shared wire
//! handlers. Both transports dispatch to the same handler functions - no
//! per-transport behavior divergence.
//!
//! HTTP exposes `POST /v1/search`, `POST /v1/get`, and `POST /v1/ingest`. MCP
//! exposes `pond_search` / `pond_get` (the kb-parity surface) plus
//! `pond_sql_query` (read-only SQL); ingest stays HTTP-only and CLI-only.

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
    //! The rmcp MCP layer: `pond_search` / `pond_get` / `pond_sql_query` tools
    //! and `schema://pond` / `schema://pond-sql` / `stats://pond` (plus
    //! `pond-sql-export://` export artifacts) resources, transport-agnostic.
    //! Mounted on stdio (via `pond mcp`) and on the `/mcp` HTTP route (via
    //! `pond serve`).

    use anyhow::Context;
    use base64::{Engine, engine::general_purpose::STANDARD};
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
    use uuid::Uuid;

    use super::AppState;
    use crate::{
        PROTOCOL_VERSION,
        handlers::pond_get as run_get,
        handlers::pond_search as run_search,
        sql,
        substrate::Table,
        wire::{
            ErrorCode as WireErrorCode, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse,
            GetResult, MessageView, PartKind, PartSummary, ProjectFilter, ResponseMode,
            ResponsePart, SearchEnvelope, SearchFilters, SearchRequest, SearchResponse,
            SessionFrom, default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource. Detail
    /// agents load on demand; the per-tool descriptions below stay tight.
    const SCHEMA_DOC: &str = "\
pond_search filters: query (semantic - concepts, not project names), limit \
(returned sessions; default 10, max 200 - also the want-more knob, there is \
no pagination), project (path substring), session_id (exact session match - \
semantic search within one session), source_agent, from_date / to_date \
(YYYY-MM-DD), format (text|json).

pond_search response: a transcript (or structured JSON when format=json). The \
first line states totals (`matched_total` is the message count before `limit` \
and byte-budget truncation), then results are grouped by session, ordered by \
each session's best hit. Each session lists up to 3 top-scoring hits, \
score-desc; each hit is a `--- [n] score | role | time | message_id | project \
| agent | session ---` rule followed by its matched text (a ~600-char indexed \
window). `score` is normalized to [0.0, 1.0] within one response. `has_more` \
warns the ranked set was cut by `limit` or the byte budget - raise `limit` to \
see the rest.

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

    /// Static documentation served as the `schema://pond-sql` resource: the
    /// table/column schema, dialect, function set, output modes, pagination
    /// pattern, drilling pattern, and worked examples for `pond_sql_query`.
    /// Loaded on demand so the tool description stays tight.
    ///
    /// TODO(#47): when the lance v8 FM-Index on parts.variant_data lands,
    /// tool-body substring search becomes `contains(variant_data, 'needle')`;
    /// update the routing guidance below (drop the "Never LIKE over parts ...
    /// no substring index (yet)" framing) and the timeout message in
    /// src/sql.rs.
    const SQL_SCHEMA_DOC: &str = "\
pond_sql_query runs ONE read-only SELECT (DataFusion SQL, PostgreSQL-compatible) \
over three registered tables. Read-only is hard-enforced: anything other than a \
single SELECT/WITH (or EXPLAIN of one) is rejected (no INSERT/UPDATE/DELETE/\
CREATE/DROP/COPY/SET).

Routing - pick the right surface before writing SQL:
- counts, group-by, time buckets, joins over metadata -> this tool, on \
messages/sessions.
- which tools ran / failed, tool params -> this tool, on parts (type = \
'tool_call' / 'tool_result'); worked example below.
- find text in conversations -> WHERE contains_tokens(search_text, '...') to \
filter, FROM fts('messages', ...) to rank (both below), or pond_search for \
meaning-based recall. Never LIKE over parts - tool bodies are JSON with no \
substring index (yet), and the conversational text is messages.search_text.
- read a transcript (a session, a message with context) -> pond_get, not SQL.

Tables and columns:
- messages(session_id text, message_id text, timestamp timestamp(us, UTC), role \
text {user|assistant|system|tool}, source_agent text, project text, content text \
NULL [system-role messages only], search_text text NULL [the conversational text - \
null for system/tool messages], embedding_model text NULL, options json). The \
embedding `vector` column exists but is never returned (omitted from results) and \
explicit projection of it is rejected; you may still filter on it in WHERE, e.g. \
`vector IS NOT NULL`. For semantic search, use pond_search.
- sessions(session_id text, parent_session_id text NULL, parent_message_id text \
NULL, source_agent text, created_at timestamp(us, UTC), project text, options json).
- parts(session_id text, message_id text, id text, ordinal int, type text \
{text|reasoning|file|tool_call|tool_result|tool_approval_request|\
tool_approval_response - exact strings, underscores not hyphens}, provenance \
text {conversational|injected}, variant_data json, options json). The verbatim \
part body lives in `variant_data`; its fields follow the part type, e.g. \
tool_call carries {call_id, name, params}, tool_result carries {call_id, name, \
is_failure, result}, text/reasoning carry {text}. FilePart binary payloads are \
not exposed in SQL.
Enum literals matter: a wrong value (e.g. 'tool-call') is valid SQL and silently \
returns zero rows. Discovery from SQL works too: SELECT table_name, column_name, \
data_type FROM information_schema.columns.

Join keys: messages.session_id = sessions.session_id; parts.session_id = \
messages.session_id AND parts.message_id = messages.message_id. Subagents are \
sessions whose source_agent matches '%/%' (e.g. 'claude-code/general-purpose').

Indexed (fast) filter columns: messages.project / session_id / timestamp / role / \
source_agent / message_id; parts.session_id / message_id; sessions.session_id. \
Prefer equality/range predicates on these. Known limitation: prefix LIKE ('x%') and starts_with() FAIL \
on bitmap-indexed columns (messages.source_agent, messages.role) with \"LIKE \
prefix queries are not supported for bitmap indexes\". Workarounds: equality, \
split_part(source_agent, '/', 1) = 'claude-code', or an infix pattern \
(LIKE '%/%' is fine - leading-wildcard patterns are not pushed to the index).

JSON columns (options, variant_data) are binary JSONB. Rules:
- NEVER CAST a JSON column (`variant_data::text` is rejected at plan time - the \
binary encoding can otherwise silently render as garbage). Stringify with \
json_extract(col, '$').
- A leading-wildcard LIKE over the whole document \
(`json_extract(variant_data, '$') LIKE '%...%'`) is rejected at plan time: it \
stringifies and scans every row and never finishes over parts. Match a single \
field (`json_extract(variant_data, '$.field') LIKE '...'`), scope to one session, \
or use contains_tokens for conversational text. (Substring search over tool \
bodies arrives with the FM-Index, #47.)
- json_extract(col, '$.a.b') takes a full JSONPath and returns JSON text of ANY \
value (objects/arrays serialize) - the right call for deeply nested or mixed-type \
fields, e.g. json_extract(variant_data, '$.params.command').
- json_get_string|json_get_int|json_get_float|json_get_bool(col, 'key', ...) walk \
a key path - json_get_string(options, 'anthropic', 'model') - array steps by \
numeric index. json_get_string serializes non-string values; the typed getters \
return NULL on a non-coercible value.
- json_get(col, 'key') returns JSONB for chaining: \
json_get_string(json_get(variant_data, 'params'), 'command').
- Also: json_array_contains(col, 'key', value), json_array_length(col, 'key').

Worked example - tool usage and failure rates over the last week:

  SELECT json_get_string(c.variant_data, 'name') AS tool,
         COUNT(*) AS calls,
         SUM(CASE WHEN json_get_bool(r.variant_data, 'is_failure') THEN 1 \
ELSE 0 END) AS failures
  FROM parts c
  JOIN messages m ON m.session_id = c.session_id AND m.message_id = c.message_id
  LEFT JOIN parts r ON r.session_id = c.session_id
   AND r.type = 'tool_result'
   AND json_get_string(r.variant_data, 'call_id') = \
json_get_string(c.variant_data, 'call_id')
  WHERE c.type = 'tool_call' AND m.timestamp >= now() - INTERVAL '7 days'
  GROUP BY tool ORDER BY calls DESC;

Full-text search in SQL is a pair - filter form and ranked form:
- Filtering (WHERE): contains_tokens(search_text, 'word1 word2') - true when the \
text contains ALL the words (split on punctuation/whitespace, case-sensitive \
tokens); accelerated by the FTS index. The right tool for exact strings, \
identifiers, and error messages - compose freely with other predicates: \
SELECT message_id FROM messages WHERE contains_tokens(search_text, 'OCC retry') \
AND project LIKE '%pond%'.
- Ranking (FROM): the fts() table function returns matches plus `_score` (BM25 \
relevance, a regular projectable column): SELECT message_id, _score, search_text \
FROM fts('messages', '{\"match\":{\"column\":\"search_text\",\"terms\":\"...\"}}') \
ORDER BY _score DESC - compose with WHERE/JOIN/GROUP BY around it. AND semantics: \
add \"operator\":\"And\" to the match; \"boolean\" queries (must/should/must_not \
over match clauses) also work. \"phrase\" queries are unavailable (index built \
without positions) - use contains_tokens or match + operator And, optionally with \
LIKE post-filters, for exact substrings.
fts() in WHERE is a plan-time error that points back here. Unlike pond_search, \
both forms cover subagent sessions (filter them out with WHERE NOT (source_agent \
LIKE '%/%') if unwanted). Vector/semantic search is NOT available in SQL; use \
pond_search for that.

Function quick-reference (exact DataFusion names so the model doesn't have to \
guess):
- aggregates: count, count(distinct ...), sum, avg, min, max, any_value, stddev, \
median, approx_distinct, approx_percentile_cont, array_agg, string_agg
- date/time: now(), date_trunc('day'|'hour'|'minute'|..., ts), date_part('year'|..., \
ts), date_bin(interval, ts, origin), to_char(ts, fmt), to_timestamp(text), \
extract(field FROM ts), age(t1, t2)
- intervals: `INTERVAL '7 days'`, `INTERVAL '1 hour'` (single-quoted, postgres-style)
- string: length, lower, upper, substr, position, split_part, regexp_like, \
regexp_match, regexp_replace, like, ilike, starts_with, ends_with, concat, \
concat_ws
- text search: contains_tokens(col, 'words') in WHERE; fts(table, query_json) in \
FROM (see above)
- numeric: round, floor, ceil, abs, sign, log, exp, power, sqrt
- conditional: CASE WHEN ... THEN ... ELSE ..., coalesce, nullif, greatest, least
- cast: CAST(x AS TYPE) or x::TYPE - but never on JSON columns (see the JSON \
rules above)
Quote identifiers with double quotes when they collide with keywords (e.g. \
\"timestamp\"); string literals use single quotes.

EXPLAIN is allowed: `EXPLAIN <query>` or `EXPLAIN ANALYZE <query>` returns the \
DataFusion plan (and per-operator timings for ANALYZE) so you can self-diagnose \
slow queries without leaving SQL.

Output modes (the `format` arg):
- text (default): a row-capped rendered ASCII table with a header showing \
`{total_rows} in {elapsed_ms} ms; showing {shown}` and, on truncation, a \
keyset-pagination hint.
- json: same row-capped payload as `text` but delivered as a JSON object \
{total_rows, shown_rows, truncated, elapsed_ms, columns, rows: [{col: val, ...}]}. \
Spec-compliant dual delivery: the structured JSON rides MCP's `structuredContent` \
field; clients that don't surface that channel get the same JSON as a text block. \
Empirically validated on Claude Code 2.1.165 - the agent reads the structured form.
- parquet | ndjson: write the FULL result set to a file and return a \
`pond-sql-export://<id>` resource link; read it via MCP resources/read. On a \
local/stdio install the response also names the on-disk path so you can open it \
directly with duckdb/polars.

Pagination - keyset (preferred):
Use ORDER BY on indexed columns plus a composite seek key for stable tie-breaking. \
The agent owns the cursor (the last sort value it saw); no server-side state.

  -- page 1: most recent 100 messages in pond
  SELECT message_id, timestamp, role, project
  FROM messages
  WHERE project LIKE '%pond%'
  ORDER BY timestamp DESC, message_id DESC
  LIMIT 100;

  -- page 2: pass back the last (timestamp, message_id) the agent saw
  SELECT message_id, timestamp, role, project
  FROM messages
  WHERE project LIKE '%pond%'
    AND (timestamp, message_id) < (TIMESTAMP '2026-06-05T08:14:22.123456Z', 'last-id')
  ORDER BY timestamp DESC, message_id DESC
  LIMIT 100;

Keyset stays stable across concurrent ingest (older rows don't shift) and uses \
the btree on `timestamp`/`message_id` directly. For known-bounded full results, skip \
pagination entirely: format=parquet writes everything in one call. OFFSET works \
but scans-and-discards prior rows and shifts pages under writes - prefer keyset.

Drilling from aggregates to content (instead of N round-trips of pond_get):
JOIN to messages/parts directly. Example - top 10 longest sessions with first \
user message:

  WITH top_sessions AS (
    SELECT session_id, COUNT(*) AS msgs
    FROM messages
    GROUP BY session_id
    ORDER BY msgs DESC
    LIMIT 10
  )
  SELECT ts.session_id, ts.msgs, s.project, s.source_agent,
         m.search_text AS first_user_msg
  FROM top_sessions ts
  JOIN sessions s ON s.session_id = ts.session_id
  LEFT JOIN messages m
    ON m.session_id = ts.session_id
   AND m.role = 'user'
   AND m.timestamp = (
     SELECT MIN(timestamp) FROM messages
     WHERE session_id = ts.session_id AND role = 'user'
   );

One call, agent picks exactly which columns to hydrate. When you want the \
pond_get-style rendered transcript (tool-call lines, subagent footer), call \
pond_get with the session_id - that's its job.

Examples (4 patterns the agent should recognize):

  -- 1. Activity by project this week
  SELECT project, COUNT(*) AS msgs, COUNT(DISTINCT session_id) AS sessions
  FROM messages
  WHERE timestamp >= now() - INTERVAL '7 days'
  GROUP BY project
  ORDER BY msgs DESC
  LIMIT 20;

  -- 2. Subagent breakdown
  SELECT source_agent, COUNT(*) AS n
  FROM sessions
  WHERE source_agent LIKE '%/%'
  GROUP BY source_agent
  ORDER BY n DESC;

  -- 3. Text filter in WHERE (all words must appear), composed with metadata
  SELECT message_id, timestamp, project, substr(search_text, 1, 120) AS preview
  FROM messages
  WHERE contains_tokens(search_text, 'race condition')
    AND timestamp >= now() - INTERVAL '30 days'
  ORDER BY timestamp DESC
  LIMIT 50;

  -- 4. BM25 search in FROM, joined with metadata, relevance-ranked
  SELECT m.session_id, m.timestamp, m.project, f._score, m.search_text
  FROM fts('messages', \
'{\"match\":{\"column\":\"search_text\",\"terms\":\"race condition\"}}') f
  JOIN messages m ON m.message_id = f.message_id
  WHERE m.project LIKE '%pond%'
  ORDER BY f._score DESC
  LIMIT 50;";

    /// `pond_search` MCP tool parameters.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpSearchParams {
        /// What to search for: concepts and keywords. Keep it semantic - do
        /// not put project names in the query, use the `project` filter
        /// instead.
        query: String,
        /// Max sessions to return. Default 10, server-capped at 200. This is
        /// also the "want more results" knob - raise it; there is no pagination.
        #[serde(default)]
        limit: Option<usize>,
        /// Filter to projects whose path contains this substring.
        #[serde(default)]
        project: Option<String>,
        /// Filter to one session (exact match) - semantic search within a
        /// single, possibly long, session.
        #[serde(default)]
        session_id: Option<String>,
        /// Filter to one source agent, e.g. "claude-code" or
        /// "claude-code/general-purpose" (a subagent).
        #[serde(default)]
        source_agent: Option<String>,
        /// Include subagent / sub-task sessions. Default false: search targets
        /// the main sessions where the human and agent talked. Set true to
        /// include subagent sessions (source_agent like "claude-code/<name>").
        #[serde(default)]
        include_subagents: Option<bool>,
        /// Only messages on or after this date (YYYY-MM-DD).
        #[serde(default)]
        from_date: Option<String>,
        /// Only messages on or before this date (YYYY-MM-DD).
        #[serde(default)]
        to_date: Option<String>,
        /// Output shape: "text" (default - a rendered transcript of the ranked
        /// hits) or "json" (the same hits as structured data).
        #[serde(default)]
        format: Option<String>,
    }

    /// `pond_get` MCP tool parameters. Exactly one of `message_id` /
    /// `session_id` is required.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpGetParams {
        /// Retrieve this message: its full parts plus `context_depth` sibling
        /// messages each side (conversational siblings by default; set
        /// response_mode to widen).
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
        /// Depth: "conversational" (default; human/model text only, with part
        /// summaries), "complete" (all messages incl. system/tool carriers,
        /// with part summaries), or "verbatim" (all messages with full parts
        /// inline; session mode only for the parts). In message mode it
        /// selects which siblings fill the context window.
        #[serde(default)]
        response_mode: Option<String>,
        /// Session mode only: which end to read `limit` messages from -
        /// "start" (oldest, default) or "end" (most recent, e.g. to recover
        /// recent context after compaction). Results stay chronological;
        /// ignored in message mode.
        #[serde(default)]
        session_from: Option<String>,
        /// Exclusive continuation anchor from a prior response: the last
        /// `message_id` (session mode) or last `part_id` (message mode).
        #[serde(default)]
        after_id: Option<String>,
    }

    /// `pond_sql_query` MCP tool parameters.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpSqlParams {
        /// One read-only SQL statement (DataFusion / PostgreSQL-compatible).
        /// SELECT/WITH only (or EXPLAIN of one); writes and side-effecting
        /// statements are rejected. Exact columns - messages(session_id,
        /// message_id, timestamp, role, source_agent, project, content
        /// [system-role only], search_text [the conversational text],
        /// embedding_model, options) | sessions(session_id,
        /// parent_session_id, parent_message_id, source_agent, created_at,
        /// project, options) | parts(session_id, message_id, id, ordinal,
        /// type, provenance, variant_data, options). parts.type enums use
        /// underscores: 'tool_call', 'tool_result', 'text', 'reasoning',
        /// 'file'. JSON columns (variant_data, options) are JSONB: read
        /// fields with json_extract(col, '$.a.b') or json_get_string(col,
        /// 'key', ...), never CAST them. Text search: WHERE
        /// contains_tokens(search_text, 'words') to filter, FROM
        /// fts('messages', '{...}') for BM25-ranked results. Control row count
        /// with SQL `LIMIT`; inline output is capped at 100 rows (use
        /// format=parquet|ndjson to get every row). See the `schema://pond-sql`
        /// resource for joins, JSON/FTS functions, pagination + drilling
        /// patterns, and worked examples.
        #[serde(alias = "sql")]
        query: String,
        /// Output format: "text" (default; rendered ASCII table with metrics
        /// footer), "json" (same row-capped data as a structured JSON object,
        /// delivered via MCP structuredContent), "parquet", or "ndjson". For
        /// parquet/ndjson the full result set is written to a file and a
        /// `pond-sql-export://` resource link is returned (no truncation).
        #[serde(default)]
        format: Option<String>,
    }

    fn parse_session_from(value: Option<String>) -> SessionFrom {
        match value.as_deref() {
            Some("end") => SessionFrom::End,
            _ => SessionFrom::Start,
        }
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
                           format and the first line states totals plus how many searchable \
                           messages the filters left in scope (the absence signal - search only \
                           sees conversational text, never tool calls/results), then results are \
                           grouped by session, ordered by each session's best hit. Each hit is a \
                           `--- [n] score | role | time | message_id | project | agent | session \
                           ---` delimiter rule followed by the matched text. Pass a returned \
                           `message_id` to `pond_get` for full text. Common args: \
                           query (semantic - concepts, not project names), then project / \
                           from_date / to_date to scope, limit to widen (no pagination - raise \
                           limit for more). Advanced: source_agent (e.g. \"claude-code\", or \
                           \"claude-code/general-purpose\" for subagents), session_id (search \
                           within one long session), include_subagents (subagent sessions are \
                           excluded by default), format (\"text\" default, or \"json\" for \
                           structured hits). \
                           Scores are relative within one response; there is no min_score. For \
                           exact strings, identifiers, or error messages, pond_sql_query is the \
                           sharper tool - WHERE contains_tokens(search_text, 'words') to \
                           filter, FROM fts('messages', ...) for BM25 ranking - and it sees \
                           subagent sessions too.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_search(
            &self,
            Parameters(params): Parameters<McpSearchParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let json = matches!(params.format.as_deref(), Some("json"));
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                query: params.query,
                filters: SearchFilters {
                    project: params.project.map(ProjectFilter::Contains),
                    session_id: params.session_id,
                    source_agent: params.source_agent,
                    from_date: params.from_date,
                    to_date: params.to_date,
                    // min_score is intentionally not on the MCP surface; scores
                    // are response-relative, so a server-side threshold is a
                    // footgun for agent callers. CLI / HTTP still exposes it
                    // for the bench harness.
                    min_score: 0.0,
                    include_subagents: params.include_subagents.unwrap_or(false),
                },
                limit: params.limit.unwrap_or(10),
                mode_override: None,
            };
            match run_search(
                &self.state.store,
                &self.state.embedder,
                request.clone(),
                &self.state.search,
            )
            .await
            {
                SearchEnvelope::Success(response) if json => {
                    // `structured()` mirrors the same bytes into the text
                    // content block, so shadowing clients still get the data.
                    Ok(CallToolResult::structured(
                        serde_json::to_value(&response).map_err(|error| {
                            ErrorData::internal_error(
                                format!("failed to serialize search response: {error}"),
                                None,
                            )
                        })?,
                    ))
                }
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
                           limit (cap), after_id (paging - pass the value the footer shows), \
                           session_from (\"start\"|\"end\"; \"end\" returns the most recent \
                           messages, \
                           e.g. to recover context after compaction). \
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
                session_from: parse_session_from(params.session_from),
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

        #[tool(
            description = "Run ONE read-only SQL query (DataFusion / PostgreSQL-compatible) \
                           over the stored corpus as three tables: sessions, messages, parts. \
                           For filtering, joins, and aggregation (counts, group-by, time \
                           buckets) - the analytic complement to pond_search's semantic \
                           recall. SELECT/WITH only (or EXPLAIN of one); writes and side- \
                           effecting statements are rejected. The exact column lists are in \
                           the `query` parameter description - use those names, do not guess \
                           (column discovery also works: SELECT column_name FROM \
                           information_schema.columns WHERE table_name = 'messages'). \
                           Routing: metadata analytics -> SQL on messages/sessions; tool-call \
                           analytics -> parts WHERE type = 'tool_call' with \
                           json_get_string(variant_data, 'name'); text search -> WHERE \
                           contains_tokens(search_text, 'words') to filter or FROM \
                           fts('messages', '{...json...}') for BM25-ranked results, or \
                           pond_search for semantic recall; reading a transcript -> pond_get, \
                           not SQL. The embedding `vector` column is never returned (explicit \
                           projection is rejected; filtering in WHERE is fine). Control row \
                           count with SQL `LIMIT`; inline output (format text|json) is capped \
                           at 100 rows. format defaults to text (a row-capped rendered table); \
                           set format=json for a structured JSON payload (delivered via MCP \
                           structuredContent), or format=parquet|ndjson to write the full \
                           result to a file returned as a pond-sql-export:// resource. Read \
                           resource schema://pond-sql \
                           for joins, indexed columns, JSON access rules, the function \
                           quick-reference, pagination + drilling patterns, and worked \
                           examples.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_sql_query(
            &self,
            Parameters(params): Parameters<McpSqlParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let mode = match params.format.as_deref() {
                None | Some("text") => sql::Mode::Inline,
                Some("json") => sql::Mode::InlineJson,
                Some("parquet") => sql::Mode::Export(sql::Format::Parquet),
                Some("ndjson") => sql::Mode::Export(sql::Format::Ndjson),
                Some(other) => {
                    return Ok(CallToolResult::error(vec![Content::text(format!(
                        "unknown format {other:?}; use \"text\", \"json\", \"parquet\", \
                         or \"ndjson\""
                    ))]));
                }
            };
            let inline_rows = sql::DEFAULT_INLINE_ROWS;

            // The three tables are independent (per-table caches/mutexes), so
            // overlap their freshness/manifest fetches rather than serialize.
            let store = &self.state.store;
            let tables = match tokio::try_join!(
                store.dataset(Table::Sessions),
                store.dataset(Table::Messages),
                store.dataset(Table::Parts),
            ) {
                Ok((sessions, messages, parts)) => sql::Tables {
                    sessions,
                    messages,
                    parts,
                },
                Err(_) => {
                    return Err(ErrorData::internal_error(
                        "sql datasets unavailable".to_owned(),
                        None,
                    ));
                }
            };

            match sql::run(&tables, &params.query, mode, inline_rows).await {
                Ok(sql::Outcome::Inline(text)) => Ok(tool_result(text)),
                Ok(sql::Outcome::InlineJson(value)) => Ok(CallToolResult::structured(value)),
                Ok(sql::Outcome::Export {
                    bytes,
                    format,
                    rows,
                    columns,
                }) => {
                    let name = format!("{}.{}", Uuid::now_v7(), format.ext());
                    match store.export_write(&name, &bytes).await {
                        Ok(_) => Ok(export_result(
                            store,
                            &name,
                            format,
                            rows,
                            &columns,
                            bytes.len(),
                        )),
                        Err(error) => Err(ErrorData::internal_error(
                            format!("export write failed: {error}"),
                            None,
                        )),
                    }
                }
                Err(sql::SqlError::Query(message)) => {
                    Ok(CallToolResult::error(vec![Content::text(message)]))
                }
                Err(sql::SqlError::Infra(error)) => Err(ErrorData::internal_error(
                    format!("sql execution failed: {error}"),
                    None,
                )),
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
                 (path substring), session_id, source_agent, from_date / to_date - \
                 keep query semantic (concepts, not project names). Scores are relative \
                 within one response; there is no min_score. Subagents are stored as their \
                 own sessions (source_agent like \"claude-code/general-purpose\"); pond_get \
                 on a parent session lists them in a footer so you can open each. Recover \
                 context lost to compaction: find this session via pond_search (a distinctive \
                 recent topic + project + from_date=today), then pond_get(session_id, \
                 session_from=\"end\") for the recent pre-compaction turns. Deeper \
                 reference on demand: resource schema://pond (all filters + response format), \
                 stats://pond (corpus + embedding stats). For structured/analytic queries \
                 (filtering, joins, counts, group-by) use pond_sql_query: read-only SQL \
                 (SELECT only) over the sessions/messages/parts tables, with optional \
                 parquet/ndjson export; see resource schema://pond-sql. Search only indexes \
                 conversational text (tool calls/results are invisible to it), and a \
                 zero/weak result is not proof of absence - for exact strings, \
                 identifiers, or error messages run pond_sql_query with WHERE \
                 contains_tokens(search_text, 'words') (all words must match; \
                 index-accelerated), or FROM fts('messages', \
                 '{\"match\":{\"column\":\"search_text\",\"terms\":\"...\"}}') for \
                 BM25-ranked results; both cover subagent sessions too.",
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
                    RawResource::new("schema://pond-sql", "pond SQL table schema").no_annotation(),
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
                "schema://pond-sql" => Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    SQL_SCHEMA_DOC,
                    request.uri,
                )])),
                // `pond_sql_query` export artifacts: read the file pond wrote
                // (parquet -> base64 blob, ndjson -> text). The filename is
                // validated to a minted `<uuid>.<ext>` so the URI can't traverse.
                uri if uri.starts_with("pond-sql-export://") => {
                    let name = uri.trim_start_matches("pond-sql-export://").to_owned();
                    if !valid_export_name(&name) {
                        return Err(ErrorData::resource_not_found(
                            format!("invalid export id: {name}"),
                            None,
                        ));
                    }
                    let bytes = self.state.store.export_read(&name).await.map_err(|error| {
                        ErrorData::resource_not_found(format!("export not found: {error}"), None)
                    })?;
                    let contents = if name.ends_with(".ndjson") {
                        ResourceContents::text(
                            String::from_utf8_lossy(&bytes).into_owned(),
                            request.uri,
                        )
                        .with_mime_type("application/x-ndjson")
                    } else {
                        ResourceContents::blob(STANDARD.encode(&bytes), request.uri)
                            .with_mime_type("application/vnd.apache.parquet")
                    };
                    Ok(ReadResourceResult::new(vec![contents]))
                }
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
                "pond_sql_query" => 80_000,
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

    /// Build the `pond_sql_query` export result: a text summary plus a
    /// `resource_link` to the artifact (the spec-canonical way to hand back a
    /// tool-produced file - the bytes ride `resources/read`, not the tool
    /// result, so they don't load into context unless the host fetches them).
    /// On a `file://` install the summary also names the on-disk path so a
    /// co-located agent can read it directly.
    fn export_result(
        store: &crate::sessions::Store,
        name: &str,
        format: sql::Format,
        rows: usize,
        columns: &[String],
        bytes: usize,
    ) -> CallToolResult {
        let uri = format!("pond-sql-export://{name}");
        let column_list = if columns.is_empty() {
            "(none)".to_owned()
        } else {
            columns.join(", ")
        };
        let mut summary = format!(
            "Exported {rows} row(s), {bytes} bytes ({}). Columns: {column_list}.\n\
             Fetch via MCP resources/read on {uri}.",
            format.ext()
        );
        if let Some(path) = store.export_local_path(name) {
            summary.push_str(&format!(
                "\nLocal file: {} - on this (stdio) install you can read it directly \
                 (e.g. duckdb, polars).",
                path.display()
            ));
        }
        let link = RawResource::new(uri, name.to_owned())
            .with_description(format!("pond SQL export ({}, {rows} rows)", format.ext()))
            .with_mime_type(format.mime().to_owned())
            .with_size(u32::try_from(bytes).unwrap_or(u32::MAX));
        CallToolResult::success(vec![Content::text(summary), Content::resource_link(link)])
    }

    /// Accept only the export filenames pond mints (`<uuid>.parquet|ndjson`),
    /// guarding the `pond-sql-export://` resource against path traversal.
    fn valid_export_name(name: &str) -> bool {
        let Some((stem, ext)) = name.rsplit_once('.') else {
            return false;
        };
        matches!(ext, "parquet" | "ndjson")
            && !stem.is_empty()
            && stem.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
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
        // Must mirror build_filter's default-exclusion condition, else the note lies.
        let subagent_note = if !request.filters.include_subagents
            && request.filters.session_id.is_none()
            && request.filters.source_agent.is_none()
        {
            " Subagent sessions excluded; pass include_subagents=true to include them."
        } else {
            ""
        };
        if response.sessions.is_empty() {
            // spec.md#search-absence-honesty: name the scope size and the
            // recovery path - a zero-hit response must distinguish "nothing
            // relevant exists" from "the filters excluded everything".
            if response.searchable_in_scope == 0 {
                return format!(
                    "pond_search: 0 searchable messages in scope - the filters exclude \
                     everything before retrieval. Widen or drop project/date filters.\
                     {subagent_note}\n"
                );
            }
            let fts_hint = " For exact strings or identifiers, try pond_sql_query: SELECT \
                            message_id, session_id, search_text FROM messages WHERE \
                            contains_tokens(search_text, '...').";
            return format!(
                "pond_search: no matches for {:?} across {} searchable messages in \
                 scope.{subagent_note}{fts_hint}\n",
                request.query, response.searchable_in_scope
            );
        }
        let shown: usize = response.sessions.iter().map(|s| s.matches.len()).sum();
        let mut out = String::new();
        let _ = writeln!(
            out,
            "pond_search: {} matching messages ({} searchable in scope), showing {} hits from {} \
             sessions.{}",
            response.matched_total,
            response.searchable_in_scope,
            shown,
            response.sessions.len(),
            subagent_note,
        );
        let _ = writeln!(
            out,
            "key: session rules group hits by session, ordered by best hit; \"--- [n] score | role | time | message_id | project | agent | session ---\" delimits each hit + matched text. pond_get <message_id> for full; raise limit for more (no pagination)."
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
                    match request.session_from {
                        SessionFrom::Start => {
                            let _ = writeln!(
                                out,
                                "... {} more messages; pass after_id={} to pond_get to continue",
                                messages_remaining, last.id,
                            );
                        }
                        // Tail page: the remaining messages are *earlier*, before this
                        // page. after_id only pages forward, so it can't reach them -
                        // point back to the start instead of a cursor that dead-ends.
                        SessionFrom::End => {
                            let _ = writeln!(
                                out,
                                "... {messages_remaining} earlier messages precede this tail; call pond_get with session_from=\"start\" to read from the beginning",
                            );
                        }
                    }
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
                let label = file_name
                    .as_deref()
                    .or(media_type.as_deref())
                    .unwrap_or("file");
                let _ = writeln!(out, "  [file {label}]");
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
                session_from: SessionFrom::default(),
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
                searchable_in_scope: 2,
                has_more: false,
            };
            let request = SearchRequest {
                protocol_version: crate::PROTOCOL_VERSION,
                namespace: None,
                query: "hi".to_owned(),
                mode_override: None,
                filters: SearchFilters::default(),
                limit: 10,
            };

            let transcript = render_search_transcript(&response, &request);
            assert!(transcript.starts_with(
                "pond_search: 1 matching messages (2 searchable in scope), showing 1 hits from 1 \
                 sessions."
            ));
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
