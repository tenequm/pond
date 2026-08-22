//! The HTTP+JSON and stdio-MCP transports: thin adapters over the shared wire
//! handlers. Both transports dispatch to the same handler functions - no
//! per-transport behavior divergence.
//!
//! HTTP exposes `POST /v1/search`, `POST /v1/get-session`, `POST /v1/get-message`,
//! and `POST /v1/ingest`. MCP
//! exposes `pond_search` / `pond_get_session` / `pond_get_message` plus
//! `pond_sql` (read-only SQL); ingest stays HTTP-only and CLI-only.

use std::sync::Arc;

use crate::{config::SearchConfig, embed::LazyEmbedder, sessions::Store};

/// Shared state handed to both transports. `embedder` holds a lazy handle:
/// the model isn't loaded until the first vector search asks for it, so
/// `pond mcp` idles at ~50 MB resident and only pays the ~600 MB load cost on
/// the first query that needs it (spec.md#search opt-in).
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub embedder: Arc<LazyEmbedder>,
    pub search: SearchConfig,
}

pub mod http {
    //! axum HTTP+JSON server: `POST /v1/search`, `POST /v1/get-session`,
    //! `POST /v1/get-message`, and the `/mcp` route carrying rmcp's
    //! streamable-HTTP MCP transport.

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
        handlers::{pond_get_message, pond_get_session, pond_ingest, pond_search},
        wire::{
            ErrorCode, GetEnvelope, GetMessageRequest, GetSessionRequest, IngestEnvelope,
            IngestRequest, SearchEnvelope, SearchRequest, default_namespace, new_request_id,
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
            .route("/v1/get-session", post(get_session))
            .route("/v1/get-message", post(get_message))
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

    async fn get_session(
        State(state): State<AppState>,
        Json(mut request): Json<GetSessionRequest>,
    ) -> Response {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_get_session(&state.store, request).await;
        let status = match &envelope {
            GetEnvelope::Success(_) => StatusCode::OK,
            GetEnvelope::Error(error) => status_for(&error.error.code),
        };
        with_request_id((status, Json(envelope)).into_response())
    }

    async fn get_message(
        State(state): State<AppState>,
        Json(mut request): Json<GetMessageRequest>,
    ) -> Response {
        request.namespace.get_or_insert_with(default_namespace);
        let envelope = pond_get_message(&state.store, request).await;
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
    //! The rmcp MCP layer: `pond_search` / `pond_get_session` /
    //! `pond_get_message` / `pond_sql` tools
    //! and `schema://pond` / `schema://pond-sql` / `stats://pond` (plus
    //! `pond-sql-export://` export artifacts) resources, transport-agnostic.
    //! Mounted on stdio (via `pond mcp`) and on the `/mcp` HTTP route (via
    //! `pond serve`).

    use std::sync::Arc;

    use anyhow::Context;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use rmcp::{
        ErrorData, RoleServer, ServerHandler, ServiceExt,
        handler::server::{router::tool::ToolRouter, wrapper::Parameters},
        model::{
            AnnotateAble, CallToolResult, Content, ErrorCode as JsonRpcErrorCode, Implementation,
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
        handlers::{
            pond_get_message as run_get_message, pond_get_session as run_get_session,
            pond_search as run_search,
        },
        sql,
        substrate::Table,
        wire::{
            ErrorCode as WireErrorCode, ErrorEnvelope, GetEnvelope, GetMessageRequest,
            GetSessionRequest, ProjectFilter, SearchEnvelope, SearchFilters, SearchModeWire,
            SearchRequest, SessionFrom, SortBy, default_namespace,
        },
    };

    /// Static documentation served as the `schema://pond` resource. Detail
    /// agents load on demand; the per-tool descriptions below stay tight.
    const SCHEMA_DOC: &str = "\
pond_search params: query (the distinctive words you expect in the \
conversation, not project names), mode (fts default - matches exact whole \
words, BM25 | vector - matches on meaning), \
sort_by (relevance default | recency), limit (returned sessions; default 10, \
max 200 - also the want-more knob, there is no pagination), project (path \
substring), session_id (exact session match - search within one session), \
source_agent (one source harness, exact-or-subpath: \"openclaw\" also covers \
\"openclaw/subagent\", not \"openclaw-x\"), from_date / to_date (YYYY-MM-DD). \
Subagents are excluded by default: a root source_agent like \"openclaw\" returns \
main sessions only, while naming a subpath (e.g. \"claude-code/general-purpose\") \
reaches those subagent sessions and is the opt-in that disables the default \
exclusion. Subagents are also reachable via pond_sql (parent_session_id).

pond_search response: a transcript. The first line states totals \
(`matched_total` is the message count before `limit` and byte-budget \
truncation), then results are grouped by session, best session first; within \
a session, matching messages are newest-first. Each hit is a `--- [n] score | \
role | time | message_id | project | agent | session ---` rule followed by its \
matched text (a ~600-char indexed window). `score` is in [0.0, 1.0] within one \
response (raw cosine for vector, normalized BM25 for fts). vector relevance \
ordering adds a gentle recency tiebreaker; sort_by=recency orders strictly \
newest-first and the response labels itself so. `has_more` warns the ranked \
set was cut by `limit` or the byte budget - raise `limit` to see the rest.

pond_search multilingual: pond's embedder (multilingual-e5-small) is trained \
for cross-lingual retrieval, so a vector query in language A can match indexed \
text in language B. fts is word-tokenized (the `simple` tokenizer with English \
stemming) and matches surface words only, so it stays within one language.

pond_get_session: id (a session id; a message id also works and resolves to \
its parent session with the page anchored at that message - the response notes \
the resolution). Output is a transcript - each message is a `--- [n] role | \
time | message_id ---` rule, then its text/content as real lines, then \
one-line tool refs (`-> name [call_id]` tool call, `<- name [call_id] \
(ok|failed)` result); full part bodies stay one pond_get_message away. limit \
defaults to 20; from=start|end picks which end the first page reads from (end \
= recent tail - the session's final state, e.g. post-compaction recovery). \
A partial page's header states the span and session total (`messages 1-130 \
of 234`); pages are bounded by limit and a size budget - page with \
after_message_id (forward) or before_message_id (backward) using the id a \
page marker shows. The first page \
also lists the session's subagents (each stored as its own session) in a \
footer; pass a listed id back to open it. Not for bulk export - use `pond \
copy --to <file>`.

pond_get_message: id (a message id - the target renders marked `>` with its \
full part bodies, reasoning included, plus context_before / context_after \
conversational siblings each side, both default 3, like grep -B/-A). A \
session id is rejected with a hint naming pond_get_session - it cannot pick \
one message.";

    /// `schema://pond` for the serving instance. With embedding off the doc
    /// must not offer the vector arm
    /// (spec.md#protocol-self-describing-capabilities): the same
    /// self-description rule `annotate_search_mode_with`
    /// enforces on `tools/list`. Each rewrite is asserted so the doc and the
    /// rewrite cannot drift apart silently.
    fn schema_doc_for(enabled: bool) -> String {
        if enabled {
            return SCHEMA_DOC.to_owned();
        }
        let rewrites = [
            (
                "mode (fts default - matches exact whole \
words, BM25 | vector - matches on meaning)",
                "mode (fts, the only arm on this instance - matches exact whole words, BM25)",
            ),
            (
                "(raw cosine for vector, normalized BM25 for fts). vector relevance \
ordering adds a gentle recency tiebreaker;",
                "(normalized BM25);",
            ),
            (
                "pond's embedder (multilingual-e5-small) is trained \
for cross-lingual retrieval, so a vector query in language A can match indexed \
text in language B. fts",
                "fts",
            ),
        ];
        let mut doc = SCHEMA_DOC.to_owned();
        for (from, to) in rewrites {
            debug_assert!(
                doc.contains(from),
                "schema doc drifted from rewrite: {from}"
            );
            doc = doc.replace(from, to);
        }
        doc
    }

    /// Static documentation served as the `schema://pond-sql` resource: the
    /// table/column schema, dialect, function set, output modes, pagination
    /// pattern, drilling pattern, and worked examples for `pond_sql`.
    /// Loaded on demand so the tool description stays tight.
    ///
    /// TODO(#47): when the lance v8 FM-Index on parts.variant_data lands,
    /// tool-body substring search becomes `contains(variant_data, 'needle')`;
    /// update the routing guidance below (drop the "Never LIKE over parts ...
    /// no substring index (yet)" framing) and the timeout message in
    /// src/sql.rs.
    const SQL_SCHEMA_DOC: &str = "\
pond_sql runs ONE read-only SELECT (DataFusion SQL, PostgreSQL-compatible) \
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
- read a transcript -> pond_get_session (a session) / pond_get_message (a \
message with context), not SQL.

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
text {conversational|injected}, tool_name text NULL, call_id text NULL, \
is_failure boolean NULL, variant_data json, options json). `tool_name` / \
`call_id` / `is_failure` are narrow native columns materialized from the tool \
part bodies - ALWAYS prefer them over json_get_* on `variant_data` for tool \
analytics (name filters, GROUP BY, call->result joins, failure rates); they are \
NULL on non-tool parts (`is_failure` is non-NULL only on tool_result). The \
verbatim part body lives in `variant_data`; its fields follow the part type, \
e.g. tool_call carries {call_id, name, params}, tool_result carries {call_id, \
name, is_failure, result}, text/reasoning carry {text}. FilePart binary \
payloads are not exposed in SQL.
Enum literals matter: a wrong value (e.g. 'tool-call') is valid SQL and silently \
returns zero rows. Discovery from SQL works too: SELECT table_name, column_name, \
data_type FROM information_schema.columns.

Join keys: messages.session_id = sessions.session_id; parts.session_id = \
messages.session_id AND parts.message_id = messages.message_id. Subagents are \
sessions whose source_agent matches '%/%' (e.g. 'claude-code/general-purpose').

Indexed (fast) filter columns: messages.session_id / source_agent; \
parts.session_id / message_id / tool_name; sessions.session_id. \
Prefer equality/range predicates on these. Known limitation: prefix LIKE ('x%') and starts_with() FAIL \
on bitmap-indexed columns (messages.source_agent) with \"LIKE \
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

Worked example - tool usage and failure rates over the last week, entirely on \
the narrow native columns (no JSON getters):

  SELECT c.tool_name AS tool,
         COUNT(*) AS calls,
         SUM(CASE WHEN r.is_failure THEN 1 ELSE 0 END) AS failures
  FROM parts c
  JOIN messages m ON m.session_id = c.session_id AND m.message_id = c.message_id
  LEFT JOIN parts r ON r.session_id = c.session_id
   AND r.type = 'tool_result' AND r.call_id = c.call_id
  WHERE c.type = 'tool_call' AND m.timestamp >= now() - INTERVAL '7 days'
  GROUP BY tool ORDER BY calls DESC;

Worked example - counting tokens correctly (Claude Code sessions): pond is \
lossless - one source JSONL line = one row - so a multi-block assistant turn \
stores N rows sharing one provider message id \
(options.anthropic.id), EACH carrying that line's usage snapshot. Summing \
usage per row double-counts (~2-3x inflation). Usage is cumulative within a \
turn, so collapse to one value per provider message id with MAX (the final \
snapshot; DISTINCT over the usage tuple still double-counts when snapshots \
differ mid-turn), then aggregate. Always scope by session_id: usage lives \
inside the wide options column, which no index can serve (GROUP BY / MAX \
over JSON paths reads every candidate row's whole document), so an unscoped \
sweep is a full-column scan - minutes-to-timeout on a remote store. Even \
scoped, a remote store reads that session's options pages: raise \
timeout_seconds. For corpus-wide accounting, loop over session_ids or \
accept the full scan with timeout_seconds near the 600s cap.

  WITH turns AS (
    SELECT json_get_string(options, 'anthropic', 'id') AS turn_id,
           MAX(json_get_int(options, 'anthropic', 'usage', 'input_tokens')) AS in_tok,
           MAX(json_get_int(options, 'anthropic', 'usage', 'output_tokens')) AS out_tok,
           MAX(json_get_int(options, 'anthropic', 'usage', 'cache_read_input_tokens')) AS cache_read
    FROM messages
    WHERE session_id = '<session-id>' AND role = 'assistant'
      AND json_get_string(options, 'anthropic', 'id') IS NOT NULL
    GROUP BY turn_id
  )
  SELECT COUNT(*) AS turns, SUM(in_tok) AS input_tokens,
         SUM(out_tok) AS output_tokens, SUM(cache_read) AS cache_read_tokens
  FROM turns;

Worked example - scope-then-scan, the pattern for substring-hunting tool \
bodies without a substring index: first collect candidate session_ids from \
the indexed conversational text, then run the field LIKE only inside them \
(the session_id btree makes the body scan tractable; unscoped it times out):

  WITH hits AS (
    SELECT DISTINCT session_id FROM messages
    WHERE contains_tokens(search_text, 'cargo bench')
  )
  SELECT DISTINCT json_extract(p.variant_data, '$.params.command') AS cmd
  FROM parts p JOIN hits h ON h.session_id = p.session_id
  WHERE p.type = 'tool_call' AND p.tool_name = 'Bash'
    AND json_extract(p.variant_data, '$.params.command') LIKE '%cargo bench%';

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
keyset-pagination hint. Long text cells clip at ~1000 chars with a \
`[+N chars ...]` marker - the full value is one format=ndjson call away, or \
project a narrower json_extract path.
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

Drilling from aggregates to content (instead of N round-trips of \
pond_get_message):
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
rendered transcript (tool-call lines, subagent footer), call pond_get_session \
with the session_id - that's its job.

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
        /// What to search for: the distinctive words you expect in the
        /// conversation (error strings, symbol names, product names). Project
        /// names go in the project filter, not the query.
        query: String,
        /// "fts" (default): exact whole words via BM25. "vector": matches on
        /// meaning; available because this instance has embeddings enabled.
        #[serde(default)]
        mode: Option<String>,
        /// Result order: "relevance" (default - best match first) or "recency"
        /// (newest first; the response is labeled so you don't read rank-1 as
        /// the best match).
        #[serde(default)]
        sort_by: Option<String>,
        /// Max sessions to return. Default 10, server-capped at 200. This is
        /// also the "want more results" knob - raise it; there is no pagination.
        #[serde(default)]
        limit: Option<usize>,
        /// Filter to projects whose path contains this substring.
        #[serde(default)]
        project: Option<String>,
        /// Filter to one session (exact match) - search within a single,
        /// possibly long, session.
        #[serde(default)]
        session_id: Option<String>,
        /// Filter to one source harness. A root value ("openclaw",
        /// "claude-code") returns that harness's main sessions (subagents stay
        /// excluded, like the default). Name a subpath
        /// ("claude-code/general-purpose", "openclaw/subagent") to search those
        /// subagent sessions directly - a subpath value is the deliberate opt-in
        /// that disables the default subagent exclusion.
        #[serde(default)]
        source_agent: Option<String>,
        /// Only messages on or after this date (YYYY-MM-DD).
        #[serde(default)]
        from_date: Option<String>,
        /// Only messages on or before this date (YYYY-MM-DD).
        #[serde(default)]
        to_date: Option<String>,
    }

    /// `pond_get_session` MCP tool parameters.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpGetSessionParams {
        /// The session to read - a session_id from pond_search or a subagent
        /// footer. A message_id also works: it resolves to its parent session
        /// with the page anchored at that message.
        #[serde(alias = "session_id")]
        id: String,
        /// Max messages per page. Default 20, max 1000.
        #[serde(default)]
        limit: Option<usize>,
        /// Which end to read the first page from: "start" (oldest, default) or
        /// "end" (most recent - the session's final state, e.g. to recover
        /// context after compaction). Pages stay chronological.
        #[serde(default)]
        from: Option<String>,
        /// Page forward: a message id from a prior page's bottom marker;
        /// returns messages after it.
        #[serde(default)]
        after_message_id: Option<String>,
        /// Page backward: a message id from a prior page's top marker; returns
        /// messages before it.
        #[serde(default)]
        before_message_id: Option<String>,
    }

    /// `pond_get_message` MCP tool parameters.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpGetMessageParams {
        /// The message to expand - a message_id from pond_search or a
        /// transcript line.
        #[serde(alias = "message_id")]
        id: String,
        /// Conversational sibling messages to include before the target
        /// (mirrors grep -B). Default 3.
        #[serde(default)]
        context_before: Option<usize>,
        /// Conversational sibling messages to include after the target
        /// (mirrors grep -A). Default 3.
        #[serde(default)]
        context_after: Option<usize>,
    }

    /// `pond_sql` MCP tool parameters.
    #[derive(Debug, Deserialize, schemars::JsonSchema)]
    struct McpSqlParams {
        /// One read-only SQL statement (SELECT/WITH only; writes rejected).
        /// Exact columns - messages(session_id, message_id, timestamp, role,
        /// source_agent, project, content [system-role only], search_text
        /// [the conversational text], embedding_model, options) |
        /// sessions(session_id, parent_session_id, parent_message_id,
        /// source_agent, created_at, project, options) | parts(session_id,
        /// message_id, id, ordinal, type, provenance, tool_name, call_id,
        /// is_failure, variant_data, options). parts.type enums use
        /// underscores: 'tool_call', 'tool_result', 'text', 'reasoning',
        /// 'file'. Tool bodies live in JSONB variant_data - tool_call is
        /// {call_id, name, params} (a Bash command is
        /// json_extract(variant_data, '$.params.command')), tool_result is
        /// {call_id, name, is_failure, result}; never CAST JSON columns. No
        /// substring index covers tool bodies: an unscoped LIKE over
        /// variant_data full-scans and times out on a remote store -
        /// scope-then-scan instead (collect session_ids WHERE
        /// contains_tokens(search_text, '...'), then match variant_data
        /// fields only within them; worked example in schema://pond-sql).
        /// Tool analytics: prefer the narrow native columns (tool_name,
        /// call_id, is_failure). Text search: WHERE contains_tokens(search_text,
        /// 'words'), or FROM fts('messages', '{...}') for BM25 ranking.
        /// Joins, indexed columns, JSON functions, pagination, worked
        /// examples: resource schema://pond-sql.
        #[serde(alias = "sql")]
        query: String,
        /// Output format: "text" (default; rendered ASCII table with metrics
        /// footer, row-capped), "parquet", or "ndjson". For parquet/ndjson the
        /// full result set is written to a file and a `pond-sql-export://`
        /// resource link is returned (no truncation) - ndjson is the path for
        /// machine-readable JSON output.
        #[serde(default)]
        format: Option<String>,
        /// Per-query timeout in seconds (default 30, max 600). Raise it for a
        /// genuinely long-running query (e.g. a large remote-store scan);
        /// prefer narrower predicates and the indexed/native columns first.
        #[serde(default)]
        timeout_seconds: Option<u64>,
    }

    fn parse_session_from(value: Option<String>) -> SessionFrom {
        match value.as_deref() {
            Some("end") => SessionFrom::End,
            _ => SessionFrom::Start,
        }
    }

    /// Parse the `mode` param; absent defaults to fts. Returns `None` for an
    /// explicit unknown value so the caller can reject it.
    fn parse_search_mode(value: Option<&str>) -> Option<SearchModeWire> {
        match value {
            None | Some("fts") => Some(SearchModeWire::Fts),
            Some("vector") => Some(SearchModeWire::Vector),
            Some(_) => None,
        }
    }

    fn parse_sort_by(value: Option<&str>) -> Option<SortBy> {
        match value {
            None | Some("relevance") => Some(SortBy::Relevance),
            Some("recency") => Some(SortBy::Recency),
            Some(_) => None,
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
            description = "Find relevant messages in past sessions - the entry point for \
                           recall: \"have we worked on X\", \"what did we decide about Y\", \
                           \"find the session where...\". mode=\"fts\" (default) matches exact \
                           whole words (BM25); mode=\"vector\" matches meaning. Scope with \
                           project / session_id / source_agent / from_date / to_date; put the \
                           distinctive words you expect in the conversation in the query \
                           (error strings, symbol names, product names); project names go in \
                           the project filter. Returns scored hits grouped by \
                           session, best session first; pass a hit's session_id to \
                           pond_get_session or its message_id to pond_get_message to read \
                           it. Searches conversational text only (tool \
                           calls/results and reasoning are excluded by design - a gap there is \
                           expected, not a failure) and excludes subagent sessions; reach both \
                           via pond_sql. Response format details: resource schema://pond.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_search(
            &self,
            Parameters(params): Parameters<McpSearchParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let Some(mode) = parse_search_mode(params.mode.as_deref()) else {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "unknown mode {:?}; use \"vector\" or \"fts\"",
                    params.mode.unwrap_or_default()
                ))]));
            };
            let Some(sort_by) = parse_sort_by(params.sort_by.as_deref()) else {
                return Ok(CallToolResult::error(vec![Content::text(format!(
                    "unknown sort_by {:?}; use \"relevance\" or \"recency\"",
                    params.sort_by.unwrap_or_default()
                ))]));
            };
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                query: params.query,
                mode,
                sort_by,
                filters: SearchFilters {
                    project: params.project.map(ProjectFilter::Contains),
                    session_id: params.session_id,
                    source_agent: params.source_agent,
                    from_date: params.from_date,
                    to_date: params.to_date,
                },
                limit: params.limit.unwrap_or(10),
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
                    Ok(tool_result(crate::render::render_search_transcript(
                        &response,
                        &request,
                        crate::render::Surface::Mcp,
                    )))
                }
                // The `mode=vector` refusal is the caller's to fix in-turn (retry
                // with fts), so it rides back as a tool error, not a JSON-RPC
                // one: a JSON-RPC error makes plugin hosts tear down and respawn
                // the pond child on every call.
                SearchEnvelope::Error(envelope) if is_semantic_disabled(&envelope) => Ok(
                    CallToolResult::error(vec![Content::text(envelope.error.message.clone())]),
                ),
                SearchEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Read a whole past session as a chronological transcript - the tool \
                           for analyzing, reviewing, or summarizing a session (user/assistant \
                           text plus one-line tool/file refs; tool bodies stay one \
                           pond_get_message away). Pass the id from pond_search or a subagent \
                           footer; a message_id also works - it resolves to its parent \
                           session with the page anchored at that message. Paging: limit \
                           (default 20), from=\"end\" reads the most recent turns first (the \
                           session's final state; late conclusions supersede early ones), \
                           after_message_id / before_message_id continue from a page marker. \
                           The first page lists subagent sessions in a footer - pass a listed \
                           id back to open one. Not for bulk export - use `pond copy --to \
                           <file>`. Response format details: resource schema://pond.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_get_session(
            &self,
            Parameters(params): Parameters<McpGetSessionParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let first_page =
                params.after_message_id.is_none() && params.before_message_id.is_none();
            let request = GetSessionRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                id: params.id,
                limit: params.limit.unwrap_or(20),
                from: parse_session_from(params.from),
                after_message_id: params.after_message_id,
                before_message_id: params.before_message_id,
            };
            match run_get_session(&self.state.store, request).await {
                GetEnvelope::Success(response) => {
                    let mut transcript = crate::render::render_get_transcript(
                        &response,
                        crate::render::Surface::Mcp,
                    );
                    // Spawn-only subagents are stored as their own sessions
                    // (spec.md#datasets); surface them on the parent's first page
                    // so an agent can open each (otherwise they are undiscoverable
                    // from the MCP surface). Best-effort: a lookup failure just
                    // omits the footer rather than failing the get.
                    if first_page
                        && let Ok(children) =
                            self.state.store.child_sessions(&response.session.id).await
                        && !children.is_empty()
                    {
                        transcript.push_str(&crate::render::render_subagents_footer(
                            &children,
                            crate::render::Surface::Mcp,
                        ));
                    }
                    Ok(tool_result(transcript))
                }
                GetEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Expand one message with its full part bodies (tool_call / \
                           tool_result / reasoning / file), plus conversational neighbors \
                           for context. Pass a message_id from pond_search or a transcript \
                           line; context_before / context_after size the neighbor window \
                           (default 3, like grep -B/-A). For the whole session use \
                           pond_get_session. Response format details: resource schema://pond.",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_get_message(
            &self,
            Parameters(params): Parameters<McpGetMessageParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let request = GetMessageRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(default_namespace()),
                id: params.id,
                context_before: params.context_before.unwrap_or(3),
                context_after: params.context_after.unwrap_or(3),
            };
            match run_get_message(&self.state.store, request).await {
                GetEnvelope::Success(response) => Ok(tool_result(
                    crate::render::render_get_transcript(&response, crate::render::Surface::Mcp),
                )),
                GetEnvelope::Error(envelope) => Err(to_error_data(&envelope)),
            }
        }

        #[tool(
            description = "Advanced escape hatch: run ONE read-only SQL statement \
                           (SELECT/WITH, DataFusion / PostgreSQL-compatible) over the \
                           sessions / messages / parts tables. NOT for finding or reading \
                           conversations - pond_search and pond_get_session / \
                           pond_get_message cover almost all recall. \
                           Reach for SQL only for: aggregation (counts, group-by, joins, time \
                           buckets), exact strings or identifiers in conversational text \
                           (contains_tokens / fts), tool-call analytics and tool bodies, \
                           subagent sessions, bulk export (format=parquet|ndjson). Read \
                           resource schema://pond-sql FIRST - exact columns, indexed \
                           predicates, JSON access rules, worked examples; do not guess \
                           column names or JSON paths. Inline text output is row-capped \
                           and long cells clip with a +N chars marker (full values via \
                           format=parquet|ndjson); queries are wall-clock-capped (raise \
                           via timeout_seconds).",
            annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
        )]
        async fn pond_sql(
            &self,
            Parameters(params): Parameters<McpSqlParams>,
        ) -> Result<CallToolResult, ErrorData> {
            let mode = match params.format.as_deref() {
                None | Some("text") => sql::Mode::Inline,
                Some("parquet") => sql::Mode::Export(sql::Format::Parquet),
                Some("ndjson") => sql::Mode::Export(sql::Format::Ndjson),
                Some(other) => {
                    return Ok(CallToolResult::error(vec![Content::text(format!(
                        "unknown format {other:?}; use \"text\", \"parquet\", or \"ndjson\""
                    ))]));
                }
            };
            let inline_rows = sql::DEFAULT_INLINE_ROWS;

            // Open only the tables the query names (spec.md#search): the slow
            // `parts.lance` open is pure waste for the common messages-only
            // query. The referenced tables are independent (per-table
            // caches/mutexes), so overlap their freshness/manifest fetches.
            let store = &self.state.store;
            let query = params.query.as_str();
            let tables = match tokio::try_join!(
                async {
                    anyhow::Ok(match sql::mentions_table(query, "sessions") {
                        true => Some(store.dataset(Table::Sessions).await?),
                        false => None,
                    })
                },
                async {
                    anyhow::Ok(match sql::mentions_table(query, "messages") {
                        true => Some(store.dataset(Table::Messages).await?),
                        false => None,
                    })
                },
                async {
                    anyhow::Ok(match sql::mentions_table(query, "parts") {
                        true => Some(store.dataset(Table::Parts).await?),
                        false => None,
                    })
                },
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

            match sql::run(
                &tables,
                &params.query,
                mode,
                inline_rows,
                params.timeout_seconds,
            )
            .await
            {
                Ok(sql::Outcome::Inline(text)) => Ok(tool_result(text)),
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
            // rmcp's default `from_build_env` reports the rmcp crate (name +
            // version) - clients display the server's own identity, so set it.
            .with_server_info(Implementation::new("pond", env!("CARGO_PKG_VERSION")))
            // The one pond-authored text eagerly loaded into every connected
            // session (clients defer tool descriptions behind tool search),
            // so it carries ALL the routing; cookbook detail lives in the
            // schema:// resources. Wording is deliberate: "analyze / review /
            // summarize a session" must bind to pond_get_session, and
            // pond_sql must never read as the general "analyze" tool.
            .with_instructions(format!(
                "pond recalls past agent sessions (Claude Code and others) - prior work, \
                 decisions, and context across sessions, not the live conversation. Three \
                 tools cover almost every request: pond_search finds relevant messages, \
                 pond_get_session reads a whole session, pond_get_message expands one \
                 message with its full tool bodies; all return readable transcripts. To \
                 analyze, review, or summarize a past session: pond_get_session(id) - one \
                 call, the whole transcript (any id works: a message id resolves to its \
                 session). For \"what did we decide / latest state\" questions, early \
                 conclusions in a long session are often superseded later - read from the \
                 end (pond_get_session from=\"end\") or re-sort with pond_search \
                 sort_by=\"recency\"; relevance rank favors the early, confident, possibly \
                 overturned phrasing. To recover context lost to compaction: pond_search (a \
                 distinctive recent topic + project + from_date=today), then \
                 pond_get_session(id, from=\"end\"). pond_sql is the advanced escape hatch \
                 for what search+get cannot express: corpus-wide aggregation (counts, \
                 group-by, joins), exact strings or identifiers inside tool bodies, \
                 bulk export (parquet/ndjson) - read resource \
                 schema://pond-sql before writing SQL. Scope with filters (project, \
                 session_id, source_agent, from_date / to_date), and phrase queries with \
                 the distinctive words you expect in the conversation (error strings, \
                 symbol names, product names) - project names go in the project filter. \
                 source_agent is exact-or-subpath: a root \
                 value (\"openclaw\", \"claude-code\") returns that harness's main \
                 sessions; subagents are stored as their own sessions and excluded by \
                 default - name a subpath (\"claude-code/general-purpose\") to search \
                 them, or reach them via pond_sql (parent_session_id). Search covers only \
                 user/assistant conversational text by \
                 design (tool calls/results and reasoning are excluded as low-signal \
                 noise, not a bug) - a zero/weak result is not proof of absence: verify \
                 exact strings with pond_sql WHERE contains_tokens(search_text, '...') \
                 before concluding something never happened. Deeper reference: resources \
                 schema://pond (search/get params + response format), schema://pond-sql \
                 (SQL columns, functions, worked examples), stats://pond (corpus stats).{}",
                if crate::embed::embeddings_enabled() {
                    ""
                } else {
                    " Semantic (meaning-based) search is an opt-in on this instance - \
                     [embeddings].enabled in pond's config."
                },
            ))
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
                    schema_doc_for(crate::embed::embeddings_enabled()),
                    request.uri,
                )])),
                "schema://pond-sql" => Ok(ReadResourceResult::new(vec![ResourceContents::text(
                    SQL_SCHEMA_DOC,
                    request.uri,
                )])),
                // `pond_sql` export artifacts: read the file pond wrote
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
                    // Both embedding probes are data-page scans over `messages`,
                    // so an instance with embedding off skips the calls outright
                    // and nulls the embeddings keys. The searchable count is an
                    // FTS fact, not an embedding one: when disabled it comes
                    // from the FTS index's num_docs (index-resident; a fresh
                    // pre-index store falls back to a count scan) so the corpus
                    // block stays complete on the default posture.
                    let embedding_fut = async {
                        if crate::embed::embeddings_enabled() {
                            let (progress, stale) = tokio::try_join!(
                                store.embedding_progress(),
                                store.stale_embedding_count(),
                            )?;
                            let searchable = progress.total;
                            anyhow::Ok((Some((progress, stale)), searchable))
                        } else {
                            let searchable = store.searchable_message_count().await?;
                            anyhow::Ok((None, searchable))
                        }
                    };
                    let ((sessions, messages, parts), (embedding, searchable), indices) =
                        tokio::try_join!(store.row_counts(), embedding_fut, store.index_status())
                            .map_err(map_err)?;

                    let embedded_percent = embedding.as_ref().map(|(progress, _)| {
                        if progress.total == 0 {
                            0.0
                        } else {
                            #[allow(clippy::cast_precision_loss)]
                            let pct = (progress.embedded as f64 / progress.total as f64) * 100.0;
                            (pct * 10.0).round() / 10.0
                        }
                    });
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
                            "searchable_messages": searchable,
                            "parts": parts,
                        },
                        "embeddings": {
                            "model": embedding.as_ref().map(|(p, _)| p.model),
                            "embedded": embedding.as_ref().map(|(p, _)| p.embedded),
                            "searchable_total": embedding.as_ref().map(|(p, _)| p.total),
                            "embedded_percent": embedded_percent,
                            "stale_under_other_model": embedding.as_ref().map(|(_, s)| *s),
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
            annotate_search_mode_with(&mut result, crate::embed::embeddings_enabled());
            Ok(result)
        }
    }

    /// `pond_search` description with the whole `mode=` sentence removed - what
    /// an instance with embedding off advertises.
    const SEARCH_DESCRIPTION_FTS_ONLY: &str = "Find relevant messages in past sessions - the \
        entry point for recall: \"have we worked on X\", \"what did we decide about Y\", \"find \
        the session where...\". Scope with project / session_id / source_agent / from_date / \
        to_date; put the distinctive words you expect in the conversation in the query (error \
        strings, symbol names, product names); project names go in the project filter. Returns \
        scored hits grouped by session, best session first; pass a hit's session_id to \
        pond_get_session or its message_id to pond_get_message to read it. Searches \
        conversational text only (tool calls/results and reasoning are excluded by design - a \
        gap there is expected, not a failure) and excludes subagent sessions; reach both via \
        pond_sql. Response format details: resource schema://pond.";

    /// `mode` property description on an instance with embedding off.
    const MODE_DOC_FTS_ONLY: &str =
        "\"fts\" (default and only arm on this instance): exact whole words via BM25.";

    /// True for the `mode=vector` refusal (`resolve_effective_mode`), the one
    /// search error the caller can fix in-turn.
    fn is_semantic_disabled(envelope: &ErrorEnvelope) -> bool {
        envelope
            .error
            .details
            .get("config_key")
            .and_then(serde_json::Value::as_str)
            == Some(crate::handlers::SEMANTIC_DISABLED_CONFIG_KEY)
    }

    /// With embedding off, the `pond_search` surface must not invite a vector
    /// query: the static attribute text names both arms, so rewrite the tool
    /// description and the `mode` property description at list time. The wire
    /// contract is untouched - the refusal in `resolve_effective_mode` handles
    /// an explicit attempt.
    fn annotate_search_mode_with(result: &mut ListToolsResult, embeddings_enabled: bool) {
        if embeddings_enabled {
            return;
        }
        for tool in result
            .tools
            .iter_mut()
            .filter(|tool| tool.name == "pond_search")
        {
            tool.description = Some(SEARCH_DESCRIPTION_FTS_ONLY.into());
            if let Some(mode) = Arc::make_mut(&mut tool.input_schema)
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
                .and_then(|properties| properties.get_mut("mode"))
                .and_then(serde_json::Value::as_object_mut)
            {
                mode.insert(
                    "description".to_owned(),
                    serde_json::Value::String(MODE_DOC_FTS_ONLY.to_owned()),
                );
            }
        }
    }

    fn annotate_tool_limits(result: &mut ListToolsResult) {
        for tool in &mut result.tools {
            let chars = match tool.name.as_ref() {
                "pond_search" => 80_000,
                "pond_get_session" | "pond_get_message" => 200_000,
                "pond_sql" => 80_000,
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

    /// Build the `pond_sql` export result: a text summary plus a
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
        use crate::wire::{ErrorBody, ErrorCode};

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
        fn search_params_deserialize_source_agent_and_map_to_filters() {
            let params: McpSearchParams = serde_json::from_value(serde_json::json!({
                "query": "vector store",
                "source_agent": "openclaw/subagent",
            }))
            .expect("params deserialize");
            assert_eq!(params.source_agent.as_deref(), Some("openclaw/subagent"));

            // Mirror the pond_search mapping: the param flows verbatim into the
            // wire filter that the handler translates to a scope predicate.
            let filters = crate::wire::SearchFilters {
                source_agent: params.source_agent,
                ..Default::default()
            };
            assert_eq!(filters.source_agent.as_deref(), Some("openclaw/subagent"));
        }

        #[test]
        fn annotate_tool_limits_sets_anthropic_meta() {
            let schema = Arc::new(serde_json::Map::new());
            let mut result = ListToolsResult {
                tools: vec![
                    Tool::new("pond_search", "Search", Arc::clone(&schema)),
                    Tool::new("pond_get_session", "Get session", Arc::clone(&schema)),
                    Tool::new("pond_get_message", "Get message", Arc::clone(&schema)),
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
            assert_eq!(value("pond_get_session"), Some(200_000));
            assert_eq!(value("pond_get_message"), Some(200_000));
        }

        #[test]
        fn parse_search_mode_defaults_to_fts() {
            assert_eq!(parse_search_mode(None), Some(SearchModeWire::Fts));
            assert_eq!(parse_search_mode(Some("fts")), Some(SearchModeWire::Fts));
            assert_eq!(
                parse_search_mode(Some("vector")),
                Some(SearchModeWire::Vector)
            );
            assert_eq!(parse_search_mode(Some("hybrid")), None);
        }

        /// Only the refusal takes the in-turn tool-error branch; every other
        /// validation failure stays a JSON-RPC error.
        #[test]
        fn semantic_disabled_predicate_matches_only_the_refusal() {
            let envelope = |details: serde_json::Value| ErrorEnvelope {
                error: ErrorBody {
                    code: ErrorCode::ValidationFailed,
                    message: "boom".to_owned(),
                    details,
                },
            };
            assert!(is_semantic_disabled(&envelope(serde_json::json!({
                "field": "mode",
                "config_key": "embeddings.enabled",
                "retryable": false,
            }))));
            assert!(!is_semantic_disabled(&envelope(
                serde_json::json!({ "field": "query" })
            )));
            assert!(!is_semantic_disabled(&envelope(serde_json::json!({
                "config_key": "search.nprobes"
            }))));
        }

        fn listed_tools() -> ListToolsResult {
            ListToolsResult {
                tools: PondMcp::tool_router().list_all(),
                next_cursor: None,
                meta: None,
            }
        }

        fn search_tool(result: &ListToolsResult) -> &Tool {
            result
                .tools
                .iter()
                .find(|tool| tool.name == "pond_search")
                .expect("pond_search is registered")
        }

        fn mode_description(tool: &Tool) -> &str {
            tool.input_schema["properties"]["mode"]["description"]
                .as_str()
                .expect("the mode property carries a description")
        }

        #[test]
        fn search_surface_hides_the_vector_arm_when_embeddings_are_disabled() {
            let mut disabled = listed_tools();
            annotate_search_mode_with(&mut disabled, false);
            let tool = search_tool(&disabled);
            let description = tool.description.as_deref().unwrap_or_default();
            assert!(
                !description.contains("vector"),
                "tool description must not offer an arm this instance refuses: {description}"
            );
            let mode = mode_description(tool);
            assert!(
                !mode.contains("vector"),
                "mode description must not offer the vector arm: {mode}"
            );
            // Only prose is rewritten - the wire contract still accepts both
            // arms, and the refusal is what answers an explicit attempt.
            assert_eq!(
                tool.input_schema["properties"]["mode"]["type"],
                serde_json::json!(["string", "null"]),
            );

            let mut enabled = listed_tools();
            annotate_search_mode_with(&mut enabled, true);
            let tool = search_tool(&enabled);
            assert!(
                tool.description
                    .as_deref()
                    .unwrap_or_default()
                    .contains("vector"),
                "an enabled instance advertises both arms"
            );
            assert!(mode_description(tool).contains("vector"));
        }

        /// The rewrite needles live in prose, so a reworded `SCHEMA_DOC` would
        /// make every `String::replace` silently no-op (the `debug_assert`s in
        /// `schema_doc_for` never run in release) - this is the drift guard.
        #[test]
        fn schema_doc_hides_the_vector_arm_when_embeddings_are_disabled() {
            let disabled = schema_doc_for(false);
            assert_ne!(disabled, SCHEMA_DOC, "no rewrite landed");
            assert!(
                !disabled.contains("vector"),
                "schema doc must not offer an arm this instance refuses: {disabled}"
            );
            assert_eq!(schema_doc_for(true), SCHEMA_DOC);
        }
    }
}
