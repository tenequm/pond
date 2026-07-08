//! `pond_sql`: read-only DataFusion SQL over the three Lance tables
//! (`sessions` / `messages` / `parts`), registered as `LanceTableProvider`s
//! (behind plan-time views that rename `id` to `message_id` / `session_id`)
//! on a fresh per-call `SessionContext`. Read-only is enforced in two layers - a
//! single-`SELECT` pre-parse and `sql_with_options` with DDL/DML/statements all
//! disabled - so no statement that mutates the corpus or touches the filesystem
//! (INSERT/UPDATE/DELETE/CREATE/DROP/COPY/CREATE EXTERNAL TABLE/SET) can run.
//! Results render inline (row-capped) or export to a parquet/ndjson file the
//! caller fetches via the `pond-sql-export://` resource (`src/transport.rs`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arrow_json::LineDelimitedWriter;
use lance::Dataset;
use lance::datafusion::LanceTableProvider;
use lance::deps::arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use lance::deps::arrow_array::{
    Array, ArrayRef, GenericStringArray, LargeBinaryArray, OffsetSizeTrait, RecordBatch,
    StringArray, StringViewArray,
};
use lance::deps::arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use lance::deps::datafusion::arrow::util::pretty::pretty_format_batches;
use lance::deps::datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use lance::deps::datafusion::common::ScalarValue;
use lance::deps::datafusion::datasource::{ViewTable, provider_as_source};
use lance::deps::datafusion::error::DataFusionError;
use lance::deps::datafusion::execution::SessionStateBuilder;
use lance::deps::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use lance::deps::datafusion::logical_expr::{
    ColumnarValue, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use lance::deps::datafusion::logical_expr::{Expr, TableType};
use lance::deps::datafusion::physical_plan::ExecutionPlan;
use lance::deps::datafusion::prelude::{SQLOptions, SessionConfig, SessionContext, col};
use lance::deps::datafusion::sql::parser::{DFParser, Statement as DfStatement};
use lance::deps::datafusion::sql::sqlparser::ast::{SetExpr, Statement as SqlStatement};
use lance_arrow::SchemaExt;
use lance_datafusion::udf::register_functions;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::parser::from_json;
use parquet::arrow::ArrowWriter;

/// Per-query memory ceiling for the DataFusion runtime. Not enforced on every
/// operator (datafusion caveat), so the timeout below is the hard backstop.
const MEM_LIMIT_BYTES: usize = 512 * 1024 * 1024;
/// Wall-clock cap on `collect()`. DataFusion 53 has no built-in query timeout,
/// so this `tokio::time::timeout` is the only guard against a runaway plan.
/// Callers may raise it per query up to [`MAX_QUERY_TIMEOUT_SECS`].
pub const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;
/// Ceiling on the caller-supplied timeout: `collect()` is cancellable only by
/// this timeout, so an absurd value must not pin the server on a runaway scan.
pub const MAX_QUERY_TIMEOUT_SECS: u64 = 600;

fn effective_timeout(timeout_secs: Option<u64>) -> Duration {
    Duration::from_secs(
        timeout_secs
            .unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS)
            .clamp(1, MAX_QUERY_TIMEOUT_SECS),
    )
}
/// Byte budget for the inline (rendered table) result; rows are dropped to fit.
const INLINE_BUDGET_BYTES: usize = 80_000;
/// Hard ceiling on an export artifact: base64'd over `resources/read` it costs
/// ~1.33x this in the response, so keep it well under any process envelope.
const MAX_EXPORT_BYTES: usize = 100 * 1024 * 1024;
/// Default inline row cap when the caller passes no `limit`.
pub const DEFAULT_INLINE_ROWS: usize = 100;
/// Upper bound on the caller-supplied inline `limit`.
pub const MAX_INLINE_ROWS: usize = 1_000;

/// Export serialization format. Vector columns are excluded and JSON columns
/// are decoded to text before encoding (see [`displayable`]).
#[derive(Debug, Clone, Copy)]
pub enum Format {
    Parquet,
    Ndjson,
}

impl Format {
    pub fn ext(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::Ndjson => "ndjson",
        }
    }

    pub fn mime(self) -> &'static str {
        match self {
            Self::Parquet => "application/vnd.apache.parquet",
            Self::Ndjson => "application/x-ndjson",
        }
    }
}

/// How `pond_sql` returns results.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Render a row-capped table into the tool result.
    Inline,
    /// Write the full result to a file and return a `pond-sql-export://` link.
    Export(Format),
}

/// The Lance datasets a query references, fetched fresh per call so each query
/// sees a current snapshot (the handle freshness gate runs on each
/// `Store::dataset`). A field is `None` when the query never names that table -
/// the caller skips opening it, avoiding the slow `parts.lance` open on the
/// common messages-only query (spec.md#search). See [`mentions_table`].
pub struct Tables {
    pub sessions: Option<Arc<Dataset>>,
    pub messages: Option<Arc<Dataset>>,
    pub parts: Option<Arc<Dataset>>,
}

/// Whether `sql` references the table named `table`. A DataFusion query can
/// only reach a registered table by writing its name literally - no alias hides
/// the base name - so this lowercase word-boundary scan never yields a false
/// negative. At worst it matches the name inside a string or column literal and
/// opens a table the query won't touch: a cheap, safe false positive. Lets the
/// caller open only the datasets a query needs.
pub fn mentions_table(sql: &str, table: &str) -> bool {
    sql.to_ascii_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|token| token == table)
}

/// Result of a successful `run`.
pub enum Outcome {
    /// A rendered, row-capped table (already includes the metrics footer).
    Inline(String),
    /// Encoded export bytes plus metadata for the caller's summary/resource.
    Export {
        bytes: Vec<u8>,
        format: Format,
        rows: usize,
        columns: Vec<String>,
    },
}

/// Two error channels: `Query` is caller-fixable (parse/plan/exec/limits) and
/// the tool surfaces it as an `isError` result so the model self-corrects;
/// `Infra` is an internal failure surfaced as a protocol error.
#[derive(Debug)]
pub enum SqlError {
    Query(String),
    Infra(anyhow::Error),
}

fn infra(error: ArrowError) -> SqlError {
    SqlError::Infra(anyhow::Error::new(error))
}

/// Execute one read-only SQL query and return either a rendered table, a JSON
/// payload, or encoded export bytes.
pub async fn run(
    tables: &Tables,
    sql: &str,
    mode: Mode,
    inline_rows: usize,
    timeout_secs: Option<u64>,
) -> Result<Outcome, SqlError> {
    let parsed = parse_and_gate(sql)?;
    if matches!(parsed.kind, StatementKind::Explain) && matches!(mode, Mode::Export(_)) {
        return Err(SqlError::Query(
            "EXPLAIN returns a plan, not a result set; use format=text (or json) to read it"
                .to_owned(),
        ));
    }
    if projection_mentions_vector(parsed.projection_query()) {
        return Err(SqlError::Query(
            "the `vector` column is not selectable from pond_sql (it is a \
             FixedSizeList<f32> embedding, ~600 bytes per row and not useful in a result). \
             For semantic search use pond_search. Filtering on it is allowed in WHERE \
             (e.g. `vector IS NOT NULL`)."
                .to_owned(),
        ));
    }
    if jsonb_cast_misuse(sql) {
        return Err(SqlError::Query(
            "CAST / `::` does not work on the binary JSONB columns (variant_data, options) - \
             when the bytes happen to be valid text it can even silently return garbage. \
             Stringify the whole value with json_extract(col, '$') or read one field with \
             json_extract(col, '$.field')."
                .to_owned(),
        ));
    }
    if jsonb_fulldoc_like_scan(sql) {
        return Err(SqlError::Query(
            "a leading-wildcard LIKE over the whole JSONB document - \
             json_extract(variant_data, '$') LIKE '%...%' - stringifies and scans every row, \
             so over parts it will not finish within the time limit. There is no substring \
             index on tool bodies yet (TODO #47: lance v8 FM-Index). Instead match a single \
             field with json_extract(variant_data, '$.field') LIKE '...', scope to one session \
             with session_id = '<id>' and read it with pond_get, or search conversational text \
             with contains_tokens(search_text, '...')."
                .to_owned(),
        ));
    }
    let ctx = build_context()?;
    register(&ctx, tables)?;

    // Defense in depth on top of the pre-parse gate: SQLOptions blocks DDL/DML
    // at planning time. `allow_statements` stays false for a plain SELECT (the
    // parse-time gate already rejects SET/SHOW etc.) but must be true for
    // EXPLAIN, which DataFusion classifies as a Statement node. The inner
    // query of an EXPLAIN was vetted by the gate above.
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(matches!(parsed.kind, StatementKind::Explain));
    let df = ctx
        .sql_with_options(sql, options)
        .await
        .map_err(|error| SqlError::Query(enrich(&format!("SQL error: {error}"))))?;

    // Captured before `collect()` consumes `df`, so an empty result still
    // renders its column headers.
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let started = Instant::now();
    // TODO(#47): substring hunts inside parts.variant_data (json_extract +
    // LIKE full scans) are the dominant real-world cause of this timeout. The
    // planned fix is lance v8's FM-Index on variant_data (raw-byte substring
    // search via `contains(variant_data, 'needle')`); until it lands, the
    // message steers agents to predicates the current indexes can serve.
    let timeout = effective_timeout(timeout_secs);
    let collected = tokio::time::timeout(timeout, df.collect())
        .await
        .map_err(|_| {
            SqlError::Query(format!(
                "query exceeded the {}s limit; add a narrower WHERE or a LIMIT, or raise \
                 the per-query timeout (`timeout_seconds` on pond_sql, `--timeout` \
                 on pond sql; max {MAX_QUERY_TIMEOUT_SECS}s) if it legitimately needs \
                 longer. On a remote object store, queries over parts cost seconds per \
                 round-trip - scope by session_id / tool_name, and to reconstruct one \
                 session use pond_get(session_id), not SQL. For tool analytics use the \
                 narrow native columns (tool_name, \
                 call_id, is_failure) instead of json_get_* over variant_data. If you were \
                 substring-scanning variant_data (json_extract + LIKE), there is no \
                 substring index on tool bodies yet: filter parts by type and tool_name \
                 first, or search conversational text with \
                 contains_tokens(search_text, '...') instead.",
                timeout.as_secs()
            ))
        })?
        .map_err(|error| SqlError::Query(enrich(&format!("SQL error: {error}"))))?;
    let elapsed = started.elapsed();

    let display: Vec<RecordBatch> = if collected.is_empty() {
        vec![displayable(&RecordBatch::new_empty(result_schema)).map_err(infra)?]
    } else {
        collected
            .into_iter()
            .map(|batch| displayable(&batch))
            .collect::<Result<_, _>>()
            .map_err(infra)?
    };

    match mode {
        Mode::Inline => Ok(Outcome::Inline(
            render_inline(&display, inline_rows, elapsed).map_err(infra)?,
        )),
        Mode::Export(format) => {
            let rows = display.iter().map(RecordBatch::num_rows).sum();
            let columns = display
                .first()
                .map(|batch| {
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let bytes = match format {
                Format::Parquet => encode_parquet(&display)?,
                Format::Ndjson => encode_ndjson(&display)?,
            };
            if bytes.len() > MAX_EXPORT_BYTES {
                return Err(SqlError::Query(format!(
                    "export is {} bytes, over the {MAX_EXPORT_BYTES} byte limit; \
                     narrow the query or aggregate",
                    bytes.len()
                )));
            }
            Ok(Outcome::Export {
                bytes,
                format,
                rows,
                columns,
            })
        }
    }
}

/// Top-level statement shape allowed past the read-only gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatementKind {
    /// A plain `Query` (SELECT/WITH/VALUES/UNION).
    Query,
    /// `EXPLAIN [ANALYZE] <query>` - planning info only, no mutation.
    Explain,
}

/// Parsed top-level statement, normalized so downstream checks always see a
/// projection-bearing `Query` regardless of whether the user wrote `SELECT`
/// or `EXPLAIN SELECT`. DataFusion's parser wraps EXPLAIN in its own
/// `DfStatement::Explain` variant (separate from sqlparser's
/// `SqlStatement::Explain`), so the gate has to peel both layers.
struct ParsedStatement {
    kind: StatementKind,
    query: lance::deps::datafusion::sql::sqlparser::ast::Query,
}

impl ParsedStatement {
    fn projection_query(&self) -> &lance::deps::datafusion::sql::sqlparser::ast::Query {
        &self.query
    }
}

/// Read-only gate: parse the SQL and require exactly one top-level `Query` or
/// `EXPLAIN <Query>`. Rejects DDL/DML/COPY/SET/SHOW and multi-statement input,
/// which `SQLOptions` alone does not catch at planning time. EXPLAIN of a
/// non-Query (e.g. `EXPLAIN INSERT ...`) is also rejected: EXPLAIN itself is
/// read-only, but letting the inner shape be DDL/DML widens the surface area
/// the gate has to reason about for no real agent gain.
fn parse_and_gate(sql: &str) -> Result<ParsedStatement, SqlError> {
    let statements = DFParser::parse_sql(sql)
        .map_err(|error| SqlError::Query(format!("SQL parse error: {error}")))?;
    if statements.len() != 1 {
        return Err(SqlError::Query(
            "pond_sql runs exactly one statement; submit a single SELECT".to_owned(),
        ));
    }
    let Some(front) = statements.front() else {
        return Err(read_only_rejection());
    };
    match front {
        DfStatement::Statement(boxed) => match boxed.as_ref() {
            SqlStatement::Query(query) => Ok(ParsedStatement {
                kind: StatementKind::Query,
                query: query.as_ref().clone(),
            }),
            _ => Err(read_only_rejection()),
        },
        DfStatement::Explain(explain) => match explain.statement.as_ref() {
            DfStatement::Statement(inner) => match inner.as_ref() {
                SqlStatement::Query(query) => Ok(ParsedStatement {
                    kind: StatementKind::Explain,
                    query: query.as_ref().clone(),
                }),
                _ => Err(read_only_rejection()),
            },
            _ => Err(read_only_rejection()),
        },
        _ => Err(read_only_rejection()),
    }
}

fn read_only_rejection() -> SqlError {
    // Surface-neutral wording: this message reaches both the pond_sql
    // MCP tool and the `pond sql` CLI, so it names neither.
    SqlError::Query(
        "pond's SQL surface is read-only: only a single SELECT/WITH (or EXPLAIN of one) is \
         allowed (no INSERT/UPDATE/DELETE/CREATE/DROP/COPY/SET)"
            .to_owned(),
    )
}

/// Reject any top-level projection that explicitly references the embedding
/// `vector` column. Today such queries silently return an empty column (the
/// FixedSizeList<f32> is stripped by `displayable`), which wastes agent tokens
/// diagnosing. WHERE/HAVING references stay legal - the doc lets agents filter
/// on it (e.g. `WHERE vector IS NOT NULL`); only projecting the column out is
/// blocked. Heuristic: tokenize each top-level SELECT item and look for a bare
/// `vector` identifier. Covers `SELECT vector`, `SELECT id, vector`,
/// `SELECT m.vector`, and `SELECT array_length(vector)`. Wildcards (`*` /
/// `messages.*`) keep the existing silent-strip behavior since they don't name
/// the column explicitly.
fn projection_mentions_vector(query: &lance::deps::datafusion::sql::sqlparser::ast::Query) -> bool {
    walk_set_expr_for_vector(query.body.as_ref())
}

fn walk_set_expr_for_vector(expr: &SetExpr) -> bool {
    match expr {
        SetExpr::Select(select) => select
            .projection
            .iter()
            .any(|item| mentions_vector_token(&item.to_string())),
        SetExpr::Query(inner) => walk_set_expr_for_vector(inner.body.as_ref()),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr_for_vector(left) || walk_set_expr_for_vector(right)
        }
        _ => false,
    }
}

fn mentions_vector_token(text: &str) -> bool {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|token| token == "vector")
}

/// Plan-time gate for CAST / `::` on the binary JSONB columns. The runtime
/// failure is data-dependent (CAST only errors when a non-UTF8 byte is hit;
/// JSONB header bytes are often valid ASCII, so it can silently "succeed" and
/// return binary garbage), so reject before scanning. Token-scan heuristic in
/// the spirit of `projection_mentions_vector`; an aliased column that slips
/// through still hits the `enrich` runtime hint.
fn jsonb_cast_misuse(sql: &str) -> bool {
    const JSONB_COLUMNS: [&str; 2] = ["variant_data", "options"];
    let lowered = sql.to_ascii_lowercase();
    let bytes = lowered.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    // `<col> :: <type>`
    for column in JSONB_COLUMNS {
        let mut start = 0;
        while let Some(pos) = lowered[start..].find(column) {
            let begin = start + pos;
            let end = begin + column.len();
            start = end;
            let bounded = (begin == 0 || !is_ident(bytes[begin - 1]))
                && (end == bytes.len() || !is_ident(bytes[end]));
            if bounded && lowered[end..].trim_start().starts_with("::") {
                return true;
            }
        }
    }

    // `CAST(<qualifier.>col AS <type>`
    let mut start = 0;
    while let Some(pos) = lowered[start..].find("cast") {
        let begin = start + pos;
        start = begin + 4;
        if begin > 0 && is_ident(bytes[begin - 1]) {
            continue;
        }
        let Some(open) = lowered[begin + 4..].trim_start().strip_prefix('(') else {
            continue;
        };
        let mut operand = open.trim_start();
        if let Some(dot) = operand.find('.')
            && dot > 0
            && operand.as_bytes()[..dot].iter().all(|b| is_ident(*b))
        {
            operand = &operand[dot + 1..];
        }
        for column in JSONB_COLUMNS {
            if let Some(after) = operand.strip_prefix(column)
                && !after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_')
                && after
                    .trim_start()
                    .strip_prefix("as")
                    .is_some_and(|rest| rest.starts_with(char::is_whitespace))
            {
                return true;
            }
        }
    }
    false
}

/// Plan-time gate for the one substring shape that reliably exhausts the
/// wall-clock cap: a leading-wildcard LIKE/ILIKE over the *whole-document*
/// stringify of a binary JSONB column - `json_extract(variant_data|options,
/// '$') LIKE '%...%'`. That materializes every row's entire JSONB blob just to
/// substring-scan it, and the leading `%` defeats every index; over parts
/// (>1M rows) it does not finish, even scoped to a day. A single-field extract
/// (`'$.name'`) or any non-leading pattern is left to run - only the
/// whole-document murder shape is rejected, so the agent gets the indexed path
/// in milliseconds instead of a timeout. Token-scan heuristic in the spirit of
/// `jsonb_cast_misuse`; the timeout message remains the backstop for anything
/// that slips through.
/// TODO(#47): lance v8's FM-Index gives raw-byte substring search
/// (`contains(variant_data, 'needle')`); retire this gate once it lands.
fn jsonb_fulldoc_like_scan(sql: &str) -> bool {
    const JSONB_COLUMNS: [&str; 2] = ["variant_data", "options"];
    const NEEDLE: &str = "json_extract";
    let lowered = sql.to_ascii_lowercase();
    let bytes = lowered.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';

    let mut start = 0;
    while let Some(pos) = lowered[start..].find(NEEDLE) {
        let begin = start + pos;
        start = begin + NEEDLE.len();
        if begin > 0 && is_ident(bytes[begin - 1]) {
            continue;
        }
        let Some(rest) = lowered[start..].trim_start().strip_prefix('(') else {
            continue;
        };
        let mut operand = rest.trim_start();
        // optional `qualifier.`
        if let Some(dot) = operand.find('.')
            && dot > 0
            && operand.as_bytes()[..dot].iter().all(|b| is_ident(*b))
        {
            operand = &operand[dot + 1..];
        }
        let Some(col) = JSONB_COLUMNS.into_iter().find(|c| operand.starts_with(c)) else {
            continue;
        };
        // Require the whole-document path `, '$' )` exactly - a single-field
        // extract (`'$.name'`) is fine and must keep running.
        let tail = operand[col.len()..].trim_start();
        let Some(tail) = tail
            .strip_prefix(',')
            .map(str::trim_start)
            .and_then(|t| t.strip_prefix("'$'"))
            .map(str::trim_start)
            .and_then(|t| t.strip_prefix(')'))
        else {
            continue;
        };
        // Step past any wrapper close-parens (`lower(...)`/`upper(...)`).
        let mut tail = tail.trim_start();
        while let Some(next) = tail.strip_prefix(')') {
            tail = next.trim_start();
        }
        if let Some(next) = tail.strip_prefix("not")
            && next.starts_with(char::is_whitespace)
        {
            tail = next.trim_start();
        }
        for op in ["like", "ilike"] {
            if let Some(next) = tail.strip_prefix(op)
                && next.starts_with(char::is_whitespace)
                && next.trim_start().starts_with("'%")
            {
                return true;
            }
        }
    }
    false
}

fn build_context() -> Result<SessionContext, SqlError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(MEM_LIMIT_BYTES, 1.0)
        .build_arc()
        .map_err(|error| SqlError::Infra(anyhow!("datafusion runtime init failed: {error}")))?;
    // information_schema is the standard self-discovery path (SELECT ... FROM
    // information_schema.columns); agents reach for it before any doc.
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_information_schema(true))
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    Ok(SessionContext::new_with_state(state))
}

/// Plan-time key renames: each table's storage `id` is exposed under a
/// self-describing name so the same value never changes name between tables -
/// agents copy column names across queries. One source drives both the
/// registered views and fts() output so they cannot diverge.
fn renamed_key(table: &str) -> Option<&'static str> {
    match table {
        "messages" => Some("message_id"),
        "sessions" => Some("session_id"),
        _ => None,
    }
}

fn register(ctx: &SessionContext, tables: &Tables) -> Result<(), SqlError> {
    for (name, dataset) in [
        ("sessions", &tables.sessions),
        ("messages", &tables.messages),
    ] {
        let Some(dataset) = dataset else { continue };
        // LanceTableProvider (not the bare Dataset impl) so WHERE/projection/
        // limit push into Lance's indexed scan; (false, false) hides _rowid /
        // _rowaddr from the SQL schema. The view applies `renamed_key`
        // plan-time only; storage keeps `id`.
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        let key = renamed_key(name).unwrap_or("id");
        let view = renamed_view(name, Arc::new(provider), "id", key)
            .map_err(|error| SqlError::Infra(anyhow!("build {name} view: {error}")))?;
        ctx.register_table(name, Arc::new(view))
            .map_err(|error| SqlError::Infra(anyhow!("register table {name}: {error}")))?;
    }
    // `parts` hides the `data` blob column behind a projecting view: blob
    // columns scan as `{position, size}` descriptor structs, so any SQL touch
    // dies in the planner with an opaque CAST error. The view inlines at plan
    // time - filters still push into the Lance scan underneath.
    if let Some(parts) = &tables.parts {
        let provider = LanceTableProvider::new(parts.clone(), false, false);
        let keep: Vec<_> = parts
            .schema()
            .fields
            .iter()
            .filter(|field| field.name != "data")
            .map(|field| col(field.name.as_str()))
            .collect();
        let plan = LogicalPlanBuilder::scan("parts", provider_as_source(Arc::new(provider)), None)
            .and_then(|builder| builder.project(keep))
            .and_then(LogicalPlanBuilder::build)
            .map_err(|error| SqlError::Infra(anyhow!("build parts view: {error}")))?;
        ctx.register_table("parts", Arc::new(ViewTable::new(plan, None)))
            .map_err(|error| SqlError::Infra(anyhow!("register table parts: {error}")))?;
    }
    // `fts('messages', '{...}')` BM25 search-in-SQL (vendored provider with a
    // declared `_score` column - see `ScoredFtsUdtf`), and lance's JSON /
    // contains_tokens UDFs for filtering inside the JSON columns. Only the
    // referenced tables are present, matching the registered views above.
    let datasets = [
        ("sessions", &tables.sessions),
        ("messages", &tables.messages),
        ("parts", &tables.parts),
    ]
    .into_iter()
    .filter_map(|(name, dataset)| dataset.clone().map(|d| (name.to_owned(), d)))
    .collect();
    let fts = ScoredFtsUdtf { datasets };
    ctx.register_udtf("fts", Arc::new(fts));
    register_functions(ctx);
    // Shadow lance's strict json_get_* by name: the strict versions abort the
    // whole scan when any row's field is non-scalar (e.g. tool_result `result`
    // arrays), turning one polymorphic value into a dead query.
    for udf in lenient_json_udfs() {
        ctx.register_udf(udf);
    }
    // `any_value` (Postgres 16 / DuckDB / BigQuery - agents reach for it)
    // doesn't exist in DataFusion 53; alias first_value, which satisfies the
    // same contract (any_value promises no ordering, so first-encountered is
    // a valid answer). register_udaf indexes aliases.
    if let Some(first_value) = ctx.state().aggregate_functions().get("first_value") {
        ctx.register_udaf(first_value.as_ref().clone().with_aliases(["any_value"]));
    }
    // `fts` as a *scalar* exists only to fail at plan time with the correction:
    // agents pattern-match FTS into WHERE (MySQL MATCH / Postgres @@ priors)
    // and DataFusion's stock error is "Did you mean 'cos'?". Scalar and
    // table-function registries are separate namespaces, so the real fts()
    // UDTF in FROM position is unaffected.
    ctx.register_udf(ScalarUDF::new_from_impl(FtsMisuse::new()));
    Ok(())
}

/// Wrap `provider` in a view projecting every column, with `from` renamed to
/// `to`. The view inlines at plan time, so filters and projections still push
/// into the underlying Lance scan.
fn renamed_view(
    scan_name: &str,
    provider: Arc<dyn TableProvider>,
    from: &str,
    to: &str,
) -> Result<ViewTable, DataFusionError> {
    let projection: Vec<_> = provider
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let column = col(field.name().as_str());
            if field.name() == from {
                column.alias(to)
            } else {
                column
            }
        })
        .collect();
    let plan = LogicalPlanBuilder::scan(scan_name, provider_as_source(provider), None)?
        .project(projection)?
        .build()?;
    Ok(ViewTable::new(plan, None))
}

const FTS_MISUSE: &str = "fts is a table function and goes in FROM, not in WHERE or the \
    projection. For filtering use WHERE contains_tokens(search_text, 'word1 word2') (all \
    words must match; index-accelerated). For ranked results: SELECT m.message_id, f._score \
    FROM fts('messages', '{\"match\":{\"column\":\"search_text\",\"terms\":\"...\"}}') f \
    JOIN messages m ON m.message_id = f.message_id ORDER BY f._score DESC.";

/// See the registration comment: a plan-time teaching error for `WHERE fts(...)`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct FtsMisuse {
    signature: Signature,
}

impl FtsMisuse {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FtsMisuse {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "fts"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Err(DataFusionError::Plan(FTS_MISUSE.to_owned()))
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        Err(DataFusionError::Plan(FTS_MISUSE.to_owned()))
    }
}

/// Vendored replacement for lance's `FtsQueryUDTF` (lance-7.0.0
/// src/dataset/udtf.rs). The upstream provider omits `_score` from its
/// declared schema while leaving the scanner's scoring autoprojection on, so
/// `_score` is physically appended but logically unknown: naming it in SQL
/// fails ("No field named _score") and any aggregate over fts() dies on
/// DataFusion's physical-vs-logical schema check (COUNT plans 0 columns,
/// receives 1). This provider declares `_score` as a regular nullable Float32
/// column, projects it explicitly, and disables the autoprojection - which is
/// also lance's documented intended end state for score columns
/// (scanner.rs "_score/_distance should become regular output columns").
/// Delete once fixed upstream.
#[derive(Debug)]
struct ScoredFtsUdtf {
    datasets: HashMap<String, Arc<Dataset>>,
}

impl TableFunctionImpl for ScoredFtsUdtf {
    fn call(
        &self,
        expr: &[Expr],
    ) -> Result<Arc<dyn TableProvider>, lance::deps::datafusion::error::DataFusionError> {
        let [table_expr, query_expr] = expr else {
            return Err(DataFusionError::Execution(
                "fts() takes (table_name, fts_query_json)".to_owned(),
            ));
        };
        let Expr::Literal(ScalarValue::Utf8(Some(table_name)), _) = table_expr else {
            return Err(DataFusionError::Execution(
                "fts() first argument must be a table name string".to_owned(),
            ));
        };
        let Expr::Literal(ScalarValue::Utf8(Some(fts_query)), _) = query_expr else {
            return Err(DataFusionError::Execution(
                "fts() second argument must be the fts query as a JSON string".to_owned(),
            ));
        };
        let dataset = self.datasets.get(table_name).ok_or_else(|| {
            DataFusionError::Execution(format!("fts(): table {table_name} not found"))
        })?;
        let mut full_schema = Schema::from(dataset.schema());
        full_schema = full_schema
            .try_with_column(Field::new(SCORE_COLUMN, DataType::Float32, true))
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        let provider: Arc<dyn TableProvider> = Arc::new(ScoredFtsProvider {
            dataset: dataset.clone(),
            fts_query: FullTextSearchQuery::new_query(from_json(fts_query)?),
            full_schema: Arc::new(full_schema),
        });
        // Same `renamed_key` as the registered views, so fts() output joins
        // without a name switch.
        match renamed_key(table_name) {
            Some(key) => Ok(Arc::new(renamed_view("fts", provider, "id", key)?)),
            None => Ok(provider),
        }
    }
}

const SCORE_COLUMN: &str = "_score";

#[derive(Debug)]
struct ScoredFtsProvider {
    dataset: Arc<Dataset>,
    fts_query: FullTextSearchQuery,
    full_schema: SchemaRef,
}

#[async_trait::async_trait]
impl TableProvider for ScoredFtsProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.full_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, lance::deps::datafusion::error::DataFusionError> {
        let mut scan = self.dataset.scan();
        scan.full_text_search(self.fts_query.clone())?;
        // `_score` is a declared column projected explicitly below; with the
        // autoprojection off, the physical batch always matches the logical
        // plan (the mismatch is what breaks aggregates upstream).
        scan.disable_scoring_autoprojection();
        match projection {
            Some(projection) if projection.is_empty() => {
                scan.empty_project()?;
            }
            Some(projection) => {
                let columns: Vec<&str> = projection
                    .iter()
                    .map(|idx| self.full_schema.field(*idx).name().as_str())
                    .collect();
                scan.project(&columns)?;
            }
            None => {
                let columns: Vec<&str> = self
                    .full_schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect();
                scan.project(&columns)?;
            }
        }
        if let Some(combined) = filters
            .iter()
            .cloned()
            .reduce(|left, right| left.and(right))
        {
            scan.filter_expr(combined);
        }
        scan.limit(limit.map(|l| l as i64), None)?;
        scan.create_plan().await.map_err(DataFusionError::from)
    }
}

/// The four scalar shapes the lenient JSON getters produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JsonGet {
    Text,
    Int,
    Float,
    Bool,
}

/// Deepest key path the lenient getters accept; deeper nesting is what
/// json_extract's JSONPath is for.
const MAX_JSON_KEYS: usize = 6;

/// Lenient replacements for lance's `json_get_string` / `_int` / `_float` /
/// `_bool`. The strict originals call jsonb's exact converters and turn one
/// non-scalar field value into a query-wide abort ("Failed to convert to
/// string: InvalidCast"). Lenient semantics: a string getter serializes
/// objects/arrays to JSON text; the typed getters return NULL on a
/// non-coercible value. Unlike lance's one-key originals they take a variadic
/// key path - `json_get_string(col, 'a', 'b')` - the datafusion-functions-json
/// convention agents reach for first. Registered after `register_functions`
/// so they shadow by name.
fn lenient_json_udfs() -> [ScalarUDF; 4] {
    let make = |name: &'static str, kind: JsonGet, return_type: DataType| {
        ScalarUDF::new_from_impl(LenientJsonGet {
            name,
            kind,
            return_type,
            signature: json_key_path_signature(),
        })
    };
    [
        make("json_get_string", JsonGet::Text, DataType::Utf8),
        make("json_get_int", JsonGet::Int, DataType::Int64),
        make("json_get_float", JsonGet::Float, DataType::Float64),
        make("json_get_bool", JsonGet::Bool, DataType::Boolean),
    ]
}

/// `(LargeBinary, Utf8)` through `(LargeBinary, Utf8 x MAX_JSON_KEYS)`.
fn json_key_path_signature() -> Signature {
    let arities = (1..=MAX_JSON_KEYS)
        .map(|keys| {
            let mut types = vec![DataType::LargeBinary];
            types.extend(std::iter::repeat_n(DataType::Utf8, keys));
            TypeSignature::Exact(types)
        })
        .collect();
    Signature::one_of(arities, Volatility::Immutable)
}

/// See [`lenient_json_udfs`].
#[derive(Debug, PartialEq, Eq, Hash)]
struct LenientJsonGet {
    name: &'static str,
    kind: JsonGet,
    return_type: DataType,
    signature: Signature,
}

impl ScalarUDFImpl for LenientJsonGet {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        json_get_lenient(&args.args, &self.kind)
    }
}

/// One step of the key walk: object member by name, array element by index.
fn json_step(raw: jsonb::RawJsonb<'_>, key: &str) -> Option<jsonb::OwnedJsonb> {
    let value = if raw.is_object().unwrap_or(false) {
        raw.get_by_name(key, false).ok().flatten()
    } else if raw.is_array().unwrap_or(false) {
        key.parse::<usize>()
            .ok()
            .and_then(|index| raw.get_by_index(index).ok().flatten())
    } else {
        None
    };
    value.filter(|value| !value.as_raw().is_null().unwrap_or(false))
}

fn json_get_lenient(
    args: &[ColumnarValue],
    kind: &JsonGet,
) -> Result<ColumnarValue, DataFusionError> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let Some((jsonb_arg, key_args)) = arrays.split_first().filter(|(_, keys)| !keys.is_empty())
    else {
        return Err(DataFusionError::Execution(
            "json_get_* takes (json_column, 'key', ...) - at least one key".to_owned(),
        ));
    };
    let jsonb_array = jsonb_arg
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "json_get_* argument 1 must be a JSON column (variant_data, options)".to_owned(),
            )
        })?;
    let key_arrays: Vec<&StringArray> = key_args
        .iter()
        .map(|key_arg| {
            key_arg
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("json_get_* keys must be string literals".to_owned())
                })
        })
        .collect::<Result<_, _>>()?;

    let field = |row: usize| -> Option<jsonb::OwnedJsonb> {
        if jsonb_array.is_null(row) {
            return None;
        }
        let mut keys = key_arrays.iter();
        let first = keys.next()?;
        if first.is_null(row) {
            return None;
        }
        let mut current = json_step(
            jsonb::RawJsonb::new(jsonb_array.value(row)),
            first.value(row),
        )?;
        for key_array in keys {
            if key_array.is_null(row) {
                return None;
            }
            current = json_step(current.as_raw(), key_array.value(row))?;
        }
        Some(current)
    };

    let rows = jsonb_array.len();
    let array: Arc<dyn Array> = match kind {
        JsonGet::Text => {
            let mut builder = StringBuilder::with_capacity(rows, 1024);
            for row in 0..rows {
                match field(row) {
                    // Scalar strings come back unquoted; objects/arrays/
                    // numbers serialize to JSON text instead of erroring.
                    Some(value) => match value.as_raw().to_str() {
                        Ok(text) => builder.append_value(text),
                        Err(_) => builder.append_value(value.to_string()),
                    },
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        JsonGet::Int => {
            let mut builder = Int64Builder::with_capacity(rows);
            for row in 0..rows {
                builder.append_option(field(row).and_then(|value| value.as_raw().to_i64().ok()));
            }
            Arc::new(builder.finish())
        }
        JsonGet::Float => {
            let mut builder = Float64Builder::with_capacity(rows);
            for row in 0..rows {
                builder.append_option(field(row).and_then(|value| value.as_raw().to_f64().ok()));
            }
            Arc::new(builder.finish())
        }
        JsonGet::Bool => {
            let mut builder = BooleanBuilder::with_capacity(rows);
            for row in 0..rows {
                builder.append_option(field(row).and_then(|value| value.as_raw().to_bool().ok()));
            }
            Arc::new(builder.finish())
        }
    };
    Ok(ColumnarValue::Array(array))
}

/// Failures name the fix: append a recovery hint to the DataFusion error
/// classes agents actually hit, so a failed call teaches the correct next
/// query instead of starting a guessing loop. First match wins.
fn enrich(message: &str) -> String {
    const HINTS: &[(&str, &str)] = &[
        (
            "No field named",
            "columns are messages(session_id, message_id, timestamp, role, source_agent, \
             project, content [system-role only], search_text [the conversational text], \
             embedding_model, options) | sessions(session_id, parent_session_id, \
             parent_message_id, source_agent, created_at, project, options) | \
             parts(session_id, message_id, id, ordinal, type, provenance, tool_name, \
             call_id, is_failure, variant_data, options). Part bodies live in \
             parts.variant_data (JSONB) and nest by part type: tool_call is {call_id, \
             name, params} - a Bash command is json_extract(variant_data, \
             '$.params.command') - tool_result is {call_id, name, is_failure, result}, \
             text/reasoning carry {text}. For text search use \
             contains_tokens(search_text, '...') in WHERE, or the fts('messages', ...) \
             table function in FROM for ranked results; to read a transcript use pond_get. \
             Full doc: resource schema://pond-sql.",
        ),
        (
            "Encountered non UTF-8 data",
            "JSON columns (variant_data, options) are binary JSONB - CAST / ::text does not \
             work on them. Stringify the whole value with json_extract(col, '$'), or fetch \
             one field with json_extract(col, '$.field').",
        ),
        (
            "Resources exhausted",
            "the query ran out of memory - usually from carrying whole JSON columns \
             (variant_data, options) through a join or sort. Project narrow fields with \
             json_extract(col, '$.field') instead of whole columns, filter before joining, \
             or export the full set with format=parquet.",
        ),
        (
            "LIKE prefix queries are not supported for bitmap indexes",
            "prefix LIKE ('x%') and starts_with() fail on bitmap-indexed columns \
             (messages.source_agent). Use equality, \
             split_part(source_agent, '/', 1) = '...', or an infix pattern (LIKE '%x%').",
        ),
        (
            "call to 'json_",
            "JSON function signatures: json_get_string|json_get_int|json_get_float|\
             json_get_bool(col, 'key', ...) walk a key path (array steps by numeric \
             index); json_get(col, 'key') returns JSONB for chaining; json_extract(col, \
             '$.a.b') takes a JSONPath and returns JSON text of any value (the right tool \
             for deeply nested or mixed-type fields).",
        ),
        (
            "Invalid function 'json",
            "available JSON functions: json_get_string, json_get_int, json_get_float, \
             json_get_bool (col, 'key', ...); json_get(col, 'key') -> JSONB for chaining; \
             json_extract(col, '$.a.b') -> JSON text; json_array_contains; \
             json_array_length. See resource schema://pond-sql.",
        ),
        (
            // Defensive: lance's fts `boolean` query can plan a CollectLeft
            // HashJoin over multi-partition match arms, which the optimizer
            // does not always repair (works through pond's vendored fts()
            // provider; kept for any path that still trips it).
            "does not satisfy distribution requirements",
            "this fts query shape planned an unexecutable join. For AND semantics use a \
             single match query with operator And: fts('messages', \
             '{\"match\":{\"column\":\"search_text\",\"terms\":\"a b\",\"operator\":\"And\"}}'), \
             optionally with LIKE post-filters in WHERE.",
        ),
        (
            "position is not found but required for phrase queries",
            "the full-text index is built without positions, so \"phrase\" queries are \
             unavailable. Use a match query with operator And plus LIKE post-filters for \
             exact-substring matching.",
        ),
    ];
    for (pattern, hint) in HINTS {
        if message.contains(pattern) {
            return format!("{message}\nhint: {hint}");
        }
    }
    message.to_owned()
}

/// Decode lance JSONB columns to JSON text, then drop columns that don't render
/// readably (the embedding `vector` FixedSizeList and any leftover binary).
fn displayable(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    let decoded = lance_arrow::json::convert_lance_json_to_arrow(batch)?;
    let keep: Vec<usize> = decoded
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| is_displayable(field.data_type()))
        .map(|(index, _)| index)
        .collect();
    decoded.project(&keep)
}

fn is_displayable(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::FixedSizeList(_, _)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// One physical line per row: embedded newlines in cell values (markdown,
/// multi-line commands) otherwise explode a row across many table lines that
/// hard-wrap unreadably in narrow clients. The literal two-char `\n` matches
/// the JSON escaping agents already read, and keeps row boundaries
/// unambiguous. Inline table mode only - json and export modes keep raw data.
fn collapse_newlines(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, ArrowError> {
    fn escape<O: OffsetSizeTrait>(array: &GenericStringArray<O>) -> ArrayRef {
        let escaped: GenericStringArray<O> =
            array.iter().map(|value| value.map(escape_cell)).collect();
        Arc::new(escaped)
    }
    fn escape_cell(text: &str) -> std::borrow::Cow<'_, str> {
        if text.contains(['\n', '\r']) {
            std::borrow::Cow::Owned(text.replace("\r\n", "\\n").replace(['\n', '\r'], "\\n"))
        } else {
            std::borrow::Cow::Borrowed(text)
        }
    }
    batches
        .iter()
        .map(|batch| {
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|array| match array.data_type() {
                    DataType::Utf8 => array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map_or_else(|| array.clone(), escape),
                    DataType::LargeUtf8 => array
                        .as_any()
                        .downcast_ref::<GenericStringArray<i64>>()
                        .map_or_else(|| array.clone(), escape),
                    DataType::Utf8View => array
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .map_or_else(
                            || array.clone(),
                            |view| {
                                let escaped: StringViewArray =
                                    view.iter().map(|value| value.map(escape_cell)).collect();
                                Arc::new(escaped)
                            },
                        ),
                    _ => array.clone(),
                })
                .collect();
            RecordBatch::try_new(batch.schema(), columns)
        })
        .collect()
}

fn render_inline(
    display: &[RecordBatch],
    max_rows: usize,
    elapsed: Duration,
) -> Result<String, ArrowError> {
    let total: usize = display.iter().map(RecordBatch::num_rows).sum();
    let elapsed_ms = elapsed.as_millis();
    if total == 0 {
        // Still render the header so the caller sees the result columns.
        return Ok(format!(
            "0 rows ({elapsed_ms} ms).\n{}",
            pretty_format_batches(display)?
        ));
    }
    let render = |shown: usize| -> Result<String, ArrowError> {
        let limited = collapse_newlines(&limit_batches(display, shown))?;
        Ok(pretty_format_batches(&limited)?.to_string())
    };
    let mut shown = total.min(max_rows);
    let mut table = render(shown)?;
    while table.len() > INLINE_BUDGET_BYTES && shown > 1 {
        shown = (shown / 2).max(1);
        table = render(shown)?;
    }
    let mut out = format!("{total} row(s) in {elapsed_ms} ms; showing {shown}.\n{table}");
    if shown < total {
        out.push_str(&format!(
            "\n... {} row(s) omitted. To page: ORDER BY <indexed col> (e.g. timestamp, \
             message_id), then in the next call add `WHERE (col, message_id) < \
             (<last_col>, <last_message_id>)` - keyset pagination, see schema://pond-sql. \
             For the full set: format=parquet or format=ndjson.",
            total - shown
        ));
    }
    Ok(out)
}

fn limit_batches(batches: &[RecordBatch], max_rows: usize) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut remaining = max_rows;
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            out.push(batch.clone());
        } else {
            out.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

fn encode_parquet(batches: &[RecordBatch]) -> Result<Vec<u8>, SqlError> {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| SqlError::Query("query returned no columns to export".to_owned()))?;
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)
        .map_err(|error| SqlError::Infra(anyhow!("parquet init failed: {error}")))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| SqlError::Infra(anyhow!("parquet write failed: {error}")))?;
    }
    writer
        .close()
        .map_err(|error| SqlError::Infra(anyhow!("parquet close failed: {error}")))?;
    Ok(buffer)
}

fn encode_ndjson(batches: &[RecordBatch]) -> Result<Vec<u8>, SqlError> {
    let mut buffer = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        writer
            .write_batches(&refs)
            .map_err(|error| SqlError::Infra(anyhow!("ndjson write failed: {error}")))?;
        writer
            .finish()
            .map_err(|error| SqlError::Infra(anyhow!("ndjson finish failed: {error}")))?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn rejected(sql: &str) -> bool {
        matches!(parse_and_gate(sql), Err(SqlError::Query(_)))
    }

    fn parses_as(sql: &str, expected: StatementKind) -> bool {
        match parse_and_gate(sql) {
            Ok(parsed) => matches!(
                (&parsed.kind, &expected),
                (StatementKind::Query, StatementKind::Query)
                    | (StatementKind::Explain, StatementKind::Explain)
            ),
            Err(_) => false,
        }
    }

    #[test]
    fn mentions_table_is_sound_for_open_pruning() {
        // Referenced => must be detected (no false negative, else a valid query
        // breaks). Case-insensitive: DataFusion lowercases identifiers.
        assert!(mentions_table("SELECT * FROM messages", "messages"));
        assert!(mentions_table("select * from MESSAGES", "messages"));
        assert!(mentions_table(
            "SELECT s.id FROM sessions s JOIN parts p ON s.id = p.session_id",
            "parts",
        ));
        assert!(mentions_table(
            "SELECT * FROM fts('messages', '{\"match\":{}}')",
            "messages",
        ));
        assert!(mentions_table(
            "WITH x AS (SELECT * FROM sessions) SELECT * FROM x",
            "sessions",
        ));
        // Not referenced => skip the open. Word boundary: a longer identifier
        // that merely contains the name is not a reference.
        assert!(!mentions_table("SELECT * FROM messages", "parts"));
        assert!(!mentions_table("SELECT * FROM messages", "sessions"));
        assert!(!mentions_table(
            "SELECT counterparts FROM messages",
            "parts"
        ));
    }

    #[test]
    fn allows_single_select_and_cte() {
        assert!(parses_as("SELECT 1", StatementKind::Query));
        assert!(parses_as(
            "SELECT role, count(*) FROM messages GROUP BY role",
            StatementKind::Query
        ));
        assert!(parses_as(
            "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
            StatementKind::Query
        ));
    }

    #[test]
    fn allows_explain_of_select() {
        assert!(parses_as("EXPLAIN SELECT 1", StatementKind::Explain));
        assert!(parses_as(
            "EXPLAIN ANALYZE SELECT role FROM messages",
            StatementKind::Explain
        ));
    }

    #[test]
    fn rejects_explain_of_non_query() {
        // EXPLAIN of a side-effecting statement: the inner statement is what
        // would matter; reject to keep the surface tight.
        assert!(rejected("EXPLAIN INSERT INTO messages VALUES ('x')"));
    }

    #[test]
    fn rejects_writes_and_side_effects() {
        assert!(rejected("INSERT INTO messages VALUES ('x')"));
        assert!(rejected("UPDATE messages SET role = 'x'"));
        assert!(rejected("DELETE FROM messages"));
        assert!(rejected("CREATE TABLE t (x INT)"));
        assert!(rejected("CREATE VIEW v AS SELECT 1"));
        assert!(rejected("DROP TABLE messages"));
        assert!(rejected(
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/etc'"
        ));
        assert!(rejected("COPY (SELECT 1) TO '/tmp/x.parquet'"));
        assert!(rejected("SET a = 1"));
    }

    #[test]
    fn rejects_multiple_statements() {
        assert!(rejected("SELECT 1; SELECT 2"));
        assert!(rejected("SELECT 1; DROP TABLE messages"));
    }

    #[test]
    fn rejects_unparseable() {
        assert!(rejected("NOT SQL AT ALL ;;"));
    }

    fn mentions_vector(sql: &str) -> bool {
        match parse_and_gate(sql) {
            Ok(parsed) => projection_mentions_vector(parsed.projection_query()),
            Err(_) => false,
        }
    }

    #[test]
    fn explicit_vector_projection_is_rejected() {
        assert!(mentions_vector("SELECT vector FROM messages"));
        assert!(mentions_vector("SELECT id, vector FROM messages"));
        assert!(mentions_vector("SELECT m.vector FROM messages m"));
        assert!(mentions_vector("SELECT array_length(vector) FROM messages"));
        assert!(mentions_vector("EXPLAIN SELECT vector FROM messages"));
    }

    #[test]
    fn enrich_appends_recovery_hints() {
        // One literal error string per class, captured from real failed calls.
        let cases = [
            (
                "SQL error: Schema error: No field named created_at.",
                "schema://pond-sql",
            ),
            (
                "SQL error: External error: Arrow error: Invalid argument error: \
                 Encountered non UTF-8 data",
                "json_extract",
            ),
            (
                "SQL error: External error: Not supported: LIKE prefix queries are not \
                 supported for bitmap indexes",
                "split_part",
            ),
            (
                "SQL error: Error during planning: Failed to coerce arguments to satisfy \
                 a call to 'json_get_string' function",
                "JSONPath",
            ),
            (
                "SQL error: Error during planning: Invalid function 'json_get_json'.",
                "json_extract",
            ),
            (
                "SQL error: Resources exhausted: Additional allocation failed for \
                 HashJoinInput[0] with top memory consumers",
                "json_extract",
            ),
        ];
        for (raw, marker) in cases {
            let enriched = enrich(raw);
            assert!(enriched.starts_with(raw), "original kept: {enriched}");
            assert!(enriched.contains("hint:"), "hint appended: {enriched}");
            assert!(enriched.contains(marker), "hint names the fix: {enriched}");
        }
        // Unrecognized errors pass through untouched.
        assert_eq!(
            enrich("SQL error: division by zero"),
            "SQL error: division by zero"
        );
    }

    #[test]
    fn select_star_and_where_vector_are_allowed() {
        // `SELECT *` falls through to the existing silent-strip in displayable.
        assert!(!mentions_vector("SELECT * FROM messages"));
        // Filtering on `vector` is documented as legal (`vector IS NOT NULL`).
        assert!(!mentions_vector(
            "SELECT message_id FROM messages WHERE vector IS NOT NULL"
        ));
    }

    #[test]
    fn jsonb_cast_misuse_detects_cast_and_coloncolon() {
        for sql in [
            "SELECT CAST(variant_data AS VARCHAR) FROM parts",
            "SELECT cast(p.variant_data as text) FROM parts p",
            "SELECT variant_data::text FROM parts",
            "SELECT p.variant_data :: varchar FROM parts p",
            "SELECT options::text FROM messages",
            "SELECT lower(CAST(variant_data AS VARCHAR)) FROM parts",
        ] {
            assert!(jsonb_cast_misuse(sql), "should reject: {sql}");
        }
    }

    #[test]
    fn jsonb_cast_misuse_allows_legitimate_use() {
        for sql in [
            "SELECT json_extract(variant_data, '$') FROM parts",
            "SELECT json_get_string(variant_data, 'name') FROM parts",
            "SELECT CAST(ordinal AS BIGINT) FROM parts",
            "SELECT timestamp::date FROM messages",
            // `options` as part of a longer identifier is not the column.
            "SELECT my_options::text FROM t",
            "SELECT CAST(json_extract(variant_data, '$.x') AS BIGINT) FROM parts",
        ] {
            assert!(!jsonb_cast_misuse(sql), "should allow: {sql}");
        }
    }

    #[test]
    fn jsonb_fulldoc_like_scan_detects_whole_document_substring() {
        for sql in [
            "SELECT * FROM parts WHERE json_extract(variant_data, '$') LIKE '%needle%'",
            "SELECT * FROM parts p WHERE lower(json_extract(p.variant_data, '$')) LIKE '%x%'",
            "SELECT * FROM messages WHERE json_extract(options, '$') ILIKE '%y%'",
            "SELECT * FROM parts WHERE json_extract(variant_data,'$') NOT LIKE '%z%'",
            // The real timeout shape: day-scoped join still scans every part.
            "SELECT p.message_id FROM parts p JOIN messages m ON p.message_id = m.message_id \
             WHERE m.timestamp >= '2026-06-11' AND lower(json_extract(p.variant_data, '$')) \
             LIKE '%weekly limit%'",
        ] {
            assert!(jsonb_fulldoc_like_scan(sql), "should reject: {sql}");
        }
    }

    #[test]
    fn jsonb_fulldoc_like_scan_allows_targeted_and_nonleading() {
        for sql in [
            // single-field extract, not the whole document
            "SELECT * FROM parts WHERE json_extract(variant_data, '$.name') LIKE '%x%'",
            // non-leading (prefix) pattern can be served without a full stringify
            "SELECT * FROM parts WHERE json_extract(variant_data, '$') LIKE 'pre%'",
            // plain text LIKE has no whole-document stringify
            "SELECT * FROM messages WHERE search_text LIKE '%x%'",
            // indexed predicate, the path agents should take
            "SELECT * FROM messages WHERE contains_tokens(search_text, 'x')",
            // projecting the stringified value is fine; no LIKE scan
            "SELECT json_extract(variant_data, '$') FROM parts LIMIT 1",
        ] {
            assert!(!jsonb_fulldoc_like_scan(sql), "should allow: {sql}");
        }
    }

    #[test]
    fn render_inline_collapses_newlines_in_cells() {
        let schema = Arc::new(Schema::new(vec![Field::new("t", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some(
                "line one\nline two\r\nline three",
            )]))],
        )
        .expect("single-column batch");
        let out = render_inline(&[batch], 10, Duration::from_millis(1)).expect("render succeeds");
        assert!(
            out.contains("line one\\nline two\\nline three"),
            "newlines collapse to literal \\n: {out}"
        );
        // The data row renders as one physical line: header rule, header,
        // rule, row, rule - the row itself never wraps.
        let row_lines: Vec<&str> = out
            .lines()
            .filter(|line| line.contains("line one"))
            .collect();
        assert_eq!(row_lines.len(), 1, "one physical line per row: {out}");
    }

    #[test]
    fn effective_timeout_defaults_and_clamps() {
        assert_eq!(
            effective_timeout(None),
            Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS)
        );
        assert_eq!(effective_timeout(Some(60)), Duration::from_secs(60));
        assert_eq!(effective_timeout(Some(0)), Duration::from_secs(1));
        assert_eq!(
            effective_timeout(Some(u64::MAX)),
            Duration::from_secs(MAX_QUERY_TIMEOUT_SECS)
        );
    }
}
