//! `pond_sql_query`: read-only DataFusion SQL over the three Lance tables
//! (`sessions` / `messages` / `parts`), registered as `LanceTableProvider`s on
//! a fresh per-call `SessionContext`. Read-only is enforced in two layers - a
//! single-`SELECT` pre-parse and `sql_with_options` with DDL/DML/statements all
//! disabled - so no statement that mutates the corpus or touches the filesystem
//! (INSERT/UPDATE/DELETE/CREATE/DROP/COPY/CREATE EXTERNAL TABLE/SET) can run.
//! Results render inline (row-capped) or export to a parquet/ndjson file the
//! caller fetches via the `pond-sql-export://` resource (`src/transport.rs`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arrow_json::LineDelimitedWriter;
use lance::Dataset;
use lance::datafusion::LanceTableProvider;
use lance::dataset::udtf::FtsQueryUDTFBuilder;
use lance::deps::arrow_array::RecordBatch;
use lance::deps::arrow_schema::{ArrowError, DataType};
use lance::deps::datafusion::arrow::util::pretty::pretty_format_batches;
use lance::deps::datafusion::execution::SessionStateBuilder;
use lance::deps::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use lance::deps::datafusion::prelude::{SQLOptions, SessionConfig, SessionContext};
use lance::deps::datafusion::sql::parser::{DFParser, Statement as DfStatement};
use lance::deps::datafusion::sql::sqlparser::ast::{SetExpr, Statement as SqlStatement};
use lance_datafusion::udf::register_functions;
use parquet::arrow::ArrowWriter;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

/// Per-query memory ceiling for the DataFusion runtime. Not enforced on every
/// operator (datafusion caveat), so the timeout below is the hard backstop.
const MEM_LIMIT_BYTES: usize = 512 * 1024 * 1024;
/// Wall-clock cap on `collect()`. DataFusion 53 has no built-in query timeout,
/// so this `tokio::time::timeout` is the only guard against a runaway plan.
const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
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

/// How `pond_sql_query` returns results.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// Render a row-capped table into the tool result.
    Inline,
    /// Return a row-capped JSON payload; the MCP layer surfaces it through
    /// `structuredContent` (with a stringified text fallback for clients that
    /// do not surface the structured channel). Empirically validated on Claude
    /// Code 2.1.165: when both channels carry the same payload, the agent reads
    /// the structured one and the text block is a soft-landing for other
    /// clients (spec 2025-11-25 server SHOULD).
    InlineJson,
    /// Write the full result to a file and return a `pond-sql-export://` link.
    Export(Format),
}

/// The three Lance datasets, fetched fresh per call so each query sees a
/// current snapshot (the handle freshness gate runs on each `Store::dataset`).
pub struct Tables {
    pub sessions: Arc<Dataset>,
    pub messages: Arc<Dataset>,
    pub parts: Arc<Dataset>,
}

/// Result of a successful `run`.
pub enum Outcome {
    /// A rendered, row-capped table (already includes the metrics footer).
    Inline(String),
    /// A row-capped JSON payload with metadata fields (`total_rows`,
    /// `shown_rows`, `truncated`, `elapsed_ms`, `columns`, `rows`).
    InlineJson(JsonValue),
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
) -> Result<Outcome, SqlError> {
    let parsed = parse_and_gate(sql)?;
    if matches!(parsed.kind, StatementKind::Explain) && matches!(mode, Mode::Export(_)) {
        return Err(SqlError::Query(
            "EXPLAIN returns a plan, not a result set; use output=table (or json) to read it"
                .to_owned(),
        ));
    }
    if projection_mentions_vector(parsed.projection_query()) {
        return Err(SqlError::Query(
            "the `vector` column is not selectable from pond_sql_query (it is a \
             FixedSizeList<f32> embedding, ~600 bytes per row and not useful in a result). \
             For semantic search use pond_search. Filtering on it is allowed in WHERE \
             (e.g. `vector IS NOT NULL`)."
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
        .map_err(|error| SqlError::Query(format!("SQL error: {error}")))?;

    // Captured before `collect()` consumes `df`, so an empty result still
    // renders its column headers.
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let started = Instant::now();
    let collected = tokio::time::timeout(QUERY_TIMEOUT, df.collect())
        .await
        .map_err(|_| {
            SqlError::Query(format!(
                "query exceeded the {}s limit; add a narrower WHERE or a LIMIT",
                QUERY_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| SqlError::Query(format!("SQL error: {error}")))?;
    let elapsed = started.elapsed();

    let display: Vec<RecordBatch> = if collected.is_empty() {
        vec![displayable(&RecordBatch::new_empty(result_schema)).map_err(infra)?]
    } else {
        collected
            .iter()
            .map(displayable)
            .collect::<Result<_, _>>()
            .map_err(infra)?
    };

    match mode {
        Mode::Inline => Ok(Outcome::Inline(
            render_inline(&display, inline_rows, elapsed).map_err(infra)?,
        )),
        Mode::InlineJson => Ok(Outcome::InlineJson(render_inline_json(
            &display,
            inline_rows,
            elapsed,
        )?)),
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
            "pond_sql_query runs exactly one statement; submit a single SELECT".to_owned(),
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
    SqlError::Query(
        "pond_sql_query is read-only: only a single SELECT/WITH (or EXPLAIN of one) is \
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

fn build_context() -> Result<SessionContext, SqlError> {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(MEM_LIMIT_BYTES, 1.0)
        .build_arc()
        .map_err(|error| SqlError::Infra(anyhow!("datafusion runtime init failed: {error}")))?;
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_runtime_env(runtime)
        .with_default_features()
        .build();
    Ok(SessionContext::new_with_state(state))
}

fn register(ctx: &SessionContext, tables: &Tables) -> Result<(), SqlError> {
    for (name, dataset) in [
        ("sessions", &tables.sessions),
        ("messages", &tables.messages),
        ("parts", &tables.parts),
    ] {
        // LanceTableProvider (not the bare Dataset impl) so WHERE/projection/
        // limit push into Lance's indexed scan; (false, false) hides _rowid /
        // _rowaddr from the SQL schema.
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        ctx.register_table(name, Arc::new(provider))
            .map_err(|error| SqlError::Infra(anyhow!("register table {name}: {error}")))?;
    }
    // `fts('messages', '{...}')` BM25 search-in-SQL, and lance's JSON /
    // contains_tokens UDFs for filtering inside the JSON columns.
    let fts = FtsQueryUDTFBuilder::builder()
        .register_table("sessions", tables.sessions.clone())
        .register_table("messages", tables.messages.clone())
        .register_table("parts", tables.parts.clone())
        .build();
    ctx.register_udtf("fts", Arc::new(fts));
    register_functions(ctx);
    Ok(())
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
    let mut shown = total.min(max_rows);
    let mut table = pretty_format_batches(&limit_batches(display, shown))?.to_string();
    while table.len() > INLINE_BUDGET_BYTES && shown > 1 {
        shown = (shown / 2).max(1);
        table = pretty_format_batches(&limit_batches(display, shown))?.to_string();
    }
    let mut out = format!("{total} row(s) in {elapsed_ms} ms; showing {shown}.\n{table}");
    if shown < total {
        out.push_str(&format!(
            "\n... {} row(s) omitted. To page: ORDER BY <indexed col> (e.g. timestamp, \
             id), then in the next call add `WHERE (col, id) < (<last_col>, <last_id>)` - \
             keyset pagination, see schema://pond-sql. For the full set: output=parquet \
             or output=ndjson.",
            total - shown
        ));
    }
    Ok(out)
}

/// JSON sibling of `render_inline`: same row cap and byte-budget shrinking,
/// returned as a `JsonValue` so the MCP layer can hand it to
/// `CallToolResult::structured` (text fallback + structured channel in one
/// call, see [`Mode::InlineJson`]).
fn render_inline_json(
    display: &[RecordBatch],
    max_rows: usize,
    elapsed: Duration,
) -> Result<JsonValue, SqlError> {
    let total: usize = display.iter().map(RecordBatch::num_rows).sum();
    let columns: Vec<String> = display
        .first()
        .map(|batch| {
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);

    if total == 0 {
        return Ok(json!({
            "total_rows": 0,
            "shown_rows": 0,
            "truncated": false,
            "elapsed_ms": elapsed_ms,
            "columns": columns,
            "rows": [],
        }));
    }

    let mut shown = total.min(max_rows);
    let mut rows = batches_to_json_rows(&limit_batches(display, shown))?;
    let mut serialized = serde_json::to_string(&rows)
        .map_err(|error| SqlError::Infra(anyhow!("json serialize: {error}")))?;
    while serialized.len() > INLINE_BUDGET_BYTES && shown > 1 {
        shown = (shown / 2).max(1);
        rows = batches_to_json_rows(&limit_batches(display, shown))?;
        serialized = serde_json::to_string(&rows)
            .map_err(|error| SqlError::Infra(anyhow!("json serialize: {error}")))?;
    }

    let mut payload = JsonMap::new();
    payload.insert("total_rows".to_owned(), json!(total));
    payload.insert("shown_rows".to_owned(), json!(shown));
    payload.insert("truncated".to_owned(), json!(shown < total));
    payload.insert("elapsed_ms".to_owned(), json!(elapsed_ms));
    payload.insert("columns".to_owned(), json!(columns));
    payload.insert("rows".to_owned(), JsonValue::Array(rows));
    if shown < total {
        payload.insert(
            "next_steps".to_owned(),
            json!(format!(
                "{} row(s) omitted; ORDER BY + keyset (`WHERE (col, id) < \
                 (<last_col>, <last_id>)`) to page, or output=parquet|ndjson for the \
                 full set. See schema://pond-sql.",
                total - shown
            )),
        );
    }
    Ok(JsonValue::Object(payload))
}

/// Convert RecordBatches to a JSON array of row objects via the existing
/// NDJSON writer (handles all Arrow types, including the decoded JSON columns
/// that come out of `displayable`).
fn batches_to_json_rows(batches: &[RecordBatch]) -> Result<Vec<JsonValue>, SqlError> {
    if batches.iter().all(|batch| batch.num_rows() == 0) {
        return Ok(Vec::new());
    }
    let mut buffer = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        let refs: Vec<&RecordBatch> = batches.iter().collect();
        writer
            .write_batches(&refs)
            .map_err(|error| SqlError::Infra(anyhow!("ndjson encode: {error}")))?;
        writer
            .finish()
            .map_err(|error| SqlError::Infra(anyhow!("ndjson finish: {error}")))?;
    }
    let text = String::from_utf8(buffer)
        .map_err(|error| SqlError::Infra(anyhow!("ndjson not utf-8: {error}")))?;
    text.lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            serde_json::from_str::<JsonValue>(line)
                .map_err(|error| SqlError::Infra(anyhow!("ndjson parse: {error}")))
        })
        .collect()
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
    fn select_star_and_where_vector_are_allowed() {
        // `SELECT *` falls through to the existing silent-strip in displayable.
        assert!(!mentions_vector("SELECT * FROM messages"));
        // Filtering on `vector` is documented as legal (`vector IS NOT NULL`).
        assert!(!mentions_vector(
            "SELECT id FROM messages WHERE vector IS NOT NULL"
        ));
    }
}
