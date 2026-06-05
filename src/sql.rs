//! `pond_sql_query`: read-only DataFusion SQL over the three Lance tables
//! (`sessions` / `messages` / `parts`), registered as `LanceTableProvider`s on
//! a fresh per-call `SessionContext`. Read-only is enforced in two layers - a
//! single-`SELECT` pre-parse and `sql_with_options` with DDL/DML/statements all
//! disabled - so no statement that mutates the corpus or touches the filesystem
//! (INSERT/UPDATE/DELETE/CREATE/DROP/COPY/CREATE EXTERNAL TABLE/SET) can run.
//! Results render inline (row-capped) or export to a parquet/ndjson file the
//! caller fetches via the `pond-sql-export://` resource (`src/transport.rs`).

use std::sync::Arc;
use std::time::Duration;

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
use lance::deps::datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use lance_datafusion::udf::register_functions;
use parquet::arrow::ArrowWriter;

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
pub(crate) const DEFAULT_INLINE_ROWS: usize = 100;
/// Upper bound on the caller-supplied inline `limit`.
pub(crate) const MAX_INLINE_ROWS: usize = 1_000;

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
    /// A rendered, row-capped table.
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
pub enum SqlError {
    Query(String),
    Infra(anyhow::Error),
}

fn infra(error: ArrowError) -> SqlError {
    SqlError::Infra(anyhow::Error::new(error))
}

/// Execute one read-only SQL query and return either a rendered table or
/// encoded export bytes.
pub async fn run(
    tables: &Tables,
    sql: &str,
    mode: Mode,
    inline_rows: usize,
) -> Result<Outcome, SqlError> {
    ensure_single_select(sql)?;
    let ctx = build_context()?;
    register(&ctx, tables)?;

    // Defense in depth on top of the single-SELECT pre-parse: verify_plan walks
    // the plan with subqueries and rejects any DDL/DML/COPY/Statement node.
    let options = SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false);
    let df = ctx
        .sql_with_options(sql, options)
        .await
        .map_err(|error| SqlError::Query(format!("SQL error: {error}")))?;

    // Captured before `collect()` consumes `df`, so an empty result still
    // renders its column headers.
    let result_schema = Arc::new(df.schema().as_arrow().clone());
    let collected = tokio::time::timeout(QUERY_TIMEOUT, df.collect())
        .await
        .map_err(|_| {
            SqlError::Query(format!(
                "query exceeded the {}s limit; add a narrower WHERE or a LIMIT",
                QUERY_TIMEOUT.as_secs()
            ))
        })?
        .map_err(|error| SqlError::Query(format!("SQL error: {error}")))?;

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
            render_inline(&display, inline_rows).map_err(infra)?,
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

/// Read-only gate: parse the SQL and require exactly one top-level `Query`
/// (SELECT/WITH/VALUES/UNION). This also rejects EXPLAIN/ANALYZE/DESCRIBE/SET
/// and multi-statement input, which `SQLOptions` alone does not.
fn ensure_single_select(sql: &str) -> Result<(), SqlError> {
    let statements = DFParser::parse_sql(sql)
        .map_err(|error| SqlError::Query(format!("SQL parse error: {error}")))?;
    if statements.len() != 1 {
        return Err(SqlError::Query(
            "pond_sql_query runs exactly one statement; submit a single SELECT".to_owned(),
        ));
    }
    match statements.front() {
        Some(DfStatement::Statement(statement))
            if matches!(statement.as_ref(), SqlStatement::Query(_)) =>
        {
            Ok(())
        }
        _ => Err(SqlError::Query(
            "pond_sql_query is read-only: only a single SELECT/WITH query is allowed \
             (no INSERT/UPDATE/DELETE/CREATE/DROP/COPY/EXPLAIN/SET)"
                .to_owned(),
        )),
    }
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

fn render_inline(display: &[RecordBatch], max_rows: usize) -> Result<String, ArrowError> {
    let total: usize = display.iter().map(RecordBatch::num_rows).sum();
    if total == 0 {
        // Still render the header so the caller sees the result columns.
        return Ok(format!("0 rows.\n{}", pretty_format_batches(display)?));
    }
    let mut shown = total.min(max_rows);
    let mut table = pretty_format_batches(&limit_batches(display, shown))?.to_string();
    while table.len() > INLINE_BUDGET_BYTES && shown > 1 {
        shown = (shown / 2).max(1);
        table = pretty_format_batches(&limit_batches(display, shown))?.to_string();
    }
    let mut out = format!("{total} row(s); showing {shown}.\n{table}");
    if shown < total {
        out.push_str(&format!(
            "\n... {} row(s) omitted; add LIMIT/WHERE or set output=parquet|ndjson \
             for the full result.",
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
    use super::*;

    fn rejected(sql: &str) -> bool {
        matches!(ensure_single_select(sql), Err(SqlError::Query(_)))
    }

    #[test]
    fn allows_single_select_and_cte() {
        assert!(ensure_single_select("SELECT 1").is_ok());
        assert!(ensure_single_select("SELECT role, count(*) FROM messages GROUP BY role").is_ok());
        assert!(ensure_single_select("WITH t AS (SELECT 1 AS a) SELECT a FROM t").is_ok());
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
        assert!(rejected("EXPLAIN SELECT 1"));
        assert!(rejected("EXPLAIN ANALYZE SELECT 1"));
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
}
