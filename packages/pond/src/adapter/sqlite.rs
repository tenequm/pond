//! Shared read-only SQLite plumbing for DB-backed adapters (opencode, openclaw,
//! hermes, nanoclaw).
//!
//! Seam rule (CLAUDE.md "Seam boundaries"): this module carries only
//! cross-implementation infrastructure with two real callers and no
//! adapter-specific assumption. The adapter's own `NAME` threads through as a
//! `&'static str` purely for error attribution; nothing here knows a source
//! layout, table name, or record shape.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};

use super::AdapterError;

/// Event-channel bound; doubles as backpressure - the blocking reader parks on
/// `blocking_send` when the consumer lags.
pub(crate) const CHANNEL_CAP: usize = 256;

/// Read-side SQLite busy timeout - source DBs are written live (WAL) by their
/// owning process, so a short read burst must not stall on a checkpoint.
pub(crate) const DB_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(5000);

/// Open a read-only connection (`READ_ONLY | URI`) with the shared busy timeout.
pub(crate) fn open_db(adapter: &'static str, path: &Path) -> Result<Connection, AdapterError> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|error| db_error(adapter, path, "open", &error))?;
    conn.busy_timeout(DB_BUSY_TIMEOUT)
        .map_err(|error| db_error(adapter, path, "set busy_timeout", &error))?;
    Ok(conn)
}

/// Get-or-open a cached connection for `path`.
pub(crate) fn connection<'a>(
    adapter: &'static str,
    conns: &'a mut HashMap<PathBuf, Connection>,
    path: &Path,
) -> Result<&'a Connection, AdapterError> {
    match conns.entry(path.to_path_buf()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(open_db(adapter, path)?)),
    }
}

/// A DB-interaction failure (open, prepare, query, row read) classified as
/// [`AdapterError::io`] - these are substrate faults, not source-shape faults
/// (genuine shape errors route through each adapter's own parse errors).
pub(crate) fn db_error(
    adapter: &'static str,
    path: &Path,
    op: &str,
    error: &rusqlite::Error,
) -> AdapterError {
    AdapterError::io(
        adapter,
        path.display().to_string(),
        std::io::Error::other(format!("sqlite {op} failed: {error}")),
    )
}

/// A blocking-task panic is a pond bug, not bad source data, so it fails the
/// whole run rather than skipping a session.
pub(crate) fn join_error(adapter: &'static str, join: tokio::task::JoinError) -> AdapterError {
    AdapterError::io(
        adapter,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

/// SQLite affinity of each column in a mirrored SELECT list, so a row decodes to
/// the right JSON scalar. `Real` covers float columns (hermes's epoch-second
/// timestamps); openclaw's columns are all `Str`/`Int`.
#[derive(Clone, Copy)]
pub(crate) enum ColKind {
    Str,
    Int,
    Real,
}

/// The column names of a `(name, kind)` list, in order, comma-joined for a SELECT.
pub(crate) fn columns_sql(columns: &[(&str, ColKind)]) -> String {
    columns
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Rebuild a row as a JSON object, column names kept verbatim, null columns
/// omitted (spec.md#model-lossless-projection - every non-null column
/// recoverable). One `(name, kind)` list drives both the SELECT and this decode,
/// so the projection can never drift from the query.
pub(crate) fn row_to_json(
    row: &rusqlite::Row,
    columns: &[(&str, ColKind)],
) -> rusqlite::Result<Value> {
    let mut map = serde_json::Map::new();
    for (idx, (name, kind)) in columns.iter().enumerate() {
        match kind {
            ColKind::Str => {
                if let Some(value) = row.get::<_, Option<String>>(idx)? {
                    map.insert((*name).to_owned(), json!(value));
                }
            }
            ColKind::Int => {
                if let Some(value) = row.get::<_, Option<i64>>(idx)? {
                    map.insert((*name).to_owned(), json!(value));
                }
            }
            ColKind::Real => {
                if let Some(value) = row.get::<_, Option<f64>>(idx)? {
                    map.insert((*name).to_owned(), json!(value));
                }
            }
        }
    }
    Ok(Value::Object(map))
}

/// Send one yield through a blocking read channel, returning `false` from the
/// caller (stop reading this source) when the consumer dropped the receiver.
macro_rules! emit {
    ($tx:expr, $item:expr) => {
        if $tx.blocking_send($item).is_err() {
            return false;
        }
    };
}
pub(crate) use emit;
