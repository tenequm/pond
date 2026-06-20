//! Bounded streaming reader for JSONL-tree sources, and the `JsonlTree` driver
//! that composes it.
//!
//! The read path is plain `std::fs` on `spawn_blocking`: `tokio::fs` is itself
//! `spawn_blocking` underneath and benchmarks far slower. Every record is
//! bounded before it leaves this module (spec.md#adapter-bounded-values) - a line
//! within `RECORD_CAP` via `serde_json` + `bound_value`, a longer line via the
//! `struson` cap-parser so peak memory stays bounded.

use std::{
    ffi::OsStr,
    io::{BufRead, BufReader, Cursor, Read},
    path::{Path, PathBuf},
};

use async_stream::stream;
use serde_json::Value;
use struson::reader::{JsonReader, JsonStreamReader, ValueType};
use tokio::{sync::mpsc, task::JoinError};

use super::{
    AdapterError, AdapterYield, AdapterYieldStream, DiscoverFuture, SkipOracle, SkipReason,
    extract::{LEAF_CAP, bound_value, truncate_to_marker},
};
use crate::{
    sessions::IngestEvent,
    wire::{ProviderOptions, Session},
};

/// Fast-path / slow-path split and the largest record `serde_json` parses in
/// one shot. 3x the largest legitimate whole record in a real-corpus survey.
pub(crate) const RECORD_CAP: usize = 32 * 1024 * 1024;

/// Event-channel bound; doubles as backpressure - the blocking reader parks on
/// `blocking_send` when the consumer lags.
const CHANNEL_CAP: usize = 256;

/// One parsed source record; `value`'s string leaves are all bounded at `LEAF_CAP`.
pub(crate) struct BoundedRow {
    pub line: usize,
    pub value: Value,
}

pub(crate) fn source_line(options: &ProviderOptions) -> Option<u64> {
    options
        .get("source")
        .and_then(|source| source.get("line"))
        .and_then(Value::as_u64)
}

/// A "walk a tree, one `.jsonl` per session, line equals record" adapter. The
/// driver supplies the format-specific decode; [`jsonl_tree_events`] supplies
/// the walk, freshness gate, bounded read, and error attribution.
pub(crate) trait JsonlTree: Clone + Send + Sync + 'static {
    type State: Default;

    fn name(&self) -> &'static str;
    fn root(&self) -> &Path;

    /// Session id for the freshness gate, from a file's raw first non-empty
    /// line; `None` disables the skip for that file.
    fn peek_session_id(&self, path: &Path, first_line: &str) -> Option<String>;

    /// Latest message timestamp (micros) for the freshness gate - the source-side
    /// watermark compared against pond's stored max. The driver reads it from the
    /// file tail (the last message line, or a bounded backward scan); `None`
    /// disables the skip (the file re-reads).
    fn peek_last_ts(&self, path: &Path) -> Option<i64>;

    fn session(&self, path: &Path, rows: &[BoundedRow]) -> Result<Session, AdapterError>;

    fn events_from_row(
        &self,
        session: &Session,
        row: &BoundedRow,
        state: &mut Self::State,
    ) -> Result<Vec<IngestEvent>, String>;

    /// A file the adapter structurally recognizes as a sidecar whose specific
    /// shape this version cannot ingest. Returning `Some(reason)` makes the read
    /// loop skip the file as a VISIBLE, counted failure instead of deriving a
    /// content-borrowed id that could silently merge it into another session.
    /// Default: no such category.
    fn unsupported_reason(&self, _path: &Path) -> Option<String> {
        None
    }
}

/// Path-bearing io error; callers remap it into an [`AdapterError`].
pub(crate) struct IoAtPath {
    pub path: String,
    pub source: std::io::Error,
}

/// Walk `root` recursively for every `*.jsonl` file, sorted for deterministic
/// ingest order.
pub(crate) fn collect_jsonl_files(root: &Path) -> Result<Vec<PathBuf>, IoAtPath> {
    let mut paths = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let at_dir = |source| IoAtPath {
            path: dir.display().to_string(),
            source,
        };
        for entry in std::fs::read_dir(&dir).map_err(at_dir)? {
            let entry = entry.map_err(at_dir)?;
            let file_type = entry.file_type().map_err(at_dir)?;
            let child = entry.path();
            if file_type.is_dir() {
                stack.push(child);
            } else if child.extension() == Some(OsStr::new("jsonl")) {
                paths.push(child);
            }
        }
    }
    paths.sort();
    Ok(paths)
}

pub(crate) fn jsonl_tree_discover<D: JsonlTree>(driver: &D) -> DiscoverFuture<'_> {
    let driver = driver.clone();
    let name = driver.name();
    Box::pin(async move {
        tokio::task::spawn_blocking(move || {
            collect_jsonl_files(driver.root())
                .map(|files| files.len())
                .map_err(|io| AdapterError::io(driver.name(), io.path, io.source))
        })
        .await
        .map_err(|join| join_error(name, join))?
    })
}

pub(crate) fn jsonl_tree_events<'a, D: JsonlTree>(
    driver: &'a D,
    oracle: &'a dyn SkipOracle,
) -> AdapterYieldStream<'a> {
    let driver = driver.clone();
    Box::pin(stream! {
        let name = driver.name();

        let heads = {
            let driver = driver.clone();
            let oracle_is_empty = oracle.is_empty();
            tokio::task::spawn_blocking(move || collect_heads(&driver, oracle_is_empty)).await
        };
        let heads = match heads {
            Ok(Ok(heads)) => heads,
            Ok(Err(error)) => { yield Err(error); return; }
            Err(join) => { yield Err(join_error(name, join)); return; }
        };

        // Fresh skips batch: per-file yield made recurring sync ~60s on a
        // ~9k-file corpus from indicatif Mutex + per-callback work.
        let mut survivors = Vec::with_capacity(heads.len());
        let mut fresh_count = 0usize;
        for head in heads {
            if let Some(id) = &head.session_id
                && crate::adapter::is_session_fresh(oracle, id, head.last_ts)
            {
                fresh_count += 1;
                continue;
            }
            survivors.push(head.path);
        }
        if fresh_count > 0 {
            yield Ok(AdapterYield::SkippedBatch {
                reason: SkipReason::Fresh,
                count: fresh_count,
            });
        }

        let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
        let reader = driver.clone();
        let handle = tokio::task::spawn_blocking(move || read_files(&reader, survivors, &tx));
        while let Some(item) = rx.recv().await {
            yield item;
        }
        if let Err(join) = handle.await {
            yield Err(join_error(name, join));
        }
    })
}

/// A blocking-task panic is a pond bug, not bad source data, so it fails the
/// whole run rather than skipping a file.
fn join_error(name: &'static str, join: JoinError) -> AdapterError {
    AdapterError::io(
        name,
        "blocking read task",
        std::io::Error::other(join.to_string()),
    )
}

struct FileHead {
    path: PathBuf,
    session_id: Option<String>,
    last_ts: Option<i64>,
}

fn collect_heads<D: JsonlTree>(
    driver: &D,
    oracle_is_empty: bool,
) -> Result<Vec<FileHead>, AdapterError> {
    let name = driver.name();
    let files = collect_jsonl_files(driver.root())
        .map_err(|io| AdapterError::io(name, io.path, io.source))?;
    let mut heads = Vec::with_capacity(files.len());
    for path in files {
        // The freshness peek (first line -> session id, file tail -> latest
        // timestamp) costs bounded reads + JSON decodes per file. On a first-time
        // ingest (`NoopOracle` or an empty map) there is nothing to compare
        // against, so skip the peek entirely.
        let (session_id, last_ts) = if oracle_is_empty {
            (None, None)
        } else {
            let first_line = peek_first_line(&path).unwrap_or_default();
            let session_id = driver.peek_session_id(&path, &first_line);
            let last_ts = session_id.as_ref().and_then(|_| driver.peek_last_ts(&path));
            (session_id, last_ts)
        };
        heads.push(FileHead {
            path,
            session_id,
            last_ts,
        });
    }
    Ok(heads)
}

/// First non-empty line of `path`, read bounded so a pathological first line
/// cannot blow up the cheap freshness peek. Any io error yields `None`.
fn peek_first_line(path: &Path) -> Option<String> {
    let mut reader = BufReader::new(std::fs::File::open(path).ok()?);
    loop {
        let mut buf = Vec::new();
        let read = (&mut reader)
            .take(RECORD_CAP as u64)
            .read_until(b'\n', &mut buf)
            .ok()?;
        if read == 0 {
            return None;
        }
        if buf.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        return Some(String::from_utf8_lossy(&buf).into_owned());
    }
}

/// Tail of `path`: the last `min(len, cap)` bytes, so a freshness peek stays
/// cheap on multi-GB logs (codex rollouts reach several GB). The window may start
/// mid-record, but the LAST line is always whole - appends write complete lines
/// and the window runs to EOF. `None` on an empty file or any io error.
pub(crate) fn read_tail(path: &Path, cap: u64) -> Option<Vec<u8>> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    let window = len.min(cap);
    file.seek(SeekFrom::Start(len - window)).ok()?;
    let mut buf = Vec::with_capacity(window as usize);
    (&mut file).take(window).read_to_end(&mut buf).ok()?;
    Some(buf)
}

/// Default tail window for the freshness peek. Big enough that the last record is
/// whole on any realistic transcript; a single record larger than this yields a
/// `None` peek and a safe re-read.
pub(crate) const TAIL_CAP: u64 = 32 * 1024 * 1024;

/// Last non-empty line of `path` from a bounded tail window. A record larger than
/// the window, or any io error, yields `None` - the file then re-reads (safe).
pub(crate) fn peek_last_line(path: &Path) -> Option<String> {
    let buf = read_tail(path, TAIL_CAP)?;
    buf.split(|&byte| byte == b'\n')
        .rev()
        .find(|line| !line.iter().all(u8::is_ascii_whitespace))
        .map(|line| String::from_utf8_lossy(line).into_owned())
}

fn read_files<D: JsonlTree>(
    driver: &D,
    paths: Vec<PathBuf>,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) {
    for path in paths {
        if !read_one_file(driver, &path, tx) {
            return;
        }
    }
}

/// Returns `false` when the consumer dropped the receiver and the read should stop.
fn read_one_file<D: JsonlTree>(
    driver: &D,
    path: &Path,
    tx: &mpsc::Sender<Result<AdapterYield, AdapterError>>,
) -> bool {
    macro_rules! emit {
        ($item:expr) => {
            if tx.blocking_send($item).is_err() {
                return false;
            }
        };
    }

    let name = driver.name();
    let display = path.display().to_string();
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(source) => {
            emit!(Err(AdapterError::io(name, display, source)));
            return true;
        }
    };

    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut rows = Vec::new();
    let mut line = 0usize;
    loop {
        line += 1;
        match next_record(&mut reader, name, &display, line) {
            RecordOutcome::Eof => break,
            RecordOutcome::Empty => continue,
            RecordOutcome::Record(value) => rows.push(BoundedRow { line, value }),
            RecordOutcome::ParseError(error) => emit!(Err(error)),
            RecordOutcome::IoError(error) => {
                emit!(Err(error));
                return true;
            }
        }
    }

    // A file that yields no importable session - empty, sidecar-only, or an
    // unextractable header - is a benign skip, not a per-event drop. Routing it
    // through `Skipped` (session_id=None) keeps it off the in-flight session in
    // the handler, so a header failure can't be misattributed to whatever
    // session preceded it in the stream. The cause is debug-logged, not lost.
    if rows.is_empty() {
        emit!(Ok(AdapterYield::Skipped {
            session_id: None,
            project: None,
            reason: SkipReason::Empty,
        }));
        return true;
    }
    // A file the driver flags as a recognized-but-unsupported sidecar is a
    // visible failure, not a benign skip: it must never fall through to a
    // content-derived id that could merge it into another session.
    if let Some(reason) = driver.unsupported_reason(path) {
        emit!(Ok(AdapterYield::Skipped {
            session_id: None,
            project: None,
            reason: SkipReason::Unsupported(reason),
        }));
        return true;
    }
    let session = match driver.session(path, &rows) {
        Ok(session) => session,
        Err(error) => {
            tracing::debug!(%error, "skipping file with no extractable session");
            emit!(Ok(AdapterYield::Skipped {
                session_id: None,
                project: None,
                reason: SkipReason::Empty,
            }));
            return true;
        }
    };
    emit!(Ok(AdapterYield::Event(IngestEvent::Session(
        session.clone()
    ))));

    let mut state = D::State::default();
    for row in &rows {
        match driver.events_from_row(&session, row, &mut state) {
            Ok(events) => {
                for event in events {
                    emit!(Ok(AdapterYield::Event(event)));
                }
            }
            Err(message) => emit!(Err(AdapterError::schema(
                name,
                format!("{display}:{}", row.line),
                message,
            ))),
        }
    }
    true
}

enum RecordOutcome {
    Eof,
    Empty,
    Record(Value),
    ParseError(AdapterError),
    IoError(AdapterError),
}

/// Read and bound one line. A newline within `RECORD_CAP` is the fast path;
/// no newline within `RECORD_CAP` routes the oversized line to `struson`.
fn next_record<R: BufRead>(
    reader: &mut R,
    name: &'static str,
    display: &str,
    line: usize,
) -> RecordOutcome {
    let mut buf = Vec::new();
    let read = match reader.take(RECORD_CAP as u64).read_until(b'\n', &mut buf) {
        Ok(read) => read,
        Err(source) => {
            return RecordOutcome::IoError(AdapterError::io(name, display.to_owned(), source));
        }
    };
    if read == 0 {
        return RecordOutcome::Eof;
    }

    if buf.len() == RECORD_CAP && buf.last() != Some(&b'\n') {
        return read_oversized(reader, buf, name, display, line);
    }

    if buf.iter().all(u8::is_ascii_whitespace) {
        return RecordOutcome::Empty;
    }
    match serde_json::from_slice::<Value>(&buf) {
        Ok(mut value) => {
            bound_value(&mut value);
            RecordOutcome::Record(value)
        }
        Err(source) => {
            RecordOutcome::ParseError(AdapterError::parse(name, display.to_owned(), line, source))
        }
    }
}

/// Slow path: `prefix` holds the line's first `RECORD_CAP` bytes, the rest is
/// still in `reader`. `struson` cap-parses the record while `NewlineDelimited`
/// stops it at the line boundary; the trailing newline is consumed afterwards.
fn read_oversized<R: BufRead>(
    reader: &mut R,
    prefix: Vec<u8>,
    name: &'static str,
    display: &str,
    line: usize,
) -> RecordOutcome {
    let outcome = {
        let chained = Cursor::new(prefix).chain(NewlineDelimited::new(reader));
        let mut json = JsonStreamReader::new(chained);
        match capped_value(&mut json) {
            Ok(value) => RecordOutcome::Record(value),
            Err(error) => RecordOutcome::ParseError(AdapterError::schema(
                name,
                format!("{display}:{line}"),
                format!("oversized line failed to parse: {error}"),
            )),
        }
    };
    if let Err(source) = reader.read_until(b'\n', &mut Vec::new()) {
        return RecordOutcome::IoError(AdapterError::io(name, display.to_owned(), source));
    }
    outcome
}

/// A `Read` over `inner` that stops at the next `\n` and leaves it unconsumed.
/// It uses `fill_buf`/`consume` exactly, so `inner` is never advanced past the
/// newline however `struson` buffers - which keeps the next line readable.
struct NewlineDelimited<'r, R: BufRead> {
    inner: &'r mut R,
    done: bool,
}

impl<'r, R: BufRead> NewlineDelimited<'r, R> {
    fn new(inner: &'r mut R) -> Self {
        Self { inner, done: false }
    }
}

impl<R: BufRead> Read for NewlineDelimited<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.done || out.is_empty() {
            return Ok(0);
        }
        let available = self.inner.fill_buf()?;
        if available.is_empty() {
            self.done = true;
            return Ok(0);
        }
        let upto = match available.iter().position(|&b| b == b'\n') {
            Some(0) => {
                self.done = true;
                return Ok(0);
            }
            Some(at) => at,
            None => available.len(),
        };
        let n = upto.min(out.len());
        out[..n].copy_from_slice(&available[..n]);
        self.inner.consume(n);
        Ok(n)
    }
}

type CapResult<T> = Result<T, Box<dyn std::error::Error>>;

fn capped_value<R: Read>(json: &mut JsonStreamReader<R>) -> CapResult<Value> {
    match json.peek()? {
        ValueType::Null => {
            json.next_null()?;
            Ok(Value::Null)
        }
        ValueType::Boolean => Ok(Value::Bool(json.next_bool()?)),
        ValueType::Number => {
            let number = json.next_number_as_string()?;
            Ok(serde_json::from_str(&number)?)
        }
        ValueType::String => Ok(Value::String(capped_string(json)?)),
        ValueType::Array => {
            json.begin_array()?;
            let mut items = Vec::new();
            while json.has_next()? {
                items.push(capped_value(json)?);
            }
            json.end_array()?;
            Ok(Value::Array(items))
        }
        ValueType::Object => {
            json.begin_object()?;
            let mut map = serde_json::Map::new();
            while json.has_next()? {
                let key = json.next_name_owned()?;
                map.insert(key, capped_value(json)?);
            }
            json.end_object()?;
            Ok(Value::Object(map))
        }
    }
}

/// Read one JSON string, capping it at `LEAF_CAP`. The value streams through
/// `struson`'s incremental string reader, so bytes past the cap are counted
/// and discarded rather than held.
fn capped_string<R: Read>(json: &mut JsonStreamReader<R>) -> CapResult<String> {
    let mut value = json.next_string_reader()?;
    let mut head = Vec::new();
    (&mut value)
        .take(LEAF_CAP as u64 + 1)
        .read_to_end(&mut head)?;
    if head.len() <= LEAF_CAP {
        return Ok(String::from_utf8_lossy(&head).into_owned());
    }

    let mut original = head.len();
    let mut sink = vec![0u8; 256 * 1024];
    loop {
        let read = value.read(&mut sink)?;
        if read == 0 {
            break;
        }
        original += read;
    }
    drop(value);

    Ok(truncate_to_marker(&head, original))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::adapter::extract::truncated_values_count;

    fn parse_line(line: &[u8]) -> Value {
        let mut reader = BufReader::new(Cursor::new(line.to_vec()));
        match next_record(&mut reader, "test", "mem", 1) {
            RecordOutcome::Record(value) => value,
            _ => panic!("expected a record"),
        }
    }

    #[test]
    fn small_record_round_trips_unchanged() {
        let value = parse_line(br#"{"a":"hi","b":[1,2,{"c":true}]}"#);
        assert_eq!(value["a"], "hi");
        assert_eq!(value["b"][2]["c"], true);
    }

    #[test]
    fn fast_path_truncates_only_the_oversized_leaf() {
        let big = "x".repeat(LEAF_CAP + 4096);
        let line = format!(r#"{{"keep":"small","huge":"{big}","tail":"end"}}"#);
        let value = parse_line(line.as_bytes());
        assert_eq!(value["keep"], "small");
        assert_eq!(value["tail"], "end");
        let huge = value["huge"].as_str().unwrap();
        assert!(huge.len() <= LEAF_CAP);
        assert!(huge.ends_with(&format!("{} bytes>", LEAF_CAP + 4096)));
    }

    #[test]
    fn slow_path_caps_the_violating_leaf_and_keeps_the_rest() {
        let before = truncated_values_count();
        let huge = "y".repeat(RECORD_CAP + LEAF_CAP);
        let line = format!(r#"{{"head":"a","huge":"{huge}","after":"z"}}"#);
        let value = parse_line(line.as_bytes());
        assert_eq!(value["head"], "a");
        assert_eq!(value["after"], "z");
        let capped = value["huge"].as_str().unwrap();
        assert!(capped.len() <= LEAF_CAP);
        assert!(capped.ends_with(&format!("{} bytes>", RECORD_CAP + LEAF_CAP)));
        assert!(truncated_values_count() > before);
    }

    #[test]
    fn slow_path_leaves_the_next_line_readable() {
        let huge = "z".repeat(RECORD_CAP + 16);
        let corpus = format!("{{\"a\":\"{huge}\"}}\n{{\"b\":\"next\"}}\n");
        let mut reader = BufReader::new(Cursor::new(corpus.into_bytes()));
        let first = match next_record(&mut reader, "test", "mem", 1) {
            RecordOutcome::Record(value) => value,
            _ => panic!("expected first record"),
        };
        assert!(first["a"].as_str().unwrap().len() <= LEAF_CAP);
        let second = match next_record(&mut reader, "test", "mem", 2) {
            RecordOutcome::Record(value) => value,
            _ => panic!("expected second record"),
        };
        assert_eq!(second["b"], "next");
    }
}
