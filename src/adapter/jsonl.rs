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
    AdapterError, AdapterYield, AdapterYieldStream, DiscoverFuture, PlanFuture, SkipOracle,
    SkipReason, SourceWatermark, SyncPlan,
    extract::{LEAF_CAP, bound_value, truncate_to_marker},
    source_in_sync,
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
    /// Every root this driver reads from - almost always one, but an
    /// adapter configured with `paths = [...]` pools more than one directory
    /// into a single walk (spec.md#adapter-multi-root).
    fn roots(&self) -> &[PathBuf];

    /// Session id for the freshness gate, from a file's raw first non-empty
    /// line; `None` disables the skip for that file.
    fn peek_session_id(&self, path: &Path, first_line: &str) -> Option<String>;

    /// Source-side freshness verdict for a non-empty file: the latest
    /// ingestible-content timestamp ([`SourceWatermark::At`], compared against
    /// pond's stored max), a proof there is nothing to ingest
    /// ([`SourceWatermark::Empty`]), or undeterminable
    /// ([`SourceWatermark::Opaque`] - the file re-reads). The driver reads the
    /// file tail (the last message line, or a bounded backward scan). Zero-byte
    /// files are `Empty` at the seam before this is called.
    fn peek_watermark(&self, path: &Path) -> SourceWatermark;

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

    /// A file the adapter knows carries no session data (a runner control file
    /// whose content duplicates real transcripts). Excluded from the walk
    /// entirely: never counted as a source, never read, never pending. Only for
    /// files whose non-session nature is structural and certain - anything the
    /// adapter merely can't decode must stay IN the walk so it surfaces as a
    /// visible skip instead of vanishing. Default: no such category.
    fn skip_source(&self, _path: &Path) -> bool {
        false
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

/// [`collect_jsonl_files`] over every root, concatenated. Roots are deduped
/// and never nested (`config_roots` guarantees this), so each file appears
/// under exactly one root; the final sort makes ingest order deterministic
/// and independent of root order, matching the single-root walk it replaces.
///
/// A root that doesn't exist (yet) is skipped with a warning, not a hard
/// error - the same tolerance `pond watch` already applies to a root it
/// can't watch (watch.rs). A single second root missing on a machine that
/// hasn't written to it yet must not break sync for every OTHER configured
/// root; the root reappearing (e.g. a work laptop mounting its dir) is
/// picked up on the next sync with no special handling. A real permissions
/// or other io error still fails loudly.
pub(crate) fn collect_jsonl_files_multi(roots: &[PathBuf]) -> Result<Vec<PathBuf>, IoAtPath> {
    let mut paths = Vec::new();
    for root in roots {
        match collect_jsonl_files(root) {
            Ok(files) => paths.extend(files),
            Err(io) if io.source.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    root = %root.display(),
                    "configured source root does not exist yet; skipping (picked up once it appears)",
                );
            }
            Err(io) => return Err(io),
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
            collect_jsonl_files_multi(driver.roots())
                .map(|files| {
                    files
                        .iter()
                        .filter(|path| !driver.skip_source(path))
                        .count()
                })
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
            if source_in_sync(oracle, head.session_id.as_deref(), head.watermark) {
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

/// The freshness gate of [`jsonl_tree_events`] run standalone: the same
/// `collect_heads` peek, classified instead of read. On an empty oracle the
/// peek is skipped entirely, so a first sync's plan is "everything pending"
/// at directory-walk cost.
pub(crate) fn jsonl_tree_plan<'a, D: JsonlTree>(
    driver: &'a D,
    oracle: &'a dyn SkipOracle,
) -> PlanFuture<'a> {
    let name = driver.name();
    Box::pin(async move {
        let heads = {
            let driver = driver.clone();
            let oracle_is_empty = oracle.is_empty();
            tokio::task::spawn_blocking(move || collect_heads(&driver, oracle_is_empty))
                .await
                .map_err(|join| join_error(name, join))??
        };
        if oracle.is_empty() {
            return Ok(Some(SyncPlan::all_pending(heads.len())));
        }
        Ok(Some(SyncPlan::from_heads(
            oracle,
            heads
                .iter()
                .map(|head| (head.session_id.as_deref(), head.watermark)),
        )))
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
    watermark: SourceWatermark,
}

fn collect_heads<D: JsonlTree>(
    driver: &D,
    oracle_is_empty: bool,
) -> Result<Vec<FileHead>, AdapterError> {
    let name = driver.name();
    let mut files = collect_jsonl_files_multi(driver.roots())
        .map_err(|io| AdapterError::io(name, io.path, io.source))?;
    files.retain(|path| !driver.skip_source(path));
    // The freshness peek (first line -> session id, file tail -> latest
    // timestamp) costs bounded reads + JSON decodes per file. On a first-time
    // ingest (`NoopOracle` or an empty map) there is nothing to compare
    // against, so skip the peek entirely.
    if oracle_is_empty {
        return Ok(files
            .into_iter()
            .map(|path| FileHead {
                path,
                session_id: None,
                watermark: SourceWatermark::Opaque,
            })
            .collect());
    }
    // Peeks are independent per-file io; fan them out over contiguous chunks
    // of the already-sorted file list and stitch results back in chunk order,
    // so head order (and therefore ingest order) is byte-identical to the
    // sequential walk. Already inside `spawn_blocking`, so scoped OS threads
    // are the right tool - no async machinery.
    let workers = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
        .min(8);
    let chunk_size = files.len().div_ceil(workers).max(1);
    let mut heads = Vec::with_capacity(files.len());
    std::thread::scope(|scope| {
        let handles: Vec<_> = files
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(|path| peek_head(driver, path))
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        for handle in handles {
            let chunk_heads = handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            heads.extend(chunk_heads);
        }
    });
    Ok(heads)
}

fn peek_head<D: JsonlTree>(driver: &D, path: &Path) -> FileHead {
    // A zero-byte file provably holds nothing to ingest, in any format - the
    // one `Empty` verdict the seam owns. Drivers only judge non-empty files.
    if std::fs::metadata(path).is_ok_and(|meta| meta.len() == 0) {
        return FileHead {
            path: path.to_owned(),
            session_id: None,
            watermark: SourceWatermark::Empty,
        };
    }
    let first_line = peek_first_line(path).unwrap_or_default();
    let session_id = driver.peek_session_id(path, &first_line);
    let watermark = if session_id.is_some() {
        driver.peek_watermark(path)
    } else {
        // No readable session id: the stored side can't be looked up, so a
        // watermark would be unusable - re-read (and let a deliberately
        // id-less file keep surfacing its `unsupported_reason` every sync).
        SourceWatermark::Opaque
    };
    FileHead {
        path: path.to_owned(),
        session_id,
        watermark,
    }
}

/// First non-empty line of `path`, read bounded so a pathological first line
/// cannot blow up the cheap freshness peek. Any io error yields `None`.
pub(crate) fn peek_first_line(path: &Path) -> Option<String> {
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

/// Escalation ceiling for the freshness peek. Big enough that the last record is
/// whole on any realistic transcript; a single record larger than this yields a
/// `None` peek and a safe re-read.
pub(crate) const TAIL_CAP: u64 = 32 * 1024 * 1024;

/// First-pass tail window for the freshness peek. The watermark line sits within
/// the last few KB on virtually every real transcript; escalating to [`TAIL_CAP`]
/// only on a miss cut the per-sync peek read from 6.63GB to 0.59GB on the real
/// corpus with identical coverage.
pub(crate) const PEEK_TAIL_CAP: u64 = 64 * 1024;

/// Tail window restricted to whole lines: the raw `read_tail` buffer plus the
/// offset of its first complete line - 0 when the window provably covers the
/// whole file, else one past the first newline, discarding the possibly
/// mid-record first chunk. This is the escalation's correctness guard: the
/// small pass never judges a truncated line. `None` when the window holds no
/// whole line at all (single line larger than `cap`), which escalates.
fn tail_whole_lines(path: &Path, cap: u64) -> Option<(Vec<u8>, usize)> {
    let buf = read_tail(path, cap)?;
    let start = if (buf.len() as u64) < cap {
        0
    } else {
        buf.iter().position(|&byte| byte == b'\n')? + 1
    };
    Some((buf, start))
}

/// Last-to-first walk over the non-empty lines of `buf`, returning the first
/// that `pick` maps to `Some`.
fn pick_lines_rev<T>(buf: &[u8], pick: &impl Fn(&str) -> Option<T>) -> Option<T> {
    buf.split(|&byte| byte == b'\n')
        .rev()
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .find_map(|line| pick(&String::from_utf8_lossy(line)))
}

/// Last non-empty line of `path` from a bounded tail window. A record larger than
/// the window, or any io error, yields `None` - the file then re-reads (safe).
pub(crate) fn peek_last_line(path: &Path) -> Option<String> {
    peek_last_mapped(path, |line| Some(line.to_owned()))
}

/// Walk non-empty lines of the tail window from last to first, returning the first
/// that `pick` maps to `Some`. Lets an adapter skip trailing non-message records to
/// find the latest real message's watermark - e.g. Claude Code appends metadata
/// rows (`last-prompt`, `permission-mode`, ...) with no timestamp after the
/// conversation, so the literal last line is rarely the watermark. Short-circuits
/// at the first match (the newest message sits just behind that trailing metadata),
/// so it stays as cheap as a single-line peek in practice.
///
/// Two passes: a [`PEEK_TAIL_CAP`] window over whole lines only, then - if that
/// found nothing - the full [`TAIL_CAP`] window with the historical semantics.
/// Every outcome is therefore either an exact match from a complete line or
/// exactly what the single big window would have returned.
pub(crate) fn peek_last_mapped<T>(path: &Path, pick: impl Fn(&str) -> Option<T>) -> Option<T> {
    peek_last_mapped_with_caps(path, PEEK_TAIL_CAP, TAIL_CAP, pick)
}

fn peek_last_mapped_with_caps<T>(
    path: &Path,
    small_cap: u64,
    full_cap: u64,
    pick: impl Fn(&str) -> Option<T>,
) -> Option<T> {
    if let Some((buf, start)) = tail_whole_lines(path, small_cap)
        && let Some(found) = pick_lines_rev(&buf[start..], &pick)
    {
        return Some(found);
    }
    let buf = read_tail(path, full_cap)?;
    pick_lines_rev(&buf, &pick)
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

    fn pick_ts(line: &str) -> Option<i64> {
        serde_json::from_str::<Value>(line).ok()?["ts"].as_i64()
    }

    fn write_peek_file(dir: &tempfile::TempDir, name: &str, content: &str) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn peek_escalates_when_the_small_window_misses_the_watermark() {
        let dir = tempfile::TempDir::new().unwrap();
        // Watermark line, then enough timestamp-less metadata to push it out
        // of a 64-byte first pass entirely.
        let content = format!("{{\"ts\":7}}\n{}", "{\"m\":1}\n".repeat(10));
        let path = write_peek_file(&dir, "escalate.jsonl", &content);
        assert_eq!(
            peek_last_mapped_with_caps(&path, 64, TAIL_CAP, pick_ts),
            Some(7),
            "the escalated pass must find what the small window cannot",
        );
    }

    #[test]
    fn peek_never_matches_a_truncated_first_chunk() {
        let dir = tempfile::TempDir::new().unwrap();
        // Line B is garbage whose final 10 bytes parse as `{"ts":999}` on
        // their own. Trailing metadata is sized so the 64-byte window starts
        // exactly at that fragment: an unguarded walk would return 999; the
        // whole-lines pass discards the fragment, escalates, and finds the
        // real watermark on line A.
        let line_a = "{\"ts\":111}\n";
        let line_b = format!("{}{{\"ts\":999}}\n", "X".repeat(30));
        let meta = format!("{{\"pad\":\"{}\"}}\n", "y".repeat(42));
        assert_eq!(meta.len(), 53, "fragment(10) + newline(1) + meta = 64");
        let path = write_peek_file(&dir, "cut.jsonl", &format!("{line_a}{line_b}{meta}"));
        assert_eq!(
            peek_last_mapped_with_caps(&path, 64, TAIL_CAP, pick_ts),
            Some(111),
            "a mid-line fragment must never satisfy the peek",
        );
    }

    #[test]
    fn peek_small_window_covers_a_small_file_from_its_first_line() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_peek_file(&dir, "small.jsonl", "{\"ts\":42}\n{\"m\":1}\n");
        assert_eq!(
            peek_last_mapped_with_caps(&path, 64, TAIL_CAP, pick_ts),
            Some(42),
            "a window covering the whole file must consider its first line",
        );
    }

    #[derive(Clone)]
    struct PeekTree {
        roots: Vec<PathBuf>,
    }

    impl JsonlTree for PeekTree {
        type State = ();

        fn name(&self) -> &'static str {
            "peek-test"
        }
        fn roots(&self) -> &[PathBuf] {
            &self.roots
        }
        fn peek_session_id(&self, path: &Path, _first_line: &str) -> Option<String> {
            Some(path.file_stem()?.to_str()?.to_owned())
        }
        fn peek_watermark(&self, _path: &Path) -> SourceWatermark {
            SourceWatermark::At(1)
        }
        fn session(&self, _path: &Path, _rows: &[BoundedRow]) -> Result<Session, AdapterError> {
            unreachable!("collect_heads never builds sessions")
        }
        fn events_from_row(
            &self,
            _session: &Session,
            _row: &BoundedRow,
            _state: &mut Self::State,
        ) -> Result<Vec<IngestEvent>, String> {
            unreachable!("collect_heads never reads rows")
        }
    }

    #[test]
    fn parallel_peek_keeps_heads_in_sorted_path_order() {
        let dir = tempfile::TempDir::new().unwrap();
        // More files than the worker cap, created out of name order.
        for i in [7, 3, 19, 0, 12, 5, 16, 1, 9, 14, 2, 18, 4, 11, 6] {
            write_peek_file(&dir, &format!("s{i:02}.jsonl"), "{\"ts\":1}\n");
        }
        let tree = PeekTree {
            roots: vec![dir.path().to_owned()],
        };
        let heads = collect_heads(&tree, false).unwrap();
        let paths: Vec<_> = heads.iter().map(|head| head.path.clone()).collect();
        let mut sorted = paths.clone();
        sorted.sort();
        assert_eq!(paths, sorted, "head order must stay deterministic");
        assert!(
            heads
                .iter()
                .all(|head| head.session_id.is_some() && head.watermark == SourceWatermark::At(1)),
            "the peeked fields must survive the parallel fan-out",
        );
    }
}
