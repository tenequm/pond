#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Ingest-path microbenchmark. Drives [`ingest_adapter`] against one or more
//! claude-code corpora and emits a structured per-stage breakdown. The probe
//! captures `pond::perf` tracing events (emitted by [`ingest_adapter`] and
//! [`Handle::merge_insert`]) into an in-memory aggregator so each run prints:
//!
//! - **total wall** time end-to-end
//! - **decode** time (file walk + JSON parse, summed across `events.next()`)
//! - **validator** time (per-event push + final flush, the path that fans out
//!   to the three `merge_insert` calls per session)
//! - **merge_insert** breakdown per table (call count, total, mean, min, max,
//!   row count) - the load-bearing detail for batching / concurrency work
//! - **error class split**: adapter-level skips vs validator-rejected rows
//!
//! Reproducible: identical inputs produce identical aggregates modulo wall
//! noise. This is the harness that proves a perf optimization is real before
//! it ships, and that it stayed real after it shipped.
//!
//! Run:
//!   cargo bench --bench ingest_bench -- --source-dir ~/.claude/projects/-Users-tenequm-Projects-blackbox
//!   cargo bench --bench ingest_bench -- --source-dir <a> --source-dir <b>
//!   cargo bench --bench ingest_bench               # defaults to the fixture corpus

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::Result;
use clap::Parser;
use pond::{
    adapter::ClaudeCodeAdapter,
    handlers::{SyncEvent, SyncStatus, ingest_adapter},
    sessions::Store,
};
use tempfile::TempDir;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;

/// The committed redacted fixture corpus - the no-arg default so a fresh
/// clone always has something to bench against.
const FIXTURE_CORPUS: &str = "tests/fixtures/session-samples/claude-code/projects";

#[derive(Parser)]
#[command(about = "pond ingest-path microbenchmark: per-stage timing and merge_insert breakdown")]
struct Args {
    /// One or more source-dirs to bench, in order. Defaults to the committed
    /// fixture corpus when none are given.
    #[arg(long, value_name = "PATH")]
    source_dir: Vec<PathBuf>,
    /// Print the top reasons sessions get skipped or validator-rejected. Used
    /// to diagnose "why is my rejection rate so high?" without grepping the
    /// bar output (which indicatif suppresses on non-tty stderr).
    #[arg(long)]
    show_rejections: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let corpora = if args.source_dir.is_empty() {
        vec![PathBuf::from(FIXTURE_CORPUS)]
    } else {
        args.source_dir.clone()
    };

    // Wire the perf-event collector as a tracing layer. The capture is silent
    // (no console output) - only `report()` materializes the aggregate.
    let capture = Arc::new(PerfCapture::default());
    tracing_subscriber::registry()
        .with(PerfLayer {
            sink: Arc::clone(&capture),
        })
        .init();

    for corpus in corpora {
        capture.reset();
        let file_count = count_jsonl(&corpus);
        let temp = TempDir::new()?;
        let store = Store::open_local(temp.path()).await?;
        let adapter = ClaudeCodeAdapter::new(&corpus);

        let mut sync_skips = 0u64;
        let mut sync_errors = 0u64;
        // Bucket rejection reasons so we can show top causes per corpus. The
        // key is the leading prefix of the reason (first line, first 120
        // chars) so near-duplicate messages collapse onto one row.
        let mut skip_reasons: HashMap<String, u64> = HashMap::new();
        let mut error_reasons: HashMap<String, u64> = HashMap::new();
        let started = Instant::now();
        let mut sync_partial = 0u64;
        let mut sync_partial_drops = 0u64;
        let summary = ingest_adapter(&store, &adapter, |event| {
            if let SyncEvent::SessionDone(outcome) = event {
                match &outcome.status {
                    SyncStatus::Skipped { reason } => {
                        sync_skips += 1;
                        *skip_reasons.entry(bucket_reason(reason)).or_default() += 1;
                    }
                    SyncStatus::Rejected { reason } => {
                        sync_errors += 1;
                        *error_reasons.entry(bucket_reason(reason)).or_default() += 1;
                    }
                    SyncStatus::Partial { dropped_events } => {
                        sync_partial += 1;
                        sync_partial_drops += *dropped_events as u64;
                    }
                    SyncStatus::Ok => {}
                }
            }
        })
        .await?;
        let wall = started.elapsed();

        report(Report {
            corpus: &corpus,
            files: file_count,
            wall_ms: wall.as_millis() as u64,
            inserted: summary.inserted as u64,
            matched: summary.matched as u64,
            dropped_events: summary.dropped_events as u64,
            dropped_sessions: summary.dropped_sessions as u64,
            skipped_files: summary.skipped_files as u64,
            sync_skips,
            sync_errors,
            sync_partial,
            sync_partial_drops,
            capture: capture.snapshot(),
        });

        if args.show_rejections {
            print_top_reasons("validator-error reasons", &error_reasons);
            print_top_reasons("adapter-skip reasons", &skip_reasons);
        }
    }
    Ok(())
}

/// Collapse near-duplicate rejection reasons onto one key: drop trailing
/// path/line context after the first newline and cap at 120 chars. Keeps
/// the histogram small without losing the human-meaningful prefix.
fn bucket_reason(reason: &str) -> String {
    let first_line = reason.lines().next().unwrap_or(reason);
    let truncated: String = first_line.chars().take(120).collect();
    truncated.trim().to_owned()
}

fn print_top_reasons(label: &str, reasons: &HashMap<String, u64>) {
    if reasons.is_empty() {
        return;
    }
    let mut entries: Vec<(&String, &u64)> = reasons.iter().collect();
    entries.sort_by(|a, b| b.1.cmp(a.1));
    println!("  {label}:");
    let total: u64 = reasons.values().sum();
    for (reason, count) in entries.into_iter().take(10) {
        println!("    {count:>5}  {reason}");
    }
    if reasons.len() > 10 {
        let shown: u64 = reasons.iter().take(10).map(|(_, c)| *c).sum();
        println!(
            "    {:>5}  (... {} more reasons)",
            total - shown,
            reasons.len() - 10
        );
    }
    println!();
}

fn count_jsonl(root: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, count: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(root, &mut count);
    count
}

/// The `pond::perf` events that [`PerfLayer`] captures, accumulated by the
/// aggregator. One variant per probe site so the report is self-describing.
#[derive(Debug, Default)]
struct PerfCapture {
    inner: Mutex<PerfCaptureInner>,
}

#[derive(Debug, Default)]
struct PerfCaptureInner {
    merge_inserts: HashMap<String, Vec<(u64, u64)>>, // table -> [(rows, ms)]
    summary: Option<IngestAdapterSummary>,
}

#[derive(Debug, Clone, Copy)]
struct IngestAdapterSummary {
    total_ms: u64,
    decode_ms: u64,
    validator_ms: u64,
    other_ms: u64,
    decode_calls: u64,
    validator_calls: u64,
}

impl PerfCapture {
    fn reset(&self) {
        let mut g = self.inner.lock().unwrap();
        g.merge_inserts.clear();
        g.summary = None;
    }
    fn snapshot(&self) -> PerfSnapshot {
        let g = self.inner.lock().unwrap();
        PerfSnapshot {
            merge_inserts: g.merge_inserts.clone(),
            summary: g.summary,
        }
    }
}

#[derive(Debug, Clone)]
struct PerfSnapshot {
    merge_inserts: HashMap<String, Vec<(u64, u64)>>,
    summary: Option<IngestAdapterSummary>,
}

struct PerfLayer {
    sink: Arc<PerfCapture>,
}

impl<S: Subscriber> Layer<S> for PerfLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let meta = event.metadata();
        // Only events tagged with `target: "pond::perf"` count. Lance and
        // pond's own non-probe lines pass through untouched.
        if meta.target() != "pond::perf" {
            return;
        }
        let mut fields = FieldCollector::default();
        event.record(&mut fields);
        let message = fields.message.as_deref().unwrap_or("");
        match message {
            "merge_insert" => {
                let table = fields.string("table").unwrap_or_else(|| "?".to_owned());
                let rows = fields.u64("rows").unwrap_or(0);
                let ms = fields.u64("elapsed_ms").unwrap_or(0);
                let mut g = self.sink.inner.lock().unwrap();
                g.merge_inserts.entry(table).or_default().push((rows, ms));
            }
            "ingest_adapter complete" => {
                let summary = IngestAdapterSummary {
                    total_ms: fields.u64("total_ms").unwrap_or(0),
                    decode_ms: fields.u64("decode_ms").unwrap_or(0),
                    validator_ms: fields.u64("validator_ms").unwrap_or(0),
                    other_ms: fields.u64("other_ms").unwrap_or(0),
                    decode_calls: fields.u64("decode_calls").unwrap_or(0),
                    validator_calls: fields.u64("validator_calls").unwrap_or(0),
                };
                let mut g = self.sink.inner.lock().unwrap();
                g.summary = Some(summary);
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct FieldCollector {
    message: Option<String>,
    strings: HashMap<String, String>,
    ints: HashMap<String, u64>,
}

impl FieldCollector {
    fn string(&self, name: &str) -> Option<String> {
        self.strings.get(name).cloned()
    }
    fn u64(&self, name: &str) -> Option<u64> {
        self.ints.get(name).copied()
    }
}

impl Visit for FieldCollector {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.strings
            .insert(field.name().to_owned(), value.to_owned());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.ints.insert(field.name().to_owned(), value);
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        if value >= 0 {
            self.ints.insert(field.name().to_owned(), value as u64);
        }
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let formatted = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(formatted.trim_matches('"').to_owned());
        } else {
            self.strings.insert(
                field.name().to_owned(),
                formatted.trim_matches('"').to_owned(),
            );
        }
    }
}

struct Report<'a> {
    corpus: &'a std::path::Path,
    files: u64,
    wall_ms: u64,
    inserted: u64,
    matched: u64,
    dropped_events: u64,
    dropped_sessions: u64,
    skipped_files: u64,
    sync_skips: u64,
    sync_errors: u64,
    sync_partial: u64,
    sync_partial_drops: u64,
    capture: PerfSnapshot,
}

fn report(r: Report<'_>) {
    println!("=== {} ({} jsonl files) ===", r.corpus.display(), r.files);
    let wall_s = (r.wall_ms as f64) / 1000.0;
    println!("wall              {wall_s:>7.2} s");
    println!(
        "rows              inserted={}  matched={}",
        r.inserted, r.matched
    );
    println!(
        "summary           dropped_events={}  dropped_sessions={}  skipped_files={}",
        r.dropped_events, r.dropped_sessions, r.skipped_files
    );
    println!(
        "session outcomes  skipped(file)={}  rejected(session)={}  partial(session)={} (drops={})",
        r.sync_skips, r.sync_errors, r.sync_partial, r.sync_partial_drops
    );
    if let Some(s) = r.capture.summary {
        let t = s.total_ms.max(1);
        println!(
            "stages            decode={:.2}s ({:>2.0}%)  validator={:.2}s ({:>2.0}%)  other={:.2}s ({:>2.0}%)",
            s.decode_ms as f64 / 1000.0,
            100.0 * s.decode_ms as f64 / t as f64,
            s.validator_ms as f64 / 1000.0,
            100.0 * s.validator_ms as f64 / t as f64,
            s.other_ms as f64 / 1000.0,
            100.0 * s.other_ms as f64 / t as f64,
        );
        println!(
            "calls             decode_calls={}  validator_calls={}",
            s.decode_calls, s.validator_calls
        );
    }
    let mut total_mi_ms = 0u64;
    for table in ["sessions", "messages", "parts", "embeddings"] {
        if let Some(rows) = r.capture.merge_inserts.get(table) {
            let n = rows.len() as u64;
            let row_sum: u64 = rows.iter().map(|(rows, _)| rows).sum();
            let ms_sum: u64 = rows.iter().map(|(_, ms)| ms).sum();
            let ms_min = rows.iter().map(|(_, ms)| *ms).min().unwrap_or(0);
            let ms_max = rows.iter().map(|(_, ms)| *ms).max().unwrap_or(0);
            let mean = if n > 0 { ms_sum as f64 / n as f64 } else { 0.0 };
            total_mi_ms += ms_sum;
            println!(
                "merge_insert {:10}  calls={:>5}  total={:>5.2}s  mean={:>5.1}ms  min={:>3}ms  max={:>4}ms  rows={}",
                table,
                n,
                ms_sum as f64 / 1000.0,
                mean,
                ms_min,
                ms_max,
                row_sum,
            );
        }
    }
    if let Some(s) = r.capture.summary {
        let denom = s.total_ms.max(1);
        let pct = 100.0 * total_mi_ms as f64 / denom as f64;
        println!(
            "merge_insert SUM  {:.2}s  ({:.0}% of total)",
            total_mi_ms as f64 / 1000.0,
            pct
        );
    }
    println!();
}
