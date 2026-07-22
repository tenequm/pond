#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Embedding stress-test harness: a repeatable run of the real [`EmbedWorker`]
//! over a corpus, instrumented for the three questions that matter when sizing
//! the embedding path:
//!
//! - **peak memory** - a background thread samples process RSS; the report is
//!   the max. Run the same workload over corpora of different sizes: if peak
//!   RSS tracks the *limit* and not the *data*, memory is capped.
//! - **throughput** - end-to-end messages/sec, split into pure e5 (candle)
//!   inference vs. the merge-update store write, plus a per-batch breakdown.
//!   The report also prints the device the model ran on (`metal` / `cpu`).
//! - **padding waste** - the tokenizer pads every batch to its longest member,
//!   so a batch mixing short and long messages embeds the short ones at the
//!   long one's length. `padding_waste` is the fraction of embedded
//!   token-bytes that were padding, not content.
//!
//! It links the `pond` library and uses only public API - changing the worker
//! breaks this at `cargo check --benches`, so it cannot rot silently.
//!
//! Run (the `bench` profile is release-optimized):
//!   cargo bench --bench embed_bench -- --limit 200
//!   cargo bench --bench embed_bench -- --window 32   (length-sort disabled)

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use pond::{
    adapter::ClaudeCodeAdapter,
    embed::{CandleEmbedder, DEFAULT_BATCH_SIZE, DEFAULT_SORT_WINDOW, EmbedWorker, Embedder},
    handlers::ingest_adapter,
    sessions::Store,
};
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser)]
#[command(
    about = "pond embedding stress-test: peak memory, throughput, and padding-waste breakdown"
)]
struct Args {
    /// Corpus to ingest (a claude-code project tree). Default: the gitignored
    /// `benches/corpus/` real-session set if present, else the committed
    /// `tests/fixtures` set so a fresh clone still runs.
    #[arg(long)]
    source_dir: Option<PathBuf>,
    /// Messages to embed, the stable comparison workload. 0 = no cap (embed
    /// the whole corpus).
    #[arg(long, default_value_t = 50)]
    limit: usize,
    /// RSS sampling interval in milliseconds.
    #[arg(long, default_value_t = 150)]
    rss_interval_ms: u64,
    /// Length-sort window: messages buffered and sorted by length before model
    /// batching. Omitted uses the worker default; `32` disables sorting.
    #[arg(long)]
    window: Option<usize>,
    /// Model-inference batch size: messages per `embed()` call. Omitted uses
    /// the worker default ([`DEFAULT_BATCH_SIZE`]). Swept to find the
    /// throughput-optimal batch on this device.
    #[arg(long)]
    batch: Option<usize>,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// The gitignored real-session corpus, if it has been populated.
const LOCAL_CORPUS: &str = "benches/corpus";
/// The committed redacted fixture corpus - always present.
const FIXTURE_CORPUS: &str = "tests/fixtures/adapter/claude_code/projects";

impl Args {
    /// Resolve the corpus path: an explicit `--source-dir` wins, otherwise the
    /// local real-session set if present, otherwise the committed fixtures.
    fn corpus(&self) -> PathBuf {
        if let Some(dir) = &self.source_dir {
            return dir.clone();
        }
        let local = PathBuf::from(LOCAL_CORPUS);
        if local.is_dir() {
            local
        } else {
            PathBuf::from(FIXTURE_CORPUS)
        }
    }
}

/// One `embed()` call's shape and timing.
struct BatchStat {
    count: usize,
    /// Longest input text in the batch (bytes) - what the batch is padded to.
    max_bytes: usize,
    /// Sum of input text lengths (bytes) - the real content.
    sum_bytes: usize,
    elapsed_ms: u128,
}

/// Wraps the real embedder, recording the shape and wall time of every
/// `embed()` call - one call is one worker batch.
struct InstrumentedBackend<'a> {
    inner: &'a dyn Embedder,
    calls: Mutex<Vec<BatchStat>>,
}

impl Embedder for InstrumentedBackend<'_> {
    fn device(&self) -> &str {
        self.inner.device()
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let count = texts.len();
        let max_bytes = texts.iter().map(String::len).max().unwrap_or(0);
        let sum_bytes: usize = texts.iter().map(String::len).sum();
        let start = Instant::now();
        let vectors = self.inner.embed(texts)?;
        let elapsed_ms = start.elapsed().as_millis();

        let mut calls = self.calls.lock().unwrap();
        calls.push(BatchStat {
            count,
            max_bytes,
            sum_bytes,
            elapsed_ms,
        });
        // Live per-batch line to stderr (stdout stays reserved for the final
        // report + JSON). This is what lets you watch the cold-to-steady curve
        // and per-batch padding waste *as the run goes*, not only at the end.
        let padded = count.saturating_mul(max_bytes);
        let waste_pct = sum_bytes
            .saturating_mul(100)
            .checked_div(padded)
            .map_or(0, |used_pct| 100 - used_pct);
        eprintln!(
            "  batch {:>3}  count={:>3}  max_bytes={:>8}  elapsed={:>7}ms  waste={:>3}%",
            calls.len(),
            count,
            max_bytes,
            elapsed_ms,
            waste_pct,
        );

        Ok(vectors)
    }
}

/// A background RSS sampler. `peak` holds the max RSS in KB seen so far; the
/// returned handle stops the thread on `stop()`.
struct RssSampler {
    peak_kb: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start(interval: Duration) -> Self {
        let peak_kb = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let pid = std::process::id().to_string();
        let handle = {
            let peak_kb = Arc::clone(&peak_kb);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(kb) = sample_rss_kb(&pid) {
                        peak_kb.fetch_max(kb, Ordering::Relaxed);
                    }
                    thread::sleep(interval);
                }
            })
        };
        Self {
            peak_kb,
            stop,
            handle: Some(handle),
        }
    }

    /// Stop sampling and return the peak RSS in KB.
    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
        self.peak_kb.load(Ordering::Relaxed)
    }
}

/// Read this process's resident set size in KB via `ps` (portable across
/// macOS and Linux; a benchmark harness can afford the fork).
fn sample_rss_kb(pid: &str) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", pid])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

/// Render a byte count with a binary unit - readable for both a 13-message
/// fixture and a full corpus.
#[allow(clippy::cast_precision_loss)]
fn human_bytes(n: usize) -> String {
    let bytes = n as f64;
    if bytes >= 1024.0 * 1024.0 {
        format!("{:.1} MiB", bytes / (1024.0 * 1024.0))
    } else if bytes >= 1024.0 {
        format!("{:.1} KiB", bytes / 1024.0)
    } else {
        format!("{n} B")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_env("POND_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("warn"));
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();

    // Ingest sessions/messages/parts only - no model, fast. Not under
    // measurement, so the RSS sampler is not running yet.
    let corpus = args.corpus();
    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    let ingest_start = Instant::now();
    ingest_adapter(
        &store,
        &ClaudeCodeAdapter::new(&corpus),
        &pond::adapter::NoopOracle,
        |_| {},
    )
    .await?;
    // spec.md#fold-on-write: ingest_adapter already folded FTS + scalars.
    let ingest_elapsed = ingest_start.elapsed();
    let (sessions, messages, parts) = store.row_counts().await?;

    // Start RSS sampling *before* model load: weight loading is a real
    // transient (the safetensors are mmap'd and the candle model is built on
    // the GPU), and the report covers "the whole run, model load included", so
    // the sampler must be live for it. No warmup beyond that: the optimize embed
    // stage runs
    // once and pays the cold start once, so the honest number includes it - the
    // per-batch table shows the cold-to-steady curve directly.
    let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));

    let load_start = Instant::now();
    let embedder = CandleEmbedder::load()?;
    let load_elapsed = load_start.elapsed();
    let device = embedder.device();

    let backend = InstrumentedBackend {
        inner: &embedder,
        calls: Mutex::new(Vec::new()),
    };
    let mut worker = EmbedWorker::new(&store, &backend);
    if args.limit > 0 {
        worker = worker.with_limit(args.limit);
    }
    if let Some(batch) = args.batch {
        worker = worker.with_batch_size(batch);
    }
    if let Some(window) = args.window {
        worker = worker.with_sort_window(window);
    }
    let embed_start = Instant::now();
    let summary = worker.run().await?;
    let embed_elapsed = embed_start.elapsed();

    let peak_rss_kb = sampler.finish();
    let stats = backend.calls.into_inner().unwrap();

    report(&Report {
        args: &args,
        corpus: &corpus,
        device,
        sessions,
        messages,
        parts,
        ingest_elapsed,
        load_elapsed,
        embed_elapsed,
        embedded: summary.messages,
        batches: summary.batches,
        peak_rss_kb,
        stats: &stats,
    });
    Ok(())
}

struct Report<'a> {
    args: &'a Args,
    corpus: &'a Path,
    device: &'a str,
    sessions: usize,
    messages: usize,
    parts: usize,
    ingest_elapsed: Duration,
    load_elapsed: Duration,
    embed_elapsed: Duration,
    embedded: usize,
    batches: usize,
    peak_rss_kb: u64,
    stats: &'a [BatchStat],
}

#[allow(clippy::cast_precision_loss)]
fn report(r: &Report<'_>) {
    let embed_s = r.embed_elapsed.as_secs_f64();
    let msg_per_s = if embed_s > 0.0 {
        r.embedded as f64 / embed_s
    } else {
        0.0
    };
    let peak_rss_mb = r.peak_rss_kb as f64 / 1024.0;

    // `embed_elapsed` wraps two costs per batch: the model call and the
    // merge-update write of `messages.vector`. Split it - summed per-batch
    // model time vs. the remainder (store writes) - so the report says which
    // side is the bottleneck, not just a conflated end-to-end rate.
    let model_ms: u128 = r.stats.iter().map(|s| s.elapsed_ms).sum();
    let model_s = model_ms as f64 / 1000.0;
    let write_s = (embed_s - model_s).max(0.0);
    let model_msg_per_s = if model_s > 0.0 {
        r.embedded as f64 / model_s
    } else {
        0.0
    };

    // Padding waste: the tokenizer pads each batch to its longest member, so
    // the model embeds `count * max_bytes` token-bytes while only `sum_bytes`
    // were real content. The gap is wasted compute.
    let padded: usize = r.stats.iter().map(|s| s.count * s.max_bytes).sum();
    let real: usize = r.stats.iter().map(|s| s.sum_bytes).sum();
    let waste_pct = if padded > 0 {
        100.0 * (1.0 - real as f64 / padded as f64)
    } else {
        0.0
    };

    let mut elapsed: Vec<u128> = r.stats.iter().map(|s| s.elapsed_ms).collect();
    elapsed.sort_unstable();
    let pct = |p: f64| -> u128 {
        if elapsed.is_empty() {
            0
        } else {
            let idx = ((elapsed.len() as f64 - 1.0) * p).round() as usize;
            elapsed[idx]
        }
    };

    let mut slowest: Vec<&BatchStat> = r.stats.iter().collect();
    slowest.sort_unstable_by_key(|b| std::cmp::Reverse(b.elapsed_ms));

    let fmt_batch = |stat: &BatchStat| -> String {
        let batch_waste = if stat.count * stat.max_bytes > 0 {
            100.0 * (1.0 - stat.sum_bytes as f64 / (stat.count * stat.max_bytes) as f64)
        } else {
            0.0
        };
        format!(
            "elapsed={:>6}ms  count={:>3}  max_bytes={:>7}  sum_bytes={:>8}  waste={:>3.0}%",
            stat.elapsed_ms, stat.count, stat.max_bytes, stat.sum_bytes, batch_waste,
        )
    };

    println!("=== pond embedding bench ===");
    let window = r.args.window.unwrap_or(DEFAULT_SORT_WINDOW);
    let batch = r.args.batch.unwrap_or(DEFAULT_BATCH_SIZE);
    println!(
        "config        device={}  limit={}  batch={}  sort-window={}",
        r.device,
        if r.args.limit > 0 {
            r.args.limit.to_string()
        } else {
            "none".to_owned()
        },
        batch,
        window,
    );
    println!(
        "corpus        {}  sessions={} messages={} parts={}  (ingest {:.1}s)",
        r.corpus.display(),
        r.sessions,
        r.messages,
        r.parts,
        r.ingest_elapsed.as_secs_f64(),
    );
    println!("model load    {:.1}s", r.load_elapsed.as_secs_f64());
    println!("--- embedding ---");
    println!(
        "messages      {} embedded in {} batches",
        r.embedded, r.batches
    );
    println!("model         {model_s:.1}s  ->  {model_msg_per_s:.1} msg/s   (e5 inference only)");
    println!("store write   {write_s:.1}s              (merge-update of messages.vector)");
    println!("wall          {embed_s:.1}s  ->  {msg_per_s:.2} msg/s   (end-to-end)");
    println!("peak RSS      {peak_rss_mb:.0} MB   (over the whole run, model load included)");
    println!(
        "batch ms      p50={}  p90={}  max={}",
        pct(0.5),
        pct(0.9),
        pct(1.0),
    );
    println!(
        "padding waste {waste_pct:.1}%   (real {} of {} padded token-bytes embedded)",
        human_bytes(real),
        human_bytes(padded),
    );
    // Execution order - the first batch carries the cold start, so the curve
    // from batch 0 onward shows steady state settling in directly.
    println!("batches (in execution order, first 10):");
    for stat in r.stats.iter().take(10) {
        println!("  {}", fmt_batch(stat));
    }
    println!("slowest batches:");
    for stat in slowest.iter().take(5) {
        println!("  {}", fmt_batch(stat));
    }

    // One-line machine-readable summary so two runs diff cleanly.
    let json = serde_json::json!({
        "device": r.device,
        "limit": r.args.limit,
        "batch_size": batch,
        "sort_window": window,
        "messages": r.embedded,
        "batches": r.batches,
        "ingest_s": r.ingest_elapsed.as_secs_f64(),
        "model_load_s": r.load_elapsed.as_secs_f64(),
        "embed_wall_s": embed_s,
        "msg_per_s": msg_per_s,
        "model_s": model_s,
        "model_msg_per_s": model_msg_per_s,
        "store_write_s": write_s,
        "peak_rss_mb": peak_rss_mb,
        "padding_waste_pct": waste_pct,
        "batch_ms_p50": pct(0.5),
        "batch_ms_p90": pct(0.9),
        "batch_ms_max": pct(1.0),
    });
    println!("JSON {json}");
}
