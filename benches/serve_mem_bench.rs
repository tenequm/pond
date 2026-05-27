#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Read-only memory bench for `pond mcp` / `pond serve`. Opens an existing
//! `~/.local/share/pond/` corpus, drives realistic `pond_search` / `pond_get`
//! workloads, and reports peak RSS per phase against a 500 MiB default target.
//!
//! No ingest, no embed-worker, no index build - this measures *only* the
//! steady-state read path that a stdio MCP serves. The real `E5Embedder`
//! loads on the first hybrid query, matching production lazy-load behavior.
//!
//! Phases (sequential, matches MCP's stdio request serialization):
//!   - cold_open    : RSS right after `Store::open_local`
//!   - fts_warm     : a few FTS queries to settle metadata cache
//!   - fts_steady   : N FTS-only queries (no embedder loaded)
//!   - first_hybrid : the model-load spike (cold E5 -> Metal/CUDA/CPU)
//!   - hybrid_warm  : a few hybrid queries to settle index cache
//!   - hybrid_steady: N hybrid queries (worst-case steady RAM)
//!   - get_calls    : N `pond_get` calls on previous hits
//!   - idle         : sleep N seconds to see if RSS drains
//!
//! Run:
//!   cargo bench --bench serve_mem_bench
//!   cargo bench --bench serve_mem_bench -- --queries 50 --target-mib 500
//!   cargo bench --bench serve_mem_bench -- --data-dir ~/.local/share/pond --skip-idle

use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use pond::{
    PROTOCOL_VERSION,
    config::SearchConfig,
    embed::{E5Embedder, EmbedBackend, LazyEmbedder},
    handlers::{pond_get, pond_search},
    sessions::Store,
    wire::{
        GetEnvelope, GetRequest, SearchEnvelope, SearchFilters, SearchModeWire, SearchRequest,
        SearchResponse,
    },
};

/// Realistic queries against a Claude-Code conversation history corpus. Mix of
/// short single-term, multi-term technical, and project-name-ish queries so
/// the FTS path exercises both rare and common postings.
const QUERIES: &[&str] = &[
    "rust async tokio",
    "Lance dataset write",
    "MCP server stdio",
    "vector index IVF_PQ",
    "TypeScript React hook",
    "merge insert conflict",
    "embedding model e5",
    "S3 object store backend",
    "search latency benchmark",
    "memory bound RSS",
    "compaction cleanup retention",
    "transaction OCC retry",
    "tokenizer padding waste",
    "FTS BM25 ranking",
    "scalar index bitmap",
    "session manifest version",
    "fragment reuse index",
    "io buffer batch size",
    "candle metal cuda",
    "schema evolution add column",
];

#[derive(Parser)]
#[command(about = "pond mcp/serve read-only memory bench: peak RSS per phase vs target ceiling")]
struct Args {
    /// Pond data directory to open read-only. Defaults to `~/.local/share/pond`
    /// when present.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Queries per steady-state phase.
    #[arg(long, default_value_t = 30)]
    queries: usize,
    /// Warmup queries before each steady phase. Excluded from latency stats.
    #[arg(long, default_value_t = 5)]
    warmup: usize,
    /// RSS sampling interval in ms. Lower = tighter peak detection, higher CPU.
    #[arg(long, default_value_t = 100)]
    rss_interval_ms: u64,
    /// Memory budget in MiB. Used only for the PASS/FAIL line; doesn't enforce.
    #[arg(long, default_value_t = 500)]
    target_mib: u64,
    /// Per-query result limit (mirrors `pond mcp` defaults).
    #[arg(long, default_value_t = 10)]
    limit: usize,
    /// Skip the hybrid phases (no E5 load). Useful to see the FTS-only floor.
    #[arg(long)]
    skip_hybrid: bool,
    /// Skip the idle drain phase.
    #[arg(long)]
    skip_idle: bool,
    /// Idle drain duration in seconds.
    #[arg(long, default_value_t = 10)]
    idle_seconds: u64,
    /// Probe-only mode: skip Store/search and trace the E5 model's RSS
    /// footprint across load, embed, and drop. Used to verify whether the
    /// FP32 staging RAM lingers in candle's Metal buffer pool (see
    /// metal_backend/device.rs:44-57).
    #[arg(long)]
    probe_embedder: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// Background sampler: peak RSS via `ps -o rss=`. `peak_kb` is the running max
/// since `start()`; `phase_peak_kb` is reset by `mark_phase_start`.
struct RssSampler {
    peak_kb: Arc<AtomicU64>,
    phase_peak_kb: Arc<AtomicU64>,
    current_kb: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start(interval: Duration) -> Self {
        let peak_kb = Arc::new(AtomicU64::new(0));
        let phase_peak_kb = Arc::new(AtomicU64::new(0));
        let current_kb = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let pid = std::process::id().to_string();
        let handle = {
            let peak = Arc::clone(&peak_kb);
            let phase = Arc::clone(&phase_peak_kb);
            let current = Arc::clone(&current_kb);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(kb) = sample_rss_kb(&pid) {
                        current.store(kb, Ordering::Relaxed);
                        peak.fetch_max(kb, Ordering::Relaxed);
                        phase.fetch_max(kb, Ordering::Relaxed);
                    }
                    thread::sleep(interval);
                }
            })
        };
        Self {
            peak_kb,
            phase_peak_kb,
            current_kb,
            stop,
            handle: Some(handle),
        }
    }

    fn current_kb(&self) -> u64 {
        self.current_kb.load(Ordering::Relaxed)
    }

    fn peak_kb(&self) -> u64 {
        self.peak_kb.load(Ordering::Relaxed)
    }

    fn mark_phase_start(&self) {
        // Reset the per-phase max to the current sample so the next read of
        // `phase_peak_kb` reflects only what this phase did.
        let now = self.current_kb.load(Ordering::Relaxed);
        self.phase_peak_kb.store(now, Ordering::Relaxed);
    }

    fn phase_peak_kb(&self) -> u64 {
        self.phase_peak_kb.load(Ordering::Relaxed)
    }

    fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
        self.peak_kb.load(Ordering::Relaxed)
    }
}

fn sample_rss_kb(pid: &str) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", pid])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

fn search_request(query: &str, mode: Option<SearchModeWire>, limit: usize) -> SearchRequest {
    SearchRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode_override: mode,
        similar_to: None,
        filters: SearchFilters::default(),
        boost_recent: true,
        group_by_conversation: false,
        full: false,
        limit,
    }
}

fn get_request(message_id: String) -> GetRequest {
    GetRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: None,
        message_id: Some(message_id),
        up_to: None,
        context_depth: 0,
        max_messages: 50,
        include_thinking: false,
        include_tool_results: false,
    }
}

#[derive(Default, Clone)]
struct PhaseStats {
    name: &'static str,
    queries: usize,
    elapsed_ms: Vec<u128>,
    rss_start_kb: u64,
    rss_end_kb: u64,
    rss_phase_peak_kb: u64,
    rss_global_peak_kb: u64,
    notes: String,
}

impl PhaseStats {
    fn p50(&self) -> u128 {
        percentile(&self.elapsed_ms, 0.5)
    }
    fn p95(&self) -> u128 {
        percentile(&self.elapsed_ms, 0.95)
    }
    fn max(&self) -> u128 {
        percentile(&self.elapsed_ms, 1.0)
    }
}

fn percentile(values: &[u128], p: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    #[allow(clippy::cast_precision_loss)]
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn first_hit_message_id(response: &SearchResponse) -> Option<String> {
    match &response.result {
        pond::wire::SearchResultBody::Hits { hits } => {
            hits.first().map(|hit| hit.message_id.clone())
        }
        pond::wire::SearchResultBody::Groups { groups } => groups
            .first()
            .map(|group| group.best_hit_message_id.clone()),
    }
}

struct SearchPhase<'a> {
    name: &'static str,
    store: &'a Store,
    embedder: &'a LazyEmbedder,
    cfg: &'a SearchConfig,
    sampler: &'a RssSampler,
    mode: Option<SearchModeWire>,
    queries: &'a [&'a str],
    limit: usize,
    record_hits: bool,
    hit_sink: &'a Mutex<Vec<String>>,
}

async fn run_search_phase(input: SearchPhase<'_>) -> Result<PhaseStats> {
    let SearchPhase {
        name,
        store,
        embedder,
        cfg,
        sampler,
        mode,
        queries,
        limit,
        record_hits,
        hit_sink,
    } = input;
    let rss_start_kb = sampler.current_kb();
    sampler.mark_phase_start();
    let mut elapsed_ms: Vec<u128> = Vec::with_capacity(queries.len());

    for query in queries {
        let request = search_request(query, mode, limit);
        let t = Instant::now();
        let envelope = pond_search(store, embedder, request, cfg).await;
        elapsed_ms.push(t.elapsed().as_millis());
        match envelope {
            SearchEnvelope::Success(response) => {
                if record_hits && let Some(id) = first_hit_message_id(&response) {
                    hit_sink.lock().unwrap().push(id);
                }
            }
            SearchEnvelope::Error(error) => {
                anyhow::bail!("{name}: query {query:?} failed: {error:?}");
            }
        }
    }

    Ok(PhaseStats {
        name,
        queries: queries.len(),
        elapsed_ms,
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: String::new(),
    })
}

async fn run_get_phase(
    name: &'static str,
    store: &Store,
    sampler: &RssSampler,
    message_ids: &[String],
) -> Result<PhaseStats> {
    let rss_start_kb = sampler.current_kb();
    sampler.mark_phase_start();
    let mut elapsed_ms: Vec<u128> = Vec::with_capacity(message_ids.len());

    for id in message_ids {
        let request = get_request(id.clone());
        let t = Instant::now();
        let envelope = pond_get(store, request).await;
        elapsed_ms.push(t.elapsed().as_millis());
        match envelope {
            GetEnvelope::Success(_) => {}
            GetEnvelope::Error(error) => {
                anyhow::bail!("{name}: get {id} failed: {error:?}");
            }
        }
    }

    Ok(PhaseStats {
        name,
        queries: message_ids.len(),
        elapsed_ms,
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: String::new(),
    })
}

fn resolve_data_dir(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME not set; pass --data-dir")?;
    Ok(home.join(".local").join("share").join("pond"))
}

fn print_phase_header() {
    println!(
        "{:<16}  {:>5}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>7}  {:>6}",
        "phase", "n", "start_M", "end_M", "peak_M", "gpeak_M", "p50_ms", "p95_ms", "max_ms",
    );
    println!("{}", "-".repeat(95));
}

fn print_phase_row(stat: &PhaseStats) {
    let start_m = stat.rss_start_kb as f64 / 1024.0;
    let end_m = stat.rss_end_kb as f64 / 1024.0;
    let peak_m = stat.rss_phase_peak_kb as f64 / 1024.0;
    let gpeak_m = stat.rss_global_peak_kb as f64 / 1024.0;
    println!(
        "{:<16}  {:>5}  {:>7.1}  {:>7.1}  {:>7.1}  {:>7.1}  {:>7}  {:>7}  {:>6}",
        stat.name,
        stat.queries,
        start_m,
        end_m,
        peak_m,
        gpeak_m,
        stat.p50(),
        stat.p95(),
        stat.max(),
    );
    if !stat.notes.is_empty() {
        println!("                  {}", stat.notes);
    }
}

/// Probe the embedder lifecycle in isolation: load, run a few embed() calls
/// (forces Metal command-buffer flushes -> drop_unused_buffers), drop, idle.
/// Tracks RSS at each step so we can see whether candle's Metal buffer pool
/// retains FP32 staging RAM after the F32 -> F16 conversion.
fn probe_embedder(sampler: &RssSampler, idle_seconds: u64) -> Result<Vec<PhaseStats>> {
    let mut phases = Vec::new();

    // --- baseline ---
    thread::sleep(Duration::from_millis(300));
    let baseline = sampler.current_kb();

    // --- E5 load ---
    sampler.mark_phase_start();
    let load_start = Instant::now();
    let embedder = E5Embedder::load()?;
    let load_ms = load_start.elapsed().as_millis();
    thread::sleep(Duration::from_millis(400));
    phases.push(PhaseStats {
        name: "load",
        queries: 0,
        elapsed_ms: vec![load_ms],
        rss_start_kb: baseline,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: format!(
            "E5Embedder::load() = {load_ms} ms; device={}",
            embedder.device()
        ),
    });

    // --- embed warm + flush ---
    // Run a handful of forward passes. Each `command_encoder()` triggers a
    // flush check; on flush, candle's MetalDevice runs `drop_unused_buffers`,
    // which is the only path that reclaims pool slots with strong_count==1.
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let mut elapsed_ms = Vec::new();
    let texts: Vec<String> = (0..8).map(|i| format!("query: probe pass {i}")).collect();
    for round in 0..6 {
        let t = Instant::now();
        let _ = embedder.embed(&texts)?;
        elapsed_ms.push(t.elapsed().as_millis());
        // Brief pause so the sampler catches steady-state between rounds.
        if round == 0 {
            thread::sleep(Duration::from_millis(200));
        }
    }
    thread::sleep(Duration::from_millis(400));
    phases.push(PhaseStats {
        name: "embed_warm",
        queries: texts.len() * 6,
        elapsed_ms,
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: "6 rounds * 8 texts; forward passes trigger Metal command flushes".to_owned(),
    });

    // --- drop + idle ---
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    drop(embedder);
    // Give the allocator/OS a moment to settle. On macOS large freed
    // allocations typically munmap promptly; if RSS doesn't drop here, the
    // load delta was held by something outside the embedder's drop path.
    thread::sleep(Duration::from_secs(idle_seconds.max(2)));
    phases.push(PhaseStats {
        name: "post_drop",
        queries: 0,
        elapsed_ms: vec![idle_seconds.saturating_mul(1000) as u128],
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: format!("E5Embedder dropped; slept {idle_seconds}s"),
    });

    // --- reload to see if the second load is cheaper (fragmentation signal) ---
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let reload_start = Instant::now();
    let embedder2 = E5Embedder::load()?;
    let reload_ms = reload_start.elapsed().as_millis();
    thread::sleep(Duration::from_millis(400));
    phases.push(PhaseStats {
        name: "reload",
        queries: 0,
        elapsed_ms: vec![reload_ms],
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: format!("E5Embedder::load() #2 = {reload_ms} ms"),
    });
    drop(embedder2);

    Ok(phases)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.probe_embedder {
        println!("=== pond serve-mem bench: --probe-embedder ===");
        println!("(no Store, no search; isolates the E5 model's RSS footprint)");
        println!();
        let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        let baseline_kb = sampler.current_kb();
        println!(
            "baseline RSS     {:.1} MiB (this process before E5 load)",
            baseline_kb as f64 / 1024.0,
        );
        println!();
        let phases =
            tokio::task::spawn_blocking(move || probe_embedder(&sampler, args.idle_seconds))
                .await??;
        print_phase_header();
        for phase in &phases {
            print_phase_row(phase);
        }
        let json_phases: Vec<_> = phases
            .iter()
            .map(|p| {
                serde_json::json!({
                    "phase": p.name,
                    "n": p.queries,
                    "rss_start_mib": p.rss_start_kb as f64 / 1024.0,
                    "rss_end_mib": p.rss_end_kb as f64 / 1024.0,
                    "rss_phase_peak_mib": p.rss_phase_peak_kb as f64 / 1024.0,
                    "rss_global_peak_mib": p.rss_global_peak_kb as f64 / 1024.0,
                    "p50_ms": p.p50(),
                    "p95_ms": p.p95(),
                    "max_ms": p.max(),
                    "notes": p.notes,
                })
            })
            .collect();
        let json = serde_json::json!({
            "mode": "probe_embedder",
            "baseline_mib": baseline_kb as f64 / 1024.0,
            "phases": json_phases,
        });
        println!();
        println!("JSON {json}");
        return Ok(());
    }

    let data_dir = resolve_data_dir(args.data_dir.clone())?;
    if !data_dir.join("sessions.lance").exists() {
        anyhow::bail!(
            "no Lance datasets under {} - pass --data-dir to a populated pond",
            data_dir.display(),
        );
    }

    let cfg = SearchConfig::default();
    let queries: Vec<&str> = QUERIES.iter().copied().take(args.queries).collect();
    let warmup: Vec<&str> = QUERIES.iter().copied().cycle().take(args.warmup).collect();

    println!("=== pond serve-mem bench (read-only) ===");
    println!("data_dir         {}", data_dir.display());
    println!(
        "queries          {} per phase, warmup={}, limit={}",
        args.queries, args.warmup, args.limit,
    );
    println!("target_budget    {} MiB", args.target_mib);
    println!();

    let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
    // One pre-open sample so `cold_open` `start_M` is the pre-pond baseline.
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    let baseline_kb = sampler.current_kb();
    println!(
        "baseline RSS     {:.1} MiB (this process before pond)",
        baseline_kb as f64 / 1024.0
    );
    println!();

    // ---- Phase: cold_open ----
    let mut phases: Vec<PhaseStats> = Vec::new();
    sampler.mark_phase_start();
    let t = Instant::now();
    let store = Store::open_local(&data_dir).await?;
    let open_ms = t.elapsed().as_millis();
    // Give the sampler one tick to catch any post-open allocation.
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    let (sessions, messages, parts) = store.row_counts().await?;
    let mut cold = PhaseStats {
        name: "cold_open",
        queries: 0,
        elapsed_ms: vec![open_ms],
        rss_start_kb: baseline_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        notes: format!("sessions={sessions} messages={messages} parts={parts} open_ms={open_ms}",),
    };
    // For cold_open, latency stats use the one open() call.
    cold.elapsed_ms = vec![open_ms];
    phases.push(cold);

    // LazyEmbedder created but NOT loaded - matches `pond mcp` lazy behavior.
    let embedder = LazyEmbedder::new();
    let hit_sink: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // ---- Phase: fts_warm ----
    run_search_phase(SearchPhase {
        name: "fts_warm",
        store: &store,
        embedder: &embedder,
        cfg: &cfg,
        sampler: &sampler,
        mode: Some(SearchModeWire::Fts),
        queries: &warmup,
        limit: args.limit,
        record_hits: false,
        hit_sink: &hit_sink,
    })
    .await
    .map(|s| phases.push(s))?;

    // ---- Phase: fts_steady ----
    run_search_phase(SearchPhase {
        name: "fts_steady",
        store: &store,
        embedder: &embedder,
        cfg: &cfg,
        sampler: &sampler,
        mode: Some(SearchModeWire::Fts),
        queries: &queries,
        limit: args.limit,
        record_hits: true,
        hit_sink: &hit_sink,
    })
    .await
    .map(|s| phases.push(s))?;

    if !args.skip_hybrid {
        // ---- Phase: first_hybrid ----
        sampler.mark_phase_start();
        let rss_start_kb = sampler.current_kb();
        let request = search_request(QUERIES[0], Some(SearchModeWire::Hybrid), args.limit);
        let t = Instant::now();
        let envelope = pond_search(&store, &embedder, request, &cfg).await;
        let first_ms = t.elapsed().as_millis();
        if let SearchEnvelope::Error(error) = envelope {
            anyhow::bail!("first_hybrid failed: {error:?}");
        }
        // Let the sampler catch the post-load steady state.
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        phases.push(PhaseStats {
            name: "first_hybrid",
            queries: 1,
            elapsed_ms: vec![first_ms],
            rss_start_kb,
            rss_end_kb: sampler.current_kb(),
            rss_phase_peak_kb: sampler.phase_peak_kb(),
            rss_global_peak_kb: sampler.peak_kb(),
            notes: format!("first_call_ms={first_ms} (includes E5 model load)"),
        });

        // ---- Phase: hybrid_warm ----
        run_search_phase(SearchPhase {
            name: "hybrid_warm",
            store: &store,
            embedder: &embedder,
            cfg: &cfg,
            sampler: &sampler,
            mode: Some(SearchModeWire::Hybrid),
            queries: &warmup,
            limit: args.limit,
            record_hits: false,
            hit_sink: &hit_sink,
        })
        .await
        .map(|s| phases.push(s))?;

        // ---- Phase: hybrid_steady ----
        run_search_phase(SearchPhase {
            name: "hybrid_steady",
            store: &store,
            embedder: &embedder,
            cfg: &cfg,
            sampler: &sampler,
            mode: Some(SearchModeWire::Hybrid),
            queries: &queries,
            limit: args.limit,
            record_hits: true,
            hit_sink: &hit_sink,
        })
        .await
        .map(|s| phases.push(s))?;
    }

    // ---- Phase: get_calls ----
    let ids: Vec<String> = {
        let guard = hit_sink.lock().unwrap();
        guard.iter().take(args.queries).cloned().collect()
    };
    if !ids.is_empty() {
        run_get_phase("get_calls", &store, &sampler, &ids)
            .await
            .map(|s| phases.push(s))?;
    }

    // ---- Phase: idle ----
    if !args.skip_idle {
        sampler.mark_phase_start();
        let rss_start_kb = sampler.current_kb();
        thread::sleep(Duration::from_secs(args.idle_seconds));
        phases.push(PhaseStats {
            name: "idle",
            queries: 0,
            elapsed_ms: vec![(args.idle_seconds * 1000) as u128],
            rss_start_kb,
            rss_end_kb: sampler.current_kb(),
            rss_phase_peak_kb: sampler.phase_peak_kb(),
            rss_global_peak_kb: sampler.peak_kb(),
            notes: format!("slept {}s with no requests", args.idle_seconds),
        });
    }

    let global_peak_kb = sampler.finish();
    let global_peak_mib = global_peak_kb as f64 / 1024.0;
    #[allow(clippy::cast_precision_loss)]
    let target_mib = args.target_mib as f64;
    let pass = global_peak_mib <= target_mib;

    println!();
    print_phase_header();
    for phase in &phases {
        print_phase_row(phase);
    }
    println!();
    println!(
        "PEAK RSS  {:.1} MiB   target {:.0} MiB   {}  (headroom: {:+.1} MiB)",
        global_peak_mib,
        target_mib,
        if pass { "PASS" } else { "FAIL" },
        target_mib - global_peak_mib,
    );

    // JSON one-liner for diffing across runs.
    let json_phases: Vec<_> = phases
        .iter()
        .map(|p| {
            serde_json::json!({
                "phase": p.name,
                "n": p.queries,
                "rss_start_mib": p.rss_start_kb as f64 / 1024.0,
                "rss_end_mib": p.rss_end_kb as f64 / 1024.0,
                "rss_phase_peak_mib": p.rss_phase_peak_kb as f64 / 1024.0,
                "rss_global_peak_mib": p.rss_global_peak_kb as f64 / 1024.0,
                "p50_ms": p.p50(),
                "p95_ms": p.p95(),
                "max_ms": p.max(),
                "notes": p.notes,
            })
        })
        .collect();
    let json = serde_json::json!({
        "data_dir": data_dir.display().to_string(),
        "queries_per_phase": args.queries,
        "target_mib": args.target_mib,
        "baseline_mib": baseline_kb as f64 / 1024.0,
        "peak_mib": global_peak_mib,
        "pass": pass,
        "phases": json_phases,
    });
    println!("JSON {json}");

    if !pass {
        std::process::exit(1);
    }
    Ok(())
}
