#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]
// The macOS `proc_pid_rusage` FFI for phys_footprint sampling needs `unsafe`.
#![allow(unsafe_code)]
#![allow(unreachable_pub, dead_code)]

//! Read-only memory bench for `pond mcp` / `pond serve`. Opens an existing
//! `~/.local/share/pond/` corpus, drives realistic `pond_search` / `pond_get`
//! workloads, and reports peak RSS per phase against a 500 MiB default target.
//!
//! No ingest, no embed-worker, no index build - this measures *only* the
//! steady-state read path that a stdio MCP serves. The candle embedder
//! loads lazily on the first hybrid query, matching production behavior.
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
    embed::{CandleEmbedder, Embedder, LazyEmbedder},
    handlers::{pond_get, pond_search},
    sessions::Store,
    substrate::RuntimeCaps,
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
    /// Sweep the workload across a fixed grid of `(metadata_cache_bytes,
    /// index_cache_bytes)` pairs, printing peak RSS and p50/p95 hybrid latency
    /// for each. Used to calibrate the `[runtime]` defaults (`docs/plans/mcp-
    /// memory-budget.md` Q5).
    #[arg(long)]
    cap_sweep: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// macOS phys_footprint accessors (`proc_pid_rusage(RUSAGE_INFO_V4)`). This
/// is what Activity Monitor / `footprint(1)` / `top` / WebKit / psutil read
/// and the only metric the kernel's Jetsam OOM-killer cares about. RSS is
/// wrong in both directions on macOS - overcounts shared dyld pages,
/// undercounts compressed pages. We sample both for one or two runs to
/// quantify the gap, then drop RSS.
#[cfg(target_os = "macos")]
mod footprint {
    // Mirror of `<sys/resource.h>` `rusage_info_v4` (Apple's stable layout).
    // We need `ri_phys_footprint`, `ri_lifetime_max_phys_footprint`, and
    // `ri_interval_max_phys_footprint` so we can read per-phase peak via
    // `proc_reset_footprint_interval` between phases.
    #[repr(C)]
    #[derive(Default, Copy, Clone)]
    pub struct RUsageInfoV4 {
        pub ri_uuid: [u8; 16],
        pub ri_user_time: u64,
        pub ri_system_time: u64,
        pub ri_pkg_idle_wkups: u64,
        pub ri_interrupt_wkups: u64,
        pub ri_pageins: u64,
        pub ri_wired_size: u64,
        pub ri_resident_size: u64,
        pub ri_phys_footprint: u64,
        pub ri_proc_start_abstime: u64,
        pub ri_proc_exit_abstime: u64,
        pub ri_child_user_time: u64,
        pub ri_child_system_time: u64,
        pub ri_child_pkg_idle_wkups: u64,
        pub ri_child_interrupt_wkups: u64,
        pub ri_child_pageins: u64,
        pub ri_child_elapsed_abstime: u64,
        pub ri_diskio_bytesread: u64,
        pub ri_diskio_byteswritten: u64,
        pub ri_cpu_time_qos_default: u64,
        pub ri_cpu_time_qos_maintenance: u64,
        pub ri_cpu_time_qos_background: u64,
        pub ri_cpu_time_qos_utility: u64,
        pub ri_cpu_time_qos_legacy: u64,
        pub ri_cpu_time_qos_user_initiated: u64,
        pub ri_cpu_time_qos_user_interactive: u64,
        pub ri_billed_system_time: u64,
        pub ri_serviced_system_time: u64,
        pub ri_logical_writes: u64,
        pub ri_lifetime_max_phys_footprint: u64,
        pub ri_instructions: u64,
        pub ri_cycles: u64,
        pub ri_billed_energy: u64,
        pub ri_serviced_energy: u64,
        pub ri_interval_max_phys_footprint: u64,
        pub ri_runnable_time: u64,
    }

    pub const RUSAGE_INFO_V4: libc::c_int = 4;

    // `proc_reset_footprint_interval` lives in `<libproc_internal.h>` (private
    // API). Stable in practice - WebKit, psutil, and footprint(1) all use it.
    // libc doesn't declare it; declare manually.
    unsafe extern "C" {
        pub fn proc_pid_rusage(
            pid: libc::c_int,
            flavor: libc::c_int,
            buffer: *mut libc::c_void,
        ) -> libc::c_int;
        pub fn proc_reset_footprint_interval(pid: libc::c_int) -> libc::c_int;
    }

    fn read() -> Option<RUsageInfoV4> {
        let mut info = RUsageInfoV4::default();
        let pid = unsafe { libc::getpid() };
        let kr = unsafe {
            proc_pid_rusage(
                pid,
                RUSAGE_INFO_V4,
                &mut info as *mut _ as *mut libc::c_void,
            )
        };
        (kr == 0).then_some(info)
    }

    /// Current phys_footprint in KiB.
    pub fn current_kb() -> Option<u64> {
        read().map(|i| i.ri_phys_footprint / 1024)
    }

    /// Peak phys_footprint since the last `reset_interval` call, in KiB. Used
    /// for per-phase peak instead of carrying a lifetime watermark in user
    /// space.
    pub fn interval_peak_kb() -> Option<u64> {
        read().map(|i| i.ri_interval_max_phys_footprint / 1024)
    }

    /// Reset the kernel-tracked phys_footprint interval-peak counter.
    pub fn reset_interval() {
        let pid = unsafe { libc::getpid() };
        unsafe {
            let _ = proc_reset_footprint_interval(pid);
        }
    }
}

#[cfg(target_os = "macos")]
fn sample_phys_footprint_kb() -> Option<u64> {
    footprint::current_kb()
}

#[cfg(not(target_os = "macos"))]
fn sample_phys_footprint_kb() -> Option<u64> {
    None
}

#[cfg(target_os = "macos")]
fn footprint_interval_peak_kb() -> Option<u64> {
    footprint::interval_peak_kb()
}

#[cfg(not(target_os = "macos"))]
fn footprint_interval_peak_kb() -> Option<u64> {
    None
}

#[cfg(target_os = "macos")]
fn reset_footprint_interval() {
    footprint::reset_interval();
}

#[cfg(not(target_os = "macos"))]
fn reset_footprint_interval() {}

/// Background sampler: tracks both `ps -o rss=` (the classic RSS, overcounts
/// shared libs on macOS) and macOS `phys_footprint` (the private-memory-cost
/// metric Activity Monitor uses, excludes shared libs). `peak_kb` is the
/// running max since `start()`; `phase_peak_kb` is reset by
/// `mark_phase_start`. All `*_kb` fields are tracked in parallel for both
/// metrics, suffixed `_pf` for phys_footprint.
struct RssSampler {
    peak_kb: Arc<AtomicU64>,
    phase_peak_kb: Arc<AtomicU64>,
    current_kb: Arc<AtomicU64>,
    peak_pf_kb: Arc<AtomicU64>,
    phase_peak_pf_kb: Arc<AtomicU64>,
    current_pf_kb: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start(interval: Duration) -> Self {
        let peak_kb = Arc::new(AtomicU64::new(0));
        let phase_peak_kb = Arc::new(AtomicU64::new(0));
        let current_kb = Arc::new(AtomicU64::new(0));
        let peak_pf_kb = Arc::new(AtomicU64::new(0));
        let phase_peak_pf_kb = Arc::new(AtomicU64::new(0));
        let current_pf_kb = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let pid = std::process::id().to_string();
        let handle = {
            let peak = Arc::clone(&peak_kb);
            let phase = Arc::clone(&phase_peak_kb);
            let current = Arc::clone(&current_kb);
            let peak_pf = Arc::clone(&peak_pf_kb);
            let phase_pf = Arc::clone(&phase_peak_pf_kb);
            let current_pf = Arc::clone(&current_pf_kb);
            let stop = Arc::clone(&stop);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(kb) = sample_rss_kb(&pid) {
                        current.store(kb, Ordering::Relaxed);
                        peak.fetch_max(kb, Ordering::Relaxed);
                        phase.fetch_max(kb, Ordering::Relaxed);
                    }
                    if let Some(kb) = sample_phys_footprint_kb() {
                        current_pf.store(kb, Ordering::Relaxed);
                        peak_pf.fetch_max(kb, Ordering::Relaxed);
                        phase_pf.fetch_max(kb, Ordering::Relaxed);
                    }
                    thread::sleep(interval);
                }
            })
        };
        Self {
            peak_kb,
            phase_peak_kb,
            current_kb,
            peak_pf_kb,
            phase_peak_pf_kb,
            current_pf_kb,
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

    fn current_pf_kb(&self) -> u64 {
        self.current_pf_kb.load(Ordering::Relaxed)
    }

    fn peak_pf_kb(&self) -> u64 {
        self.peak_pf_kb.load(Ordering::Relaxed)
    }

    fn mark_phase_start(&self) {
        // RSS per-phase max is tracked in userspace from the current sample.
        // phys_footprint interval-peak is tracked by the kernel via
        // `proc_reset_footprint_interval` - resetting here means the next
        // `ri_interval_max_phys_footprint` read returns the peak within this
        // phase only, with no userspace sampling-race-condition risk.
        let now = self.current_kb.load(Ordering::Relaxed);
        self.phase_peak_kb.store(now, Ordering::Relaxed);
        reset_footprint_interval();
        // Mirror the same reset on the userspace pf atomic so any sampler
        // reads between reset and the next sample don't carry stale values.
        let now_pf = self.current_pf_kb.load(Ordering::Relaxed);
        self.phase_peak_pf_kb.store(now_pf, Ordering::Relaxed);
    }

    fn phase_peak_kb(&self) -> u64 {
        self.phase_peak_kb.load(Ordering::Relaxed)
    }

    /// Kernel-tracked interval peak for phys_footprint since the last
    /// `mark_phase_start`. Falls back to the userspace-sampled peak on
    /// non-macOS or if the syscall failed.
    fn phase_peak_pf_kb(&self) -> u64 {
        footprint_interval_peak_kb()
            .unwrap_or_else(|| self.phase_peak_pf_kb.load(Ordering::Relaxed))
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
        limit,
        cursor: None,
    }
}

fn get_request(message_id: String) -> GetRequest {
    GetRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        session_id: None,
        message_id: Some(message_id),
        context_depth: 0,
        limit: 50,
        response_mode: pond::wire::ResponseMode::Conversational,
        after_id: None,
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
    pf_start_kb: u64,
    pf_end_kb: u64,
    pf_phase_peak_kb: u64,
    pf_global_peak_kb: u64,
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
    response
        .sessions
        .first()
        .and_then(|session| session.matches.first())
        .map(|hit| hit.message_id.clone())
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
    let pf_start_kb = sampler.current_pf_kb();
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
        pf_start_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
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
    let pf_start_kb = sampler.current_pf_kb();
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
        pf_start_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
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
    // rss_*: macOS `ps -o rss=` - overcounts shared dyld pages, undercounts
    // compressed pages. Reported for one-or-two-run side-by-side comparison.
    // pf_*: macOS `task_vm_info.phys_footprint` (kernel's own ledger; what
    // Activity Monitor, Jetsam, footprint(1) all use). This is the
    // load-bearing memory-budget metric.
    println!(
        "{:<14}  {:>4}  {:>6}  {:>6}  {:>6}  {:>6}  {:>5}  {:>5}",
        "phase", "n", "rssEnd", "rssPk", "pfEnd", "pfPk", "p50", "p95",
    );
    println!("{}", "-".repeat(80));
}

fn print_phase_row(stat: &PhaseStats) {
    let rss_end_m = stat.rss_end_kb as f64 / 1024.0;
    let rss_peak_m = stat.rss_phase_peak_kb as f64 / 1024.0;
    let pf_end_m = stat.pf_end_kb as f64 / 1024.0;
    let pf_peak_m = stat.pf_phase_peak_kb as f64 / 1024.0;
    println!(
        "{:<14}  {:>4}  {:>6.1}  {:>6.1}  {:>6.1}  {:>6.1}  {:>5}  {:>5}",
        stat.name,
        stat.queries,
        rss_end_m,
        rss_peak_m,
        pf_end_m,
        pf_peak_m,
        stat.p50(),
        stat.p95(),
    );
    if !stat.notes.is_empty() {
        println!("                {}", stat.notes);
    }
}

/// Probe the embedder lifecycle in isolation for the selected backend:
/// load, run a few embed() calls, drop, idle, reload. Tracks RSS at each
/// step so we can quantify the candle/Metal buffer pool retention that
/// shows up in `phys_footprint` (see docs/researches/embeddings.md).
fn probe_embedder(sampler: &RssSampler, idle_seconds: u64) -> Result<Vec<PhaseStats>> {
    let mut phases = Vec::new();

    let load_backend = || -> Result<CandleEmbedder> { CandleEmbedder::load() };

    // --- baseline ---
    thread::sleep(Duration::from_millis(300));
    let baseline = sampler.current_kb();
    let baseline_pf = sampler.current_pf_kb();

    // --- load ---
    sampler.mark_phase_start();
    let load_start = Instant::now();
    let embedder = load_backend()?;
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
        pf_start_kb: baseline_pf,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: format!("candle::load() = {load_ms} ms"),
    });

    // --- embed warm + flush ---
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let pf_start_kb = sampler.current_pf_kb();
    let mut elapsed_ms = Vec::new();
    let texts: Vec<String> = (0..8).map(|i| format!("query: probe pass {i}")).collect();
    for round in 0..6 {
        let t = Instant::now();
        let _ = embedder.embed(&texts)?;
        elapsed_ms.push(t.elapsed().as_millis());
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
        pf_start_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: "6 rounds * 8 texts".to_owned(),
    });

    // --- drop + idle ---
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let pf_start_kb = sampler.current_pf_kb();
    drop(embedder);
    thread::sleep(Duration::from_secs(idle_seconds.max(2)));
    phases.push(PhaseStats {
        name: "post_drop",
        queries: 0,
        elapsed_ms: vec![idle_seconds.saturating_mul(1000) as u128],
        rss_start_kb,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        pf_start_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: format!("embedder dropped; slept {idle_seconds}s"),
    });

    // --- reload to surface allocator fragmentation ---
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let pf_start_kb = sampler.current_pf_kb();
    let reload_start = Instant::now();
    let embedder2 = load_backend()?;
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
        pf_start_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: format!("candle::load() #2 = {reload_ms} ms"),
    });
    drop(embedder2);

    Ok(phases)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.probe_embedder {
        println!("=== pond serve-mem bench: --probe-embedder ===");
        println!("(no Store, no search; isolates the candle embedder's RSS footprint)");
        println!();
        let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        let baseline_kb = sampler.current_kb();
        println!(
            "baseline RSS     {:.1} MiB (this process before load)",
            baseline_kb as f64 / 1024.0,
        );
        println!();
        let idle = args.idle_seconds;
        let phases = tokio::task::spawn_blocking(move || probe_embedder(&sampler, idle)).await??;
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

    if args.cap_sweep {
        return run_cap_sweep(&args, &data_dir).await;
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
    let baseline_pf_kb = sampler.current_pf_kb();
    println!(
        "baseline RSS     {:.1} MiB    PF {:.1} MiB    (this process before pond)",
        baseline_kb as f64 / 1024.0,
        baseline_pf_kb as f64 / 1024.0,
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
        pf_start_kb: baseline_pf_kb,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: format!("sessions={sessions} messages={messages} parts={parts} open_ms={open_ms}",),
    };
    // For cold_open, latency stats use the one open() call.
    cold.elapsed_ms = vec![open_ms];
    phases.push(cold);

    // LazyEmbedder created but NOT loaded - matches `pond mcp` lazy behavior.
    let embedder = LazyEmbedder::candle();
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
        let pf_start_kb = sampler.current_pf_kb();
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
            pf_start_kb,
            pf_end_kb: sampler.current_pf_kb(),
            pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
            pf_global_peak_kb: sampler.peak_pf_kb(),
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
        let pf_start_kb = sampler.current_pf_kb();
        thread::sleep(Duration::from_secs(args.idle_seconds));
        phases.push(PhaseStats {
            name: "idle",
            queries: 0,
            elapsed_ms: vec![(args.idle_seconds * 1000) as u128],
            rss_start_kb,
            rss_end_kb: sampler.current_kb(),
            rss_phase_peak_kb: sampler.phase_peak_kb(),
            rss_global_peak_kb: sampler.peak_kb(),
            pf_start_kb,
            pf_end_kb: sampler.current_pf_kb(),
            pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
            pf_global_peak_kb: sampler.peak_pf_kb(),
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

/// Sweep `(metadata, index)` Lance cache caps across a fixed MiB grid and
/// print peak RSS + p50 / p95 hybrid latency for each. Used to calibrate the
/// `[runtime]` defaults against a real corpus (`docs/plans/mcp-memory-budget.md`
/// Q5). The E5 embedder loads once and is reused across grid points so the
/// model-load spike is not double-counted.
async fn run_cap_sweep(args: &Args, data_dir: &std::path::Path) -> Result<()> {
    const SWEEP_MIB: &[u64] = &[32, 64, 128, 256, 512, 1024];

    println!("=== pond serve-mem bench: --cap-sweep ===");
    println!("data_dir         {}", data_dir.display());
    println!(
        "queries/phase    {} (warmup={}, limit={})",
        args.queries, args.warmup, args.limit
    );
    println!();
    println!(
        "{:>10}  {:>10}  {:>10}  {:>10}  {:>8}  {:>8}",
        "meta_MiB", "index_MiB", "peak_MiB", "steady_MiB", "p50_ms", "p95_ms",
    );
    println!("{}", "-".repeat(70));

    let cfg = SearchConfig::default();
    let queries: Vec<&str> = QUERIES.iter().copied().take(args.queries).collect();
    let warmup: Vec<&str> = QUERIES.iter().copied().cycle().take(args.warmup).collect();
    let embedder = LazyEmbedder::candle();
    let hit_sink: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let mut rows: Vec<serde_json::Value> = Vec::new();

    for &mib in SWEEP_MIB {
        let bytes = (mib as usize) * 1024 * 1024;
        let caps = RuntimeCaps {
            index_cache_bytes: Some(bytes),
            metadata_cache_bytes: Some(bytes),
        };

        let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        let url = pond::config::url_for_path(data_dir)?;
        let store = Store::open_with_options(&url, Default::default(), caps).await?;

        run_search_phase(SearchPhase {
            name: "warm",
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
        .await?;
        let steady = run_search_phase(SearchPhase {
            name: "steady",
            store: &store,
            embedder: &embedder,
            cfg: &cfg,
            sampler: &sampler,
            mode: Some(SearchModeWire::Hybrid),
            queries: &queries,
            limit: args.limit,
            record_hits: false,
            hit_sink: &hit_sink,
        })
        .await?;

        let peak_mib = sampler.peak_kb() as f64 / 1024.0;
        let steady_mib = steady.rss_end_kb as f64 / 1024.0;
        sampler.finish();
        drop(store);

        println!(
            "{:>10}  {:>10}  {:>10.1}  {:>10.1}  {:>8}  {:>8}",
            mib,
            mib,
            peak_mib,
            steady_mib,
            steady.p50(),
            steady.p95(),
        );
        rows.push(serde_json::json!({
            "metadata_mib": mib,
            "index_mib": mib,
            "peak_mib": peak_mib,
            "steady_mib": steady_mib,
            "p50_ms": steady.p50(),
            "p95_ms": steady.p95(),
        }));
    }

    let json = serde_json::json!({
        "mode": "cap_sweep",
        "data_dir": data_dir.display().to_string(),
        "rows": rows,
    });
    println!();
    println!("JSON {json}");
    Ok(())
}
