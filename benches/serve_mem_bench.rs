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
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::InvertedIndexParams;
use pond::{
    PROTOCOL_VERSION,
    config::{Config, SearchConfig},
    embed::{CandleEmbedder, Embedder, LazyEmbedder},
    handlers::{pond_get, pond_search},
    sessions::Store,
    sql::{self, Mode, Tables},
    substrate::{Predicate, ResolvedStorage, RuntimeCaps, StorageUrl, Table},
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

/// `pond_sql_query` workload: one metadata-only count (manifest, no data
/// read), two column scans, a token filter (FTS-accelerated), and a group-by.
/// Mirrors the analytic shapes the MCP tool actually serves so the SQL phase
/// exercises both the cheap-metadata and the read-the-column-from-S3 paths.
const SQL_QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM messages",
    "SELECT MIN(timestamp), MAX(timestamp) FROM messages",
    "SELECT COUNT(*) FROM messages WHERE project LIKE '%pond%'",
    "SELECT message_id FROM messages WHERE contains_tokens(search_text, 'storage') LIMIT 10",
    "SELECT source_agent, COUNT(*) AS n FROM messages GROUP BY source_agent ORDER BY n DESC LIMIT 5",
];

#[derive(Parser)]
#[command(about = "pond mcp/serve read-only memory bench: peak RSS per phase vs target ceiling")]
struct Args {
    /// Pond data directory to open read-only. Defaults to `~/.local/share/pond`
    /// when present.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Remote storage URL (e.g. `s3+https://host/bucket/prefix`). When set, the
    /// bench opens the remote store with creds resolved from the pond config
    /// instead of `--data-dir` - this is how we measure real S3 read cost.
    #[arg(long)]
    storage_path: Option<String>,
    /// Config file for creds (default `~/.config/pond/config.toml`). Only used
    /// with `--storage-path`.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Override the Lance index cache cap (MiB). Default: remote 1024, local 256.
    #[arg(long)]
    index_cache_mib: Option<u64>,
    /// Override the Lance metadata cache cap (MiB). Default: remote 512, local 128.
    #[arg(long)]
    metadata_cache_mib: Option<u64>,
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
    /// Warm the search indices (vector + FTS) right after open, as a measured
    /// `prewarm` phase, mirroring `pond mcp` startup. With it on, the cold S3
    /// index load shows up here instead of in `fts_warm`/`first_hybrid`.
    #[arg(long)]
    prewarm: bool,
    /// IVF `nprobes` for the vector arm. Unset lets Lance probe up to every
    /// partition (num_rows/4096), which on a remote store is one S3 read per
    /// partition - the dominant cost of an unbounded vector scan.
    #[arg(long)]
    nprobes: Option<usize>,
    /// Attribution mode: time the three hybrid components in isolation -
    /// `searchable_in_scope` (the per-query IsNotNull(search_text) count),
    /// `fts_search`, and `vector_search` - over the query set, printing p50/p95
    /// per component. Pinpoints which one is the real steady-state floor instead
    /// of only seeing max(arms) through the full `pond_search`. Runs after
    /// prewarm and exits before the normal phases.
    #[arg(long)]
    attribute: bool,
    /// S3 IO attribution: count the exact GETs (read_iops), bytes, and - built
    /// with `--features io-trace` - the per-request paths each warm query issues
    /// against the remote store, broken out by component (scope count / fts /
    /// vector / get). Answers "how much and why are we hitting S3 per query".
    /// Runs after prewarm and exits before the normal phases.
    #[arg(long)]
    io_trace: bool,
    /// Recall mode: score hybrid retrieval against a ground-truth TSV
    /// (`id\tlang\tstratum\tquery\tground_truth`, ground_truth = `prefix:<8-char
    /// session/msg id>,...`). Runs one hybrid search per query, prints overall
    /// Success@3 / P@1 / MRR and per-query ranks. Used to confirm a vector-index
    /// change (e.g. PQ->SQ) holds recall. Runs after prewarm, exits before the
    /// normal phases. `--storage-path` selects the corpus; no Python harness.
    #[arg(long)]
    recall: Option<std::path::PathBuf>,
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
    /// Rebuild the `search_text` inverted index under each tokenizer (simple,
    /// whitespace, ngram) on a LOCAL `--data-dir` copy and report index size +
    /// FTS RAM + latency per tokenizer. Requires a local data dir (mutates its
    /// indexes); never point it at a shared remote store.
    #[arg(long)]
    tokenizer_sweep: bool,
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
        filters: SearchFilters::default(),
        limit,
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
        session_from: pond::wire::SessionFrom::Start,
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

/// Classify an S3 object path into a coarse bucket so the io-trace request
/// histogram shows *what* each GET is for: a specific index file (FTS posting
/// segment, IVF partition store), a data fragment, the manifest, or the
/// transaction log.
#[cfg(feature = "io-trace")]
fn io_bucket(path: &str) -> String {
    if let Some(pos) = path.find("/_indices/") {
        let after = &path[pos + "/_indices/".len()..];
        let file = after.rsplit('/').next().unwrap_or(after);
        format!("index/{file}")
    } else if path.contains("/data/") {
        "data".to_string()
    } else if path.contains("manifest") || path.contains("/_versions/") {
        "manifest".to_string()
    } else if path.contains("_transactions") {
        "txn".to_string()
    } else {
        path.rsplit('/').next().unwrap_or(path).to_string()
    }
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

async fn run_sql_phase(
    name: &'static str,
    store: &Store,
    sampler: &RssSampler,
    queries: &[&str],
) -> Result<PhaseStats> {
    let rss_start_kb = sampler.current_kb();
    let pf_start_kb = sampler.current_pf_kb();
    sampler.mark_phase_start();
    let mut elapsed_ms: Vec<u128> = Vec::with_capacity(queries.len());

    for q in queries {
        // Mirror the MCP tool exactly: build `Tables` fresh per call (the
        // try_join of the three dataset() freshness gates) then run one
        // read-only query. The dataset handles are cached in the shared
        // Session, so this isn't a reopen - it's the same per-request shape
        // `transport.rs` serves.
        let t = Instant::now();
        let tables = Tables {
            sessions: Some(store.dataset(Table::Sessions).await?),
            messages: Some(store.dataset(Table::Messages).await?),
            parts: Some(store.dataset(Table::Parts).await?),
        };
        match sql::run(&tables, q, Mode::Inline, sql::DEFAULT_INLINE_ROWS).await {
            Ok(_) => {}
            Err(error) => anyhow::bail!("{name}: sql {q:?} failed: {error:?}"),
        }
        elapsed_ms.push(t.elapsed().as_millis());
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

/// Where the bench opens its store. `Local` keeps the original
/// `~/.local/share/pond` behavior; `Remote` carries a creds-resolved S3
/// destination so we can measure real object-store read cost.
enum OpenTarget {
    Local(PathBuf),
    Remote(Box<ResolvedStorage>),
}

fn target_label(target: &OpenTarget) -> String {
    match target {
        OpenTarget::Local(path) => format!("local  {}", path.display()),
        OpenTarget::Remote(resolved) => format!("remote {}", resolved.lance_url()),
    }
}

fn load_bench_config(args: &Args) -> Result<Config> {
    let path = args.config.clone().unwrap_or_else(|| {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_default();
        home.join(".config").join("pond").join("config.toml")
    });
    Config::load(&path).with_context(|| format!("load config {}", path.display()))
}

/// Explicit cache caps from CLI; `None` lets the substrate pick its
/// backend-aware default (remote 2 GiB/512 MiB, local 256/128 MiB).
fn bench_caps(args: &Args) -> RuntimeCaps {
    RuntimeCaps {
        index_cache_bytes: args.index_cache_mib.map(|m| (m as usize) * 1024 * 1024),
        metadata_cache_bytes: args.metadata_cache_mib.map(|m| (m as usize) * 1024 * 1024),
    }
}

fn resolve_open_target(args: &Args) -> Result<OpenTarget> {
    if let Some(storage_path) = &args.storage_path {
        let config = load_bench_config(args)?;
        let url = StorageUrl::parse(storage_path).context("parse --storage-path")?;
        let resolved = url
            .resolve(&config.creds)
            .context("resolve creds for --storage-path")?;
        Ok(OpenTarget::Remote(Box::new(resolved)))
    } else {
        Ok(OpenTarget::Local(resolve_data_dir(args.data_dir.clone())?))
    }
}

async fn open_bench_store(target: &OpenTarget, caps: RuntimeCaps) -> Result<Store> {
    match target {
        OpenTarget::Local(dir) => {
            let url = pond::config::url_for_path(dir)?;
            Store::open_with_options(&url, std::collections::HashMap::new(), caps).await
        }
        OpenTarget::Remote(resolved) => {
            Store::open_with_options(resolved.lance_url(), resolved.options.clone(), caps).await
        }
    }
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

    if args.tokenizer_sweep {
        return run_tokenizer_sweep(&args).await;
    }

    let target = resolve_open_target(&args)?;
    if let OpenTarget::Local(dir) = &target
        && !dir.join("sessions.lance").exists()
    {
        anyhow::bail!(
            "no Lance datasets under {} - pass --data-dir or --storage-path",
            dir.display(),
        );
    }

    if args.cap_sweep {
        return run_cap_sweep(&args, &target).await;
    }

    let cfg = SearchConfig {
        nprobes: args.nprobes,
    };
    let queries: Vec<&str> = QUERIES.iter().copied().take(args.queries).collect();
    let warmup: Vec<&str> = QUERIES.iter().copied().cycle().take(args.warmup).collect();

    println!("=== pond serve-mem bench (read-only) ===");
    println!("store            {}", target_label(&target));
    println!(
        "caps             index={} metadata={} (None = backend default)",
        args.index_cache_mib
            .map_or_else(|| "default".to_owned(), |m| format!("{m} MiB")),
        args.metadata_cache_mib
            .map_or_else(|| "default".to_owned(), |m| format!("{m} MiB")),
    );
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

    // Arm S3 IO tracing before the store opens so the tracker is injected as
    // the object-store wrapper on every dataset read open.
    if args.io_trace {
        pond::substrate::io_trace::enable();
    }

    // ---- Phase: cold_open ----
    let mut phases: Vec<PhaseStats> = Vec::new();
    sampler.mark_phase_start();
    let t = Instant::now();
    let store = open_bench_store(&target, bench_caps(&args)).await?;
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

    // LazyEmbedder created but NOT loaded yet - matches `pond mcp` lazy
    // behavior. With `--prewarm` it is warmed in the prewarm phase (mirroring
    // a startup embedder prewarm), so eviction is disabled to keep the model
    // resident through the long S3 phases up to `first_hybrid`.
    let embedder = if args.prewarm {
        LazyEmbedder::candle().with_idle_threshold(Duration::MAX)
    } else {
        LazyEmbedder::candle()
    };

    // ---- Phase: prewarm (optional; mirrors `pond mcp` startup) ----
    if args.prewarm {
        sampler.mark_phase_start();
        let rss_start_kb = sampler.current_kb();
        let pf_start_kb = sampler.current_pf_kb();
        let t = Instant::now();
        let cache = tempfile::tempdir()?;
        store.prewarm(cache.path()).await?;
        // Warm the E5 model too, off the request path - production loads it at
        // startup so the first user hybrid query never pays the model load.
        let embed_t = Instant::now();
        embedder.get().await?;
        let embed_ms = embed_t.elapsed().as_millis();
        let prewarm_ms = t.elapsed().as_millis();
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        phases.push(PhaseStats {
            name: "prewarm",
            queries: 0,
            elapsed_ms: vec![prewarm_ms],
            rss_start_kb,
            rss_end_kb: sampler.current_kb(),
            rss_phase_peak_kb: sampler.phase_peak_kb(),
            rss_global_peak_kb: sampler.peak_kb(),
            pf_start_kb,
            pf_end_kb: sampler.current_pf_kb(),
            pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
            pf_global_peak_kb: sampler.peak_pf_kb(),
            notes: format!(
                "prewarm_ms={prewarm_ms} (vector prewarm_index + synthetic FTS, e5 model {embed_ms}ms)"
            ),
        });
    }

    // ---- Phase: sql_cold (first sql_query on a freshly opened store) ----
    // One column-scan query, cache cold - the cold-start a `pond_sql_query`
    // caller pays before anything is resident.
    run_sql_phase("sql_cold", &store, &sampler, &SQL_QUERIES[1..2])
        .await
        .map(|s| phases.push(s))?;

    // ---- Phase: sql_steady (warm-cache sql_query latency) ----
    run_sql_phase("sql_steady", &store, &sampler, SQL_QUERIES)
        .await
        .map(|s| phases.push(s))?;

    // ---- Attribution: isolate the three hybrid components ----
    // pond_search runs `searchable_in_scope` (an IsNotNull(search_text) count)
    // concurrently with the retrieval arms via try_join!, so the observed
    // latency is max(scope_count, fts, vector). Time each alone to see the
    // real floor. Exits before the normal phases.
    if args.attribute {
        let empty = Predicate::And(Vec::new());
        let backend = embedder.get().await?;
        // Warm each path once so steady-state cache state matches the phases.
        store.searchable_in_scope(&empty).await?;
        store.fts_search(QUERIES[0], 100, &empty).await?;
        let warm_vec = backend.embed(&[QUERIES[0].to_string()])?;
        store
            .vector_search(
                warm_vec.first().context("no warm vec")?,
                100,
                &empty,
                Some(&cfg),
            )
            .await?;

        let mut scope_ms = Vec::new();
        let mut fts_ms = Vec::new();
        let mut vec_ms = Vec::new();
        for q in &queries {
            let t = Instant::now();
            store.searchable_in_scope(&empty).await?;
            scope_ms.push(t.elapsed().as_millis());

            let t = Instant::now();
            store.fts_search(q, 100, &empty).await?;
            fts_ms.push(t.elapsed().as_millis());

            let v = backend.embed(&[(*q).to_string()])?;
            let t = Instant::now();
            store
                .vector_search(v.first().context("no vec")?, 100, &empty, Some(&cfg))
                .await?;
            vec_ms.push(t.elapsed().as_millis());
        }
        println!("\n=== attribution (isolated component latency, ms) ===");
        println!("{:<22}{:>8}{:>8}", "component", "p50", "p95");
        for (name, v) in [
            ("searchable_in_scope", &scope_ms),
            ("fts_search", &fts_ms),
            ("vector_search", &vec_ms),
        ] {
            println!(
                "{name:<22}{:>8}{:>8}",
                percentile(v, 0.5),
                percentile(v, 0.95)
            );
        }
        println!("raw scope_ms={scope_ms:?}");
        println!("raw fts_ms={fts_ms:?}");
        println!("raw vec_ms={vec_ms:?}");
        return Ok(());
    }

    // ---- IO attribution: exact S3 GETs per warm query, per component ----
    if args.io_trace {
        use pond::substrate::io_trace;
        let empty = Predicate::And(Vec::new());
        let backend = embedder.get().await?;
        // Warm every path so we measure a warm server's steady-state IO, not
        // the one-time cold index/metadata load.
        store.searchable_in_scope(&empty).await?;
        store.fts_search(QUERIES[0], 100, &empty).await?;
        let wv = backend.embed(&[QUERIES[0].to_string()])?;
        store
            .vector_search(wv.first().context("no warm vec")?, 100, &empty, Some(&cfg))
            .await?;
        let warm_hits = store.fts_search(QUERIES[0], 1, &empty).await?;
        let get_id = warm_hits.first().map(|(key, _)| key.message_id.clone());
        if let Some(id) = &get_id {
            let _ = pond_get(&store, get_request(id.clone())).await;
        }
        io_trace::take(); // discard warm IO

        let labels = ["scope_count", "fts_search", "vector_search", "pond_get"];
        let mut iops: [Vec<u128>; 4] = Default::default();
        let mut rbytes: [Vec<u128>; 4] = Default::default();
        #[cfg(feature = "io-trace")]
        let mut hist: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();

        macro_rules! meas {
            ($idx:expr, $body:expr) => {{
                io_trace::take();
                $body;
                let s = io_trace::take().unwrap_or_default();
                iops[$idx].push(u128::from(s.read_iops));
                rbytes[$idx].push(u128::from(s.read_bytes));
                #[cfg(feature = "io-trace")]
                for r in &s.requests {
                    let key = format!(
                        "{:<14}{:>11}  {}",
                        labels[$idx],
                        r.method,
                        io_bucket(&r.path.to_string())
                    );
                    let entry = hist.entry(key).or_insert((0u64, 0u64));
                    entry.0 += 1;
                    entry.1 += r.range.as_ref().map_or(0, |x| x.end - x.start);
                }
            }};
        }

        for q in &queries {
            meas!(0, store.searchable_in_scope(&empty).await?);
            meas!(1, store.fts_search(q, 100, &empty).await?);
            let v = backend.embed(&[(*q).to_string()])?;
            meas!(
                2,
                store
                    .vector_search(v.first().context("no vec")?, 100, &empty, Some(&cfg))
                    .await?
            );
            if let Some(id) = &get_id {
                meas!(3, {
                    let _ = pond_get(&store, get_request(id.clone())).await;
                });
            }
        }

        println!("\n=== S3 IO per warm query (component isolated, optimized corpus) ===");
        println!(
            "{:<16}{:>10}{:>10}{:>13}{:>13}",
            "component", "iops_p50", "iops_p95", "bytes_p50", "bytes_p95"
        );
        for i in 0..labels.len() {
            if iops[i].is_empty() {
                continue;
            }
            println!(
                "{:<16}{:>10}{:>10}{:>13}{:>13}",
                labels[i],
                percentile(&iops[i], 0.5),
                percentile(&iops[i], 0.95),
                percentile(&rbytes[i], 0.5),
                percentile(&rbytes[i], 0.95),
            );
        }
        #[cfg(feature = "io-trace")]
        {
            println!(
                "\n=== request breakdown (component / method / path-bucket -> reqs, bytes), summed over {} queries ===",
                queries.len()
            );
            let mut rows: Vec<(String, (u64, u64))> = hist.into_iter().collect();
            rows.sort_by_key(|row| std::cmp::Reverse(row.1.0));
            for (key, (count, bytes)) in rows.into_iter().take(50) {
                println!("{count:>6} reqs  {bytes:>11} B   {key}");
            }
        }
        #[cfg(not(feature = "io-trace"))]
        println!("(build with --features io-trace for the per-request path breakdown)");
        return Ok(());
    }

    // ---- Recall: score hybrid retrieval against a ground-truth TSV ----
    if let Some(path) = &args.recall {
        let tsv = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read recall TSV {}", path.display()))?;
        let mut n = 0usize;
        let mut s_at_3 = 0usize;
        let mut p_at_1 = 0usize;
        let mut mrr_sum = 0.0f64;
        println!("\n=== recall (hybrid, {}) ===", target_label(&target));
        println!("{:<10}{:>6}  query", "id", "rank");
        for line in tsv.lines().skip(1) {
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.len() < 5 {
                continue;
            }
            let (id, query, gt) = (cols[0], cols[3], cols[4]);
            let tokens: Vec<&str> = gt
                .strip_prefix("prefix:")
                .unwrap_or(gt)
                .split(',')
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .collect();
            let request = search_request(query, Some(SearchModeWire::Hybrid), args.limit);
            let rank = match pond_search(&store, &embedder, request, &cfg).await {
                SearchEnvelope::Success(response) => {
                    let mut hit_rank = 0usize;
                    let mut position = 0usize;
                    'scan: for session in &response.sessions {
                        let sid: String = session.session_id.chars().take(8).collect();
                        for hit in &session.matches {
                            position += 1;
                            let mid: String = hit.message_id.chars().take(8).collect();
                            if tokens.iter().any(|token| *token == sid || *token == mid) {
                                hit_rank = position;
                                break 'scan;
                            }
                        }
                    }
                    hit_rank
                }
                SearchEnvelope::Error(error) => {
                    anyhow::bail!("recall: query {id} failed: {error:?}")
                }
            };
            n += 1;
            if rank == 1 {
                p_at_1 += 1;
            }
            if rank != 0 && rank <= 3 {
                s_at_3 += 1;
            }
            if rank != 0 {
                #[allow(clippy::cast_precision_loss)]
                let recip = 1.0 / rank as f64;
                mrr_sum += recip;
            }
            let rank_disp = if rank == 0 {
                "miss".to_owned()
            } else {
                rank.to_string()
            };
            println!("{id:<10}{rank_disp:>6}  {query}");
        }
        if n > 0 {
            #[allow(clippy::cast_precision_loss)]
            let (success_at_3, precision_at_1, mrr) = (
                s_at_3 as f64 / n as f64,
                p_at_1 as f64 / n as f64,
                mrr_sum / n as f64,
            );
            println!("\nN={n}  Success@3={success_at_3:.3}  P@1={precision_at_1:.3}  MRR={mrr:.3}");
        }
        return Ok(());
    }

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

        // ---- Phase: idle_drained (the achievable low-idle floor) ----
        // Drop the embedder to simulate the idle-unload of the candle model
        // (the ~5-min unload we want in `pond mcp`), then sleep and resample.
        // This is the load-bearing question: can the server sit idle-cheap and
        // spike only during requests? Compares directly against the `idle`
        // floor above (model still resident).
        drop(embedder);
        sampler.mark_phase_start();
        let rss_start_kb = sampler.current_kb();
        let pf_start_kb = sampler.current_pf_kb();
        thread::sleep(Duration::from_secs(args.idle_seconds));
        phases.push(PhaseStats {
            name: "idle_drained",
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
            notes: "embedder dropped (idle-unload); slept to measure drained floor".to_owned(),
        });

        // ---- Phase: from_idle (the goal metric) ----
        // The literal "search latency from idle on remote storage": recreate the
        // embedder the way `pond mcp` does on the next call after an idle-unload,
        // then run the hybrid query set with no warmup. Query 1 pays the E5
        // reload (~0.4s) and whatever throttle state survived the idle gap; the
        // rest show how fast it recovers. Indices stay cached across idle, so
        // this isolates per-query S3 read cost, not cold index load.
        if !args.skip_hybrid {
            let reloaded = LazyEmbedder::candle().with_idle_threshold(Duration::MAX);
            run_search_phase(SearchPhase {
                name: "from_idle",
                store: &store,
                embedder: &reloaded,
                cfg: &cfg,
                sampler: &sampler,
                mode: Some(SearchModeWire::Hybrid),
                queries: &queries,
                limit: args.limit,
                record_hits: false,
                hit_sink: &hit_sink,
            })
            .await
            .map(|s| phases.push(s))?;
        }
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
        "store": target_label(&target),
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

/// Sum of all regular-file sizes under `path` (recursive, best-effort).
fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&p) else {
            continue;
        };
        for entry in entries.flatten() {
            match entry.file_type() {
                Ok(ft) if ft.is_dir() => stack.push(entry.path()),
                Ok(_) => total += entry.metadata().map(|m| m.len()).unwrap_or(0),
                Err(_) => {}
            }
        }
    }
    total
}

/// Size of the most-recently-modified subdir under `_indices` - the index just
/// built. Lance keeps prior index versions until GC, so a before/after delta is
/// unreliable; the newest subdir is the new index.
fn newest_index_size_bytes(indices_dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(indices_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .max_by_key(|e| e.metadata().and_then(|m| m.modified()).ok())
        .map(|e| dir_size_bytes(&e.path()))
        .unwrap_or(0)
}

/// Rebuild the `search_text` inverted index under one tokenizer; return build ms.
/// Matches pond's production FTS config (stem off, stop-words kept) so only the
/// tokenizer differs across variants.
/// Open the `messages` dataset for index rebuild - locally by path, or remotely
/// via the resolved S3 URL + creds (mirrors how pond's substrate opens it).
async fn open_messages_dataset(target: &OpenTarget) -> Result<Dataset> {
    match target {
        OpenTarget::Local(dir) => {
            let path = dir.join("messages.lance");
            Ok(Dataset::open(path.to_str().context("non-utf8 path")?).await?)
        }
        OpenTarget::Remote(resolved) => {
            let base = resolved.lance_url().as_str().trim_end_matches('/');
            let uri = format!("{base}/messages.lance");
            Ok(DatasetBuilder::from_uri(&uri)
                .with_storage_options(resolved.options.clone())
                .load()
                .await?)
        }
    }
}

async fn build_fts_index(dataset: &mut Dataset, tokenizer: &str) -> Result<u128> {
    let params = match tokenizer {
        // The former ngram config (min=3, max=5), kept as a sweep comparison
        // point now that production indexes with word+stem.
        "ngram" => InvertedIndexParams::default()
            .base_tokenizer("ngram".to_owned())
            .ngram_min_length(3)
            .ngram_max_length(5)
            .stem(false)
            .remove_stop_words(false),
        "whitespace" => InvertedIndexParams::default()
            .base_tokenizer("whitespace".to_owned())
            .stem(false)
            .remove_stop_words(false),
        _ => InvertedIndexParams::default()
            .stem(false)
            .remove_stop_words(false),
    };
    let t = Instant::now();
    // Reuse pond's exact index name so replace(true) overwrites the existing
    // FTS index rather than leaving two inverted indexes on search_text.
    dataset
        .create_index_builder(&["search_text"], IndexType::Inverted, &params)
        .name("messages_search_text_fts".to_owned())
        .replace(true)
        .await?;
    Ok(t.elapsed().as_millis())
}

/// Rebuild the FTS index under each tokenizer on a LOCAL corpus copy and report
/// index size + FTS peak RSS + p50/p95 latency. Answers "do other tokenizers
/// move the needle" without mutating any shared remote store.
async fn run_tokenizer_sweep(args: &Args) -> Result<()> {
    let target = resolve_open_target(args)?;
    // Index dir is only locally measurable; remote reports size as n/a (the
    // point at 2M-scale is RAM + latency, not on-disk size which we already
    // have from the local small-corpus sweep).
    let local_indices_dir = match &target {
        OpenTarget::Local(dir) => Some(dir.join("messages.lance").join("_indices")),
        OpenTarget::Remote(_) => None,
    };

    println!("=== pond serve-mem bench: --tokenizer-sweep ===");
    println!("store            {}", target_label(&target));
    println!(
        "caps             index={} metadata={}",
        args.index_cache_mib
            .map_or_else(|| "default".to_owned(), |m| format!("{m} MiB")),
        args.metadata_cache_mib
            .map_or_else(|| "default".to_owned(), |m| format!("{m} MiB")),
    );
    println!(
        "queries/phase    {} (warmup={}, limit={})",
        args.queries, args.warmup, args.limit
    );
    println!();
    println!(
        "{:>11}  {:>10}  {:>9}  {:>9}  {:>8}  {:>8}",
        "tokenizer", "index_MiB", "build_s", "ftsPk_MB", "p50_ms", "p95_ms",
    );
    println!("{}", "-".repeat(70));

    let cfg = SearchConfig::default();
    let queries: Vec<&str> = QUERIES.iter().copied().take(args.queries).collect();
    let warmup: Vec<&str> = QUERIES.iter().copied().cycle().take(args.warmup).collect();
    let embedder = LazyEmbedder::candle();
    let hit_sink: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let mut rows: Vec<serde_json::Value> = Vec::new();

    for tokenizer in ["simple", "whitespace", "ngram"] {
        let mut dataset = open_messages_dataset(&target).await?;
        let build_ms = build_fts_index(&mut dataset, tokenizer).await?;
        drop(dataset);
        let index_mib = local_indices_dir
            .as_ref()
            .map(|d| newest_index_size_bytes(d) as f64 / 1024.0 / 1024.0);

        let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
        thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
        let store = open_bench_store(&target, bench_caps(args)).await?;
        run_search_phase(SearchPhase {
            name: "warm",
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
        .await?;
        let steady = run_search_phase(SearchPhase {
            name: "steady",
            store: &store,
            embedder: &embedder,
            cfg: &cfg,
            sampler: &sampler,
            mode: Some(SearchModeWire::Fts),
            queries: &queries,
            limit: args.limit,
            record_hits: false,
            hit_sink: &hit_sink,
        })
        .await?;
        let fts_peak_mib = sampler.peak_pf_kb() as f64 / 1024.0;
        sampler.finish();
        drop(store);

        let index_col = index_mib.map_or_else(|| "n/a".to_owned(), |m| format!("{m:.1}"));
        println!(
            "{:>11}  {:>10}  {:>9.1}  {:>9.1}  {:>8}  {:>8}",
            tokenizer,
            index_col,
            build_ms as f64 / 1000.0,
            fts_peak_mib,
            steady.p50(),
            steady.p95(),
        );
        rows.push(serde_json::json!({
            "tokenizer": tokenizer,
            "index_mib": index_mib,
            "build_s": build_ms as f64 / 1000.0,
            "fts_peak_pf_mib": fts_peak_mib,
            "p50_ms": steady.p50(),
            "p95_ms": steady.p95(),
        }));
    }

    println!();
    println!(
        "JSON {}",
        serde_json::json!({
            "mode": "tokenizer_sweep",
            "store": target_label(&target),
            "rows": rows,
        })
    );
    Ok(())
}

/// Sweep `(metadata, index)` Lance cache caps across a fixed MiB grid and
/// print peak RSS + p50 / p95 hybrid latency for each. Used to calibrate the
/// `[runtime]` defaults against a real corpus (`docs/plans/mcp-memory-budget.md`
/// Q5). The E5 embedder loads once and is reused across grid points so the
/// model-load spike is not double-counted.
async fn run_cap_sweep(args: &Args, target: &OpenTarget) -> Result<()> {
    const SWEEP_MIB: &[u64] = &[32, 64, 128, 256, 512, 1024];

    println!("=== pond serve-mem bench: --cap-sweep ===");
    println!("store            {}", target_label(target));
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
        let store = open_bench_store(target, caps).await?;

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
        "store": target_label(target),
        "rows": rows,
    });
    println!();
    println!("JSON {json}");
    Ok(())
}
