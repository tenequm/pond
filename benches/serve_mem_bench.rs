#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]
// The macOS `proc_pid_rusage` FFI for phys_footprint sampling needs `unsafe`.
#![allow(unsafe_code)]
#![allow(unreachable_pub, dead_code)]

//! Read-path bench for `pond mcp` / `pond serve`. Opens an existing
//! `~/.local/share/pond/` corpus and measures *pond's own* steady-state read
//! path: the resident meta cache we build, the two retrieval arms (vector
//! default, fts), and `pond_get` hydration - with the cache loaded, the way the
//! server actually serves.
//!
//! It reports total memory at every phase and breaks it into parts - store
//! open, resident meta cache, Lance caches, candle E5 model - so you can see
//! what costs what, nothing omitted. The load-bearing check is the idle floor
//! (`idle_drained`: candle model unloaded the way `pond mcp` idle-unloads it,
//! resident cache still mapped), which must stay under 500 MiB; the serving
//! peak (model resident) only needs to stay under 2 GiB.
//!
//! Phases (sequential, matches MCP's stdio request serialization):
//!   - cold_open     : RSS/PF right after `Store::open`
//!   - build_rowmap  : `ensure_rowmap` builds + mmaps the resident meta cache
//!   - fts_steady    : N fts-arm queries (no embedder) - pond's core read path
//!   - vector_first  : first vector query (the cold E5 model-load spike)
//!   - vector_steady : N vector-arm queries (default arm; needs the embedder)
//!   - get_steady    : N `pond_get` hydration calls on prior hits
//!   - sql_steady    : N `pond_sql` calls (the analytic tool)
//!   - idle/drained  : resting footprint; drained drops the model (cache stays)
//!
//! Run:
//!   cargo bench --bench serve_mem_bench
//!   cargo bench --bench serve_mem_bench -- --queries 50 --skip-idle
//!   cargo bench --bench serve_mem_bench -- --attribute   # per-arm latency
//!   cargo bench --bench serve_mem_bench -- --io-trace    # S3 GETs per arm

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

use anyhow::{Context, Result};
use clap::Parser;
use pond::{
    PROTOCOL_VERSION,
    config::{Config, SearchConfig},
    embed::LazyEmbedder,
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
/// short single-term, multi-term technical, and project-name-ish queries so the
/// arms exercise both rare and common postings.
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

/// `pond_sql` workload: one metadata-only count (manifest, no data read),
/// two column scans, a token filter (FTS-accelerated), and a group-by. Mirrors
/// the analytic shapes the MCP tool actually serves.
const SQL_QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM messages",
    "SELECT MIN(timestamp), MAX(timestamp) FROM messages",
    "SELECT COUNT(*) FROM messages WHERE project LIKE '%pond%'",
    "SELECT message_id FROM messages WHERE contains_tokens(search_text, 'storage') LIMIT 10",
    "SELECT source_agent, COUNT(*) AS n FROM messages GROUP BY source_agent ORDER BY n DESC LIMIT 5",
];

#[derive(Parser)]
#[command(about = "pond mcp/serve read-path bench: resident cache + per-arm search latency/memory")]
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
    /// Idle-footprint ceiling (MiB) for `idle_drained` - the production `pond
    /// mcp` idle state (resident cache mapped, candle model unloaded). The
    /// load-bearing budget: idle pond must sit under this.
    #[arg(long, default_value_t = 500)]
    idle_target_mib: u64,
    /// Serving-peak ceiling (MiB). The peak (model resident) only needs to stay
    /// under this looser bound.
    #[arg(long, default_value_t = 2048)]
    peak_target_mib: u64,
    /// Per-query result limit (mirrors `pond mcp` defaults).
    #[arg(long, default_value_t = 10)]
    limit: usize,
    /// IVF `nprobes` for the vector arm. Unset lets Lance probe up to every
    /// partition (num_rows/4096), which on a remote store is one S3 read per
    /// partition - the dominant cost of an unbounded vector scan.
    #[arg(long)]
    nprobes: Option<usize>,
    /// Attribution mode: time the retrieval arms in isolation -
    /// `searchable_in_scope` (the per-query IsNotNull(search_text) count),
    /// `fts_search`, and `vector_search` - over the query set, printing p50/p95
    /// per arm. Pinpoints which one is the real steady-state floor instead of
    /// only seeing max(arms) through the full `pond_search`. Runs after the
    /// cache build and exits before the normal phases.
    #[arg(long)]
    attribute: bool,
    /// S3 IO attribution: count the exact GETs (read_iops), bytes, and - built
    /// with `--features io-trace` - the per-request paths each warm query issues
    /// against the remote store, broken out by component (scope count / fts /
    /// vector / get). Answers "how much and why are we hitting S3 per query".
    /// Runs after the cache build and exits before the normal phases.
    #[arg(long)]
    io_trace: bool,
    /// Skip the idle drain phase.
    #[arg(long)]
    skip_idle: bool,
    /// Idle drain duration in seconds.
    #[arg(long, default_value_t = 10)]
    idle_seconds: u64,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// macOS phys_footprint accessors (`proc_pid_rusage(RUSAGE_INFO_V4)`). This is
/// what Activity Monitor / `footprint(1)` / `top` / WebKit / psutil read and the
/// only metric the kernel's Jetsam OOM-killer cares about. RSS is wrong in both
/// directions on macOS - overcounts shared dyld pages, undercounts compressed
/// pages. We sample both: RSS shows the resident cache's mmap pages, PF shows
/// they don't count against the memory budget.
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
/// running max since `start()`; `phase_peak_kb` is reset by `mark_phase_start`.
/// All `*_kb` fields are tracked in parallel for both metrics, suffixed `_pf`
/// for phys_footprint.
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
        // Mirror the same reset on the userspace pf atomic so any sampler reads
        // between reset and the next sample don't carry stale values.
        let now_pf = self.current_pf_kb.load(Ordering::Relaxed);
        self.phase_peak_pf_kb.store(now_pf, Ordering::Relaxed);
    }

    fn phase_peak_kb(&self) -> u64 {
        self.phase_peak_kb.load(Ordering::Relaxed)
    }

    /// Kernel-tracked interval peak for phys_footprint since the last
    /// `mark_phase_start`. Falls back to the userspace-sampled peak on non-macOS
    /// or if the syscall failed.
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

fn search_request(query: &str, mode: SearchModeWire, limit: usize) -> SearchRequest {
    SearchRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode,
        sort_by: pond::wire::SortBy::Relevance,
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
        session_limit: 20,
        session_from: pond::wire::SessionFrom::Start,
        session_after_message_id: None,
        session_before_message_id: None,
        message_context_before: 3,
        message_context_after: 3,
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

/// Sum of `rowmetamap-*.rmm` segment sizes in the cache dir - the on-disk size
/// of the resident meta cache (base + any deltas).
fn rowmap_file_bytes(cache_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(cache_dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("rowmetamap-") && name.ends_with(".rmm")
        })
        .filter_map(|entry| entry.metadata().ok().map(|m| m.len()))
        .sum()
}

struct SearchPhase<'a> {
    name: &'static str,
    store: &'a Store,
    embedder: &'a LazyEmbedder,
    cfg: &'a SearchConfig,
    sampler: &'a RssSampler,
    mode: SearchModeWire,
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
        // read-only query. The dataset handles are cached in the shared Session,
        // so this isn't a reopen - it's the same per-request shape
        // `transport.rs` serves.
        let t = Instant::now();
        let tables = Tables {
            sessions: Some(store.dataset(Table::Sessions).await?),
            messages: Some(store.dataset(Table::Messages).await?),
            parts: Some(store.dataset(Table::Parts).await?),
        };
        match sql::run(&tables, q, Mode::Inline, sql::DEFAULT_INLINE_ROWS, None).await {
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
    // compressed pages, but DOES count the resident cache's mmap pages. pf_*:
    // macOS `phys_footprint` (kernel's own ledger; what Activity Monitor,
    // Jetsam, footprint(1) use) - the load-bearing memory-budget metric, and
    // ~excludes the cache's clean file-backed pages (they're reclaimable).
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

fn find_phase<'a>(phases: &'a [PhaseStats], name: &str) -> Option<&'a PhaseStats> {
    phases.iter().find(|p| p.name == name)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let target = resolve_open_target(&args)?;
    if let OpenTarget::Local(dir) = &target
        && !dir.join("sessions.lance").exists()
    {
        anyhow::bail!(
            "no Lance datasets under {} - pass --data-dir or --storage-path",
            dir.display(),
        );
    }

    let cfg = SearchConfig {
        nprobes: args.nprobes,
    };
    let queries: Vec<&str> = QUERIES.iter().copied().take(args.queries).collect();
    let warmup: Vec<&str> = QUERIES.iter().copied().cycle().take(args.warmup).collect();

    println!("=== pond serve read-path bench (resident cache + per-arm search) ===");
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
    println!();

    let sampler = RssSampler::start(Duration::from_millis(args.rss_interval_ms));
    // One pre-open sample so `cold_open` `start` is the pre-pond baseline.
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    let baseline_kb = sampler.current_kb();
    let baseline_pf_kb = sampler.current_pf_kb();
    println!(
        "baseline         RSS {:.1} MiB  PF {:.1} MiB  (this process before pond)",
        baseline_kb as f64 / 1024.0,
        baseline_pf_kb as f64 / 1024.0,
    );
    println!();

    // Arm S3 IO tracing before the store opens so the tracker is injected as the
    // object-store wrapper on every dataset read open.
    if args.io_trace {
        pond::substrate::io_trace::enable();
    }

    // ---- Phase: cold_open ----
    let mut phases: Vec<PhaseStats> = Vec::new();
    sampler.mark_phase_start();
    let t = Instant::now();
    let store = open_bench_store(&target, bench_caps(&args)).await?;
    let open_ms = t.elapsed().as_millis();
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    let (sessions, messages, parts) = store.row_counts().await?;
    phases.push(PhaseStats {
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
        notes: format!("sessions={sessions} messages={messages} parts={parts} open_ms={open_ms}"),
    });

    // ---- Phase: build_rowmap (the resident meta cache - our code) ----
    // `ensure_rowmap` does one sequential scan of `messages`, builds the
    // dict-encoded + block-zstd segment, and mmaps it. The mmap is demand-paged,
    // so the RSS/PF deltas captured here are just the build's transient buffers
    // plus the header - the body faults in as the search phases hydrate hits.
    let cache = tempfile::tempdir()?;
    sampler.mark_phase_start();
    let rss_before = sampler.current_kb();
    let pf_before = sampler.current_pf_kb();
    let t = Instant::now();
    store.ensure_rowmap(cache.path()).await?;
    let build_ms = t.elapsed().as_millis();
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    println!(
        "[probe] lance cache after build: {:.1} MiB",
        store.lance_cache_bytes() as f64 / 1024.0 / 1024.0
    );
    let rmm_mib = rowmap_file_bytes(cache.path()) as f64 / 1024.0 / 1024.0;
    let rss_delta = sampler.current_kb().saturating_sub(rss_before) as f64 / 1024.0;
    let pf_delta = sampler.current_pf_kb().saturating_sub(pf_before) as f64 / 1024.0;
    phases.push(PhaseStats {
        name: "build_rowmap",
        queries: 0,
        elapsed_ms: vec![build_ms],
        rss_start_kb: rss_before,
        rss_end_kb: sampler.current_kb(),
        rss_phase_peak_kb: sampler.phase_peak_kb(),
        rss_global_peak_kb: sampler.peak_kb(),
        pf_start_kb: pf_before,
        pf_end_kb: sampler.current_pf_kb(),
        pf_phase_peak_kb: sampler.phase_peak_pf_kb(),
        pf_global_peak_kb: sampler.peak_pf_kb(),
        notes: format!(
            "{rmm_mib:.1} MiB .rmm on disk, build {build_ms}ms; mmap demand-paged (+{rss_delta:.1} MiB RSS / +{pf_delta:.1} MiB PF so far, file-backed -> reclaimable)"
        ),
    });

    // LazyEmbedder: created but NOT loaded. Only the vector arm touches it; the
    // fts phases run without it, which is what isolates pond's core footprint
    // from the candle model. Idle threshold MAX keeps it resident once loaded,
    // until we explicitly drop it for `idle_drained`.
    let embedder = LazyEmbedder::candle().with_idle_threshold(Duration::MAX);

    // ---- Attribution: isolate the retrieval arms ----
    // pond_search runs `searchable_in_scope` (an IsNotNull(search_text) count)
    // concurrently with the arms via try_join!, so the observed latency is
    // max(scope_count, fts, vector). Time each alone to see the real floor.
    // Exits before the normal phases.
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
        println!("\n=== attribution (isolated arm latency, ms) ===");
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
        // Warm every path so we measure a warm server's steady-state IO, not the
        // one-time cold index/metadata load.
        store.searchable_in_scope(&empty).await?;
        store.fts_search(QUERIES[0], 100, &empty).await?;
        let wv = backend.embed(&[QUERIES[0].to_string()])?;
        store
            .vector_search(wv.first().context("no warm vec")?, 100, &empty, Some(&cfg))
            .await?;
        let warm_hits = store.fts_search(QUERIES[0], 1, &empty).await?;
        let get_id = warm_hits.first().map(|hit| hit.key.message_id.clone());
        if let Some(id) = &get_id {
            let _ = pond_get(&store, get_request(id.clone())).await;
        }
        let _ = pond_search(
            &store,
            &embedder,
            search_request(QUERIES[0], SearchModeWire::Vector, args.limit),
            &cfg,
        )
        .await;
        io_trace::take(); // discard warm IO

        let labels = [
            "scope_count",
            "fts_search",
            "vector_search",
            "pond_get",
            "pond_search",
        ];
        let mut iops: [Vec<u128>; 5] = Default::default();
        let mut rbytes: [Vec<u128>; 5] = Default::default();
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
            // Full request: scope + arm + hydration. Hydration GETs are the
            // remainder once the separately-measured arms are subtracted.
            meas!(4, {
                let _ = pond_search(
                    &store,
                    &embedder,
                    search_request(q, SearchModeWire::Vector, args.limit),
                    &cfg,
                )
                .await;
            });
        }

        println!("\n=== S3 IO per warm query (component isolated) ===");
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
        // Hydration = the full pond_search request minus its arms (scope_count +
        // fts + vector); the remainder is message-meta + per-session-count reads.
        // Derived from p50s (GET counts are additive per request, so the
        // subtraction is exact at each percentile).
        let hydration = |p: f64| {
            percentile(&iops[4], p) as i128
                - percentile(&iops[0], p) as i128
                - percentile(&iops[1], p) as i128
                - percentile(&iops[2], p) as i128
        };
        if !iops[4].is_empty() {
            println!(
                "{:<16}{:>10}{:>10}    (derived: pond_search - scope - fts - vector)",
                "  hydration",
                hydration(0.5),
                hydration(0.95),
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

    let hit_sink: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // ---- Phase: fts_warm ----
    run_search_phase(SearchPhase {
        name: "fts_warm",
        store: &store,
        embedder: &embedder,
        cfg: &cfg,
        sampler: &sampler,
        mode: SearchModeWire::Fts,
        queries: &warmup,
        limit: args.limit,
        record_hits: false,
        hit_sink: &hit_sink,
    })
    .await
    .map(|s| phases.push(s))?;

    // ---- Phase: fts_steady (pond's core read path - no embedder) ----
    run_search_phase(SearchPhase {
        name: "fts_steady",
        store: &store,
        embedder: &embedder,
        cfg: &cfg,
        sampler: &sampler,
        mode: SearchModeWire::Fts,
        queries: &queries,
        limit: args.limit,
        record_hits: true,
        hit_sink: &hit_sink,
    })
    .await
    .map(|s| phases.push(s))?;

    // ---- Phase: vector_first (the cold E5 model-load spike) ----
    sampler.mark_phase_start();
    let rss_start_kb = sampler.current_kb();
    let pf_start_kb = sampler.current_pf_kb();
    let request = search_request(QUERIES[0], SearchModeWire::Vector, args.limit);
    let t = Instant::now();
    let envelope = pond_search(&store, &embedder, request, &cfg).await;
    let first_ms = t.elapsed().as_millis();
    if let SearchEnvelope::Error(error) = envelope {
        anyhow::bail!("vector_first failed: {error:?}");
    }
    thread::sleep(Duration::from_millis(args.rss_interval_ms * 2));
    phases.push(PhaseStats {
        name: "vector_first",
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
        notes: format!("first_call_ms={first_ms} (includes E5 model load + vector index)"),
    });

    // ---- Phase: vector_steady (the default arm) ----
    run_search_phase(SearchPhase {
        name: "vector_steady",
        store: &store,
        embedder: &embedder,
        cfg: &cfg,
        sampler: &sampler,
        mode: SearchModeWire::Vector,
        queries: &queries,
        limit: args.limit,
        record_hits: true,
        hit_sink: &hit_sink,
    })
    .await
    .map(|s| phases.push(s))?;

    // ---- Phase: get_steady (pond_get hydration on prior hits) ----
    let ids: Vec<String> = {
        let guard = hit_sink.lock().unwrap();
        guard.iter().take(args.queries).cloned().collect()
    };
    if !ids.is_empty() {
        run_get_phase("get_steady", &store, &sampler, &ids)
            .await
            .map(|s| phases.push(s))?;
    }

    // ---- Phase: sql_steady (the pond_sql analytic tool) ----
    run_sql_phase("sql_steady", &store, &sampler, SQL_QUERIES)
        .await
        .map(|s| phases.push(s))?;

    // ---- Phase: idle / idle_drained ----
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
            notes: format!(
                "slept {}s with no requests (model resident)",
                args.idle_seconds
            ),
        });

        // Drop the embedder to simulate `pond mcp`'s idle-unload of the candle
        // model; the resident cache (mmap) stays. This is the resting floor a
        // server settles to between bursts - pond's own footprint, model gone.
        println!(
            "[probe] lance cache at idle: {:.1} MiB",
            store.lance_cache_bytes() as f64 / 1024.0 / 1024.0
        );
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
            notes: "embedder dropped; cache stays resident (pond resting floor)".to_owned(),
        });
    }

    let peak_rss = sampler.peak_kb() as f64 / 1024.0;
    let peak_pf = sampler.peak_pf_kb() as f64 / 1024.0;
    sampler.finish();

    println!();
    print_phase_header();
    for phase in &phases {
        print_phase_row(phase);
    }

    // ---- Total memory, attributed by part (nothing subtracted away) ----
    // PF (phys_footprint) is the macOS "Memory" number Jetsam enforces; RSS also
    // counts the resident cache's file-backed mmap pages (reclaimable, so they
    // are real RSS but ~free in PF). We report both at every line.
    let mib = |kb: u64| kb as f64 / 1024.0;
    let cold = find_phase(&phases, "cold_open");
    let idle_model = find_phase(&phases, "idle");
    let idle_drained = find_phase(&phases, "idle_drained");
    let idle_floor_pf = idle_drained.map(|p| mib(p.pf_end_kb));

    println!();
    println!(
        "=== memory attribution (PF = macOS \"Memory\"/Jetsam; RSS also counts the cache mmap) ==="
    );
    println!(
        "  baseline (pre-pond)          PF {:>7.1}   RSS {:>7.1} MiB",
        mib(baseline_pf_kb),
        mib(baseline_kb),
    );
    if let Some(c) = cold {
        println!(
            "  + store open                 PF {:>+7.1}   RSS {:>+7.1} MiB",
            mib(c.pf_end_kb) - mib(baseline_pf_kb),
            mib(c.rss_end_kb) - mib(baseline_kb),
        );
    }
    println!(
        "  + resident meta cache        {rmm_mib:.1} MiB .rmm on disk, built {build_ms}ms (mmap; pages file-backed -> reclaimable, ~0 PF)"
    );
    if let Some(d) = idle_drained {
        println!(
            "  = idle floor (model off)     PF {:>7.1}   RSS {:>7.1} MiB   <- idle `pond mcp`",
            mib(d.pf_end_kb),
            mib(d.rss_end_kb),
        );
    }
    if let (Some(i), Some(d)) = (idle_model, idle_drained) {
        println!(
            "  + candle E5 model (serving)  PF {:>+7.1}   RSS {:>+7.1} MiB   (lazy; vector arm only)",
            mib(i.pf_end_kb) - mib(d.pf_end_kb),
            mib(i.rss_end_kb) - mib(d.rss_end_kb),
        );
    }
    println!("  = serving peak               PF {peak_pf:>7.1}   RSS {peak_rss:>7.1} MiB");

    println!();
    let idle_pass = match idle_floor_pf {
        Some(floor) => {
            let ok = floor <= args.idle_target_mib as f64;
            println!(
                "  idle target  < {:>4} MiB PF   {}   (idle floor {floor:.1} MiB, headroom {:+.1})",
                args.idle_target_mib,
                if ok { "PASS" } else { "FAIL" },
                args.idle_target_mib as f64 - floor,
            );
            ok
        }
        None => {
            println!("  idle target  skipped (--skip-idle; no idle floor measured)");
            true
        }
    };
    let peak_pass = peak_pf <= args.peak_target_mib as f64;
    println!(
        "  peak target  < {:>4} MiB PF   {}   (peak {peak_pf:.1} MiB, headroom {:+.1})",
        args.peak_target_mib,
        if peak_pass { "PASS" } else { "FAIL" },
        args.peak_target_mib as f64 - peak_pf,
    );
    let pass = idle_pass && peak_pass;

    // JSON one-liner for diffing across runs.
    let json_phases: Vec<_> = phases
        .iter()
        .map(|p| {
            serde_json::json!({
                "phase": p.name,
                "n": p.queries,
                "rss_end_mib": p.rss_end_kb as f64 / 1024.0,
                "rss_phase_peak_mib": p.rss_phase_peak_kb as f64 / 1024.0,
                "pf_end_mib": p.pf_end_kb as f64 / 1024.0,
                "pf_phase_peak_mib": p.pf_phase_peak_kb as f64 / 1024.0,
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
        "rowmap_mib": rmm_mib,
        "build_rowmap_ms": build_ms,
        "idle_floor_pf_mib": idle_floor_pf,
        "peak_pf_mib": peak_pf,
        "peak_rss_mib": peak_rss,
        "idle_target_mib": args.idle_target_mib,
        "peak_target_mib": args.peak_target_mib,
        "pass": pass,
        "phases": json_phases,
    });
    println!();
    println!("JSON {json}");

    if !pass {
        std::process::exit(1);
    }
    Ok(())
}
