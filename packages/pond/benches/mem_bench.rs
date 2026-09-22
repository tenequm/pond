#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Memory scenarios for #245 - the harness behind the `gate` bench.
//!
//! One scenario per process invocation (`--scenario <name>`): peak RSS is a
//! process-lifetime high-water mark and dhat cannot reset, so the runner
//! invokes this binary once per scenario and reads one JSON row off stdout.
//!
//! | scenario | exercises | catches |
//! |---|---|---|
//! | `sync-noop-local`     | rowmap oracle + full ingest pass, zero new data | the no-op sync spike |
//! | `sync-incremental`    | +1 session then ingest + finalize               | peak scaling with delta vs store |
//! | `rowmap-build-cold`   | `ensure_rowmap` from an empty cache             | the #61 rowmap transient |
//! | `mcp-query-growth`    | N iterations of search + get + sql              | the per-query ratchet |
//! | `ingest-large-session`| one session, many messages (#229 shape)         | flush-batch byte scaling |
//! | `search-query-latency`| warmup, then N timed FTS searches               | query latency drift |
//! | `ingest-throughput`   | N synthetic sessions into a fresh store         | write throughput drift |
//! | `serve-sync-retention`| serve-like prewarm + sync, then a settling pause | a map or buffer pinned after sync ends |
//! | `sync-under-contention`| sync while the rowmap build lock is held       | the trailing-oracle path's cost |
//! | `rowmap-build-cold-partial-embed` | cold build after scattered embed windows | a cold build that stops streaming (`scan_fallbacks`) |
//!
//! The last five are RECORD-ONLY: they run in the gate and append rows, but
//! `gate --check` judges none of their *memory* numbers - peak RSS and peak
//! heap included - because the gate's `RECORD_ONLY` list carves them out.
//! Phase 1 gathers the spread; phase 2 derives a threshold from the committed
//! rows' median/IQR and promotes a scenario by dropping it from that list.
//! `scan_fallbacks` is exempt from all of that: it is a count, not a
//! measurement, and the gate fails on any increase, on every scenario that
//! reports it.
//!
//! Heap numbers need `--features mem-probe` (the counting allocator); without
//! it the row still carries wall time and the scenario detail, with null memory
//! fields. `--features dhat-heap` swaps in dhat instead and writes
//! `dhat-heap.json` for per-site attribution.
//!
//! The corpus is synthetic and content-addressed by (generator version,
//! profile), cached under `~/.cache/pond-bench/<key>/`. Build it once, outside
//! any measurement, with `--prepare`; a measuring run then reuses it and fails
//! rather than silently paying for a build inside the measured region.
//!
//! Run:
//!   cargo bench --bench mem_bench --features mem-probe -- --prepare --profile ci
//!   cargo bench --bench mem_bench --features mem-probe -- --scenario rowmap-build-cold
//!   cargo bench --bench gate -- --only mem   # all scenarios, one row each
//!   ops/scripts/profile-mem.sh rowmap-build-cold heaptrack

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use pond::{
    PROTOCOL_VERSION,
    adapter::{
        Adapter, AdapterYield, AdapterYieldStream, DiscoverFuture, SkipOracle, SkipReason,
        extract_self_str, is_session_fresh,
    },
    config::SearchConfig,
    embed::{DEFAULT_SORT_WINDOW, LazyEmbedder},
    handlers::{
        self, IngestEvent, IngestValidator, pond_get_message, pond_get_session, pond_search,
    },
    rowmap::rowmap_scan_fallbacks,
    sessions::{EmbeddedMessage, RowmapOracle, Store},
    sql::{self, Mode, Tables},
    substrate::{MaintenancePolicy, Table},
    wire::{
        GetEnvelope, GetMessageRequest, GetSessionRequest, Message, Part, PartKind, Provenance,
        ProviderOptions, SearchEnvelope, SearchFilters, SearchModeWire, SearchRequest, Session,
        SessionFrom, SortBy,
    },
};
use serde_json::{Value, json};

#[cfg(any(feature = "mem-probe", feature = "dhat-heap"))]
use pond::memprobe;

/// Stand-in for `pond::memprobe` in a default build, where the module (and the
/// allocator swap with it) is compiled out: every probe reports "unavailable",
/// so a feature-less run still produces a row with wall time and nulls.
#[cfg(not(any(feature = "mem-probe", feature = "dhat-heap")))]
mod memprobe {
    // Mirrors the real module's surface, so the items are `pub` without a
    // crate boundary to be reachable from.
    #![allow(unreachable_pub)]
    use std::time::Duration;

    pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

    #[derive(Clone, Copy)]
    pub struct HeapStats {
        pub live_bytes: u64,
        pub peak_bytes: u64,
        pub total_alloc_bytes: u64,
    }

    #[derive(Default, Clone, Copy)]
    pub struct RssStats {
        pub vm_rss_kb: u64,
        pub vm_hwm_kb: u64,
        pub rss_anon_kb: u64,
    }

    pub fn heap_stats() -> Option<HeapStats> {
        None
    }
    pub fn reset_heap_peak() {}
    pub fn rss() -> Option<RssStats> {
        None
    }
    pub fn reset_peak_rss() -> bool {
        false
    }
    pub fn ru_maxrss_kb() -> Option<u64> {
        None
    }

    pub struct RssSampler;

    impl RssSampler {
        pub fn start(_interval: Duration) -> Self {
            Self
        }
        pub fn finish(self) -> u64 {
            0
        }
    }
}

/// Bumped whenever the generated corpus changes shape or text. Part of the
/// cache key, so a bump earns a fresh corpus instead of a stale one.
const GENERATOR_VERSION: u32 = 1;

/// Rotating topic tokens so FTS queries have something selective to match;
/// every part otherwise carries identical text and every query hits everything.
const TOPICS: &[&str] = &[
    "tokio",
    "lance",
    "arrow",
    "datafusion",
    "rowmap",
    "embedding",
    "compaction",
    "manifest",
];

const QUERIES: &[&str] = &[
    "lance dataset",
    "rowmap payload",
    "tokio scheduler",
    "compaction manifest",
    "embedding vector",
];

const SQL_QUERIES: &[&str] = &[
    "SELECT COUNT(*) FROM messages",
    "SELECT MIN(timestamp), MAX(timestamp) FROM messages",
    "SELECT session_id, COUNT(*) AS n FROM messages GROUP BY session_id ORDER BY n DESC LIMIT 5",
    "SELECT message_id FROM messages WHERE contains_tokens(search_text, 'lance') LIMIT 10",
];

#[derive(Parser)]
#[command(about = "pond memory scenarios: one scenario per process, one JSON row per run")]
struct Args {
    /// Scenario to measure. One per invocation - see the table in the module docs.
    #[arg(long)]
    scenario: Option<String>,
    /// Corpus size class: `ci` (~100k messages) or `large` (1M+).
    #[arg(long, default_value = "ci", value_parser = ["ci", "large"])]
    profile: String,
    /// Build (or refresh) the cached corpus for `--profile` and exit. Keeps the
    /// generator's own allocations out of every measured run.
    #[arg(long)]
    prepare: bool,
    /// Iterations for `mcp-query-growth`. Defaults to the profile's value.
    #[arg(long)]
    iterations: Option<usize>,
    /// Messages for `ingest-large-session`. Defaults to the profile's value.
    #[arg(long)]
    steps: Option<usize>,
    /// Corpus cache root. Defaults to `$XDG_CACHE_HOME/pond-bench` (or `~/.cache/pond-bench`).
    #[arg(long)]
    cache_root: Option<PathBuf>,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(Clone, Copy)]
struct Profile {
    name: &'static str,
    sessions: usize,
    messages: usize,
    /// `mcp-query-growth` iterations.
    iterations: usize,
    /// `ingest-large-session` message count.
    steps: usize,
    /// `search-query-latency` untimed warmup queries, then timed ones. The
    /// warmup pays the one-off index paging the percentiles must not carry.
    latency_warmup: usize,
    latency_iterations: usize,
    /// `ingest-throughput` sessions replayed into a fresh store.
    throughput_sessions: usize,
}

impl Profile {
    fn parse(name: &str) -> Result<Self> {
        match name {
            "ci" => Ok(Self {
                name: "ci",
                sessions: 5_000,
                messages: 20,
                iterations: 20,
                steps: 20_000,
                latency_warmup: 5,
                latency_iterations: 50,
                throughput_sessions: 500,
            }),
            "large" => Ok(Self {
                name: "large",
                sessions: 20_000,
                messages: 50,
                iterations: 50,
                steps: 200_000,
                latency_warmup: 10,
                latency_iterations: 200,
                throughput_sessions: 2_000,
            }),
            other => bail!("unknown profile {other:?}; expected ci|large"),
        }
    }

    /// Content address of what the generator will produce. Anything that
    /// changes the bytes must be in here.
    fn cache_key(self) -> String {
        let seed = format!(
            "v{GENERATOR_VERSION}:{}:{}:{}",
            self.name, self.sessions, self.messages
        );
        let digest = blake3::hash(seed.as_bytes()).to_hex();
        format!("{}-{}", self.name, &digest.as_str()[..12])
    }
}

fn base_ts() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).single().expect("ts")
}

fn session_id(index: usize) -> String {
    format!("membench-{index:08}")
}

/// Store-side watermark this session will have once ingested: the timestamp of
/// its last message, in micros. The freshness gate compares against exactly
/// this, so a no-op sync skips every session.
fn last_ts_micros(messages: usize) -> i64 {
    (base_ts() + chrono::Duration::seconds(messages.saturating_sub(1) as i64)).timestamp_micros()
}

/// One session's full event set, mirroring `write_bench`'s generator so both
/// benches produce the same commit/fragment shape a real sync does.
fn session_events(index: usize, messages: usize) -> Vec<IngestEvent> {
    let session_id = session_id(index);
    let created = base_ts();
    let mut events = Vec::with_capacity(1 + messages * 2);
    events.push(IngestEvent::Session(Session {
        id: session_id.clone(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: created,
        project: extract_self_str(&Value::String("/tmp/membench".to_owned())).unwrap(),
        options: ProviderOptions::new(),
    }));
    for m in 0..messages {
        let message = Message::User {
            id: format!("{session_id}-msg-{m}"),
            session_id: session_id.clone(),
            timestamp: created + chrono::Duration::seconds(m as i64),
            options: ProviderOptions::new(),
        };
        let topic = TOPICS[(index + m) % TOPICS.len()];
        let text = format!(
            "lorem ipsum {topic} dolor sit amet mem bench payload session {index} message {m}"
        );
        let part = Part {
            session_id: session_id.clone(),
            id: format!("{session_id}-msg-{m}:0001"),
            message_id: message.id().to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: extract_self_str(&Value::String(text)),
            },
        };
        events.push(IngestEvent::Message(message));
        events.push(IngestEvent::Part(part));
    }
    events
}

/// Mirror `ingest_adapter`'s flush cadence (`handlers::ADAPTER_FLUSH_BATCH`).
const SEED_FLUSH_BATCH: usize = 100;

/// Seed through the real batched ingest path - the same `IngestValidator`
/// push/flush/finish cycle `ingest_adapter` drives in production, including
/// its flush test: after every event, on the substream count OR the byte
/// budget. Testing only at session end would miss the budget inside one large
/// session, and would drain the open session's messages on a substream-count
/// flush - a partial write production never makes, because by then the count
/// has already fired on the `Session` event that closed the previous one.
async fn ingest_batched(
    store: &Store,
    sessions: impl IntoIterator<Item = Vec<IngestEvent>>,
) -> Result<()> {
    let mut validator = IngestValidator::default();
    for (index, event) in sessions.into_iter().flatten().enumerate() {
        validator.push(store, index, event).await?;
        if validator.pending_substreams() >= SEED_FLUSH_BATCH || validator.byte_budget_reached() {
            validator.flush(store).await?;
        }
    }
    validator.finish(store).await?;
    Ok(())
}

/// The corpus replayed as a sync source. No files on disk: #245's sync
/// pathology is store-side (rowmap oracle, scan buffering, flush batching), and
/// a synthetic adapter drives the exact `ingest_adapter` pipeline `pond sync`
/// runs without a fixture tree to generate and cache alongside the store.
struct SyntheticAdapter {
    sessions: usize,
    messages: usize,
}

impl Adapter for SyntheticAdapter {
    fn discover(&self) -> DiscoverFuture<'_> {
        Box::pin(async move { Ok(self.sessions) })
    }

    fn events_with<'a>(&'a self, oracle: &'a dyn SkipOracle) -> AdapterYieldStream<'a> {
        Box::pin(async_stream::stream! {
            let mut fresh = 0usize;
            for index in 0..self.sessions {
                let id = session_id(index);
                if is_session_fresh(oracle, &id, Some(last_ts_micros(self.messages))) {
                    fresh += 1;
                    continue;
                }
                for event in session_events(index, self.messages) {
                    yield Ok(AdapterYield::Event(event));
                }
            }
            if fresh > 0 {
                yield Ok(AdapterYield::SkippedBatch { reason: SkipReason::Fresh, count: fresh });
            }
        })
    }
}

fn cache_root(args: &Args) -> Result<PathBuf> {
    if let Some(root) = &args.cache_root {
        return Ok(root.clone());
    }
    if let Some(xdg) = std::env::var_os("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(xdg).join("pond-bench"));
    }
    let home = std::env::var_os("HOME").context("neither --cache-root, XDG_CACHE_HOME nor HOME")?;
    Ok(PathBuf::from(home).join(".cache").join("pond-bench"))
}

struct Corpus {
    dir: PathBuf,
    profile: Profile,
}

impl Corpus {
    fn locate(args: &Args, profile: Profile) -> Result<Self> {
        Ok(Self {
            dir: cache_root(args)?.join(profile.cache_key()),
            profile,
        })
    }

    fn store_dir(&self) -> PathBuf {
        self.dir.join("store")
    }

    /// A corpus is usable only once the marker is written, so an interrupted
    /// build is rebuilt rather than measured.
    fn ready(&self) -> bool {
        self.dir.join("READY").exists()
    }

    async fn prepare(&self) -> Result<()> {
        if self.ready() {
            eprintln!("corpus ready: {}", self.dir.display());
            return Ok(());
        }
        if self.dir.exists() {
            std::fs::remove_dir_all(&self.dir).context("clear partial corpus")?;
        }
        std::fs::create_dir_all(self.store_dir()).context("create corpus dir")?;
        let started = Instant::now();
        eprintln!(
            "building corpus {} ({} sessions x {} messages)",
            self.dir.display(),
            self.profile.sessions,
            self.profile.messages,
        );
        let store = Store::open_local(self.store_dir()).await?;
        ingest_batched(
            &store,
            (0..self.profile.sessions).map(|i| session_events(i, self.profile.messages)),
        )
        .await?;
        // Indexes must exist before `mcp-query-growth` searches it; building
        // them here keeps index-build memory out of every measured run.
        store
            .optimize_indices(None, &MaintenancePolicy::always_compact())
            .await
            .context("optimize_indices on the fresh corpus")?;
        drop(store);
        std::fs::write(
            self.dir.join("READY"),
            json!({
                "generator_version": GENERATOR_VERSION,
                "profile": self.profile.name,
                "sessions": self.profile.sessions,
                "messages": self.profile.messages,
                "built_ms": started.elapsed().as_millis() as u64,
            })
            .to_string(),
        )?;
        eprintln!("corpus built in {} ms", started.elapsed().as_millis());
        Ok(())
    }

    fn require_ready(&self) -> Result<()> {
        if self.ready() {
            return Ok(());
        }
        bail!(
            "corpus {} is missing; build it first with `--prepare --profile {}` (a build inside a \
             measured run would pollute the row)",
            self.dir.display(),
            self.profile.name,
        )
    }

    async fn open(&self) -> Result<Store> {
        Store::open_local(self.store_dir()).await
    }

    /// Writable copy for the scenarios that mutate the store, so the cached
    /// corpus stays byte-identical across runs.
    fn copy_to_temp(&self) -> Result<tempfile::TempDir> {
        let temp = tempfile::tempdir().context("scratch store dir")?;
        copy_dir(&self.store_dir(), temp.path())?;
        Ok(temp)
    }
}

fn copy_dir(from: &Path, to: &Path) -> Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

/// Probe state around one measured region. `begin` resets both high-water marks
/// (heap peak in user space, `VmHWM` through `/proc/self/clear_refs`) so the
/// peaks belong to the scenario rather than to process startup.
struct Probe {
    started: Instant,
    start_heap_bytes: Option<u64>,
    sampler: memprobe::RssSampler,
    hwm_reset: bool,
}

impl Probe {
    fn begin() -> Self {
        memprobe::reset_heap_peak();
        let hwm_reset = memprobe::reset_peak_rss();
        Self {
            started: Instant::now(),
            start_heap_bytes: memprobe::heap_stats().map(|h| h.live_bytes),
            sampler: memprobe::RssSampler::start(memprobe::SAMPLE_INTERVAL),
            hwm_reset,
        }
    }

    fn finish(self) -> Readings {
        let wall_ms = self.started.elapsed().as_millis() as u64;
        let sampled_peak_kb = self.sampler.finish();
        let heap = memprobe::heap_stats();
        let rss = memprobe::rss();
        let ru_maxrss_kb = memprobe::ru_maxrss_kb();
        Readings {
            wall_ms,
            start_heap_bytes: self.start_heap_bytes,
            heap_peak_bytes: heap.map(|h| h.peak_bytes),
            heap_end_bytes: heap.map(|h| h.live_bytes),
            total_alloc_bytes: heap.map(|h| h.total_alloc_bytes),
            // `VmHWM` is the kernel's own watermark and was reset for this
            // region; off Linux the only peak available is the un-resettable
            // process-lifetime `ru_maxrss` (see `hwm_reset`).
            peak_rss_kb: match rss {
                Some(r) => Some(r.vm_hwm_kb.max(sampled_peak_kb)),
                None => ru_maxrss_kb,
            }
            .filter(|kb| *kb > 0),
            vm_hwm_kb: rss.map(|r| r.vm_hwm_kb),
            end_rss_kb: rss.map(|r| r.vm_rss_kb).filter(|kb| *kb > 0),
            rss_anon_end_kb: rss.map(|r| r.rss_anon_kb),
            ru_maxrss_kb,
            hwm_reset: self.hwm_reset,
        }
    }
}

struct Readings {
    wall_ms: u64,
    start_heap_bytes: Option<u64>,
    heap_peak_bytes: Option<u64>,
    heap_end_bytes: Option<u64>,
    total_alloc_bytes: Option<u64>,
    peak_rss_kb: Option<u64>,
    vm_hwm_kb: Option<u64>,
    end_rss_kb: Option<u64>,
    rss_anon_end_kb: Option<u64>,
    ru_maxrss_kb: Option<u64>,
    /// False when `VmHWM` could not be reset (non-Linux): the peak then carries
    /// process startup in it and is only comparable to other such rows.
    hwm_reset: bool,
}

/// Read a scenario's probe once the scenario has returned and its locals have
/// dropped - the point every committed baseline row was measured at.
fn close((probe, detail): (Probe, Value)) -> (Readings, Value) {
    (probe.finish(), detail)
}

/// Least-squares slope over `values`, in units per index step. Zero for fewer
/// than two points.
fn slope_per_iter(values: &[f64]) -> f64 {
    let n = values.len();
    if n < 2 {
        return 0.0;
    }
    let mean_x = (n as f64 - 1.0) / 2.0;
    let mean_y = values.iter().sum::<f64>() / n as f64;
    let mut num = 0.0;
    let mut den = 0.0;
    for (i, y) in values.iter().enumerate() {
        let dx = i as f64 - mean_x;
        num += dx * (y - mean_y);
        den += dx * dx;
    }
    if den == 0.0 { 0.0 } else { num / den }
}

/// Retained bytes right now: the counting allocator's live figure when built
/// with `mem-probe`, else resident-set bytes (which includes allocator
/// retention, so the slope still shows a ratchet).
fn retained_bytes() -> Option<f64> {
    if let Some(heap) = memprobe::heap_stats() {
        return Some(heap.live_bytes as f64);
    }
    memprobe::rss().map(|r| (r.vm_rss_kb * 1024) as f64)
}

async fn scenario_sync_noop(corpus: &Corpus) -> Result<(Probe, Value)> {
    let store = corpus.open().await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    let adapter = SyntheticAdapter {
        sessions: corpus.profile.sessions,
        messages: corpus.profile.messages,
    };
    let probe = Probe::begin();
    // The oracle build is part of the measured region on purpose: `pond sync`
    // pays for it before reading a single source, and the full rowmap rebuild
    // is the prime suspect behind the 3.8 GB no-op spike (plan, Phase 0).
    store.ensure_rowmap(cache.path()).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    let summary =
        handlers::ingest_adapter(&store, &adapter, &oracle as &dyn SkipOracle, |_| {}).await?;
    // A no-op sync writes nothing; anything written means the freshness gate
    // missed and the shared corpus now holds rows the generator did not put
    // there, so say so instead of measuring a tainted store forever after.
    if summary.inserted > 0 {
        bail!(
            "sync-noop-local inserted {} rows - the cached corpus at {} is no longer pristine; \
             rebuild it (rm -rf that dir, then --prepare)",
            summary.inserted,
            corpus.dir.display(),
        );
    }
    Ok((
        probe,
        json!({
            "sessions": corpus.profile.sessions,
            "messages_per_session": corpus.profile.messages,
            "oracle_present": oracle.0.is_some(),
            "inserted": summary.inserted,
            "matched": summary.matched,
        }),
    ))
}

async fn scenario_sync_incremental(corpus: &Corpus) -> Result<(Probe, Value)> {
    let scratch = corpus.copy_to_temp()?;
    let store = Store::open_local(scratch.path()).await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    // One session beyond the corpus: every existing session is fresh, so the
    // work is delta-sized while the store is not.
    let adapter = SyntheticAdapter {
        sessions: corpus.profile.sessions + 1,
        messages: corpus.profile.messages,
    };
    let probe = Probe::begin();
    store.ensure_rowmap(cache.path()).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    let summary =
        handlers::ingest_adapter(&store, &adapter, &oracle as &dyn SkipOracle, |_| {}).await?;
    let policy = MaintenancePolicy {
        compaction_fragment_cap: 0,
        cleanup_older_than: chrono::Duration::days(1),
        cleanup_interval: 1,
        scalar_fold_row_threshold: 0,
        index_fold_row_threshold: 0,
    };
    store.optimize_indices(None, &policy).await?;
    Ok((
        probe,
        json!({
            "sessions": corpus.profile.sessions + 1,
            "messages_per_session": corpus.profile.messages,
            "inserted": summary.inserted,
            "matched": summary.matched,
        }),
    ))
}

async fn scenario_rowmap_build_cold(corpus: &Corpus) -> Result<(Probe, Value)> {
    let store = corpus.open().await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    let probe = Probe::begin();
    store.ensure_rowmap(cache.path()).await?;
    let entries = store.rowmap_snapshot().map_or(0, |set| set.len());
    Ok((
        probe,
        json!({
            "rowmap_entries": entries,
            "sessions": corpus.profile.sessions,
        }),
    ))
}

async fn scenario_mcp_query_growth(corpus: &Corpus, iterations: usize) -> Result<(Probe, Value)> {
    let store = corpus.open().await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    // `pond mcp` builds the rowmap at startup; the ratchet under measurement is
    // per-query, so the build happens before the measured region.
    store.ensure_rowmap(cache.path()).await?;
    let embedder = LazyEmbedder::candle();
    let search_cfg = SearchConfig::default();
    let sessions = corpus.profile.sessions;
    let messages = corpus.profile.messages;

    let probe = Probe::begin();
    let mut retained: Vec<f64> = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let request = SearchRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            query: QUERIES[i % QUERIES.len()].to_owned(),
            mode: SearchModeWire::Fts,
            sort_by: SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 20,
        };
        match pond_search(&store, &embedder, request, &search_cfg).await {
            SearchEnvelope::Success(_) => {}
            SearchEnvelope::Error(error) => bail!("search failed: {error:?}"),
        }

        let sid = session_id(i % sessions);
        let session_request = GetSessionRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: sid.clone(),
            limit: 50,
            from: SessionFrom::Start,
            after_message_id: None,
            before_message_id: None,
        };
        if let GetEnvelope::Error(error) = pond_get_session(&store, session_request).await {
            bail!("get_session failed: {error:?}");
        }

        let message_request = GetMessageRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            id: format!("{sid}-msg-{}", i % messages),
            context_before: 3,
            context_after: 3,
        };
        if let GetEnvelope::Error(error) = pond_get_message(&store, message_request).await {
            bail!("get_message failed: {error:?}");
        }

        // Mirror the MCP tool (transport.rs `pond_sql`): `Tables` is rebuilt per
        // call (the dataset freshness gates), only the tables the query names
        // are opened, and the query runs read-only on a fresh SessionContext.
        // Opening all three would charge the row for `parts.lance` retention the
        // real path never pays.
        let query = SQL_QUERIES[i % SQL_QUERIES.len()];
        let tables = Tables {
            sessions: match sql::mentions_table(query, "sessions") {
                true => Some(store.dataset(Table::Sessions).await?),
                false => None,
            },
            messages: match sql::mentions_table(query, "messages") {
                true => Some(store.dataset(Table::Messages).await?),
                false => None,
            },
            parts: match sql::mentions_table(query, "parts") {
                true => Some(store.dataset(Table::Parts).await?),
                false => None,
            },
        };
        sql::run(&tables, query, Mode::Inline, sql::DEFAULT_INLINE_ROWS, None)
            .await
            .map_err(|error| anyhow::anyhow!("sql {query:?} failed: {error:?}"))?;

        if let Some(bytes) = retained_bytes() {
            retained.push(bytes);
        }
    }

    // The first iterations pay one-off warmup (index pages, DataFusion
    // metadata); the ratchet is what the steady state keeps adding.
    let warmup = (iterations / 4).max(1).min(retained.len());
    let steady = &retained[warmup..];
    Ok((
        probe,
        json!({
            "iterations": iterations,
            "warmup_iterations": warmup,
            "retained_series_bytes": retained.iter().map(|v| *v as u64).collect::<Vec<_>>(),
            "growth_slope_bytes_per_iter": slope_per_iter(steady).round() as i64,
        }),
    ))
}

async fn scenario_ingest_large_session(steps: usize) -> Result<(Readings, Value)> {
    let temp = tempfile::tempdir().context("scratch store dir")?;
    let store = Store::open_local(temp.path()).await?;
    // One session with `steps` messages: the substream-count flush never fires
    // inside it (#229), so only the byte budget bounds the buffer here.
    // Generated before the probe starts, like the cached corpus is: `steps` is
    // up to 200k, and counting the generator's own allocations would swamp the
    // ingest peak this row exists to measure.
    let events = session_events(0, steps);
    let probe = Probe::begin();
    ingest_batched(&store, std::iter::once(events)).await?;
    let (_, messages, _) = store.row_counts().await?;
    let mut detail = json!({ "steps": steps, "messages_written": messages });
    // Everything below is post-measurement. The store is dropped and the probe
    // read at exactly the point they were before fragment accounting existed,
    // so this row's peak and end numbers stay comparable to the committed
    // baseline; only the scratch dir's removal moved after the read (an rmdir,
    // which allocates nothing that survives it).
    drop(store);
    let readings = probe.finish();

    // Best effort: the measurement is already in hand, so a manifest that will
    // not reopen costs the row its fragment fields (they stay null, like on
    // every scenario that never produces them) rather than the whole row.
    if let Value::Object(map) = &mut detail {
        match fragment_stats(temp.path()).await {
            Ok(fragments) => {
                map.insert("frag_count".to_owned(), fragments.total_count.into());
                map.insert("data_file_bytes".to_owned(), fragments.total_bytes.into());
                map.insert("fragments".to_owned(), fragments.per_table);
            }
            Err(error) => {
                map.insert("fragments_error".to_owned(), error.to_string().into());
            }
        }
    }
    Ok((readings, detail))
}

/// Fragment shape of one ingest, read from the manifests after the measured
/// region closes: a table's fragment count and the data-file bytes those
/// fragments claim. Turns "did this change rewrite the write path into many
/// small files" into a number the baseline carries.
struct FragmentStats {
    total_count: u64,
    total_bytes: u64,
    per_table: Value,
}

async fn fragment_stats(store_dir: &Path) -> Result<FragmentStats> {
    let store = Store::open_local(store_dir).await?;
    let mut per_table = serde_json::Map::new();
    let mut total_count = 0u64;
    let mut total_bytes = 0u64;
    for table in [Table::Messages, Table::Parts] {
        let dataset = store.dataset(table).await?;
        let (mut count, mut bytes, mut rows) = (0u64, 0u64, 0u64);
        for fragment in dataset.get_fragments() {
            let meta = fragment.metadata();
            count += 1;
            rows += meta.physical_rows.unwrap_or(0) as u64;
            // A fragment whose files do not all carry sizes contributes nothing
            // rather than a wrong total; `frag_count` still says what the shape
            // is.
            bytes += meta
                .files
                .iter()
                .try_fold(0u64, |total, file| {
                    Some(total + file.file_size_bytes.get()?.get())
                })
                .unwrap_or(0);
        }
        per_table.insert(
            table.as_str().to_owned(),
            json!({ "frag_count": count, "data_file_bytes": bytes, "rows": rows }),
        );
        total_count += count;
        total_bytes += bytes;
    }
    Ok(FragmentStats {
        total_count,
        total_bytes,
        per_table: Value::Object(per_table),
    })
}

/// Nearest-rank percentile over an ascending slice. Empty input is 0.
fn percentile_ms(sorted_us: &[u64], q: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let rank = ((q * sorted_us.len() as f64).ceil() as usize).clamp(1, sorted_us.len());
    sorted_us[rank - 1] as f64 / 1000.0
}

/// Record-only query latency: `warmup` untimed searches to page the index in,
/// then `iterations` timed ones over the cached corpus. Phase 1 of the latency
/// lane - the row carries p50/p95/max so a threshold can be derived from the
/// accumulated spread later, and nothing fails on these numbers today.
async fn scenario_search_query_latency(
    corpus: &Corpus,
    warmup: usize,
    iterations: usize,
) -> Result<(Probe, Value)> {
    let store = corpus.open().await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    // Same staging as `mcp-query-growth`: a server has its rowmap resident
    // before it answers anything, so the build is not part of a query's cost.
    store.ensure_rowmap(cache.path()).await?;
    let embedder = LazyEmbedder::candle();
    let search_cfg = SearchConfig::default();

    let search = async |query: &str| -> Result<()> {
        let request = SearchRequest {
            protocol_version: PROTOCOL_VERSION,
            namespace: Some("local".to_owned()),
            query: query.to_owned(),
            mode: SearchModeWire::Fts,
            sort_by: SortBy::Relevance,
            filters: SearchFilters::default(),
            limit: 20,
        };
        match pond_search(&store, &embedder, request, &search_cfg).await {
            SearchEnvelope::Success(_) => Ok(()),
            SearchEnvelope::Error(error) => bail!("search failed: {error:?}"),
        }
    };

    for i in 0..warmup {
        search(QUERIES[i % QUERIES.len()]).await?;
    }

    let probe = Probe::begin();
    let mut samples_us: Vec<u64> = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let started = Instant::now();
        search(QUERIES[i % QUERIES.len()]).await?;
        samples_us.push(started.elapsed().as_micros() as u64);
    }
    let mut sorted = samples_us.clone();
    sorted.sort_unstable();
    Ok((
        probe,
        json!({
            "iterations": iterations,
            "warmup_iterations": warmup,
            "latency_p50_ms": percentile_ms(&sorted, 0.50),
            "latency_p95_ms": percentile_ms(&sorted, 0.95),
            "latency_max_ms": percentile_ms(&sorted, 1.0),
            "samples_us": samples_us,
        }),
    ))
}

/// Record-only ingest throughput: the profile's first N synthetic sessions -
/// the same ones the corpus generator emits, regenerated rather than read back,
/// so this scenario needs no prepared corpus - replayed into a fresh store
/// through the production batched path. Events are built before the probe
/// starts, like every other ingest scenario, and the rate's denominator is the
/// ingest call alone - not the closing `row_counts()`, which runs inside the
/// measured region. The generator's buffer is still resident when the probe
/// begins and drains only as the ingest consumes it, so it floors this row's
/// peaks; `start_heap_bytes` records that floor.
async fn scenario_ingest_throughput(profile: Profile) -> Result<(Readings, Value)> {
    let (sessions, messages) = (profile.throughput_sessions, profile.messages);
    let corpus_sessions: Vec<Vec<IngestEvent>> = (0..sessions)
        .map(|index| session_events(index, messages))
        .collect();
    let temp = tempfile::tempdir().context("scratch store dir")?;
    let store = Store::open_local(temp.path()).await?;

    let probe = Probe::begin();
    let started = Instant::now();
    ingest_batched(&store, corpus_sessions).await?;
    let ingest = started.elapsed();
    let (sessions_written, messages_written, parts_written) = store.row_counts().await?;
    let readings = probe.finish();

    let rows = (sessions_written + messages_written + parts_written) as f64;
    let seconds = ingest.as_secs_f64().max(f64::EPSILON);
    Ok((
        readings,
        json!({
            "sessions": sessions,
            "messages_per_session": messages,
            "sessions_written": sessions_written,
            "messages_written": messages_written,
            "parts_written": parts_written,
            // The denominator, on the record: `wall_ms` also carries the row
            // count, so the rate cannot be re-derived from the row without it.
            "ingest_ms": ingest.as_millis() as u64,
            "throughput_rows_per_s": (rows / seconds).round() as u64,
            "throughput_messages_per_s": (messages_written as f64 / seconds).round() as u64,
        }),
    ))
}

/// How long the retention scenario waits for background work to quiesce before
/// reading the end-state numbers. Long enough for a flushed writer's tasks to
/// finish, short enough not to dominate the row's wall time.
const RETENTION_SETTLE: Duration = Duration::from_millis(500);

/// Record-only retention floor: a serve-like process prewarms, runs one sync to
/// completion, settles, and is then measured while still alive. The primary
/// metrics are the row's END fields (`end_rss_kb`, `rss_anon_end_kb`,
/// `end_heap_bytes`) - a future change that leaves a map or scan buffer pinned
/// after sync returns (the `sync_oracle_map` class of bug) shows up there even
/// though the peak is unchanged.
async fn scenario_serve_sync_retention(corpus: &Corpus) -> Result<(Readings, Value)> {
    let scratch = corpus.copy_to_temp()?;
    let store = Store::open_local(scratch.path()).await?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    let adapter = SyntheticAdapter {
        sessions: corpus.profile.sessions,
        messages: corpus.profile.messages,
    };

    let probe = Probe::begin();
    // `serve` prewarms before it serves anything, and `--with-sync` then syncs
    // in the same process - so the floor this row measures is what that whole
    // startup leaves behind, not what a one-shot `pond sync` exits with.
    store.prewarm(cache.path()).await?;
    let oracle = store.sync_rowmap_oracle(cache.path()).await?;
    let summary =
        handlers::ingest_adapter(&store, &adapter, &oracle as &dyn SkipOracle, |_| {}).await?;
    if summary.inserted > 0 {
        bail!(
            "serve-sync-retention inserted {} rows - the cached corpus at {} is no longer \
             pristine; rebuild it (rm -rf that dir, then --prepare)",
            summary.inserted,
            corpus.dir.display(),
        );
    }
    let oracle_entries = oracle.0.as_ref().map_or(0, |set| set.len());
    // Everything the sync itself owned goes away here; what the STORE still
    // holds is the measurement.
    drop(oracle);
    // Sampled either side of the pause, because the pause is a fixed duration
    // rather than a convergence test: a delta at zero is the evidence it was
    // long enough, and a growing one is how a change that keeps allocating
    // after `sync` returns announces itself instead of hiding in the noise.
    let settle_start_heap = memprobe::heap_stats().map(|h| h.live_bytes);
    tokio::time::sleep(RETENTION_SETTLE).await;
    let readings = probe.finish();
    let settle_heap_delta_bytes = settle_start_heap
        .zip(readings.heap_end_bytes)
        .map(|(before, after)| after as i64 - before as i64);

    Ok((
        readings,
        json!({
            "sessions": corpus.profile.sessions,
            "messages_per_session": corpus.profile.messages,
            "inserted": summary.inserted,
            "matched": summary.matched,
            "oracle_entries": oracle_entries,
            // The map the planner parked on the store. Retained by design
            // today (it seeds the sync cursor); the row makes its cost visible.
            "sync_oracle_retained": store.sync_oracle_snapshot().is_some(),
            "rowmap_entries": store.rowmap_snapshot().map_or(0, |set| set.len()),
            "settle_ms": RETENTION_SETTLE.as_millis() as u64,
            "settle_heap_delta_bytes": settle_heap_delta_bytes,
        }),
    ))
}

/// Every Nth fragment gets an embed window in the partial-embed scenario, so
/// the rewritten fragments are spread across the whole store.
const PARTIAL_EMBED_FRAGMENT_STRIDE: usize = 5;

/// Rows per `merge_update` - the size `EmbedWorker::drain_window` actually
/// writes in, taken from the constant itself so it cannot drift.
const PARTIAL_EMBED_WINDOW: usize = DEFAULT_SORT_WINDOW;

/// Record-only cold build over a partially embedded store: the shape that took
/// the unordered fallback before #260. An embed pass writes one
/// `merge_update` per window (`embed.rs` `drain_window`), and each rewrite
/// appends a fragment holding *lower* row ids than the fragments after it. So
/// the setup embeds a window at the head of every Nth fragment, each as its own
/// `merge_update`, then the measured region is a cold `ensure_rowmap`.
///
/// The cached corpus is compacted into one `messages` fragment, where only the
/// ends can be rewritten without splitting its live ids, so the setup ingests
/// the profile's sessions into a fresh store instead: one fragment per
/// production flush, the layout a store has before its first compaction.
///
/// `scan_fallbacks` is the row's assertion, and it is the gate that judges it
/// (`gate --check` fails on any increase over the committed row) because
/// whether this shape is plannable at all depends on the store's size:
/// `merge_update` returns the rewritten rows in row-id order on a small store
/// but not on a large one (measured on this corpus: ordered at 120k rows,
/// scrambled at 160k), and a fragment whose own live ids do not ascend has no
/// plan by construction. So the ci row records a streamed build and the large
/// row records the fallback, and either one moving the wrong way fails.
async fn scenario_rowmap_build_cold_partial_embed(profile: Profile) -> Result<(Probe, Value)> {
    let scratch = tempfile::tempdir().context("scratch store dir")?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    let messages = profile.messages;
    let (windows, embedded_rows) = {
        let store = Store::open_local(scratch.path()).await?;
        ingest_batched(
            &store,
            (0..profile.sessions).map(|index| session_events(index, messages)),
        )
        .await?;
        // The fresh store is append-only in session order, so fragment order is
        // row order and row `r` is message `r % messages` of session
        // `r / messages`.
        let fragment_rows: Vec<usize> = store
            .dataset(Table::Messages)
            .await?
            .get_fragments()
            .iter()
            .map(|fragment| fragment.metadata().physical_rows.unwrap_or(0))
            .collect();
        let dim = pond::sessions::embedding_dim();
        let (mut windows, mut embedded_rows, mut first_row) = (0usize, 0usize, 0usize);
        for (ordinal, rows) in fragment_rows.iter().enumerate() {
            if ordinal % PARTIAL_EMBED_FRAGMENT_STRIDE == 0 && *rows > 1 {
                let span = PARTIAL_EMBED_WINDOW.min(*rows);
                let window: Vec<EmbeddedMessage> = (first_row..first_row + span)
                    .map(|row| {
                        let session = session_id(row / messages);
                        EmbeddedMessage {
                            id: format!("{session}-msg-{}", row % messages),
                            session_id: session,
                            vector: vec![(row % 97) as f32 / 97.0; dim],
                        }
                    })
                    .collect();
                store.write_embeddings(&window).await?;
                windows += 1;
                embedded_rows += window.len();
            }
            first_row += rows;
        }
        if windows < 2 {
            bail!(
                "the fresh store embedded {windows} of its {} message fragments; the partial-embed \
                 shape needs at least two windows, so at least {} fragments of more than one row \
                 each at stride {PARTIAL_EMBED_FRAGMENT_STRIDE}",
                fragment_rows.len(),
                PARTIAL_EMBED_FRAGMENT_STRIDE + 1,
            );
        }
        (windows, embedded_rows)
    };

    // A fresh handle, so the build reads the post-embed manifest cold.
    let store = Store::open_local(scratch.path()).await?;
    let fragments = store.dataset(Table::Messages).await?.get_fragments().len();
    let (_, messages_in_store, _) = store.row_counts().await?;
    let fallbacks_before = rowmap_scan_fallbacks();
    let probe = Probe::begin();
    store.ensure_rowmap(cache.path()).await?;
    let fallbacks = rowmap_scan_fallbacks() - fallbacks_before;
    // Whichever path ran, the map it published has to describe the whole store.
    let entries = store.rowmap_snapshot().map_or(0, |set| set.len());
    if entries != messages_in_store {
        bail!("rowmap holds {entries} entries for {messages_in_store} messages");
    }
    Ok((
        probe,
        json!({
            "rowmap_entries": entries,
            "sessions": profile.sessions,
            "messages_per_session": messages,
            "embed_windows": windows,
            "embedded_rows": embedded_rows,
            "message_fragments": fragments,
            "scan_fallbacks": fallbacks,
        }),
    ))
}

/// Hold the rowmap build lock the way a concurrent builder would: the same
/// `flock` on the same path `Store::extend_rowmap_coordinated` takes. `flock`
/// is per open file description, so this conflicts with a build in this very
/// process - no second process, no sleep, no race.
fn hold_rowmap_build_lock(store: &Store, cache_dir: &Path) -> Result<std::fs::File> {
    let path = cache_dir.join(format!("rowmetamap-{}.lock", store.store_key()));
    let lock = std::fs::File::create(&path)
        .with_context(|| format!("create rowmap build lock {}", path.display()))?;
    lock.try_lock().context("hold rowmap build lock")?;
    Ok(lock)
}

/// Sync while a sibling owns the rowmap build: the #251 composition, measured.
/// The chain on disk trails the store by one session, the build lock is held,
/// so `sync_rowmap_oracle` must fall back to the trailing map and the sync must
/// still complete against it. Asserts the fallback actually happened - a future
/// change that silently turns this into a full re-read would otherwise just
/// look like a slower row.
async fn scenario_sync_under_contention(corpus: &Corpus) -> Result<(Probe, Value)> {
    let scratch = corpus.copy_to_temp()?;
    let cache = tempfile::tempdir().context("scratch rowmap cache")?;
    let extra_index = corpus.profile.sessions;
    {
        let builder = Store::open_local(scratch.path()).await?;
        builder.ensure_rowmap(cache.path()).await?;
        // One session past the chain, so the published chain no longer covers
        // the store's current version and the sync has to build (and lose).
        ingest_batched(
            &builder,
            std::iter::once(session_events(extra_index, corpus.profile.messages)),
        )
        .await?;
    }

    let store = Store::open_local(scratch.path()).await?;
    let _lock = hold_rowmap_build_lock(&store, cache.path())?;
    let adapter = SyntheticAdapter {
        sessions: corpus.profile.sessions + 1,
        messages: corpus.profile.messages,
    };

    let probe = Probe::begin();
    let oracle = store.sync_rowmap_oracle(cache.path()).await?;
    if oracle.is_empty() {
        bail!("contended sync fell back to an empty oracle; the trailing map was not usable");
    }
    if store.rowmap_snapshot().is_some() {
        bail!("the rowmap build won the lock; this scenario did not measure contention");
    }
    let summary =
        handlers::ingest_adapter(&store, &adapter, &oracle as &dyn SkipOracle, |_| {}).await?;
    if summary.inserted > 0 {
        bail!(
            "sync-under-contention inserted {} rows - every session it re-read was already in \
             the store, so the trailing oracle mis-planned",
            summary.inserted,
        );
    }
    Ok((
        probe,
        json!({
            "sessions": corpus.profile.sessions + 1,
            "messages_per_session": corpus.profile.messages,
            "inserted": summary.inserted,
            "matched": summary.matched,
            "oracle_entries": oracle.0.as_ref().map_or(0, |set| set.len()),
            "trailing_oracle": true,
            "sync_oracle_retained": store.sync_oracle_snapshot().is_some(),
        }),
    ))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _dhat = pond::memprobe::start_dhat_profiler();

    let args = Args::parse();
    let profile = Profile::parse(&args.profile)?;
    let corpus = Corpus::locate(&args, profile)?;

    if args.prepare {
        corpus.prepare().await?;
        return Ok(());
    }

    let Some(scenario) = args.scenario.clone() else {
        bail!("pass --scenario <name> (or --prepare to build the corpus)");
    };

    let iterations = args.iterations.unwrap_or(profile.iterations);
    let steps = args.steps.unwrap_or(profile.steps);
    // A scenario that reads its own probe (one needing work AFTER the measured
    // region, like fragment accounting) returns `Readings` directly; every
    // other one hands back a live `Probe` that is read here - after the
    // scenario's own locals have dropped, exactly where it always was.
    let (readings, detail) = match scenario.as_str() {
        "sync-noop-local" => {
            corpus.require_ready()?;
            close(scenario_sync_noop(&corpus).await?)
        }
        "sync-incremental" => {
            corpus.require_ready()?;
            close(scenario_sync_incremental(&corpus).await?)
        }
        "rowmap-build-cold" => {
            corpus.require_ready()?;
            close(scenario_rowmap_build_cold(&corpus).await?)
        }
        "mcp-query-growth" => {
            corpus.require_ready()?;
            close(scenario_mcp_query_growth(&corpus, iterations).await?)
        }
        "ingest-large-session" => scenario_ingest_large_session(steps).await?,
        "search-query-latency" => {
            corpus.require_ready()?;
            close(
                scenario_search_query_latency(
                    &corpus,
                    profile.latency_warmup,
                    profile.latency_iterations,
                )
                .await?,
            )
        }
        // No `require_ready`: this one regenerates its sessions rather than
        // reading the cached corpus back.
        "ingest-throughput" => scenario_ingest_throughput(profile).await?,
        "serve-sync-retention" => {
            corpus.require_ready()?;
            scenario_serve_sync_retention(&corpus).await?
        }
        "sync-under-contention" => {
            corpus.require_ready()?;
            close(scenario_sync_under_contention(&corpus).await?)
        }
        // No `require_ready`: this one builds its own uncompacted store.
        "rowmap-build-cold-partial-embed" => {
            close(scenario_rowmap_build_cold_partial_embed(profile).await?)
        }
        other => bail!(
            "unknown scenario {other:?}; expected sync-noop-local|sync-incremental|\
             rowmap-build-cold|mcp-query-growth|ingest-large-session|search-query-latency|\
             ingest-throughput|serve-sync-retention|sync-under-contention|\
             rowmap-build-cold-partial-embed"
        ),
    };

    let row = json!({
        "scenario": scenario,
        "profile": profile.name,
        "wall_ms": readings.wall_ms,
        "start_heap_bytes": readings.start_heap_bytes,
        "peak_heap_bytes": readings.heap_peak_bytes,
        "end_heap_bytes": readings.heap_end_bytes,
        "total_alloc_bytes": readings.total_alloc_bytes,
        "peak_rss_kb": readings.peak_rss_kb,
        "vm_hwm_kb": readings.vm_hwm_kb,
        "end_rss_kb": readings.end_rss_kb,
        "rss_anon_end_kb": readings.rss_anon_end_kb,
        "ru_maxrss_kb": readings.ru_maxrss_kb,
        "hwm_reset": readings.hwm_reset,
        // Promoted out of `detail` so a row reads without digging, and null on
        // the scenarios that do not produce them - additive, so every committed
        // row from before these scenarios existed still parses unchanged.
        "growth_slope_bytes_per_iter": detail.get("growth_slope_bytes_per_iter").cloned(),
        "latency_p50_ms": detail.get("latency_p50_ms").cloned(),
        "latency_p95_ms": detail.get("latency_p95_ms").cloned(),
        "latency_max_ms": detail.get("latency_max_ms").cloned(),
        "throughput_rows_per_s": detail.get("throughput_rows_per_s").cloned(),
        // Judged by `gate --check` on every scenario that reports it,
        // record-only ones included: a cold build that stops streaming is a
        // cliff, not a drift.
        "scan_fallbacks": detail.get("scan_fallbacks").cloned(),
        "frag_count": detail.get("frag_count").cloned(),
        "data_file_bytes": detail.get("data_file_bytes").cloned(),
        "detail": detail,
    });
    println!("{row}");
    Ok(())
}
