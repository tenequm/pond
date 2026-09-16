#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Memory scenarios for #245 - the harness behind `ops/scripts/mem-gate.sh`.
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
//!   ops/scripts/mem-gate.sh                # all scenarios, one row each
//!   ops/scripts/profile-mem.sh rowmap-build-cold heaptrack

use std::path::{Path, PathBuf};
use std::time::Instant;

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
    embed::LazyEmbedder,
    handlers::{
        self, IngestEvent, IngestValidator, pond_get_message, pond_get_session, pond_search,
    },
    sessions::{RowmapOracle, Store},
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
            }),
            "large" => Ok(Self {
                name: "large",
                sessions: 20_000,
                messages: 50,
                iterations: 50,
                steps: 200_000,
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
/// push/flush/finish cycle `ingest_adapter` drives in production.
async fn ingest_batched(
    store: &Store,
    sessions: impl IntoIterator<Item = Vec<IngestEvent>>,
) -> Result<()> {
    let mut validator = IngestValidator::default();
    let mut index = 0usize;
    for events in sessions {
        for event in events {
            validator.push(store, index, event).await?;
            index += 1;
        }
        if validator.pending_substreams() >= SEED_FLUSH_BATCH {
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

async fn scenario_ingest_large_session(steps: usize) -> Result<(Probe, Value)> {
    let temp = tempfile::tempdir().context("scratch store dir")?;
    let store = Store::open_local(temp.path()).await?;
    // One session with `steps` messages: the flush batch is bounded by session
    // count, not bytes (#229), so a single huge session is the worst shape.
    // Generated before the probe starts, like the cached corpus is: `steps` is
    // up to 200k, and counting the generator's own allocations would swamp the
    // ingest peak this row exists to measure.
    let events = session_events(0, steps);
    let probe = Probe::begin();
    ingest_batched(&store, std::iter::once(events)).await?;
    let (_, messages, _) = store.row_counts().await?;
    Ok((
        probe,
        json!({ "steps": steps, "messages_written": messages }),
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
    let (probe, detail) = match scenario.as_str() {
        "sync-noop-local" => {
            corpus.require_ready()?;
            scenario_sync_noop(&corpus).await?
        }
        "sync-incremental" => {
            corpus.require_ready()?;
            scenario_sync_incremental(&corpus).await?
        }
        "rowmap-build-cold" => {
            corpus.require_ready()?;
            scenario_rowmap_build_cold(&corpus).await?
        }
        "mcp-query-growth" => {
            corpus.require_ready()?;
            scenario_mcp_query_growth(&corpus, iterations).await?
        }
        "ingest-large-session" => scenario_ingest_large_session(steps).await?,
        other => bail!(
            "unknown scenario {other:?}; expected sync-noop-local|sync-incremental|\
             rowmap-build-cold|mcp-query-growth|ingest-large-session"
        ),
    };
    let readings = probe.finish();

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
        "growth_slope_bytes_per_iter": detail.get("growth_slope_bytes_per_iter").cloned(),
        "detail": detail,
    });
    println!("{row}");
    Ok(())
}
