#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Read-path latency as a function of the unindexed tail - the number that
//! settles the FTS/vector fold-batching threshold (`DEFAULT_SYNC_INDEX_FOLD_ROWS`).
//!
//! pond's retrievers use `fast_search` (index-only) when an index has no
//! unindexed tail, and drop it when a tail exists so Lance index-probes the
//! folded rows and flat-scans the tail (complete recall while the fold is
//! deferred). Batching the fold trades a cheaper sync (fold every ~N syncs) for
//! a slower search *while a tail exists* (the flat-scan). This bench measures
//! exactly that slowdown: it builds a small indexed base, folds it (tail = 0),
//! then grows the unindexed tail in steps and times FTS / vector / hybrid
//! queries at each tail size. The flat-scan touches only the tail fragments, so
//! the per-tail delta is independent of corpus size - a small base over a real
//! object store reflects the production S3 round-trip cost.
//!
//! Run (fresh scratch store on the object store you want to characterize):
//!   cargo bench --bench read_bench -- --url s3+https://host/bucket/read-bench
//!   cargo bench --bench read_bench -- --url ... --tail-steps 1000,5000,10000 --queries 20
//!   cargo bench --bench read_bench --                       # local temp store

use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use pond::{
    PROTOCOL_VERSION,
    config::{self, Config, SearchConfig},
    embed::{EmbedWorker, Embedder, LazyEmbedder},
    handlers::pond_search,
    sessions::{MessageWrite, Store, embedding_dim},
    substrate::{MaintenancePolicy, RuntimeCaps, StorageUrl},
    wire::{
        Message, Part, PartKind, Provenance, ProviderOptions, SearchEnvelope, SearchFilters,
        SearchModeWire, SearchRequest, Session,
    },
};
use tempfile::TempDir;

#[derive(Parser)]
#[command(about = "pond read-path benchmark: search latency vs unindexed tail size")]
struct Args {
    /// Store URL (spec.md#storage-url-grammar); creds resolve from the user
    /// config's [creds.*] sets like any pond command. Omit for a local temp store.
    #[arg(long, value_name = "URL")]
    url: Option<String>,
    /// Base corpus: a few large sessions for the indexed part. Few sessions =
    /// few commits (remote cost is ~1s/commit); the tail-scan delta this bench
    /// isolates does not depend on base size, so a small base is deliberate.
    #[arg(long, default_value_t = 4)]
    base_sessions: usize,
    #[arg(long, default_value_t = 500)]
    base_messages: usize,
    /// Cumulative unindexed-tail sizes to measure, in rows.
    #[arg(long, default_value = "1000,5000,10000", value_delimiter = ',')]
    tail_steps: Vec<usize>,
    /// Query iterations per mode per tail size (latency percentiles).
    #[arg(long, default_value_t = 20)]
    queries: usize,
    #[arg(long, hide = true)]
    bench: bool,
}

struct FakeBackend;
impl Embedder for FakeBackend {
    fn device(&self) -> &str {
        "fake"
    }
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        Ok(texts.iter().map(|t| pseudo_vector(t)).collect())
    }
}

fn pseudo_vector(text: &str) -> Vec<f32> {
    let mut state = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (0..embedding_dim())
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            #[allow(clippy::cast_precision_loss)]
            let unit = (state >> 33) as f32 / (1u64 << 31) as f32;
            unit - 1.0
        })
        .collect()
}

const SAMPLE_TEXTS: &[&str] = &[
    "vector search latency on s3 backed lance datasets",
    "ingest pipeline embedding parallelism throughput",
    "object store conditional put optimistic concurrency",
    "metal accelerated transformer inference on mac",
    "manifest commit handler dynamodb external store",
    "hybrid full text plus dense retrieval reciprocal rank",
    "ivf pq nprobes refine factor recall tradeoff",
    "fragment compaction cleanup retention policy",
];

fn make_session(i: usize) -> Session {
    Session {
        id: format!("01HXYREAD{i:010}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": format!("/tmp/p/{i}")}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

fn message_for(session: &Session, idx: usize) -> Message {
    Message::User {
        id: format!("{}:msg-{idx:07}", session.id),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    }
}

fn part_for(msg: &Message, idx: usize) -> Part {
    Part {
        session_id: msg.session_id().to_owned(),
        id: format!("{}:{idx:04}", msg.id()),
        message_id: msg.id().to_owned(),
        ordinal: 0,
        provenance: Provenance::Conversational,
        options: ProviderOptions::new(),
        kind: PartKind::Text {
            text: pond::adapter::extract_str(
                &serde_json::json!({"x": SAMPLE_TEXTS[idx % SAMPLE_TEXTS.len()]}),
                "x",
            ),
        },
    }
}

/// Append `count` searchable, embedded messages under one fresh session without
/// folding them into any index - i.e. grow the unindexed tail by `count` rows.
async fn append_unindexed(store: &Store, session_ordinal: usize, count: usize) -> Result<()> {
    append_unindexed_no_embed(store, session_ordinal, count).await?;
    // Fill the vector column (no fold) so the tail is embedded-but-unindexed,
    // matching a real post-ingest / pre-fold state.
    EmbedWorker::new(store, &FakeBackend).run().await?;
    Ok(())
}

fn search_request(query: &str, mode: SearchModeWire) -> SearchRequest {
    SearchRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode,
        sort_by: pond::wire::SortBy::Relevance,
        filters: SearchFilters::default(),
        limit: 10,
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64) * p).floor() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

async fn measure(
    store: &Store,
    embedder: &LazyEmbedder,
    cfg: &SearchConfig,
    mode: SearchModeWire,
    queries: usize,
) -> Result<(u128, u128)> {
    let mut ms: Vec<u128> = Vec::with_capacity(queries);
    for q in 0..queries {
        let query = SAMPLE_TEXTS[q % SAMPLE_TEXTS.len()];
        let t = Instant::now();
        match pond_search(store, embedder, search_request(query, mode), cfg).await {
            SearchEnvelope::Success(_) => {}
            SearchEnvelope::Error(e) => return Err(anyhow::anyhow!("search failed: {e:?}")),
        }
        ms.push(t.elapsed().as_millis());
    }
    ms.sort_unstable();
    Ok((percentile(&ms, 0.5), percentile(&ms, 0.95)))
}

struct Row {
    tail: usize,
    fts: (u128, u128),
    vec: (u128, u128),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    );
    let cfg = Config::load(&config_path)?;

    let _guard: Option<TempDir>;
    let store = match args.url.clone() {
        Some(raw) => {
            _guard = None;
            let storage = StorageUrl::parse(&raw)?;
            let resolved = storage.resolve(&cfg.creds)?;
            if resolved.options.is_empty() {
                eprintln!("warning: no [creds.*] matched; using ambient credentials");
            }
            println!("store: {}", resolved.display());
            Store::open_with_options(
                resolved.lance_url(),
                resolved.options.clone(),
                RuntimeCaps::default(),
            )
            .await?
        }
        None => {
            let tmp = TempDir::new()?;
            println!("store: local temp {}", tmp.path().display());
            let s = Store::open_local(tmp.path()).await?;
            _guard = Some(tmp);
            s
        }
    };

    let embedder = LazyEmbedder::from_loaded(Arc::new(FakeBackend) as Arc<dyn Embedder>);
    let search_cfg = SearchConfig::default();

    // Base corpus: sessions + searchable messages, embedded, then folded so the
    // indexed part serves `fast_search` at tail = 0.
    println!(
        "building base: {} sessions x {} messages ...",
        args.base_sessions, args.base_messages
    );
    for s in 0..args.base_sessions {
        append_unindexed_no_embed(&store, 10_000 + s, args.base_messages).await?;
    }
    EmbedWorker::new(&store, &FakeBackend).run().await?;
    println!("folding base (tail -> 0) ...");
    store
        .optimize_indices(None, &MaintenancePolicy::always_compact())
        .await?
        .into_result()?;

    let mut rows: Vec<Row> = Vec::new();
    // tail = 0 baseline (fast_search path).
    rows.push(Row {
        tail: 0,
        fts: measure(
            &store,
            &embedder,
            &search_cfg,
            SearchModeWire::Fts,
            args.queries,
        )
        .await?,
        vec: measure(
            &store,
            &embedder,
            &search_cfg,
            SearchModeWire::Vector,
            args.queries,
        )
        .await?,
    });

    let mut tail_now = 0usize;
    let mut session_ordinal = 0usize;
    for &target in &args.tail_steps {
        // Append the whole delta in one session so each step is a few commits,
        // not one per 1000 rows (remote cost is commit-bound).
        let delta = target.saturating_sub(tail_now);
        if delta > 0 {
            append_unindexed(&store, session_ordinal, delta).await?;
            session_ordinal += 1;
            tail_now = target;
        }
        println!("measuring at tail = {tail_now} ...");
        rows.push(Row {
            tail: tail_now,
            fts: measure(
                &store,
                &embedder,
                &search_cfg,
                SearchModeWire::Fts,
                args.queries,
            )
            .await?,
            vec: measure(
                &store,
                &embedder,
                &search_cfg,
                SearchModeWire::Vector,
                args.queries,
            )
            .await?,
        });
    }

    println!();
    println!(
        "pond read-path benchmark: search latency vs unindexed tail ({} queries/mode, ms)",
        args.queries
    );
    println!();
    println!(
        "{:>8}  {:>9}  {:>9}  {:>9}  {:>9}",
        "tail", "fts_p50", "fts_p95", "vec_p50", "vec_p95",
    );
    println!("{}", "-".repeat(52));
    for r in &rows {
        println!(
            "{:>8}  {:>9}  {:>9}  {:>9}  {:>9}",
            r.tail, r.fts.0, r.fts.1, r.vec.0, r.vec.1,
        );
    }
    println!();
    println!(
        "(tail = 0 uses fast_search; tail > 0 index-probes + flat-scans the tail for complete recall)"
    );
    Ok(())
}

/// Write one session's messages + parts without embedding. The base build
/// embeds the whole corpus in one `EmbedWorker` run afterward; `append_unindexed`
/// wraps this and embeds the just-written tail immediately.
async fn append_unindexed_no_embed(
    store: &Store,
    session_ordinal: usize,
    count: usize,
) -> Result<()> {
    let session = make_session(session_ordinal);
    store
        .upsert_sessions(std::slice::from_ref(&session))
        .await?;
    let messages: Vec<Message> = (0..count).map(|i| message_for(&session, i)).collect();
    let parts: Vec<Part> = messages
        .iter()
        .enumerate()
        .map(|(i, msg)| part_for(msg, i))
        .collect();
    let writes: Vec<MessageWrite> = messages
        .iter()
        .enumerate()
        .map(|(i, msg)| MessageWrite {
            message: msg,
            parts: std::slice::from_ref(&parts[i]),
            search_text: Some(SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()]),
        })
        .collect();
    store.upsert_messages(&session, &writes).await?;
    store.upsert_parts(&parts).await?;
    Ok(())
}
