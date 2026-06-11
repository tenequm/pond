#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Backend latency comparison: local file:// vs an object-store URL the user
//! passes via `--s3-url`. Reads `[storage]` from the user config the same way
//! the CLI does. Times: open, bulk write, embed-worker pass, index build,
//! FTS query, vector query, single-row read, row counts.
//!
//! Run:
//!   cargo bench --bench backend_bench -- --s3-url s3://ttq/data
//!   cargo bench --bench backend_bench -- --s3-url s3://ttq/data --messages 500 --queries 20
//!   cargo bench --bench backend_bench -- --skip-search           # write-only smoke
//!   cargo bench --bench backend_bench -- --skip-remote           # local only

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
    substrate::MaintenancePolicy,
    wire::{
        Message, Part, PartKind, Provenance, ProviderOptions, SearchEnvelope, SearchFilters,
        SearchModeWire, SearchRequest, Session,
    },
};
use tempfile::TempDir;

#[derive(Parser)]
#[command(about = "pond backend benchmark: local FS vs object-store URL")]
struct Args {
    /// Remote storage URL (spec.md#storage-url-grammar); creds resolve from
    /// the user config's [creds.*] sets like any pond command.
    #[arg(long, value_name = "URL")]
    s3_url: Option<String>,
    #[arg(long, default_value_t = 100)]
    sessions: usize,
    #[arg(long, default_value_t = 3)]
    rounds: usize,
    #[arg(long, default_value_t = 200)]
    messages: usize,
    #[arg(long, default_value_t = 10)]
    queries: usize,
    #[arg(long)]
    skip_local: bool,
    #[arg(long)]
    skip_remote: bool,
    #[arg(long)]
    skip_search: bool,
    /// Reuse a stable local data dir (`target/bench-pond-local`) and skip
    /// dataset creation on subsequent runs. Lets you measure warm-open and
    /// incremental-write cost without paying the cold table-create tax each
    /// invocation. Remote (`--s3-url`) already persists across runs.
    #[arg(long)]
    existing: bool,
    #[arg(long, hide = true)]
    bench: bool,
}

#[derive(Default)]
struct BenchRow {
    label: String,
    open_ms: u128,
    write_total_ms: u128,
    write_p50_ms: u128,
    write_p95_ms: u128,
    embed_ms: Option<u128>,
    index_ms: Option<u128>,
    fts_p50_ms: Option<u128>,
    vec_p50_ms: Option<u128>,
    read_one_ms: u128,
    row_counts_ms: u128,
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

fn make_session(i: usize) -> Session {
    Session {
        id: format!("01HXYBENCH{i:09}"),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": format!("/tmp/p/{i}")}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }
}

fn make_sessions(offset: usize, n: usize) -> Vec<Session> {
    (0..n).map(|i| make_session(offset + i)).collect()
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

fn message_for(session: &Session, idx: usize) -> Message {
    Message::User {
        id: format!("msg-{idx:06}"),
        session_id: session.id.clone(),
        timestamp: Utc::now(),
        options: ProviderOptions::new(),
    }
}

fn part_for(session: &Session, msg: &Message, idx: usize) -> Part {
    Part {
        session_id: session.id.clone(),
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

fn search_request(query: &str, mode: Option<SearchModeWire>) -> SearchRequest {
    SearchRequest {
        protocol_version: PROTOCOL_VERSION,
        namespace: Some("local".to_owned()),
        query: query.to_owned(),
        mode_override: mode,
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

async fn run_bench(label: String, store: &Store, args: &Args, open_ms: u128) -> Result<BenchRow> {
    let mut per_round_ms: Vec<u128> = Vec::with_capacity(args.rounds);
    for r in 0..args.rounds {
        let batch = make_sessions(r * args.sessions, args.sessions);
        let t = Instant::now();
        store.upsert_sessions(&batch).await?;
        per_round_ms.push(t.elapsed().as_millis());
    }
    let write_total_ms: u128 = per_round_ms.iter().sum();
    per_round_ms.sort_unstable();
    let write_p50_ms = percentile(&per_round_ms, 0.5);
    let write_p95_ms = percentile(&per_round_ms, 0.95);

    let mut embed_ms = None;
    let mut index_ms = None;
    let mut fts_p50_ms = None;
    let mut vec_p50_ms = None;

    if !args.skip_search {
        let session = make_session(usize::MAX);
        store
            .upsert_sessions(std::slice::from_ref(&session))
            .await?;
        let messages: Vec<Message> = (0..args.messages)
            .map(|i| message_for(&session, i))
            .collect();
        let mut parts: Vec<Part> = Vec::with_capacity(args.messages);
        let mut writes: Vec<MessageWrite> = Vec::with_capacity(args.messages);
        for (i, msg) in messages.iter().enumerate() {
            parts.push(part_for(&session, msg, i));
        }
        for (i, msg) in messages.iter().enumerate() {
            writes.push(MessageWrite {
                message: msg,
                parts: std::slice::from_ref(&parts[i]),
                search_text: Some(SAMPLE_TEXTS[i % SAMPLE_TEXTS.len()]),
            });
        }
        store.upsert_messages(&session, &writes).await?;
        store.upsert_parts(&parts).await?;

        let backend = FakeBackend;
        let t = Instant::now();
        EmbedWorker::new(store, &backend).run().await?;
        embed_ms = Some(t.elapsed().as_millis());

        let t = Instant::now();
        store
            .optimize_indices(None, &MaintenancePolicy::always_compact())
            .await?
            .into_result()?;
        index_ms = Some(t.elapsed().as_millis());

        let embedder = LazyEmbedder::from_loaded(Arc::new(FakeBackend) as Arc<dyn Embedder>);
        let cfg = SearchConfig::default();
        let mut fts_ms: Vec<u128> = Vec::with_capacity(args.queries);
        let mut vec_ms: Vec<u128> = Vec::with_capacity(args.queries);
        for q in 0..args.queries {
            let query = SAMPLE_TEXTS[q % SAMPLE_TEXTS.len()];
            let t = Instant::now();
            match pond_search(
                store,
                &embedder,
                search_request(query, Some(SearchModeWire::Fts)),
                &cfg,
            )
            .await
            {
                SearchEnvelope::Success(_) => {}
                SearchEnvelope::Error(e) => {
                    return Err(anyhow::anyhow!("fts search failed: {e:?}"));
                }
            }
            fts_ms.push(t.elapsed().as_millis());

            let t = Instant::now();
            match pond_search(
                store,
                &embedder,
                search_request(query, Some(SearchModeWire::Vector)),
                &cfg,
            )
            .await
            {
                SearchEnvelope::Success(_) => {}
                SearchEnvelope::Error(e) => {
                    return Err(anyhow::anyhow!("vec search failed: {e:?}"));
                }
            }
            vec_ms.push(t.elapsed().as_millis());
        }
        fts_ms.sort_unstable();
        vec_ms.sort_unstable();
        fts_p50_ms = Some(percentile(&fts_ms, 0.5));
        vec_p50_ms = Some(percentile(&vec_ms, 0.5));
    }

    let first_id = format!("01HXYBENCH{:09}", 0);
    let t = Instant::now();
    let _ = store.get_session(&first_id).await?;
    let read_one_ms = t.elapsed().as_millis();

    let t = Instant::now();
    let _ = store.row_counts().await?;
    let row_counts_ms = t.elapsed().as_millis();

    Ok(BenchRow {
        label,
        open_ms,
        write_total_ms,
        write_p50_ms,
        write_p95_ms,
        embed_ms,
        index_ms,
        fts_p50_ms,
        vec_p50_ms,
        read_one_ms,
        row_counts_ms,
    })
}

fn cell_opt(value: Option<u128>) -> String {
    value.map_or_else(|| "-".to_owned(), |ms| ms.to_string())
}

fn print_table(rows: &[BenchRow], args: &Args) {
    println!();
    println!(
        "pond backend benchmark: {} rounds x {} sessions; {} messages, {} queries",
        args.rounds, args.sessions, args.messages, args.queries,
    );
    println!();
    let headers = [
        "backend",
        "open",
        "write_tot",
        "w_p50",
        "w_p95",
        "embed_run",
        "index",
        "fts_p50",
        "vec_p50",
        "read1",
        "rowcnt",
    ];
    println!(
        "{:<28}  {:>6}  {:>10}  {:>6}  {:>6}  {:>10}  {:>6}  {:>8}  {:>8}  {:>6}  {:>7}",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        headers[4],
        headers[5],
        headers[6],
        headers[7],
        headers[8],
        headers[9],
        headers[10],
    );
    println!("{}", "-".repeat(120));
    for r in rows {
        println!(
            "{:<28}  {:>6}  {:>10}  {:>6}  {:>6}  {:>10}  {:>6}  {:>8}  {:>8}  {:>6}  {:>7}",
            r.label,
            r.open_ms,
            r.write_total_ms,
            r.write_p50_ms,
            r.write_p95_ms,
            cell_opt(r.embed_ms),
            cell_opt(r.index_ms),
            cell_opt(r.fts_p50_ms),
            cell_opt(r.vec_p50_ms),
            r.read_one_ms,
            r.row_counts_ms,
        );
    }
    println!();
    println!(
        "(all times in ms; embed_run uses a deterministic fake backend so the delta isolates storage I/O)"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let config_path = config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    );
    let cfg = Config::load(&config_path)?;

    let mut rows: Vec<BenchRow> = Vec::new();

    if !args.skip_local {
        // Keep the TempDir guard alive for the lifetime of the local Store.
        let (local_path, _guard) = if args.existing {
            let p = PathBuf::from("target/bench-pond-local");
            std::fs::create_dir_all(&p)?;
            (p, None)
        } else {
            let tmp = TempDir::new()?;
            let p = tmp.path().to_path_buf();
            (p, Some(tmp))
        };
        let t = Instant::now();
        let store = Store::open_local(&local_path).await?;
        let open_ms = t.elapsed().as_millis();
        rows.push(run_bench("local file://".to_owned(), &store, &args, open_ms).await?);
    }

    if !args.skip_remote {
        match args.s3_url.clone() {
            Some(raw) => {
                let storage = pond::substrate::StorageUrl::parse(&raw)?;
                let resolved = storage.resolve(&cfg.creds)?;
                if resolved.options.is_empty() {
                    eprintln!(
                        "warning: no [creds.*] set matched in {} - remote arm will use ambient credentials",
                        config_path.display()
                    );
                }
                let t = Instant::now();
                let store = Store::open_with_options(
                    resolved.lance_url(),
                    resolved.options.clone(),
                    pond::substrate::RuntimeCaps::default(),
                )
                .await?;
                let open_ms = t.elapsed().as_millis();
                rows.push(
                    run_bench(
                        format!("remote {}", resolved.display()),
                        &store,
                        &args,
                        open_ms,
                    )
                    .await?,
                );
            }
            None => {
                eprintln!("skip remote: pass --s3-url s3://bucket/path to bench an object store");
            }
        }
    }

    print_table(&rows, &args);
    Ok(())
}
