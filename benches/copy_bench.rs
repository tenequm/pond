#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Store-to-store `pond copy` microbenchmark. Proves the two properties the
//! incremental copy path is supposed to have:
//!
//!   1. **Streaming beats temp-staging on a full copy.** The new path streams
//!      the source scan straight into the destination - appending absent
//!      sessions under one commit per table; the old path rewrote every row
//!      into a local staging dataset first, then re-read it. Both are timed
//!      back to back on identical inputs, and the new path's commit count is
//!      printed to show it does not scale with scan batches.
//!   2. **A re-copy is delta-proportional.** After a full copy, a no-op re-copy
//!      moves zero rows (detection only), and a one-session delta moves only
//!      that session - not the whole corpus.
//!
//! Wall times are printed per scenario; this is the harness that shows the
//! optimization is real and stayed real.
//!
//! Run:
//!   cargo bench --bench copy_bench
//!   cargo bench --bench copy_bench -- --sessions 2000 --messages 8
//!   cargo bench --bench copy_bench -- --backend memory

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use std::path::PathBuf;

use pond::{
    config::Config,
    handlers::ingest_events,
    sessions::{DeltaPlan, IngestEvent, IngestValidator, Store},
    substrate::{
        DEFAULT_SYNC_CLEANUP_INTERVAL, MaintenancePolicy, OptimizeEvent, OptimizeProgressFn,
        RuntimeCaps, StorageUrl, Table,
    },
    wire::{Message, Part, PartKind, Provenance, ProviderOptions, Session},
};
use tempfile::TempDir;
use url::Url;

#[derive(Parser)]
#[command(
    about = "pond store-to-store copy microbenchmark: streaming vs temp-staging, delta scaling"
)]
struct Args {
    /// Number of sessions to seed into the source store.
    #[arg(long, default_value_t = 500)]
    sessions: usize,
    /// Messages (and parts) per seeded session.
    #[arg(long, default_value_t = 5)]
    messages: usize,
    /// Storage backend for the SOURCE store (and dests when --dest-url unset).
    #[arg(long, default_value = "local", value_parser = ["local", "memory"])]
    backend: String,
    /// C8 S3 mode: base store URL for the destinations. When set, the append
    /// and merge dests open as real stores at `<base>-c8append` / `<base>-c8merge`
    /// (creds from the pond config), so the append-vs-merge comparison runs on
    /// the actual remote write path. The bench prints the two prefixes; clean
    /// them up after (they are scratch).
    #[arg(long)]
    dest_url: Option<String>,
    /// Use an existing store as the SOURCE (read-only) instead of seeding a
    /// synthetic one - e.g. the real local corpus, for representative shapes
    /// at full scale. Skips the seed entirely.
    #[arg(long)]
    source_url: Option<String>,
    /// Run only one copy path: `append` or `merge`. Each then runs as the
    /// FIRST (cold) S3 op in its own process, removing the connection-warmup
    /// bias of running append-then-merge in sequence.
    #[arg(long)]
    only: Option<String>,
    /// Append write-path commit-size sweep. For each comma-separated
    /// rows-per-commit B, appends absent `messages` rows into a FRESH dest in
    /// commits of B and reports rows/s, #commits, and ms/commit - the
    /// per-commit latency floor (small B) vs the bandwidth ceiling (large B).
    /// A trailing `bulk` point runs the wholesale single-commit copy path.
    /// In `--dest-url` mode each B gets its own scratch prefix
    /// `<base>-sweep-b<B>` (clean up after).
    #[arg(long)]
    append_sweep: Option<String>,
    /// Cap the number of commits per sweep point so small-B runs stay bounded
    /// on S3 (B=1 with a 50k-row corpus would otherwise be 50k round trips).
    /// Rows appended per point = min(absent, cap * B); ms/commit is unbiased.
    #[arg(long, default_value_t = 30)]
    sweep_commits_cap: usize,
    /// Grown sessions to bake into the `grown` corpus (`--prepare`), or to
    /// expect when running `--corpus`. Each grows by `--messages` messages.
    #[arg(long, default_value_t = 0)]
    grown: usize,
    /// Seed `<dir>/base` and `<dir>/grown` persistent local corpora once, then
    /// exit. Reused by `--corpus` so measurements never re-seed.
    #[arg(long)]
    prepare: Option<PathBuf>,
    /// Measure the grown-delta copy from a `--prepare`d corpus through the real
    /// product path: `plan_incremental_from` + `copy_delta_from` + index fold
    /// (the corpus arm documents the merge-vs-append-across-builds rationale).
    #[arg(long)]
    corpus: Option<PathBuf>,
    /// Profile the optimize/index path's phase breakdown to a LOCAL dir (or
    /// `--dest-url` S3 prefix `<base>-profile`). Seeds `--sessions` base rows,
    /// runs optimize #1 (the from-scratch index BUILD), appends a `--grown`
    /// tail, then runs optimize #2 (the incremental FOLD on this branch, full
    /// REBUILD pre-change) - printing every phase (compact/cleanup/index-*) with
    /// its elapsed_ms so the fold-vs-rebuild cost is visible per index.
    #[arg(long)]
    profile_optimize: Option<PathBuf>,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

fn phase_printer(label: &'static str) -> OptimizeProgressFn {
    Arc::new(move |event| {
        if let OptimizeEvent::PhaseDone {
            table,
            phase,
            elapsed_ms,
        } = event
        {
            println!(
                "  [{label}] {:9} {:13} {:>8} ms",
                table.as_str(),
                phase.label(),
                elapsed_ms,
            );
        }
    })
}

async fn open_configured(url: &str) -> Result<Store> {
    let config_path = pond::config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(std::path::PathBuf::from),
        std::env::var_os("HOME").map(std::path::PathBuf::from),
    );
    let config = Config::load(&config_path)?;
    let storage = StorageUrl::parse(url)?;
    let resolved = storage.resolve(&config.creds)?;
    let caps = RuntimeCaps::from_config(&config.runtime);
    Store::open_with_options(resolved.lance_url(), resolved.options.clone(), caps).await
}

/// Holds backing dirs alive for the store's lifetime (local backend only).
struct StoreHandle {
    store: Store,
    _temp: Option<TempDir>,
}

async fn open_store(backend: &str, authority: &str) -> Result<StoreHandle> {
    match backend {
        "memory" => Ok(StoreHandle {
            store: Store::open(&Url::parse(&format!(
                "shared-memory://copy-bench-{authority}/"
            ))?)
            .await?,
            _temp: None,
        }),
        _ => {
            let temp = TempDir::new()?;
            let store = Store::open_local(temp.path()).await?;
            Ok(StoreHandle {
                store,
                _temp: Some(temp),
            })
        }
    }
}

/// Open a fresh destination for one sweep point: a scratch S3 prefix
/// `<base>-<suffix>` when `--dest-url` is set, else a throwaway local store.
async fn open_sweep_dest(args: &Args, suffix: &str) -> Result<StoreHandle> {
    match &args.dest_url {
        Some(base) => {
            let url = format!("{base}-{suffix}");
            println!("  dest (scratch, clean up): {url}");
            Ok(StoreHandle {
                store: open_configured(&url).await?,
                _temp: None,
            })
        }
        None => open_store(&args.backend, &format!("sweep-{suffix}")).await,
    }
}

/// One sweep point: append absent `messages` rows into a fresh dest in commits
/// of `batch` rows, capped at `commits_cap` commits. Returns (rows appended,
/// commits issued, wall ms). `batch == 0` means the wholesale single-commit
/// copy path (`copy_delta_from`), the fresh-bulk ceiling.
async fn append_sweep_point(
    source: &Store,
    dest: &Store,
    batch: usize,
    commits_cap: usize,
) -> Result<(usize, u64, u128)> {
    let before = dest.dataset(Table::Messages).await?.version_id();
    if batch == 0 {
        let started = Instant::now();
        let plan = dest.plan_incremental_from(source).await?;
        dest.copy_delta_from(source, &plan).await?;
        let ms = started.elapsed().as_millis();
        let commits = dest.dataset(Table::Messages).await?.version_id() - before;
        let (_, rows, _) = source.row_counts().await?;
        return Ok((rows, commits, ms));
    }
    let all: Vec<String> = source
        .collect_ids(Table::Messages)
        .await?
        .into_iter()
        .collect();
    let take = all.len().min(commits_cap.saturating_mul(batch));
    let ids = &all[..take];
    let started = Instant::now();
    for slice in ids.chunks(batch) {
        dest.append_absent_rows(source, Table::Messages, "id", slice)
            .await?;
    }
    let ms = started.elapsed().as_millis();
    let commits = dest.dataset(Table::Messages).await?.version_id() - before;
    Ok((take, commits, ms))
}

fn base_ts() -> DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000, 0).single().expect("ts")
}

/// One session's full event set: a session row plus `messages` message/part
/// pairs, each message timestamped after the last (the timestamps are
/// incidental - incremental detection keys on the per-session message count).
fn session_events(index: usize, messages: usize) -> Vec<IngestEvent> {
    let session_id = format!("copybench-{index:08}");
    let created = base_ts();
    let mut events = Vec::with_capacity(1 + messages * 2);
    events.push(IngestEvent::Session(Session {
        id: session_id.clone(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: created,
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/copybench"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }));
    for m in 0..messages {
        let message = Message::User {
            id: format!("{session_id}-msg-{m}"),
            session_id: session_id.clone(),
            timestamp: created + chrono::Duration::seconds(m as i64),
            options: ProviderOptions::new(),
        };
        let part = Part {
            session_id: session_id.clone(),
            id: format!("{session_id}-msg-{m}:0001"),
            message_id: message.id().to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: pond::adapter::extract_str(
                    &serde_json::json!({"x": "lorem ipsum dolor sit amet copy bench payload"}),
                    "x",
                ),
            },
        };
        events.push(IngestEvent::Message(message));
        events.push(IngestEvent::Part(part));
    }
    events
}

/// Append `count` new messages (ids `from_m..`) to an already-seeded session.
fn grow_events(index: usize, from_m: usize, count: usize) -> Vec<IngestEvent> {
    let session_id = format!("copybench-{index:08}");
    let created = base_ts();
    let mut events = Vec::with_capacity(1 + count * 2);
    events.push(IngestEvent::Session(Session {
        id: session_id.clone(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: created,
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/copybench"}), "x")
            .unwrap(),
        options: ProviderOptions::new(),
    }));
    for m in from_m..from_m + count {
        let message = Message::User {
            id: format!("{session_id}-msg-{m}"),
            session_id: session_id.clone(),
            timestamp: created + chrono::Duration::seconds(m as i64),
            options: ProviderOptions::new(),
        };
        let part = Part {
            session_id: session_id.clone(),
            id: format!("{session_id}-msg-{m}:0001"),
            message_id: message.id().to_owned(),
            ordinal: 0,
            provenance: Provenance::Conversational,
            options: ProviderOptions::new(),
            kind: PartKind::Text {
                text: pond::adapter::extract_str(
                    &serde_json::json!({"x": "lorem ipsum dolor sit amet copy bench payload"}),
                    "x",
                ),
            },
        };
        events.push(IngestEvent::Message(message));
        events.push(IngestEvent::Part(part));
    }
    events
}

/// Mirror `ingest_adapter`'s flush cadence (`handlers::ADAPTER_FLUSH_BATCH`).
const SEED_FLUSH_BATCH: usize = 100;

/// Seed/grow through the real batched ingest path - the same `IngestValidator`
/// push/flush/finish cycle `ingest_adapter` drives in production - so the
/// fixture's commit/fragment shape matches a real sync. One `ingest_events` per
/// session would instead commit per session, turning a tiny corpus into GBs of
/// O(n^2) manifest churn (each commit rewrites the growing manifest).
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

async fn seed(store: &Store, sessions: usize, messages: usize) -> Result<()> {
    ingest_batched(
        store,
        (0..sessions).map(|index| session_events(index, messages)),
    )
    .await
}

/// New path: plan the delta, then append absent sessions and filtered-append
/// grown ones into the destination. Also returns the destination `messages`
/// version bump, i.e. the number of commits this copy added - the metric that
/// proves the append collapses to one commit per table instead of one per scan
/// batch.
async fn streaming_copy(from: &Store, to: &Store) -> Result<(DeltaPlan, u128, u64)> {
    let before = to.dataset(Table::Messages).await?.version_id();
    let started = Instant::now();
    let plan = to.plan_incremental_from(from).await?;
    to.copy_delta_from(from, &plan).await?;
    let elapsed = started.elapsed().as_millis();
    let after = to.dataset(Table::Messages).await?.version_id();
    Ok((plan, elapsed, after - before))
}

/// Old path: rewrite every visible row into a local staging dataset, then
/// re-read and merge it into the destination.
async fn temp_staging_copy(from: &Store, to: &Store) -> Result<u128> {
    let staging = TempDir::new()?;
    let data_dir = staging.path().join("data");
    let started = Instant::now();
    from.export_clean_lance_datasets(&data_dir).await?;
    to.import_clean_lance_datasets(&data_dir).await?;
    Ok(started.elapsed().as_millis())
}

/// C8 treatment: force every absent session through merge-insert instead of the
/// append fast-path - the counterfactual cost of routing copy through a
/// merge-insert seam (`WhenMatched::DoNothing`) rather than the filtered append
/// it now uses. Same inputs as `streaming_copy`; the only difference is append
/// ids are moved into the merge bucket, so the destination pays a per-row target
/// probe/join. The delta vs `streaming_copy` is the cost of dropping the append
/// fast-path.
async fn merge_copy(from: &Store, to: &Store) -> Result<(DeltaPlan, u128, u64)> {
    let before = to.dataset(Table::Messages).await?.version_id();
    let mut plan = to.plan_incremental_from(from).await?;
    for table in [&mut plan.sessions, &mut plan.messages, &mut plan.parts] {
        let appended = std::mem::take(&mut table.append);
        table.merge.extend(appended);
    }
    let started = Instant::now();
    to.copy_delta_from(from, &plan).await?;
    let elapsed = started.elapsed().as_millis();
    let after = to.dataset(Table::Messages).await?.version_id();
    Ok((plan, elapsed, after - before))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let total_rows = args.sessions * args.messages;
    println!(
        "copy_bench: backend={} sessions={} messages/session={} (~{} message rows)\n",
        args.backend, args.sessions, args.messages, total_rows,
    );

    if let Some(dir) = &args.profile_optimize {
        let store = match &args.dest_url {
            Some(base) => {
                let url = format!("{base}-profile");
                println!("store (scratch, clean up): {url}\n");
                open_configured(&url).await?
            }
            None => {
                std::fs::create_dir_all(dir)?;
                Store::open_local(dir).await?
            }
        };
        let policy = MaintenancePolicy::always_compact();
        let t = Instant::now();
        ingest_batched(
            &store,
            (0..args.sessions).map(|index| session_events(index, args.messages)),
        )
        .await?;
        println!(
            "seed base: {} sessions x {} msgs = {} rows  ({} ms)\n",
            args.sessions,
            args.messages,
            args.sessions * args.messages,
            t.elapsed().as_millis(),
        );

        println!("optimize #1 - from-scratch index BUILD over the whole base:");
        let t1 = Instant::now();
        store
            .optimize_indices(Some(phase_printer("build")), &policy)
            .await?;
        println!("  build total: {} ms\n", t1.elapsed().as_millis());

        let sync_policy = MaintenancePolicy::always_compact()
            .with_cleanup_interval(DEFAULT_SYNC_CLEANUP_INTERVAL);
        println!(
            "optimize #2 - {} incremental folds at the sync cleanup interval ({}); each round \
             appends a small tail and finalizes, like one cron sync. A round with NO `cleanup` \
             line skipped the ~per-table version-log walk:",
            args.grown, DEFAULT_SYNC_CLEANUP_INTERVAL,
        );
        for round in 0..args.grown {
            ingest_batched(
                &store,
                std::iter::once(grow_events(round, args.messages, args.messages)),
            )
            .await?;
            let version = store.dataset(Table::Messages).await?.version_id();
            let started = Instant::now();
            store
                .optimize_indices(Some(phase_printer("fold")), &sync_policy)
                .await?;
            println!(
                "  round {round:>2}  messages.v={version}  finalize total: {} ms",
                started.elapsed().as_millis(),
            );
        }
        return Ok(());
    }

    if let Some(dir) = &args.prepare {
        std::fs::create_dir_all(dir.join("base"))?;
        std::fs::create_dir_all(dir.join("grown"))?;
        let base = Store::open_local(&dir.join("base")).await?;
        let t = Instant::now();
        seed(&base, args.sessions, args.messages).await?;
        println!(
            "seed base  ({} x {} msgs): {} ms",
            args.sessions,
            args.messages,
            t.elapsed().as_millis()
        );
        let grown = Store::open_local(&dir.join("grown")).await?;
        let t2 = Instant::now();
        seed(&grown, args.sessions, args.messages).await?;
        ingest_batched(
            &grown,
            (0..args.grown).map(|index| grow_events(index, args.messages, args.messages)),
        )
        .await?;
        println!(
            "seed grown (+{} sessions x {} msgs): {} ms",
            args.grown,
            args.messages,
            t2.elapsed().as_millis()
        );
        println!("corpus ready at {}", dir.display());
        return Ok(());
    }

    if let Some(dir) = &args.corpus {
        let base = Store::open_local(&dir.join("base")).await?;
        let grown = Store::open_local(&dir.join("grown")).await?;
        // The grown-delta copy through the real product path (`plan_incremental_from`
        // + `copy_delta_from`) plus the index fold. The write primitive is whatever
        // the linked library ships: merge_insert on the pre-change build, filtered
        // append on the new one - so running this same bench against each build is
        // the honest merge-vs-append (and full-rebuild-vs-incremental-fold) A/B.
        let dest_handle = match &args.dest_url {
            Some(base_url) => {
                let url = format!("{base_url}-corpus");
                println!("dest (scratch, clean up): {url}\n");
                StoreHandle {
                    store: open_configured(&url).await?,
                    _temp: None,
                }
            }
            None => open_store(&args.backend, "dest-corpus").await?,
        };
        let dest = &dest_handle.store;
        streaming_copy(&base, dest).await?;

        let before = dest.dataset(Table::Messages).await?.version_id();
        let t = Instant::now();
        let plan = dest.plan_incremental_from(&grown).await?;
        let plan_ms = t.elapsed().as_millis();
        let t2 = Instant::now();
        dest.copy_delta_from(&grown, &plan).await?;
        let copy_ms = t2.elapsed().as_millis();
        let commits = dest.dataset(Table::Messages).await?.version_id() - before;
        let t3 = Instant::now();
        dest.optimize_indices(None, &MaintenancePolicy::always_compact())
            .await?;
        let opt = t3.elapsed().as_millis();
        println!(
            "grown delta (copy_delta_from): plan {plan_ms:>5} + copy {copy_ms:>6} + optimize {opt:>7} = {:>7} ms | grown sessions={} msg commits={commits}",
            plan_ms + copy_ms + opt,
            plan.messages.merge.len(),
        );

        let mut missing = 0usize;
        for table in [Table::Sessions, Table::Messages, Table::Parts] {
            let s = grown.collect_ids(table).await?;
            let d = dest.collect_ids(table).await?;
            missing += s.difference(&d).count();
        }
        println!("  missing source rows: {missing} (expect 0)");
        return Ok(());
    }

    let source = match &args.source_url {
        Some(url) => {
            println!("source: existing store {url} (no seed)");
            StoreHandle {
                store: open_configured(url).await?,
                _temp: None,
            }
        }
        None => {
            let source = open_store(&args.backend, "source").await?;
            let seed_start = Instant::now();
            seed(&source.store, args.sessions, args.messages).await?;
            println!("seed source: {} ms", seed_start.elapsed().as_millis());
            source
        }
    };

    if let Some(spec) = &args.append_sweep {
        println!(
            "\nappend write-path sweep (messages table, fresh dest per point, cap {} commits):",
            args.sweep_commits_cap
        );
        println!(
            "{:>6}  {:>8}  {:>9}  {:>9}  {:>11}",
            "batch", "rows", "commits", "wall_ms", "ms/commit"
        );
        for token in spec.split(',').map(str::trim).filter(|t| !t.is_empty()) {
            let batch = if token.eq_ignore_ascii_case("bulk") {
                0
            } else {
                token.parse::<usize>()?
            };
            let suffix = if batch == 0 {
                "sweep-bulk".to_owned()
            } else {
                format!("sweep-b{batch}")
            };
            let dest = open_sweep_dest(&args, &suffix).await?;
            let (rows, commits, ms) =
                append_sweep_point(&source.store, &dest.store, batch, args.sweep_commits_cap)
                    .await?;
            let per_commit = if commits > 0 {
                ms as f64 / commits as f64
            } else {
                0.0
            };
            let rate = if ms > 0 {
                rows as f64 * 1000.0 / ms as f64
            } else {
                0.0
            };
            let label = if batch == 0 {
                "bulk".to_owned()
            } else {
                batch.to_string()
            };
            println!(
                "{label:>6}  {rows:>8}  {commits:>9}  {ms:>9}  {per_commit:>9.1}    ({rate:.0} rows/s)"
            );
        }
        return Ok(());
    }

    // Open the two dests: S3 scratch stores when --dest-url is set, else the
    // same backend as the source. The append dest carries [1]+[3]+[4]; the
    // merge dest carries [1b]; they must be distinct so each measures a copy
    // into a fresh destination.
    let (append_dest, merge_dest) = match &args.dest_url {
        Some(base) => {
            let append_url = format!("{base}-c8append");
            let merge_url = format!("{base}-c8merge");
            println!(
                "S3 scratch dests (clean up after):\n  append: {append_url}\n  merge:  {merge_url}\n"
            );
            (
                StoreHandle {
                    store: open_configured(&append_url).await?,
                    _temp: None,
                },
                StoreHandle {
                    store: open_configured(&merge_url).await?,
                    _temp: None,
                },
            )
        }
        None => (
            open_store(&args.backend, "dest-stream").await?,
            open_store(&args.backend, "dest-merge").await?,
        ),
    };

    // --only: run a single path as the FIRST (cold) S3 op in this process, so
    // append and merge each get an unbiased cold measurement (no warmup
    // inherited from the other).
    match args.only.as_deref() {
        Some("append") => {
            let (plan, ms, commits) = streaming_copy(&source.store, &append_dest.store).await?;
            println!(
                "[append-only] full copy streaming  : {ms:>6} ms  (delta sessions={}, messages commits={commits})",
                plan.total(),
            );
            return Ok(());
        }
        Some("merge") => {
            let (plan, ms, commits) = merge_copy(&source.store, &merge_dest.store).await?;
            println!(
                "[merge-only]  full copy merge-insert: {ms:>6} ms  (delta sessions={}, messages commits={commits})",
                plan.total(),
            );
            return Ok(());
        }
        Some(other) => anyhow::bail!("--only must be append|merge, got {other:?}"),
        None => {}
    }

    // 1. Full copy, streaming (append fast-path).
    let dest = append_dest;
    let (full_plan, stream_ms, full_commits) = streaming_copy(&source.store, &dest.store).await?;
    println!(
        "[1] full copy  streaming      : {stream_ms:>6} ms  (delta sessions={}, messages commits={full_commits})",
        full_plan.total(),
    );

    // 1b. C8: full copy of the SAME source via merge-insert (unified seam)
    // into a fresh destination - the append-vs-merge comparison.
    let dest_merge = merge_dest;
    let (merge_plan, merge_ms, merge_commits) =
        merge_copy(&source.store, &dest_merge.store).await?;
    let merge_ratio = if stream_ms > 0 {
        merge_ms as f64 / stream_ms as f64
    } else {
        f64::INFINITY
    };
    println!(
        "[1b] full copy  merge-insert   : {merge_ms:>6} ms  ({merge_ratio:.2}x append; delta sessions={}, messages commits={merge_commits})",
        merge_plan.total(),
    );

    // 2. Full copy, temp-staging (old path). Local-only: it's the 400x-slow
    // legacy baseline and would need a third S3 dest, so skip it in S3 mode.
    if args.dest_url.is_none() {
        let dest_legacy = open_store(&args.backend, "dest-legacy").await?;
        let legacy_ms = temp_staging_copy(&source.store, &dest_legacy.store).await?;
        let speedup = if stream_ms > 0 {
            legacy_ms as f64 / stream_ms as f64
        } else {
            f64::INFINITY
        };
        println!(
            "[2] full copy  temp-staging   : {legacy_ms:>6} ms  ({speedup:.2}x slower than streaming)"
        );
    }

    // 3. No-op re-copy of the streamed destination: detection only, zero rows.
    let (noop_plan, noop_ms, noop_commits) = streaming_copy(&source.store, &dest.store).await?;
    println!(
        "[3] re-copy    no change      : {noop_ms:>6} ms  (delta sessions={}, messages commits={noop_commits}, expect 0/0)",
        noop_plan.total(),
    );

    // 4. One-session delta: add a session to the source, re-copy.
    ingest_events(&source.store, session_events(args.sessions, args.messages)).await?;
    let (delta_plan, delta_ms, delta_commits) = streaming_copy(&source.store, &dest.store).await?;
    println!(
        "[4] re-copy    +1 session     : {delta_ms:>6} ms  (delta sessions={}, messages commits={delta_commits}, expect 1/1)",
        delta_plan.total(),
    );

    println!("\nverification:");
    println!(
        "  full delta == all sessions : {}",
        full_plan.total() == args.sessions
    );
    println!(
        "  full copy == 1 msg commit  : {}  (append collapses to one commit per table)",
        full_commits == 1
    );
    println!("  no-op delta == 0           : {}", noop_plan.is_empty());
    println!("  +1 delta == 1              : {}", delta_plan.total() == 1);
    Ok(())
}
