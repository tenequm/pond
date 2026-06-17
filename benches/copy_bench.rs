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

use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use pond::{
    config::Config,
    handlers::ingest_events,
    sessions::{DeltaPlan, IngestEvent, Store},
    substrate::{RuntimeCaps, StorageUrl, Table},
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
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
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

async fn seed(store: &Store, sessions: usize, messages: usize) -> Result<()> {
    for index in 0..sessions {
        ingest_events(store, session_events(index, messages)).await?;
    }
    Ok(())
}

/// New path: plan the delta, append absent sessions / merge grown ones into the
/// destination. Also returns the destination `messages` version bump, i.e. the
/// number of commits this copy added - the metric that proves the append
/// collapses to one commit per table instead of one per scan batch.
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
/// append fast-path - what routing copy through the unified write seam
/// (`upsert_session_batch`, `WhenMatched::DoNothing`) would do. Same inputs as
/// `streaming_copy`; the only difference is append ids are moved into the merge
/// bucket, so the destination pays a per-row target probe/join. The delta vs
/// `streaming_copy` is the cost of dropping the append fast-path.
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

    // Open the two dests: S3 scratch stores when --dest-url is set, else the
    // same backend as the source. The append dest carries [1]+[3]+[4]; the
    // merge dest carries [1b]; they must be distinct so each measures a copy
    // into a fresh destination.
    let (append_dest, merge_dest) = match &args.dest_url {
        Some(base) => {
            let append_url = format!("{base}-c8append");
            let merge_url = format!("{base}-c8merge");
            println!("S3 scratch dests (clean up after):\n  append: {append_url}\n  merge:  {merge_url}\n");
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
    let (merge_plan, merge_ms, merge_commits) = merge_copy(&source.store, &dest_merge.store).await?;
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
