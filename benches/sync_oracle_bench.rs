#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Empirically rank candidate "sync change-detection oracles" against the real
//! S3 corpus, so the answer is measured, not guessed.
//!
//! The oracle answers ONE question: "for each `session_id`, what watermark
//! does the destination know about?" The driver compares that against the
//! local adapter's mtime/line-count and ingests only the sessions that grew.
//!
//! Candidates timed:
//!
//!   A. `current`              -> `Store::session_last_message_ids`. Scans
//!                                 `messages.session_id` + `messages.timestamp`
//!                                 and folds `MAX(timestamp) GROUP BY session_id`
//!                                 in Rust. C is the underlying shape; A times
//!                                 the production code path end-to-end.
//!   B. `messages_group_count` -> `SELECT session_id, COUNT(*) FROM messages
//!                                 GROUP BY session_id`. Aggregation over the
//!                                 messages `session_id` column.
//!   C. `messages_group_maxts` -> `SELECT session_id, MAX(timestamp) FROM messages
//!                                 GROUP BY session_id`. Aggregation over two
//!                                 messages columns.
//!   D. `messages_total_count` -> `SELECT COUNT(*) FROM messages`. Baseline -
//!                                 the cheapest possible whole-table aggregate
//!                                 (lance metadata short-circuit).
//!   E. `sessions_ids_only`    -> `SELECT id FROM sessions`. Baseline - the
//!                                 cost of scanning just the sessions table's
//!                                 id column.
//!   F. `sessions_full_scan`   -> `SELECT id, source_agent, created_at FROM
//!                                 sessions`. What sync USED to depend on.
//!
//! Usage (always against the small benchmark corpus):
//!
//!   cargo bench --bench sync_oracle_bench -- \
//!     --url s3+https://nbg1.your-objectstorage.com/pondarium/pond-benchmark-corpus
//!
//! By default every strategy runs twice (cold then warm) in one process. The
//! later strategies inherit dataset/object-store caches from the earlier ones,
//! so the FIRST strategy in the sequence pays the cold-open tax. To get a
//! truly fresh cold measurement, pin a single strategy with `--only NAME` and
//! launch a fresh process per invocation - the harness prints the right
//! commands at the end.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use pond::sql::{Mode, Outcome, Tables};
use pond::substrate::Table;
use pond::{
    config::Config,
    sessions::Store,
    substrate::{RuntimeCaps, StorageUrl},
};

#[derive(Parser)]
#[command(about = "Rank sync change-detection oracles by wall-clock against the real store")]
struct Args {
    /// Store to benchmark. Defaults to `[storage].path` from the config.
    #[arg(long)]
    url: Option<String>,
    /// Run only one strategy (by name from the list in the module doc).
    /// Without this every strategy runs in one process - convenient but the
    /// later ones inherit caches from the earlier ones.
    #[arg(long)]
    only: Option<String>,
    /// Skip the warm pass (only time the first call). Useful when each
    /// strategy is run in its own fresh process via `--only`.
    #[arg(long)]
    cold_only: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// Time one named primitive twice and print `label COLD/WARM : <ms>` rows.
/// On a failure the error is printed inline; the bench keeps going so a
/// single broken strategy never hides the others.
async fn timed_pair<T, F, Fut>(label: &str, cold_only: bool, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    for tag in ["COLD", "WARM"] {
        if tag == "WARM" && cold_only {
            break;
        }
        let start = Instant::now();
        let outcome = f().await;
        let ms = start.elapsed().as_secs_f64() * 1000.0;
        let label_full = format!("{label} {tag}");
        match outcome {
            Ok(_) => println!("  {label_full:<40} {ms:>10.1} ms"),
            Err(error) => {
                println!("  {label_full:<40} {ms:>10.1} ms  ERR: {error:#}");
                break;
            }
        }
    }
}

async fn open_configured(url: &str, config: &Config) -> Result<Store> {
    let storage = StorageUrl::parse(url)?;
    let resolved = storage.resolve(&config.creds)?;
    let caps = RuntimeCaps::from_config(&config.runtime);
    Store::open_with_options(resolved.lance_url(), resolved.options.clone(), caps).await
}

async fn fetch_tables(store: &Store) -> Result<Tables> {
    let (sessions, messages, parts) = tokio::try_join!(
        store.dataset(Table::Sessions),
        store.dataset(Table::Messages),
        store.dataset(Table::Parts),
    )?;
    Ok(Tables {
        sessions: Some(sessions),
        messages: Some(messages),
        parts: Some(parts),
    })
}

/// Run a SQL query through pond's own `sql::run` path - so the bench exercises
/// the exact DataFusion + Lance pushdown wiring the MCP `pond_sql_query` tool
/// uses. Returns the row count from the result; the inline rendering cost is
/// kept tiny by passing `inline_rows = 0`.
async fn run_sql(tables: &Tables, sql: &str) -> Result<usize> {
    let outcome = pond::sql::run(tables, sql, Mode::Export(pond::sql::Format::Ndjson), 0)
        .await
        .map_err(|err| match err {
            pond::sql::SqlError::Query(msg) => anyhow::anyhow!("query: {msg}"),
            pond::sql::SqlError::Infra(err) => err,
        })?;
    let count = match outcome {
        Outcome::Inline(_) => 0,
        Outcome::Export { rows, .. } => rows,
    };
    Ok(count)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Set LANCE_IO_THREADS in the env to A/B thread counts for this bench.
    let args = Args::parse();
    let config_path = pond::config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    );
    let config = Config::load(&config_path)?;
    let url = match args.url.clone().or_else(|| config.storage.path.clone()) {
        Some(url) => url,
        None => bail!(
            "pass --url or set [storage].path in {}",
            config_path.display()
        ),
    };

    println!("sync_oracle_bench: {url}\n");

    let open_start = Instant::now();
    let store = open_configured(&url, &config).await?;
    let open_ms = open_start.elapsed().as_secs_f64() * 1000.0;
    println!("  {:<40} {:>10.1} ms", "open store (manifests)", open_ms);
    if !store.initialized().await? {
        bail!("store is not initialized (no data); point --url at a synced corpus");
    }
    let store = Arc::new(store);

    let run = |name: &str| -> bool {
        match args.only.as_deref() {
            None => true,
            Some(only) => only == name,
        }
    };

    if run("current") {
        println!("\n[A] current - Store::session_last_message_ids");
        let s = store.clone();
        timed_pair("current", args.cold_only, move || {
            let s = s.clone();
            async move { s.session_last_message_ids().await.map(|m| m.len()) }
        })
        .await;
    }

    if run("ingested_at") {
        println!(
            "\n[M] ingested_at - Store::session_last_ingested_at (mtime oracle; distinct-version resolution, no versions() storm)"
        );
        let s = store.clone();
        timed_pair("ingested_at", args.cold_only, move || {
            let s = s.clone();
            async move { s.session_last_ingested_at().await.map(|m| m.len()) }
        })
        .await;
    }

    if run("messages_group_count") {
        println!(
            "\n[B] messages_group_count - SELECT session_id, COUNT(*) FROM messages GROUP BY session_id"
        );
        let s = store.clone();
        timed_pair("messages_group_count", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(
                    &tables,
                    "SELECT session_id, COUNT(*) AS n FROM messages GROUP BY session_id",
                )
                .await
            }
        })
        .await;
    }

    if run("messages_group_maxts") {
        println!(
            "\n[C] messages_group_maxts - SELECT session_id, MAX(timestamp) FROM messages GROUP BY session_id"
        );
        let s = store.clone();
        timed_pair("messages_group_maxts", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(
                    &tables,
                    "SELECT session_id, MAX(timestamp) AS t FROM messages GROUP BY session_id",
                )
                .await
            }
        })
        .await;
    }

    if run("messages_total_count") {
        println!("\n[D] messages_total_count - SELECT COUNT(*) FROM messages (baseline)");
        let s = store.clone();
        timed_pair("messages_total_count", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(&tables, "SELECT COUNT(*) AS n FROM messages").await
            }
        })
        .await;
    }

    if run("sessions_ids_only") {
        println!("\n[E] sessions_ids_only - SELECT session_id FROM sessions (baseline)");
        let s = store.clone();
        timed_pair("sessions_ids_only", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(&tables, "SELECT session_id FROM sessions").await
            }
        })
        .await;
    }

    // (2) The read/plan cost the id-set approach ADDS over the current counts:
    // `collect_ids(messages)` materializes a HashSet of every message id (~2M
    // strings); `all_session_message_counts` folds into a tiny per-session map.
    // Run each with cold+warm and compare the WARM numbers - that isolates the
    // materialization cost from S3 cache noise.
    if run("messages_idset") {
        println!("\n[K] messages_idset - collect_ids(messages) -> HashSet<id> (id-diff input)");
        let s = store.clone();
        timed_pair("messages_idset", args.cold_only, move || {
            let s = s.clone();
            async move { s.collect_ids(Table::Messages).await.map(|m| m.len()) }
        })
        .await;
    }

    if run("messages_counts") {
        println!(
            "\n[L] messages_counts - all_session_message_counts() -> per-session map (current plan input)"
        );
        let s = store.clone();
        timed_pair("messages_counts", args.cold_only, move || {
            let s = s.clone();
            async move { s.all_session_message_counts().await.map(|m| m.len()) }
        })
        .await;
    }

    if run("messages_group_count_and_maxts") {
        println!(
            "\n[G] messages_group_count_and_maxts - SELECT session_id, COUNT(*), MAX(timestamp) FROM messages GROUP BY session_id"
        );
        let s = store.clone();
        timed_pair(
            "messages_group_count_and_maxts",
            args.cold_only,
            move || {
                let s = s.clone();
                async move {
                    let tables = fetch_tables(&s).await.context("fetch tables")?;
                    run_sql(
                        &tables,
                        "SELECT session_id, COUNT(*) AS n, MAX(timestamp) AS t FROM messages GROUP BY session_id",
                    )
                    .await
                }
            },
        )
        .await;
    }

    if run("sessions_full_scan") {
        println!(
            "\n[F] sessions_full_scan - SELECT session_id, source_agent, created_at FROM sessions"
        );
        let s = store.clone();
        timed_pair("sessions_full_scan", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(
                    &tables,
                    "SELECT session_id, source_agent, created_at FROM sessions",
                )
                .await
            }
        })
        .await;
    }

    // A3: cost of a per-session PARTS signal (the candidate fix for the
    // parts-only-growth under-copy). Parts is the largest table - this is the
    // number that says whether routing copy off a per-session parts key is
    // affordable on S3.
    if run("parts_group_count") {
        println!(
            "\n[H] parts_group_count - SELECT session_id, COUNT(*) FROM parts GROUP BY session_id (A3)"
        );
        let s = store.clone();
        timed_pair("parts_group_count", args.cold_only, move || {
            let s = s.clone();
            async move {
                let tables = fetch_tables(&s).await.context("fetch tables")?;
                run_sql(
                    &tables,
                    "SELECT session_id, COUNT(*) AS n FROM parts GROUP BY session_id",
                )
                .await
            }
        })
        .await;
    }

    // A2: cost of the closing id-set verify. `verify_stores` runs
    // `collect_ids` on all three tables of BOTH stores (concurrently); this
    // times one store's three-table id read - the dominant cost, the set-diff
    // is in-memory. Parts dominates.
    if run("verify_collect_ids_all") {
        println!(
            "\n[I] verify_collect_ids_all - collect_ids(sessions)+collect_ids(messages)+collect_ids(parts) (A2)"
        );
        let s = store.clone();
        timed_pair("verify_collect_ids_all", args.cold_only, move || {
            let s = s.clone();
            async move {
                let (a, b, c) = tokio::try_join!(
                    s.collect_ids(Table::Sessions),
                    s.collect_ids(Table::Messages),
                    s.collect_ids(Table::Parts),
                )?;
                Ok(a.len() + b.len() + c.len())
            }
        })
        .await;
    }

    // CDF: does `_row_created_at_version > V` prune to recent fragments (cheap,
    // metadata-driven) or full-scan the column? `V = latest-1` selects only the
    // last commit's new rows - the "what changed since the last copy" slice copy
    // would append. If this is fast vs `messages_total_count`, the change-data-feed
    // can replace the per-session count-comparison in `plan_incremental_from`.
    if run("cdf_recent") {
        println!(
            "\n[J] cdf_recent - COUNT messages WHERE _row_created_at_version > latest-1 (change data feed)"
        );
        let ds = store
            .dataset(Table::Messages)
            .await
            .context("open messages")?;
        let latest = ds.version().version;
        for k in [1u64, 20, 100, latest] {
            let v = latest.saturating_sub(k);
            let start = Instant::now();
            match ds
                .count_rows(Some(format!("_row_created_at_version > {v}")))
                .await
            {
                Ok(n) => println!(
                    "  cdf since v{v:<4} (latest=v{latest}, -{k}): {n:>8} rows  {:>8.1} ms",
                    start.elapsed().as_secs_f64() * 1000.0
                ),
                Err(error) => {
                    println!("  cdf since v{v}: ERR: {error:#}");
                    break;
                }
            }
        }
    }

    println!("\ndone");
    println!("\nFor true cold-process timings re-run with --only NAME --cold-only:");
    for name in [
        "current",
        "messages_group_count",
        "messages_group_maxts",
        "messages_group_count_and_maxts",
        "messages_total_count",
        "sessions_ids_only",
        "sessions_full_scan",
        "parts_group_count",
        "verify_collect_ids_all",
    ] {
        println!(
            "  cargo bench --bench sync_oracle_bench -- --url {url} --only {name} --cold-only"
        );
    }
    Ok(())
}
