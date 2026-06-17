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
//!   A. `current`              -> `Store::session_last_ingested_at`. Scans
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
        sessions,
        messages,
        parts,
    })
}

/// Run a SQL query through pond's own `sql::run` path - so the bench exercises
/// the exact DataFusion + Lance pushdown wiring the MCP `pond_sql_query` tool
/// uses. Returns the row count from the result; the inline rendering cost is
/// kept tiny by passing `inline_rows = 0`.
async fn run_sql(tables: &Tables, sql: &str) -> Result<usize> {
    let outcome = pond::sql::run(tables, sql, Mode::InlineJson, 0)
        .await
        .map_err(|err| match err {
            pond::sql::SqlError::Query(msg) => anyhow::anyhow!("query: {msg}"),
            pond::sql::SqlError::Infra(err) => err,
        })?;
    let count = match outcome {
        Outcome::Inline(_) => 0,
        Outcome::InlineJson(doc) => {
            doc.get("total_rows").and_then(|v| v.as_u64()).unwrap_or(0) as usize
        }
        Outcome::Export { rows, .. } => rows,
    };
    Ok(count)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
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
        println!("\n[A] current - Store::session_last_ingested_at");
        let s = store.clone();
        timed_pair("current", args.cold_only, move || {
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
    ] {
        println!(
            "  cargo bench --bench sync_oracle_bench -- --url {url} --only {name} --cold-only"
        );
    }
    Ok(())
}
