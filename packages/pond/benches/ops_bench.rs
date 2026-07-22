#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! Where does the wall-clock go on `pond status`, `pond sync`, `pond optimize`,
//! and `pond copy` against a real store (especially a remote S3 one)?
//!
//! Each CLI verb is built from a handful of `Store` primitives. This harness
//! opens the configured store and times those primitives **read-only**, grouped
//! by the verb that calls them, so a slow command can be attributed to one
//! phase instead of guessed at. The write phases (the actual ingest/embed/index
//! commit) are the work itself and are timed by each command's own progress -
//! this bench targets the detection/scan phases that dominate the *silent*
//! startup gap before a command prints anything.
//!
//! It reads `[storage]` + `[creds]` from the normal config, so it hits the same
//! backend the CLI does. Point it at the small benchmark corpus for fast
//! iteration:
//!
//!   cargo bench --bench ops_bench -- \
//!     --url s3+https://nbg1.your-objectstorage.com/pondarium/pond-benchmark-corpus
//!   cargo bench --bench ops_bench          # uses [storage].path from config
//!   cargo bench --bench ops_bench -- --no-copy-detect

use std::future::Future;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use clap::Parser;
use pond::{
    config::Config,
    sessions::Store,
    substrate::{RuntimeCaps, StorageUrl},
};

#[derive(Parser)]
#[command(
    about = "pond per-verb phase timing: attribute the wall-clock on status/sync/optimize/copy"
)]
struct Args {
    /// Store to benchmark. Defaults to `[storage].path` from the config.
    #[arg(long)]
    url: Option<String>,
    /// Skip the `pond copy` detection scan (opens a second handle to the same
    /// store and plans the delta - read-only, but a full scan on a large store).
    #[arg(long)]
    no_copy_detect: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// Time one read-only primitive and print `label : <ms>`. Propagates the result
/// so callers can reuse it (e.g. the session map feeds the copy-detect open).
async fn timed<T>(label: &str, fut: impl Future<Output = Result<T>>) -> Result<T> {
    let start = Instant::now();
    let out = fut.await;
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    match &out {
        Ok(_) => println!("  {label:<40} {ms:>9.1} ms"),
        Err(error) => println!("  {label:<40} {ms:>9.1} ms  ERR: {error}"),
    }
    out
}

async fn open_configured(url: &str, config: &Config) -> Result<Store> {
    let storage = StorageUrl::parse(url)?;
    let resolved = storage.resolve(&config.creds)?;
    let caps = RuntimeCaps::from_config(&config.runtime);
    Store::open_with_options(resolved.lance_url(), resolved.options.clone(), caps).await
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

    println!("ops_bench: {url}\n");

    let open_start = Instant::now();
    let store = open_configured(&url, &config).await?;
    println!(
        "  {:<40} {:>9.1} ms",
        "open store (manifests)",
        open_start.elapsed().as_secs_f64() * 1000.0,
    );
    if !store.initialized().await? {
        bail!("store is not initialized (no data); point --url at a synced corpus");
    }

    // pond status: every primitive its handler joins. None scans the messages
    // table - totals are manifest metadata, the adapter count is a sessions scan.
    println!("\n[status] (read-only)");
    timed("row_counts", store.row_counts()).await?;
    timed("table_sizes", store.table_sizes()).await?;
    timed("index_status", store.index_status()).await?;
    timed("embedding_progress", store.embedding_progress()).await?;
    timed("adapter_names(false)", store.adapter_names(false)).await?;

    // pond sync: the change-detection oracle built before any progress prints.
    // The key derives from durable message rows, not Lance version history.
    println!("\n[sync] change-detection oracle (read-only)");
    timed(
        "session_last_message_ids COLD",
        store.session_last_message_ids(),
    )
    .await?;
    timed(
        "session_last_message_ids WARM",
        store.session_last_message_ids(),
    )
    .await?;

    // pond optimize: the read side that decides what work is owed. The embed +
    // index commits that follow are the work itself, timed by the command.
    println!("\n[optimize] backlog probes (read-only)");
    timed("stale_embedding_count", store.stale_embedding_count()).await?;
    timed("embedding_progress", store.embedding_progress()).await?;

    // pond copy: the delta plan compares per-session message counts across two
    // stores. Self-to-self yields an empty plan but times the detection scan -
    // the phase that runs before copy moves a single row.
    if !args.no_copy_detect {
        println!("\n[copy] delta detection, self-to-self (read-only)");
        let dest = open_configured(&url, &config).await?;
        let plan = timed("plan_incremental_from", dest.plan_incremental_from(&store))
            .await
            .context("copy delta plan")?;
        println!("  (delta sessions: {})", plan.total());
    }

    println!("\ndone");
    Ok(())
}
