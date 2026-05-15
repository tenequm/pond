use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use pond::{
    adapter,
    config::{self, Config, DEFAULT_CONFIG_TOML, MaintenanceConfig, StorageLocation},
    embed::{BatchProgress, EmbedBackend, EmbedWorker, Qwen3Embedder},
    handlers::{self, IngestSummary},
    sessions::Store,
    transport::{self, AppState},
};
use serde_json::{Value, json};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "pond", version, about = "Session storage and retrieval")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print basic binary status.
    Status {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
    },
    /// Import sessions from one or more configured source adapters. With no
    /// `<adapter>` arg, syncs every entry in `[sources.*]`. With an empty
    /// `[sources]` config, runs adapter discovery: each adapter probes its
    /// canonical install location, the operator picks which to register, and
    /// the picks are written back to `config.toml` before the sync proceeds.
    Sync {
        /// Optional adapter name (`claude-code`, `codex`, ...). Omit to sync
        /// every configured source.
        adapter: Option<String>,
        /// One-off source-path override. Bypasses `[sources.<adapter>]` and
        /// does not modify `config.toml`. Requires `<adapter>` to be set.
        #[arg(long)]
        source_dir: Option<PathBuf>,
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Embed the un-embedded message backlog under the registered model.
    /// Idempotent: the PK is `(message_id, model_id, max_embed_tokens)`, so a
    /// re-run picks up where the last one left off.
    Embed {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Registry model id to embed with; defaults to the registry default.
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
        /// Optional cap on messages embedded this run (mostly for benchmarks).
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Run the HTTP+JSON server, including the streamable-HTTP MCP `/mcp` route.
    Serve {
        #[arg(long, env = "POND_HOST", default_value = "127.0.0.1")]
        host: String,
        #[arg(long, env = "POND_PORT", default_value_t = 9797)]
        port: u16,
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
    },
    /// Run the stdio MCP server only. stdout is reserved for JSON-RPC frames;
    /// all diagnostics go to stderr.
    Mcp {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
    },
    /// Run cleanup_old_versions + optimize_indices once over all datasets.
    /// Runs regardless of the `[maintenance].enabled` config flag.
    Maintenance {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<StorageLocation>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Override the cleanup retention window, in days.
        #[arg(long)]
        older_than_days: Option<u64>,
        /// Run optimize_indices only; skip cleanup_old_versions.
        #[arg(long)]
        skip_cleanup: bool,
        /// Run cleanup_old_versions only; skip optimize_indices.
        #[arg(long)]
        skip_optimize: bool,
    },
    /// Inspect configuration.
    Config {
        /// Print the fully-annotated config.toml schema.
        #[arg(long)]
        print_schema: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Status { data_dir } => {
            let store = Store::open(resolve_data_dir(data_dir)).await?;
            let (sessions, messages, parts, embeddings) = store.row_counts().await?;
            output(&format!(
                "sessions={sessions} messages={messages} parts={parts} embeddings={embeddings}"
            ))?;
        }
        Command::Sync {
            adapter,
            source_dir,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config_file = config_path(config, &data_dir);
            let loaded = Config::load(&config_file)?;
            let store = Store::open(&data_dir).await?;
            let sources =
                resolve_sync_sources(&loaded, &config_file, adapter.as_deref(), source_dir)?;
            for (name, config) in sources {
                let summary = sync_one(&store, &name, config).await?;
                output(&format!(
                    "sync {name}: accepted={} inserted={} matched={} errors={}",
                    summary.accepted(),
                    summary.inserted,
                    summary.matched,
                    summary.errors,
                ))?;
            }
            store.ensure_indices().await?;
        }
        Command::Embed {
            data_dir,
            config,
            model,
            namespace,
            limit,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Store::open(&data_dir).await?;
            let embedder = Qwen3Embedder::load(&model)?;
            // `indicatif` auto-detects tty and degrades to log-line output in
            // CI / non-tty contexts, so this is safe to always wire.
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::with_template("{spinner:.green} embed: {msg} ({elapsed_precise})")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            bar.enable_steady_tick(Duration::from_millis(120));
            let bar_for_callback = bar.clone();
            let mut worker = EmbedWorker::new(&store, &embedder, &model)?.with_progress(
                move |progress: BatchProgress| {
                    bar_for_callback.set_message(format!(
                        "batches={} messages={} (+{} this batch)",
                        progress.total_batches, progress.total_messages, progress.batch_messages,
                    ));
                },
            );
            if let Some(limit) = limit {
                worker = worker.with_limit(limit);
            }
            let summary = worker.run().await?;
            store.ensure_embedding_indices(&model).await?;
            bar.finish_with_message(format!(
                "done: batches={} messages={}",
                summary.batches, summary.messages
            ));
            output(&format!(
                "embed: model={} batches={} messages={}",
                model.id, summary.batches, summary.messages,
            ))?;
        }
        Command::Serve {
            host,
            port,
            data_dir,
            config,
            model,
            namespace,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let resolved_model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Arc::new(Store::open(&data_dir).await?);
            // Probe the embeddings table: if there's at least one row for the
            // default model identity, load the model so hybrid search works;
            // otherwise boot without weights and let `pond_search` run
            // FTS-only. Operators opt in via `pond embed`, not a config flag.
            let embedder: Option<Arc<dyn EmbedBackend>> = if store
                .has_embeddings(&resolved_model.id, resolved_model.max_embed_tokens as i32)
                .await?
            {
                Some(Arc::new(Qwen3Embedder::load(&resolved_model)?))
            } else {
                None
            };
            if config.maintenance.enabled {
                spawn_maintenance(Arc::clone(&store), &config.maintenance);
            }
            let state = AppState { store, embedder };
            transport::http::serve(state, host, port).await?;
        }
        Command::Mcp {
            data_dir,
            config,
            model,
            namespace,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let resolved_model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Arc::new(Store::open(&data_dir).await?);
            let embedder: Option<Arc<dyn EmbedBackend>> = if store
                .has_embeddings(&resolved_model.id, resolved_model.max_embed_tokens as i32)
                .await?
            {
                Some(Arc::new(Qwen3Embedder::load(&resolved_model)?))
            } else {
                None
            };
            // `pond mcp` writes only JSON-RPC frames to stdout; the maintenance
            // task is `pond serve`-only, so it is not spawned here.
            transport::mcp::serve_stdio(AppState { store, embedder }).await?;
        }
        Command::Maintenance {
            data_dir,
            config,
            older_than_days,
            skip_cleanup,
            skip_optimize,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let retention_days = older_than_days.unwrap_or(config.maintenance.retention_days);
            let retention =
                chrono::Duration::days(i64::try_from(retention_days).unwrap_or(i64::MAX));
            let store = Store::open(&data_dir).await?;
            let report = store
                .maintenance(retention, skip_cleanup, skip_optimize)
                .await;
            output(&format!(
                "maintenance: versions_removed={} bytes_reclaimed={} tables_optimized={} tables_failed={}",
                report.versions_removed,
                report.bytes_reclaimed,
                report.tables_optimized,
                report.tables_failed,
            ))?;
        }
        Command::Config { print_schema } => {
            if print_schema {
                output(DEFAULT_CONFIG_TOML.trim_end())?;
            } else {
                output("usage: pond config --print-schema")?;
            }
        }
    }

    Ok(())
}

/// Spawn the background maintenance task: `cleanup_old_versions` +
/// `optimize_indices` every `interval_secs` (design.md 3.2.0). The first tick
/// fires immediately, so it is consumed up front - `pond serve` does not run
/// maintenance at boot. Failures are logged at warn and retried next interval;
/// they never crash the server.
fn spawn_maintenance(store: Arc<Store>, config: &MaintenanceConfig) {
    let interval = Duration::from_secs(config.interval_secs);
    let retention =
        chrono::Duration::days(i64::try_from(config.retention_days).unwrap_or(i64::MAX));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let report = store.maintenance(retention, false, false).await;
            tracing::info!(
                versions_removed = report.versions_removed,
                bytes_reclaimed = report.bytes_reclaimed,
                tables_optimized = report.tables_optimized,
                tables_failed = report.tables_failed,
                "background maintenance pass complete",
            );
        }
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("POND_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("warn"));

    fmt().with_env_filter(filter).with_writer(io::stderr).init();
}

#[allow(clippy::print_stdout)]
fn output(message: &str) -> anyhow::Result<()> {
    pond::output::line(message)
}

/// Resolve the data dir from the CLI/env argument, falling back to the XDG
/// location (see [`pond::config::resolve_data_dir`]).
fn resolve_data_dir(explicit: Option<StorageLocation>) -> StorageLocation {
    pond::config::resolve_data_dir(
        explicit,
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

/// The config path: an explicit `--config` wins; otherwise local data dirs
/// default to `<data_dir>/config.toml` and URI-backed data dirs fall back to
/// `$XDG_CONFIG_HOME/pond/config.toml` (the config file is always local -
/// you can't read the bucket without the config that names the bucket).
fn config_path(explicit: Option<PathBuf>, data_dir: &StorageLocation) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    match data_dir.local_path() {
        Some(path) => path.join("config.toml"),
        None => pond::config::default_config_path(
            std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
            std::env::var_os("HOME").map(PathBuf::from),
        ),
    }
}

/// Resolve which (adapter, path) pairs `pond sync` should drive in this run.
///
/// Precedence:
/// 1. `--source-dir <path>` with `<adapter>` set: one-off run, no config writes.
/// 2. `<adapter>` set, `[sources.<adapter>].path` present: use that.
/// 3. `<adapter>` set, no config entry: run per-adapter discovery (one
///    candidate), prompt to add it, persist, then use it.
/// 4. No `<adapter>`, `[sources]` non-empty: sync every entry.
/// 5. No `<adapter>`, empty `[sources]`: discover across every adapter,
///    prompt, persist, then sync the picks.
fn resolve_sync_sources(
    config: &Config,
    config_file: &Path,
    name: Option<&str>,
    source_dir: Option<PathBuf>,
) -> anyhow::Result<Vec<(String, Value)>> {
    if let Some(source_dir) = source_dir {
        let name = name.ok_or_else(|| {
            anyhow::anyhow!("--source-dir requires an explicit <adapter> positional argument")
        })?;
        let known = adapter::known_names();
        if !known.contains(&name) {
            bail!("unknown adapter {name:?}; known: {}", known.join(", "));
        }
        // `--source-dir` is a filesystem-shaped override. Adapters that need
        // a richer config blob can't use this path; they must edit config.toml.
        return Ok(vec![(name.to_owned(), json!({ "path": source_dir }))]);
    }

    if let Some(name) = name {
        let known = adapter::known_names();
        if !known.contains(&name) {
            bail!("unknown adapter {name:?}; known: {}", known.join(", "));
        }
        if let Some(blob) = config.sources.get(name) {
            return Ok(vec![(name.to_owned(), blob.clone())]);
        }
        let candidates = adapter::discover(Some(name));
        let picks = adapter::prompt_and_persist(config_file, &candidates)?;
        return Ok(picks.into_iter().map(|c| (c.name, c.config)).collect());
    }

    if !config.sources.is_empty() {
        return config.resolve_sources(None);
    }
    let candidates = adapter::discover(None);
    let picks = adapter::prompt_and_persist(config_file, &candidates)?;
    Ok(picks.into_iter().map(|c| (c.name, c.config)).collect())
}

/// Run one adapter's ingest pass into `store`. Looks up the factory by name,
/// opens it against the provided config blob, and drains its events stream.
async fn sync_one(store: &Store, name: &str, config: Value) -> anyhow::Result<IngestSummary> {
    let factory = adapter::by_name(name).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown adapter {name:?}; known: {}",
            adapter::known_names().join(", "),
        )
    })?;
    let adapter = factory.open(config)?;
    handlers::ingest_adapter(store, adapter.as_ref()).await
}
