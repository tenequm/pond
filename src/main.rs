use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use pond::{
    adapter,
    config::{self, Config, DEFAULT_CONFIG_TOML, MaintenanceConfig},
    embed::{BatchProgress, EmbedBackend, EmbedWorker, Qwen3Embedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{AdapterStats, CorpusStats, RowTotals, StorageSizes, Store},
    transport::{self, AppState},
};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tracing_subscriber::{EnvFilter, fmt};
use url::Url;

/// Adapter clap can call to parse `--data-dir` / `POND_DATA_DIR`. clap's
/// default value parser uses `FromStr`, which `Url` does provide - but
/// `Url::from_str("/srv/pond")` rejects bare paths. This indirection runs
/// every input through Lance's `uri_to_url` (which converts bare paths to
/// `file://...`) so pond accepts both forms transparently.
fn parse_data_dir(input: &str) -> anyhow::Result<Url> {
    config::parse_data_dir(input)
}

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
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Import sessions from one or more configured source adapters. With no
    /// `<adapter>` arg, syncs every entry in `[sources.*]`. With an empty
    /// `[sources]` config, runs adapter discovery: each adapter probes its
    /// canonical install location, the operator picks which to register, and
    /// the picks are written back to `config.toml` before the sync proceeds.
    Sync {
        /// Optional adapter name (`claude-code`, `codex-cli`, ...). Omit to sync
        /// every configured source.
        adapter: Option<String>,
        /// One-off source-path override. Bypasses `[sources.<adapter>]` and
        /// does not modify `config.toml`. Requires `<adapter>` to be set.
        #[arg(long)]
        source_dir: Option<PathBuf>,
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Embed the un-embedded message backlog under the registered model.
    /// Idempotent: the PK is `(message_id, model_id, max_embed_tokens)`, so a
    /// re-run picks up where the last one left off.
    Embed {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
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
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
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
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
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
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
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
    /// Stream every stored session out as JSONL `IngestEvent`s. The output
    /// is byte-identical with what `pond ingest` / `pond_ingest` consume,
    /// so `pond export -o backup.jsonl` plus `pond ingest backup.jsonl`
    /// (or piping into `POST /v1/ingest`) is a portable backup loop -
    /// useful for migration and as a snapshot before risky operations.
    Export {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Filter the export to a single session id. Default: every session.
        #[arg(long)]
        session: Option<String>,
        /// Write JSONL to this path. Default: stdout.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Status { data_dir, config } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let stats = store.corpus_stats().await?;
            let sizes = storage_sizes_for(&data_dir).await?;
            render_status(&stats, sizes.as_ref())?;
        }
        Command::Sync {
            adapter,
            source_dir,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config_file = config_path(config, &data_dir);
            let loaded = Config::load(&config_file)?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let sources =
                resolve_sync_sources(&loaded, &config_file, adapter.as_deref(), source_dir)?;
            for (name, config) in sources {
                let summary = sync_with_progress(&store, &name, config).await?;
                output(&format!(
                    "sync {name}: inserted={} matched={} dropped_events={} \
                     dropped_sessions={} skipped_files={} storage_errors={}",
                    summary.inserted,
                    summary.matched,
                    summary.dropped_events,
                    summary.dropped_sessions,
                    summary.skipped_files,
                    summary.storage_errors,
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
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Store::open_with_options(&data_dir, storage_map(&config)).await?;
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
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let resolved_model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Arc::new(Store::open_with_options(&data_dir, storage_map(&config)).await?);
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
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let resolved_model = config::resolve_model(&config, model.as_deref(), &namespace)?;
            let store = Arc::new(Store::open_with_options(&data_dir, storage_map(&config)).await?);
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
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let retention_days = older_than_days.unwrap_or(config.maintenance.retention_days);
            let retention =
                chrono::Duration::days(i64::try_from(retention_days).unwrap_or(i64::MAX));
            let store = Store::open_with_options(&data_dir, storage_map(&config)).await?;
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
        Command::Export {
            data_dir,
            config,
            session,
            out,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let summary = match out {
                Some(path) => {
                    let file = tokio::fs::File::create(&path)
                        .await
                        .with_context(|| format!("failed to open {}", path.display()))?;
                    let mut writer = tokio::io::BufWriter::new(file);
                    let summary =
                        handlers::pond_export(&store, session.as_deref(), &mut writer).await?;
                    writer.flush().await.context("export: flush")?;
                    summary
                }
                None => {
                    let mut stdout = tokio::io::stdout();
                    handlers::pond_export(&store, session.as_deref(), &mut stdout).await?
                }
            };
            output(&format!(
                "export: sessions={} messages={} parts={}",
                summary.sessions, summary.messages, summary.parts,
            ))?;
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

/// Materialize `Config.storage` (a sorted `BTreeMap` for round-tripping) into
/// the `HashMap<String, String>` Lance accepts. Empty by default; populated
/// from `config.toml [storage]` on installs that need S3/GCS/Azure creds.
fn storage_map(config: &Config) -> std::collections::HashMap<String, String> {
    config
        .storage
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Resolve the data dir from the CLI/env argument, falling back to the XDG
/// location (see [`pond::config::resolve_data_dir`]).
fn resolve_data_dir(explicit: Option<Url>) -> anyhow::Result<Url> {
    pond::config::resolve_data_dir(
        explicit,
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

/// The config path: an explicit `--config` (or `POND_CONFIG`) wins; otherwise
/// `$XDG_CONFIG_HOME/pond/config.toml` (default `~/.config/pond/config.toml`),
/// regardless of where the data dir lives. Config and data are different XDG
/// categories - they always live in different directories, even when both are
/// local.
fn config_path(explicit: Option<PathBuf>, _data_dir: &Url) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    pond::config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
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

/// Run one adapter's ingest pass into `store` with a live progress bar and
/// one greppable log line per finished (or skipped) session.
async fn sync_with_progress(
    store: &Store,
    name: &str,
    config: Value,
) -> anyhow::Result<IngestSummary> {
    let factory = adapter::by_name(name).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown adapter {name:?}; known: {}",
            adapter::known_names().join(", "),
        )
    })?;
    let adapter = factory.open(config)?;

    let bar = ProgressBar::new(0);
    bar.set_style(
        ProgressStyle::with_template(
            "sync {prefix} [{elapsed_precise}] [{bar:24}] {pos}/{len} sessions  {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-"),
    );
    bar.set_prefix(name.to_owned());
    bar.enable_steady_tick(Duration::from_millis(250));

    let mut messages: u64 = 0;
    let mut errors: u64 = 0;
    let mut drops: u64 = 0;
    let started = std::time::Instant::now();
    let bar_ref = &bar;

    let summary = handlers::ingest_adapter(store, adapter.as_ref(), |event| match event {
        SyncEvent::Discovered { total } => {
            if let Some(total) = total {
                bar_ref.set_length(total as u64);
            }
        }
        SyncEvent::SessionDone(outcome) => {
            // Map the four-class status to a compact bar tag + a tracing
            // status label. `dropped` is shown for Partial sessions so the
            // operator can see when one of the bar's "ok-ish" sessions
            // actually has missing events.
            let dropped_count: usize;
            let optional_reason: Option<String>;
            let tag: &str;
            let status_label: &str;
            match &outcome.status {
                SyncStatus::Ok => {
                    tag = "ok  ";
                    status_label = "ok";
                    dropped_count = 0;
                    optional_reason = None;
                }
                SyncStatus::Partial { dropped_events } => {
                    drops += *dropped_events as u64;
                    tag = "part";
                    status_label = "partial";
                    dropped_count = *dropped_events;
                    optional_reason =
                        Some(format!("dropped {dropped_events} event(s) mid-session"));
                }
                SyncStatus::Skipped { reason } => {
                    errors += 1;
                    tag = "skip";
                    status_label = "skipped";
                    dropped_count = 0;
                    optional_reason = Some(reason.clone());
                }
                SyncStatus::Rejected { reason } => {
                    errors += 1;
                    tag = "rej ";
                    status_label = "rejected";
                    dropped_count = 0;
                    optional_reason = Some(reason.clone());
                }
            }
            messages += outcome.messages as u64;
            bar_ref.println(format_sync_line(
                name,
                tag,
                &outcome,
                optional_reason.as_deref(),
            ));
            // The bar's `println` only renders when stderr is a TTY; in
            // piped runs (CI, `pond sync 2>&1 | tee`) operators still need
            // visibility per session. The same data goes out as a `tracing`
            // event on `pond::sync` at INFO. Default verbosity is `warn` so
            // this is silent unless the operator asks via `POND_LOG=info`
            // (or `POND_LOG=pond::sync=info` for sync-only detail).
            match optional_reason.as_deref() {
                None => tracing::info!(
                    target: "pond::sync",
                    adapter = name,
                    status = status_label,
                    project = outcome.project.as_deref().unwrap_or("-"),
                    session = outcome.session_id.as_deref().unwrap_or("-"),
                    messages = outcome.messages,
                    dropped = dropped_count,
                    "session done"
                ),
                Some(reason) => tracing::info!(
                    target: "pond::sync",
                    adapter = name,
                    status = status_label,
                    project = outcome.project.as_deref().unwrap_or("-"),
                    session = outcome.session_id.as_deref().unwrap_or("-"),
                    messages = outcome.messages,
                    dropped = dropped_count,
                    %reason,
                    "session done"
                ),
            }
            bar_ref.inc(1);
            bar_ref.set_message(format_bar_message(
                messages,
                drops,
                errors,
                started.elapsed(),
            ));
        }
    })
    .await?;

    bar.finish_with_message(format!(
        "{} msgs  {} dropped  {} err  done",
        format_thousands(messages),
        format_thousands(drops),
        format_thousands(errors),
    ));
    Ok(summary)
}

/// One greppable per-session log line. Examples:
///
/// ```text
/// [00:04:32] claude-code ok    project=/Users/tenequm/Projects/pond  session=58a96901-4a4f-40be-a3c1-62419ec8c580  msgs=512
/// [00:04:33] claude-code skip  /Users/tenequm/.../58a96901-....jsonl: empty jsonl session
/// ```
fn format_sync_line(
    adapter: &str,
    tag: &str,
    outcome: &SessionOutcome,
    reason: Option<&str>,
) -> String {
    let ts = chrono::Local::now().format("%H:%M:%S");
    let project = outcome.project.as_deref().unwrap_or("-");
    let session = outcome.session_id.as_deref().unwrap_or("-");
    match reason {
        None => format!(
            "[{ts}] {adapter} {tag}  project={project}  session={session}  msgs={}",
            outcome.messages,
        ),
        Some(reason) => format!("[{ts}] {adapter} {tag}  {reason}"),
    }
}

fn format_bar_message(messages: u64, drops: u64, errors: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(0.001);
    let msg_per_sec = (messages as f64) / secs;
    format!(
        "{} msgs  {} dropped  {} err  {:.0} msg/s",
        format_thousands(messages),
        format_thousands(drops),
        format_thousands(errors),
        msg_per_sec,
    )
}

/// Render an integer with thousands separators (`12_345_678` -> `"12,345,678"`).
fn format_thousands(value: u64) -> String {
    let raw = value.to_string();
    let mut out = String::with_capacity(raw.len() + raw.len() / 3);
    for (idx, ch) in raw.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Pretty-print a byte count: `2_589_934_592 -> "2.41 GiB"`. Plain function
/// rather than a humansize-crate add: the spec is small, deterministic, and
/// the dep would land just to format one line of `pond status`.
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if value >= 100.0 {
        format!("{:.0} {}", value, UNITS[unit])
    } else if value >= 10.0 {
        format!("{:.1} {}", value, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

/// Walk the data dir when it's local; for remote data dirs return `None` and
/// note in `pond status` that sizes are unavailable until the S3 backend
/// lands (see plan.md S3 backend stage). The remote `LIST` plumbing is wired
/// alongside the rest of the S3 work, where it can be tested end-to-end.
async fn storage_sizes_for(data_dir: &Url) -> anyhow::Result<Option<StorageSizes>> {
    if let Some(path) = config::local_path(data_dir) {
        let sizes = tokio::task::spawn_blocking(move || StorageSizes::from_local_dir(&path))
            .await
            .context("storage size walk panicked")??;
        Ok(Some(sizes))
    } else {
        Ok(None)
    }
}

/// Render `pond status` as a tree: data-dir + per-table sizes on top, then
/// totals, then one section per adapter in registry order with its projects.
fn render_status(stats: &CorpusStats, sizes: Option<&StorageSizes>) -> anyhow::Result<()> {
    output("pond status")?;
    output(&format!(
        "\u{2514}\u{2500}\u{2500} data-dir: {}",
        stats.data_url
    ))?;
    match sizes {
        Some(sizes) => {
            output(&format!("    ({} on disk)", format_bytes(sizes.total())))?;
            output(&format!(
                "    \u{251C}\u{2500}\u{2500} sessions    {:>10}",
                format_bytes(sizes.sessions),
            ))?;
            output(&format!(
                "    \u{251C}\u{2500}\u{2500} messages    {:>10}",
                format_bytes(sizes.messages),
            ))?;
            output(&format!(
                "    \u{251C}\u{2500}\u{2500} parts       {:>10}",
                format_bytes(sizes.parts),
            ))?;
            output(&format!(
                "    \u{251C}\u{2500}\u{2500} embeddings  {:>10}",
                format_bytes(sizes.embeddings),
            ))?;
            output(&format!(
                "    \u{2514}\u{2500}\u{2500} other       {:>10}",
                format_bytes(sizes.other),
            ))?;
        }
        None => {
            output("    (size on disk unavailable for remote backends; wired with S3 stage)")?;
        }
    }

    let RowTotals {
        sessions,
        messages,
        parts,
        embeddings,
    } = stats.totals;
    output("")?;
    output(&format!(
        "totals: {} sessions  {} messages  {} parts  {} embeddings",
        format_thousands(sessions),
        format_thousands(messages),
        format_thousands(parts),
        format_thousands(embeddings),
    ))?;

    // Render adapters in registry order so the tree matches the discovery
    // picker; adapters present in the data but not in the registry append at
    // the bottom (defensive: catches deleted adapters whose data is still on
    // disk).
    let mut by_name: std::collections::HashMap<&str, &AdapterStats> = stats
        .adapters
        .iter()
        .map(|stat| (stat.adapter.as_str(), stat))
        .collect();
    for factory in adapter::registry() {
        if let Some(stat) = by_name.remove(factory.name()) {
            render_adapter_block(stat)?;
        }
    }
    for stat in by_name.values() {
        render_adapter_block(stat)?;
    }
    Ok(())
}

fn render_adapter_block(stat: &AdapterStats) -> anyhow::Result<()> {
    output("")?;
    output(&format!(
        "{}  ({} sessions, {} messages, {} projects)",
        stat.adapter,
        format_thousands(stat.sessions),
        format_thousands(stat.messages),
        format_thousands(stat.projects.len() as u64),
    ))?;
    let last_index = stat.projects.len().saturating_sub(1);
    for (idx, project) in stat.projects.iter().enumerate() {
        let glyph = if idx == last_index {
            "\u{2514}\u{2500}\u{2500}"
        } else {
            "\u{251C}\u{2500}\u{2500}"
        };
        let label = project.project.as_deref().unwrap_or("(no project)");
        output(&format!(
            "{glyph} {label:<60}  {:>7} sessions   {:>9} msgs",
            format_thousands(project.sessions),
            format_thousands(project.messages),
        ))?;
    }
    Ok(())
}
