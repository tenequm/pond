use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use pond::{
    adapter::{Adapter, ClaudeCodeAdapter},
    config::{Config, DEFAULT_CONFIG_TOML, MaintenanceConfig, known_model_download_mb},
    embed::{EmbedBackend, EmbedWorker, Qwen3Embedder, model_is_cached},
    substrate::PondStore,
    transport::{self, AppState},
};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(name = "pond", version, about = "Session storage and retrieval")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Resolve the data dir, write a default config, fetch and verify the model.
    Setup {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Registry model id to set up; defaults to the registry default.
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
        /// Skip the download confirmation prompt (scripts, CI, package hooks).
        #[arg(long)]
        yes: bool,
    },
    /// Print basic binary status.
    Status {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
    },
    /// Ingest sessions from a source adapter: parse, store, embed, and index.
    Ingest {
        #[arg(long = "from", value_enum)]
        from: SourceName,
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        source_dir: Option<PathBuf>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Registry model id to embed with; defaults to the registry default.
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
    },
    /// Run the HTTP+JSON server, including the streamable-HTTP MCP `/mcp` route.
    Serve {
        #[arg(long, env = "POND_HOST", default_value = "127.0.0.1")]
        host: String,
        #[arg(long, env = "POND_PORT", default_value_t = 9797)]
        port: u16,
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
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
        data_dir: Option<PathBuf>,
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
        data_dir: Option<PathBuf>,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SourceName {
    ClaudeCode,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Setup {
            data_dir,
            config,
            model,
            namespace,
            yes,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            tokio::fs::create_dir_all(&data_dir)
                .await
                .with_context(|| format!("failed to create data dir {}", data_dir.display()))?;
            output(&format!("data dir: {}", data_dir.display()))?;

            let config_path = config_path(config, &data_dir);
            if config_path.exists() {
                output(&format!("config:   {} (exists)", config_path.display()))?;
            } else {
                tokio::fs::write(&config_path, DEFAULT_CONFIG_TOML)
                    .await
                    .with_context(|| format!("failed to write config {}", config_path.display()))?;
                output(&format!("config:   {} (created)", config_path.display()))?;
            }

            let config = Config::load(&config_path)?;
            let model = resolve_model(&config, model.as_deref(), &namespace)?;

            if model_is_cached(model.load_repo()) {
                output(&format!(
                    "model:    {} (present, skipping download)",
                    model.id
                ))?;
            } else {
                let size = known_model_download_mb(model.load_repo())
                    .map_or_else(|| "unknown size".to_owned(), |mb| format!("~{mb} MB"));
                output(&format!(
                    "model:    {} (not cached - downloading {size} to the HuggingFace cache)",
                    model.id
                ))?;
                if !yes && !confirm("proceed with download?")? {
                    output("setup aborted")?;
                    return Ok(());
                }
            }

            // Load on the selected device and embed a probe: this verifies the
            // weights, the output dimension, and the device path in one shot.
            let embedder = Qwen3Embedder::load(&model)?;
            let probe = embedder.embed(&["pond setup readiness probe".to_owned()])?;
            let probe_dim = probe.first().map_or(0, Vec::len);
            if probe_dim != model.dim as usize {
                anyhow::bail!(
                    "model {} produced dim {probe_dim}, registry declares {}",
                    model.id,
                    model.dim,
                );
            }
            output(&format!("device:   {}", embedder.device()))?;
            output(&format!(
                "ready:    model {} verified at {} dim",
                model.id, model.dim
            ))?;
            output("next:     pond ingest --from claude-code")?;
        }
        Command::Status { data_dir } => {
            let store = PondStore::open(resolve_data_dir(data_dir)).await?;
            let (sessions, messages, parts, embeddings) = store.row_counts().await?;
            output(&format!(
                "sessions={sessions} messages={messages} parts={parts} embeddings={embeddings}"
            ))?;
        }
        Command::Ingest {
            from,
            data_dir,
            source_dir,
            config,
            model,
            namespace,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let model = resolve_model(&config, model.as_deref(), &namespace)?;
            let store = PondStore::open(&data_dir).await?;
            let adapter = match from {
                SourceName::ClaudeCode => Adapter::ClaudeCode(ClaudeCodeAdapter::new(
                    source_dir.unwrap_or_else(default_claude_code_dir),
                )),
            };
            let ingest = adapter.ingest(&store).await?;
            store.ensure_indices().await?;

            // Embedding is part of ingest: a message is either fully in the
            // system - parsed, stored, indexed, searchable - or not in at all.
            let embedder = Qwen3Embedder::load(&model)?;
            let embed = EmbedWorker::new(&store, &embedder, &model)?.run().await?;
            store.ensure_embedding_indices(&model).await?;

            output(&format!(
                "accepted={} inserted={} matched={} errors={} embedded={}",
                ingest.accepted(),
                ingest.inserted,
                ingest.matched,
                ingest.errors,
                embed.messages,
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
            let model = resolve_model(&config, model.as_deref(), &namespace)?;
            // Load the embedding model at boot: it is required to embed search
            // queries, so a missing or broken model fails loudly here, not on
            // the first search.
            let embedder: Arc<dyn EmbedBackend> = Arc::new(Qwen3Embedder::load(&model)?);
            let store = Arc::new(PondStore::open(&data_dir).await?);
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
            let model = resolve_model(&config, model.as_deref(), &namespace)?;
            let embedder: Arc<dyn EmbedBackend> = Arc::new(Qwen3Embedder::load(&model)?);
            let store = Arc::new(PondStore::open(&data_dir).await?);
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
            let store = PondStore::open(&data_dir).await?;
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
fn spawn_maintenance(store: Arc<PondStore>, config: &MaintenanceConfig) {
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

/// Resolve the embedding model from config: an explicit `--model` id, otherwise
/// the registry default, with any namespace overrides applied.
fn resolve_model(
    config: &Config,
    model: Option<&str>,
    namespace: &str,
) -> anyhow::Result<pond::config::EmbeddingModel> {
    match model {
        Some(id) => config.embeddings.model(id, namespace),
        None => config.embeddings.default_model(namespace),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_env("POND_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("warn"));

    fmt().with_env_filter(filter).with_writer(io::stderr).init();
}

#[allow(clippy::print_stdout)]
fn output(message: &str) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{message}").context("failed to write command output")
}

/// Prompt on stdout for a yes/no answer; anything but an explicit yes is no.
#[allow(clippy::print_stdout)]
fn confirm(prompt: &str) -> anyhow::Result<bool> {
    print!("{prompt} [y/N] ");
    io::stdout().flush().context("failed to flush prompt")?;
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .context("failed to read confirmation")?;
    Ok(matches!(answer.trim(), "y" | "Y" | "yes" | "Yes"))
}

/// Resolve the data dir from the CLI/env argument, falling back to the XDG
/// location (see [`pond::config::resolve_data_dir`]).
fn resolve_data_dir(explicit: Option<PathBuf>) -> PathBuf {
    pond::config::resolve_data_dir(
        explicit,
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

/// The config path: an explicit `--config` wins, otherwise `<data_dir>/config.toml`.
fn config_path(explicit: Option<PathBuf>, data_dir: &Path) -> PathBuf {
    explicit.unwrap_or_else(|| data_dir.join("config.toml"))
}

fn default_claude_code_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".claude")
        .join("projects")
}
