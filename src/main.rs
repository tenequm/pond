use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use pond::{
    adapter::{Adapter, ClaudeCodeAdapter},
    config::{Config, DEFAULT_CONFIG_TOML, known_model_download_mb},
    embed::{EmbedBackend, EmbedWorker, Qwen3Embedder, load_tokenizer, model_is_cached},
    substrate::PondStore,
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
    /// Ingest sessions from a source adapter.
    Ingest {
        #[arg(long = "from", value_enum)]
        from: SourceName,
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
        #[arg(long)]
        source_dir: Option<PathBuf>,
    },
    /// Embed ingested messages and build the search indexes.
    EmbedWorker {
        #[arg(long, env = "POND_DATA_DIR")]
        data_dir: Option<PathBuf>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Registry model id to embed with; defaults to the registry default.
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
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
            let model = match model {
                Some(id) => config.embeddings.model(&id, &namespace)?,
                None => config.embeddings.default_model(&namespace)?,
            };

            if model_is_cached(&model.fastembed_code) {
                output(&format!(
                    "model:    {} (present, skipping download)",
                    model.id
                ))?;
            } else {
                let size = known_model_download_mb(&model.fastembed_code)
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
        } => {
            let store = PondStore::open(resolve_data_dir(data_dir)).await?;
            let adapter = match from {
                SourceName::ClaudeCode => Adapter::ClaudeCode(ClaudeCodeAdapter::new(
                    source_dir.unwrap_or_else(default_claude_code_dir),
                )),
            };
            let summary = adapter.ingest(&store).await?;
            store.ensure_indices().await?;
            output(&format!(
                "accepted={} inserted={} matched={} errors={}",
                summary.accepted(),
                summary.inserted,
                summary.matched,
                summary.errors
            ))?;
        }
        Command::EmbedWorker {
            data_dir,
            config,
            model,
            namespace,
        } => {
            let data_dir = resolve_data_dir(data_dir);
            let config = Config::load(config_path(config, &data_dir))?;
            let model = match model {
                Some(id) => config.embeddings.model(&id, &namespace)?,
                None => config.embeddings.default_model(&namespace)?,
            };
            let store = PondStore::open(data_dir).await?;
            let embedder = Qwen3Embedder::load(&model)?;
            let tokenizer = load_tokenizer(&model.fastembed_code)?;
            let summary = EmbedWorker::new(&store, &embedder, &tokenizer, &model)?
                .run()
                .await?;
            store.ensure_embedding_indices(&model).await?;
            output(&format!(
                "model={} messages={} chunks={} batches={}",
                model.id, summary.messages, summary.chunks, summary.batches
            ))?;
        }
    }

    Ok(())
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
