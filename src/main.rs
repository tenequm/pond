use std::{
    io::{self, Write},
    path::PathBuf,
};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use pond::{
    adapter::{Adapter, ClaudeCodeAdapter},
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
    /// Print basic binary status.
    Status {
        #[arg(long, env = "POND_DATA_DIR", default_value = ".pond")]
        data_dir: PathBuf,
    },
    /// Ingest sessions from a source adapter.
    Ingest {
        #[arg(long = "from", value_enum)]
        from: SourceName,
        #[arg(long, env = "POND_DATA_DIR", default_value = ".pond")]
        data_dir: PathBuf,
        #[arg(long)]
        source_dir: Option<PathBuf>,
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
        Command::Status { data_dir } => {
            let store = PondStore::open(data_dir).await?;
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
            let store = PondStore::open(data_dir).await?;
            let adapter = match from {
                SourceName::ClaudeCode => Adapter::ClaudeCode(ClaudeCodeAdapter::new(
                    source_dir.unwrap_or_else(default_claude_code_dir),
                )),
            };
            let summary = adapter.ingest(&store).await?;
            output(&format!(
                "accepted={} inserted={} matched={} errors={}",
                summary.accepted(),
                summary.inserted,
                summary.matched,
                summary.errors
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

fn default_claude_code_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".claude")
        .join("projects")
}
