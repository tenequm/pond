use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Utc};

use anyhow::{Context, bail};
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use comfy_table::{
    Attribute, Cell, CellAlignment, ColumnConstraint, ContentArrangement, Table, presets::NOTHING,
};
use indicatif::{ProgressBar, ProgressStyle};
use pond::{
    PROTOCOL_VERSION, adapter,
    config::{self, Config, DEFAULT_CONFIG_TOML},
    embed::{BatchProgress, E5Embedder, EmbedSummary, EmbedWorker, LazyEmbedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{
        AdapterStats, CleanupConfig, CorpusStats, EmbeddingProgress, LanceArchiveCounts,
        LanceArchiveExport, LanceArchiveImport, OptimizeOutcome, RowTotals, Store,
    },
    substrate::{IndexStatus, OptimizeEvent, OptimizeProgressFn, PhaseOutcome, TableSizes},
    transport::{self, AppState},
    wire::{
        self, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse, GetResult, Group, Hit, Message,
        Part, PartKind, ProjectFilter, SearchEnvelope, SearchFilters, SearchModeWire,
        SearchRequest, SearchResponse, SearchResultBody,
    },
};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct PondArchiveManifest {
    archive_version: u32,
    pond_version: String,
    protocol_version: u16,
    created_at: DateTime<Utc>,
    rows: LanceArchiveCounts,
    source_versions: pond::sessions::LanceArchiveVersions,
    embedding_model: String,
    embedding_dim: usize,
}

/// CLI surface for `pond search --mode`. Maps 1:1 to `SearchModeWire`; kept
/// separate so the clap derive lives next to the rest of the CLI types.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliSearchMode {
    Fts,
    Vector,
    Hybrid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum SyncStage {
    Import,
    Embed,
    UpdateIndexes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ServeTransport {
    Http,
    Stdio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ExportFormat {
    Pond,
    Jsonl,
}

impl From<CliSearchMode> for SearchModeWire {
    fn from(mode: CliSearchMode) -> Self {
        match mode {
            CliSearchMode::Fts => SearchModeWire::Fts,
            CliSearchMode::Vector => SearchModeWire::Vector,
            CliSearchMode::Hybrid => SearchModeWire::Hybrid,
        }
    }
}
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tracing_subscriber::{EnvFilter, fmt};
use url::Url;

/// `SkipOracle` backed by a pre-loaded `Store::last_message_timestamps` map.
struct StoredWatermarks {
    map: HashMap<String, DateTime<Utc>>,
}

impl StoredWatermarks {
    fn new(map: HashMap<String, DateTime<Utc>>) -> Self {
        Self { map }
    }
}

impl pond::adapter::SkipOracle for StoredWatermarks {
    fn last_ingested_at(&self, session_id: &str) -> Option<DateTime<Utc>> {
        self.map.get(session_id).copied()
    }
}

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
    /// Show pond health, data, and source status.
    Status {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Include detailed per-index and per-project rows.
        #[arg(long)]
        verbose: bool,
        /// Show one section per `source_agent` (including sub-agents like
        /// `claude-code/general-purpose`). Default rolls sessions up to the
        /// main agent only.
        #[arg(long)]
        include_subagents: bool,
    },
    /// Make pond current: import, embed, then update indexes.
    Sync {
        /// Optional adapter name (`claude-code`, `codex-cli`, ...).
        adapter: Option<String>,
        /// One-off source-path override. Bypasses `[sources.<adapter>]` and
        /// does not modify `config.toml`. Requires `<adapter>` to be set.
        #[arg(long)]
        source_dir: Option<PathBuf>,
        /// Restore a `.pond` archive. Alias: `pond import <archive>`.
        #[arg(long)]
        import_from: Option<PathBuf>,
        /// Run exactly one stage: import, embed, or update-indexes.
        #[arg(long, value_enum)]
        only: Option<SyncStage>,
        /// Skip a stage. Can be passed multiple times.
        #[arg(long, value_enum)]
        skip: Vec<SyncStage>,
        /// Re-embed stale rows after an embedding model change.
        #[arg(long)]
        force_embed: bool,
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Embed the backlog of un-embedded messages (spec.md#search). Idempotent:
    /// the backlog is every message with a null `vector`, so a re-run picks up
    /// exactly where the last one stopped. A model swap (rows embedded under
    /// a different model id) requires `--force`, which clears those rows and
    /// drops the IVF_PQ before the new vectors land.
    #[command(hide = true)]
    Embed {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Optional cap on messages embedded this run (mostly for benchmarks).
        #[arg(long)]
        limit: Option<usize>,
        /// Allow re-embedding rows whose stored `embedding_model` does not
        /// match the configured model. Without this flag, such rows abort the
        /// run with a typed error so a model swap is never silent.
        #[arg(long)]
        force: bool,
    },
    /// Run the server.
    Serve {
        #[arg(long, value_enum, default_value_t = ServeTransport::Http)]
        transport: ServeTransport,
        #[arg(long, env = "POND_HOST", default_value = "127.0.0.1")]
        host: String,
        #[arg(long, env = "POND_PORT", default_value_t = 9797)]
        port: u16,
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Alias for `pond serve --transport stdio`.
    Mcp {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Inspect configuration.
    #[command(hide = true)]
    Config {
        /// Print the fully-annotated config.toml schema.
        #[arg(long)]
        print_schema: bool,
    },
    /// Search stored messages.
    Search {
        /// Free-text query. Semantic concepts work best; project names belong
        /// in `--project`.
        query: String,
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        #[arg(long, default_value = "local")]
        namespace: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Operator-only retrieval mode override. Production callers should
        /// omit this and let the server pick (hybrid when embeddings exist,
        /// FTS-only otherwise); benchmark and ablation harnesses use it to
        /// force one arm against the same corpus.
        #[arg(long, value_enum)]
        mode: Option<CliSearchMode>,
        /// Disable the recency boost. The server defaults to enabled (matches
        /// the MCP/HTTP surface).
        #[arg(long)]
        no_boost_recent: bool,
        /// Collapse to one row per session, keeping the best-scoring message.
        #[arg(long)]
        group_by_conversation: bool,
        /// Include each hit's full indexed text instead of just the match-
        /// windowed snippet. Default off - the CLI shows snippets for the
        /// human reading the table; pass `--full` for one-shot scripts that
        /// want the body in the same response.
        #[arg(long)]
        full: bool,
        /// Substring match by default (`--project pond` -> contains "pond"). Prefix
        /// with `re:` for regex (`--project 're:^/Users/.*/x402'`); `lit:` escapes
        /// a literal value that would otherwise be parsed as a prefix.
        #[arg(long, value_parser = parse_project_filter)]
        project: Option<ProjectFilter>,
        #[arg(long, value_name = "ID")]
        session_id: Option<String>,
        #[arg(long)]
        source_agent: Option<String>,
        /// ISO date (YYYY-MM-DD) lower bound, inclusive.
        #[arg(long)]
        from_date: Option<String>,
        /// ISO date (YYYY-MM-DD) upper bound, inclusive.
        #[arg(long)]
        to_date: Option<String>,
        /// Restrict to a single role (`user` | `assistant` | `system` | `tool`).
        #[arg(long)]
        role: Option<String>,
        /// Server-side score threshold; hits below this are dropped.
        #[arg(long, default_value_t = 0.0)]
        min_score: f64,
        /// Print Lance query plans instead of search results.
        #[arg(long)]
        explain: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Pretty)]
        format: OutputFormat,
    },
    /// Inspect and maintain Lance indexes.
    #[command(hide = true)]
    Index {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        #[command(subcommand)]
        command: IndexCommand,
    },
    /// Fetch a session or message.
    #[command(group(ArgGroup::new("get_selector")
        .required(true)
        .args(["session_id", "message_id"])))]
    Get {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        #[arg(long, default_value = "local")]
        namespace: String,
        /// Fetch an entire session by id. Mutually exclusive with `--message-id`.
        #[arg(long, value_name = "ID")]
        session_id: Option<String>,
        /// Fetch a single message by id. Mutually exclusive with `--session-id`.
        #[arg(long, value_name = "ID")]
        message_id: Option<String>,
        /// Truncate session output at this message id. Requires `--session-id`.
        #[arg(
            long,
            value_name = "ID",
            requires = "session_id",
            conflicts_with = "message_id"
        )]
        up_to: Option<String>,
        /// For `--message-id` mode: include this many surrounding messages
        /// from the same session. Ignored in session mode.
        #[arg(long, default_value_t = 0)]
        context_depth: usize,
        /// Cap on returned messages.
        #[arg(long, default_value_t = 100)]
        max_messages: usize,
        /// Include reasoning parts in the response (server-side filter).
        #[arg(long)]
        include_thinking: bool,
        /// Include tool-result parts in the response (server-side filter).
        #[arg(long)]
        include_tool_results: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Pretty)]
        format: OutputFormat,
    },
    /// Export a compact restorable `.pond` archive.
    Export {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Output path. Default: `pond-export.pond` for archive format, stdout for JSONL.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
        #[arg(long, value_enum, default_value_t = ExportFormat::Pond)]
        format: ExportFormat,
    },
    /// Restore a `.pond` archive.
    Import {
        archive: PathBuf,
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum IndexCommand {
    Status,
    Optimize {
        #[arg(long)]
        wait: bool,
        /// Override the manifest-retention window for this run.
        /// Accepts Ns/Nm/Nh/Nd (default: 1d). Implies aggressive deletion
        /// (delete_unverified=true): reclaims files Lance's 7-day in-progress
        /// guard would otherwise protect. Unsafe under concurrent writers;
        /// see --vacuum for a one-shot full reclaim.
        #[arg(long, value_parser = parse_retention_arg)]
        cleanup_older_than: Option<chrono::Duration>,
        /// Reclaim every orphan immediately. Sugar for
        /// `--cleanup-older-than 0s`. Same safety caveat applies.
        #[arg(long, conflicts_with = "cleanup_older_than")]
        vacuum: bool,
        /// Skip the confirmation prompt when aggressive cleanup is enabled.
        /// Required for non-interactive use of --vacuum / --cleanup-older-than.
        #[arg(long, short = 'y')]
        yes: bool,
    },
    Rebuild {
        intent: Option<String>,
    },
}

/// `clap` value-parser for `--cleanup-older-than`. Accepts `Ns`/`Nm`/`Nh`/`Nd`
/// (or bare `N` interpreted as seconds). Mirrors LanceDB's docs without taking
/// a humantime dependency.
fn parse_retention_arg(input: &str) -> Result<chrono::Duration, String> {
    let trimmed = input.trim();
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (num, unit) = trimmed.split_at(split_at);
    let n: i64 = num
        .parse()
        .map_err(|_| format!("invalid duration {input:?} (expected like `1h`, `30m`, `0s`)"))?;
    match unit {
        "s" | "" => Ok(chrono::Duration::seconds(n)),
        "m" => Ok(chrono::Duration::minutes(n)),
        "h" => Ok(chrono::Duration::hours(n)),
        "d" => Ok(chrono::Duration::days(n)),
        _ => Err(format!(
            "unknown duration unit {unit:?} in {input:?} (use s/m/h/d)"
        )),
    }
}

fn format_retention(d: chrono::Duration) -> String {
    let s = d.num_seconds();
    if s == 0 {
        return "0s".into();
    }
    if s.rem_euclid(86_400) == 0 {
        return format!("{}d", s / 86_400);
    }
    if s.rem_euclid(3_600) == 0 {
        return format!("{}h", s / 3_600);
    }
    if s.rem_euclid(60) == 0 {
        return format!("{}m", s / 60);
    }
    format!("{s}s")
}

/// Resolve `--cleanup-older-than` / `--vacuum` / `--yes` into a `CleanupConfig`
/// override (or `None` to use pond's safe default). Any explicit retention flag
/// implies `delete_unverified=true` to bypass Lance's 7-day in-progress guard;
/// that bypass is unsafe under concurrent writers, so a confirmation prompt
/// fires unless `--yes` is set (non-interactive callers must pass `--yes`).
fn resolve_cleanup_config(
    cleanup_older_than: Option<chrono::Duration>,
    vacuum: bool,
    yes: bool,
) -> anyhow::Result<Option<CleanupConfig>> {
    let aggressive = vacuum || cleanup_older_than.is_some();
    if !aggressive {
        return Ok(None);
    }
    let older_than = if vacuum {
        chrono::Duration::zero()
    } else {
        cleanup_older_than.unwrap_or_else(chrono::Duration::zero)
    };
    let cfg = CleanupConfig {
        older_than,
        delete_unverified: true,
    };
    let warning = format!(
        "warning: cleanup_older_than={} with delete_unverified=true.\n\
         warning: this deletes orphan files newer than Lance's 7-day in-progress guard.\n\
         warning: ensure no other pond writer (serve, sync, embed) is active on this data dir.",
        format_retention(cfg.older_than),
    );
    eprintln!("{}", pond::output::paint(&warning, pond::output::yellow()));
    if yes {
        return Ok(Some(cfg));
    }
    if !io::stdin().is_terminal() {
        anyhow::bail!(
            "refusing to run aggressive cleanup non-interactively; pass --yes to confirm"
        );
    }
    let proceed = dialoguer::Confirm::new()
        .with_prompt("Continue?")
        .default(false)
        .interact()
        .context("failed to read confirmation")?;
    if !proceed {
        anyhow::bail!("aborted by operator");
    }
    Ok(Some(cfg))
}

/// Parse `--project <value>` into a `ProjectFilter`. `re:<pattern>` selects
/// regex; `lit:<text>` escapes a literal value that would otherwise be
/// parsed as a prefix; everything else is a substring match.
fn parse_project_filter(input: &str) -> Result<ProjectFilter, String> {
    if let Some(pattern) = input.strip_prefix("re:") {
        Ok(ProjectFilter::Regex(pattern.to_owned()))
    } else if let Some(literal) = input.strip_prefix("lit:") {
        Ok(ProjectFilter::Contains(literal.to_owned()))
    } else {
        Ok(ProjectFilter::Contains(input.to_owned()))
    }
}

/// Output mode for `pond search` and `pond get`. Pretty is the human default;
/// Json emits the wire envelope verbatim (including error envelopes), so
/// scripts can `--format json | jq ...` against the same surface as the HTTP
/// transport.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Pretty,
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Status {
            data_dir,
            config,
            verbose,
            include_subagents,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let sizes = store.table_sizes().await?;
            render_status_header(&data_dir, &sizes)?;
            output(&format!(
                "{} checking corpus, embeddings, indexes...",
                pond::output::paint("status:", pond::output::dim()),
            ))?;
            // Sample is bounded so this remains O(sample) and `pond status`
            // stays sub-second on a million-message corpus.
            let (stats, index_status, embedding, scripts) = tokio::try_join!(
                store.corpus_stats(include_subagents),
                store.index_status(),
                store.embedding_progress(),
                store.text_script_histogram(2000),
            )?;
            render_status_checks(&stats, &index_status, embedding, &scripts, verbose)?;
        }
        Command::Sync {
            adapter,
            source_dir,
            import_from,
            only,
            skip,
            force_embed,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config_file = config_path(config, &data_dir);
            let loaded = Config::load(&config_file)?;
            let store = open_store_with_spinner(&data_dir, storage_map(&loaded)).await?;
            let stages = SyncStages::resolve(only, &skip)?;
            if import_from.is_some() && !stages.import {
                bail!(
                    "--import-from requires the import stage; remove `--skip import` or use `--only import`"
                );
            }
            let mut summary = SyncRunSummary::default();
            if stages.import {
                if let Some(path) = import_from {
                    summary.archive_import = Some(import_pond_archive(&store, &path).await?);
                } else {
                    summary.ingest = Some(
                        run_import_stage(&store, &loaded, &config_file, adapter, source_dir)
                            .await?,
                    );
                }
            }
            if stages.embed {
                summary.embed = Some(run_embed_stage(&store, force_embed).await?);
            }
            if stages.update_indexes {
                summary.indexes = Some(run_update_indexes_stage(&store).await?);
            }
            render_sync_summary(&summary)?;
        }
        Command::Embed {
            data_dir,
            config,
            limit,
            force,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store = open_store_with_spinner(&data_dir, storage_map(&config)).await?;
            let summary = run_embed_stage_with_limit(&store, force, limit, "--force").await?;
            if !summary.summary.cancelled && summary.summary.messages > 0 {
                let outcome = run_update_indexes_stage(&store).await?;
                if outcome.any_indices_failed() {
                    std::process::exit(1);
                }
            }
        }
        Command::Serve {
            transport,
            host,
            port,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store = Arc::new(open_store_with_spinner(&data_dir, storage_map(&config)).await?);
            let embedder = Arc::new(LazyEmbedder::new());
            let state = AppState {
                store,
                embedder,
                search: config.search.clone(),
            };
            match transport {
                ServeTransport::Http => {
                    output(&format!("serve: http listening on http://{host}:{port}"))?;
                    transport::http::serve(state, host, port).await?;
                }
                ServeTransport::Stdio => {
                    eprintln!("serve: stdio MCP ready; stdout is reserved for JSON-RPC");
                    transport::mcp::serve_stdio(state).await?;
                }
            }
        }
        Command::Mcp { data_dir, config } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store = Arc::new(open_store_with_spinner(&data_dir, storage_map(&config)).await?);
            // Lazy: idle `pond mcp` instances in every Claude Code session
            // stay light. The model load only happens once per process on the
            // first `pond_search` tool call that needs hybrid retrieval.
            let embedder = Arc::new(LazyEmbedder::new());
            transport::mcp::serve_stdio(AppState {
                store,
                embedder,
                search: config.search.clone(),
            })
            .await?;
        }
        Command::Search {
            query,
            data_dir,
            config,
            namespace,
            limit,
            mode,
            no_boost_recent,
            group_by_conversation,
            full,
            project,
            session_id,
            source_agent,
            from_date,
            to_date,
            role,
            min_score,
            explain,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            // Same `LazyEmbedder` pattern as the daemons: a single one-shot
            // `pond search "foo"` against an FTS-only corpus never loads the
            // model. The cost is one extra `.await` on first call.
            let embedder = LazyEmbedder::new();
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                query,
                mode_override: mode.map(SearchModeWire::from),
                similar_to: None,
                filters: SearchFilters {
                    project,
                    session_id,
                    source_agent,
                    from_date,
                    to_date,
                    role,
                    min_score,
                },
                boost_recent: !no_boost_recent,
                group_by_conversation,
                full,
                limit,
            };
            if explain {
                let plans = explain_search(&store, &embedder, &request, &loaded.search).await?;
                output(&plans)?;
                return Ok(());
            }
            let envelope = handlers::pond_search(&store, &embedder, request, &loaded.search).await;
            if !render_search_envelope(format, &envelope)? {
                std::process::exit(1);
            }
        }
        Command::Index {
            data_dir,
            config,
            command,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            match command {
                IndexCommand::Status => {
                    let statuses = store.index_status().await?;
                    render_index_status(&statuses)?;
                }
                IndexCommand::Optimize {
                    wait,
                    cleanup_older_than,
                    vacuum,
                    yes,
                } => {
                    let cleanup = resolve_cleanup_config(cleanup_older_than, vacuum, yes)?;
                    if let Some(c) = cleanup {
                        output(&format!(
                            "{}  cleanup_older_than={}{}",
                            pond::output::paint("optimize:", pond::output::dim()),
                            format_retention(c.older_than),
                            if c.delete_unverified {
                                " (aggressive)"
                            } else {
                                ""
                            },
                        ))?;
                    }
                    let (progress, bar) = optimize_progress_bar();
                    let outcome = store.optimize_indices(Some(progress), cleanup).await?;
                    bar.finish_and_clear();
                    render_optimize_outcome(&outcome)?;
                    if wait {
                        wait_for_index_catchup(&store).await?;
                    }
                    let statuses = store.index_status().await?;
                    render_index_status(&statuses)?;
                    if outcome.any_indices_failed() {
                        std::process::exit(1);
                    }
                }
                IndexCommand::Rebuild { intent } => {
                    store.rebuild_indices(intent.as_deref()).await?;
                    let statuses = store.index_status().await?;
                    render_index_status(&statuses)?;
                }
            }
        }
        Command::Get {
            data_dir,
            config,
            namespace,
            session_id,
            message_id,
            up_to,
            context_depth,
            max_messages,
            include_thinking,
            include_tool_results,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let request = GetRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                session_id,
                message_id,
                up_to,
                context_depth,
                max_messages,
                include_thinking,
                include_tool_results,
            };
            let envelope = handlers::pond_get(&store, request).await;
            if !render_get_envelope(format, &envelope)? {
                std::process::exit(1);
            }
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
            out,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            match format {
                ExportFormat::Pond => {
                    let path = out.unwrap_or_else(|| PathBuf::from("pond-export.pond"));
                    let summary = export_pond_archive(&store, &path).await?;
                    output(&format!(
                        "{} {}  sessions={} messages={} parts={}",
                        pond::output::paint("export:", pond::output::dim()),
                        path.display(),
                        summary.rows.sessions,
                        summary.rows.messages,
                        summary.rows.parts,
                    ))?;
                }
                ExportFormat::Jsonl => {
                    let summary = match out {
                        Some(path) => {
                            let file = tokio::fs::File::create(&path)
                                .await
                                .with_context(|| format!("failed to open {}", path.display()))?;
                            let mut writer = tokio::io::BufWriter::new(file);
                            let summary = handlers::pond_export(&store, None, &mut writer).await?;
                            writer.flush().await.context("export: flush")?;
                            summary
                        }
                        None => {
                            let mut stdout = tokio::io::stdout();
                            handlers::pond_export(&store, None, &mut stdout).await?
                        }
                    };
                    output(&format!(
                        "{} jsonl sessions={} messages={} parts={}",
                        pond::output::paint("export:", pond::output::dim()),
                        summary.sessions,
                        summary.messages,
                        summary.parts,
                    ))?;
                }
            }
        }
        Command::Import {
            archive,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let summary = import_pond_archive(&store, &archive).await?;
            output(&format!(
                "{} sessions={} messages={} parts={} inserted_sessions={} inserted_messages={} inserted_parts={}",
                pond::output::paint("import:", pond::output::dim()),
                summary.rows.sessions,
                summary.rows.messages,
                summary.rows.parts,
                summary.inserted.sessions,
                summary.inserted.messages,
                summary.inserted.parts,
            ))?;
            output(&format!(
                "{} run `pond sync --only update-indexes` to rebuild search indexes",
                pond::output::paint("hint", pond::output::dim()),
            ))?;
        }
    }

    Ok(())
}

fn init_tracing() {
    // Lance's IVF_PQ builder warns once per empty centroid during merge
    // (rust/lance/src/index/vector/builder.rs: "partition N is empty, skipping").
    // It already handles the case - records a zero-sized partition and continues -
    // so the warning is benign log noise during index maintenance.
    // POND_LOG / RUST_LOG still override this default.
    let filter = EnvFilter::try_from_env("POND_LOG")
        .or_else(|_| EnvFilter::try_from_default_env())
        .unwrap_or_else(|_| EnvFilter::new("warn,lance::index::vector::builder=error"));

    fmt().with_env_filter(filter).with_writer(io::stderr).init();
}

#[allow(clippy::print_stdout)]
fn output(message: &str) -> anyhow::Result<()> {
    pond::output::line(message)
}

/// Open the store with an indicatif spinner ticking while
/// [`Store::open_with_options`] runs. Open itself is cheap; the spinner only
/// matters for visual consistency with other long-running commands.
async fn open_store_with_spinner(
    location: &Url,
    storage: HashMap<String, String>,
) -> anyhow::Result<Store> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.green} opening pond store... [{elapsed_precise}]")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.enable_steady_tick(Duration::from_millis(120));
    let result = Store::open_with_options(location, storage).await;
    spinner.finish_and_clear();
    result
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

#[derive(Debug, Default)]
struct SyncStages {
    import: bool,
    embed: bool,
    update_indexes: bool,
}

impl SyncStages {
    fn resolve(only: Option<SyncStage>, skip: &[SyncStage]) -> anyhow::Result<Self> {
        let mut stages = match only {
            Some(SyncStage::Import) => Self {
                import: true,
                ..Self::default()
            },
            Some(SyncStage::Embed) => Self {
                embed: true,
                ..Self::default()
            },
            Some(SyncStage::UpdateIndexes) => Self {
                update_indexes: true,
                ..Self::default()
            },
            None => Self {
                import: true,
                embed: true,
                update_indexes: true,
            },
        };
        for stage in skip {
            match stage {
                SyncStage::Import => stages.import = false,
                SyncStage::Embed => stages.embed = false,
                SyncStage::UpdateIndexes => stages.update_indexes = false,
            }
        }
        if !(stages.import || stages.embed || stages.update_indexes) {
            bail!("no sync stages selected");
        }
        Ok(stages)
    }
}

#[derive(Debug, Default)]
struct SyncRunSummary {
    ingest: Option<IngestSummary>,
    archive_import: Option<LanceArchiveImport>,
    embed: Option<EmbedStageSummary>,
    indexes: Option<OptimizeOutcome>,
}

#[derive(Debug, Clone)]
struct EmbedStageSummary {
    summary: EmbedSummary,
    device: Option<String>,
    backlog: usize,
    already_embedded: usize,
    eligible_total: usize,
    model: &'static str,
}

async fn run_import_stage(
    store: &Store,
    loaded: &Config,
    config_file: &Path,
    adapter: Option<String>,
    source_dir: Option<PathBuf>,
) -> anyhow::Result<IngestSummary> {
    let sources = resolve_sync_sources(loaded, config_file, adapter.as_deref(), source_dir)?;
    if sources.is_empty() {
        output(&format!(
            "{} no sources configured or discovered",
            pond::output::paint("import:", pond::output::dim()),
        ))?;
        return Ok(IngestSummary::default());
    }
    let watermarks = StoredWatermarks::new(store.session_last_ingested_at().await?);
    let mut total = IngestSummary::default();
    for (name, blob) in sources {
        let summary = sync_with_progress(store, &name, blob, &watermarks).await?;
        total.inserted += summary.inserted;
        total.matched += summary.matched;
        total.dropped_events += summary.dropped_events;
        total.dropped_sessions += summary.dropped_sessions;
        total.skipped_files += summary.skipped_files;
        total.skipped_fresh += summary.skipped_fresh;
        total.storage_errors += summary.storage_errors;
        total.truncated_values += summary.truncated_values;
        for (reason, count) in summary.drop_reasons {
            *total.drop_reasons.entry(reason).or_insert(0) += count;
        }
    }
    Ok(total)
}

async fn run_embed_stage(store: &Store, force: bool) -> anyhow::Result<EmbedStageSummary> {
    run_embed_stage_with_limit(store, force, None, "--force-embed").await
}

async fn run_embed_stage_with_limit(
    store: &Store,
    force: bool,
    limit: Option<usize>,
    force_hint: &'static str,
) -> anyhow::Result<EmbedStageSummary> {
    let stale = store.stale_embedding_count().await?;
    if stale > 0 {
        if !force {
            bail!(
                "{stale} message(s) embedded under a different model id; pass \
                 `{force_hint}` to re-embed (the vector index will be rebuilt under \
                 the configured model {:?})",
                pond::embed::model_id(),
            );
        }
        output(&pond::output::paint(
            &format!(
                "embed: --force: re-embedding {} stale-model row(s) after dropping IVF_PQ",
                format_thousands(stale as u64),
            ),
            pond::output::yellow(),
        ))?;
        store.drop_vector_index().await?;
    }

    let progress = store.embedding_progress().await?;
    let backlog = progress.total.saturating_sub(progress.embedded);
    let bar_total = match limit {
        Some(cap) => backlog.min(cap),
        None => backlog,
    };
    output(&format!(
        "{} backlog={} already_embedded={} eligible_total={} model={}",
        pond::output::paint("embed:", pond::output::dim()),
        format_thousands(bar_total as u64),
        format_thousands(progress.embedded as u64),
        format_thousands(progress.total as u64),
        progress.model,
    ))?;
    if bar_total == 0 && stale == 0 {
        return Ok(EmbedStageSummary {
            summary: EmbedSummary::default(),
            device: None,
            backlog: bar_total,
            already_embedded: progress.embedded,
            eligible_total: progress.total,
            model: progress.model,
        });
    }

    let embedder = E5Embedder::load()?;
    let device = embedder.device().to_owned();
    let bar = ProgressBar::new(bar_total as u64);
    bar.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} embed [{elapsed_precise}] [{bar:24}] {pos}/{len} ({percent}%) {per_sec} eta {eta}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-"),
    );
    bar.enable_steady_tick(Duration::from_millis(120));
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            eprintln!("\ninterrupted; flushing window (Ctrl-C again to abort)...");
            let _ = tokio::signal::ctrl_c().await;
            std::process::exit(130);
        });
    }
    let bar_for_callback = bar.clone();
    let mut worker = EmbedWorker::new(store, &embedder)
        .with_cancel(cancel)
        .with_progress(move |progress: BatchProgress| {
            bar_for_callback.set_position(progress.total_messages as u64);
        });
    if force {
        worker = worker.include_stale();
    }
    if let Some(limit) = limit {
        worker = worker.with_limit(limit);
    }
    let summary = worker.run().await?;
    bar.finish_and_clear();
    output(&format!(
        "{} done: batches={} messages={} device={}{}",
        pond::output::paint("embed:", pond::output::dim()),
        summary.batches,
        summary.messages,
        device,
        if summary.cancelled {
            " (interrupted)"
        } else {
            ""
        },
    ))?;
    Ok(EmbedStageSummary {
        summary,
        device: Some(device),
        backlog: bar_total,
        already_embedded: progress.embedded,
        eligible_total: progress.total,
        model: progress.model,
    })
}

async fn run_update_indexes_stage(store: &Store) -> anyhow::Result<OptimizeOutcome> {
    output(&pond::output::paint(
        "update-indexes: folding new rows into search indexes...",
        pond::output::dim(),
    ))?;
    let (progress, bar) = optimize_progress_bar();
    let outcome = store.optimize_indices(Some(progress), None).await?;
    bar.finish_and_clear();
    render_optimize_outcome(&outcome)?;
    Ok(outcome)
}

fn render_sync_summary(summary: &SyncRunSummary) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint, red, yellow};

    let mut parts = Vec::new();
    if let Some(ingest) = &summary.ingest {
        parts.push(format!(
            "import inserted={} matched={} dropped={} skipped={}",
            format_thousands(ingest.inserted as u64),
            format_thousands(ingest.matched as u64),
            format_thousands((ingest.dropped_events + ingest.dropped_sessions) as u64),
            format_thousands((ingest.skipped_files + ingest.skipped_fresh) as u64),
        ));
    }
    if let Some(imported) = &summary.archive_import {
        parts.push(format!(
            "import archive_rows={} inserted={}",
            format_thousands(
                (imported.rows.sessions + imported.rows.messages + imported.rows.parts) as u64,
            ),
            format_thousands(
                (imported.inserted.sessions + imported.inserted.messages + imported.inserted.parts)
                    as u64,
            ),
        ));
    }
    if let Some(embed) = &summary.embed {
        let suffix = match embed.device.as_deref() {
            Some(device) => format!(" device={device}"),
            None => String::new(),
        };
        parts.push(format!(
            "embed messages={} backlog={} embedded={}/{} model={}{}",
            format_thousands(embed.summary.messages as u64),
            format_thousands(embed.backlog as u64),
            format_thousands(embed.already_embedded as u64),
            format_thousands(embed.eligible_total as u64),
            embed.model,
            suffix,
        ));
    }
    if let Some(indexes) = &summary.indexes {
        let failed = indexes.any_indices_failed();
        parts.push(format!(
            "update-indexes {}",
            if failed {
                paint("failed", red())
            } else {
                paint("ok", bold())
            }
        ));
        if failed {
            output(&paint(
                "sync: index maintenance reported failures; see table above",
                yellow(),
            ))?;
        }
    }
    if parts.is_empty() {
        output(&format!("{} no stages ran", paint("sync:", dim())))?;
    } else {
        output(&format!("{} {}", paint("sync:", dim()), parts.join(" | ")))?;
    }
    Ok(())
}

async fn export_pond_archive(store: &Store, path: &Path) -> anyhow::Result<LanceArchiveExport> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temp = tempfile::Builder::new()
        .prefix("pond-export-")
        .tempdir()
        .context("failed to create export staging dir")?;
    let data_dir = temp.path().join("data");
    let summary = store.export_clean_lance_datasets(&data_dir).await?;
    let manifest = PondArchiveManifest {
        archive_version: 1,
        pond_version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol_version: PROTOCOL_VERSION,
        created_at: Utc::now(),
        rows: summary.rows,
        source_versions: summary.source_versions,
        embedding_model: pond::embed::model_id().to_owned(),
        embedding_dim: pond::sessions::embedding_dim(),
    };
    let manifest_json =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize archive manifest")?;
    fs::write(temp.path().join("manifest.json"), manifest_json)
        .context("failed to write archive manifest")?;
    zip_directory(temp.path(), path)?;
    Ok(summary)
}

async fn import_pond_archive(store: &Store, path: &Path) -> anyhow::Result<LanceArchiveImport> {
    let temp = tempfile::Builder::new()
        .prefix("pond-import-")
        .tempdir()
        .context("failed to create import staging dir")?;
    unzip_archive(path, temp.path())?;
    let manifest_path = temp.path().join("manifest.json");
    let manifest_bytes = fs::read(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let manifest: PondArchiveManifest =
        serde_json::from_slice(&manifest_bytes).context("failed to parse archive manifest")?;
    if manifest.archive_version != 1 {
        bail!(
            "unsupported .pond archive version {}; supported: 1",
            manifest.archive_version
        );
    }
    if manifest.protocol_version != PROTOCOL_VERSION {
        bail!(
            "unsupported .pond protocol version {}; supported: {}",
            manifest.protocol_version,
            PROTOCOL_VERSION
        );
    }
    if manifest.embedding_dim != pond::sessions::embedding_dim() {
        bail!(
            "archive embedding_dim={} does not match configured embedding_dim={}",
            manifest.embedding_dim,
            pond::sessions::embedding_dim()
        );
    }
    store
        .import_clean_lance_datasets(&temp.path().join("data"))
        .await
}

fn zip_directory(source: &Path, dest: &Path) -> anyhow::Result<()> {
    let file =
        File::create(dest).with_context(|| format!("failed to create {}", dest.display()))?;
    let mut zip = zip::ZipWriter::new(file);
    let file_options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o644);
    let dir_options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .unix_permissions(0o755);
    for entry in walkdir::WalkDir::new(source).sort_by_file_name() {
        let entry = entry.context("failed to walk archive staging dir")?;
        let path = entry.path();
        let rel = path
            .strip_prefix(source)
            .context("failed to compute archive relative path")?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        let name = rel
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        if entry.file_type().is_dir() {
            zip.add_directory(format!("{name}/"), dir_options)
                .with_context(|| format!("failed to add archive directory {name}"))?;
            continue;
        }
        zip.start_file(&name, file_options)
            .with_context(|| format!("failed to add archive file {name}"))?;
        let mut input =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        io::copy(&mut input, &mut zip)
            .with_context(|| format!("failed to write archive file {name}"))?;
    }
    zip.finish().context("failed to finalize archive")?;
    Ok(())
}

fn unzip_archive(source: &Path, dest: &Path) -> anyhow::Result<()> {
    let file = File::open(source)
        .with_context(|| format!("failed to open archive {}", source.display()))?;
    let mut archive = zip::ZipArchive::new(file).context("failed to read .pond archive")?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .context("failed to read archive entry")?;
        let Some(enclosed) = entry.enclosed_name() else {
            bail!("archive contains unsafe path {}", entry.name());
        };
        let outpath = dest.join(enclosed);
        if entry.is_dir() {
            fs::create_dir_all(&outpath)
                .with_context(|| format!("failed to create {}", outpath.display()))?;
            continue;
        }
        if let Some(parent) = outpath.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let mut output = File::create(&outpath)
            .with_context(|| format!("failed to create {}", outpath.display()))?;
        io::copy(&mut entry, &mut output)
            .with_context(|| format!("failed to extract {}", outpath.display()))?;
    }
    Ok(())
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
        let picks =
            adapter::prompt_and_persist(config_file, &candidates, io::stdin().is_terminal())?;
        return Ok(picks.into_iter().map(|c| (c.name, c.config)).collect());
    }

    if !config.sources.is_empty() {
        return config.resolve_sources(None);
    }
    let candidates = adapter::discover(None);
    let picks = adapter::prompt_and_persist(config_file, &candidates, io::stdin().is_terminal())?;
    Ok(picks.into_iter().map(|c| (c.name, c.config)).collect())
}

/// Run one adapter's ingest pass into `store` with a live progress bar and
/// one greppable log line per finished (or skipped) session.
async fn sync_with_progress(
    store: &Store,
    name: &str,
    config: Value,
    oracle: &dyn pond::adapter::SkipOracle,
) -> anyhow::Result<IngestSummary> {
    let factory = adapter::by_name(name).ok_or_else(|| {
        anyhow::anyhow!(
            "unknown adapter {name:?}; known: {}",
            adapter::known_names().join(", "),
        )
    })?;
    let adapter = factory.open(config)?;

    // `stderr_with_hz(8)` (indicatif 0.18.4) lowers the redraw rate from the
    // 20Hz default so SIGWINCH-triggered terminal reflows have time to
    // settle between renders. `{wide_msg}` truncates the (variable-length)
    // message instead of wrapping past the column count, which would leave
    // the previous render's tail orphaned in scrollback when the user
    // resizes mid-run (indicatif#144, #695, microsoft/terminal#6932).
    let bar =
        ProgressBar::with_draw_target(Some(0), indicatif::ProgressDrawTarget::stderr_with_hz(8));
    bar.set_style(
        ProgressStyle::with_template(
            "sync {prefix} [{elapsed_precise}] [{bar:24}] {pos}/{len} sessions  {wide_msg}",
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

    let summary = handlers::ingest_adapter(store, adapter.as_ref(), oracle, |event| match event {
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
            let status_label: &str;
            match &outcome.status {
                SyncStatus::Ok => {
                    status_label = "ok";
                    dropped_count = 0;
                    optional_reason = None;
                }
                SyncStatus::Partial {
                    dropped_events,
                    first_drop_reason,
                } => {
                    drops += *dropped_events as u64;
                    status_label = "partial";
                    dropped_count = *dropped_events;
                    optional_reason = Some(match first_drop_reason {
                        Some(reason) => {
                            format!("dropped {dropped_events} event(s) mid-session: {reason}")
                        }
                        None => format!("dropped {dropped_events} event(s) mid-session"),
                    });
                }
                SyncStatus::Skipped { reason } => {
                    errors += 1;
                    status_label = "skipped";
                    dropped_count = 0;
                    optional_reason = Some(reason.clone());
                }
                SyncStatus::Rejected { reason } => {
                    errors += 1;
                    status_label = "rejected";
                    dropped_count = 0;
                    optional_reason = Some(reason.clone());
                }
                SyncStatus::Fresh => {
                    status_label = "fresh";
                    dropped_count = 0;
                    optional_reason = None;
                }
            }
            messages += outcome.messages as u64;
            // Only surface the non-`ok`/`fresh` cases as scroll-back lines;
            // the bulk are routine successes already counted by the bar's
            // pos/len/msg counters. `pond::sync` at INFO still carries the
            // full per-session detail for `POND_LOG=pond::sync=info` runs.
            if !matches!(outcome.status, SyncStatus::Ok | SyncStatus::Fresh) {
                bar_ref.println(format_sync_line(name, &outcome, optional_reason.as_deref()));
            }
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
fn format_sync_line(adapter: &str, outcome: &SessionOutcome, reason: Option<&str>) -> String {
    use pond::output::{green, paint, red, yellow};

    let (raw_tag, tag_style) = match &outcome.status {
        SyncStatus::Ok => ("ok  ", green()),
        SyncStatus::Partial { .. } => ("part", yellow()),
        SyncStatus::Skipped { .. } => ("skip", red()),
        SyncStatus::Rejected { .. } => ("rej ", red()),
        SyncStatus::Fresh => ("fresh", green()),
    };
    let tag = paint(raw_tag, tag_style);
    if matches!(outcome.status, SyncStatus::Fresh) {
        let ts = chrono::Local::now().format("%H:%M:%S");
        let session = outcome.session_id.as_deref().unwrap_or("-");
        return format!("[{ts}] {adapter} {tag}  session={session}  (cached)");
    }
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

async fn explain_search(
    store: &Store,
    embedder: &LazyEmbedder,
    request: &SearchRequest,
    search: &config::SearchConfig,
) -> anyhow::Result<String> {
    handlers::explain_search_plan(store, embedder, request.clone(), search)
        .await
        .map_err(|envelope| anyhow::anyhow!("{envelope:?}"))
}

async fn wait_for_index_catchup(store: &Store) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(600);
    loop {
        let statuses = store.index_status().await?;
        if statuses.iter().all(|status| status.unindexed_rows == 0) {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for indexes to catch up");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Build the spinner + progress callback pair for index maintenance.
/// `PhaseStart` updates the spinner so the operator sees what's running;
/// `PhaseDone` writes a completed line via `output()` (stdout) so per-phase
/// timing is captured by pipes and scripts too. The bar's draw target is
/// stderr; when stderr isn't a TTY the bar silently degrades and only the
/// `output()` lines are visible.
fn optimize_progress_bar() -> (OptimizeProgressFn, ProgressBar) {
    use pond::output::{dim, paint};
    let bar = ProgressBar::new_spinner();
    bar.set_style(
        ProgressStyle::with_template("{spinner:.green} {elapsed_precise} {wide_msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    bar.enable_steady_tick(Duration::from_millis(120));
    let bar_for_callback = bar.clone();
    let callback: OptimizeProgressFn = Box::new(move |event| match event {
        OptimizeEvent::PhaseStart {
            table,
            phase,
            detail,
        } => {
            let label = match detail {
                Some(d) => format!("{} {} ({d})", table.as_str(), phase.label()),
                None => format!("{} {}", table.as_str(), phase.label()),
            };
            bar_for_callback.set_message(label);
        }
        OptimizeEvent::PhaseDone {
            table,
            phase,
            elapsed_ms,
        } => {
            let line = format!(
                "  {:9}  {:<14}  {} ms",
                table.as_str(),
                phase.label(),
                format_thousands(elapsed_ms),
            );
            let _ = output(&paint(&line, dim()));
        }
    });
    (callback, bar)
}

fn render_optimize_outcome(outcome: &OptimizeOutcome) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint, red, yellow};
    let mut table = new_table();
    table.set_header(vec!["table", "indices", "compaction"]);
    for entry in &outcome.tables {
        table.add_row(vec![
            Cell::new(entry.table.as_str()),
            phase_cell(&entry.indices, "indices"),
            phase_cell(&entry.compaction, "compaction"),
        ]);
    }
    output(&paint("index maintenance", bold()))?;
    output(&table.to_string())?;
    let mut hinted = false;
    for entry in &outcome.tables {
        if matches!(entry.compaction, PhaseOutcome::SkippedConflict) {
            hinted = true;
            output(&format!(
                "{}  compaction on {} deferred: concurrent writer; rerun once it finishes",
                paint("hint", dim()),
                entry.table.as_str(),
            ))?;
        }
    }
    for entry in &outcome.tables {
        if let PhaseOutcome::Failed(error) = &entry.indices {
            output(&paint(
                &format!("error  indices on {}: {error:#}", entry.table.as_str()),
                red(),
            ))?;
            hinted = true;
        }
        if let PhaseOutcome::Failed(error) = &entry.compaction {
            output(&paint(
                &format!("error  compaction on {}: {error:#}", entry.table.as_str()),
                yellow(),
            ))?;
            hinted = true;
        }
    }
    let _ = hinted;
    Ok(())
}

fn phase_cell(outcome: &PhaseOutcome, _phase: &str) -> Cell {
    use pond::output::{dim, paint, red, yellow};
    match outcome {
        PhaseOutcome::Ok => Cell::new("ok"),
        PhaseOutcome::Noop => Cell::new(paint("-", dim())),
        PhaseOutcome::NotAttempted => Cell::new(paint("-", dim())),
        PhaseOutcome::SkippedConflict => Cell::new(paint("skipped (conflict)", yellow())),
        PhaseOutcome::Failed(_) => Cell::new(paint("failed", red())),
    }
}

fn render_index_status(statuses: &[IndexStatus]) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint, yellow};
    let mut table = new_table();
    table.set_header(vec![
        "table",
        "intent",
        "exists",
        "fragments",
        "unindexed rows",
    ]);
    for status in statuses {
        let unindexed = format_thousands(status.unindexed_rows as u64);
        let unindexed_cell = if status.unindexed_rows == 0 {
            Cell::new(unindexed)
        } else {
            Cell::new(paint(&unindexed, yellow()))
        };
        table.add_row(vec![
            Cell::new(status.table.as_str()),
            Cell::new(&status.intent_name),
            Cell::new(if status.exists { "yes" } else { "no" }),
            Cell::new(status.fragments_covered.to_string()),
            unindexed_cell.set_alignment(CellAlignment::Right),
        ]);
    }
    output(&paint("index status", bold()))?;
    output(&table.to_string())?;
    if statuses.iter().any(|status| status.unindexed_rows > 0) {
        output(&format!(
            "{}  run `pond sync --only update-indexes` to fold trailing fragments",
            paint("hint", dim()),
        ))?;
    }
    Ok(())
}

fn render_status_header(data_url: &Url, sizes: &TableSizes) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    output(&paint("pond status", bold()))?;
    output(&format!("{}  {}", paint("data-dir", dim()), data_url))?;

    let mut table = new_table();
    let total = sizes.sessions + sizes.messages + sizes.parts + sizes.other;
    for (label, bytes) in [
        ("sessions", sizes.sessions),
        ("messages", sizes.messages),
        ("parts", sizes.parts),
        ("other", sizes.other),
    ] {
        table.add_row(vec![
            Cell::new(format!("  {label}")),
            Cell::new(format_bytes(bytes)).set_alignment(CellAlignment::Right),
        ]);
    }
    table.add_row(vec![
        Cell::new("  total").add_attribute(Attribute::Bold),
        Cell::new(format_bytes(total))
            .set_alignment(CellAlignment::Right)
            .add_attribute(Attribute::Bold),
    ]);
    output(&table.to_string())?;
    Ok(())
}

/// Render the checks that can take longer on a large corpus. The command
/// prints storage first, then calls this once the bounded scans finish.
fn render_status_checks(
    stats: &CorpusStats,
    index_status: &[IndexStatus],
    embedding: EmbeddingProgress,
    scripts: &[(String, usize)],
    verbose: bool,
) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint, yellow};

    let RowTotals {
        sessions,
        messages,
        parts,
    } = stats.totals;
    output("")?;
    output(&format!(
        "{}  {} sessions  {} messages  {} parts",
        paint("totals", dim()),
        paint(&format_thousands(sessions), bold()),
        paint(&format_thousands(messages), bold()),
        paint(&format_thousands(parts), bold()),
    ))?;
    let pending = embedding.total.saturating_sub(embedding.embedded);
    if embedding.total == 0 {
        output(&format!(
            "{}  no embeddable messages (model={})",
            paint("embeddings", dim()),
            embedding.model,
        ))?;
    } else if pending == 0 {
        output(&format!(
            "{}  {}/{} messages  model={}",
            paint("embeddings", dim()),
            paint(&format_thousands(embedding.embedded as u64), bold()),
            paint(&format_thousands(embedding.total as u64), bold()),
            embedding.model,
        ))?;
    } else {
        output(&paint(
            &format!(
                "embeddings  {}/{} messages  model={} - run `pond sync --only embed` to fill the {} backlog",
                format_thousands(embedding.embedded as u64),
                format_thousands(embedding.total as u64),
                embedding.model,
                format_thousands(pending as u64),
            ),
            yellow(),
        ))?;
    }
    let index_backlog: usize = index_status
        .iter()
        .map(|status| status.unindexed_rows)
        .sum();
    let existing_indices = index_status.iter().filter(|status| status.exists).count();
    if index_status.is_empty() {
        output(&format!(
            "{}  no configured indexes",
            paint("indexes", dim())
        ))?;
    } else if index_backlog == 0 && existing_indices == index_status.len() {
        output(&format!(
            "{}  {}/{} ready",
            paint("indexes", dim()),
            existing_indices,
            index_status.len(),
        ))?;
    } else if index_backlog == 0 {
        output(&format!(
            "{}  {}/{} built; no unindexed rows",
            paint("indexes", dim()),
            existing_indices,
            index_status.len(),
        ))?;
    } else {
        output(&paint(
            &format!(
                "indexes  {}/{} built - {} unindexed row(s), run `pond sync --only update-indexes`",
                existing_indices,
                index_status.len(),
                format_thousands(index_backlog as u64),
            ),
            yellow(),
        ))?;
    }
    if verbose {
        output("")?;
        output(&paint("index detail", dim()))?;
        for status in index_status {
            let line = format!(
                "  {}.{}  exists={}  fragments={}  unindexed={}",
                status.table.as_str(),
                status.intent_name,
                if status.exists { "yes" } else { "no" },
                status.fragments_covered,
                format_thousands(status.unindexed_rows as u64),
            );
            if status.unindexed_rows == 0 {
                output(&line)?;
            } else {
                output(&paint(&line, yellow()))?;
            }
        }
    }
    // Surfaces the corpus's language mix so an agent can decide whether
    // bilingual querying is worth attempting (cross-lingual recall is a
    // caller-layer concern; pond does not translate internally - see the
    // `pond_search` MCP description). Sample is bounded; total = sum of
    // alphabetic characters in the sampled `search_text`.
    if !scripts.is_empty() {
        let total: usize = scripts.iter().map(|(_, count)| *count).sum();
        if total > 0 {
            let parts = scripts
                .iter()
                .filter(|(_, count)| *count > 0)
                .map(|(name, count)| {
                    let pct = (*count as f64 / total as f64) * 100.0;
                    format!("{name} {pct:.0}%")
                })
                .collect::<Vec<_>>()
                .join("  ");
            output(&format!("{}  {parts}", paint("scripts", dim())))?;
        }
    }
    if !stats.include_subagents && verbose {
        output(&paint(
            "  note: totals above include sub-agent sessions; the rollup below shows main-agent only. Pass `--include-subagents` for the per-agent breakdown.",
            dim(),
        ))?;
    }

    if !verbose {
        output(&format!(
            "{}  {} adapter(s), top projects hidden; pass `--verbose` for project tables",
            paint("sources", dim()),
            stats.adapters.len(),
        ))?;
        return Ok(());
    }

    // Render adapters in registry order so the layout matches the discovery
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
    use pond::output::{bold, cyan, paint};

    output("")?;
    output(&format!(
        "{}  {} sessions  {} messages  {} projects",
        paint(&stat.adapter, cyan().bold()),
        paint(&format_thousands(stat.sessions), bold()),
        paint(&format_thousands(stat.messages), bold()),
        paint(&format_thousands(stat.projects.len() as u64), bold()),
    ))?;
    if stat.projects.is_empty() {
        return Ok(());
    }
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("project")
            .add_attribute(Attribute::Bold)
            .add_attribute(Attribute::Dim),
        Cell::new("sessions")
            .set_alignment(CellAlignment::Right)
            .add_attribute(Attribute::Bold)
            .add_attribute(Attribute::Dim),
        Cell::new("messages")
            .set_alignment(CellAlignment::Right)
            .add_attribute(Attribute::Bold)
            .add_attribute(Attribute::Dim),
    ]);
    for project in &stat.projects {
        let label = project.project.as_str();
        table.add_row(vec![
            Cell::new(label),
            Cell::new(format_thousands(project.sessions)).set_alignment(CellAlignment::Right),
            Cell::new(format_thousands(project.messages)).set_alignment(CellAlignment::Right),
        ]);
    }
    // Let the project column flex; right-size the numeric columns to their
    // content so the long path takes the remaining width and truncates with
    // an ellipsis on narrow terminals.
    if let Some(col) = table.column_mut(1) {
        col.set_constraint(ColumnConstraint::ContentWidth);
    }
    if let Some(col) = table.column_mut(2) {
        col.set_constraint(ColumnConstraint::ContentWidth);
    }
    output(&table.to_string())?;
    Ok(())
}

/// House style for `pond status` tables: borderless, dynamic-width, no inner
/// rules. Centralized so future tabular commands match without copy-paste.
fn new_table() -> Table {
    let mut table = Table::new();
    table
        .load_preset(NOTHING)
        .set_content_arrangement(ContentArrangement::Dynamic);
    table
}

/// Dispatch an envelope through the chosen format. Returns `true` when the
/// envelope was a `Success` (callers exit non-zero on `false`). JSON mode
/// always emits the envelope to stdout so scripts can pipe both success and
/// error bodies through `jq`; pretty mode routes errors to stderr so stdout
/// stays parseable.
fn render_search_envelope(format: OutputFormat, envelope: &SearchEnvelope) -> anyhow::Result<bool> {
    match format {
        OutputFormat::Json => {
            output(
                &serde_json::to_string_pretty(envelope)
                    .context("serialize search envelope as JSON")?,
            )?;
            Ok(matches!(envelope, SearchEnvelope::Success(_)))
        }
        OutputFormat::Pretty => match envelope {
            SearchEnvelope::Success(response) => {
                render_search_pretty(response)?;
                Ok(true)
            }
            SearchEnvelope::Error(error) => {
                render_error_pretty(error);
                Ok(false)
            }
        },
    }
}

fn render_get_envelope(format: OutputFormat, envelope: &GetEnvelope) -> anyhow::Result<bool> {
    match format {
        OutputFormat::Json => {
            output(
                &serde_json::to_string_pretty(envelope)
                    .context("serialize get envelope as JSON")?,
            )?;
            Ok(matches!(envelope, GetEnvelope::Success(_)))
        }
        OutputFormat::Pretty => match envelope {
            GetEnvelope::Success(response) => {
                render_get_pretty(response)?;
                Ok(true)
            }
            GetEnvelope::Error(error) => {
                render_error_pretty(error);
                Ok(false)
            }
        },
    }
}

fn render_search_pretty(response: &SearchResponse) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    match &response.result {
        SearchResultBody::Hits { hits } => {
            output(&format!(
                "{} {} {}",
                paint("search:", dim()),
                paint(&format_thousands(response.total as u64), bold()),
                if response.total == 1 { "hit" } else { "hits" },
            ))?;
            if hits.is_empty() {
                return Ok(());
            }
            for (idx, hit) in hits.iter().enumerate() {
                output("")?;
                render_hit(idx + 1, hit)?;
            }
        }
        SearchResultBody::Groups { groups } => {
            output(&format!(
                "{} {} {}",
                paint("search:", dim()),
                paint(&format_thousands(response.total as u64), bold()),
                if response.total == 1 {
                    "session"
                } else {
                    "sessions"
                },
            ))?;
            if groups.is_empty() {
                return Ok(());
            }
            for (idx, group) in groups.iter().enumerate() {
                output("")?;
                render_group(idx + 1, group)?;
            }
        }
    }
    Ok(())
}

fn render_hit(rank: usize, hit: &Hit) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    let matched = if hit.matched_via.is_empty() {
        "-".to_owned()
    } else {
        hit.matched_via.join("+")
    };
    output(&format!(
        "{}  {}  {}",
        paint(&format!("[{rank}]"), dim()),
        paint(&format!("{:.4}", hit.score), bold()),
        paint(&matched, dim()),
    ))?;
    output(&format!(
        "    {}  {}  {}  {}",
        paint(
            &hit.timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            dim(),
        ),
        paint_role(&hit.role),
        paint(&hit.project, dim()),
        paint(&hit.message_id, dim()),
    ))?;
    render_hit_text(&hit.text, hit.snippet.as_deref())?;
    Ok(())
}

fn render_group(rank: usize, group: &Group) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    output(&format!(
        "{}  best={}  {} messages",
        paint(&format!("[{rank}]"), dim()),
        paint(&format!("{:.4}", group.best_score), bold()),
        paint(&format_thousands(group.message_count as u64), bold()),
    ))?;
    output(&format!(
        "    {} -> {}  {}  {}  {}",
        paint(
            &group.first_timestamp.format("%Y-%m-%dT%H:%M").to_string(),
            dim(),
        ),
        paint(
            &group.last_timestamp.format("%Y-%m-%dT%H:%M").to_string(),
            dim(),
        ),
        paint(&group.project, dim()),
        paint(&group.source_agent, dim()),
        paint(&group.session_id, dim()),
    ))?;
    render_hit_text(&group.text, group.snippet.as_deref())?;
    Ok(())
}

/// Render a hit's `text` payload, then a `snippet` block when one is present
/// (the text was truncated). Empty text renders nothing.
fn render_hit_text(text: &str, snippet: Option<&str>) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let prefix = paint(">", dim());
    for line in text.lines() {
        output(&format!("    {prefix} {line}"))?;
    }
    if let Some(snippet) = snippet {
        output(&format!("    {}", paint("snippet:", dim())))?;
        for line in snippet.lines() {
            output(&format!("    {prefix} {line}"))?;
        }
    }
    Ok(())
}

fn render_get_pretty(response: &GetResponse) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    let (session, messages, parts) = match &response.result {
        GetResult::Session {
            session,
            messages,
            parts,
        }
        | GetResult::Message {
            session,
            messages,
            parts,
        } => (session, messages, parts),
    };

    output(&format!(
        "{} {}  source={}  project={}",
        paint("session", dim()),
        paint(&session.id, bold()),
        session.source_agent,
        session.project.as_str(),
    ))?;
    output(&format!(
        "{} {}",
        paint("created:", dim()),
        paint(
            &session.created_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            dim(),
        ),
    ))?;

    // Group parts by message_id, sorted by ordinal, so each message renders
    // its parts in order without re-scanning the parts vec per message.
    let mut parts_by_msg: std::collections::HashMap<&str, Vec<&Part>> =
        std::collections::HashMap::new();
    for part in parts {
        parts_by_msg
            .entry(part.message_id.as_str())
            .or_default()
            .push(part);
    }
    for parts_for_msg in parts_by_msg.values_mut() {
        parts_for_msg.sort_by_key(|p| p.ordinal);
    }

    for (idx, message) in messages.iter().enumerate() {
        output("")?;
        let parts_for_msg = parts_by_msg
            .get(message.id())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        render_message(idx + 1, message, parts_for_msg)?;
    }

    output("")?;
    output(&format!(
        "{} {} messages, {} parts{}",
        paint("(total:", dim()),
        paint(&format_thousands(messages.len() as u64), bold()),
        paint(&format_thousands(parts.len() as u64), bold()),
        paint(")", dim()),
    ))?;
    Ok(())
}

fn render_message(rank: usize, message: &Message, parts: &[&Part]) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    output(&format!(
        "{}  {}  {}  {}",
        paint(&format!("[{rank}]"), dim()),
        paint(
            &message.timestamp().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            dim(),
        ),
        paint_role(message.role().as_str()),
        paint(message.id(), dim()),
    ))?;
    if let Some(content) = message.system_content() {
        render_hit_text(content, None)?;
        return Ok(());
    }
    for part in parts {
        render_part(part)?;
    }
    Ok(())
}

fn render_part(part: &Part) -> anyhow::Result<()> {
    use pond::output::{dim, paint, yellow};

    let prefix = paint(">", dim());
    match &part.kind {
        // `Option<String>`: render only what's there. A `None` text part
        // means the source row carried no text field; printing nothing is
        // the faithful representation - no "<unresolved>" placeholder.
        PartKind::Text { text } => {
            if let Some(text) = text {
                for line in text.lines() {
                    output(&format!("    {prefix} {line}"))?;
                }
            }
        }
        PartKind::Reasoning { text } => {
            let tag = paint("[reasoning]", dim());
            if let Some(text) = text {
                for line in text.lines() {
                    output(&format!("    {tag} {prefix} {line}"))?;
                }
            }
        }
        PartKind::File {
            media_type,
            file_name,
            ..
        } => {
            output(&format!(
                "    {} media_type={media_type} file_name={}",
                paint("[file]", yellow()),
                file_name.as_deref().unwrap_or("-"),
            ))?;
        }
        // For tool_call / tool_result: omit the field entirely when None.
        // Concretely: a tool_result with no resolvable name prints as
        // `[tool_result] call_id=toolu_01...` (no name token), not
        // `[tool_result] unknown call_id=toolu_01...` (which lied) and
        // not `[tool_result] - call_id=toolu_01...` (which translates).
        PartKind::ToolCall { call_id, name, .. } => {
            let name_token = name.as_deref().map(|n| format!(" {n}")).unwrap_or_default();
            let call_id_token = call_id
                .as_deref()
                .map(|id| format!(" call_id={id}"))
                .unwrap_or_default();
            output(&format!(
                "    {}{name_token}{call_id_token}",
                paint("[tool_call]", yellow()),
            ))?;
        }
        PartKind::ToolResult {
            call_id,
            name,
            is_failure,
            ..
        } => {
            let name_token = name.as_deref().map(|n| format!(" {n}")).unwrap_or_default();
            let call_id_token = call_id
                .as_deref()
                .map(|id| format!(" call_id={id}"))
                .unwrap_or_default();
            output(&format!(
                "    {}{name_token}{call_id_token}{}",
                paint("[tool_result]", yellow()),
                if *is_failure { " (failure)" } else { "" },
            ))?;
        }
        PartKind::ToolApprovalRequest {
            approval_id,
            tool_call_id,
        } => {
            output(&format!(
                "    {} approval_id={approval_id} tool_call_id={tool_call_id}",
                paint("[approval_request]", yellow()),
            ))?;
        }
        PartKind::ToolApprovalResponse {
            approval_id,
            approved,
            reason,
        } => {
            let suffix = reason
                .as_deref()
                .map(|r| format!(" reason={r}"))
                .unwrap_or_default();
            output(&format!(
                "    {} approval_id={approval_id} approved={approved}{suffix}",
                paint("[approval_response]", yellow()),
            ))?;
        }
    }
    Ok(())
}

fn render_error_pretty(error: &ErrorEnvelope) {
    use pond::output::{bold, dim, paint, red};

    let code = match error.error.code {
        wire::ErrorCode::ValidationFailed => "validation_failed",
        wire::ErrorCode::VersionUnsupported => "version_unsupported",
        wire::ErrorCode::NotFound => "not_found",
        wire::ErrorCode::NamespaceUnknown => "namespace_unknown",
        wire::ErrorCode::StorageUnavailable => "storage_unavailable",
        wire::ErrorCode::Conflict => "conflict",
        wire::ErrorCode::Internal => "internal",
    };
    eprintln!(
        "{} {} {}",
        paint("error", red().bold()),
        paint(code, bold()),
        error.error.message,
    );
    let details_present = !error.error.details.is_null()
        && !error
            .error
            .details
            .as_object()
            .map(|map| map.is_empty())
            .unwrap_or(false);
    if details_present {
        eprintln!(
            "{}",
            paint(&format!("  details: {}", error.error.details), dim()),
        );
    }
    eprintln!(
        "{}",
        paint(&format!("  request_id: {}", error.request_id), dim()),
    );
}

fn paint_role(role: &str) -> String {
    use pond::output::{cyan, dim, green, paint, yellow};
    let style = match role {
        "user" => green(),
        "assistant" => cyan(),
        "tool" => yellow(),
        _ => dim(),
    };
    paint(role, style)
}
