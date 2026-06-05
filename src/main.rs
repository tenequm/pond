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
    embed::{BatchProgress, CandleEmbedder, EmbedSummary, EmbedWorker, Embedder, LazyEmbedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{
        AdapterStats, CorpusStats, EmbeddingProgress, LanceArchiveCounts, LanceArchiveExport,
        LanceArchiveImport, MESSAGES_FTS_INDEX, MESSAGES_VECTOR_INDEX, OptimizeOutcome, RowTotals,
        Store,
    },
    substrate::{
        IndexStatus, OptimizeEvent, OptimizeProgressFn, PhaseOutcome, TableSizes,
        index_lag_threshold,
    },
    transport::{self, AppState},
    wire::{
        self, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse, GetResult, MessageView,
        PartKind, PartSummary, ProjectFilter, ResponseMode, ResponsePart, SearchEnvelope,
        SearchFilters, SearchModeWire, SearchRequest, SearchResponse, SearchResult, SearchSession,
        SessionFrom,
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

/// CLI surface for `pond get --response-mode`. Maps 1:1 to wire `ResponseMode`.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliResponseMode {
    Conversational,
    Complete,
    Verbatim,
}

impl From<CliResponseMode> for ResponseMode {
    fn from(mode: CliResponseMode) -> Self {
        match mode {
            CliResponseMode::Conversational => ResponseMode::Conversational,
            CliResponseMode::Complete => ResponseMode::Complete,
            CliResponseMode::Verbatim => ResponseMode::Verbatim,
        }
    }
}

/// CLI surface for `pond get --from`. Maps 1:1 to wire `SessionFrom`.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliSessionFrom {
    Start,
    End,
}

impl From<CliSessionFrom> for SessionFrom {
    fn from(value: CliSessionFrom) -> Self {
        match value {
            CliSessionFrom::Start => SessionFrom::Start,
            CliSessionFrom::End => SessionFrom::End,
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
    /// `-v` / `-vv` / `-vvv` raise the tracing log level; `-q` / `-qq` lower
    /// it. The display layout itself is independent of this knob - see
    /// per-command flags like `pond status --adapters` for that.
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,
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
        /// Show one section per adapter, including the per-project tables
        /// and per-intent index detail. Implies `--include-subagents` so the
        /// rollup reconciles with the data-dir row counts above.
        #[arg(long)]
        adapters: bool,
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
        /// Auto-accept every probe prompt; non-interactive runs use this to
        /// enable freshly-detected adapters without a TTY.
        #[arg(long, short = 'y')]
        yes: bool,
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
        /// Substring match by default (`--project pond` -> contains "pond"). Prefix
        /// with `re:` for regex (`--project 're:^/Users/.*/x402'`); `lit:` escapes
        /// a literal value that would otherwise be parsed as a prefix.
        #[arg(long, value_parser = parse_project_filter)]
        project: Option<ProjectFilter>,
        #[arg(long, value_name = "ID")]
        session_id: Option<String>,
        #[arg(long)]
        source_agent: Option<String>,
        /// Include subagent sessions (excluded by default).
        #[arg(long)]
        include_subagents: bool,
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
        /// For `--message-id` mode: include this many sibling messages on each
        /// side (grep -C style). Ignored in session mode.
        #[arg(long, default_value_t = 0)]
        context_depth: usize,
        /// Cap on returned messages (session mode) or parts (message mode).
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Session-mode depth: conversational (text + part summaries), complete
        /// (all messages + summaries), or verbatim (full parts inline). Ignored
        /// with `--message-id`.
        #[arg(
            long,
            value_enum,
            default_value_t = CliResponseMode::Conversational,
            conflicts_with = "message_id"
        )]
        response_mode: CliResponseMode,
        /// Session mode only: which end to read from - start (oldest, default)
        /// or end (most recent, e.g. to recover context after compaction).
        #[arg(
            long,
            value_enum,
            default_value_t = CliSessionFrom::Start,
            conflicts_with = "message_id"
        )]
        session_from: CliSessionFrom,
        /// Continuation anchor from a prior response: last message id (session)
        /// or last part id (message). Exclusive lower bound.
        #[arg(long, value_name = "ID")]
        after_id: Option<String>,
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
    },
    Rebuild {
        intent: Option<String>,
    },
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
    human_panic::setup_panic!();

    let cli = Cli::parse();
    init_tracing(cli.verbose.tracing_level_filter());

    match cli.command {
        Command::Status {
            data_dir,
            config,
            adapters,
            include_subagents,
        } => {
            // --adapters needs sub-agents broken out so the per-adapter
            // rollup reconciles with the data-dir row counts above.
            let include_subagents = include_subagents || adapters;
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
            let (sizes, stats, index_status, embedding) = tokio::try_join!(
                store.table_sizes(),
                store.corpus_stats(include_subagents),
                store.index_status(),
                store.embedding_progress(),
            )?;
            render_status_header(&data_dir, &sizes, &stats.totals)?;
            render_status_checks(&stats, &index_status, embedding, adapters)?;
            let probes = adapter::probe_unconfigured(&loaded.sources);
            if !probes.is_empty() {
                let names: Vec<&str> = probes.iter().map(|c| c.name.as_str()).collect();
                output_err(&pond::output::paint(
                    &format!(
                        "hint     {} unconfigured adapter(s): {} - run `pond sync` to enable",
                        probes.len(),
                        names.join(", "),
                    ),
                    pond::output::dim(),
                ))?;
            }
        }
        Command::Sync {
            adapter,
            source_dir,
            import_from,
            only,
            skip,
            force_embed,
            yes,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config_file = config_path(config, &data_dir);
            let mut loaded = Config::load(&config_file)?;
            let store =
                open_store_with_spinner(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
            let stages = SyncStages::resolve(only, &skip)?;
            if import_from.is_some() && !stages.import {
                bail!(
                    "--import-from requires the import stage; remove `--skip import` or use `--only import`"
                );
            }
            let mut summary = SyncRunSummary::default();
            let did_archive_import = import_from.is_some();
            // `--source-dir` is an explicit one-off bypass of `[sources.<name>]`
            // (resolve_sync_sources honors it directly), so skip the config-based
            // re-enable - otherwise a probe-less adapter with no config entry
            // (e.g. claude-ai-export) errors before the override is applied.
            if stages.import
                && !did_archive_import
                && source_dir.is_none()
                && let Some(name) = adapter.as_deref()
            {
                maybe_reenable_positional(&mut loaded, &config_file, name, yes).await?;
            }
            if stages.import {
                if let Some(path) = import_from {
                    summary.archive_import = Some(import_pond_archive(&store, &path).await?);
                } else {
                    summary.ingest = Some(
                        run_import_stage(
                            &store,
                            &loaded,
                            &config_file,
                            adapter.clone(),
                            source_dir,
                        )
                        .await?,
                    );
                }
            }
            if stages.import && adapter.is_none() && !did_archive_import {
                let extra = handle_probe_prompt(&store, &mut loaded, &config_file, yes).await?;
                match summary.ingest.as_mut() {
                    Some(existing) => existing.merge(&extra),
                    None => summary.ingest = Some(extra),
                }
            }
            if stages.embed {
                run_embed_stage(&store, force_embed).await?;
            }
            if stages.update_indexes {
                run_update_indexes_stage(&store, configured_compaction_cap(&loaded)).await?;
            }
            render_sync_summary(&store, &summary).await?;
        }
        Command::Embed {
            data_dir,
            config,
            limit,
            force,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store =
                open_store_with_spinner(&data_dir, storage_map(&config), runtime_caps(&config))
                    .await?;
            let summary = run_embed_stage_with_limit(&store, force, limit, "--force").await?;
            if !summary.cancelled && summary.messages > 0 {
                let outcome =
                    run_update_indexes_stage(&store, configured_compaction_cap(&config)).await?;
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
            let store = Arc::new(
                open_store_with_spinner(&data_dir, storage_map(&config), runtime_caps(&config))
                    .await?,
            );
            let embedder = Arc::new(LazyEmbedder::candle());
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
            let store = Arc::new(
                open_store_with_spinner(&data_dir, storage_map(&config), runtime_caps(&config))
                    .await?,
            );
            // Lazy: idle `pond mcp` instances in every Claude Code session
            // stay light. The model load only happens once per process on the
            // first `pond_search` tool call that needs hybrid retrieval.
            let embedder = Arc::new(LazyEmbedder::candle());
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
            project,
            session_id,
            source_agent,
            include_subagents,
            from_date,
            to_date,
            role,
            min_score,
            explain,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
            let embedder = LazyEmbedder::candle();
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
                    include_subagents,
                },
                limit,
                cursor: None,
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
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
            match command {
                IndexCommand::Status => {
                    let statuses = store.index_status().await?;
                    render_index_status(&statuses)?;
                }
                IndexCommand::Optimize { wait } => {
                    let (progress, bar) = optimize_progress_bar();
                    let outcome = store
                        .optimize_indices(Some(progress), configured_compaction_cap(&loaded))
                        .await?;
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
            context_depth,
            limit,
            response_mode,
            session_from,
            after_id,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
            let request = GetRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                session_id,
                message_id,
                context_depth,
                limit,
                response_mode: ResponseMode::from(response_mode),
                session_from: SessionFrom::from(session_from),
                after_id,
            };
            let view_from = request.session_from;
            let envelope = handlers::pond_get(&store, request).await;
            if !render_get_envelope(format, &envelope, view_from)? {
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
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
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
            let store =
                Store::open_with_options(&data_dir, storage_map(&loaded), runtime_caps(&loaded))
                    .await?;
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

fn init_tracing(cli_level: tracing::level_filters::LevelFilter) {
    // Lance's IVF_PQ builder warns once per empty centroid during merge
    // (rust/lance/src/index/vector/builder.rs: "partition N is empty, skipping").
    // It already handles the case - records a zero-sized partition and continues -
    // so the warning is benign log noise during index maintenance.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!("{cli_level},lance::index::vector::builder=error"))
    });
    fmt().with_env_filter(filter).with_writer(io::stderr).init();
}

#[allow(clippy::print_stdout)]
fn output(message: &str) -> anyhow::Result<()> {
    pond::output::line(message)
}

fn output_err(message: &str) -> anyhow::Result<()> {
    pond::output::line_err(message)
}

/// Partition `outcomes` into accepts/declines, persist both, and refresh
/// `loaded` from disk. Shared between the post-import probe sweep and the
/// positional re-enable path.
fn apply_outcomes(
    loaded: &mut Config,
    config_file: &Path,
    outcomes: &[adapter::PromptOutcome],
) -> anyhow::Result<usize> {
    let accepts: Vec<adapter::Candidate> = outcomes
        .iter()
        .filter(|o| o.enable)
        .map(|o| o.candidate.clone())
        .collect();
    let declines: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.enable)
        .map(|o| o.candidate.name.as_str())
        .collect();
    if !accepts.is_empty() {
        adapter::persist_accept(config_file, &accepts)?;
    }
    if !declines.is_empty() {
        adapter::persist_decline(config_file, &declines)?;
    }
    if !accepts.is_empty() || !declines.is_empty() {
        *loaded = Config::load(config_file)?;
    }
    Ok(accepts.len())
}

/// `pond sync <name>` positional override. When `<name>` is currently
/// absent or `enabled = false`, re-probe just that one adapter and prompt
/// (or auto-accept on `--yes`). Used to re-enable a previously-declined
/// adapter without editing `config.toml` by hand.
async fn maybe_reenable_positional(
    loaded: &mut Config,
    config_file: &Path,
    name: &str,
    auto_accept: bool,
) -> anyhow::Result<()> {
    use serde_json::Value;
    use std::io::IsTerminal;
    let present = loaded.sources.get(name);
    let is_enabled = present
        .and_then(|b| b.get("enabled").and_then(Value::as_bool))
        .unwrap_or(false);
    if is_enabled {
        return Ok(());
    }
    let candidates = adapter::discover(Some(name));
    if candidates.is_empty() {
        bail!("no `[sources.{name}]` and probe returned nothing; add the entry manually");
    }
    if !auto_accept && !std::io::stdin().is_terminal() {
        bail!(
            "source [{name}] is disabled and stdin is not a terminal; pass --yes or re-run on a TTY"
        );
    }
    let outcomes = adapter::prompt_each(&candidates, auto_accept)?;
    let accepted = apply_outcomes(loaded, config_file, &outcomes)?;
    if accepted == 0 {
        bail!("declined; nothing to sync");
    }
    Ok(())
}

/// After `pond sync`'s import stage, prompt the operator about any
/// freshly-detectable adapter that has no `[sources.<name>]` section yet.
/// `enabled = true`/`enabled = false` is persisted either way so the
/// decision sticks; only the positional `pond sync <name>` re-prompts a
/// previously-declined adapter. Non-TTY runs (and `--yes` runs without a
/// TTY) emit a one-line stderr hint and continue without writing - the
/// operator can re-run on a TTY to opt in/out.
async fn handle_probe_prompt(
    store: &Store,
    loaded: &mut Config,
    config_file: &Path,
    auto_accept: bool,
) -> anyhow::Result<IngestSummary> {
    use std::io::IsTerminal;
    let mut accumulated = IngestSummary::default();
    let candidates = adapter::probe_unconfigured(&loaded.sources);
    if candidates.is_empty() {
        return Ok(accumulated);
    }
    let interactive = std::io::stdin().is_terminal();
    if !interactive && !auto_accept {
        let names: Vec<&str> = candidates.iter().map(|c| c.name.as_str()).collect();
        output_err(&pond::output::paint(
            &format!(
                "hint     {} unconfigured adapter(s): {} - run `pond sync` on a TTY to enable",
                candidates.len(),
                names.join(", "),
            ),
            pond::output::dim(),
        ))?;
        return Ok(accumulated);
    }
    let outcomes = adapter::prompt_each(&candidates, auto_accept)?;
    apply_outcomes(loaded, config_file, &outcomes)?;
    for outcome in &outcomes {
        if outcome.enable && outcome.sync_now {
            let summary = run_import_stage(
                store,
                loaded,
                config_file,
                Some(outcome.candidate.name.clone()),
                None,
            )
            .await?;
            accumulated.merge(&summary);
        }
    }
    Ok(accumulated)
}

/// Open the store with an indicatif spinner ticking while
/// [`Store::open_with_options`] runs. Open itself is cheap; the spinner only
/// matters for visual consistency with other long-running commands.
async fn open_store_with_spinner(
    location: &Url,
    storage: HashMap<String, String>,
    caps: pond::substrate::RuntimeCaps,
) -> anyhow::Result<Store> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.green} opening pond store... [{elapsed_precise}]")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.enable_steady_tick(Duration::from_millis(120));
    let result = Store::open_with_options(location, storage, caps).await;
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

/// Resolve `[runtime]` into the substrate's typed caps struct. A standalone
/// helper to keep every call site terse and to make sure no surface forgets
/// the caps.
fn runtime_caps(config: &Config) -> pond::substrate::RuntimeCaps {
    pond::substrate::RuntimeCaps::from_config(&config.runtime)
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

/// Only the import recap survives to `render_sync_summary`; embed and index
/// each print their own line as they finish, so their outcomes aren't threaded
/// back here.
#[derive(Debug, Default)]
struct SyncRunSummary {
    ingest: Option<IngestSummary>,
    archive_import: Option<LanceArchiveImport>,
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
        let disabled = loaded.disabled_source_names();
        let label = pond::output::paint("import:", pond::output::dim());
        if disabled.is_empty() {
            output(&format!(
                "{label} no sources configured. Run `pond sync` on a TTY to detect adapters, or add `[sources.<name>]` blocks to {}.",
                config_file.display(),
            ))?;
        } else {
            output(&format!(
                "{label} no enabled sources. Found {} disabled: {}. Add `enabled = true` to the section in {}, or re-enable interactively with `pond sync <name>`.",
                disabled.len(),
                disabled.join(", "),
                config_file.display(),
            ))?;
        }
        return Ok(IngestSummary::default());
    }
    let watermarks = StoredWatermarks::new(store.session_last_ingested_at().await?);
    let mut total = IngestSummary::default();
    for (name, blob) in sources {
        let summary = sync_with_progress(store, &name, blob, &watermarks).await?;
        total.merge(&summary);
    }
    Ok(total)
}

async fn run_embed_stage(store: &Store, force: bool) -> anyhow::Result<EmbedSummary> {
    run_embed_stage_with_limit(store, force, None, "--force-embed").await
}

async fn run_embed_stage_with_limit(
    store: &Store,
    force: bool,
    limit: Option<usize>,
    force_hint: &'static str,
) -> anyhow::Result<EmbedSummary> {
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
    if bar_total == 0 && stale == 0 {
        // No backlog and no stale rows: stage is silent. The summary's
        // `indexes` line will confirm "semantic ready" downstream.
        return Ok(EmbedSummary::default());
    }

    let embedder = CandleEmbedder::load()?;
    let device = embedder.device().to_owned();
    let bar = ProgressBar::with_draw_target(
        Some(bar_total as u64),
        indicatif::ProgressDrawTarget::stderr_with_hz(8),
    );
    bar.set_style(
        ProgressStyle::with_template(
            "semantic embedding [{elapsed_precise}] [{bar:24}] {pos}/{len} messages  {wide_msg}",
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
    let started = std::time::Instant::now();
    let bar_for_callback = bar.clone();
    let mut worker = EmbedWorker::new(store, &embedder)
        .with_cancel(cancel)
        .with_progress(move |progress: BatchProgress| {
            let secs = started.elapsed().as_secs_f64().max(0.001);
            let rate = progress.total_messages as f64 / secs;
            bar_for_callback.set_position(progress.total_messages as u64);
            bar_for_callback.set_message(format!("{rate:.0} msgs/s"));
        });
    if force {
        worker = worker.include_stale();
    }
    if let Some(limit) = limit {
        worker = worker.with_limit(limit);
    }
    let summary = worker.run().await?;
    bar.finish_and_clear();
    if summary.messages > 0 {
        let label = if summary.cancelled {
            "semantic embedding (interrupted)"
        } else {
            "semantic embedding"
        };
        output(&format!(
            "{}  +{} messages  ({})",
            pond::output::paint(label, pond::output::dim()),
            format_thousands(summary.messages as u64),
            device,
        ))?;
    }
    Ok(summary)
}

/// Compaction fragment cap from `[search].compaction_fragment_cap`, or the default.
fn configured_compaction_cap(config: &Config) -> usize {
    config
        .search
        .compaction_fragment_cap
        .unwrap_or(pond::substrate::DEFAULT_COMPACTION_FRAGMENT_CAP)
}

async fn run_update_indexes_stage(
    store: &Store,
    compaction_cap: usize,
) -> anyhow::Result<OptimizeOutcome> {
    let (progress, bar) = optimize_progress_bar();
    let outcome = store
        .optimize_indices(Some(progress), compaction_cap)
        .await?;
    bar.finish_and_clear();
    // No `index:` recap line: the `render_sync_summary` (or `pond status`)
    // `indexes  text + semantic ready` line is the single source of truth
    // for index health. Hints below still fire on real conflicts / failures.
    render_optimize_hints(&outcome)?;
    Ok(outcome)
}

/// Final recap of a `pond sync` run. Three-line shape:
///
/// ```text
///
/// indexes   text + semantic ready
/// added     +44 sessions, +2,337 messages
/// stored    8,460 sessions, 172,710 messages
///
/// (messages = searchable text rows; use -v for full counts)
/// ```
///
/// `added` is suppressed when both deltas are zero (a no-op sync still
/// always prints the `stored` line so an operator can confirm corpus
/// state without re-running `pond status`). The trailing disclaimer
/// goes to stderr per the rust-cli/book result-vs-meta discipline.
async fn render_sync_summary(store: &Store, summary: &SyncRunSummary) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    let (sessions_added, messages_added) = if let Some(ingest) = &summary.ingest {
        (
            ingest.sessions_inserted as u64,
            ingest.messages_inserted_searchable as u64,
        )
    } else if let Some(imported) = &summary.archive_import {
        (
            imported.inserted.sessions as u64,
            imported.inserted.messages as u64,
        )
    } else {
        (0, 0)
    };

    let (index_status, embedding, stats) = tokio::try_join!(
        store.index_status(),
        store.embedding_progress(),
        store.corpus_stats(false),
    )?;
    let health = classify_index_health(&index_status, index_lag_threshold(), &embedding);

    output("")?;
    output(&render_indexes_line(&health))?;
    if sessions_added + messages_added > 0 {
        output(&format!(
            "{}     +{} sessions, +{} messages",
            paint("added", dim()),
            format_thousands(sessions_added),
            format_thousands(messages_added),
        ))?;
    }
    output(&format!(
        "{}    {} sessions, {} messages",
        paint("stored", dim()),
        format_thousands(stats.totals.sessions),
        format_thousands(embedding.total as u64),
    ))?;
    output_err("")?;
    output_err(&paint(
        "(messages = searchable text rows; use -v for full counts)",
        dim(),
    ))?;
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
            "sync {prefix} [{elapsed_precise}] [{bar:12}] {pos}/{len} sessions  {wide_msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("##-"),
    );
    // Pad to the widest adapter name so stacked bars align.
    bar.set_prefix(format!("{name:<12}"));
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
                SyncStatus::Empty => {
                    // Sidecar/metadata files shrink the denominator so the
                    // bar lands at `N/N sessions` instead of leaving a gap.
                    let len = bar_ref.length().unwrap_or(0);
                    bar_ref.set_length(len.saturating_sub(1));
                    status_label = "empty";
                    dropped_count = 0;
                    optional_reason = None;
                }
            }
            messages += outcome.messages as u64;
            // Only surface the non-`ok`/`fresh` cases as scroll-back lines;
            // the bulk are routine successes already counted by the bar's
            // pos/len/msg counters. `pond::sync` at INFO still carries the
            // full per-session detail at `-v` verbosity.
            if !matches!(
                outcome.status,
                SyncStatus::Ok | SyncStatus::Fresh | SyncStatus::Empty
            ) {
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
            if !matches!(outcome.status, SyncStatus::Empty) {
                // Empty already shrunk `len`; ticking `pos` would over-count.
                bar_ref.inc(1);
            }
            bar_ref.set_message(format_bar_message(
                messages,
                drops,
                errors,
                started.elapsed(),
            ));
        }
    })
    .await?;

    let tail = format_sync_outcome(&summary, drops, errors);
    bar.finish_with_message(tail);
    Ok(summary)
}

/// Frozen per-adapter bar tail after `sync_with_progress` finishes. Replaces
/// the in-flight throughput display (`N msgs / X msgs/s`) with the outcome
/// counts so scroll-back tells the story of what landed, not what was
/// decoded. Empty/sidecar files are intentionally not surfaced here -
/// per design they are part of normal operation, not a signal to the user.
fn format_sync_outcome(summary: &IngestSummary, drops: u64, errors: u64) -> String {
    let new_sessions = summary.sessions_inserted as u64;
    let new_messages = summary.messages_inserted_searchable as u64;
    let mut tail = if new_sessions == 0 && new_messages == 0 {
        "up to date".to_owned()
    } else {
        format!(
            "+{} sessions (+{} messages)",
            format_thousands(new_sessions),
            format_thousands(new_messages),
        )
    };
    if drops > 0 {
        tail.push_str(&format!("  {} dropped", format_thousands(drops)));
    }
    if errors > 0 {
        tail.push_str(&format!("  {} err", format_thousands(errors)));
    }
    tail
}

/// One greppable per-session log line. Examples:
///
/// ```text
/// [00:04:32] claude-code ok    project=/Users/tenequm/Projects/pond  session=58a96901-4a4f-40be-a3c1-62419ec8c580  msgs=512
/// [00:04:33] claude-code skip  /Users/tenequm/.../58a96901-....jsonl: empty jsonl session
/// ```
fn format_sync_line(adapter: &str, outcome: &SessionOutcome, reason: Option<&str>) -> String {
    use pond::output::{dim, green, paint, red, yellow};

    let (raw_tag, tag_style) = match &outcome.status {
        SyncStatus::Ok => ("ok  ", green()),
        SyncStatus::Partial { .. } => ("part", yellow()),
        SyncStatus::Skipped { .. } => ("skip", red()),
        SyncStatus::Rejected { .. } => ("rej ", red()),
        SyncStatus::Fresh => ("fresh", green()),
        SyncStatus::Empty => ("empty", dim()),
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
    let mut out = format!(
        "{} msgs  {:.0} msgs/s",
        format_thousands(messages),
        msg_per_sec,
    );
    // Only surface the trouble counters once they're nonzero; a clean run
    // stays short enough to fit the bar without truncation.
    if drops > 0 {
        out.push_str(&format!("  {} dropped", format_thousands(drops)));
    }
    if errors > 0 {
        out.push_str(&format!("  {} err", format_thousands(errors)));
    }
    out
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
/// `PhaseStart` updates the spinner so the operator sees what's running live;
/// per-phase timing lands at `-vv` (debug) verbosity rather than the default
/// output, which carries one `indexes` line in the sync/status footer instead.
fn optimize_progress_bar() -> (OptimizeProgressFn, ProgressBar) {
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
            tracing::debug!(
                target: "pond::sync",
                table = table.as_str(),
                phase = phase.label(),
                elapsed_ms,
                "index phase done"
            );
        }
    });
    (callback, bar)
}

fn render_optimize_outcome(outcome: &OptimizeOutcome) -> anyhow::Result<()> {
    use pond::output::{bold, paint};
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
    render_optimize_hints(outcome)
}

/// Deferral and failure lines only, no per-table table. Used by `pond sync`,
/// whose summary line already carries per-table status; the table would just
/// repeat it.
fn render_optimize_hints(outcome: &OptimizeOutcome) -> anyhow::Result<()> {
    use pond::output::{dim, paint, red, yellow};
    for entry in &outcome.tables {
        if matches!(entry.compaction, PhaseOutcome::SkippedConflict) {
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
        }
        if let PhaseOutcome::Failed(error) = &entry.compaction {
            output(&paint(
                &format!("error  compaction on {}: {error:#}", entry.table.as_str()),
                yellow(),
            ))?;
        }
    }
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

fn render_status_header(
    data_url: &Url,
    sizes: &TableSizes,
    totals: &RowTotals,
) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    output(&paint("pond status", bold()))?;
    output(&format!("{}  {}", paint("data-dir", dim()), data_url))?;

    let mut table = new_table();
    let total_bytes = sizes.sessions + sizes.messages + sizes.parts + sizes.other;
    let rows = [
        ("sessions", sizes.sessions, Some(totals.sessions)),
        ("messages", sizes.messages, Some(totals.messages)),
        ("parts", sizes.parts, Some(totals.parts)),
        ("other", sizes.other, None),
    ];
    for (label, bytes, rows_opt) in rows {
        table.add_row(vec![
            Cell::new(format!("  {label}")),
            Cell::new(format_bytes(bytes)).set_alignment(CellAlignment::Right),
            Cell::new(
                rows_opt
                    .map(|n| format!("{} rows", format_thousands(n)))
                    .unwrap_or_default(),
            )
            .set_alignment(CellAlignment::Right),
        ]);
    }
    table.add_row(vec![
        Cell::new("  total").add_attribute(Attribute::Bold),
        Cell::new(format_bytes(total_bytes))
            .set_alignment(CellAlignment::Right)
            .add_attribute(Attribute::Bold),
        Cell::new(""),
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
    adapters: bool,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint, yellow};

    output("")?;
    let health = classify_index_health(index_status, index_lag_threshold(), &embedding);
    output(&render_indexes_line(&health))?;
    output(&format!(
        "{}    {} sessions, {} messages",
        paint("stored", dim()),
        format_thousands(stats.totals.sessions),
        format_thousands(embedding.total as u64),
    ))?;
    if adapters {
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

    if !adapters {
        output(&format!(
            "{}    {} adapter(s); pass `--adapters` for project tables",
            paint("sources", dim()),
            stats.adapters.len(),
        ))?;
    } else {
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
    }
    output_err("")?;
    output_err(&paint(
        "(messages = searchable text rows; use -v for full counts)",
        dim(),
    ))?;
    Ok(())
}

#[derive(Debug, Clone)]
enum IndexHealthState {
    NotBuilt,
    Ready,
    Pending(u64),
}

#[derive(Debug, Clone)]
struct IndexHealth {
    text: IndexHealthState,
    semantic: IndexHealthState,
}

/// `Ready` means the substrate's lag guard is intentionally batching; queries
/// fall through to the brute-force scan over a small remainder. `Pending(N)`
/// means the trailing fragment count crossed the threshold and a fold is owed.
fn classify_index_health(
    statuses: &[IndexStatus],
    lag_threshold: usize,
    embedding: &EmbeddingProgress,
) -> IndexHealth {
    use IndexHealthState::*;

    fn classify_one(status: &IndexStatus, lag_threshold: usize) -> IndexHealthState {
        if !status.exists {
            return NotBuilt;
        }
        if status.unindexed_rows == 0 || status.unindexed_fragments < lag_threshold {
            Ready
        } else {
            Pending(status.unindexed_rows as u64)
        }
    }

    let mut text = NotBuilt;
    let mut semantic = NotBuilt;
    for status in statuses {
        match status.intent_name.as_str() {
            MESSAGES_FTS_INDEX => text = classify_one(status, lag_threshold),
            MESSAGES_VECTOR_INDEX => semantic = classify_one(status, lag_threshold),
            _ => {}
        }
    }
    // Semantic search misses unembedded rows even when IVF_PQ's own
    // unindexed-fragments check passes.
    let embed_backlog = embedding.total.saturating_sub(embedding.embedded);
    if embed_backlog > 0 && matches!(semantic, Ready) {
        semantic = Pending(embed_backlog as u64);
    }
    IndexHealth { text, semantic }
}

fn render_indexes_line(health: &IndexHealth) -> String {
    use IndexHealthState::*;
    use pond::output::{dim, paint, yellow};

    let body = match (&health.text, &health.semantic) {
        (Ready, Ready) => "text + semantic ready".to_owned(),
        _ => {
            let text_part = match &health.text {
                Ready => "text ready".to_owned(),
                Pending(n) => format!("text {} pending", format_thousands(*n)),
                NotBuilt => "text not built".to_owned(),
            };
            let semantic_part = match &health.semantic {
                Ready => "semantic ready".to_owned(),
                Pending(n) => format!("semantic {} pending", format_thousands(*n)),
                NotBuilt => "semantic below activation threshold".to_owned(),
            };
            format!("{text_part} . {semantic_part}")
        }
    };
    let any_pending = matches!(health.text, Pending(_)) || matches!(health.semantic, Pending(_));
    let label = if any_pending {
        paint("indexes", yellow())
    } else {
        paint("indexes", dim())
    };
    format!("{label}   {body}")
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

fn render_get_envelope(
    format: OutputFormat,
    envelope: &GetEnvelope,
    session_from: SessionFrom,
) -> anyhow::Result<bool> {
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
                render_get_pretty(response, session_from)?;
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

    output(&format!(
        "{} {} matched {}  {} returned {}",
        paint("search:", dim()),
        paint(&format_thousands(response.matched_total as u64), bold()),
        if response.matched_total == 1 {
            "message"
        } else {
            "messages"
        },
        paint(&format_thousands(response.sessions.len() as u64), bold()),
        if response.sessions.len() == 1 {
            "session"
        } else {
            "sessions"
        },
    ))?;
    if response.sessions.is_empty() {
        return Ok(());
    }
    for (idx, session) in response.sessions.iter().enumerate() {
        output("")?;
        render_search_session(idx + 1, session)?;
    }
    if let Some(cursor) = response.next_cursor.as_deref() {
        output("")?;
        output(&format!("{} {}", paint("next-cursor:", dim()), cursor))?;
    }
    Ok(())
}

fn render_search_session(rank: usize, session: &SearchSession) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    let best_score = session
        .matches
        .first()
        .map(|hit| hit.score)
        .unwrap_or_default();
    output(&format!(
        "{}  best={}  {}/{} matched",
        paint(&format!("[{rank}]"), dim()),
        paint(&format!("{best_score:.4}"), bold()),
        paint(
            &format_thousands(session.matched_message_count as u64),
            bold(),
        ),
        paint(
            &format_thousands(session.session_messages_count as u64),
            bold(),
        ),
    ))?;
    output(&format!(
        "    {}  {}  {}",
        paint(&session.project, dim()),
        paint(&session.source_agent, dim()),
        paint(&session.session_id, dim()),
    ))?;
    for hit in &session.matches {
        render_search_match(hit)?;
    }
    Ok(())
}

fn render_search_match(hit: &SearchResult) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};
    output(&format!(
        "    {}  {}  {}  {}",
        paint(
            &hit.timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            dim(),
        ),
        paint_role(hit.role.as_str()),
        paint(&format!("{:.4}", hit.score), bold()),
        paint(&hit.message_id, dim()),
    ))?;
    render_hit_text(&hit.text)?;
    Ok(())
}

fn render_hit_text(text: &str) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let prefix = paint(">", dim());
    for line in text.lines() {
        output(&format!("    {prefix} {line}"))?;
    }
    Ok(())
}

fn render_session_header(session: &pond::wire::GetSession) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};
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
    ))
}

fn render_get_pretty(response: &GetResponse, session_from: SessionFrom) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    render_session_header(&response.session)?;
    match &response.result {
        GetResult::Session {
            messages,
            messages_remaining,
        } => {
            for (idx, message) in messages.iter().enumerate() {
                output("")?;
                let parts = message.parts.as_deref().unwrap_or(&[]);
                render_message_view(idx + 1, message, parts, false)?;
            }
            output("")?;
            // Tail page: the remaining messages are *earlier*, before this page;
            // after_id only pages forward, so label them "earlier" and omit the
            // dead-end cursor (the start path keeps the forward after-id cursor).
            let tail = matches!(session_from, SessionFrom::End);
            let mut footer = format!(
                "{} {} messages",
                paint("(total:", dim()),
                paint(&format_thousands(messages.len() as u64), bold()),
            );
            if *messages_remaining > 0 {
                footer.push_str(&format!(
                    " {} {}",
                    paint(&format_thousands(*messages_remaining as u64), bold()),
                    paint(if tail { "earlier" } else { "remaining [more]" }, dim()),
                ));
            }
            footer.push_str(&paint(")", dim()));
            output(&footer)?;
            if *messages_remaining > 0 {
                if tail {
                    output(&paint(
                        "session-from: start to read from the beginning",
                        dim(),
                    ))?;
                } else if let Some(last) = messages.last() {
                    output(&format!("{} {}", paint("after-id:", dim()), last.id))?;
                }
            }
        }
        GetResult::Message {
            target,
            target_parts,
            target_parts_remaining,
            siblings,
        } => {
            // Interleave the target with its siblings in timestamp order so the
            // thread reads top-to-bottom; the target carries its full parts.
            let mut thread: Vec<(&MessageView, bool)> =
                siblings.iter().map(|view| (view, false)).collect();
            thread.push((target, true));
            thread.sort_by_key(|(view, _)| view.timestamp);
            for (idx, (view, is_target)) in thread.iter().enumerate() {
                output("")?;
                let parts = if *is_target {
                    target_parts.as_slice()
                } else {
                    &[]
                };
                render_message_view(idx + 1, view, parts, *is_target)?;
            }
            if *target_parts_remaining > 0 {
                output("")?;
                output(&format!(
                    "{} {} parts remaining {}",
                    paint("(target:", dim()),
                    paint(&format_thousands(*target_parts_remaining as u64), bold()),
                    paint("[more])", dim()),
                ))?;
                if let Some(last) = target_parts.last() {
                    output(&format!("{} {}", paint("after-id:", dim()), last.id))?;
                }
            }
        }
    }
    Ok(())
}

/// Render one message view: header, text/content, then either full parts (when
/// supplied - verbatim session parts or a message-mode target) or the compact
/// part summaries otherwise.
fn render_message_view(
    rank: usize,
    view: &MessageView,
    full_parts: &[ResponsePart],
    is_target: bool,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    let marker = if is_target {
        paint("  <- target", dim())
    } else {
        String::new()
    };
    output(&format!(
        "{}  {}  {}  {}{marker}",
        paint(&format!("[{rank}]"), dim()),
        paint(
            &view.timestamp.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            dim(),
        ),
        paint_role(view.role.as_str()),
        paint(&view.id, dim()),
    ))?;
    // When full parts are present they are the complete content; rendering
    // `text` (a search_text projection of those same parts) too would just
    // double the body.
    if full_parts.is_empty() {
        if let Some(text) = &view.text {
            render_hit_text(text)?;
        }
        if let Some(content) = &view.content {
            render_hit_text(content)?;
        }
        for summary in &view.parts_summary {
            render_part_summary(summary)?;
        }
    } else {
        for part in full_parts {
            render_part(part)?;
        }
    }
    Ok(())
}

fn render_part_summary(summary: &PartSummary) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let mut line = format!("[{}]", summary.kind);
    if let Some(label) = &summary.label {
        line.push(' ');
        line.push_str(label);
    }
    if let Some(call_id) = &summary.call_id {
        line.push_str(&format!(" call_id={call_id}"));
    }
    output(&format!("    {}", paint(&line, dim())))
}

fn render_part(part: &ResponsePart) -> anyhow::Result<()> {
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
                "    {} media_type={} file_name={}",
                paint("[file]", yellow()),
                media_type.as_deref().unwrap_or("-"),
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
