use std::{
    collections::HashMap,
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
    embed::{BatchProgress, E5Embedder, EmbedBackend, EmbedWorker, LazyEmbedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{AdapterStats, CorpusStats, EmbeddingProgress, RowTotals, Store},
    substrate::TableSizes,
    transport::{self, AppState},
    wire::{
        self, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse, GetResult, Group, Hit, Message,
        Part, PartKind, ProjectFilter, SearchEnvelope, SearchFilters, SearchModeWire,
        SearchRequest, SearchResponse, SearchResultBody,
    },
};

/// CLI surface for `pond search --mode`. Maps 1:1 to `SearchModeWire`; kept
/// separate so the clap derive lives next to the rest of the CLI types.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliSearchMode {
    Fts,
    Vector,
    Hybrid,
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
    /// Print basic binary status.
    Status {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
        /// Show one section per `source_agent` (including sub-agents like
        /// `claude-code/general-purpose`). Default rolls sessions up to the
        /// main agent only.
        #[arg(long)]
        include_subagents: bool,
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
    /// Embed the backlog of un-embedded messages (spec.md#search). Idempotent:
    /// the backlog is every message with a null `vector`, so a re-run picks up
    /// exactly where the last one stopped. A model swap (rows embedded under
    /// a different model id) requires `--force`, which clears those rows and
    /// drops the IVF_PQ before the new vectors land.
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
    },
    /// Run the stdio MCP server only. stdout is reserved for JSON-RPC frames;
    /// all diagnostics go to stderr.
    Mcp {
        #[arg(long, env = "POND_DATA_DIR", value_parser = parse_data_dir)]
        data_dir: Option<Url>,
        #[arg(long, env = "POND_CONFIG")]
        config: Option<PathBuf>,
    },
    /// Inspect configuration.
    Config {
        /// Print the fully-annotated config.toml schema.
        #[arg(long)]
        print_schema: bool,
    },
    /// Hybrid (vector + BM25 + RRF) search over stored messages. Mirrors the
    /// `pond_search` MCP tool: hybrid mode kicks in automatically when
    /// embeddings exist for the resolved model, FTS-only otherwise. The
    /// pretty default is human-readable; `--format json` emits the wire
    /// envelope verbatim for scripting.
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
        /// Reciprocal-rank-fusion constant. Lower values emphasize top
        /// retriever ranks; the server default is the same.
        #[arg(long, default_value_t = 10)]
        rrf_k: u32,
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
        #[arg(long, value_enum, default_value_t = OutputFormat::Pretty)]
        format: OutputFormat,
    },
    /// Fetch a session or a single message (with optional thread context),
    /// mirroring the `pond_get` MCP tool. Exactly one of `--session-id` or
    /// `--message-id` is required.
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
        /// Write JSONL to this path. Default: stdout.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
        #[command(subcommand)]
        command: Option<ExportCommand>,
    },
}

#[derive(Debug, Subcommand)]
enum ExportCommand {
    /// Export one session as canonical JSONL or restore it to a client format.
    Session {
        id: String,
        /// Restore the session to this adapter's native client format.
        #[arg(long = "as")]
        as_adapter: Option<String>,
        /// Canonical mode: output file. Restore mode: required output directory.
        #[arg(long, short = 'o')]
        out: Option<PathBuf>,
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
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Command::Status {
            data_dir,
            config,
            include_subagents,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = open_store_with_spinner(&data_dir, storage_map(&loaded)).await?;
            let stats = store.corpus_stats(include_subagents).await?;
            let sizes = store.table_sizes().await?;
            let unindexed = store.unindexed_message_backlog().await?;
            let vector_unindexed = store.unindexed_vector_backlog().await?;
            let embedding = store.embedding_progress().await?;
            // Sample is bounded so this remains O(sample) and `pond status`
            // stays sub-second on a million-message corpus.
            let scripts = store.text_script_histogram(2000).await?;
            render_status(
                &stats,
                &sizes,
                unindexed,
                vector_unindexed,
                embedding,
                &scripts,
            )?;
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
            let store = open_store_with_spinner(&data_dir, storage_map(&loaded)).await?;
            let sources =
                resolve_sync_sources(&loaded, &config_file, adapter.as_deref(), source_dir)?;
            let started = std::time::Instant::now();
            let map = store.session_last_ingested_at().await?;
            tracing::info!(
                target: "pond::sync",
                sessions = map.len(),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "loaded staleness-skip watermarks",
            );
            let oracle = StoredWatermarks::new(map);
            for (name, config) in sources {
                let summary = sync_with_progress(&store, &name, config, &oracle).await?;
                output(&format!(
                    "{} inserted={} matched={} dropped_events={} \
                     dropped_sessions={} skipped_files={} skipped_fresh={} \
                     storage_errors={} truncated_values={}",
                    pond::output::paint(&format!("sync {name}:"), pond::output::dim()),
                    summary.inserted,
                    summary.matched,
                    summary.dropped_events,
                    summary.dropped_sessions,
                    summary.skipped_files,
                    summary.skipped_fresh,
                    summary.storage_errors,
                    summary.truncated_values,
                ))?;
                // Top-N drop reasons follow the summary line. Empty when
                // nothing dropped, which is the common case. The bucket
                // keys are stable `&'static str` (DROP_REASON_*) so the
                // operator can grep for them in the sync log or use them
                // as predicates in scripted post-sync analysis.
                if !summary.drop_reasons.is_empty() {
                    let mut reasons: Vec<(&&'static str, &usize)> =
                        summary.drop_reasons.iter().collect();
                    reasons.sort_by(|a, b| b.1.cmp(a.1));
                    let top = reasons
                        .iter()
                        .take(3)
                        .map(|(k, v)| format!("{k}={v}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let suffix = if reasons.len() > 3 {
                        format!(" (+{} more)", reasons.len() - 3)
                    } else {
                        String::new()
                    };
                    output(&format!(
                        "  {} {top}{suffix}",
                        pond::output::paint("top drop reasons:", pond::output::dim()),
                    ))?;
                }
            }
            // spec.md#fold-on-write: every ingest batch lands data with its
            // index folds in one atomic merge, so by the time the loop above
            // returns every maintained index covers every appended row. No
            // explicit post-sync upkeep step. A non-zero backlog here would
            // indicate an interrupted write; open-time reconciliation
            // self-heals it on the next open.
            let unindexed = store.unindexed_message_backlog().await?;
            if unindexed > 0 {
                output(&pond::output::paint(
                    &format!(
                        "warning: FTS index trails data ({unindexed} unindexed \
                         messages) - the next `pond` open will reconcile"
                    ),
                    pond::output::yellow(),
                ))?;
            }
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

            // Model-swap detection: rows with a vector under a different
            // model id are silent-correctness landmines (IVF_PQ centroids
            // belong to one distance space; mixing two corrupts neighbors).
            // Require explicit `--force`, then clear those rows and drop the
            // IVF_PQ before the worker writes the new vectors.
            let stale = store.stale_embedding_count().await?;
            if stale > 0 {
                if !force {
                    bail!(
                        "{stale} message(s) embedded under a different model id; pass \
                         `--force` to re-embed (the stale vectors will be cleared and \
                         the IVF_PQ rebuilt under the configured model {:?})",
                        pond::embed::model_id(),
                    );
                }
                output(&pond::output::paint(
                    &format!(
                        "embed: --force: clearing {} stale-model row(s) and dropping IVF_PQ",
                        format_thousands(stale as u64),
                    ),
                    pond::output::yellow(),
                ))?;
                // Enumerate stale keys (streamed), then clear in one
                // merge_update which fold-on-write-prunes the rewritten
                // fragments from any existing IVF_PQ coverage. Then drop
                // the IVF_PQ outright so the next merge re-bootstraps it.
                use tokio_stream::StreamExt;
                let mut stream = Box::pin(store.stale_embedding_keys());
                let mut stale_keys = Vec::with_capacity(stale);
                while let Some(key) = stream.next().await {
                    stale_keys.push(key?);
                }
                store.clear_embeddings(&stale_keys).await?;
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
            let embedder = E5Embedder::load()?;
            // `indicatif` auto-detects tty and degrades to log-line output in
            // CI / non-tty contexts, so this is safe to always wire.
            let bar = ProgressBar::new(bar_total as u64);
            bar.set_style(
                ProgressStyle::with_template(
                    "{spinner:.green} embed [{elapsed_precise}] [{bar:24}] {pos}/{len} ({percent}%) {per_sec} eta {eta}",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("##-"),
            );
            bar.enable_steady_tick(Duration::from_millis(120));
            // First Ctrl-C: cooperative drain (worker exits after the next
            // window write, indices still rebuild). Second Ctrl-C: terminate
            // hard with the SIGINT exit code so the user can always escape.
            let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let cancel = cancel.clone();
                tokio::spawn(async move {
                    let _ = tokio::signal::ctrl_c().await;
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    eprintln!(
                        "\ninterrupted; flushing window and finishing indices (Ctrl-C again to abort)..."
                    );
                    let _ = tokio::signal::ctrl_c().await;
                    std::process::exit(130);
                });
            }
            let bar_for_callback = bar.clone();
            let mut worker = EmbedWorker::new(&store, &embedder)
                .with_cancel(cancel)
                .with_progress(move |progress: BatchProgress| {
                    bar_for_callback.set_position(progress.total_messages as u64);
                });
            if let Some(limit) = limit {
                worker = worker.with_limit(limit);
            }
            let summary = worker.run().await?;
            // spec.md#fold-on-write: every per-window merge_update inside
            // the worker landed its column write together with the index
            // folds; the specific merge that crossed
            // VECTOR_INDEX_ACTIVATION_ROWS triggered the IVF_PQ creation
            // (sized to the live vector count) inline. There is no
            // post-embed upkeep verb - the seam owns the contract.
            bar.finish_and_clear();
            output(&format!(
                "{} done: batches={} messages={} device={}{}",
                pond::output::paint("embed:", pond::output::dim()),
                summary.batches,
                summary.messages,
                embedder.device(),
                if summary.cancelled {
                    " (interrupted)"
                } else {
                    ""
                },
            ))?;
        }
        Command::Serve {
            host,
            port,
            data_dir,
            config,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store = Arc::new(open_store_with_spinner(&data_dir, storage_map(&config)).await?);
            // Lazy: idle `pond serve` keeps RSS ~50 MB; the candle/Metal model
            // load (~600 MB) only triggers when the first hybrid search asks.
            let embedder = Arc::new(LazyEmbedder::new(config.embeddings.enabled));
            let state = AppState { store, embedder };
            transport::http::serve(state, host, port).await?;
        }
        Command::Mcp { data_dir, config } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let config = Config::load(config_path(config, &data_dir))?;
            let store = Arc::new(open_store_with_spinner(&data_dir, storage_map(&config)).await?);
            // Lazy: idle `pond mcp` instances in every Claude Code session
            // stay light. The model load only happens once per process on the
            // first `pond_search` tool call that needs hybrid retrieval.
            let embedder = Arc::new(LazyEmbedder::new(config.embeddings.enabled));
            transport::mcp::serve_stdio(AppState { store, embedder }).await?;
        }
        Command::Search {
            query,
            data_dir,
            config,
            namespace,
            limit,
            rrf_k,
            mode,
            no_boost_recent,
            group_by_conversation,
            project,
            session_id,
            source_agent,
            from_date,
            to_date,
            role,
            min_score,
            format,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            let store = open_store_with_spinner(&data_dir, storage_map(&loaded)).await?;
            // One-shot CLI: the embedder load cost has to be paid anyway if
            // hybrid/vector mode wins, so load eagerly. `LazyEmbedder::get`
            // would also work but adds an `.await` for zero benefit here.
            // `--mode hybrid|vector` is an operator override that forces a
            // load even when `config.embeddings.enabled = false`, so the
            // benchmark harness can run against any corpus that already has
            // vectors populated regardless of the user's daily-config switch.
            let force_embedder =
                matches!(mode, Some(CliSearchMode::Hybrid | CliSearchMode::Vector));
            let embedder = if force_embedder || loaded.embeddings.enabled {
                Some(Arc::new(E5Embedder::load()?) as Arc<dyn EmbedBackend>)
            } else {
                None
            };
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                query,
                mode_override: mode.map(SearchModeWire::from),
                rrf_k,
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
                limit,
            };
            let envelope = handlers::pond_search(&store, embedder.as_deref(), request).await;
            if !render_search_envelope(format, &envelope)? {
                std::process::exit(1);
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
            let store = open_store_with_spinner(&data_dir, storage_map(&loaded)).await?;
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
            command,
        } => {
            let data_dir = resolve_data_dir(data_dir)?;
            let loaded = Config::load(config_path(config, &data_dir))?;
            // Export streams raw data through the canonical model and never
            // reads an index, so open through the minimal path: no policy,
            // no open-time creation, no fold work.
            let store = Store::open_minimal(&data_dir, storage_map(&loaded)).await?;
            match command {
                None => {
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
                        "{} sessions={} messages={} parts={}",
                        pond::output::paint("export:", pond::output::dim()),
                        summary.sessions,
                        summary.messages,
                        summary.parts,
                    ))?;
                }
                Some(ExportCommand::Session {
                    id,
                    as_adapter,
                    out,
                }) => {
                    if let Some(target) = as_adapter {
                        let out_dir = out.context("export session --as requires --out <dir>")?;
                        let (session_count, file_count) =
                            restore_session(&store, &id, &target, &out_dir).await?;
                        output(&format!(
                            "{} sessions={} target={} files={}",
                            pond::output::paint("restore:", pond::output::dim()),
                            session_count,
                            target,
                            file_count,
                        ))?;
                    } else {
                        let summary = match out {
                            Some(path) => {
                                let file =
                                    tokio::fs::File::create(&path).await.with_context(|| {
                                        format!("failed to open {}", path.display())
                                    })?;
                                let mut writer = tokio::io::BufWriter::new(file);
                                let summary =
                                    handlers::pond_export(&store, Some(&id), &mut writer).await?;
                                writer.flush().await.context("export: flush")?;
                                summary
                            }
                            None => {
                                let mut stdout = tokio::io::stdout();
                                handlers::pond_export(&store, Some(&id), &mut stdout).await?
                            }
                        };
                        output(&format!(
                            "{} sessions={} messages={} parts={}",
                            pond::output::paint("export:", pond::output::dim()),
                            summary.sessions,
                            summary.messages,
                            summary.parts,
                        ))?;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn restore_session(
    store: &Store,
    session_id: &str,
    target: &str,
    out_dir: &Path,
) -> anyhow::Result<(usize, usize)> {
    let factory = adapter::by_name(target).with_context(|| {
        format!(
            "unknown adapter {target}; known: {}",
            adapter::known_names().join(", ")
        )
    })?;
    let sessions = handlers::restore_lineage(store, session_id).await?;

    let mut file_count = 0usize;
    for session in &sessions {
        // `source_agent` is `brand` or `brand/sub`; the brand prefix picks fidelity.
        let source_agent = &session.session.source_agent;
        let brand = source_agent.split('/').next().unwrap_or(source_agent);
        let fidelity = if brand == factory.name() {
            adapter::RestoreFidelity::Native
        } else {
            adapter::RestoreFidelity::Foreign
        };
        for file in factory.serialize(session, fidelity)? {
            let path = out_dir.join(&file.relative_path);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            tokio::fs::write(&path, file.bytes)
                .await
                .with_context(|| format!("failed to write {}", path.display()))?;
            file_count += 1;
        }
    }
    Ok((sessions.len(), file_count))
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

/// Open the store with an indicatif spinner ticking while
/// [`Store::open_with_options`] runs. Open is normally instant, but it can be
/// slow when the substrate's open-time fold-and-create pass has real work to
/// do (a freshly bulk-loaded data dir, or an interrupted prior write's
/// trail to reconcile). Surfacing it visibly keeps the CLI honest about
/// where the latency went.
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
                SyncStatus::Partial { dropped_events } => {
                    drops += *dropped_events as u64;
                    status_label = "partial";
                    dropped_count = *dropped_events;
                    optional_reason =
                        Some(format!("dropped {dropped_events} event(s) mid-session"));
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
            bar_ref.println(format_sync_line(name, &outcome, optional_reason.as_deref()));
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

/// Render `pond status` as: a header + storage breakdown table on top, a
/// totals line, and one section per adapter in registry order with a project
/// table. Tables width-adapt via comfy-table; on non-TTY stdout (piped to a
/// file or test) coloring strips automatically via `pond::output::paint`.
fn render_status(
    stats: &CorpusStats,
    sizes: &TableSizes,
    unindexed: usize,
    vector_unindexed: usize,
    embedding: EmbeddingProgress,
    scripts: &[(String, usize)],
) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint, yellow};

    output(&paint("pond status", bold()))?;
    output(&format!("{}  {}", paint("data-dir", dim()), stats.data_url))?;

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
    if unindexed == 0 {
        output(&format!("{}  complete", paint("fts index", dim())))?;
    } else {
        // spec.md#fold-on-write: this should be zero between commits. A
        // non-zero count means the last write was interrupted; the next
        // `pond` open will reconcile.
        output(&paint(
            &format!("fts index  {unindexed} messages unindexed - self-heals on next open"),
            yellow(),
        ))?;
    }
    // Below the activation threshold no IVF_PQ is built (brute-force vector
    // scan is exact at that size); above it, `vector_unindexed` is the count
    // of rows in fragments rewritten since the index was last rebuilt, so a
    // non-zero value means vector search misses those rows.
    if embedding.embedded < pond::substrate::VECTOR_INDEX_ACTIVATION_ROWS {
        output(&format!(
            "{}  {} vectors  not indexable yet (activates at {})",
            paint("vector index", dim()),
            paint(&format_thousands(embedding.embedded as u64), bold()),
            format_thousands(pond::substrate::VECTOR_INDEX_ACTIVATION_ROWS as u64),
        ))?;
    } else if vector_unindexed == 0 {
        output(&format!(
            "{}  complete ({} vectors)",
            paint("vector index", dim()),
            paint(&format_thousands(embedding.embedded as u64), bold()),
        ))?;
    } else {
        // spec.md#fold-on-write: this should be zero between commits;
        // open-time reconciliation folds the trail on the next open.
        output(&paint(
            &format!(
                "vector index  {} vectors  {} unindexed - self-heals on next open",
                format_thousands(embedding.embedded as u64),
                format_thousands(vector_unindexed as u64),
            ),
            yellow(),
        ))?;
    }
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
                "embeddings  {}/{} messages  model={} - run `pond embed` to fill the {} backlog",
                format_thousands(embedding.embedded as u64),
                format_thousands(embedding.total as u64),
                embedding.model,
                format_thousands(pending as u64),
            ),
            yellow(),
        ))?;
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
    if !stats.include_subagents {
        output(&paint(
            "  note: totals above include sub-agent sessions; the rollup below shows main-agent only. Pass `--include-subagents` for the per-agent breakdown.",
            dim(),
        ))?;
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
