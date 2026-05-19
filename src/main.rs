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
    config::{self, Config, DEFAULT_CONFIG_TOML, MaintenanceConfig},
    embed::{BatchProgress, EmbedBackend, EmbedWorker, Qwen3Embedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{AdapterStats, CorpusStats, RowTotals, StorageSizes, Store},
    transport::{self, AppState},
    wire::{
        self, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse, GetResult, Group, Hit, Message,
        Part, PartKind, ProjectFilter, SearchEnvelope, SearchFilters, SearchRequest,
        SearchResponse, SearchResultBody,
    },
};
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
        /// Registry model id used for the hybrid retriever; defaults to the
        /// registry default. FTS-only mode ignores this.
        #[arg(long)]
        model: Option<String>,
        #[arg(long, default_value = "local")]
        namespace: String,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Reciprocal-rank-fusion constant. Lower values emphasize top
        /// retriever ranks; the server default (60) is sane for most queries.
        #[arg(long, default_value_t = 60)]
        rrf_k: u32,
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
        /// Filter the export to a single session id. Default: every session.
        #[arg(long)]
        session: Option<String>,
        /// Write JSONL to this path. Default: stdout.
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
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let stats = store.corpus_stats(include_subagents).await?;
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
                     dropped_sessions={} skipped_files={} skipped_fresh={} storage_errors={}",
                    pond::output::paint(&format!("sync {name}:"), pond::output::dim()),
                    summary.inserted,
                    summary.matched,
                    summary.dropped_events,
                    summary.dropped_sessions,
                    summary.skipped_files,
                    summary.skipped_fresh,
                    summary.storage_errors,
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
            store.ensure_indices().await?;
            let retention = chrono::Duration::days(
                i64::try_from(loaded.maintenance.retention_days).unwrap_or(i64::MAX),
            );
            let report = store.maintenance(retention).await;
            output(&format!(
                "{} versions_removed={} bytes_reclaimed={} tables_optimized={} tables_failed={}",
                pond::output::paint("maintenance:", pond::output::dim()),
                report.versions_removed,
                report.bytes_reclaimed,
                report.tables_optimized,
                report.tables_failed,
            ))?;
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
                "{} model={} batches={} messages={}",
                pond::output::paint("embed:", pond::output::dim()),
                model.id,
                summary.batches,
                summary.messages,
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
            let embedder: Option<Arc<dyn EmbedBackend>> = if config.embeddings.enabled {
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
            let embedder: Option<Arc<dyn EmbedBackend>> = if config.embeddings.enabled {
                Some(Arc::new(Qwen3Embedder::load(&resolved_model)?))
            } else {
                None
            };
            // `pond mcp` writes only JSON-RPC frames to stdout; the maintenance
            // task is `pond serve`-only, so it is not spawned here.
            transport::mcp::serve_stdio(AppState { store, embedder }).await?;
        }
        Command::Search {
            query,
            data_dir,
            config,
            model,
            namespace,
            limit,
            rrf_k,
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
            let resolved_model = config::resolve_model(&loaded, model.as_deref(), &namespace)?;
            let store = Store::open_with_options(&data_dir, storage_map(&loaded)).await?;
            let embedder: Option<Arc<dyn EmbedBackend>> = if loaded.embeddings.enabled {
                Some(Arc::new(Qwen3Embedder::load(&resolved_model)?))
            } else {
                None
            };
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                query,
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
                "{} sessions={} messages={} parts={}",
                pond::output::paint("export:", pond::output::dim()),
                summary.sessions,
                summary.messages,
                summary.parts,
            ))?;
        }
    }

    Ok(())
}

/// Spawn the background maintenance task: `cleanup_old_versions` +
/// `optimize_indices` every `interval_secs` (design.md#schemas-write-params). The first tick
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
            let report = store.maintenance(retention).await;
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

/// Render `pond status` as: a header + storage breakdown table on top, a
/// totals line, and one section per adapter in registry order with a project
/// table. Tables width-adapt via comfy-table; on non-TTY stdout (piped to a
/// file or test) coloring strips automatically via `pond::output::paint`.
fn render_status(stats: &CorpusStats, sizes: Option<&StorageSizes>) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};

    output(&paint("pond status", bold()))?;
    output(&format!("{}  {}", paint("data-dir", dim()), stats.data_url))?;

    match sizes {
        Some(sizes) => {
            let mut table = new_table();
            for (label, bytes) in [
                ("sessions", sizes.sessions),
                ("messages", sizes.messages),
                ("parts", sizes.parts),
                ("embeddings", sizes.embeddings),
                ("other", sizes.other),
            ] {
                table.add_row(vec![
                    Cell::new(format!("  {label}")),
                    Cell::new(format_bytes(bytes)).set_alignment(CellAlignment::Right),
                ]);
            }
            table.add_row(vec![
                Cell::new("  total").add_attribute(Attribute::Bold),
                Cell::new(format_bytes(sizes.total()))
                    .set_alignment(CellAlignment::Right)
                    .add_attribute(Attribute::Bold),
            ]);
            output(&table.to_string())?;
        }
        None => {
            output(&paint(
                "  (size on disk unavailable for remote backends; wired with S3 stage)",
                dim(),
            ))?;
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
        "{}  {} sessions  {} messages  {} parts  {} embeddings",
        paint("totals", dim()),
        paint(&format_thousands(sessions), bold()),
        paint(&format_thousands(messages), bold()),
        paint(&format_thousands(parts), bold()),
        paint(&format_thousands(embeddings), bold()),
    ))?;
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
    render_preview(&hit.preview)?;
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
    render_preview(&group.preview)?;
    Ok(())
}

fn render_preview(preview: &str) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    if preview.is_empty() {
        return Ok(());
    }
    let prefix = paint(">", dim());
    for line in preview.lines() {
        output(&format!("    {prefix} {line}"))?;
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
        render_preview(content)?;
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
