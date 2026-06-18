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
use clap::{ArgGroup, CommandFactory, Parser, Subcommand, ValueEnum};
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Table, presets::NOTHING};
use indicatif::{ProgressBar, ProgressStyle};
use pond::{
    PROTOCOL_VERSION, adapter,
    config::{self, Config, DEFAULT_CONFIG_TOML},
    embed::{BatchProgress, CandleEmbedder, EmbedSummary, EmbedWorker, Embedder, LazyEmbedder},
    handlers::{self, IngestSummary, SessionOutcome, SyncEvent, SyncStatus},
    sessions::{
        EmbeddingProgress, LanceArchiveCounts, LanceArchiveExport, LanceArchiveImport,
        MESSAGES_FTS_INDEX, MESSAGES_VECTOR_INDEX, OptimizeOutcome, RowTotals, Store,
    },
    substrate::{
        self, CheckFailure, CredsBinding, IndexStatus, MaintenancePolicy, OptimizeEvent,
        OptimizeProgressFn, PhaseOutcome, ResolvedStorage, StorageUrl, TableSizes,
        VECTOR_INDEX_ACTIVATION_ROWS, default_cleanup_older_than,
    },
    transport::{self, AppState},
    wire::{
        self, ErrorEnvelope, GetEnvelope, GetRequest, GetResponse, GetResult, MessageView,
        PartKind, PartSummary, ProjectFilter, ResponseMode, ResponsePart, SearchEnvelope,
        SearchFilters, SearchModeWire, SearchRequest, SearchResponse, SearchResult, SearchSession,
        SessionFrom,
    },
};

// Bin-only subsystems: the interactive setup wizard and the OS-scheduler
// integration. Neither has a library caller, so they stay out of `pond::`.
mod init;
mod schedule;

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
enum OptimizeStage {
    Embed,
    Index,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ServeTransport {
    Http,
    Stdio,
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

/// CLI surface for `pond sql --format`. Maps to `sql::Mode` / `sql::Format`.
/// Mirrors the MCP `pond_sql_query` `format` arg (text|json|parquet|ndjson).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CliSqlFormat {
    Text,
    Json,
    Ndjson,
    Parquet,
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
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};
use url::Url;

/// The platform default local data dir (`$XDG_DATA_HOME/pond`, then
/// `~/.local/share/pond`) as a `StorageUrl`. Backs both the no-config fallback
/// and the `local` keyword so they resolve to exactly the same place.
fn default_local_storage() -> anyhow::Result<StorageUrl> {
    let url = pond::config::default_storage_path(
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )?;
    StorageUrl::parse(url.as_str())
}

/// The search row-key map's cache dir. Resolved in this env-owning layer and
/// passed into `prewarm` so `Store` stays env-agnostic.
fn default_cache_dir() -> PathBuf {
    pond::config::default_cache_path(
        std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

/// Adapter clap can call to parse `--storage-path` / `POND_STORAGE_PATH`:
/// bare paths, `~/...`, `file://`, and the remote URL grammar
/// (spec.md#storage-url-grammar) including the fat `s3+https://` form. The bare
/// keyword `local` (or `default`, case-insensitive) resolves to the platform
/// default local data dir - so rolling back never needs the path memorized.
/// A directory literally named `local` is still reachable as `./local`.
fn parse_storage_path(input: &str) -> anyhow::Result<StorageUrl> {
    if matches!(
        input.trim().to_ascii_lowercase().as_str(),
        "local" | "default"
    ) {
        return default_local_storage();
    }
    StorageUrl::parse(input)
}

/// First executable named `name` on `PATH`. Shared by the `pond init` MCP
/// probe and the scheduler's stable-binary resolution (`schedule::pond_bin`).
fn find_on_path(name: &str) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    std::env::split_paths(&paths).find_map(|dir| {
        let candidate = dir.join(name);
        is_executable(&candidate).then_some(candidate)
    })
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

/// Help palette tied to `pond::output`: bold headers/usage, cyan literals,
/// dim placeholders. clap's styling types are anstyle re-exports, so this is
/// the same color stack the rest of the CLI renders with.
const STYLES: clap::builder::styling::Styles = clap::builder::styling::Styles::styled()
    .header(anstyle::Style::new().bold())
    .usage(anstyle::Style::new().bold())
    .literal(anstyle::AnsiColor::Cyan.on_default())
    .placeholder(anstyle::Style::new().dimmed());

/// `--version` detail line: crate version plus the release commit (stamped
/// into release builds via `POND_BUILD_COMMIT`) and the build target. `-V`
/// stays the short crate version.
static LONG_VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    let target = format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS);
    match option_env!("POND_BUILD_COMMIT") {
        Some(commit) if !commit.is_empty() => {
            format!("{} ({commit} {target})", env!("CARGO_PKG_VERSION"))
        }
        _ => format!("{} ({target})", env!("CARGO_PKG_VERSION")),
    }
});

/// Root `-h`/`--help` command list, hand-grouped into blocks with blank-line
/// spacing. clap can't group or space subcommands natively (clap-rs/clap#1553;
/// PR #6183 unmerged as of 4.6), so the visible list lives here while
/// `{options}` and `{after-help}` render the rest. The
/// `help_template_lists_every_subcommand` test fails if a verb is added to
/// `Command` without a line here.
const HELP_TEMPLATE: &str = "\
{about-with-newline}
{usage-heading} {usage}

Commands:
  Setup
    init         Set up pond (idempotent: safe to re-run)
    adapters     Choose which adapters pond sync ingests
    storage      Probe and switch storage destinations
    creds        Manage URL-scoped credential sets
    schedule     Manage the automatic sync schedule
    config       Inspect configuration

  Data flow
    sync         Make pond current: import, embed, index
    optimize     Embed the backlog, then fold the indexes
    copy         Copy data between stores, archives, JSONL

  Query
    search       Search stored messages
    get          Fetch a session or message
    sql          Run one read-only SQL query
    status       Show pond health, data, and adapters

  Serve
    serve        Run the HTTP API server
    mcp          Serve the MCP tools over stdio

  Shell
    completions  Generate shell completions
    help         Print this message or a subcommand's help

Options:
{options}{after-help}";

/// Lossless storage and hybrid search for sessions from any AI agent client.
#[derive(Debug, Parser)]
#[command(
    name = "pond",
    version,
    long_version = LONG_VERSION.as_str(),
    styles = STYLES,
    max_term_width = 100,
    help_template = HELP_TEMPLATE,
    after_long_help = "\
Getting started:
  pond init                                  set up storage, adapters, MCP, and scheduling
  pond sync                                  import new sessions, embed, update indexes
  pond search \"that auth refactor\"           find past work
  claude mcp add -s user pond -- pond mcp    register pond as an MCP server in Claude Code

Every command documents itself: `pond <command> --help` carries examples."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    // `global = true` on these args, so they resolve at the root
    // (`pond --storage-path X status`) and after any subcommand alike.
    #[command(flatten)]
    store: StoreArgs,
    #[command(flatten)]
    #[command(next_help_heading = "Global options")]
    verbose: clap_verbosity_flag::Verbosity<clap_verbosity_flag::WarnLevel>,
}

/// Storage and config selectors. Flattened once into the root `Cli` with
/// `global = true` args, so every subcommand inherits them and they parse
/// before or after the subcommand name.
#[derive(Debug, clap::Args)]
#[command(next_help_heading = "Global options")]
struct StoreArgs {
    /// Storage destination: a local path or remote URL.
    ///
    /// Accepts a bare path, `~/path`, `file://`, `s3://bucket/prefix`,
    /// `s3+https://host/bucket/prefix`, `gs://`, `az://`, or the keyword
    /// `local` (the platform default local data dir). Default:
    /// `[storage].path` from config, then the platform data dir
    /// (`~/.local/share/pond`).
    #[arg(
        long,
        global = true,
        env = "POND_STORAGE_PATH",
        hide_env_values = true,
        value_parser = parse_storage_path,
        value_name = "URL"
    )]
    storage_path: Option<StorageUrl>,
    /// Config file to read (default: `~/.config/pond/config.toml`).
    #[arg(
        long = "config-file",
        global = true,
        env = "POND_CONFIG_FILE",
        hide_env_values = true,
        value_name = "PATH"
    )]
    config: Option<PathBuf>,
}

// Parsed once, matched once, immediately destructured - the size spread
// between variants (StorageUrl-carrying vs unit-like) has no runtime cost.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Set up pond (idempotent: safe to re-run).
    ///
    /// Walks through storage, adapters, MCP registration, and an
    /// optional sync schedule, then writes config.toml in one pass at the
    /// end. Re-running repairs or updates an existing setup; flags answer
    /// sections non-interactively.
    #[command(after_long_help = "Examples:
  pond init                                   interactive setup (or repair)
  pond init --yes                             accept defaults, no prompts
  pond init --storage-path s3://bucket/pond   preset storage, prompt for the rest
  pond init --adapters claude-code,codex-cli --yes
  pond init --every 1h --yes                  also register an hourly sync")]
    // The visible `-h` command list (grouping + blank-line spacing) is rendered
    // by HELP_TEMPLATE, not by these display_order values - clap can't group or
    // space subcommands itself. display_order is kept only as the fallback order
    // if the template is ever removed.
    #[command(display_order = 1)]
    Init(init::InitArgs),
    /// Make pond current: import, embed, update indexes.
    ///
    /// The everyday command: pulls fresh sessions from every enabled
    /// `[adapters.*]` entry (or one named adapter), embeds the backlog, and
    /// folds new rows into the search indexes. It only ever syncs adapters you
    /// have already enabled - enabling one is an explicit step (`pond adapters
    /// enable` / `pond adapters discover` / `pond init`), never a side effect
    /// of sync.
    #[command(after_long_help = "Examples:
  pond sync                                  sync every enabled adapter
  pond sync claude-code                      sync one enabled adapter
  pond sync codex-cli --path ~/backup        one-off path override, config untouched
  pond sync --no-optimize                    import only; embed and index later
  pond sync --verify                         full re-read; heal anything the skip missed")]
    #[command(display_order = 7)]
    Sync {
        /// Adapter name (claude-code, codex-cli, ...); default: every enabled adapter.
        adapter: Option<String>,
        /// One-off source-path override (requires <ADAPTER>).
        ///
        /// Bypasses `[adapters.<adapter>]` and does not modify config.toml.
        #[arg(long, value_name = "DIR")]
        path: Option<PathBuf>,
        /// Import only: skip the embed + index maintenance that normally runs
        /// after the import. Catch up later with `pond optimize`.
        #[arg(long)]
        no_optimize: bool,
        /// Reconcile pass: bypass the freshness skip and re-read every source
        /// body, re-ingesting through the idempotent merge. The skip compares
        /// source mtime to pond's per-session ingest watermark; a session that
        /// was partially flushed before the commit-row-last fix kept a frozen
        /// watermark that mtime can never re-read past. This is the body-reading
        /// tier that heals that historical damage (and audits completeness). It
        /// is slower - every file is decoded - so it is opt-in, not the default.
        #[arg(long)]
        verify: bool,
    },
    /// Manage which adapters `pond sync` ingests (the `[adapters.*]` entries).
    ///
    /// Enabling an adapter is the explicit owner of adapter state: `list` shows
    /// what is configured and what was detected, `discover` probes and enables
    /// interactively, `enable`/`disable` flip one by name. `pond sync` reads
    /// these but never writes them.
    #[command(after_long_help = "Examples:
  pond adapters list                         configured + detected adapters
  pond adapters discover                     probe this machine, pick what to enable
  pond adapters enable claude-code           enable one (discovers its path if needed)
  pond adapters disable opencode             stop syncing one, keep it on record")]
    #[command(display_order = 2)]
    Adapters {
        #[command(subcommand)]
        command: AdaptersCmd,
    },
    /// Show pond health, data, and adapter status.
    ///
    /// Storage destination and size, row counts, index readiness, embedding
    /// backlog, configured adapters, and the sync schedule. Anything off
    /// names its fix inline.
    #[command(after_long_help = "Examples:
  pond status                       the one-screen overview
  pond status --include-subagents   count each subagent as its own adapter")]
    #[command(display_order = 13)]
    Status {
        /// Count each sub-agent `source_agent` (e.g. `claude-code/general-purpose`)
        /// as its own adapter. Default counts only main agents.
        #[arg(long)]
        include_subagents: bool,
        /// Output format: `text` (human tables) or `json` (one document).
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Search stored messages.
    ///
    /// Hybrid retrieval (semantic + full-text) when embeddings exist,
    /// full-text otherwise. Keep the query about concepts; scope with
    /// `--project`, `--from-date`, and friends instead of putting names in
    /// the query.
    #[command(after_long_help = "Examples:
  pond search \"lance compaction tuning\"
  pond search \"auth retry\" --project pond --limit 5
  pond search \"migration plan\" --from-date 2026-05-01 --format json")]
    #[command(display_order = 10)]
    Search {
        /// Free-text query. Semantic concepts work best; project names belong
        /// in `--project`.
        query: String,
        /// Tenant namespace. The personal pond has exactly one; leave at the
        /// default.
        #[arg(long, default_value = "local")]
        namespace: String,
        /// Max sessions to return.
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Operator-only retrieval mode override.
        ///
        /// Production callers should omit this and let the server pick
        /// (hybrid when embeddings exist, FTS-only otherwise); benchmark and
        /// ablation harnesses use it to force one arm against the same corpus.
        #[arg(long, value_enum)]
        mode: Option<CliSearchMode>,
        /// Project filter: substring match by default.
        ///
        /// `--project pond` -> contains "pond". Prefix with `re:` for regex
        /// (`--project 're:^/Users/.*/x402'`); `lit:` escapes a literal value
        /// that would otherwise be parsed as a prefix.
        #[arg(long, value_parser = parse_project_filter)]
        project: Option<ProjectFilter>,
        /// Filter to one session (exact match) - search within a single,
        /// possibly long, session.
        #[arg(long, value_name = "ID")]
        session_id: Option<String>,
        /// Filter to one source agent, e.g. `claude-code` (or
        /// `claude-code/general-purpose` for a subagent).
        #[arg(long, value_name = "AGENT")]
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
        /// Server-side score threshold; hits below this are dropped.
        ///
        /// Not an absence signal: present and absent content score in
        /// overlapping bands, so leave at 0 unless trimming an over-long tail.
        #[arg(long, default_value_t = 0.0)]
        min_score: f64,
        /// Print Lance query plans instead of search results.
        #[arg(long)]
        explain: bool,
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Fetch a session or message.
    ///
    /// Returns readable transcripts by id: a whole session (`--session-id`)
    /// or one message with optional surrounding context (`--message-id`).
    /// Pagination cursors (`after-id:`) print at the end of truncated output.
    #[command(after_long_help = "Examples:
  pond get --session-id 58a96901-4a4f-40be-a3c1-62419ec8c580
  pond get --session-id <ID> --session-from end      most recent messages first
  pond get --message-id <ID> --context-depth 3       a hit plus its neighbors
  pond get --session-id <ID> --response-mode verbatim --format json")]
    #[command(group(ArgGroup::new("get_selector")
        .required(true)
        .args(["session_id", "message_id"])))]
    #[command(display_order = 11)]
    Get {
        /// Fetch an entire session by id. Mutually exclusive with `--message-id`.
        #[arg(long, value_name = "ID")]
        session_id: Option<String>,
        /// Fetch a single message by id. Mutually exclusive with `--session-id`.
        #[arg(long, value_name = "ID")]
        message_id: Option<String>,
        /// Tenant namespace. The personal pond has exactly one; leave at the
        /// default.
        #[arg(long, default_value = "local")]
        namespace: String,
        /// For `--message-id` mode: include this many sibling messages on each
        /// side (grep -C style). Ignored in session mode.
        #[arg(long, default_value_t = 0)]
        context_depth: usize,
        /// Cap on returned messages (session mode) or parts (message mode).
        #[arg(long, default_value_t = 20)]
        limit: usize,
        /// Depth: conversational, complete, or verbatim.
        ///
        /// conversational = text + part summaries; complete = all messages +
        /// summaries; verbatim = full parts inline. With `--message-id` it
        /// selects which siblings fill the context window.
        #[arg(
            long,
            value_enum,
            default_value_t = CliResponseMode::Conversational,
        )]
        response_mode: CliResponseMode,
        /// Session mode only: which end to read from.
        ///
        /// start = oldest first (default); end = most recent, e.g. to recover
        /// context after compaction.
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
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Run one read-only SQL query over the corpus.
    ///
    /// DataFusion / PostgreSQL-compatible SELECT/WITH over the sessions,
    /// messages, and parts tables; writes and side-effecting statements are
    /// rejected. Same surface as the `pond_sql_query` MCP tool - the MCP
    /// resource `schema://pond-sql` documents columns, indexed predicates,
    /// and pagination patterns.
    #[command(after_long_help = "Examples:
  pond sql \"SELECT count(*) FROM sessions\"
  pond sql \"SELECT session_id, ts, role FROM messages WHERE contains_tokens(search_text, 'occ retry') LIMIT 20\"
  pond sql \"SELECT * FROM messages\" --format parquet -o messages.parquet")]
    #[command(display_order = 12)]
    Sql {
        /// The SQL query. Wrap in quotes; remember to escape `$` in zsh/bash.
        sql: String,
        /// Output format. text/json/ndjson go to stdout; parquet requires
        /// `--output-file` (binary).
        #[arg(long, value_enum, default_value_t = CliSqlFormat::Text)]
        format: CliSqlFormat,
        /// Inline row cap for text/json output. Default 100, max 1000.
        /// Ignored for ndjson/parquet (which return every row).
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Write the export bytes here instead of stdout (required for
        /// `--format parquet`; optional for ndjson). Ignored for text/json.
        #[arg(long, short = 'o')]
        output_file: Option<PathBuf>,
    },
    /// Run the HTTP API server (or MCP over stdio with --transport stdio).
    ///
    /// Serves the wire protocol over HTTP on --host:--port. Most agent
    /// setups want `pond mcp` instead; `serve` is for the HTTP transport and
    /// for supervised deployments.
    #[command(after_long_help = "Examples:
  pond serve                       HTTP on 127.0.0.1:9797
  pond serve --port 8080
  pond serve --transport stdio     same as `pond mcp`")]
    #[command(display_order = 14)]
    Serve {
        /// Wire transport: the HTTP API, or MCP over stdio.
        #[arg(long, value_enum, default_value_t = ServeTransport::Http)]
        transport: ServeTransport,
        /// Bind address for the HTTP transport.
        #[arg(
            long,
            env = "POND_HOST",
            hide_env_values = true,
            default_value = "127.0.0.1"
        )]
        host: String,
        /// Bind port for the HTTP transport.
        #[arg(
            long,
            env = "POND_PORT",
            hide_env_values = true,
            default_value_t = 9797
        )]
        port: u16,
    },
    /// Serve the MCP tools over stdio (for agent clients).
    ///
    /// Equivalent to `pond serve --transport stdio`. Register once per
    /// client; the tools are pond_search, pond_get, and pond_sql_query, with
    /// resources schema://pond, schema://pond-sql, and stats://pond.
    #[command(after_long_help = "Examples:
  claude mcp add -s user pond -- pond mcp    register in Claude Code
  codex mcp add pond -- pond mcp             register in Codex CLI")]
    #[command(display_order = 15)]
    Mcp {},
    /// Manage the automatic sync schedule.
    ///
    /// Registers `pond sync -q` with the OS scheduler: launchd on macOS,
    /// systemd user timers (or a crontab fence) on Linux. `pond init` offers
    /// the same setup interactively.
    #[command(after_long_help = "Examples:
  pond schedule start              sync every hour
  pond schedule start --every 15m
  pond schedule status
  pond schedule logs
  pond schedule stop")]
    #[command(display_order = 5)]
    Schedule {
        #[command(subcommand)]
        command: schedule::ScheduleCmd,
    },
    /// Probe and switch storage destinations.
    ///
    /// `check` probes a destination end-to-end; `use` switches the
    /// configured destination - a pointer change that moves no data. To copy
    /// data between stores use `pond copy`; to see where data lives and
    /// how big it is use `pond status`.
    #[command(after_long_help = "Examples:
  pond storage check s3+https://host/bucket/pond   probe a destination end-to-end
  pond storage use s3+https://host/bucket/pond     probe, then switch the destination")]
    #[command(display_order = 3)]
    Storage {
        #[command(subcommand)]
        command: StorageCmd,
    },
    /// Manage URL-scoped credential sets in config.toml.
    ///
    /// `add` captures a set's keys (secret via hidden prompt) and writes
    /// `[creds.<name>]`; `list` shows configured sets with secrets redacted;
    /// `delete` removes one. Sets bind to storage URLs by scope match, never
    /// by activation (spec.md#creds-scope-match) - there is no "current" set.
    #[command(after_long_help = "Examples:
  pond creds add                            interactive: name (default), key, hidden secret
  pond creds add work --scope s3+https://host/work
  pond creds list
  pond creds delete work")]
    #[command(display_order = 4)]
    Creds {
        #[command(subcommand)]
        command: CredsCmd,
    },
    /// Copy canonical data between pond stores, `.pond` archives, and the JSONL
    /// wire stream.
    ///
    /// Both `--from` and `--to` are required. Each endpoint is sniffed by
    /// suffix: `*.pond` is a compact restorable archive, `*.jsonl` (or `-` for
    /// stdout) is the JSONL wire stream and is export-only, anything else is a
    /// pond store URL - `local` is the local default dir, `@` is your configured
    /// store (the one every other command uses). Store-to-store copies are an
    /// idempotent union merge (`lance-deterministic-pk` + merge-insert):
    /// re-runnable, resumable, incremental (a re-run transfers only the sessions
    /// absent or grown on the destination), valid onto a populated destination,
    /// never deletes or modifies the source; they rebuild the destination
    /// indexes, verify
    /// every row landed, and exit 6 if any are missing. `--verify-only` runs
    /// just the read-only membership check (store-to-store only). This is the
    /// durable-corpus copy path, distinct from `pond sync` which re-reads your
    /// client tools (spec.md#session-durable-copy).
    #[command(after_long_help = "Examples:
  pond copy --from local --to s3+https://host/bucket/pond          migrate the local store to a bucket
  pond copy --from @ --to backup/2026-06-16.pond                   snapshot the configured store to an archive
  pond copy --from backup/2026-06-16.pond --to @                   restore an archive into the configured store
  pond copy --from @ --to s3://bucket/pond --verify-only           re-check membership, copy nothing
  pond copy --from @ --to - | jq .                                 stream the configured store as JSONL")]
    #[command(display_order = 9)]
    Copy {
        /// Source endpoint: a store URL, `local` (local default dir), `@`
        /// (configured store), or a `*.pond` archive.
        #[arg(long, value_name = "URL|FILE")]
        from: String,
        /// Destination endpoint: a store URL, `local`, `@`, a `*.pond` archive,
        /// a `*.jsonl` file, or `-` for stdout.
        #[arg(long, value_name = "URL|FILE")]
        to: String,
        /// Only verify the destination already contains the source - copy
        /// nothing, rebuild no indexes. Read-only; store-to-store only.
        #[arg(long, conflicts_with = "no_optimize")]
        verify_only: bool,
        /// Skip the post-copy index rebuild (store-to-store and archive
        /// restore, for very large stores you will index later). Build indexes
        /// after with `pond optimize --only index --storage-path <to>`.
        #[arg(long)]
        no_optimize: bool,
    },
    /// Inspect configuration.
    ///
    /// Resolved values with provenance (show), the config file location
    /// (path), and the fully-annotated template (schema).
    #[command(flatten_help = true)]
    #[command(after_long_help = "Examples:
  pond config show                 every setting, its value, and where it came from
  pond config schema > ~/.config/pond/config.toml   start from the annotated template")]
    #[command(display_order = 6)]
    Config {
        #[command(subcommand)]
        command: ConfigCmd,
    },
    /// Generate shell completions.
    ///
    /// Writes the completion script for the given shell to stdout.
    #[command(after_long_help = "Install:
  bash:  pond completions bash > ~/.local/share/bash-completion/completions/pond
  zsh:   pond completions zsh > \"${fpath[1]}/_pond\"
  fish:  pond completions fish > ~/.config/fish/completions/pond.fish

Homebrew and nix packages ship these pre-installed.")]
    #[command(display_order = 16)]
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
    /// Keep the lake queryable: embed the backlog, then fold the search indexes.
    ///
    /// The maintenance verb. `embed` fills vectors for every message with a null
    /// `vector` (idempotent: a re-run resumes exactly where it stopped); `index`
    /// folds trailing fragments into the text + semantic indexes and runs the
    /// `[maintenance]` compaction / version-cleanup pass. `pond sync` runs both
    /// by default; this is the on-demand and post-`--no-optimize` catch-up.
    #[command(after_long_help = "Examples:
  pond optimize                    embed the backlog, then fold indexes
  pond optimize --only index       fold indexes only (e.g. after `pond copy --no-optimize`)
  pond optimize --only embed       embed only
  pond optimize --force-embed      re-embed stale rows after a model change")]
    #[command(display_order = 8)]
    Optimize {
        /// Run exactly one stage: embed or index.
        #[arg(long, value_enum)]
        only: Option<OptimizeStage>,
        /// Skip a stage. Can be passed multiple times.
        #[arg(long, value_enum)]
        skip: Vec<OptimizeStage>,
        /// Re-embed rows whose stored `embedding_model` does not match the
        /// configured model (a model swap), dropping the IVF_SQ first. Without
        /// this, such rows abort the run with a typed error so a swap is never
        /// silent.
        #[arg(long)]
        force_embed: bool,
        /// Recovery: rebuild every index from scratch via
        /// `create_index(replace=true)`. Use this when normal optimize errors
        /// with "<N> delta segments; run `pond optimize --rebuild`". Skips
        /// embed and the incremental index-fold; runs only the rebuild.
        #[arg(long, conflicts_with_all = ["only", "skip", "force_embed", "drop_index"])]
        rebuild: bool,
        /// One-shot orphan cleanup: drop a single named index from whichever
        /// table owns it. Used after renaming an intent (the old name is
        /// orphaned in the manifest until dropped). Tries every table in turn;
        /// errors when no table has an index by that name.
        #[arg(long, value_name = "NAME", conflicts_with_all = ["only", "skip", "force_embed", "rebuild"])]
        drop_index: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum StorageCmd {
    /// Probe a destination end-to-end.
    ///
    /// Parse, creds resolution, conditional put (the OCC primitive Lance's
    /// commit handler relies on), read-back, delete. Exit codes: 0 ok,
    /// 1 I/O error (network/DNS/bucket unreachable), 2 parse error,
    /// 3 no creds, 4 auth failed, 5 OCC unsupported.
    #[command(after_long_help = "Examples:
  pond storage check                                  probe the configured destination
  pond storage check s3+https://host/bucket/prefix    probe a candidate before switching")]
    Check {
        /// Destination URL. Defaults to the configured storage path.
        url: Option<String>,
        /// Output format: `text` (human lines) or `json` (one document).
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Switch the configured destination. Moves no data.
    ///
    /// Probes the destination end-to-end, then flips `[storage].path` in
    /// config.toml. Nothing is copied: switching to a different storage, or
    /// rolling back to a previous one, is a pointer change that leaves every
    /// store untouched. To copy data into a destination, run `pond copy`
    /// first - it is always a separate, explicit step.
    #[command(after_long_help = "Examples:
  pond storage use s3+https://host/bucket/pond      probe, then switch the active destination
  pond storage use local                            roll back to the default local store")]
    Use {
        /// The new destination URL, or `local` for the default local store.
        url: String,
    },
}

#[derive(Debug, Subcommand)]
enum CredsCmd {
    /// Add or replace a credential set (secret captured via a hidden prompt).
    Add {
        /// Set name. Omit to be prompted, with `default` as the default.
        name: Option<String>,
        /// Bind only to URLs under this prefix. Omit for the catch-all set.
        #[arg(long)]
        scope: Option<String>,
        /// Region override (S3-compatible stores ignore the SigV4 region).
        #[arg(long)]
        region: Option<String>,
    },
    /// List configured credential sets (secrets redacted).
    List {
        /// Output format: `text` (human table) or `json` (one document).
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Remove a credential set.
    Delete {
        /// Name of the set to remove.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum AdaptersCmd {
    /// List configured adapters (enabled state, path) and detected-but-unconfigured adapters.
    List {
        /// Output format: `text` (human table) or `json` (one document).
        #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
        format: OutputFormat,
    },
    /// Probe this machine for adapters and interactively enable the ones you pick.
    Discover,
    /// Enable an adapter, discovering its default path if it has no config entry yet.
    Enable {
        /// Adapter name (claude-code, codex-cli, ...).
        name: String,
    },
    /// Disable an adapter so `pond sync` skips it (kept as `enabled = false`).
    Disable {
        /// Adapter name to disable.
        name: String,
    },
}

// Parsed once, matched once, immediately destructured - the size spread
// between variants (StorageUrl-carrying vs unit-like) has no runtime cost.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum ConfigCmd {
    /// Show the resolved configuration.
    ///
    /// Redacted values, per-field source (cli/env/file/default), and the
    /// active URL's creds binding.
    Show {},
    /// Print the path of the config file pond loads.
    Path,
    /// Print the fully-annotated config.toml template.
    Schema,
}

fn parse_retention(raw: &str) -> Result<chrono::Duration, String> {
    let trimmed = raw.trim();
    let split = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split);
    let amount: i64 = number
        .parse()
        .map_err(|_| format!("retention {raw:?}: leading number is not an integer"))?;
    if amount < 0 {
        return Err(format!("retention {raw:?} must be non-negative"));
    }
    match unit {
        "s" => Ok(chrono::Duration::seconds(amount)),
        "m" => Ok(chrono::Duration::minutes(amount)),
        "h" => Ok(chrono::Duration::hours(amount)),
        "d" => Ok(chrono::Duration::days(amount)),
        other => Err(format!(
            "retention {raw:?}: unit {other:?} not recognized (use s/m/h/d)"
        )),
    }
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

/// Output mode for `pond search` and `pond get`. Text is the human default;
/// Json emits the wire envelope verbatim (including error envelopes), so
/// scripts can `--format json | jq ...` against the same surface as the HTTP
/// transport.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

/// Lance's DataFusion `FairSpillPool` defaults to 100 MiB total
/// (`lance-datafusion::exec::DEFAULT_LANCE_MEM_POOL_SIZE_PER_PARTITION`), which
/// the multi-consumer fair-share splits below the working set of BTree
/// `SortExec` over ~2M-row tables and crashes `create_index` with
/// `ExternalSorterMerge` reservation failures. Lance offers no programmatic
/// `MemoryPool` injection in 7.0.0; the env var is the only lever, and the
/// session-context cache resolves it at first use, so setting it here (before
/// any Lance call) is sufficient.
fn raise_lance_mem_pool() {
    if std::env::var_os("LANCE_MEM_POOL_SIZE").is_some() {
        return;
    }
    // Process-wide, set once at startup before tokio spawns Lance work. Safe
    // here per Rust 2024's set_var contract (no other threads reading env).
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("LANCE_MEM_POOL_SIZE", "1073741824");
    }
    tracing::debug!("LANCE_MEM_POOL_SIZE defaulted to 1 GiB");
}

/// Bound the per-scan readahead buffer for the long-lived read servers
/// (`pond mcp` / `pond serve`). Lance's `LANCE_DEFAULT_IO_BUFFER_SIZE` defaults
/// to 2 GiB, which lets a single scan balloon RSS; 256 MiB keeps a warm server
/// inside a fixed budget while still feeding the IO threads for the small
/// result sets reads return (spec.md#search). Set only in server processes -
/// throughput-bound `copy`/`sync` run in their own processes and keep the
/// default. Index/metadata caches are bounded separately in `resolve_cache_caps`.
fn cap_serve_io_buffer() {
    if std::env::var_os("LANCE_DEFAULT_IO_BUFFER_SIZE").is_some() {
        return;
    }
    // Process-wide, set once before opening the store. Safe per Rust 2024's
    // set_var contract (no other threads reading env yet).
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("LANCE_DEFAULT_IO_BUFFER_SIZE", "268435456");
    }
    tracing::debug!("LANCE_DEFAULT_IO_BUFFER_SIZE defaulted to 256 MiB for serving");
}

/// Warm the search indices in the background at server startup (spec.md#search).
/// The cold S3 index load (175-442 s) is paid once here, off the request path,
/// so the first user query is fast instead of catastrophic. Best effort:
/// serving starts immediately and a failure is logged, not fatal.
fn spawn_prewarm(store: Arc<Store>) {
    let cache_dir = default_cache_dir();
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        tracing::info!("prewarm: warming search indices");
        match store.prewarm(&cache_dir).await {
            Ok(()) => tracing::info!(
                elapsed_ms = started.elapsed().as_millis() as u64,
                "prewarm: search indices warm"
            ),
            Err(error) => {
                tracing::warn!(%error, "prewarm: failed; first query will pay the cold load");
            }
        }
    });
}

fn try_raise_fd_limit(target: u64) -> anyhow::Result<()> {
    use rlimit::{Resource, getrlimit, setrlimit};

    let (soft, hard) = getrlimit(Resource::NOFILE).context("getrlimit(RLIMIT_NOFILE)")?;
    let new_soft = target.min(hard);
    if new_soft <= soft {
        return Ok(());
    }
    setrlimit(Resource::NOFILE, new_soft, hard).context("setrlimit(RLIMIT_NOFILE)")?;
    tracing::info!(
        target: "pond::perf",
        soft = new_soft,
        hard,
        "raised RLIMIT_NOFILE to clear the FTS index-merge EMFILE class"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    human_panic::setup_panic!();

    let cli = Cli::parse();
    init_tracing(cli.verbose.tracing_level_filter());
    if let Err(error) = try_raise_fd_limit(65_536) {
        tracing::debug!("RLIMIT_NOFILE bump skipped: {error}");
    }
    raise_lance_mem_pool();
    // `-v` opts default `pond status` into the embedding probe, which scans the
    // `vector` and `search_text` columns on the 2M-row messages table - tens of
    // seconds on a cold remote store, because Lance v2 has no per-column
    // null_count metadata (lance 7.0.0 forces a data-page read for any filter).
    let verbose = cli.verbose.tracing_level_filter() >= tracing::level_filters::LevelFilter::INFO;
    // Root-global selectors, bound once so every arm reads the same locals it
    // did when each variant flattened its own copy.
    let StoreArgs {
        storage_path,
        config,
    } = cli.store;

    match cli.command {
        Command::Init(args) => init::run(args, storage_path, config).await?,
        Command::Status {
            include_subagents,
            format,
        } => {
            let loaded = Config::load(config_path(config))?;
            let (resolved, store) = open_store(storage_path, &loaded, false).await?;
            if !store.initialized().await? {
                match format {
                    OutputFormat::Json => output(&status_json_empty(&resolved)?)?,
                    OutputFormat::Text => {
                        render_empty_status("pond status", &resolved)?;
                        output(&crate::schedule::status_line())?;
                    }
                }
            } else {
                // Default status never scans the 2M-row `messages` table:
                // totals come from manifest metadata (`row_counts`), adapter
                // count from a scan of the small `sessions` table, and the
                // embedding probe (which DOES scan messages) only runs under
                // `-v`. Skipping it drops a cold-S3 status from ~43s to ~3s.
                let embedding_fut = async {
                    if verbose {
                        store.embedding_progress().await.map(Some)
                    } else {
                        Ok(None)
                    }
                };
                let (sizes, (sessions, messages, parts), names, index_status, embedding) = tokio::try_join!(
                    store.table_sizes(),
                    store.row_counts(),
                    store.adapter_names(include_subagents),
                    store.index_status(),
                    embedding_fut,
                )?;
                let totals = RowTotals {
                    sessions: sessions as u64,
                    messages: messages as u64,
                    parts: parts as u64,
                };
                let adapter_count = names.len();
                match format {
                    OutputFormat::Json => {
                        output(&status_json(
                            &resolved,
                            &sizes,
                            &totals,
                            adapter_count,
                            &index_status,
                            embedding,
                        )?)?;
                    }
                    OutputFormat::Text => {
                        render_status_header("pond status", &resolved, &sizes, &totals)?;
                        render_status_checks(&totals, adapter_count, &index_status, embedding)?;
                        let probes = adapter::probe_unconfigured(&loaded.adapters);
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
                }
            }
        }
        Command::Sync {
            adapter,
            path,
            no_optimize,
            verify,
        } => {
            let config_file = config_path(config);
            let loaded = Config::load(&config_file)?;
            let (_, store) = open_store(storage_path, &loaded, true).await?;
            let import_summary =
                run_import_stage(&store, &loaded, &config_file, adapter.clone(), path, verify)
                    .await?;
            let any_new_rows = import_summary.inserted > 0;
            let summary = SyncRunSummary {
                ingest: Some(import_summary),
            };
            // Optimize stages gated on `any_new_rows`: cleanup_old_versions
            // walks the version log on every call (real work even when "caught
            // up"), so an idempotent no-op sync should not pay it. Catch up an
            // accumulated backlog with `pond optimize` (the verb that exists
            // precisely for this) or the scheduled maintenance run.
            if !no_optimize && any_new_rows {
                // Same finalize seam as `pond optimize` and `pond copy`. sync
                // has no --force-embed; finalize's embed points a model swap at
                // the command that does.
                let policy = configured_maintenance_policy(&loaded, None)?;
                finalize_indexes(&store, &policy, false).await?;
            }
            render_sync_summary(&store, &summary).await?;
        }
        Command::Optimize {
            only,
            skip,
            force_embed,
            rebuild,
            drop_index,
        } => {
            let loaded = Config::load(config_path(config))?;
            let (_, store) = open_store(storage_path, &loaded, true).await?;
            if let Some(name) = drop_index {
                store
                    .drop_index_by_name(&name)
                    .await
                    .with_context(|| format!("drop_index({name}) failed"))?;
                output(&format!("optimize: dropped index {name}"))?;
            } else if rebuild {
                // Migration path: bring the store to the regular-optimize end
                // state (embed + fold + compaction + cleanup), then rebuild
                // every index from scratch so it adopts current params, then
                // reclaim the segments the rebuild superseded.
                let policy = configured_maintenance_policy(&loaded, None)?;
                let outcome = finalize_indexes(&store, &policy, force_embed).await?;
                if outcome.any_indices_failed() {
                    std::process::exit(1);
                }
                let (progress, bar) = optimize_progress_bar();
                let result = store.rebuild_indices(None, Some(progress)).await;
                bar.finish_and_clear();
                result.context("rebuild_indices failed")?;
                store
                    .cleanup_old_versions(policy.cleanup_older_than)
                    .await
                    .context("cleanup after rebuild failed")?;
                output("optimize: indexes rebuilt, old segments reclaimed")?;
            } else {
                let stages = OptimizeStages::resolve(only, &skip)?;
                let policy = configured_maintenance_policy(&loaded, None)?;
                let outcome = if stages.embed && stages.index {
                    // Full optimize: the same finalize seam sync and copy use.
                    Some(finalize_indexes(&store, &policy, force_embed).await?)
                } else {
                    if stages.embed {
                        run_embed_stage(&store, force_embed).await?;
                    }
                    if stages.index {
                        Some(run_update_indexes_stage(&store, &policy).await?)
                    } else {
                        None
                    }
                };
                if outcome.is_some_and(|outcome| outcome.any_indices_failed()) {
                    std::process::exit(1);
                }
            }
        }
        Command::Adapters { command } => {
            let config_file = config_path(config);
            let loaded = Config::load(&config_file)?;
            run_adapters(command, &config_file, &loaded)?;
        }
        Command::Serve {
            transport,
            host,
            port,
        } => {
            cap_serve_io_buffer();
            let config = Config::load(config_path(config))?;
            let store = Arc::new(open_store(storage_path, &config, true).await?.1);
            let embedder = Arc::new(LazyEmbedder::candle());
            let state = AppState {
                store,
                embedder,
                search: config.search.clone(),
            };
            spawn_prewarm(state.store.clone());
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
        Command::Mcp {} => {
            cap_serve_io_buffer();
            let config = Config::load(config_path(config))?;
            let store = Arc::new(open_store(storage_path, &config, true).await?.1);
            // Lazy: idle `pond mcp` instances in every Claude Code session
            // stay light. The model load only happens once per process on the
            // first `pond_search` tool call that needs hybrid retrieval.
            let embedder = Arc::new(LazyEmbedder::candle());
            spawn_prewarm(store.clone());
            transport::mcp::serve_stdio(AppState {
                store,
                embedder,
                search: config.search.clone(),
            })
            .await?;
        }
        Command::Search {
            query,
            namespace,
            limit,
            mode,
            project,
            session_id,
            source_agent,
            include_subagents,
            from_date,
            to_date,
            min_score,
            explain,
            format,
        } => {
            let loaded = Config::load(config_path(config))?;
            let (_, store) = open_store(storage_path, &loaded, false).await?;
            let embedder = LazyEmbedder::candle();
            let request = SearchRequest {
                protocol_version: PROTOCOL_VERSION,
                namespace: Some(namespace),
                query,
                mode_override: mode.map(SearchModeWire::from),
                filters: SearchFilters {
                    project,
                    session_id,
                    source_agent,
                    from_date,
                    to_date,
                    min_score,
                    include_subagents,
                },
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
        Command::Get {
            session_id,
            message_id,
            namespace,
            context_depth,
            limit,
            response_mode,
            session_from,
            after_id,
            format,
        } => {
            let loaded = Config::load(config_path(config))?;
            let (_, store) = open_store(storage_path, &loaded, false).await?;
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
        Command::Config { command } => {
            run_config_command(command, storage_path, config).await?;
        }
        Command::Schedule { command } => schedule::run(command)?,
        Command::Completions { shell } => {
            clap_complete::generate(shell, &mut Cli::command(), "pond", &mut io::stdout());
        }
        Command::Storage { command } => run_storage_command(command, storage_path, config).await?,
        Command::Creds { command } => {
            let config_file = config_path(config);
            let loaded = Config::load(&config_file)?;
            run_creds(command, &config_file, &loaded)?;
        }
        Command::Copy {
            from,
            to,
            verify_only,
            no_optimize,
        } => {
            run_copy(from, to, verify_only, no_optimize, storage_path, config).await?;
        }
        Command::Sql {
            sql,
            format,
            limit,
            output_file,
        } => {
            if matches!(format, CliSqlFormat::Parquet) && output_file.is_none() {
                bail!(
                    "--format parquet requires --output-file <path> (binary, can't go to stdout)"
                );
            }
            let loaded = Config::load(config_path(config))?;
            let (_, store) = open_store(storage_path, &loaded, false).await?;
            let mode = match format {
                CliSqlFormat::Text => pond::sql::Mode::Inline,
                CliSqlFormat::Json => pond::sql::Mode::InlineJson,
                CliSqlFormat::Ndjson => pond::sql::Mode::Export(pond::sql::Format::Ndjson),
                CliSqlFormat::Parquet => pond::sql::Mode::Export(pond::sql::Format::Parquet),
            };
            let inline_rows = limit.min(pond::sql::MAX_INLINE_ROWS);
            // Open only the tables the query names (spec.md#search); the slow
            // `parts.lance` open is waste for the common messages-only query.
            use pond::substrate::Table;
            let (sessions, messages, parts) = tokio::try_join!(
                async {
                    anyhow::Ok(match pond::sql::mentions_table(&sql, "sessions") {
                        true => Some(store.dataset(Table::Sessions).await?),
                        false => None,
                    })
                },
                async {
                    anyhow::Ok(match pond::sql::mentions_table(&sql, "messages") {
                        true => Some(store.dataset(Table::Messages).await?),
                        false => None,
                    })
                },
                async {
                    anyhow::Ok(match pond::sql::mentions_table(&sql, "parts") {
                        true => Some(store.dataset(Table::Parts).await?),
                        false => None,
                    })
                },
            )?;
            let tables = pond::sql::Tables {
                sessions,
                messages,
                parts,
            };
            match pond::sql::run(&tables, &sql, mode, inline_rows).await {
                Ok(pond::sql::Outcome::Inline(text)) => {
                    output(&text)?;
                }
                Ok(pond::sql::Outcome::InlineJson(value)) => {
                    // Pretty-print for the CLI - the structured payload is for
                    // agents over MCP; humans reading stdout want it readable.
                    output(&serde_json::to_string_pretty(&value)?)?;
                }
                Ok(pond::sql::Outcome::Export {
                    bytes,
                    format: _,
                    rows,
                    columns: _,
                }) => match output_file {
                    Some(path) => {
                        fs::write(&path, &bytes)
                            .with_context(|| format!("write export to {}", path.display()))?;
                        output_err(&format!(
                            "{} {} row(s), {} bytes -> {}",
                            pond::output::paint("export:", pond::output::dim()),
                            rows,
                            bytes.len(),
                            path.display()
                        ))?;
                    }
                    None => {
                        use std::io::Write;
                        io::stdout().write_all(&bytes)?;
                    }
                },
                Err(pond::sql::SqlError::Query(message)) => {
                    output_err(&format!(
                        "{} {message}",
                        pond::output::paint("sql error:", pond::output::dim())
                    ))?;
                    std::process::exit(2);
                }
                Err(pond::sql::SqlError::Infra(error)) => {
                    return Err(error);
                }
            }
        }
    }

    Ok(())
}

fn init_tracing(cli_level: tracing::level_filters::LevelFilter) {
    // Lance's IVF_SQ builder warns once per empty centroid during merge
    // (rust/lance/src/index/vector/builder.rs: "partition N is empty, skipping").
    // It already handles the case - records a zero-sized partition and continues -
    // so the warning is benign log noise during index maintenance.
    //
    // `aws_config`'s IMDS region probe WARNs when there's no instance-metadata
    // endpoint (every non-EC2 host): a 1s connect timeout that doesn't affect
    // the explicit-creds path. Lance's AIMD throttle WARNs once per retry while
    // a probe target is unreachable - the failure itself is already surfaced in
    // the check/probe error. Silencing both keeps the storage probe's spinner
    // (and the init wizard) from being corrupted mid-render; `RUST_LOG`
    // (which replaces this whole filter) still opts back in.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(format!(
            "{cli_level},lance::index::vector::builder=error,aws_config=error,lance::object_store::throttle=error"
        ))
    });
    // `IndicatifLayer` routes both spans (when they opt-in via `pb_set_*`)
    // and the fmt log writer through a shared draw target, so log lines
    // never clobber an active progress bar. Today no pond span requests a
    // bar, so the layer is essentially a "safe stderr writer for fmt that
    // coexists with future bar-rendering spans"; the migration cost when we
    // do start instrumenting spans (e.g. ingest, optimize) is zero.
    let indicatif_layer = IndicatifLayer::new();
    let fmt_layer = fmt::layer().with_writer(indicatif_layer.get_stderr_writer());
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(indicatif_layer)
        .init();
}

#[allow(clippy::print_stdout)]
fn output(message: &str) -> anyhow::Result<()> {
    pond::output::line(message)
}

fn output_err(message: &str) -> anyhow::Result<()> {
    pond::output::line_err(message)
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

/// Resolve `[runtime]` into the substrate's typed caps struct. A standalone
/// helper to keep every call site terse and to make sure no surface forgets
/// the caps.
fn runtime_caps(config: &Config) -> pond::substrate::RuntimeCaps {
    pond::substrate::RuntimeCaps::from_config(&config.runtime)
}

/// Resolve the storage destination: `--storage-path` / `POND_STORAGE_PATH`
/// (folded together by clap) > `[storage].path` > the platform-local default.
fn resolve_storage_location(
    explicit: Option<StorageUrl>,
    loaded: &Config,
) -> anyhow::Result<StorageUrl> {
    if let Some(storage) = explicit {
        return Ok(storage);
    }
    if let Some(path) = &loaded.storage.path {
        return StorageUrl::parse(path).context("invalid [storage].path in config");
    }
    default_local_storage()
}

/// Resolve creds for the destination and open the store. One chokepoint so
/// every command resolves identically and no surface forgets the
/// unmatched-set warning (misbinding must never be silent).
async fn open_store(
    explicit: Option<StorageUrl>,
    loaded: &Config,
    spinner: bool,
) -> anyhow::Result<(ResolvedStorage, Store)> {
    let storage = resolve_storage_location(explicit, loaded)?;
    let resolved = storage.resolve(&loaded.creds)?;
    warn_unmatched_sets(&[&resolved], loaded)?;
    let store = if spinner {
        open_store_with_spinner(
            resolved.lance_url(),
            resolved.options.clone(),
            runtime_caps(loaded),
        )
        .await?
    } else {
        Store::open_with_options(
            resolved.lance_url(),
            resolved.options.clone(),
            runtime_caps(loaded),
        )
        .await?
    };
    Ok((resolved, store))
}

/// spec.md#creds-scope-match: a defined set that bound to none of this
/// invocation's URLs gets named on stderr - a wrong scope must surface
/// before the auth error it causes.
fn warn_unmatched_sets(resolved: &[&ResolvedStorage], loaded: &Config) -> anyhow::Result<()> {
    for name in pond::substrate::unmatched_creds_sets(resolved, &loaded.creds) {
        output_err(&pond::output::paint(
            &format!("hint     creds set [{name}] matched no storage URL in this invocation"),
            pond::output::dim(),
        ))?;
    }
    Ok(())
}

/// The config path: an explicit `--config-file` (or `POND_CONFIG_FILE`) wins; otherwise
/// `$XDG_CONFIG_HOME/pond/config.toml` (default `~/.config/pond/config.toml`),
/// regardless of where the storage path points. Config and data are different
/// XDG categories - they always live in different directories, even when both
/// are local.
fn config_path(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    pond::config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    )
}

async fn run_config_command(
    command: ConfigCmd,
    storage_path: Option<StorageUrl>,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    match command {
        ConfigCmd::Schema => output(DEFAULT_CONFIG_TOML.trim_end()),
        ConfigCmd::Path => output(&config_path(config).display().to_string()),
        ConfigCmd::Show {} => {
            let path = config_path(config);
            let (loaded, figment) = Config::load_with_provenance(&path)?;
            // Contracted like every other human-facing path (`pond config
            // path` stays absolute for scripting).
            output(&format!(
                "{}  {}{}",
                paint("config ", dim()),
                pond::config::contract_home(&path).display(),
                if path.exists() {
                    ""
                } else {
                    "  (absent - defaults + env)"
                },
            ))?;

            // The active destination, its provenance, and its creds binding -
            // the line that makes a wrong scope match visible.
            let storage = resolve_storage_location(storage_path.clone(), &loaded)?;
            let resolved = storage.resolve(&loaded.creds)?;
            let storage_source = match &storage_path {
                // clap folds the env var into the flag; argv tells them apart
                // (value comparison can't - flag and env may carry the same
                // URL, and the flag wins per the precedence ladder).
                Some(_) => {
                    if std::env::args()
                        .any(|arg| arg == "--storage-path" || arg.starts_with("--storage-path="))
                    {
                        "cli"
                    } else {
                        "env"
                    }
                }
                None if loaded.storage.path.is_some() => classify_source(&figment, "storage.path"),
                None => "default",
            };
            output(&format!(
                "{}  {}  ({})  -> {}",
                paint("storage", dim()),
                resolved.display(),
                storage_source,
                describe_binding_with_source(&resolved.binding, &figment),
            ))?;
            // The one precedence ladder (spec.md#storage-env-mirror), shown
            // wherever sources are attributed.
            output(&paint(
                "ladder   cli flag > POND_* env > config file > ambient cloud chain > built-in defaults",
                dim(),
            ))?;
            output("")?;

            let mut table = new_table();
            table.set_header(
                ["setting", "value", "source"]
                    .map(|h| Cell::new(h).add_attributes(vec![Attribute::Dim, Attribute::Bold])),
            );
            let value = serde_json::to_value(&loaded).context("serialize config for display")?;
            let mut rows = Vec::new();
            flatten_config(String::new(), &value, &mut rows);
            for (key, raw) in rows {
                let source = classify_source(&figment, &key);
                table.add_row([
                    key.clone(),
                    redact_config_value(&key, &raw),
                    source.to_owned(),
                ]);
            }
            output(&table.to_string())?;
            warn_unmatched_sets(&[&resolved], &loaded)?;
            Ok(())
        }
    }
}

/// Map a [`CheckFailure`] onto the documented `pond storage check` exit
/// codes (3 no creds, 4 auth failed, 5 OCC unsupported, 1 other IO).
fn check_failure_exit_code(failure: &CheckFailure) -> i32 {
    match failure {
        CheckFailure::NoCreds { .. } => 3,
        CheckFailure::Auth { .. } => 4,
        CheckFailure::OccUnsupported { .. } => 5,
        CheckFailure::Io { .. } => 1,
    }
}

/// Machine-readable failure class for `pond storage check --format json`,
/// parallel to [`check_failure_exit_code`]'s exit-code mapping.
fn check_failure_class(failure: &CheckFailure) -> &'static str {
    match failure {
        CheckFailure::NoCreds { .. } => "no_creds",
        CheckFailure::Auth { .. } => "auth",
        CheckFailure::OccUnsupported { .. } => "occ_unsupported",
        CheckFailure::Io { .. } => "io",
    }
}

/// One `pond storage check --format json` document. `failure` is the
/// `(class, message)` pair on a non-zero outcome, `None` on success.
fn storage_check_json(
    url: &str,
    binding: Option<&str>,
    exit_code: i32,
    failure: Option<(&str, String)>,
) -> anyhow::Result<String> {
    let doc = serde_json::json!({
        "url": url,
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "binding": binding,
        "failure": failure.map(|(class, message)| serde_json::json!({
            "class": class,
            "message": message,
        })),
    });
    serde_json::to_string_pretty(&doc).context("serialize storage check as JSON")
}

/// One dim `cause:` line under a probe failure's fix-naming lead. The full
/// untruncated chain stays reachable via `RUST_LOG=debug`.
fn render_check_cause(failure: &CheckFailure) -> anyhow::Result<()> {
    if let Some(cause) = failure.concise_cause() {
        output_err(&pond::output::paint(
            &format!("cause: {cause}"),
            pond::output::dim(),
        ))?;
    }
    tracing::debug!("full probe failure: {failure:?}");
    Ok(())
}

async fn run_storage_command(
    command: StorageCmd,
    storage_path: Option<StorageUrl>,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let config_file = config_path(config);
    let loaded = Config::load(&config_file)?;
    match command {
        StorageCmd::Check { url, format } => {
            let storage = match url {
                Some(raw) => match parse_storage_path(&raw) {
                    Ok(parsed) => parsed,
                    Err(error) => match format {
                        OutputFormat::Json => {
                            output(&storage_check_json(
                                &raw,
                                None,
                                2,
                                Some(("parse", format!("{error:#}"))),
                            )?)?;
                            std::process::exit(2);
                        }
                        OutputFormat::Text => {
                            output_err(&format!("check: parse error: {error:#}"))?;
                            std::process::exit(2);
                        }
                    },
                },
                None => resolve_storage_location(storage_path, &loaded)?,
            };
            let resolved = storage.resolve(&loaded.creds)?;
            match format {
                OutputFormat::Json => {
                    let url = resolved.display();
                    let binding = resolved.binding.describe();
                    match pond::substrate::storage_check(&resolved).await {
                        Ok(()) => output(&storage_check_json(&url, Some(&binding), 0, None)?)?,
                        Err(failure) => {
                            let code = check_failure_exit_code(&failure);
                            output(&storage_check_json(
                                &url,
                                Some(&binding),
                                code,
                                Some((check_failure_class(&failure), failure.to_string())),
                            )?)?;
                            std::process::exit(code);
                        }
                    }
                }
                OutputFormat::Text => {
                    output(&format!(
                        "{}  {}",
                        resolved.display(),
                        paint(&format!("[{}]", resolved.binding.describe()), dim()),
                    ))?;
                    warn_unmatched_sets(&[&resolved], &loaded)?;
                    match pond::substrate::storage_check(&resolved).await {
                        Ok(()) => {
                            output(
                                "check: ok - conditional put (OCC), read-back, and delete all succeeded",
                            )?;
                        }
                        Err(failure) => {
                            // Distinct exit codes (documented in --help) so cron and
                            // CI can branch on the failure class.
                            output_err(&format!("check: {failure}"))?;
                            render_check_cause(&failure)?;
                            std::process::exit(check_failure_exit_code(&failure));
                        }
                    }
                }
            }
        }
        StorageCmd::Use { url } => {
            run_storage_use(&loaded, &config_file, storage_path, &url).await?;
        }
    }
    Ok(())
}

/// A `pond copy` endpoint after the suffix sniff: a pond store, a `.pond`
/// archive, or the JSONL wire stream (a `.jsonl` file or `-` for stdio).
// Built once, matched once, immediately consumed - the StorageUrl-vs-PathBuf
// size spread has no runtime cost.
#[allow(clippy::large_enum_variant)]
enum CopyEndpoint {
    Store(StorageUrl),
    Archive(PathBuf),
    Jsonl(Option<PathBuf>),
}

fn describe_endpoint(endpoint: &CopyEndpoint) -> String {
    match endpoint {
        CopyEndpoint::Store(url) => url.display(),
        CopyEndpoint::Archive(path) => format!("{} (archive)", path.display()),
        CopyEndpoint::Jsonl(Some(path)) => format!("{} (jsonl)", path.display()),
        CopyEndpoint::Jsonl(None) => "- (jsonl)".to_owned(),
    }
}

/// Strict suffix sniff for a `--from`/`--to` value (the converge-cli-surface
/// decision): `*.pond` -> archive, `*.jsonl` or `-` (stdio) -> JSONL wire
/// stream, anything else -> a pond store URL.
fn sniff_copy_endpoint(raw: &str) -> anyhow::Result<CopyEndpoint> {
    if raw == "-" {
        return Ok(CopyEndpoint::Jsonl(None));
    }
    if raw.ends_with(".pond") {
        return Ok(CopyEndpoint::Archive(PathBuf::from(raw)));
    }
    if raw.ends_with(".jsonl") {
        return Ok(CopyEndpoint::Jsonl(Some(PathBuf::from(raw))));
    }
    Ok(CopyEndpoint::Store(parse_storage_path(raw)?))
}

/// Sniff a `--from`/`--to` value into an endpoint, resolving the `@` keyword to
/// the configured store (what every other command defaults to). `@` needs the
/// config and `--storage-path`, so - unlike the config-free `local` keyword - it
/// can't be a value-parser keyword and is resolved here instead. A directory
/// literally named `@` is still reachable as `./@`.
fn resolve_copy_endpoint(
    raw: &str,
    storage_path: &Option<StorageUrl>,
    loaded: &Config,
) -> anyhow::Result<CopyEndpoint> {
    if raw == "@" {
        return Ok(CopyEndpoint::Store(resolve_storage_location(
            storage_path.clone(),
            loaded,
        )?));
    }
    sniff_copy_endpoint(raw)
}

/// `pond copy`: move canonical data between pond stores, `.pond` archives, and
/// the JSONL wire stream. Both endpoints are required; the verb routes on the
/// sniffed endpoint kinds (spec.md#session-durable-copy).
async fn run_copy(
    from: String,
    to: String,
    verify_only: bool,
    no_optimize: bool,
    storage_path: Option<StorageUrl>,
    config: Option<PathBuf>,
) -> anyhow::Result<()> {
    let loaded = Config::load(config_path(config))?;
    let from_ep = resolve_copy_endpoint(&from, &storage_path, &loaded)?;
    let to_ep = resolve_copy_endpoint(&to, &storage_path, &loaded)?;
    // A mistyped local `--from` would otherwise open as an empty store and
    // "copy" zero rows with a success exit; fail loudly on a missing source.
    if let CopyEndpoint::Store(url) = &from_ep
        && let Some(path) = pond::config::local_path(url.canonical())
        && !path.exists()
    {
        bail!(
            "source store {} does not exist; check --from",
            url.display()
        );
    }
    match (from_ep, to_ep) {
        (CopyEndpoint::Store(from), CopyEndpoint::Store(to)) => {
            run_store_to_store_copy(from, to, verify_only, no_optimize, &loaded).await
        }
        _ if verify_only => bail!("--verify-only applies to store-to-store copies only"),
        (CopyEndpoint::Store(from), CopyEndpoint::Archive(path)) => {
            copy_store_to_archive(from, &path, &loaded).await
        }
        (CopyEndpoint::Store(from), CopyEndpoint::Jsonl(path)) => {
            copy_store_to_jsonl(from, path, &loaded).await
        }
        (CopyEndpoint::Archive(path), CopyEndpoint::Store(to)) => {
            copy_archive_to_store(&path, to, no_optimize, &loaded).await
        }
        (CopyEndpoint::Jsonl(_), CopyEndpoint::Store(_)) => bail!(
            "jsonl is an export-only target: restore from a `.pond` archive or copy from a store URL instead"
        ),
        (from_ep, to_ep) => bail!(
            "unsupported copy {} -> {}: at least one endpoint must be a pond store",
            describe_endpoint(&from_ep),
            describe_endpoint(&to_ep),
        ),
    }
}

/// Store-to-store copy: the durable union merge, an index rebuild (unless
/// `--no-optimize`), and an id-set membership check. Exit 6 when the
/// destination is missing source rows; `--verify-only` runs just the read-only
/// check. Distinct from `pond sync`, which re-reads adapters - copy moves the
/// durable record (spec.md#session-durable-copy).
async fn run_store_to_store_copy(
    from: StorageUrl,
    to: StorageUrl,
    verify_only: bool,
    no_optimize: bool,
    loaded: &Config,
) -> anyhow::Result<()> {
    if from.canonical() == to.canonical() {
        bail!(
            "--from and --to resolve to the same store ({}); nothing to copy",
            from.display(),
        );
    }
    let (from_resolved, from_store, to_resolved, to_store) =
        resolve_and_open_pair(&from, &to, loaded).await?;
    if verify_only {
        let verify = verify_stores(&from_store, &to_store).await?;
        ensure_source_not_empty(&verify, &from_resolved.display())?;
        if !render_storage_verify(&verify, &from_resolved.display(), &to_resolved.display())?.synced
        {
            std::process::exit(6);
        }
        return Ok(());
    }

    let dim = pond::output::dim();
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.green} {msg} [{elapsed_precise}]")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.enable_steady_tick(Duration::from_millis(120));

    spinner.set_message("plan  comparing source <-> destination");
    let plan_started = std::time::Instant::now();
    // Incremental: transfer only the sessions absent or grown on the
    // destination, detected by a data-intrinsic per-session message-count key
    // (spec.md#session-durable-copy). A re-run after a small delta moves and
    // indexes only that delta, never the whole corpus.
    let plan = to_store.plan_incremental_from(&from_store).await?;
    let plan_elapsed = plan_started.elapsed();
    // A 0-row source almost always means a mistyped `--from`: a remote store
    // auto-vivifies empty, so the copy would otherwise "succeed" having moved
    // nothing. Bail before touching the destination.
    if plan.source_sessions == 0 {
        spinner.finish_and_clear();
        bail!(
            "source store {} has 0 sessions; check --from (a mistyped source opens as an empty store)",
            from_resolved.display(),
        );
    }
    let new_sessions = plan.new_sessions();
    let grown_sessions = plan.total().saturating_sub(new_sessions);
    spinner.println(format!(
        "{} {} sessions to copy ({} new + {} grown, {} on source)  {:.1}s",
        pond::output::paint("plan", dim),
        plan.total(),
        new_sessions,
        grown_sessions,
        plan.source_sessions,
        plan_elapsed.as_secs_f64(),
    ));

    if plan.is_empty() {
        spinner.println(format!(
            "{} destination already up to date (0 sessions copied)",
            pond::output::paint("copy:", dim),
        ));
    } else {
        spinner.set_message(format!(
            "stream  copying {} sessions to destination",
            plan.total(),
        ));
        let stream_started = std::time::Instant::now();
        let imported = to_store.copy_delta_from(&from_store, &plan).await?;
        let stream_elapsed = stream_started.elapsed();
        spinner.println(format!(
            "{} sessions {} ({} new)  messages {} ({} new)  parts {} ({} new)  {:.1}s",
            pond::output::paint("stream", dim),
            imported.rows.sessions,
            imported.inserted.sessions,
            imported.rows.messages,
            imported.inserted.messages,
            imported.rows.parts,
            imported.inserted.parts,
            stream_elapsed.as_secs_f64(),
        ));
    }

    // The index stage draws its own progress bar; clear ours first so two
    // indicatif renderers never fight over the cursor.
    spinner.finish_and_clear();

    // Fold destination indexes even on an empty stream plan: an interrupted
    // previous copy can leave data complete but index creation missing.
    // spec.md#lance-index-maintenance.
    let indexes_started = std::time::Instant::now();
    if no_optimize {
        output(&format!(
            "{} skipped (--no-optimize); run `pond optimize --only index --storage-path {}` before querying",
            pond::output::paint("indexes", dim),
            to_resolved.display(),
        ))?;
    } else {
        let policy = configured_maintenance_policy(loaded, None)?;
        // Same finalize seam as optimize/sync. The embed pass no-ops because the
        // copied rows arrive already embedded from the source.
        finalize_indexes(&to_store, &policy, false).await?;
        output(&format!(
            "{} rebuilt text + semantic on destination  {:.1}s",
            pond::output::paint("indexes", dim),
            indexes_started.elapsed().as_secs_f64(),
        ))?;
    }

    let verify = verify_stores(&from_store, &to_store).await?;
    if !render_storage_verify(&verify, &from_resolved.display(), &to_resolved.display())?.synced {
        std::process::exit(6);
    }

    // `pond copy` always closes with the id-set proof; `--verify-only` runs
    // just this read-only check.
    if no_optimize {
        output(&format!(
            "{} data copied; indexes pending (run `pond optimize --only index --storage-path {}`)",
            pond::output::paint("done -", dim),
            to_resolved.display(),
        ))?;
    } else {
        output(&format!(
            "{} destination is ready to query",
            pond::output::paint("done -", dim),
        ))?;
    }
    Ok(())
}

/// `pond copy --to <file>.pond`: write a compact restorable archive of the
/// source store's canonical datasets.
async fn copy_store_to_archive(
    from: StorageUrl,
    path: &Path,
    loaded: &Config,
) -> anyhow::Result<()> {
    let (_, store) = open_store(Some(from), loaded, false).await?;
    let summary = export_pond_archive(&store, path).await?;
    output(&format!(
        "{} {}  sessions={} messages={} parts={}",
        pond::output::paint("copy:", pond::output::dim()),
        path.display(),
        summary.rows.sessions,
        summary.rows.messages,
        summary.rows.parts,
    ))?;
    Ok(())
}

/// `pond copy --to <file>.jsonl` (or `-`): stream the source store as the JSONL
/// wire representation, one `IngestEvent` per line.
async fn copy_store_to_jsonl(
    from: StorageUrl,
    path: Option<PathBuf>,
    loaded: &Config,
) -> anyhow::Result<()> {
    let (_, store) = open_store(Some(from), loaded, false).await?;
    let to_stdout = path.is_none();
    let summary = match path {
        Some(path) => {
            let file = tokio::fs::File::create(&path)
                .await
                .with_context(|| format!("failed to open {}", path.display()))?;
            let mut writer = tokio::io::BufWriter::new(file);
            let summary = handlers::pond_export(&store, None, &mut writer).await?;
            writer.flush().await.context("copy: flush")?;
            summary
        }
        None => {
            let mut stdout = tokio::io::stdout();
            handlers::pond_export(&store, None, &mut stdout).await?
        }
    };
    let line = format!(
        "{} jsonl sessions={} messages={} parts={}",
        pond::output::paint("copy:", pond::output::dim()),
        summary.sessions,
        summary.messages,
        summary.parts,
    );
    // With `--to -` the wire stream owns stdout; keep the summary off it so
    // `pond copy --to - | jq` stays clean.
    if to_stdout {
        output_err(&line)?;
    } else {
        output(&line)?;
    }
    Ok(())
}

/// `pond copy --from <file>.pond`: restore an archive into the destination
/// store via the idempotent union merge.
async fn copy_archive_to_store(
    path: &Path,
    to: StorageUrl,
    no_optimize: bool,
    loaded: &Config,
) -> anyhow::Result<()> {
    let (_, store) = open_store(Some(to), loaded, false).await?;
    let summary = import_pond_archive(&store, path).await?;
    render_copy_import(&summary)?;
    let dim = pond::output::dim();
    // Leave the destination queryable, like a store-to-store copy: the archive
    // carries embeddings as data columns, so this is index-build only, no re-embed.
    if no_optimize {
        output(&format!(
            "{} indexes not rebuilt (--no-optimize); run `pond optimize --only index` before querying",
            pond::output::paint("hint", dim),
        ))?;
    } else {
        let policy = configured_maintenance_policy(loaded, None)?;
        // Same finalize seam as optimize/sync; the archive carries embeddings as
        // data columns, so finalize's embed pass no-ops.
        finalize_indexes(&store, &policy, false).await?;
        output(&format!(
            "{} rebuilt text + semantic on destination",
            pond::output::paint("indexes:", dim),
        ))?;
    }
    Ok(())
}

/// The 7-field `copy:` import recap shared by store-to-store copy and archive
/// restore - both land a `LanceArchiveImport` and report it identically.
fn render_copy_import(import: &LanceArchiveImport) -> anyhow::Result<()> {
    output(&format!(
        "{} sessions={} messages={} parts={} inserted_sessions={} inserted_messages={} inserted_parts={}",
        pond::output::paint("copy:", pond::output::dim()),
        import.rows.sessions,
        import.rows.messages,
        import.rows.parts,
        import.inserted.sessions,
        import.inserted.messages,
        import.inserted.parts,
    ))
}

/// The string `[storage].path` should carry for `url`: the display form
/// (contracted local path / verbatim remote URL) minus the trailing slash
/// Lance's `uri_to_url` appends to directory paths.
fn storage_config_value(url: &StorageUrl) -> String {
    let display = url.display();
    if url.is_local() && display.len() > 1 && display.ends_with('/') {
        display.trim_end_matches('/').to_owned()
    } else {
        display
    }
}

/// Set `[storage].path` in `doc` as a proper `[storage]` section (index
/// assignment on an absent key would synthesize the inline
/// `storage = { path = ... }` form instead).
fn set_storage_path(doc: &mut toml_edit::DocumentMut, path_value: &str) {
    use toml_edit::{Item, Table, value};
    match doc.get_mut("storage").and_then(Item::as_table_like_mut) {
        Some(storage) => {
            storage.insert("path", value(path_value));
        }
        None => {
            let mut storage = Table::new();
            storage.insert("path", value(path_value));
            doc.insert("storage", Item::Table(storage));
        }
    }
}

/// `pond storage use`: switch-only. Probe the destination end-to-end, then flip
/// `[storage].path`. It copies nothing - moving data into a destination is
/// `pond copy`, always a separate explicit step - so switching
/// stores, or rolling back to a previous one, never touches any store's data.
async fn run_storage_use(
    loaded: &Config,
    config_file: &Path,
    storage_path: Option<StorageUrl>,
    url: &str,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    let dest = match parse_storage_path(url) {
        Ok(parsed) => parsed,
        Err(error) => {
            output_err(&format!("use: parse error: {error:#}"))?;
            std::process::exit(2);
        }
    };
    let dest_resolved = dest.resolve(&loaded.creds)?;
    output(&format!(
        "{}  {}",
        dest_resolved.display(),
        paint(&format!("[{}]", dest_resolved.binding.describe()), dim()),
    ))?;
    warn_unmatched_sets(&[&dest_resolved], loaded)?;

    // Probe before any config change - same exit codes as `check`, so a typo or
    // auth failure can never strand the config pointing at a broken destination.
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.green} probing destination... [{elapsed_precise}]")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.enable_steady_tick(Duration::from_millis(120));
    let check = pond::substrate::storage_check(&dest_resolved).await;
    spinner.finish_and_clear();
    if let Err(failure) = check {
        output_err(&format!(
            "use: destination failed the end-to-end check; config not changed: {failure}"
        ))?;
        render_check_cause(&failure)?;
        std::process::exit(check_failure_exit_code(&failure));
    }
    output("check: ok - conditional put (OCC), read-back, and delete all succeeded")?;

    let current = resolve_storage_location(storage_path, loaded)?;
    let already_configured = loaded.storage.path.as_deref() == Some(dest.canonical().as_str())
        || loaded.storage.path.as_deref().is_some_and(|path| {
            StorageUrl::parse(path)
                .map(|parsed| parsed.canonical() == dest.canonical())
                .unwrap_or(false)
        });
    if current.canonical() == dest.canonical() && already_configured {
        output(&format!(
            "{} {} is already the configured destination - nothing to change",
            paint("use:", dim()),
            dest.display(),
        ))?;
        return Ok(());
    }

    let mut doc = read_config_doc(config_file)?;
    let path_value = storage_config_value(&dest);
    set_storage_path(&mut doc, &path_value);
    write_config_doc(config_file, &doc)?;
    output(&format!(
        "{} [storage].path = {} written to {}",
        paint("use:", dim()),
        path_value,
        config::display(&config::url_for_path(config_file)?),
    ))?;
    // `use` reads no store but the destination probe above - so it names the
    // explicit copy step with both URLs filled in, rather than counting rows to
    // guess whether a copy is wanted. Moving the old data over is one paste away.
    output(&format!(
        "{} previous data at {} is untouched; copy it over with `pond copy --from {} --to {}`",
        paint("hint", dim()),
        current.display(),
        current.display(),
        dest.display(),
    ))?;
    Ok(())
}

/// Read `config_file` into an editable TOML document, preserving formatting and
/// comments; an absent file parses as empty.
fn read_config_doc(config_file: &Path) -> anyhow::Result<toml_edit::DocumentMut> {
    let existing = if config_file.exists() {
        fs::read_to_string(config_file)
            .with_context(|| format!("failed to read {}", config_file.display()))?
    } else {
        String::new()
    };
    existing
        .parse()
        .with_context(|| format!("failed to parse {} as TOML", config_file.display()))
}

/// Write an edited TOML document back to `config_file`, creating its directory.
fn write_config_doc(config_file: &Path, doc: &toml_edit::DocumentMut) -> anyhow::Result<()> {
    if let Some(parent) = config_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    pond::config::write_config_file(config_file, &doc.to_string())
}

/// `pond creds {add,list,delete}`: manage the `[creds.<name>]` sets in
/// config.toml. Sets bind to URLs by scope match, never activation - there is
/// no "current" set to select (spec.md#creds-scope-match).
fn run_creds(command: CredsCmd, config_file: &Path, loaded: &Config) -> anyhow::Result<()> {
    match command {
        CredsCmd::Add {
            name,
            scope,
            region,
        } => creds_add(config_file, name, scope, region),
        CredsCmd::List { format } => creds_list(loaded, format),
        CredsCmd::Delete { name } => creds_delete(config_file, name),
    }
}

fn creds_add(
    config_file: &Path,
    name: Option<String>,
    scope: Option<String>,
    region: Option<String>,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    // The secret is captured via a hidden prompt, which needs a real terminal.
    // Non-interactive callers use POND_CREDS_<NAME>_* or a hand-written set.
    if !io::stdin().is_terminal() {
        bail!(
            "`pond creds add` needs a terminal for hidden secret entry; in non-interactive runs set POND_CREDS_<NAME>_ACCESS_KEY_ID / POND_CREDS_<NAME>_SECRET_ACCESS_KEY or add [creds.<name>] to config.toml"
        );
    }

    let name = match name {
        Some(name) => name,
        None => cliclack::input("Credential set name")
            .default_input("default")
            .interact()
            .context("credential prompt failed; nothing written")?,
    };
    if !config::valid_creds_set_name(&name) {
        bail!(
            "creds set name {name:?} must match [a-z][a-z0-9]{{0,15}} (lowercase alphanumeric, no separators)"
        );
    }

    // Replacing an existing set (rotating keys) is legitimate, but it overwrites
    // a stored secret - gate it on an explicit confirm, never a silent clobber.
    // Checked against the file, since only a file-defined set is what `add`
    // overwrites; confirm before prompting for keys so a decline costs nothing.
    let mut doc = read_config_doc(config_file)?;
    let exists = doc
        .get("creds")
        .and_then(toml_edit::Item::as_table_like)
        .is_some_and(|creds| creds.contains_key(&name));
    if exists
        && !cliclack::confirm(format!("Replace existing credential set [creds.{name}]?"))
            .initial_value(false)
            .interact()
            .context("credential prompt failed; nothing written")?
    {
        output(&format!(
            "{} kept existing [creds.{}]; nothing written",
            paint("creds:", dim()),
            name,
        ))?;
        return Ok(());
    }

    let access_key_id: String = cliclack::input("Access key ID")
        .interact()
        .context("credential prompt failed; nothing written")?;
    let secret_access_key: String = cliclack::password("Secret access key")
        .mask('*')
        .interact()
        .context("credential prompt failed; nothing written")?;

    // Build and validate the result in memory; only a config that parses and
    // passes the [creds.*] structural rules is written, so a bad set can't land.
    set_creds_set(
        &mut doc,
        &name,
        &access_key_id,
        &secret_access_key,
        scope.as_deref(),
        region.as_deref(),
    );
    Config::load_str(&doc.to_string())
        .context("the resulting config would be invalid; nothing written")?;
    write_config_doc(config_file, &doc)?;

    output(&format!(
        "{} [creds.{}] written to {} (secret redacted)",
        paint("creds:", dim()),
        name,
        config::display(&config::url_for_path(config_file)?),
    ))?;
    output(&format!(
        "{} verify it binds with `pond storage check <url>`",
        paint("hint", dim()),
    ))?;
    Ok(())
}

/// The redacted access-key / secret display strings shared by the table and
/// the JSON document - a real secret must never reach either surface.
fn creds_display_fields(set: &config::CredsSet) -> (String, String) {
    let access = match (&set.access_key_id, &set.access_key_id_file) {
        (Some(_), _) => "********".to_owned(),
        (None, Some(path)) => format!("file:{}", path.display()),
        (None, None) => "-".to_owned(),
    };
    let secret = if set.secret_access_key.is_some() {
        "********".to_owned()
    } else if let Some(path) = &set.secret_access_key_file {
        format!("file:{}", path.display())
    } else if let Some(command) = &set.secret_access_key_command {
        format!("command:{command}")
    } else {
        "-".to_owned()
    };
    (access, secret)
}

/// The `pond creds list --format json` document: one entry per set, with
/// `access_key`/`secret` carrying the same redacted display strings the table
/// shows - never a real secret.
fn creds_list_json(loaded: &Config) -> anyhow::Result<String> {
    let sets: Vec<serde_json::Value> = loaded
        .creds
        .iter()
        .map(|(name, set)| {
            let (access, secret) = creds_display_fields(set);
            serde_json::json!({
                "name": name,
                "scope": set.scope,
                "access_key": access,
                "secret": secret,
                "region": set.region,
            })
        })
        .collect();
    serde_json::to_string_pretty(&sets).context("serialize creds list as JSON")
}

fn creds_list(loaded: &Config, format: OutputFormat) -> anyhow::Result<()> {
    if let OutputFormat::Json = format {
        output(&creds_list_json(loaded)?)?;
        return Ok(());
    }
    if loaded.creds.is_empty() {
        output(
            "no credential sets configured - add one with `pond creds add`, or set POND_CREDS_<NAME>_* in the environment",
        )?;
        return Ok(());
    }
    let mut table = new_table();
    table.set_header(vec!["set", "scope", "access key", "secret", "region"]);
    for (name, set) in &loaded.creds {
        let scope = set
            .scope
            .clone()
            .unwrap_or_else(|| "(catch-all)".to_owned());
        let (access, secret) = creds_display_fields(set);
        let region = set.region.clone().unwrap_or_else(|| "-".to_owned());
        table.add_row(vec![name.clone(), scope, access, secret, region]);
    }
    output(&table.to_string())?;
    Ok(())
}

fn creds_delete(config_file: &Path, name: String) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    let mut doc = read_config_doc(config_file)?;
    let removed = doc
        .get_mut("creds")
        .and_then(toml_edit::Item::as_table_like_mut)
        .is_some_and(|creds| creds.remove(&name).is_some());
    if !removed {
        bail!(
            "no [creds.{name}] set in {}",
            config::display(&config::url_for_path(config_file)?)
        );
    }
    if doc
        .get("creds")
        .and_then(toml_edit::Item::as_table_like)
        .is_some_and(toml_edit::TableLike::is_empty)
    {
        doc.remove("creds");
    }
    write_config_doc(config_file, &doc)?;
    output(&format!(
        "{} [creds.{}] removed",
        paint("creds:", dim()),
        name
    ))?;

    // Rotation must never silently strand the default store: if removing the
    // set leaves the active storage URL matching nothing, say so.
    if let Ok(reloaded) = Config::load(config_file)
        && let Some(path) = reloaded.storage.path.as_deref()
        && let Ok(url) = StorageUrl::parse(path)
        && let Ok(resolved) = url.resolve(&reloaded.creds)
        && matches!(resolved.binding, CredsBinding::Ambient)
    {
        output(&format!(
            "{} active storage {} now matches no creds set; it will fall back to the ambient cloud chain",
            paint("note:", dim()),
            url.display(),
        ))?;
    }
    Ok(())
}

/// Write `[creds.<name>]` into `doc`, replacing any existing set of that name.
/// Only the captured fields are emitted; `scope`/`region` only when provided.
fn set_creds_set(
    doc: &mut toml_edit::DocumentMut,
    name: &str,
    access_key_id: &str,
    secret_access_key: &str,
    scope: Option<&str>,
    region: Option<&str>,
) {
    use toml_edit::{Item, Table, value};
    let mut set = Table::new();
    if let Some(scope) = scope {
        set.insert("scope", value(scope));
    }
    set.insert("access_key_id", value(access_key_id));
    set.insert("secret_access_key", value(secret_access_key));
    if let Some(region) = region {
        set.insert("region", value(region));
    }
    match doc.get_mut("creds").and_then(Item::as_table_like_mut) {
        Some(creds) => {
            creds.insert(name, Item::Table(set));
        }
        None => {
            let mut parent = Table::new();
            // Implicit so the file renders `[creds.<name>]`, not a bare `[creds]`.
            parent.set_implicit(true);
            parent.insert(name, Item::Table(set));
            doc.insert("creds", Item::Table(parent));
        }
    }
}

/// `pond adapters {list,discover,enable,disable}`: the explicit owner of
/// `[adapters.*]` state. `pond sync` reads these entries but never writes them.
fn run_adapters(command: AdaptersCmd, config_file: &Path, loaded: &Config) -> anyhow::Result<()> {
    match command {
        AdaptersCmd::List { format } => adapters_list(loaded, format),
        AdaptersCmd::Discover => adapters_discover(config_file),
        AdaptersCmd::Enable { name } => adapters_enable(config_file, &name),
        AdaptersCmd::Disable { name } => adapters_disable(config_file, &name),
    }
}

/// The `pond adapters list --format json` document: configured adapters
/// (enabled state, home-contracted path or null) plus detected-but-unconfigured
/// ones, sharing the exact `path` contraction the text table applies.
fn adapters_list_json(loaded: &Config) -> anyhow::Result<String> {
    let detected = adapter::probe_unconfigured(&loaded.adapters);
    let configured: Vec<serde_json::Value> = loaded
        .adapters
        .iter()
        .map(|(name, blob)| {
            let enabled = blob
                .get("enabled")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let path = blob
                .get("path")
                .and_then(serde_json::Value::as_str)
                .map(|p| config::contract_home(Path::new(p)).display().to_string());
            serde_json::json!({ "name": name, "enabled": enabled, "path": path })
        })
        .collect();
    let detected_json: Vec<serde_json::Value> = detected
        .iter()
        .map(|c| serde_json::json!({ "name": c.name, "hint": c.hint }))
        .collect();
    let doc = serde_json::json!({ "configured": configured, "detected": detected_json });
    serde_json::to_string_pretty(&doc).context("serialize adapters list as JSON")
}

fn adapters_list(loaded: &Config, format: OutputFormat) -> anyhow::Result<()> {
    if let OutputFormat::Json = format {
        output(&adapters_list_json(loaded)?)?;
        return Ok(());
    }
    let detected = adapter::probe_unconfigured(&loaded.adapters);
    if loaded.adapters.is_empty() && detected.is_empty() {
        output(
            "no adapters configured and none detected - run `pond adapters discover` (or `pond init`)",
        )?;
        return Ok(());
    }
    let mut table = new_table();
    table.set_header(vec!["adapter", "enabled", "path"]);
    for (name, blob) in &loaded.adapters {
        let enabled = blob
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let path = blob
            .get("path")
            .and_then(serde_json::Value::as_str)
            .map(|p| config::contract_home(Path::new(p)).display().to_string())
            .unwrap_or_else(|| "-".to_owned());
        let state = if enabled { "yes" } else { "no" }.to_owned();
        table.add_row(vec![name.clone(), state, path]);
    }
    for candidate in &detected {
        table.add_row(vec![
            candidate.name.clone(),
            "detected".to_owned(),
            candidate.hint.clone(),
        ]);
    }
    output(&table.to_string())?;
    Ok(())
}

fn adapters_discover(config_file: &Path) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let candidates = adapter::discover(None);
    let picks = adapter::prompt_and_persist(config_file, &candidates, io::stdin().is_terminal())?;
    let names: Vec<&str> = picks.iter().map(|c| c.name.as_str()).collect();
    output(&format!(
        "{} enabled {} adapter(s): {}",
        paint("adapters:", dim()),
        picks.len(),
        names.join(", "),
    ))?;
    Ok(())
}

fn adapters_enable(config_file: &Path, name: &str) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    let known = adapter::known_names();
    if !known.contains(&name) {
        bail!("unknown adapter {name:?}; known: {}", known.join(", "));
    }
    // Already configured: flip enabled = true in place, keeping its path/options.
    if adapter::set_adapter_enabled(config_file, name, true)? {
        output(&format!(
            "{} [adapters.{}] enabled = true",
            paint("adapters:", dim()),
            name,
        ))?;
        return Ok(());
    }
    // Not configured: discover its default path, then persist enabled = true.
    let candidate = adapter::discover(Some(name))
        .into_iter()
        .find(|c| c.name == name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "adapter {name:?} was not detected on this machine; add an [adapters.{name}] entry with its path manually, or run `pond sync {name} --path <path>` for a one-off"
            )
        })?;
    adapter::persist_accept(config_file, &[candidate])?;
    output(&format!(
        "{} [adapters.{}] enabled = true (discovered)",
        paint("adapters:", dim()),
        name,
    ))?;
    Ok(())
}

fn adapters_disable(config_file: &Path, name: &str) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    if adapter::set_adapter_enabled(config_file, name, false)? {
        output(&format!(
            "{} [adapters.{}] enabled = false",
            paint("adapters:", dim()),
            name,
        ))?;
        Ok(())
    } else {
        bail!(
            "no [adapters.{name}] entry to disable in {}",
            config_file.display()
        );
    }
}

/// Map a figment provenance lookup onto the source column vocabulary.
fn classify_source(figment: &figment::Figment, key: &str) -> &'static str {
    match figment.find_metadata(key) {
        Some(md) if md.name.contains("POND_") => "env",
        Some(_) => "file",
        None => "default",
    }
}

/// Binding line for `pond config show`: the set name, how it matched, and
/// which layer defined it ("creds work (scope match, file)").
fn describe_binding_with_source(binding: &CredsBinding, figment: &figment::Figment) -> String {
    match binding {
        CredsBinding::Set { name, .. } => {
            let source = classify_source(figment, &format!("creds.{name}"));
            let base = binding.describe();
            format!("{}, {source})", base.trim_end_matches(')'))
        }
        other => other.describe(),
    }
}

/// Flatten the serialized config into dotted (key, value) leaf rows,
/// skipping `null`s (unset optionals) and empty containers.
fn flatten_config(prefix: String, value: &Value, rows: &mut Vec<(String, String)>) {
    match value {
        Value::Null => {}
        Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten_config(path, child, rows);
            }
        }
        Value::String(text) => rows.push((prefix, text.clone())),
        other => rows.push((prefix, other.to_string())),
    }
}

/// spec.md#storage-redaction: any field whose name contains key / secret /
/// token / password prints `********` regardless of length - including keys
/// inside `extra`. The `_file` / `_command` variants print their literal
/// value: the path or command IS the safe part.
fn redact_config_value(key: &str, value: &str) -> String {
    let field = key.rsplit('.').next().unwrap_or(key).to_ascii_lowercase();
    if field.ends_with("_file") || field.ends_with("_command") {
        return value.to_owned();
    }
    let sensitive = ["key", "secret", "token", "password"];
    if sensitive.iter().any(|needle| field.contains(needle)) {
        return "********".to_owned();
    }
    value.to_owned()
}

/// Store-to-store `pond copy` core: export the source's clean datasets into
/// local staging, then merge-import into the destination. Idempotency comes from
/// `lance-deterministic-pk` + merge-insert: a rerun inserts nothing, and a
/// populated destination unions rather than clobbers.
/// Resolve both URLs against the loaded creds, print the from/to binding
/// lines (a wrong scope match must surface before any work, not after an auth
/// error), and open both stores. Shared by store-to-store copy and verify.
async fn resolve_and_open_pair(
    from: &StorageUrl,
    to: &StorageUrl,
    loaded: &Config,
) -> anyhow::Result<(ResolvedStorage, Store, ResolvedStorage, Store)> {
    let from_resolved = from.resolve(&loaded.creds)?;
    let to_resolved = to.resolve(&loaded.creds)?;
    warn_unmatched_sets(&[&from_resolved, &to_resolved], loaded)?;
    let dim = pond::output::dim();
    output(&format!(
        "{} {}  {}",
        pond::output::paint("from:", dim),
        from_resolved.display(),
        pond::output::paint(&format!("[{}]", from_resolved.binding.describe()), dim),
    ))?;
    output(&format!(
        "{}   {}  {}",
        pond::output::paint("to:", dim),
        to_resolved.display(),
        pond::output::paint(&format!("[{}]", to_resolved.binding.describe()), dim),
    ))?;
    let from_store = Store::open_with_options(
        from_resolved.lance_url(),
        from_resolved.options.clone(),
        runtime_caps(loaded),
    )
    .await?;
    let to_store = Store::open_with_options(
        to_resolved.lance_url(),
        to_resolved.options.clone(),
        runtime_caps(loaded),
    )
    .await?;
    Ok((from_resolved, from_store, to_resolved, to_store))
}

/// Per-table membership: `missing` = source ids absent from the destination,
/// the count that must be zero for the destination to fully contain the
/// source. A destination may legitimately hold more (copy never deletes),
/// so a surplus is not a failure and is not surfaced.
struct TableVerify {
    table: substrate::Table,
    source_rows: usize,
    missing: usize,
}

struct StorageVerify {
    tables: Vec<TableVerify>,
    dest_indexes: IndexCoverage,
}

impl StorageVerify {
    fn synced(&self) -> bool {
        self.tables.iter().all(|t| t.missing == 0)
    }
    fn total_missing(&self) -> usize {
        self.tables.iter().map(|t| t.missing).sum()
    }
    fn total_source_rows(&self) -> usize {
        self.tables.iter().map(|t| t.source_rows).sum()
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexCoverage {
    fts_present: bool,
    vector_present_or_below_activation: bool,
}

impl IndexCoverage {
    fn ready(self) -> bool {
        self.fts_present && self.vector_present_or_below_activation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VerifyOutcome {
    synced: bool,
}

/// Compare the `id` set of every table across two stores - read-only on both
/// sides. Matching row counts can hide divergent membership, so verification
/// keys on the deterministic per-row id, not the cardinalities. Drives
/// `pond copy`'s closing check and `pond copy --verify-only`.
async fn verify_stores(from: &Store, to: &Store) -> anyhow::Result<StorageVerify> {
    use substrate::Table;
    // One fan-out over both stores x three tables, not three sequential
    // round-trip pairs - on a remote backend that is 3x fewer round-trip waits.
    let (s_from, s_to, m_from, m_to, p_from, p_to, dest_index_status, dest_embedding) = tokio::try_join!(
        from.collect_ids(Table::Sessions),
        to.collect_ids(Table::Sessions),
        from.collect_ids(Table::Messages),
        to.collect_ids(Table::Messages),
        from.collect_ids(Table::Parts),
        to.collect_ids(Table::Parts),
        to.index_status(),
        to.embedding_progress(),
    )?;
    let tables = [
        (Table::Sessions, s_from, s_to),
        (Table::Messages, m_from, m_to),
        (Table::Parts, p_from, p_to),
    ]
    .into_iter()
    .map(|(table, source, dest)| TableVerify {
        table,
        source_rows: source.len(),
        missing: source.difference(&dest).count(),
    })
    .collect();
    let fts_present = dest_index_status
        .iter()
        .any(|status| status.intent_name == MESSAGES_FTS_INDEX && status.exists);
    let vector_present = dest_index_status
        .iter()
        .any(|status| status.intent_name == MESSAGES_VECTOR_INDEX && status.exists);
    let dest_indexes = IndexCoverage {
        fts_present,
        vector_present_or_below_activation: vector_present
            || dest_embedding.embedded < VECTOR_INDEX_ACTIVATION_ROWS,
    };
    Ok(StorageVerify {
        tables,
        dest_indexes,
    })
}

/// A 0-row source almost always means a mistyped `--from`: a remote store
/// auto-vivifies empty, so the copy would otherwise "succeed" having moved
/// nothing. The `run_copy` guard catches a missing local path; this catches
/// the remote/empty case for parity (spec.md#session-durable-copy).
fn ensure_source_not_empty(verify: &StorageVerify, from_display: &str) -> anyhow::Result<()> {
    if verify.total_source_rows() == 0 {
        bail!(
            "source store {from_display} has 0 rows; check --from (a mistyped source opens as an empty store)"
        );
    }
    Ok(())
}

/// One-line SYNCED / FAILED verdict so the user never guesses whether a copy
/// fully landed. On failure it names the short tables and the exact
/// idempotent re-run.
fn render_storage_verify(
    verify: &StorageVerify,
    from_display: &str,
    to_display: &str,
) -> anyhow::Result<VerifyOutcome> {
    use pond::output::{dim, paint};
    if verify.synced() {
        let detail = verify
            .tables
            .iter()
            .map(|t| {
                format!(
                    "{} {}",
                    t.table.as_str(),
                    format_thousands(t.source_rows as u64)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        output(&format!(
            "{} SYNCED - destination contains all {} source rows ({})",
            paint("verify:", dim()),
            format_thousands(verify.total_source_rows() as u64),
            detail,
        ))?;
        if !verify.dest_indexes.ready() {
            output(&format!(
                "{} data copied; indexes pending - run `pond optimize --only index --storage-path {to_display}` before querying",
                paint("verify:", dim()),
            ))?;
        }
        Ok(VerifyOutcome { synced: true })
    } else {
        let short = verify
            .tables
            .iter()
            .filter(|t| t.missing > 0)
            .map(|t| {
                format!(
                    "{} {}",
                    t.table.as_str(),
                    format_thousands(t.missing as u64)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        output_err(&format!(
            "{} FAILED - {} source rows missing from destination ({})",
            paint("verify:", dim()),
            format_thousands(verify.total_missing() as u64),
            short,
        ))?;
        output_err(&paint(
            &format!(
                "  fix: re-run `pond copy --from {from_display} --to {to_display}` (idempotent; copies only the missing rows)"
            ),
            dim(),
        ))?;
        Ok(VerifyOutcome { synced: false })
    }
}

#[derive(Debug, Default)]
struct OptimizeStages {
    embed: bool,
    index: bool,
}

impl OptimizeStages {
    fn resolve(only: Option<OptimizeStage>, skip: &[OptimizeStage]) -> anyhow::Result<Self> {
        let mut stages = match only {
            Some(OptimizeStage::Embed) => Self {
                embed: true,
                ..Self::default()
            },
            Some(OptimizeStage::Index) => Self {
                index: true,
                ..Self::default()
            },
            None => Self {
                embed: true,
                index: true,
            },
        };
        for stage in skip {
            match stage {
                OptimizeStage::Embed => stages.embed = false,
                OptimizeStage::Index => stages.index = false,
            }
        }
        if !(stages.embed || stages.index) {
            bail!("no optimize stages selected");
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
}

async fn run_import_stage(
    store: &Store,
    loaded: &Config,
    config_file: &Path,
    adapter: Option<String>,
    path: Option<PathBuf>,
    verify: bool,
) -> anyhow::Result<IngestSummary> {
    let adapters = resolve_sync_adapters(loaded, adapter.as_deref(), path)?;
    if adapters.is_empty() {
        let disabled = loaded.disabled_adapter_names();
        let label = pond::output::paint("import:", pond::output::dim());
        if disabled.is_empty() {
            output(&format!(
                "{label} no adapters configured. Run `pond adapters discover` (or `pond init`) to detect and enable adapters, or add `[adapters.<name>]` blocks to {}.",
                config_file.display(),
            ))?;
        } else {
            output(&format!(
                "{label} no enabled adapters. Found {} disabled: {}. Enable one with `pond adapters enable <name>` (or add `enabled = true` to its section in {}).",
                disabled.len(),
                disabled.join(", "),
                config_file.display(),
            ))?;
        }
        return Ok(IngestSummary::default());
    }
    // `--verify` bypasses the freshness skip: a `NoopOracle` returns no
    // watermark, so every source body is re-decoded and re-ingested through the
    // idempotent merge. This is the only path that heals historical M1 damage -
    // a session partially flushed before the commit-row-last fix keeps a frozen
    // watermark that mtime can never re-read past (spec.md#session-movement-complete).
    let noop = pond::adapter::NoopOracle;
    let watermarks;
    let oracle: &dyn pond::adapter::SkipOracle = if verify {
        output(&pond::output::paint(
            "import: --verify: re-reading every source body, bypassing the freshness skip",
            pond::output::yellow(),
        ))?;
        &noop
    } else {
        watermarks = store.session_last_ingested_at().await?;
        &watermarks
    };
    let mut total = IngestSummary::default();
    for (name, blob) in adapters {
        let summary = sync_with_progress(store, &name, blob, oracle).await?;
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
                "embed: --force-embed: re-embedding {} stale-model row(s) after dropping IVF_SQ",
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

/// Resolve the `MaintenancePolicy` for one optimize/sync invocation: start
/// from `[maintenance]` (or the in-process defaults), then apply the CLI
/// `--cleanup-older-than` override when present.
fn configured_maintenance_policy(
    config: &Config,
    cleanup_override: Option<chrono::Duration>,
) -> anyhow::Result<MaintenancePolicy> {
    let compaction_fragment_cap = config
        .maintenance
        .compaction_fragment_cap
        .unwrap_or(pond::substrate::DEFAULT_COMPACTION_FRAGMENT_CAP);
    let configured_cleanup = config
        .maintenance
        .cleanup_older_than
        .as_deref()
        .map(parse_retention)
        .transpose()
        .map_err(|err| anyhow::anyhow!("invalid [maintenance].cleanup_older_than: {err}"))?;
    let cleanup_older_than = cleanup_override
        .or(configured_cleanup)
        .unwrap_or_else(default_cleanup_older_than);
    // Readers pin a manifest version per request; cleanup reclaiming a pinned
    // version's files breaks in-flight reads on object-store backends.
    if cleanup_older_than < chrono::Duration::hours(1) {
        anyhow::bail!(
            "cleanup retention below the 1h floor; set [maintenance].cleanup_older_than \
             (or --cleanup-older-than) to 1h or longer"
        );
    }
    Ok(MaintenancePolicy {
        compaction_fragment_cap,
        cleanup_older_than,
    })
}

async fn run_update_indexes_stage(
    store: &Store,
    policy: &MaintenancePolicy,
) -> anyhow::Result<OptimizeOutcome> {
    let (progress, bar) = optimize_progress_bar();
    let outcome = store.optimize_indices(Some(progress), policy).await?;
    bar.finish_and_clear();
    // No `index:` recap line: the `render_sync_summary` (or `pond status`)
    // `indexes  text + semantic ready` line is the single source of truth
    // for index health. Hints below still fire on real conflicts / failures.
    render_optimize_hints(&outcome)?;
    Ok(outcome)
}

/// The shared index-finalize stage for a full `pond optimize`, `pond sync`, and
/// `pond copy`: embed the backlog - a no-op when every row is already embedded,
/// as with a same-model copy - then fold every trailing fragment into the
/// indexes and run the compaction + version-cleanup pass. One seam so the three
/// entry points can't drift in what "indexes up to date" means.
async fn finalize_indexes(
    store: &Store,
    policy: &MaintenancePolicy,
    force_embed: bool,
) -> anyhow::Result<OptimizeOutcome> {
    run_embed_stage(store, force_embed).await?;
    run_update_indexes_stage(store, policy).await
}

/// Final recap of a `pond sync` run. Three-line shape:
///
/// ```text
///
/// indexes   text + semantic ready
/// added     +44 sessions, +2,337 messages
/// stored    8,460 sessions, 172,710 searchable messages
/// ```
///
/// `added` is suppressed when both deltas are zero (a no-op sync still
/// always prints the `stored` line so an operator can confirm corpus
/// state without re-running `pond status`). The trailing disclaimer
/// goes to stderr per the rust-cli/book result-vs-meta discipline.
async fn render_sync_summary(store: &Store, summary: &SyncRunSummary) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    let (sessions_added, messages_added) = match &summary.ingest {
        Some(ingest) => (
            ingest.sessions_inserted as u64,
            ingest.messages_inserted_searchable as u64,
        ),
        None => (0, 0),
    };

    let (index_status, embedding, (stored_sessions, _, _)) = tokio::try_join!(
        store.index_status(),
        store.embedding_progress(),
        store.row_counts(),
    )?;
    let health = classify_index_health(&index_status, Some(&embedding));

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
        "{}    {} sessions, {} searchable messages",
        paint("stored", dim()),
        format_thousands(stored_sessions as u64),
        format_thousands(embedding.total as u64),
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
/// Read-only over config - enabling an adapter is `pond adapters` / `pond init`,
/// never a side effect of sync (spec.md#cli-verbs).
///
/// Precedence:
/// 1. `--path <dir>` with `<adapter>` set: one-off run, no config writes.
/// 2. `<adapter>` set, `[adapters.<adapter>]` present: use that.
/// 3. `<adapter>` set, no config entry: error pointing at `pond adapters enable`.
/// 4. No `<adapter>`, `[adapters]` non-empty: sync every enabled entry.
/// 5. No `<adapter>`, empty `[adapters]`: error pointing at `pond adapters discover`.
fn resolve_sync_adapters(
    config: &Config,
    name: Option<&str>,
    path: Option<PathBuf>,
) -> anyhow::Result<Vec<(String, Value)>> {
    if let Some(path) = path {
        let name = name.ok_or_else(|| {
            anyhow::anyhow!("--path requires an explicit <adapter> positional argument")
        })?;
        let known = adapter::known_names();
        if !known.contains(&name) {
            bail!("unknown adapter {name:?}; known: {}", known.join(", "));
        }
        // `--path` is a filesystem-shaped override. Adapters that need
        // a richer config blob can't use this path; they must edit config.toml.
        return Ok(vec![(name.to_owned(), json!({ "path": path }))]);
    }

    if let Some(name) = name {
        let known = adapter::known_names();
        if !known.contains(&name) {
            bail!("unknown adapter {name:?}; known: {}", known.join(", "));
        }
        if config.adapters.contains_key(name) {
            // spec.md#cli-verbs: sync only ingests already-enabled adapters and
            // never enables. resolve_adapters strips `enabled` and refuses a
            // disabled entry, pointing the user at `pond adapters enable`.
            return config.resolve_adapters(Some(name));
        }
        bail!(
            "adapter {name:?} has no [adapters.{name}] entry; enable it with `pond adapters enable {name}` (or `pond init`), then re-run `pond sync`"
        );
    }

    if !config.adapters.is_empty() {
        return config.resolve_adapters(None);
    }
    bail!(
        "no adapters configured; run `pond adapters discover` (or `pond init`) to detect and enable adapters, then re-run `pond sync`"
    );
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
        SyncEvent::SkippedBulk { status, count } => {
            match status {
                SyncStatus::Empty => {
                    let len = bar_ref.length().unwrap_or(0);
                    bar_ref.set_length(len.saturating_sub(count as u64));
                }
                _ => bar_ref.inc(count as u64),
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
/// [00:04:32] claude-code ok    project=/Users/you/Projects/app  session=58a96901-4a4f-40be-a3c1-62419ec8c580  msgs=512
/// [00:04:33] claude-code skip  /Users/you/.../58a96901-....jsonl: empty jsonl session
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
    let callback: OptimizeProgressFn = Arc::new(move |event| match event {
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
        OptimizeEvent::IndexStage {
            table,
            index,
            stage,
            completed,
            total,
            unit,
        } => {
            // Live intra-index progress, fired from Lance's IndexBuildProgress
            // callbacks. Re-render the spinner suffix every tick. Total may be
            // absent during init phases (Lance doesn't always know the count
            // upfront); show just the completed count then.
            let progress = match total {
                Some(t) if t > 0 => format!("{completed}/{t} {unit}"),
                _ => format!("{completed} {unit}"),
            };
            bar_for_callback.set_message(format!(
                "{} {} ({}): {} {}",
                table.as_str(),
                "index-build",
                index,
                stage,
                progress.trim_end(),
            ));
        }
    });
    (callback, bar)
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

/// Title + storage-destination line, shared by the populated header and the
/// empty-store render so `pond status` opens the same way in either state.
/// Per-index status word for `pond status --format json`, mirroring the
/// existence/backlog split the text `index detail` block surfaces.
fn index_status_label(status: &IndexStatus) -> &'static str {
    if !status.exists {
        "not_built"
    } else if status.unindexed_rows == 0 {
        "ready"
    } else {
        "pending"
    }
}

/// `pond status --format json` for a configured-but-never-synced store: just
/// the storage destination, no corpus/indexes/embedding (spec parity with the
/// text path's "no data yet" line).
fn status_json_empty(resolved: &ResolvedStorage) -> anyhow::Result<String> {
    let doc = serde_json::json!({
        "storage": {
            "url": resolved.display(),
            "binding": resolved.binding.describe(),
        },
        "initialized": false,
    });
    serde_json::to_string_pretty(&doc).context("serialize status as JSON")
}

/// `pond status --format json` for an initialized store: the same numbers the
/// text tables show, as one machine-readable document.
fn status_json(
    resolved: &ResolvedStorage,
    sizes: &TableSizes,
    totals: &RowTotals,
    adapter_count: usize,
    index_status: &[IndexStatus],
    embedding: Option<EmbeddingProgress>,
) -> anyhow::Result<String> {
    let total_bytes = sizes.sessions + sizes.messages + sizes.parts + sizes.other;
    let indexes: Vec<serde_json::Value> = index_status
        .iter()
        .map(|status| {
            serde_json::json!({
                "name": format!("{}.{}", status.table.as_str(), status.intent_name),
                "status": index_status_label(status),
            })
        })
        .collect();
    // null on default `pond status`; populated under `-v` (skipping the
    // 2M-row messages scan keeps default JSON status cheap on cold S3).
    let embedding_doc = embedding.map(|e| {
        serde_json::json!({
            "embedded": e.embedded,
            "eligible": e.total,
        })
    });
    let doc = serde_json::json!({
        "storage": {
            "url": resolved.display(),
            "binding": resolved.binding.describe(),
            "total_bytes": total_bytes,
            "tables": {
                "sessions": { "bytes": sizes.sessions, "rows": totals.sessions },
                "messages": { "bytes": sizes.messages, "rows": totals.messages },
                "parts": { "bytes": sizes.parts, "rows": totals.parts },
            },
        },
        "corpus": {
            "sessions": totals.sessions,
            "messages": totals.messages,
            "parts": totals.parts,
        },
        "indexes": indexes,
        "embedding": embedding_doc,
        "adapters": adapter_count,
        "schedule": crate::schedule::status_line(),
        "initialized": true,
    });
    serde_json::to_string_pretty(&doc).context("serialize status as JSON")
}

fn render_status_storage_line(title: &str, resolved: &ResolvedStorage) -> anyhow::Result<()> {
    use pond::output::{bold, dim, paint};
    output(&paint(title, bold()))?;
    output(&format!(
        "{}  {}  {}",
        paint("storage", dim()),
        resolved.display(),
        paint(&format!("[{}]", resolved.binding.describe()), dim()),
    ))?;
    Ok(())
}

/// Storage configured but never synced (no tables): the storage line plus a
/// pointer at `pond sync`, instead of erroring on the first table describe.
fn render_empty_status(title: &str, resolved: &ResolvedStorage) -> anyhow::Result<()> {
    use pond::output::{dim, paint};
    render_status_storage_line(title, resolved)?;
    output(&format!(
        "{}    no data yet - run `pond sync` to import sessions",
        paint("stored", dim()),
    ))?;
    Ok(())
}

fn render_status_header(
    title: &str,
    resolved: &ResolvedStorage,
    sizes: &TableSizes,
    totals: &RowTotals,
) -> anyhow::Result<()> {
    render_status_storage_line(title, resolved)?;

    let mut table = new_table();
    let total_bytes = sizes.sessions + sizes.messages + sizes.parts + sizes.other;
    let rows = [
        (
            "sessions",
            sizes.sessions,
            Some(totals.sessions),
            sizes.sessions_data,
        ),
        (
            "messages",
            sizes.messages,
            Some(totals.messages),
            sizes.messages_data,
        ),
        ("parts", sizes.parts, Some(totals.parts), sizes.parts_data),
        ("other", sizes.other, None, Default::default()),
    ];
    for (label, bytes, rows_opt, data) in rows {
        // Surface superseded data versions only when they matter: the gap
        // self-heals once manifests age past the cleanup retention window.
        let dead_note = data
            .dead()
            .filter(|dead| *dead > 64 * 1024 * 1024 && *dead * 10 > data.on_disk)
            .map(|dead| format!("{} pending cleanup", format_bytes(dead)))
            .unwrap_or_default();
        table.add_row(vec![
            Cell::new(format!("  {label}")),
            Cell::new(format_bytes(bytes)).set_alignment(CellAlignment::Right),
            Cell::new(
                rows_opt
                    .map(|n| format!("{} rows", format_thousands(n)))
                    .unwrap_or_default(),
            )
            .set_alignment(CellAlignment::Right),
            Cell::new(dead_note).set_alignment(CellAlignment::Right),
        ]);
    }
    table.add_row(vec![
        Cell::new("  total").add_attribute(Attribute::Bold),
        Cell::new(format_bytes(total_bytes))
            .set_alignment(CellAlignment::Right)
            .add_attribute(Attribute::Bold),
        Cell::new(""),
        Cell::new(""),
    ]);
    output(&table.to_string())?;
    Ok(())
}

/// Render the checks that can take longer on a large corpus. The command
/// prints storage first, then calls this once the bounded scans finish.
fn render_status_checks(
    totals: &RowTotals,
    adapter_count: usize,
    index_status: &[IndexStatus],
    embedding: Option<EmbeddingProgress>,
) -> anyhow::Result<()> {
    use pond::output::{dim, paint};

    output("")?;
    let health = classify_index_health(index_status, embedding.as_ref());
    output(&render_indexes_line(&health))?;
    // Default: `stored` shows the manifest row count (cheap); under `-v` the
    // embedding probe ran, so swap in the searchable subset and report it.
    let messages_label = match &embedding {
        Some(e) => format!("{} searchable messages", format_thousands(e.total as u64)),
        None => format!("{} messages", format_thousands(totals.messages)),
    };
    output(&format!(
        "{}    {} sessions, {}",
        paint("stored", dim()),
        format_thousands(totals.sessions),
        messages_label,
    ))?;
    output(&format!(
        "{}  {} adapter(s)",
        paint("adapters", dim()),
        adapter_count,
    ))?;
    output(&crate::schedule::status_line())?;
    output_err("")?;
    if embedding.is_none() {
        output_err(&paint(
            "(use -v for searchable message count + embedding backlog)",
            dim(),
        ))?;
    }
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
    embedding: Option<&EmbeddingProgress>,
) -> IndexHealth {
    use IndexHealthState::*;

    // `pond optimize` folds every trailing fragment (no lag threshold), so an
    // index is current only when nothing is unindexed; any tail means a fold is
    // still owed (e.g. after `--no-optimize`).
    fn classify_one(status: &IndexStatus) -> IndexHealthState {
        if !status.exists {
            return NotBuilt;
        }
        if status.unindexed_rows == 0 {
            Ready
        } else {
            Pending(status.unindexed_rows as u64)
        }
    }

    let mut text = NotBuilt;
    let mut semantic = NotBuilt;
    for status in statuses {
        match status.intent_name.as_str() {
            MESSAGES_FTS_INDEX => text = classify_one(status),
            MESSAGES_VECTOR_INDEX => semantic = classify_one(status),
            _ => {}
        }
    }
    // Semantic search misses unembedded rows even when IVF_SQ's own
    // unindexed-fragments check passes. Without the embedding probe (default
    // `pond status`), this correction is skipped: the IVF_SQ fragment-lag
    // check still flags indexed-but-unfolded rows; only unembedded ones hide.
    if let Some(progress) = embedding {
        let embed_backlog = progress.total.saturating_sub(progress.embedded);
        if embed_backlog > 0 && matches!(semantic, Ready) {
            semantic = Pending(embed_backlog as u64);
        }
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
/// error bodies through `jq`; text mode routes errors to stderr so stdout
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
        OutputFormat::Text => match envelope {
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
        OutputFormat::Text => match envelope {
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
        "{} {} matched {}  {} returned {}  {} {} searchable in scope",
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
        paint("of", dim()),
        paint(
            &format_thousands(response.searchable_in_scope as u64),
            bold()
        ),
    ))?;
    if response.sessions.is_empty() {
        // spec.md#search-absence-honesty: name the recovery when the scope
        // itself is empty - the filters, not the query, produced the zero.
        if response.searchable_in_scope == 0 {
            output(
                "scope is empty: the filters exclude every searchable message; widen or drop \
                 project/date filters",
            )?;
        }
        return Ok(());
    }
    for (idx, session) in response.sessions.iter().enumerate() {
        output("")?;
        render_search_session(idx + 1, session)?;
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn redaction_masks_secret_fields_but_spares_file_and_command_variants() {
        // spec.md#storage-redaction: name-based masking, including `extra`.
        assert_eq!(
            redact_config_value("creds.work.access_key_id", "AKIA"),
            "********"
        );
        assert_eq!(
            redact_config_value("creds.work.secret_access_key", "s"),
            "********"
        );
        assert_eq!(
            redact_config_value("creds.work.extra.sas_token", "t"),
            "********"
        );
        assert_eq!(
            redact_config_value("creds.work.extra.request_timeout", "60s"),
            "60s"
        );
        // The path / command IS the safe part.
        assert_eq!(
            redact_config_value("creds.work.access_key_id_file", "/k"),
            "/k"
        );
        assert_eq!(
            redact_config_value("creds.work.secret_access_key_command", "op read x"),
            "op read x"
        );
        assert_eq!(redact_config_value("creds.work.region", "fsn1"), "fsn1");
    }

    #[test]
    fn flatten_config_emits_dotted_leaves_and_skips_nulls() {
        let value = serde_json::json!({
            "storage": {"path": "/p"},
            "search": {"nprobes": null},
            "creds": {"work": {"region": "r", "extra": {"a": "b"}}},
        });
        let mut rows = Vec::new();
        flatten_config(String::new(), &value, &mut rows);
        assert!(rows.contains(&("storage.path".to_owned(), "/p".to_owned())));
        assert!(rows.contains(&("creds.work.extra.a".to_owned(), "b".to_owned())));
        assert!(rows.iter().all(|(key, _)| key != "search.nprobes"));
    }

    #[test]
    fn storage_location_resolves_flag_then_config_then_default() {
        let mut loaded = Config::default();
        // Built-in default: the platform-local dir.
        let resolved = resolve_storage_location(None, &loaded).unwrap();
        assert!(resolved.is_local());
        // `[storage].path` beats the default.
        loaded.storage.path = Some("s3://from-config/p".to_owned());
        let resolved = resolve_storage_location(None, &loaded).unwrap();
        assert_eq!(resolved.lance_url().as_str(), "s3://from-config/p");
        // The CLI/env flag (folded by clap) beats the file.
        let flag = StorageUrl::parse("s3://from-flag/p").unwrap();
        let resolved = resolve_storage_location(Some(flag), &loaded).unwrap();
        assert_eq!(resolved.lance_url().as_str(), "s3://from-flag/p");
    }

    #[test]
    fn parse_storage_path_local_keyword_resolves_to_the_default_dir() {
        // `local`/`default` (case-insensitive) resolve to the same place the
        // no-config fallback uses, so a rollback never needs the path typed out.
        let keyword = parse_storage_path("local").unwrap();
        assert!(keyword.is_local());
        assert_eq!(
            keyword.canonical(),
            default_local_storage().unwrap().canonical(),
        );
        assert_eq!(
            parse_storage_path("DEFAULT").unwrap().canonical(),
            keyword.canonical(),
        );
        // A directory literally named `local` is the relative path, not the keyword.
        let literal = parse_storage_path("./local").unwrap();
        assert_ne!(literal.canonical(), keyword.canonical());
    }

    #[test]
    fn cli_parses_and_subcommands_are_wired() {
        // The canonical clap derive self-check: catches conflicting flags,
        // broken groups, and missing value parsers at test time.
        Cli::command().debug_assert();
    }

    #[test]
    fn set_creds_set_round_trips_through_config() {
        let mut doc: toml_edit::DocumentMut =
            "[storage]\npath = \"/tmp/p\"\n".parse().expect("parse");
        set_creds_set(&mut doc, "default", "AKIA", "shh", None, None);
        set_creds_set(
            &mut doc,
            "work",
            "AKIB",
            "shh2",
            Some("s3+https://host/work"),
            Some("fsn1"),
        );
        let config = Config::load_str(&doc.to_string()).expect("valid config");
        let default = &config.creds["default"];
        assert_eq!(default.access_key_id.as_deref(), Some("AKIA"));
        assert_eq!(default.secret_access_key.as_deref(), Some("shh"));
        assert!(default.scope.is_none());
        let work = &config.creds["work"];
        assert_eq!(work.scope.as_deref(), Some("s3+https://host/work"));
        assert_eq!(work.region.as_deref(), Some("fsn1"));

        // Re-adding a name replaces in place; the single catch-all is preserved.
        set_creds_set(&mut doc, "default", "AKIC", "shh3", None, None);
        let config = Config::load_str(&doc.to_string()).expect("valid after replace");
        assert_eq!(config.creds.len(), 2);
        assert_eq!(
            config.creds["default"].access_key_id.as_deref(),
            Some("AKIC")
        );
    }

    #[test]
    fn creds_delete_removes_set_drops_empty_parent_and_errors_when_absent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_file = dir.path().join("config.toml");
        let mut doc: toml_edit::DocumentMut = String::new().parse().expect("parse empty");
        set_creds_set(&mut doc, "default", "AKIA", "shh", None, None);
        write_config_doc(&config_file, &doc).expect("write");

        assert!(
            creds_delete(&config_file, "missing".to_owned()).is_err(),
            "deleting an absent set must error"
        );
        creds_delete(&config_file, "default".to_owned()).expect("delete present set");
        let body = std::fs::read_to_string(&config_file).expect("read back");
        assert!(
            !body.contains("creds"),
            "emptied [creds] parent removed: {body:?}"
        );
    }

    #[test]
    fn adapters_list_json_carries_configured_and_detected_keys() {
        let config =
            Config::load_str("[adapters.claude-code]\nenabled = true\npath = \"/tmp/cc\"\n")
                .expect("configured");
        let json = adapters_list_json(&config).expect("json builds");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert!(value.get("configured").is_some(), "configured key present");
        assert!(value.get("detected").is_some(), "detected key present");
        let first = &value["configured"][0];
        assert_eq!(first["name"], "claude-code");
        assert_eq!(first["enabled"], true);
        assert_eq!(first["path"], "/tmp/cc");
    }

    #[test]
    fn creds_list_json_redacts_the_secret() {
        let config = Config::load_str(
            "[creds.work]\naccess_key_id = \"AKIASECRETKEY\"\nsecret_access_key = \"topsecretvalue\"\n",
        )
        .expect("configured");
        let json = creds_list_json(&config).expect("json builds");
        assert!(
            !json.contains("topsecretvalue"),
            "real secret must never appear: {json}"
        );
        assert!(
            !json.contains("AKIASECRETKEY"),
            "real access key must never appear: {json}"
        );
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(value[0]["name"], "work");
        assert_eq!(value[0]["access_key"], "********");
        assert_eq!(value[0]["secret"], "********");
    }

    #[test]
    fn resolve_sync_adapters_errors_point_at_the_adapters_commands() {
        // Empty config: sync names `pond adapters discover`, never enables.
        let empty = Config::load_str("").expect("empty config");
        let err = resolve_sync_adapters(&empty, None, None).expect_err("empty must error");
        assert!(
            err.to_string().contains("pond adapters discover"),
            "empty error should name discover: {err}"
        );

        // Named-but-unconfigured: sync names `pond adapters enable <name>`.
        let err = resolve_sync_adapters(&empty, Some("claude-code"), None)
            .expect_err("unconfigured named must error");
        assert!(
            err.to_string().contains("pond adapters enable claude-code"),
            "named error should name enable: {err}"
        );

        // Configured adapter resolves without touching config, and the
        // `enabled` discriminator is stripped before it reaches the factory.
        let configured =
            Config::load_str("[adapters.claude-code]\nenabled = true\npath = \"/tmp/cc\"\n")
                .expect("configured");
        let resolved =
            resolve_sync_adapters(&configured, Some("claude-code"), None).expect("resolves");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].0, "claude-code");
        assert!(
            resolved[0].1.get("enabled").is_none(),
            "enabled discriminator must not reach the factory blob",
        );

        // Configured-but-disabled: sync refuses and names the enable verb,
        // never silently syncing a disabled adapter (spec.md#cli-verbs).
        let disabled =
            Config::load_str("[adapters.claude-code]\nenabled = false\npath = \"/tmp/cc\"\n")
                .expect("disabled config");
        let err = resolve_sync_adapters(&disabled, Some("claude-code"), None)
            .expect_err("disabled named must error");
        assert!(
            err.to_string().contains("disabled"),
            "disabled error should say disabled: {err}"
        );
        assert!(
            err.to_string().contains("pond adapters enable claude-code"),
            "disabled error should name the enable verb: {err}"
        );
    }

    // Long-help snapshots for the root and every visible subcommand. The
    // help text IS the agent-facing API surface (the docs promise that
    // `pond <cmd> --help` carries examples), so a wording change must show
    // up in review as a snapshot diff. `max_term_width = 100` on the root
    // keeps wrapping deterministic regardless of the invoking terminal, and
    // long help never embeds the version string, so release bumps don't
    // churn these.
    #[test]
    fn help_snapshots() {
        let mut root = Cli::command();
        root.build();
        insta::assert_snapshot!("help_root", root.render_long_help().to_string());
        let visible: Vec<String> = root
            .get_subcommands()
            .filter(|sub| !sub.is_hide_set() && sub.get_name() != "help")
            .map(|sub| sub.get_name().to_owned())
            .collect();
        for name in visible {
            let sub = root
                .find_subcommand_mut(&name)
                .expect("visible subcommand exists");
            insta::assert_snapshot!(format!("help_{name}"), sub.render_long_help().to_string());
        }
    }

    // HELP_TEMPLATE hand-lists the root commands (clap can't group or space
    // them), so a new verb on `Command` would silently miss the `-h` list - the
    // help_root snapshot can't catch it, since the template text is literal and
    // doesn't change when a variant is added. This does.
    #[test]
    fn help_template_lists_every_subcommand() {
        let mut root = Cli::command();
        root.build();
        let help = root.render_long_help().to_string();
        for sub in root.get_subcommands() {
            let name = sub.get_name();
            assert!(
                help.contains(name),
                "subcommand `{name}` is missing from HELP_TEMPLATE - add a line for it",
            );
        }
    }

    #[test]
    fn copy_endpoint_at_keyword_resolves_to_the_configured_store() {
        let mut loaded = Config::default();
        loaded.storage.path = Some("s3://from-config/p".to_owned());
        // `@` follows the same ladder as every other command (flag > config >
        // default); `local` stays the config-free local default dir.
        let from_config = resolve_copy_endpoint("@", &None, &loaded).unwrap();
        assert!(
            matches!(&from_config, CopyEndpoint::Store(url) if url.lance_url().as_str() == "s3://from-config/p"),
        );
        let flag = StorageUrl::parse("s3://from-flag/p").unwrap();
        let from_flag = resolve_copy_endpoint("@", &Some(flag), &loaded).unwrap();
        assert!(
            matches!(&from_flag, CopyEndpoint::Store(url) if url.lance_url().as_str() == "s3://from-flag/p"),
        );
        assert!(matches!(
            resolve_copy_endpoint("local", &None, &loaded).unwrap(),
            CopyEndpoint::Store(url) if url.is_local()
        ));
    }

    #[test]
    fn ensure_source_not_empty_rejects_a_zero_row_source() {
        let table = |source_rows| TableVerify {
            table: substrate::Table::Sessions,
            source_rows,
            missing: 0,
        };
        let empty = StorageVerify {
            tables: vec![table(0)],
            dest_indexes: IndexCoverage {
                fts_present: false,
                vector_present_or_below_activation: true,
            },
        };
        assert!(ensure_source_not_empty(&empty, "s3://typo/bucket").is_err());
        let populated = StorageVerify {
            tables: vec![table(3)],
            dest_indexes: IndexCoverage {
                fts_present: false,
                vector_present_or_below_activation: true,
            },
        };
        assert!(ensure_source_not_empty(&populated, "local").is_ok());
    }
}
