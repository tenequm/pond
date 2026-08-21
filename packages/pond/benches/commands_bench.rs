#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]
// `libc::getrusage` is the cross-platform peak-RSS sampler; needs `unsafe`.
#![allow(unsafe_code)]

//! End-to-end timing for `pond status`, `pond sync`, and `pond copy` against a
//! real corpus. Drives the same library entry points the CLI does, so a perf
//! claim ("recurring sync should be blazingly fast") can be tested in one
//! command and compared apples-to-apples between revisions.
//!
//! What it measures, per op, per run:
//!   * wall-clock total
//!   * per-phase wall-clock (open store, oracle, per-adapter import, embed,
//!     optimize, plan, copy, verify, ...)
//!   * peak RSS (RUSAGE_SELF.ru_maxrss; bytes on macOS, KiB on Linux)
//!   * counters surfaced as phase `detail`: rows inserted/matched, plan sizes
//!
//! Across runs, prints min/median/max wall-clock per op so a "blazingly fast
//! second run" claim shows up directly. Run 0 is cold for the process; runs
//! 1+ inherit OS/file caches (and pond's internal `cached(table)` mutex map
//! is process-fresh each invocation, so the cold vs warm gap is real).
//!
//! This harness is committed because it is also the regression guard: rerun
//! it before/after any sync, copy, or optimize touch and confirm the numbers
//! did not move the wrong way (the rust-dev `references/performance.md`
//! discipline: a change is only an optimization if a bench says so).
//!
//! Usage:
//!     # all ops, 3 runs, local default-config store:
//!     cargo bench --bench commands_bench -- --runs 3
//!     # just sync, against the configured remote (uses the same creds the CLI does):
//!     cargo bench --bench commands_bench -- \
//!         --url s3+https://nbg1.your-objectstorage.com/pondarium/pond --ops sync
//!     # one adapter only, isolating its share of the import phase:
//!     cargo bench --bench commands_bench -- --ops sync --adapter claude-code
//!     # copy A -> B with two runs (run 0 fills B, run 1 should be near-instant):
//!     cargo bench --bench commands_bench -- --ops copy --from-url <src> --url <dst>
//!     # JSON output (machine-readable, suitable for trend tracking):
//!     cargo bench --bench commands_bench -- --json
//!
//! For heap profiling, build with `--features dhat-heap` (see Cargo.toml).
//! For sampling profile flamegraphs, build the bench binary directly under
//! `cargo build --profile profiling --bench commands_bench` (the
//! [profile.profiling] inheritance keeps debug = true), then point `samply`
//! at the resulting `target/profiling/deps/commands_bench-*` binary.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use pond::adapter::{self, SkipOracle};
use pond::config::Config;
use pond::handlers::{self, IngestSummary, SyncEvent, SyncStatus};
use pond::sessions::Store;
use pond::substrate::{MaintenancePolicy, Predicate, RuntimeCaps, StorageUrl};
use serde_json::{Value, json};

/// Wraps a value so the compiler cannot const-fold the bench's call site away.
/// Used for any work that is conceptually a pure read so the optimizer does
/// not delete it (rust-dev `references/performance.md`).
#[inline(always)]
fn keep<T>(value: T) -> T {
    std::hint::black_box(value)
}

#[derive(Parser)]
#[command(about = "End-to-end timing for pond status/sync/copy on a real corpus")]
struct Args {
    /// Target storage destination. Defaults to `[storage].path` from the
    /// loaded config; pass an explicit URL to point at a remote or alt store.
    #[arg(long)]
    url: Option<String>,
    /// Source store for the `copy` op. Required when --ops includes `copy`.
    #[arg(long)]
    from_url: Option<String>,
    /// Which ops to drive (comma-separated). Default: all three.
    #[arg(long, default_value = "status,sync,copy")]
    ops: String,
    /// Restrict the sync op to one configured adapter (mirrors `pond sync <ADAPTER>`).
    #[arg(long)]
    adapter: Option<String>,
    /// Runs per op. Run 0 is cold; runs 1+ inherit caches.
    #[arg(long, default_value_t = 3)]
    runs: usize,
    /// Drop the post-import optimize+embed in sync to isolate ingest timing.
    #[arg(long)]
    no_optimize: bool,
    /// Drop the post-copy `optimize_indices` to isolate copy timing.
    #[arg(long)]
    no_copy_optimize: bool,
    /// Print results as JSON (one object per run) instead of human tables.
    /// Useful for piping into a trend tracker.
    #[arg(long)]
    json: bool,
    /// `cargo bench` always passes --bench; clap would reject without this.
    #[arg(long, hide = true)]
    bench: bool,
}

fn parse_ops(input: &str) -> Result<Vec<&'static str>> {
    let mut out = Vec::with_capacity(3);
    for raw in input.split(',') {
        match raw.trim() {
            "status" => out.push("status"),
            "sync" => out.push("sync"),
            "copy" => out.push("copy"),
            "" => {}
            other => bail!("unknown op {other:?}; expected status|sync|copy"),
        }
    }
    if out.is_empty() {
        bail!("--ops resolved to empty list");
    }
    Ok(out)
}

#[derive(Debug, Clone, Default)]
struct Phase {
    label: String,
    elapsed: Duration,
    detail: String,
}

#[derive(Debug, Default)]
struct RunReport {
    op: String,
    run_index: usize,
    phases: Vec<Phase>,
    total: Duration,
    /// RSS at start, in KiB.
    rss_kb_start: i64,
    /// Peak RSS at end, in KiB.
    rss_kb_peak: i64,
}

impl RunReport {
    fn total_ms(&self) -> f64 {
        self.total.as_secs_f64() * 1000.0
    }
}

/// `libc::getrusage(RUSAGE_SELF).ru_maxrss` is bytes on macOS (Apple's
/// deviation from BSD) and KiB on Linux. Normalize to KiB so peak comparisons
/// across platforms read the same.
#[cfg(unix)]
fn peak_rss_kb() -> i64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0;
    }
    let raw: i64 = usage.ru_maxrss as i64;
    if cfg!(target_os = "macos") {
        raw / 1024
    } else {
        raw
    }
}
#[cfg(not(unix))]
fn peak_rss_kb() -> i64 {
    0
}

async fn open(url: &str, config: &Config) -> Result<Store> {
    let storage = StorageUrl::parse(url)?;
    let resolved = storage.resolve(&config.creds)?;
    let caps = RuntimeCaps::from_config(&config.runtime);
    Store::open_with_options(resolved.lance_url(), resolved.options.clone(), caps)
        .await
        .context("Store::open_with_options")
}

async fn timed<T, F>(label: impl Into<String>, phases: &mut Vec<Phase>, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let label = label.into();
    let start = Instant::now();
    let out = fut.await;
    let elapsed = start.elapsed();
    let detail = match &out {
        Ok(_) => String::new(),
        Err(err) => format!("ERR: {err}"),
    };
    phases.push(Phase {
        label,
        elapsed,
        detail,
    });
    out
}

/// Set the trailing phase's `detail` string. Called right after `timed!`
/// returns to attach observed counters (rows merged, fresh count, plan size)
/// to the phase that produced them.
fn detail(phases: &mut [Phase], text: String) {
    if let Some(p) = phases.last_mut() {
        p.detail = text;
    }
}

async fn run_status(url: &str, config: &Config, verbose: bool) -> Result<RunReport> {
    let mut phases = Vec::with_capacity(8);
    let rss_start = peak_rss_kb();
    let total_start = Instant::now();
    let store = timed("open_store", &mut phases, async { open(url, config).await }).await?;
    // The CLI runs five queries in parallel via `tokio::try_join!`. Mirror
    // that so the bench's "total" lines up with what a user actually waits
    // for; the per-call ms below are observed serial times for breakdown.
    let parallel_start = Instant::now();
    let (_sizes, row_counts, names, index_status, embedding) = if verbose {
        let r = tokio::try_join!(
            store.table_sizes(),
            store.row_counts(),
            store.adapter_names(false),
            store.index_status(),
            async { store.embedding_progress().await.map(Some) },
        )?;
        (r.0, r.1, r.2, r.3, r.4)
    } else {
        let r = tokio::try_join!(
            store.table_sizes(),
            store.row_counts(),
            store.adapter_names(false),
            store.index_status(),
        )?;
        (r.0, r.1, r.2, r.3, None)
    };
    phases.push(Phase {
        label: "parallel_probes".to_owned(),
        elapsed: parallel_start.elapsed(),
        detail: format!(
            "sessions={} messages={} parts={} adapters={} index_intents={}",
            row_counts.0,
            row_counts.1,
            row_counts.2,
            names.len(),
            index_status.len(),
        ),
    });
    if verbose {
        if let Some(p) = embedding {
            let _ = keep(p);
        }
        let searchable_start = Instant::now();
        let _ = store
            .searchable_in_scope(&Predicate::And(Vec::new()))
            .await
            .context("searchable_in_scope")?;
        phases.push(Phase {
            label: "searchable_in_scope".to_owned(),
            elapsed: searchable_start.elapsed(),
            detail: String::new(),
        });
        let stale_start = Instant::now();
        let _ = store
            .stale_embedding_count()
            .await
            .context("stale_embedding_count")?;
        phases.push(Phase {
            label: "stale_embedding_count".to_owned(),
            elapsed: stale_start.elapsed(),
            detail: String::new(),
        });
    }
    Ok(RunReport {
        op: if verbose {
            "status -v".to_owned()
        } else {
            "status".to_owned()
        },
        run_index: 0,
        phases,
        total: total_start.elapsed(),
        rss_kb_start: rss_start,
        rss_kb_peak: peak_rss_kb(),
    })
}

async fn run_sync(args: &Args, url: &str, config: &Config) -> Result<RunReport> {
    let mut phases = Vec::with_capacity(16);
    let rss_start = peak_rss_kb();
    let total_start = Instant::now();
    let store = timed("open_store", &mut phases, async { open(url, config).await }).await?;
    let cache_dir = std::env::temp_dir().join("pond-commands-bench-rowmap");
    let oracle = timed("oracle (rowmap)", &mut phases, async {
        store
            .ensure_rowmap(&cache_dir)
            .await
            .context("ensure_rowmap")?;
        Ok::<_, anyhow::Error>(pond::sessions::RowmapOracle(store.rowmap_snapshot()))
    })
    .await?;
    detail(&mut phases, format!("map_present={}", oracle.0.is_some()));

    let resolved = config
        .resolve_adapters(args.adapter.as_deref())
        .context("resolve_adapters")?;
    if resolved.is_empty() {
        bail!("no enabled adapters; run `pond adapters enable <name>`");
    }

    let mut combined = IngestSummary::default();
    for entry in resolved {
        let (name, cfg) = (entry.name, entry.config);
        let factory =
            adapter::by_name(&name).ok_or_else(|| anyhow::anyhow!("unknown adapter {name:?}"))?;
        let adapter_obj = factory.open(cfg)?;
        let mut fresh = 0usize;
        let mut ok = 0usize;
        let mut other = 0usize;
        let label = format!("import:{name}");
        let summary = timed(label, &mut phases, async {
            handlers::ingest_adapter(
                &store,
                adapter_obj.as_ref(),
                &oracle as &dyn SkipOracle,
                |ev| match ev {
                    SyncEvent::Discovered { .. } => {}
                    SyncEvent::SessionDone(o) => match o.status {
                        SyncStatus::Fresh => fresh += 1,
                        SyncStatus::Ok => ok += 1,
                        _ => other += 1,
                    },
                    SyncEvent::SkippedBulk { status, count } => match status {
                        SyncStatus::Fresh => fresh += count,
                        _ => other += count,
                    },
                    SyncEvent::Flushing { .. } => {}
                },
            )
            .await
            .context("ingest_adapter")
        })
        .await?;
        detail(
            &mut phases,
            format!(
                "fresh={fresh} ok={ok} other={other} inserted={} matched={}",
                summary.inserted, summary.matched
            ),
        );
        combined.merge(&summary);
        // black_box to keep summary observable even when no later code reads it.
        let _ = keep(&combined);
    }

    let any_new_rows = combined.inserted > 0;
    if !args.no_optimize && any_new_rows {
        if config.embeddings.enabled {
            timed("embed", &mut phases, async {
                let progress = store.embedding_progress().await?;
                let backlog = progress.total.saturating_sub(progress.embedded);
                if backlog == 0 {
                    return Ok::<usize, anyhow::Error>(0);
                }
                let embedder =
                    pond::embed::CandleEmbedder::load().context("CandleEmbedder::load")?;
                let worker = pond::embed::EmbedWorker::new(&store, &embedder);
                let s = worker.run().await.context("EmbedWorker::run")?;
                Ok(s.messages)
            })
            .await?;
        } else {
            phases.push(Phase {
                label: "embed".to_owned(),
                elapsed: Duration::ZERO,
                detail: "skipped ([embeddings].enabled = false)".to_owned(),
            });
        }
        let policy = MaintenancePolicy {
            compaction_fragment_cap: 0,
            cleanup_older_than: chrono::Duration::days(1),
            cleanup_interval: 1,
            scalar_fold_row_threshold: 0,
            index_fold_row_threshold: 0,
        };
        timed("optimize (indices+cleanup+compact)", &mut phases, async {
            store
                .optimize_indices(None, &policy)
                .await
                .context("optimize_indices")?;
            Ok(())
        })
        .await?;
    } else {
        phases.push(Phase {
            label: "optimize".to_owned(),
            elapsed: Duration::ZERO,
            detail: format!(
                "skipped (any_new_rows={any_new_rows} no_optimize={})",
                args.no_optimize
            ),
        });
    }

    Ok(RunReport {
        op: "sync".to_owned(),
        run_index: 0,
        phases,
        total: total_start.elapsed(),
        rss_kb_start: rss_start,
        rss_kb_peak: peak_rss_kb(),
    })
}

async fn run_copy(args: &Args, url: &str, config: &Config) -> Result<RunReport> {
    let from = args
        .from_url
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--ops copy requires --from-url"))?;
    let mut phases = Vec::with_capacity(8);
    let rss_start = peak_rss_kb();
    let total_start = Instant::now();
    let source = timed("open_source", &mut phases, async {
        open(from, config).await
    })
    .await?;
    let dest = timed("open_dest", &mut phases, async { open(url, config).await }).await?;
    let plan = timed("plan_incremental_from", &mut phases, async {
        dest.plan_incremental_from(&source)
            .await
            .context("plan_incremental_from")
    })
    .await?;
    detail(
        &mut phases,
        format!(
            "sessions={} msg_append={} msg_merge={} parts_append={} parts_merge={}",
            plan.source_sessions,
            plan.messages.append.len(),
            plan.messages.merge.len(),
            plan.parts.append.len(),
            plan.parts.merge.len(),
        ),
    );
    let plan = keep(plan);
    timed("copy_delta_from", &mut phases, async {
        dest.copy_delta_from(&source, &plan)
            .await
            .context("copy_delta_from")
    })
    .await?;
    if !args.no_copy_optimize {
        timed("optimize_dest", &mut phases, async {
            let policy = MaintenancePolicy::always_compact();
            dest.optimize_indices(None, &policy)
                .await
                .context("optimize_indices on dest")?;
            Ok(())
        })
        .await?;
    }
    Ok(RunReport {
        op: "copy".to_owned(),
        run_index: 0,
        phases,
        total: total_start.elapsed(),
        rss_kb_start: rss_start,
        rss_kb_peak: peak_rss_kb(),
    })
}

fn print_human(report: &RunReport) {
    let total_ms = report.total_ms();
    println!(
        "\n[{} run #{}] total {:>9.1} ms  peak_rss {:>6.1} MiB  start_rss {:>6.1} MiB",
        report.op,
        report.run_index,
        total_ms,
        report.rss_kb_peak as f64 / 1024.0,
        report.rss_kb_start as f64 / 1024.0,
    );
    for phase in &report.phases {
        let ms = phase.elapsed.as_secs_f64() * 1000.0;
        let pct = if total_ms > 0.0 {
            ms / total_ms * 100.0
        } else {
            0.0
        };
        if phase.detail.is_empty() {
            println!("  {:<38} {:>9.1} ms  {:>5.1}%", phase.label, ms, pct);
        } else {
            println!(
                "  {:<38} {:>9.1} ms  {:>5.1}%  {}",
                phase.label, ms, pct, phase.detail
            );
        }
    }
}

fn json_report(report: &RunReport) -> Value {
    let phases: Vec<Value> = report
        .phases
        .iter()
        .map(|p| {
            json!({
                "label": p.label,
                "ms": p.elapsed.as_secs_f64() * 1000.0,
                "detail": p.detail,
            })
        })
        .collect();
    json!({
        "op": report.op,
        "run_index": report.run_index,
        "total_ms": report.total_ms(),
        "peak_rss_kib": report.rss_kb_peak,
        "start_rss_kib": report.rss_kb_start,
        "phases": phases,
    })
}

/// Print a min/median/max summary across the runs of one op.
fn print_summary(op: &str, samples_ms: &[f64]) {
    if samples_ms.is_empty() {
        return;
    }
    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let min = sorted[0];
    let max = *sorted.last().unwrap();
    let median = sorted[sorted.len() / 2];
    println!(
        "\n[{} summary] runs={} min={:.1}ms median={:.1}ms max={:.1}ms",
        op,
        sorted.len(),
        min,
        median,
        max,
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = pond::config::default_config_path(
        std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
        std::env::var_os("HOME").map(PathBuf::from),
    );
    let config = Config::load(&config_path)?;
    let url = match args.url.clone().or_else(|| config.storage.path.clone()) {
        Some(url) => url,
        None => bail!(
            "pass --url or set [storage].path in {}",
            config_path.display()
        ),
    };

    if !args.json {
        println!("commands_bench: target={url}");
        if let Some(from) = &args.from_url {
            println!("                source={from}");
        }
        println!("                ops={} runs={}", args.ops, args.runs);
    }
    let ops = parse_ops(&args.ops)?;
    let runs = args.runs.max(1);
    let mut all_json = Vec::new();

    for op in ops {
        let mut samples_ms = Vec::with_capacity(runs);
        for run_index in 0..runs {
            let mut report = match op {
                "status" => run_status(&url, &config, false).await?,
                "sync" => run_sync(&args, &url, &config).await?,
                "copy" => run_copy(&args, &url, &config).await?,
                _ => unreachable!(),
            };
            report.run_index = run_index;
            samples_ms.push(report.total_ms());
            if args.json {
                all_json.push(json_report(&report));
            } else {
                print_human(&report);
            }
        }
        if !args.json {
            print_summary(op, &samples_ms);
        }
        if op == "status" {
            // status -v is materially different on remote (scans messages for
            // the searchable count); time it once for visibility.
            let mut verbose = run_status(&url, &config, true).await?;
            verbose.run_index = 0;
            if args.json {
                all_json.push(json_report(&verbose));
            } else {
                print_human(&verbose);
            }
        }
    }

    if args.json {
        let doc = json!({ "results": all_json });
        println!("{}", serde_json::to_string_pretty(&doc)?);
    } else {
        println!("\ndone");
    }
    Ok(())
}
