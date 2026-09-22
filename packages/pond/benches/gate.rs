#![allow(clippy::print_stdout, clippy::unwrap_used, clippy::expect_used)]

//! The release gate, one target for both halves of it:
//!
//!   perf  - what pond delivers against the configured remote store: CLI
//!           probes, map-vs-scan output equivalence (hard pass/fail), the
//!           read-serving benches with per-query S3 iops, the ops phase
//!           timings, and the write suite on ephemeral scratch stores.
//!   mem   - one `mem_bench` scenario per process (peak RSS is a
//!           process-lifetime high-water mark, so two scenarios in one process
//!           cannot be told apart), each producing one row.
//!
//! Both append to `docs/benchmarks/baseline.jsonl`, one JSON line per
//! measurement unit, keyed by (`scenario`, `profile`, `host`) - the perf half
//! writes a single `"scenario":"perf-gate"` row per run, the mem half one row
//! per scenario. After a run the delta printer compares the last two rows of
//! every group it touched.
//!
//!   cargo bench --bench gate                        # perf + mem (ci)
//!   cargo bench --bench gate -- --only mem --profile large
//!   cargo bench --bench gate -- --only perf
//!   cargo bench --bench gate -- --check             # gate vs committed rows
//!   cargo bench --bench gate -- --only mem --runs 5
//!
//! Each mem scenario runs `--runs` times (3 on `ci`, 1 on `large`) and the run
//! whose `peak_heap_bytes` is the median is the one recorded - whole and
//! unchanged, because an averaged row is one no process ever produced. The
//! spread across the runs is printed so a noisy scenario is visible at a glance.
//! Recording also refuses to start while other builds or benches are running
//! (`--allow-contended` overrides): a loaded host starves scan readahead and
//! reads peak heap LOW, which plants a false regression for the next run to
//! trip over - exactly how the first lance-12 rows came out.
//!
//! `--check` runs the mem scenarios, appends nothing, and fails when
//! `peak_rss_kb` or `peak_heap_bytes` regressed more than
//! MEM_GATE_MAX_REGRESSION_PCT (default 20) against the last committed row for
//! the same (scenario, profile, host), or when `scan_fallbacks` rose at all.
//! The perf numbers are record-only and are never gated - a remote store's
//! run-to-run spread buries the effects they exist to catch - so `--check`
//! skips the perf half entirely.
//!
//! Gate targets (run here, every release):
//!   serve_mem_bench  - read-serving components + per-query S3 iops (io-trace)
//!   ops_bench        - read-only phase timing of status/sync/optimize/copy
//!   write_bench      - copy suite, commit sweep, index build/fold
//!   CLI probes       - get-session / get-message / search / sql wall-clock
//! Research probes (NOT run here; run when touching their area):
//!   read_bench (fold-batching threshold), sync_oracle_bench (oracle choice),
//!   tokenizer_quality_bench, fmindex_probe (#47), multiwriter_bench (OCC),
//!   backend_bench, plus the ingest/embed benches for write-path work.
//!
//! Env: STORE_URL (default `[storage].path` from POND_CONFIG_FILE), POND_BIN
//! (measure a prebuilt binary through the CLI probes; the cargo benches compile
//! HEAD, so they are skipped and their fields land null), PROBE_SID, PROBE_MID,
//! DATED_DAYS, STORE_LABEL, SCENARIOS, RECORD_ONLY, MEM_GATE_HOST,
//! MEM_GATE_MAX_REGRESSION_PCT.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Local, Utc};
use clap::Parser;
use futures::StreamExt;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreRegistry, StorageOptionsAccessor,
};
use object_store::ObjectStore as _;
use object_store::path::Path as ObjPath;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use pond::{config::Config, substrate::StorageUrl};

/// Fixed synthetic write corpus so write rows stay comparable across time; the
/// row's `write_corpus` tag derives from these.
const WRITE_SESSIONS: usize = 500;
const WRITE_MESSAGES: usize = 5;
const WRITE_SWEEP_BATCH: usize = 512;
/// Leaf of the scratch base write_bench derives its `<base>-*` stores from; the
/// cleanup refuses any other leaf so it can never sweep a real store's siblings.
const SCRATCH_LEAF: &str = "benchw";

const DEFAULT_SCENARIOS: &str = "sync-noop-local sync-incremental rowmap-build-cold \
    mcp-query-growth ingest-large-session search-query-latency ingest-throughput \
    serve-sync-retention sync-under-contention rowmap-build-cold-partial-embed";

/// Scenarios that RUN and get a row, but whose memory numbers gate nothing yet.
/// Over ten interleaved ci runs the two small-allocation scenarios swing several
/// times wider than the 20% threshold (peak heap: 139% on sync-under-contention,
/// 19% on ingest-throughput), so a threshold there would fire on noise and teach
/// people to ignore the gate. The other three are tighter than that already
/// (0.1-7%) and wait only for enough committed rows to say what "normal" is.
/// Phase 2 promotes a scenario by deleting it from this list; `RECORD_ONLY=` in
/// the environment (empty, not unset) rehearses that by gating every scenario.
const DEFAULT_RECORD_ONLY: &str = "search-query-latency ingest-throughput \
    serve-sync-retention sync-under-contention rowmap-build-cold-partial-embed";

/// Gated on the non-record-only scenarios, as a percentage.
const GATED_METRICS: [&str; 2] = ["peak_rss_kb", "peak_heap_bytes"];
/// Judged on every scenario, record-only included, and as an absolute count
/// rather than a percentage: a cold rowmap build that stops streaming is a
/// cliff (it re-encodes the whole corpus through the sorting build), and the
/// scenarios that report it exist to catch exactly that.
const COUNTERS: [&str; 1] = ["scan_fallbacks"];

/// Row fields that identify a row rather than measure anything: the delta
/// printer never diffs them. The union of both former gates' tag lists.
const TAGS: [&str; 14] = [
    "date",
    "commit",
    "bin",
    "store",
    "host",
    "toolchain",
    "scenario",
    "profile",
    "equivalence",
    "search_mode",
    "write_backend",
    "write_corpus",
    "detail",
    "hwm_reset",
];

#[derive(Parser)]
#[command(about = "pond release gate: perf probes + memory scenarios, one baseline")]
struct Args {
    /// Run only one half of the gate: `perf` or `mem`. Default: both.
    #[arg(long, value_parser = ["perf", "mem"])]
    only: Option<String>,
    /// Corpus size class for the mem scenarios: `ci` (~100k messages) or `large` (1M+).
    #[arg(long, default_value = "ci", value_parser = ["ci", "large"])]
    profile: String,
    /// Judge the mem scenarios against the committed baseline instead of
    /// appending to it. Appends nothing and skips the perf half.
    #[arg(long)]
    check: bool,
    /// Baseline file. Defaults to `docs/benchmarks/baseline.jsonl` under the
    /// repo root - point it at a scratch copy to rehearse a record.
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Runs per mem scenario; the median run by `peak_heap_bytes` is the one
    /// recorded. Defaults to 3 on the `ci` profile and 1 on `large`, whose
    /// scenarios are minutes each.
    #[arg(long)]
    runs: Option<usize>,
    /// Record even though other builds or benches are running. They bias peak
    /// heap low, so the row is only comparable to other contended rows.
    #[arg(long)]
    allow_contended: bool,
    /// Ignored. `cargo bench` passes `--bench` to every `harness = false`
    /// target; without this flag clap would reject it as unknown.
    #[arg(long, hide = true)]
    bench: bool,
}

/// The (scenario, profile, host) triple a row is compared within. Rows missing
/// a field group under the empty string: peak RSS is a property of the machine
/// as much as the code, so a row from another box is not a threshold this one
/// can be held to.
type GroupKey = (String, String, String);

fn group_key(row: &Map<String, Value>) -> GroupKey {
    let field = |key: &str| {
        row.get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    (field("scenario"), field("profile"), field("host"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();
    let root = repo_root()?;
    // Resolved before the chdir below, so a relative `--baseline` means the
    // caller's cwd and not the repo root.
    let baseline = match &args.baseline {
        Some(path) => std::path::absolute(path)
            .with_context(|| format!("cannot resolve {}", path.display()))?,
        None => root.join("docs/benchmarks/baseline.jsonl"),
    };
    std::env::set_current_dir(&root)
        .with_context(|| format!("cannot enter repo root {}", root.display()))?;

    let only = args.only.as_deref();
    let run_mem = only != Some("perf");
    let run_perf = only != Some("mem") && !args.check;
    if args.check && only == Some("perf") {
        println!("perf metrics are record-only and are never gated; nothing to check");
        return Ok(());
    }

    // Both refusals come before the perf half: it is ~30 minutes of S3 work
    // and appends a row, which a later refusal would leave behind.
    let runs = if run_mem {
        Some(mem_runs(&args)?)
    } else {
        None
    };
    if run_mem {
        guard_against_contention(args.check, args.allow_contended)?;
    }

    let host = host_tag();
    let toolchain = toolchain();
    let date = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let commit = commit_stamp(&baseline);
    let mut touched: Vec<GroupKey> = Vec::new();

    if run_perf {
        let row = perf_gate(&root, &date, &commit, &host, &toolchain).await?;
        append_line(&baseline, &encode_row(&row))?;
        touched.push(group_key(&row.into_iter().collect()));
    }
    if let Some(runs) = runs {
        let rows = mem_gate(&args, runs, &baseline, &date, &commit, &host, &toolchain)?;
        touched.extend(rows);
    }
    if !args.check && !touched.is_empty() {
        println!("\n--- delta vs previous run (same scenario, profile and host) ---");
        print_deltas(&baseline, &touched)?;
        println!(
            "\nrows appended to {} (no thresholds in append mode - read the deltas)",
            baseline.display()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------- environment

fn repo_root() -> Result<PathBuf> {
    let top_level = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|text| !text.is_empty());
    if let Some(top_level) = top_level {
        return Ok(PathBuf::from(top_level));
    }
    // packages/pond -> repo root, for a checkout git cannot read.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .context("cannot locate the repo root")
}

/// `<short sha>` or `<short sha>-dirty`. A dirty tree means the measured binary
/// may not match the named commit; the baseline this run appends to never
/// affects the binary, so it is excluded from the check.
fn commit_stamp(baseline: &Path) -> String {
    let short = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    // A baseline outside the tree makes git reject the exclude pathspec;
    // `literal` stops glob characters in the path from excluding real changes.
    let relative = baseline
        .strip_prefix(std::env::current_dir().unwrap_or_default())
        .ok()
        .map(|path| path.display().to_string());
    let mut cmd = Command::new("git");
    cmd.args(["status", "--porcelain"]);
    if let Some(relative) = &relative {
        cmd.arg("--").arg(format!(":(exclude,literal){relative}"));
    }
    let dirty = cmd
        .output()
        .ok()
        .filter(|out| out.status.success())
        .is_some_and(|out| !String::from_utf8_lossy(&out.stdout).trim().is_empty());
    if dirty {
        format!("{short}-dirty")
    } else {
        short
    }
}

fn host_tag() -> String {
    if let Some(tag) = std::env::var("MEM_GATE_HOST")
        .ok()
        .filter(|t| !t.is_empty())
    {
        return tag;
    }
    let part = |flag: &str| {
        Command::new("uname")
            .arg(flag)
            .output()
            .ok()
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
    };
    match (part("-s"), part("-m")) {
        (Some(sys), Some(machine)) if !sys.is_empty() && !machine.is_empty() => {
            format!("{sys}-{machine}")
        }
        _ => format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
    }
}

/// The compiler the row was measured under, so a codegen-driven shift is
/// visible in the row itself rather than inferred from the commit.
fn toolchain() -> String {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_owned());
    Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn cargo() -> Command {
    Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned()))
}

// ------------------------------------------------------------- process helper

fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd
        .status()
        .with_context(|| format!("failed to spawn {cmd:?}"))?;
    if !status.success() {
        bail!("{cmd:?} exited with {status}");
    }
    Ok(())
}

fn capture(cmd: &mut Command) -> Result<String> {
    let out = cmd
        .output()
        .with_context(|| format!("failed to spawn {cmd:?}"))?;
    if !out.status.success() {
        bail!(
            "{cmd:?} exited with {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Run a bench and both show and keep its stdout: these phases take minutes,
/// and a buffered run would print nothing until they end.
fn tee(cmd: &mut Command) -> Result<String> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("failed to spawn {cmd:?}"))?;
    let mut text = String::new();
    if let Some(stdout) = child.stdout.take() {
        for line in BufReader::new(stdout).lines() {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    // Never leave a bench running against the store or the
                    // scratch prefixes after the gate has given up on it.
                    child.kill().ok();
                    child.wait().ok();
                    return Err(error).context("reading bench output");
                }
            };
            println!("{line}");
            text.push_str(&line);
            text.push('\n');
        }
    }
    let status = child.wait().context("waiting for bench")?;
    if !status.success() {
        bail!("{cmd:?} exited with {status}");
    }
    Ok(text)
}

// -------------------------------------------------------------------- baseline

/// Serialize a row with its tags first, so it reads left-to-right as
/// "when/what/where" then numbers. `serde_json`'s map is sorted, so the order
/// cannot come from the Value itself.
fn encode_row(pairs: &[(String, Value)]) -> String {
    let body = pairs
        .iter()
        .map(|(key, value)| format!("{}:{value}", Value::String(key.clone())))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

fn append_line(baseline: &Path, line: &str) -> Result<()> {
    if let Some(parent) = baseline.parent() {
        fs::create_dir_all(parent).ok();
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(baseline)
        .with_context(|| format!("cannot open {}", baseline.display()))?;
    writeln!(file, "{line}").with_context(|| format!("cannot append to {}", baseline.display()))
}

fn read_rows(baseline: &Path) -> Result<Vec<Map<String, Value>>> {
    let text = match fs::read_to_string(baseline) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", baseline.display())),
    };
    let mut rows = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("invalid JSON row in {}", baseline.display()))?;
        match value {
            Value::Object(map) => rows.push(map),
            other => bail!("baseline row is not an object: {other}"),
        }
    }
    Ok(rows)
}

// ----------------------------------------------------------------- delta print

fn print_deltas(baseline: &Path, touched: &[GroupKey]) -> Result<()> {
    let rows = read_rows(baseline)?;
    let mut seen: BTreeSet<&GroupKey> = BTreeSet::new();
    for key in touched {
        if !seen.insert(key) {
            continue;
        }
        let same: Vec<&Map<String, Value>> =
            rows.iter().filter(|row| group_key(row) == *key).collect();
        let (scenario, profile, host) = key;
        let profile = if profile.is_empty() { "-" } else { profile };
        let host = if host.is_empty() { "-" } else { host };
        println!("\n[{scenario}] profile={profile} host={host}");
        if same.len() < 2 {
            println!("  first row for this group; nothing to diff");
            continue;
        }
        print_row_delta(same[same.len() - 2], same[same.len() - 1]);
    }
    Ok(())
}

fn print_row_delta(prev: &Map<String, Value>, cur: &Map<String, Value>) {
    let label = |row: &Map<String, Value>| {
        let text = |key: &str| row.get(key).and_then(Value::as_str).unwrap_or("-");
        format!("{} {}", text("date"), text("commit"))
    };
    // Rows from different stores must not be read as a regression; the row
    // carries a digest of the store URL, never the URL itself.
    if prev.get("store") != cur.get("store") {
        println!(
            "  WARNING: different stores ({} -> {}) - the deltas below are cross-store, not a regression signal",
            prev.get("store").unwrap_or(&Value::Null),
            cur.get("store").unwrap_or(&Value::Null),
        );
    }
    // The reason a delta can be real without any source change; absent on rows
    // older than the field, which read as "unrecorded" rather than breaking.
    if prev.get("toolchain") != cur.get("toolchain") {
        let text = |row: &Map<String, Value>| {
            row.get("toolchain")
                .and_then(Value::as_str)
                .unwrap_or("unrecorded")
                .to_owned()
        };
        println!("  toolchain changed: {} -> {}", text(prev), text(cur));
    }
    println!(
        "  {:<26}{:>14}{:>14}{:>9}   ({} -> {})",
        "metric",
        "prev",
        "now",
        "delta",
        label(prev),
        label(cur),
    );
    for (key, value) in cur {
        if TAGS.contains(&key.as_str()) || !(value.is_number() || value.is_null()) {
            continue;
        }
        let before = prev.get(key);
        // A field neither row measured says nothing; the mem scenarios leave most
        // of the promoted fields null.
        if value.is_null() && before.is_none_or(Value::is_null) {
            continue;
        }
        let delta = match (before.and_then(Value::as_f64), value.as_f64()) {
            (Some(before), Some(now)) if before != 0.0 => {
                format!("{:+.0}%", (now - before) / before * 100.0)
            }
            _ => "n/a".to_owned(),
        };
        let cell = |value: Option<&Value>| match value {
            Some(Value::Null) | None => "-".to_owned(),
            Some(value) => value.to_string(),
        };
        println!(
            "  {key:<26}{:>14}{:>14}{delta:>9}",
            cell(before),
            cell(Some(value)),
        );
    }
    let wrote = cur.iter().any(|(key, value)| {
        key.starts_with("write_") && !TAGS.contains(&key.as_str()) && !value.is_null()
    });
    if cur.get("scenario").and_then(Value::as_str) == Some("perf-gate") && !wrote {
        println!(
            "  NOTE: no write_* metrics in this row - storage-path changes need a write-side A/B (AGENTS.md#benchmarking-storage-path-changes)"
        );
    }
}

// ------------------------------------------------------------------ perf gate

/// `[storage].path` out of the operator's config, without reading anything else
/// from it (the file also holds credentials).
fn storage_path_from_config(path: &Path) -> Result<String> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("cannot read {} for [storage].path", path.display()))?;
    let mut in_storage = false;
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_storage = line == "[storage]";
            continue;
        }
        if !in_storage || !line.starts_with("path") {
            continue;
        }
        if let Some(value) = line.split('"').nth(1) {
            return Ok(value.to_owned());
        }
    }
    bail!(
        "no [storage].path in {} - set STORE_URL or POND_CONFIG_FILE",
        path.display()
    )
}

/// `awk '$1 == key {print $2}'` over a bench's table output. `null` when the
/// bench skipped that phase, so a missing row can never emit invalid JSON.
fn column(text: &str, key: &str) -> Value {
    number(
        text.lines()
            .find(|line| line.split_whitespace().next() == Some(key))
            .and_then(|line| line.split_whitespace().nth(1)),
    )
}

/// The `<label> <ms> ms` lines ops_bench prints, truncated to whole ms.
fn millis(text: &str, label: &str) -> Value {
    text.lines()
        .find(|line| line.contains(label))
        .and_then(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            fields.get(fields.len().checked_sub(2)?).copied()
        })
        .and_then(|token| token.parse::<f64>().ok())
        .map_or(Value::Null, |ms| Value::from(ms.trunc() as i64))
}

/// write_bench's `[<tag>] <phase> : <ms> ms  (...)` copy lines.
fn copy_ms(text: &str, tag: &str) -> Value {
    let needle = format!("[{tag}]");
    number(
        text.lines()
            .find(|line| line.contains(&needle))
            .and_then(|line| line.split(':').nth(1))
            .and_then(|rest| rest.split_whitespace().next()),
    )
}

/// The sweep table row for the fixed batch size; `field` is 1-based.
fn sweep(text: &str, field: usize) -> Value {
    let batch = WRITE_SWEEP_BATCH.to_string();
    let token = text
        .lines()
        .find(|line| line.split_whitespace().next() == Some(batch.as_str()))
        .and_then(|line| line.split_whitespace().nth(field - 1))
        .map(|token| token.replace('(', ""));
    number(token.as_deref())
}

fn number(token: Option<&str>) -> Value {
    token
        .and_then(|token| serde_json::from_str::<Value>(token).ok())
        .filter(Value::is_number)
        .unwrap_or(Value::Null)
}

/// One timed CLI probe, `runs` times, keeping the best. Output goes to `out` so
/// the map-vs-scan equivalence check can diff it byte for byte.
fn probe(
    pond: &Path,
    store_url: &str,
    name: &str,
    argv: &[&str],
    out: &Path,
    runs: u32,
    cache_home: Option<&Path>,
) -> Result<f64> {
    let mut best = f64::MAX;
    for run in 1..=runs {
        let file = File::create(out).with_context(|| format!("cannot write {}", out.display()))?;
        let mut cmd = Command::new(pond);
        cmd.arg("--storage-path")
            .arg(store_url)
            .args(argv)
            .env("POND_EMBEDDINGS_ENABLED", "true")
            .stdout(Stdio::from(file))
            .stderr(Stdio::null());
        if let Some(cache_home) = cache_home {
            cmd.env("XDG_CACHE_HOME", cache_home);
        }
        let start = Instant::now();
        let status = cmd
            .status()
            .with_context(|| format!("failed to run probe {name}"))?;
        let seconds = (start.elapsed().as_secs_f64() * 10.0).round() / 10.0;
        if !status.success() {
            bail!("probe {name} exited with {status}");
        }
        if runs > 1 {
            println!("{name:<28} run{run}  {seconds:.1}s");
        } else {
            println!("{name:<28}       {seconds:.1}s");
        }
        best = best.min(seconds);
    }
    Ok(best)
}

async fn perf_gate(
    root: &Path,
    date: &str,
    commit: &str,
    host: &str,
    toolchain: &str,
) -> Result<Vec<(String, Value)>> {
    let config_path = match std::env::var_os("POND_CONFIG_FILE") {
        Some(path) => PathBuf::from(path),
        None => pond::config::default_config_path(
            std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from),
            std::env::var_os("HOME").map(PathBuf::from),
        ),
    };
    let store_url = match std::env::var("STORE_URL") {
        Ok(url) if !url.is_empty() => url,
        _ => storage_path_from_config(&config_path)?,
    };
    // A trailing slash would nest the benchw scratch prefix inside the store.
    let store_url = store_url.trim().trim_end_matches('/').to_owned();
    println!("=== bench gate: {store_url} ===");

    // POND_BIN: measure a prebuilt binary (e.g. the released pond) instead of
    // HEAD. Only the CLI probes run through it - the cargo benches compile HEAD
    // source, so they are skipped and their fields land null. A POND_BIN row is
    // a probe-level snapshot of the named binary, not a full row.
    let pond_bin = std::env::var("POND_BIN").ok().filter(|p| !p.is_empty());
    let pond = match &pond_bin {
        Some(path) => PathBuf::from(path),
        None => {
            run(cargo().args(["build", "--release"]))?;
            root.join("target/release/pond")
        }
    };
    // Stripped of JSON-breaking chars - the row embeds it as a string.
    let bin_version = capture(Command::new(&pond).arg("--version"))?
        .lines()
        .next()
        .unwrap_or_default()
        .replace(['"', '\\'], "");
    println!("binary: {bin_version}");

    let tmp = tempfile::tempdir().context("scratch dir for probe output")?;
    let probe_sid = env_or("PROBE_SID", "8b7b9e47-66d2-464b-8ec6-0ad70855ff57");
    let probe_mid = env_or("PROBE_MID", "419caaa5-13d7-448a-807c-5fb5105112a7");
    // Date-scoped search is the worst-measured real query shape (28-31% success
    // vs 47-48% unfiltered in the 63-day trace behind
    // docs/researches/2608-21-semantic-vs-fts-usage-eval); the timestamp zonemap
    // exists for it. Same query as `search` so the pair isolates the date filter.
    let dated_days: i64 = std::env::var("DATED_DAYS")
        .ok()
        .and_then(|days| days.parse().ok())
        .unwrap_or(7);
    let dated_from = (Local::now().date_naive() - chrono::Duration::days(dated_days)).to_string();
    let search_args = [
        "search",
        "--mode",
        "vector",
        "read performance optimization lance",
        "--limit",
        "10",
    ];
    let mut dated_args = search_args.to_vec();
    dated_args.extend(["--from-date", dated_from.as_str()]);

    println!("--- CLI probes (2 runs each, best kept) ---");
    let map_sid = tmp.path().join("map-sid.txt");
    let map_mid = tmp.path().join("map-mid.txt");
    let map_msg = tmp.path().join("map-msg.txt");
    let sid_s = probe(
        &pond,
        &store_url,
        "get_session_sid",
        &["get-session", probe_sid.as_str()],
        &map_sid,
        2,
        None,
    )?;
    let mid_s = probe(
        &pond,
        &store_url,
        "get_session_mid",
        &["get-session", probe_mid.as_str()],
        &map_mid,
        2,
        None,
    )?;
    let msg_s = probe(
        &pond,
        &store_url,
        "get_message",
        &["get-message", probe_mid.as_str()],
        &map_msg,
        2,
        None,
    )?;
    let search_s = probe(
        &pond,
        &store_url,
        "search",
        &search_args,
        &tmp.path().join("search.txt"),
        2,
        None,
    )?;
    let dated_s = probe(
        &pond,
        &store_url,
        "search_dated",
        &dated_args,
        &tmp.path().join("search-dated.txt"),
        2,
        None,
    )?;
    let sql_s = probe(
        &pond,
        &store_url,
        "sql_count",
        &["sql", "SELECT count(*) FROM messages"],
        &tmp.path().join("sql.txt"),
        1,
        None,
    )?;

    println!("--- map-vs-scan equivalence (empty cache forces the scan path) ---");
    let empty_cache = tmp.path().join("empty-cache");
    fs::create_dir_all(&empty_cache).context("empty cache dir")?;
    for (name, argv, mapped) in [
        ("sid", vec!["get-session", probe_sid.as_str()], &map_sid),
        ("mid", vec!["get-session", probe_mid.as_str()], &map_mid),
        ("msg", vec!["get-message", probe_mid.as_str()], &map_msg),
    ] {
        let scanned = tmp.path().join(format!("scan-{name}.txt"));
        probe(
            &pond,
            &store_url,
            &format!("scan_{name}"),
            &argv,
            &scanned,
            1,
            Some(&empty_cache),
        )?;
        if fs::read(mapped)? != fs::read(&scanned)? {
            bail!("EQUIVALENCE FAILED: {name} - map-served output differs from scan-served");
        }
    }
    println!("EQUIVALENCE OK: map-served output identical to scan-served");

    let mut serve = String::new();
    let mut ops = String::new();
    let mut copy = String::new();
    let mut sweep_out = String::new();
    let mut prof = String::new();
    let mut write_backend = Value::Null;
    let mut write_corpus = Value::Null;
    if pond_bin.is_none() {
        println!("--- serve_mem_bench (io-trace) ---");
        serve = tee(cargo()
            .args([
                "bench",
                "--bench",
                "serve_mem_bench",
                "--features",
                "io-trace",
                "--",
                "--storage-path",
                store_url.as_str(),
                "--io-trace",
            ])
            .env("POND_EMBEDDINGS_ENABLED", "true"))?;

        println!("--- ops_bench ---");
        // --url is load-bearing: ops_bench resolves the operator XDG config
        // directly (it ignores POND_CONFIG_FILE), so without it the ops phases
        // silently measure whatever store the operator's config names.
        ops = tee(cargo()
            .args([
                "bench",
                "--bench",
                "ops_bench",
                "--",
                "--url",
                store_url.as_str(),
            ])
            .env("POND_EMBEDDINGS_ENABLED", "true"))?;

        println!("--- write benches (scratch stores, the real store is never written) ---");
        let sessions = WRITE_SESSIONS.to_string();
        let messages = WRITE_MESSAGES.to_string();
        let mut write_args = vec![
            "--sessions".to_owned(),
            sessions,
            "--messages".to_owned(),
            messages,
        ];
        write_corpus = Value::String(format!("synthetic-{WRITE_SESSIONS}x{WRITE_MESSAGES}"));
        // Sibling prefix beside the gate store (same bucket/creds); the bench's
        // scratch stores are fixed-named benchw-* children, so stale ones from
        // an aborted run are swept up front - they would fail the copy
        // verification. Plain s3:// is excluded: its URL carries no endpoint
        // host, so the cleanup cannot address the same backend.
        let mut scratch_base: Option<String> = None;
        if store_url.starts_with("s3+http") {
            let parent = store_url
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_owned())
                .unwrap_or_default();
            let base = format!("{parent}/{SCRATCH_LEAF}");
            write_args.push("--dest-url".to_owned());
            write_args.push(base.clone());
            write_backend = Value::String("s3".to_owned());
            let leaf = store_url.rsplit('/').next().unwrap_or_default();
            if leaf.starts_with(SCRATCH_LEAF) {
                println!(
                    "WARNING: store prefix {leaf:?} would match the scratch prefix - clean s3 scratch under {base}-* manually"
                );
            } else {
                scratch_base = Some(base);
            }
        } else {
            write_backend = Value::String("local".to_owned());
        }
        // Only an s3 sweep needs creds, and like the shell's creds scrape a
        // config that will not load costs the sweep, not the gate.
        let config = match &scratch_base {
            Some(base) => match Config::load(&config_path) {
                Ok(config) => Some(config),
                Err(error) => {
                    println!(
                        "WARNING: config unavailable for scratch cleanup - clean {base}-* manually: {error:#}"
                    );
                    None
                }
            },
            None => None,
        };
        let sweep = scratch_base.as_deref().zip(config.as_ref());
        if let Some((base, config)) = sweep {
            scratch_clean(base, config).await;
        }
        let argv: Vec<&str> = write_args.iter().map(String::as_str).collect();
        let profile_dir = tmp.path().join("wprof");
        let outcome = write_passes(&argv, &profile_dir);
        // Best effort, like the shell trap it replaces: a failed pass must not
        // leave the scratch prefixes behind for the next run to trip over.
        if let Some((base, config)) = sweep {
            scratch_clean(base, config).await;
        }
        let passes = outcome?;
        copy = passes.0;
        sweep_out = passes.1;
        prof = passes.2;
    }

    // The repo is public, so the row carries a digest (or an operator-set
    // STORE_LABEL), never the store URL itself.
    let store_label = match std::env::var("STORE_LABEL") {
        Ok(label) if !label.is_empty() => label,
        _ => Sha256::digest(store_url.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            .chars()
            .take(12)
            .collect(),
    };

    // Built as an ordered list, not a map: `serde_json`'s map is sorted, and the
    // row is meant to read left-to-right as "when/what/where" then numbers.
    let mut row: Vec<(String, Value)> = Vec::new();
    let mut set = |key: &str, value: Value| {
        row.push((key.to_owned(), value));
    };
    set("date", Value::String(date.to_owned()));
    set("commit", Value::String(commit.to_owned()));
    set("scenario", Value::String("perf-gate".to_owned()));
    set("bin", Value::String(bin_version));
    set("store", Value::String(store_label));
    set("host", Value::String(host.to_owned()));
    set("toolchain", Value::String(toolchain.to_owned()));
    set("get_session_sid_s", Value::from(sid_s));
    set("get_session_mid_s", Value::from(mid_s));
    set("get_message_s", Value::from(msg_s));
    set("search_s", Value::from(search_s));
    set("search_dated_s", Value::from(dated_s));
    set("search_mode", Value::String("vector".to_owned()));
    set("sql_count_s", Value::from(sql_s));
    set("equivalence", Value::String("OK".to_owned()));
    set("fts_iops", column(&serve, "fts_search"));
    set("vector_iops", column(&serve, "vector_search"));
    set("get_message_iops", column(&serve, "pond_get_message"));
    set("search_iops", column(&serve, "pond_search"));
    set("open_store_ms", millis(&ops, "open store (manifests)"));
    set("row_counts_ms", millis(&ops, "row_counts"));
    set("rowmap_cold_ms", millis(&ops, "ensure_rowmap COLD"));
    set("rowmap_warm_ms", millis(&ops, "ensure_rowmap WARM"));
    set("write_backend", write_backend);
    set("write_corpus", write_corpus);
    set("write_copy_ms", copy_ms(&copy, "1"));
    set("write_copy_merge_ms", copy_ms(&copy, "1b"));
    set("write_copy_noop_ms", copy_ms(&copy, "3"));
    set("write_copy_delta_ms", copy_ms(&copy, "4"));
    set("write_ms_per_commit", sweep(&sweep_out, 5));
    set("write_rows_per_s", sweep(&sweep_out, 6));
    set("write_index_build_ms", build_total(&prof));
    set("write_fold_ms", fold_total(&prof));
    Ok(row)
}

/// The three write_bench passes, in the order their scrapers expect.
fn write_passes(argv: &[&str], profile_dir: &Path) -> Result<(String, String, String)> {
    let copy = tee(cargo()
        .args(["bench", "--bench", "write_bench", "--"])
        .args(argv)
        .env("POND_EMBEDDINGS_ENABLED", "true"))?;
    if copy.contains(": false") {
        bail!("WRITE VERIFICATION FAILED");
    }
    let sweep_batch = WRITE_SWEEP_BATCH.to_string();
    let sweep_out = tee(cargo()
        .args(["bench", "--bench", "write_bench", "--"])
        .args(argv)
        .args([
            "--append-sweep",
            sweep_batch.as_str(),
            "--sweep-commits-cap",
            "10",
        ])
        .env("POND_EMBEDDINGS_ENABLED", "true"))?;
    // --grown 2: round 0 folds under the eager policy, round 1 under the
    // deferred policy production sync uses - round 1 is the scraped figure.
    let prof = tee(cargo()
        .args(["bench", "--bench", "write_bench", "--"])
        .args(argv)
        .arg("--profile-optimize")
        .arg(profile_dir)
        .args(["--grown", "2"])
        .env("POND_EMBEDDINGS_ENABLED", "true"))?;
    Ok((copy, sweep_out, prof))
}

fn build_total(text: &str) -> Value {
    number(
        text.lines()
            .find(|line| line.contains("build total:"))
            .and_then(|line| line.split_whitespace().nth(2)),
    )
}

fn fold_total(text: &str) -> Value {
    number(
        text.lines()
            .find(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                fields.first() == Some(&"round")
                    && fields.get(1) == Some(&"1")
                    && fields.get(2).is_some_and(|tag| tag.starts_with("[after"))
            })
            .and_then(|line| line.split("total: ").nth(1))
            .and_then(|rest| rest.split_whitespace().next()),
    )
}

/// Delete the `<base>-*` scratch stores write_bench creates, through pond's own
/// config, credentials and object-store client. Best effort and loud: a failure
/// warns and the gate goes on, exactly as the s5cmd sweep it replaces did.
async fn scratch_clean(base: &str, config: &Config) {
    match scratch_delete(base, config).await {
        Ok(0) => println!("scratch already clean: {base}-*"),
        Ok(count) => println!("scratch cleaned: {count} objects under {base}-*"),
        Err(error) => {
            println!("WARNING: scratch cleanup failed for {base}-* - clean manually: {error:#}");
        }
    }
}

async fn scratch_delete(base: &str, config: &Config) -> Result<usize> {
    let storage = StorageUrl::parse(base)?;
    let resolved = storage.resolve(&config.creds)?;
    let lance = resolved.lance_url().clone();
    let key = lance.path().trim_matches('/').to_owned();
    // The guard the python heredoc carried: no prefix segment means `<base>-*`
    // would name buckets, not keys.
    let (parent_key, leaf) = key
        .rsplit_once('/')
        .map(|(parent, leaf)| (format!("{parent}/"), leaf.to_owned()))
        .unwrap_or_else(|| (String::new(), key.clone()));
    if leaf.is_empty() {
        bail!("{base} has no prefix segment - refusing a bucket-root scratch sweep");
    }
    // A query or fragment carrying a `/` would move the leaf onto the store's
    // own name; anything but the leaf the gate built is refused.
    if leaf != SCRATCH_LEAF {
        bail!(
            "{base} resolves to scratch leaf {leaf:?}, not {SCRATCH_LEAF:?} - refusing the sweep"
        );
    }
    let host = lance
        .host_str()
        .ok_or_else(|| anyhow!("{base} resolves to a URL with no bucket"))?;
    let parent_uri = format!("{}://{host}/{parent_key}", lance.scheme());
    let params = ObjectStoreParams {
        storage_options_accessor: (!resolved.options.is_empty()).then(|| {
            Arc::new(StorageOptionsAccessor::with_static_options(
                resolved.options.clone(),
            ))
        }),
        ..Default::default()
    };
    let registry = Arc::new(ObjectStoreRegistry::default());
    let (store, root) = ObjectStore::from_uri_and_params(registry, &parent_uri, &params)
        .await
        .map_err(|error| anyhow!("cannot open object store for {parent_uri}: {error}"))?;
    let listing = store
        .inner
        .list_with_delimiter(Some(&root))
        .await
        .with_context(|| format!("listing {parent_uri}"))?;
    let scratch_prefix = format!("{leaf}-");
    let mut victims: Vec<ObjPath> = Vec::new();
    for prefix in listing.common_prefixes {
        let matches = prefix
            .parts()
            .next_back()
            .is_some_and(|part| part.as_ref().starts_with(&scratch_prefix));
        if !matches {
            continue;
        }
        let mut objects = store.inner.list(Some(&prefix));
        while let Some(meta) = objects.next().await {
            victims.push(meta.context("listing a scratch prefix")?.location);
        }
    }
    let count = victims.len();
    let mut deleted = store
        .inner
        .delete_stream(futures::stream::iter(victims.into_iter().map(Ok)).boxed());
    while let Some(result) = deleted.next().await {
        result.context("deleting a scratch object")?;
    }
    Ok(count)
}

// ------------------------------------------------------------------- mem gate

fn word_list(value: &str) -> Vec<String> {
    value.split_whitespace().map(str::to_owned).collect()
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn mem_runs(args: &Args) -> Result<usize> {
    match args.runs {
        Some(runs) if runs >= 1 => Ok(runs),
        Some(_) => bail!("--runs must be at least 1"),
        // A `large` scenario is minutes of work and its row gates nothing yet.
        None if args.profile == "large" => Ok(1),
        None => Ok(3),
    }
}

fn mem_gate(
    args: &Args,
    runs: usize,
    baseline: &Path,
    date: &str,
    commit: &str,
    host: &str,
    toolchain: &str,
) -> Result<Vec<GroupKey>> {
    let profile = args.profile.as_str();
    let mode = if args.check { "check" } else { "append" };
    println!("\n=== mem gate: profile={profile} mode={mode} ===");
    let scenarios = word_list(&env_or("SCENARIOS", DEFAULT_SCENARIOS));
    // `var`, not a default-when-empty: `RECORD_ONLY=` on the command line means
    // "gate every scenario" - the way to rehearse a promotion.
    let record_only: BTreeSet<String> =
        word_list(&std::env::var("RECORD_ONLY").unwrap_or_else(|_| DEFAULT_RECORD_ONLY.to_owned()))
            .into_iter()
            .collect();

    println!("--- build (release, --features mem-probe) ---");
    let bench_bin = mem_bench_binary()?;
    println!("binary: {}", bench_bin.display());

    // The corpus is generated once and cached under ~/.cache/pond-bench;
    // building it inside a measured run would attribute the generator's
    // allocations to the scenario.
    println!("--- corpus ---");
    run(Command::new(&bench_bin).args(["--prepare", "--profile", profile]))?;

    let mut fresh: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    let mut touched: Vec<GroupKey> = Vec::new();
    for scenario in &scenarios {
        println!("--- {scenario} ---");
        let mut candidates: Vec<Map<String, Value>> = Vec::with_capacity(runs);
        for run in 1..=runs {
            // One scenario per process: peak RSS is a process-lifetime
            // high-water mark, so two scenarios in one process cannot be told
            // apart - and the repeats are what the median is taken over.
            let body = run_scenario(&bench_bin, scenario, profile)?;
            let tag = if runs > 1 {
                format!("  run {run}/{runs}")
            } else {
                String::new()
            };
            println!("{tag}  {}", summarize(&body));
            candidates.push(body);
        }
        let body = median_run(candidates)?;
        if args.check {
            fresh.insert(scenario.clone(), body);
        } else {
            let mut pairs: Vec<(String, Value)> = vec![
                ("date".to_owned(), Value::String(date.to_owned())),
                ("commit".to_owned(), Value::String(commit.to_owned())),
                ("host".to_owned(), Value::String(host.to_owned())),
                ("toolchain".to_owned(), Value::String(toolchain.to_owned())),
            ];
            pairs.extend(body.iter().map(|(k, v)| (k.clone(), v.clone())));
            append_line(baseline, &encode_row(&pairs))?;
            touched.push((scenario.clone(), profile.to_owned(), host.to_owned()));
        }
    }
    if args.check {
        mem_check(
            baseline,
            &scenarios,
            profile,
            host,
            toolchain,
            &record_only,
            &fresh,
        )?;
    }
    Ok(touched)
}

/// One scenario, one process, one JSON row off stdout. The scenario's own
/// progress goes to stderr and stays on the terminal.
fn run_scenario(bench_bin: &Path, scenario: &str, profile: &str) -> Result<Map<String, Value>> {
    let stdout = capture(
        Command::new(bench_bin)
            .args(["--scenario", scenario, "--profile", profile])
            .stderr(Stdio::inherit()),
    )?;
    let value: Value = serde_json::from_str(stdout.trim())
        .with_context(|| format!("mem_bench printed no JSON row for {scenario}"))?;
    match value {
        Value::Object(body) => Ok(body),
        _ => bail!("mem_bench row for {scenario} is not an object"),
    }
}

fn summarize(body: &Map<String, Value>) -> String {
    let mib = |key: &str, unit: f64| {
        body.get(key).and_then(Value::as_f64).map_or_else(
            || "n/a".to_owned(),
            |value| format!("{:.1} MiB", value / unit),
        )
    };
    format!(
        "wall {} ms  peak_rss {}  peak_heap {}",
        body.get("wall_ms").unwrap_or(&Value::Null),
        mib("peak_rss_kb", 1024.0),
        mib("peak_heap_bytes", 1_048_576.0),
    )
}

fn peak_heap(row: &Map<String, Value>) -> f64 {
    // A run with no heap figure (no `mem-probe`) sorts last rather than
    // winning the median by default.
    row.get("peak_heap_bytes")
        .and_then(Value::as_f64)
        .unwrap_or(f64::INFINITY)
}

/// The run whose `peak_heap_bytes` is the median, kept WHOLE: an averaged row
/// is one no process ever produced, and the fields would stop agreeing with
/// each other. An even run count takes the lower median.
fn median_run(mut candidates: Vec<Map<String, Value>>) -> Result<Map<String, Value>> {
    if candidates.len() < 2 {
        return candidates.pop().context("no run to record");
    }
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by(|a, b| peak_heap(&candidates[*a]).total_cmp(&peak_heap(&candidates[*b])));
    let pick = order[(order.len() - 1) / 2];
    let spread = candidates
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let heap = peak_heap(row);
            let shown = if heap.is_finite() {
                format!("{:.1}", heap / 1_048_576.0)
            } else {
                "n/a".to_owned()
            };
            format!("run {}: {shown}", index + 1)
        })
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "  peak_heap MiB over {} runs: {spread}  -> median is run {}, recorded whole",
        candidates.len(),
        pick + 1,
    );
    Ok(candidates.swap_remove(pick))
}

/// Builds and benches running elsewhere on this box starve scan readahead, and
/// a starved run reads peak heap LOW - which the next run then reads as a
/// regression. Recording refuses; a check only warns, because a CI box that
/// never goes quiet still has to be able to fail a real regression.
fn guard_against_contention(check: bool, allow_contended: bool) -> Result<()> {
    let busy = contending_processes();
    if busy.is_empty() {
        return Ok(());
    }
    let listed = busy
        .iter()
        .map(|entry| format!("\n  {entry}"))
        .collect::<String>();
    if check || allow_contended {
        println!(
            "WARNING: {} other build/bench process(es) are live; peak heap will read low:{listed}",
            busy.len(),
        );
        return Ok(());
    }
    bail!(
        "refusing to record while {} other build/bench process(es) are live - a loaded host \
         reads peak heap low and plants a false regression for the next run:{listed}\n\
         Wait for the box to go quiet, or pass --allow-contended to record anyway.",
        busy.len(),
    )
}

/// Other live `cargo`, `rustc` and `*bench*` processes, as "<pid> <cmdline>".
/// This process and its ancestors (the `cargo bench` that launched the gate)
/// are never contention.
#[cfg(target_os = "linux")]
fn contending_processes() -> Vec<String> {
    let mine = own_process_chain();
    let mut busy = Vec::new();
    let Ok(entries) = fs::read_dir("/proc") else {
        return busy;
    };
    for entry in entries.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
        else {
            continue;
        };
        if mine.contains(&pid) {
            continue;
        }
        let Ok(raw) = fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let argv: Vec<String> = raw
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect();
        // The first two words, so a wrapper (`kache rustc ...`) is judged by
        // what it wraps.
        let names = argv.iter().take(2).filter_map(|arg| {
            Path::new(arg)
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
        });
        let hit = names.into_iter().any(|name| {
            name == "cargo"
                || name == "rustc"
                || name.contains("bench")
                || name.starts_with("gate-")
        });
        if !hit {
            continue;
        }
        let line: String = argv.join(" ").chars().take(110).collect();
        busy.push(format!("{pid} {line}"));
    }
    busy.sort();
    busy
}

/// This process and every ancestor up to pid 1.
#[cfg(target_os = "linux")]
fn own_process_chain() -> BTreeSet<u32> {
    let mut chain = BTreeSet::new();
    let mut pid = std::process::id();
    while pid > 1 && chain.insert(pid) {
        let Ok(status) = fs::read_to_string(format!("/proc/{pid}/status")) else {
            break;
        };
        let parent = status
            .lines()
            .find_map(|line| line.strip_prefix("PPid:"))
            .and_then(|value| value.trim().parse::<u32>().ok());
        match parent {
            Some(parent) => pid = parent,
            None => break,
        }
    }
    chain
}

/// No /proc, no guard: the gate runs on Linux and macOS, and a macOS host has
/// no equally cheap way to enumerate processes without a new dependency.
#[cfg(not(target_os = "linux"))]
fn contending_processes() -> Vec<String> {
    Vec::new()
}

/// Build once and learn the executable's path from cargo's JSON stream; every
/// scenario then runs that binary. `cargo bench` per scenario would re-enter
/// cargo and print its own noise into the row's stdout.
fn mem_bench_binary() -> Result<PathBuf> {
    let out = cargo()
        .args([
            "build",
            "--release",
            "--bench",
            "mem_bench",
            "--features",
            "mem-probe",
            "--message-format=json",
        ])
        .stderr(Stdio::inherit())
        .output()
        .context("failed to spawn cargo build for mem_bench")?;
    if !out.status.success() {
        bail!("cargo build --bench mem_bench exited with {}", out.status);
    }
    let mut path = None;
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Ok(Value::Object(msg)) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let is_artifact = msg.get("reason").and_then(Value::as_str) == Some("compiler-artifact");
        let is_mem_bench = msg
            .get("target")
            .and_then(|target| target.get("name"))
            .and_then(Value::as_str)
            == Some("mem_bench");
        if !is_artifact || !is_mem_bench {
            continue;
        }
        if let Some(executable) = msg.get("executable").and_then(Value::as_str) {
            path = Some(PathBuf::from(executable));
        }
    }
    path.context("no mem_bench executable in the cargo build output")
}

fn mem_check(
    baseline: &Path,
    scenarios: &[String],
    profile: &str,
    host: &str,
    toolchain: &str,
    record_only: &BTreeSet<String>,
    fresh: &BTreeMap<String, Map<String, Value>>,
) -> Result<()> {
    let pct: f64 = std::env::var("MEM_GATE_MAX_REGRESSION_PCT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20.0);
    println!(
        "--- check vs baseline {} (threshold {pct:.0}%) ---",
        baseline.display()
    );
    let rows = read_rows(baseline)?;
    let mut failed = false;
    let mut unjudged: Vec<&str> = Vec::new();
    for scenario in scenarios {
        let now = fresh
            .get(scenario)
            .with_context(|| format!("no fresh row for {scenario}"))?;
        let committed: Vec<&Map<String, Value>> = rows
            .iter()
            .filter(|row| group_key(row) == (scenario.clone(), profile.to_owned(), host.to_owned()))
            .collect();
        let last = committed.last().copied();
        println!("\n[{scenario}] {profile} on {host}");
        // `get`, not indexing: rows recorded before the toolchain field existed
        // are still valid baselines and must stay comparable.
        println!(
            "  toolchain {toolchain}  (baseline row: {})",
            last.map_or("none", |row| row
                .get("toolchain")
                .and_then(Value::as_str)
                .unwrap_or("unrecorded")),
        );
        for key in COUNTERS {
            let fresh_count = now.get(key).and_then(Value::as_i64);
            let before = last.and_then(|row| row.get(key)).and_then(Value::as_i64);
            let (Some(fresh_count), Some(before)) = (fresh_count, before) else {
                // Say so rather than skipping in silence: a record-only
                // scenario never reaches the missing-row failure below, and a
                // row that predates the counter raises no other warning.
                let seen = now.get(key).is_some_and(|v| !v.is_null())
                    || last
                        .and_then(|row| row.get(key))
                        .is_some_and(|v| !v.is_null());
                if seen {
                    println!("  skip {key}: not comparable across these rows");
                }
                continue;
            };
            let verdict = if fresh_count > before { "FAIL" } else { "ok" };
            failed |= fresh_count > before;
            println!("  {verdict:<4} {key:<18} {before:>14} -> {fresh_count:<14}");
        }
        if record_only.contains(scenario) {
            // Printed with its delta, never judged: this scenario is
            // accumulating spread, and the delta is what phase 2 reads to
            // decide it has enough.
            if last.is_none() {
                // Loud, but not fatal: a fresh host has to be able to record
                // its first rows, and hard-failing here would block that
                // bootstrap. The summary carries the count so the gap cannot
                // pass as a pass.
                println!(
                    "  WARNING: UNJUDGED - record-only scenario with no committed baseline row for host {host}; nothing was compared. Run `cargo bench --bench gate -- --only mem --profile {profile}` and commit the row to give this host a baseline."
                );
                unjudged.push(scenario);
            }
            for key in GATED_METRICS {
                let (before, fresh_value) = pair(last, now, key);
                let delta = match (before, fresh_value) {
                    (Some(before), Some(value)) if before > 0.0 => {
                        format!("{:+.1}%", (value - before) / before * 100.0)
                    }
                    _ => "n/a".to_owned(),
                };
                println!(
                    "  note {key:<18} {:>14} -> {:<14} {delta:>7}  (record-only)",
                    shown(before),
                    shown(fresh_value),
                );
            }
            continue;
        }
        let Some(last) = last else {
            println!(
                "  FAIL: no committed baseline row for host {host} - run `cargo bench --bench gate -- --only mem --profile {profile}` locally and commit the baseline"
            );
            failed = true;
            continue;
        };
        for key in GATED_METRICS {
            let (before, fresh_value) = pair(Some(last), now, key);
            let (Some(before), Some(value)) = (before, fresh_value) else {
                println!("  skip {key}: not comparable across these rows");
                continue;
            };
            if before <= 0.0 {
                println!("  skip {key}: not comparable across these rows");
                continue;
            }
            let delta = (value - before) / before * 100.0;
            let verdict = if delta > pct { "FAIL" } else { "ok" };
            failed |= delta > pct;
            println!(
                "  {verdict:<4} {key:<18} {before:>14} -> {value:<14} {delta:+.1}%  (baseline {} {})",
                last.get("date").and_then(Value::as_str).unwrap_or("-"),
                last.get("commit").and_then(Value::as_str).unwrap_or("-"),
            );
        }
    }
    let summary = format!(
        "{} unjudged scenario{}{}",
        unjudged.len(),
        if unjudged.len() == 1 { "" } else { "s" },
        if unjudged.is_empty() {
            String::new()
        } else {
            format!(": {}", unjudged.join(", "))
        },
    );
    if failed {
        bail!("\nmem gate: regression beyond {pct:.0}% (or missing baseline row); {summary}");
    }
    println!(
        "\nmem gate: every judged scenario within {pct:.0}% of the committed baseline; {summary}"
    );
    Ok(())
}

fn pair(
    committed: Option<&Map<String, Value>>,
    fresh: &Map<String, Value>,
    key: &str,
) -> (Option<f64>, Option<f64>) {
    (
        committed
            .and_then(|row| row.get(key))
            .and_then(Value::as_f64),
        fresh.get(key).and_then(Value::as_f64),
    )
}

fn shown(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| format!("{value}"))
}
