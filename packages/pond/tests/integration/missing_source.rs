//! An enabled adapter whose configured source directory does not exist on this
//! host fails that adapter - and only that adapter. The run continues.
//!
//! The failure this pins (#236): a shared fleet `config.toml` enables adapters
//! for tools a given host may not have. On such a host `pond sync --dry-run`
//! aborted the whole run at the first absent source (`Error: codex-cli io error
//! at /...: No such file or directory`), so the healthy adapters produced no
//! verdict at all. Real sync did not abort, but reported the absent source as
//! `up to date  1 err` - misleading where it was visible, and absent from the
//! `--format json` summary entirely, i.e. invisible in exactly the cron logs a
//! fleet reads. `session-movement-complete` wants the still-reachable sources
//! synced; `adapter-integrity-no-silent-drops` wants the unreachable one named.
//!
//! These are CLI tests on purpose. Per-adapter isolation is only observable
//! end to end, and what an operator SEES lives in four rendering paths in
//! `main.rs` (dry-run text and JSON, real-sync stderr and JSON summary) that no
//! adapter-level test reaches - the exact thing a later refactor would quietly
//! undo while every adapter test still passed.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use assert_cmd::Command;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// One project dir from the committed claude-code fixtures, copied into the
/// sandbox so the healthy adapter has something real to ingest.
const CLAUDE_CODE_FIXTURE: &str =
    "tests/fixtures/adapter/claude_code/projects/-Users-user-Projects-myproject-a";

/// What the healthy adapter's source holds. The dry-run cases want a bare
/// existing dir - `0 sessions` is the honest count there, and it is precisely
/// the count its absent sibling must NOT report. The real-sync case needs a
/// session to land, or "the other adapter still ran" is unprovable.
enum Healthy {
    EmptyDir,
    OneFixtureSession,
}

/// `[adapters.claude-code].path` points at the `projects` dir itself.
fn claude_code_root(temp: &TempDir, healthy: &Healthy) -> PathBuf {
    let root = temp.path().join("claude").join("projects");
    std::fs::create_dir_all(&root).expect("claude-code projects dir");
    if matches!(healthy, Healthy::OneFixtureSession) {
        let fixture = Path::new(CLAUDE_CODE_FIXTURE);
        let project = root.join(fixture.file_name().expect("fixture dir name"));
        std::fs::create_dir_all(&project).expect("project dir");
        let entries = std::fs::read_dir(fixture).unwrap_or_else(|error| {
            panic!("fixtures resolve against the crate root ({CLAUDE_CODE_FIXTURE}): {error}")
        });
        let mut copied = 0;
        for entry in entries {
            let path = entry.expect("fixture entry").path();
            if path.extension().is_some_and(|ext| ext == "jsonl") {
                std::fs::copy(&path, project.join(path.file_name().expect("file name")))
                    .expect("copy fixture session");
                copied += 1;
            }
        }
        assert!(
            copied > 0,
            "the fixture project must hold a session, or the healthy-adapter assertion is vacuous",
        );
    }
    root
}

/// The source path a fleet config carries for a tool this host never installed.
/// Nested two levels deep: the parent is missing too, which is what a host
/// without the tool actually looks like.
fn absent_source(temp: &TempDir) -> PathBuf {
    let path = temp.path().join("absent").join("codex").join("sessions");
    assert!(
        !path.exists(),
        "the point of this suite is that it is absent"
    );
    path
}

fn write_config(temp: &TempDir, adapters: &str) {
    let config_dir = temp.path().join("config").join("pond");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "[storage]\npath = {:?}\n\n{adapters}",
            temp.path().join("store").display().to_string(),
        ),
    )
    .expect("write config");
}

/// The issue's repro B: two enabled adapters, one of which this host cannot
/// possibly read. Returns the absent path, which every assertion quotes.
fn fleet_config(temp: &TempDir, healthy: Healthy) -> PathBuf {
    let root = claude_code_root(temp, &healthy);
    let missing = absent_source(temp);
    write_config(
        temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n\
             [adapters.codex-cli]\nenabled = true\npath = {:?}\n",
            root.display().to_string(),
            missing.display().to_string(),
        ),
    );
    missing
}

fn run(temp: &TempDir, args: &[&str]) -> std::process::Output {
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&state).expect("state");
    Command::new(env!("CARGO_BIN_EXE_pond"))
        .args(args)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_STATE_HOME", &state)
        .env_remove("RUST_LOG")
        .env("NO_COLOR", "1")
        .output()
        .expect("run pond")
}

fn stdout(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &std::process::Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// The one-document-on-stdout contract holds on every `--format json` exit.
fn json(args: &[&str], out: &std::process::Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|error| {
        panic!(
            "{args:?} stdout was not one JSON document ({error}): {}\nstderr: {}",
            stdout(out),
            stderr(out),
        )
    })
}

fn adapter_row<'a>(doc: &'a Value, name: &str) -> &'a Value {
    doc["adapters"]
        .as_array()
        .unwrap_or_else(|| panic!("no adapters array: {doc}"))
        .iter()
        .find(|row| row["name"].as_str() == Some(name))
        .unwrap_or_else(|| panic!("no {name} row - a failed adapter must still be listed: {doc}"))
}

fn assert_exit_ok(out: &std::process::Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} must not fail the run over one host's missing source (exit {:?})\nstdout: {}\nstderr: {}",
        out.status.code(),
        stdout(out),
        stderr(out),
    );
}

/// The issue's headline: the abort blanked every other adapter's verdict. The
/// healthy adapter must still be planned, and the absent one must say why -
/// both in one run.
#[test]
fn dry_run_text_plans_the_healthy_adapter_and_names_the_missing_source() {
    let temp = TempDir::new().expect("temp");
    let missing = fleet_config(&temp, Healthy::EmptyDir);
    let out = run(&temp, &["sync", "--dry-run"]);
    assert_exit_ok(&out, "a dry run");
    let stdout = stdout(&out);

    let healthy = stdout
        .lines()
        .find(|line| line.contains("claude-code"))
        .unwrap_or_else(|| panic!("no claude-code plan row - the abort is back: {stdout}"));
    assert!(
        healthy.contains("sessions"),
        "the healthy adapter must get a real plan verdict, not a failure: {healthy:?}",
    );

    let failed = stdout
        .lines()
        .find(|line| line.contains("codex-cli"))
        .unwrap_or_else(|| panic!("no codex-cli row: {stdout}"));
    assert!(
        failed.contains(&format!(
            "source missing: {} - skipped this run",
            missing.display()
        )),
        "the failed row must name the path and its consequence: {failed:?}",
    );
}

/// A count is a claim. An adapter whose source was never readable reports null
/// plus a reason - never 0, which is what its healthy sibling's empty dir
/// legitimately says in the same document.
#[test]
fn dry_run_json_nulls_the_missing_adapter_and_counts_the_healthy_one() {
    let temp = TempDir::new().expect("temp");
    let missing = fleet_config(&temp, Healthy::EmptyDir);
    let args = ["sync", "--dry-run", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a JSON dry run");
    let doc = json(&args, &out);

    let failed = adapter_row(&doc, "codex-cli");
    assert!(
        failed["sessions"].is_null(),
        "an unreadable source must not report a session count: {failed}",
    );
    let error = failed["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no error explaining the null count: {failed}"));
    assert!(
        error.contains("source missing") && error.contains(&missing.display().to_string()),
        "the reason must name the condition and the path it looked at: {error:?}",
    );

    let healthy = adapter_row(&doc, "claude-code");
    assert_eq!(
        healthy["sessions"].as_u64(),
        Some(0),
        "an empty but readable source is a checked 0, not a failure: {healthy}",
    );
    assert!(
        healthy["error"].is_null(),
        "the healthy adapter must not inherit its sibling's failure: {healthy}",
    );
}

/// Real sync: the run succeeds, the reachable source lands, and the summary
/// still names what it could not read. Exit 0 is deliberate (decision 2) - a
/// fleet host missing one tool is not a failing host.
#[test]
fn real_sync_json_ingests_the_healthy_adapter_and_names_the_failed_one() {
    let temp = TempDir::new().expect("temp");
    let missing = fleet_config(&temp, Healthy::OneFixtureSession);
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a whole-config sync");
    let doc = json(&args, &out);

    assert_eq!(
        doc["outcome"].as_str(),
        Some("ok"),
        "one host's missing source is not a failed sync: {doc}",
    );
    assert!(
        doc["sessions_inserted"].as_u64().is_some_and(|n| n >= 1),
        "the healthy adapter must actually have run: {doc}",
    );

    let failed = doc["failed_adapters"]
        .as_array()
        .unwrap_or_else(|| panic!("no failed_adapters in the summary: {doc}"));
    assert_eq!(
        failed.len(),
        1,
        "exactly the unreachable adapter is a failure: {doc}",
    );
    let entry = &failed[0];
    assert_eq!(entry["name"].as_str(), Some("codex-cli"), "{entry}");
    assert_eq!(
        entry["path"].as_str(),
        Some(missing.display().to_string().as_str()),
        "the failure must carry the path it looked at: {entry}",
    );
    let error = entry["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no error on the failed adapter: {entry}"));
    assert!(
        error.contains("source missing") && error.contains(&missing.display().to_string()),
        "the failure must be self-describing to a log scraper: {error:?}",
    );
    assert_eq!(
        entry["reason"].as_str(),
        Some("source_missing"),
        "fleet tooling branches on the stable kind, not the human string: {entry}",
    );
}

/// Off-TTY is where fleets read sync output, and it is where the old reason
/// line vanished (`MultiProgress::println` is dropped without a terminal).
/// The tail that survived - `up to date  1 err` - was worse than silence.
#[test]
fn real_sync_text_prints_the_failure_off_tty() {
    let temp = TempDir::new().expect("temp");
    let missing = fleet_config(&temp, Healthy::OneFixtureSession);
    let out = run(&temp, &["sync"]);
    assert_exit_ok(&out, "a whole-config sync");
    let stderr = stderr(&out);

    let line = stderr
        .lines()
        .find(|line| line.contains("source missing"))
        .unwrap_or_else(|| panic!("nothing on stderr named the missing source: {stderr}"));
    assert!(
        line.contains("codex-cli"),
        "the failure must be attributed to its adapter: {line:?}",
    );
    assert!(
        line.contains(&format!(
            "source missing: {} - skipped this run",
            missing.display()
        )),
        "the line must name the path and its consequence: {line:?}",
    );
    assert!(
        !stderr
            .lines()
            .any(|line| line.contains("codex-cli") && line.contains("up to date")),
        "a source that was never readable is not 'up to date': {stderr}",
    );
}

/// Naming the adapter is naming a host's tool deliberately, so an absent source
/// is a wrong invocation, not fleet state - and a `--path` typo must keep
/// costing a non-zero exit rather than a silent no-op.
#[test]
fn explicit_narrowing_keeps_the_hard_error() {
    let temp = TempDir::new().expect("temp");
    let missing = fleet_config(&temp, Healthy::EmptyDir);
    let path = missing.display().to_string();
    for args in [
        vec!["sync", "codex-cli"],
        vec!["sync", "codex-cli", "--dry-run"],
        vec!["sync", "codex-cli", "--path", path.as_str()],
    ] {
        let out = run(&temp, &args);
        assert!(
            !out.status.success(),
            "{args:?} named the adapter, so its absent source must fail the run\nstdout: {}\nstderr: {}",
            stdout(&out),
            stderr(&out),
        );
        let stderr = stderr(&out);
        assert!(
            stderr.contains(&path),
            "{args:?} must say which path it could not find: {stderr}",
        );
        assert!(
            stderr.contains("codex-cli"),
            "{args:?} must name the adapter it was pointed at: {stderr}",
        );
        assert!(
            !stderr.contains("claude-code"),
            "{args:?} narrowed to one adapter; the others are not this run's business: {stderr}",
        );
    }
}

/// A multi-path entry named explicitly fails BEFORE its first fanned pass does
/// any work: `pond sync <adapter>` with `path = [readable, absent]` must not
/// ingest the readable dir and then error on the absent one, leaving store
/// writes behind a non-zero exit. The pre-scan runs over every resolved pass
/// up front, in real sync and dry run alike.
#[test]
fn multi_path_narrowing_fails_before_any_write() {
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::OneFixtureSession);
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = [{:?}, {:?}]\n",
            root.display().to_string(),
            missing.display().to_string(),
        ),
    );
    for args in [
        vec!["sync", "claude-code"],
        vec!["sync", "claude-code", "--dry-run"],
    ] {
        let out = run(&temp, &args);
        assert!(
            !out.status.success(),
            "{args:?} named an adapter with an absent pass, so the run must fail\nstdout: {}\nstderr: {}",
            stdout(&out),
            stderr(&out),
        );
        assert!(
            stderr(&out).contains(&missing.display().to_string()),
            "{args:?} must name the absent path: {}",
            stderr(&out),
        );
    }
    // The proof the bail preceded the first pass: the readable dir's session is
    // still pending, so nothing was ingested before the error.
    let args = ["sync", "--dry-run", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(
        &out,
        "the whole-config dry run after the failed narrow sync",
    );
    let doc = json(&args, &out);
    let readable = doc["adapters"]
        .as_array()
        .unwrap_or_else(|| panic!("no adapters array: {doc}"))
        .iter()
        .find(|row| row["path"].as_str() == Some(root.display().to_string().as_str()))
        .unwrap_or_else(|| panic!("no row for the readable pass: {doc}"));
    assert_eq!(
        readable["pending"].as_u64(),
        Some(1),
        "the narrowed run must not have ingested anything before failing: {readable}",
    );
}

/// The adjacent detail, pinned so the fix above cannot swallow it: a malformed
/// config blob is not fleet state, and keeps failing the whole resolve even
/// when a sibling adapter is perfectly healthy (spec 7.8).
#[test]
fn a_config_blob_with_no_path_still_fails_the_whole_run() {
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::OneFixtureSession);
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n\
             [adapters.codex-cli]\nenabled = true\n",
            root.display().to_string(),
        ),
    );
    for args in [vec!["sync"], vec!["sync", "--dry-run"]] {
        let out = run(&temp, &args);
        assert!(
            !out.status.success(),
            "{args:?} must abort on a bad config blob, healthy sibling or not\nstdout: {}\nstderr: {}",
            stdout(&out),
            stderr(&out),
        );
        let stderr = stderr(&out);
        assert!(
            stderr.contains("codex-cli") && stderr.contains("config"),
            "{args:?} must name the adapter whose config is wrong: {stderr}",
        );
        assert!(
            stderr.contains("path"),
            "{args:?} must name the missing key, or the fix is a guess: {stderr}",
        );
    }
}
