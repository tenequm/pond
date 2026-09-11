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

/// What the healthy adapter's source holds. Most rendering and early-exit
/// cases need only an existing directory; ingestion assertions use one session.
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

fn state_files(temp: &TempDir) -> Vec<String> {
    // `syncstate::pond_state_dir` omits the `pond` segment on Windows, where
    // `XDG_STATE_HOME` is already pond's own. A wrong path here reads as empty,
    // which would make every absence assertion pass vacuously - what keeps them
    // honest is the positive assertion in
    // `all_absent_whole_sync_skips_the_store_but_takes_the_lock`, which goes red
    // the moment this reads the wrong dir.
    let root = temp.path().join("state");
    let dir = if cfg!(windows) {
        root
    } else {
        root.join("pond")
    };
    match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect(),
        // A run that wrote nothing never creates the dir; the panic arm below
        // is for a dir that exists and still will not read.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read state dir {}: {error}", dir.display()),
    }
}

/// The per-host last-sync breadcrumb, parsed. Panics when absent: every path
/// that claims to record one must actually write it.
fn last_sync_record(temp: &TempDir) -> Value {
    let root = temp.path().join("state");
    let dir = if cfg!(windows) {
        root
    } else {
        root.join("pond")
    };
    let name = state_files(temp)
        .into_iter()
        .find(|name| name.starts_with("last-sync-"))
        .unwrap_or_else(|| panic!("no last-sync record in {}", dir.display()));
    let raw = std::fs::read(dir.join(name)).expect("read last-sync record");
    serde_json::from_slice(&raw).expect("last-sync record is JSON")
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

#[test]
fn narrowed_missing_source_json_is_a_compact_prestage_error() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::EmptyDir);
    let args = ["sync", "codex-cli", "--format", "json"];
    let out = run(&temp, &args);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("error"), "{doc}");
    assert!(doc["error"].as_str().is_some(), "{doc}");
    for key in ["sessions_inserted", "messages_inserted", "duration_secs"] {
        assert!(doc.get(key).is_none(), "pre-stage errors omit {key}: {doc}");
    }
}

#[test]
fn all_absent_whole_sync_skips_the_store_but_takes_the_lock() {
    let temp = TempDir::new().expect("temp");
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.codex-cli]\nenabled = true\npath = {:?}\n\n\
             [adapters.openclaw]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            missing.display().to_string(),
        ),
    );
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "an all-absent whole sync");
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("ok"), "{doc}");
    assert_eq!(doc["failed_adapters"].as_array().map(Vec::len), Some(2));
    assert_eq!(doc["indexes_folded"].as_bool(), Some(false), "{doc}");
    assert_eq!(doc["sessions_inserted"].as_u64(), Some(0), "{doc}");
    assert_eq!(doc["messages_inserted"].as_u64(), Some(0), "{doc}");
    // `stored` is the store's total, which this run never opened the store to
    // read. Inventing a zero there reports a populated lake as empty; dropping
    // the key breaks every consumer that reads it on an ok document. Indexing a
    // missing key yields Null, so the presence check has to come first.
    assert!(
        doc["stored"].is_object(),
        "an ok document always carries `stored`; a null count is not an absent key: {doc}"
    );
    assert!(doc["stored"]["sessions"].is_null(), "{doc}");
    assert!(doc["stored"]["messages"].is_null(), "{doc}");
    assert!(!temp.path().join("store").exists(), "store was created");
    // The flock IS taken: the run writes a last-sync record, and a lockless
    // write could overwrite a concurrent real sync's. Skipping the store open
    // and the embedder is the whole saving.
    assert!(
        state_files(&temp)
            .iter()
            .any(|name| name.starts_with("sync-") && name.ends_with(".lock")),
        "{:?}",
        state_files(&temp)
    );
    let record = last_sync_record(&temp);
    assert_eq!(record["outcome"].as_str(), Some("ok"), "{record}");
    assert_eq!(record["sessions_inserted"].as_u64(), Some(0), "{record}");
    for name in ["codex-cli", "openclaw"] {
        assert!(stderr(&out).contains(name), "{}", stderr(&out));
    }
    // One attribution per adapter, not one per emission site - and asserted on
    // the TEXT run, since the JSON arm never reaches the second emission site.
    // This is the cron log, where the same line twice reads as two hosts' worth
    // of trouble.
    let text = run(&temp, &["sync"]);
    assert_exit_ok(&text, "an all-absent whole sync in text mode");
    assert_eq!(
        stderr(&text).matches("source missing:").count(),
        2,
        "{}",
        stderr(&text)
    );
}

/// `stored` is the lake's total, not this run's delta, so the all-absent path
/// must not answer it from a store it never opened.
#[test]
fn an_all_absent_sync_does_not_report_an_existing_store_as_empty() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::OneFixtureSession);
    let args = ["sync", "--format", "json"];
    let seeded = json(&args, &run(&temp, &args));
    assert_eq!(seeded["stored"]["sessions"].as_u64(), Some(1), "{seeded}");

    // Same store, but now every source is gone - the fleet-config case.
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n             [adapters.codex-cli]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            missing.display().to_string(),
        ),
    );
    let out = run(&temp, &args);
    assert_exit_ok(&out, "an all-absent sync over a populated store");
    let doc = json(&args, &out);
    assert!(doc["stored"].is_object(), "{doc}");
    assert!(
        doc["stored"]["sessions"].is_null(),
        "a run that never opened the store must not claim its row counts: {doc}"
    );
}

/// The surface issue #236 was filed about: a host whose every source is absent
/// must still be diagnosable after the sync output scrolls away. The all-absent
/// sync creates no store, so `pond status` renders this from config alone.
#[test]
fn status_names_absent_sources_on_a_host_that_never_stored_anything() {
    let temp = TempDir::new().expect("temp");
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.codex-cli]\nenabled = true\npath = {:?}\n\n             [adapters.openclaw]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            missing.display().to_string(),
        ),
    );
    assert_exit_ok(&run(&temp, &["sync"]), "the all-absent sync");
    assert!(!temp.path().join("store").exists(), "store was created");

    let args = ["status", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "status on an all-absent host");
    let doc = json(&args, &out);
    assert_eq!(doc["initialized"].as_bool(), Some(false), "{doc}");
    for name in ["codex-cli", "openclaw"] {
        let row = adapter_row(&doc["local"], name);
        assert_eq!(row["reason"].as_str(), Some("source_missing"), "{doc}");
        assert!(row["sessions"].is_null(), "{doc}");
    }
    assert_eq!(
        doc["local"]["last_sync"]["outcome"].as_str(),
        Some("ok"),
        "the breadcrumb the sync just wrote must be readable: {doc}"
    );
    let text = stdout(&run(&temp, &["status"]));
    assert!(text.contains("source missing:"), "{text}");
    assert!(
        !text.contains("run `pond sync` to import sessions"),
        "the operator just ran it and it imported nothing: {text}"
    );
}

#[test]
fn verify_keeps_mixed_host_missing_source_semantics() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::OneFixtureSession);
    let args = ["sync", "--verify", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a verified mixed-host sync");
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("ok"), "{doc}");
    assert_eq!(doc["failed_adapters"][0]["reason"], "source_missing");
    // The fixture exists to prove the healthy adapter still ingests beside the
    // failed one; asserting only the failure would pass on a run that imported
    // nothing at all.
    assert_eq!(doc["sessions_inserted"].as_u64(), Some(1), "{doc}");
    assert!(stderr(&out).contains("--verify"), "{}", stderr(&out));
    assert!(stderr(&out).contains("codex-cli"), "{}", stderr(&out));
}

#[cfg(unix)]
#[test]
fn non_utf8_path_is_a_named_error_not_a_panic() {
    use std::os::unix::ffi::OsStringExt;
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::EmptyDir);
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("home");
    let out = Command::new(env!("CARGO_BIN_EXE_pond"))
        .args(["sync", "claude-code", "--path"])
        .arg(std::ffi::OsString::from_vec(b"/tmp/pond-\xff".to_vec()))
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_STATE_HOME", temp.path().join("state"))
        .env("NO_COLOR", "1")
        .output()
        .expect("run pond");
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("--path must be valid UTF-8"),
        "{}",
        stderr(&out)
    );
    assert!(!stderr(&out).contains("panicked"), "{}", stderr(&out));
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

/// `try_exists()` follows symlinks, so a dangling link is an absent source:
/// the verdict names the configured path, not the target it pointed at.
#[cfg(unix)]
#[test]
fn a_dangling_symlink_source_is_missing() {
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::EmptyDir);
    let dangling = temp.path().join("dangling");
    std::os::unix::fs::symlink(temp.path().join("nonexistent-target"), &dangling).expect("symlink");
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n\
             [adapters.codex-cli]\nenabled = true\npath = {:?}\n",
            root.display().to_string(),
            dangling.display().to_string(),
        ),
    );
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a sync whose source is a dangling symlink");
    let doc = json(&args, &out);
    let entry = &doc["failed_adapters"][0];
    assert_eq!(entry["name"].as_str(), Some("codex-cli"), "{doc}");
    assert_eq!(entry["reason"].as_str(), Some("source_missing"), "{doc}");
    assert_eq!(
        entry["path"].as_str(),
        Some(dangling.display().to_string().as_str()),
        "the failure names the configured path, not the link target: {entry}",
    );
}

/// Permission denied is not "missing". Both EACCES shapes - a statable root
/// with mode 000 (`try_exists` = Ok(true)) and a child behind an untraversable
/// parent (`try_exists` = Err) - keep the per-file-skip behavior, and the
/// skips surface as `degraded_adapters` with the count and first error.
/// Flipping the `Err(_)` arm of `missing_source_root` to "missing" would
/// misreport permission problems as absent directories; this pins it.
#[cfg(unix)]
#[test]
fn permission_denied_is_not_source_missing() {
    use std::os::unix::fs::PermissionsExt;
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::EmptyDir);
    let locked = temp.path().join("locked");
    let parent = temp.path().join("parent");
    let child = parent.join("child");
    std::fs::create_dir_all(&locked).expect("locked");
    std::fs::create_dir_all(&child).expect("child");
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    if std::fs::read_dir(&locked).is_ok() {
        // Root is not denied by mode bits; there is nothing to test.
        restore_dir_mode(&locked);
        restore_dir_mode(&parent);
        return;
    }
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n\
             [adapters.codex-cli]\nenabled = true\npath = {:?}\n\n\
             [adapters.agy]\nenabled = true\npath = {:?}\n\n\
             [adapters.openclaw]\nenabled = true\npath = {:?}\n",
            root.display().to_string(),
            locked.display().to_string(),
            child.display().to_string(),
            locked.display().to_string(),
        ),
    );
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    // Restore before asserting so TempDir cleanup works even on failure.
    restore_dir_mode(&locked);
    restore_dir_mode(&parent);
    assert_exit_ok(&out, "a sync over unreadable sources");
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("ok"), "{doc}");
    assert!(
        doc.get("failed_adapters").is_none(),
        "EACCES is the per-file path, never an absent source: {doc}",
    );
    assert!(
        !stderr(&out).contains("source missing"),
        "no surface may call a permission problem 'missing': {}",
        stderr(&out),
    );

    let degraded = doc["degraded_adapters"]
        .as_array()
        .unwrap_or_else(|| panic!("EACCES must be attributed in the summary: {doc}"));
    for name in ["agy", "codex-cli", "openclaw"] {
        let entry = degraded
            .iter()
            .find(|entry| entry["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("no degraded entry for {name}: {doc}"));
        assert!(
            entry["skipped_files"].as_u64().is_some_and(|n| n >= 1),
            "the skip count is the magnitude: {entry}",
        );
        let first_skip_reason = entry["first_skip_reason"]
            .as_str()
            .unwrap_or_else(|| panic!("no first_skip_reason explaining the skips: {entry}"));
        assert!(
            first_skip_reason.to_lowercase().contains("denied"),
            "the reason must carry the cause: {first_skip_reason:?}",
        );
    }
    assert!(
        !degraded
            .iter()
            .any(|entry| entry["name"].as_str() == Some("claude-code")),
        "the healthy adapter is not degraded: {doc}",
    );
    assert!(
        stderr(&out).contains("could not read"),
        "the skip must survive off-TTY: {}",
        stderr(&out),
    );
}

#[cfg(unix)]
fn restore_dir_mode(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).expect("restore mode");
}

/// The error document still names the adapters that failed before the abort:
/// an absent source recorded early must survive a later adapter's hard error
/// into the `outcome: "error"` summary (the out-param threading in
/// `run_import_stage`). `agy` sorts before `claude-code`, so its skip is
/// recorded before the read-only store kills the run.
#[cfg(unix)]
#[test]
fn failed_adapters_survive_onto_the_error_document() {
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::EmptyDir);
    let missing = temp.path().join("absent").join("agy").join("sessions");
    write_config(
        &temp,
        &format!(
            "[adapters.agy]\nenabled = true\npath = {:?}\n\n\
             [adapters.claude-code]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            root.display().to_string(),
        ),
    );
    // First sync builds the store; the fixture session arrives after, so the
    // second sync has a write to fail on.
    assert_exit_ok(&run(&temp, &["sync"]), "the store-seeding sync");
    claude_code_root(&temp, &Healthy::OneFixtureSession);
    let store = temp.path().join("store");
    chmod_dirs(&store, 0o555);
    if std::fs::write(store.join("probe"), b"x").is_ok() {
        // Read-only bits do not deny this user (root); nothing to test.
        let _ = std::fs::remove_file(store.join("probe"));
        chmod_dirs(&store, 0o755);
        return;
    }
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    chmod_dirs(&store, 0o755);
    assert!(
        !out.status.success(),
        "a store that cannot be written must fail the run\nstdout: {}\nstderr: {}",
        stdout(&out),
        stderr(&out),
    );
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("error"), "{doc}");
    assert!(doc["error"].is_string(), "{doc}");
    let entry = &doc["failed_adapters"][0];
    assert_eq!(
        entry["name"].as_str(),
        Some("agy"),
        "the skip recorded before the abort must survive it: {doc}",
    );
    assert_eq!(entry["reason"].as_str(), Some("source_missing"), "{entry}");
}

/// A file the adapter opens but cannot finish reading loses events, not the
/// whole file: that count lived only on the progress line, where the reason is
/// dropped off-TTY. It belongs on the same per-adapter verdict as the skips.
#[cfg(unix)]
#[test]
fn dropped_events_are_attributed_to_their_adapter() {
    use std::os::unix::fs::PermissionsExt;
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::OneFixtureSession);
    // A second session file the walk lists but the read cannot open: the
    // adapter gets far enough to charge the loss to a session, not to the file.
    let project = root.join("denied-project");
    std::fs::create_dir_all(&project).expect("project dir");
    let denied = project.join("denied.jsonl");
    std::fs::write(&denied, "{\"type\":\"user\"}\n").expect("write");
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).expect("chmod");
    if std::fs::read(&denied).is_ok() {
        // Running as root: the mode bits deny nothing, so there is no drop.
        return;
    }
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n",
            root.display().to_string(),
        ),
    );
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o644)).expect("restore");
    assert_exit_ok(&out, "a sync whose source holds one unreadable file");
    let doc = json(&args, &out);
    let entry = doc["degraded_adapters"]
        .as_array()
        .and_then(|entries| entries.first())
        .unwrap_or_else(|| panic!("the loss must be attributed to the adapter: {doc}"));
    assert_eq!(entry["name"].as_str(), Some("claude-code"), "{entry}");
    // Either population is a correct answer - which one depends on whether a
    // session was in flight when the read failed - but the loss must be counted
    // as unreadable, never as a routine validator drop.
    let lost = entry["unreadable_events"].as_u64().unwrap_or(0)
        + entry["skipped_files"].as_u64().unwrap_or(0);
    assert!(lost >= 1, "the count is the magnitude: {entry}");
    let reason = entry["first_unreadable_reason"]
        .as_str()
        .or_else(|| entry["first_skip_reason"].as_str())
        .unwrap_or_else(|| panic!("a count with no cause is the silence we removed: {entry}"));
    assert!(
        reason.to_lowercase().contains("denied"),
        "the reason must name the cause: {reason:?}",
    );
    assert!(
        stderr(&out).contains("first error:"),
        "and it must survive off-TTY: {}",
        stderr(&out),
    );
}

/// The validator's dedupe floor firing is expected behavior (spec
/// `adapter-integrity-dedup`), so a sync that ingested everything it was asked
/// to must not warn about it - the counts belong in the record, not in an
/// alarm. `claude-desktop-app`'s own fixtures drop ~26 duplicate/mismatch
/// events while inserting every session.
#[test]
fn a_routine_validator_drop_is_recorded_but_never_warns() {
    let temp = TempDir::new().expect("temp");
    let src =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/adapter/claude_desktop_app");
    write_config(
        &temp,
        &format!(
            "[adapters.claude-desktop-app]\nenabled = true\npath = {:?}\n",
            src.display().to_string(),
        ),
    );
    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a sync of the desktop-app fixtures");
    let doc = json(&args, &out);
    // The silence is only correct while every session still lands: a run that
    // dropped all four would be a real loss this test must not vouch for.
    assert_eq!(
        doc["sessions_inserted"].as_u64(),
        Some(4),
        "every fixture session must survive the dedupe floor: {doc}",
    );
    assert!(
        doc["drop_reasons"][pond::sessions::DROP_REASON_DUPLICATE_MESSAGE_ID]
            .as_u64()
            .is_some_and(|count| count > 0),
        "the dedupe floor must be what fired, and it belongs in the record: {doc}",
    );
    assert!(
        doc.get("degraded_adapters").is_none(),
        "a dedupe floor is not a degraded adapter: {doc}",
    );
    let noise: Vec<_> = stderr(&out)
        .lines()
        .filter(|line| line.starts_with("import: "))
        .map(str::to_owned)
        .collect();
    assert!(
        noise.is_empty(),
        "a successful sync must not warn about expected drops: {noise:?}",
    );
}

/// One condition, one machine-readable kind, whichever surface reports it:
/// the dry-run row carries the same `reason` token as `failed_adapters`.
#[test]
fn the_dry_run_row_carries_the_same_reason_token() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::EmptyDir);
    let args = ["sync", "--dry-run", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a whole-config dry run");
    let doc = json(&args, &out);
    assert_eq!(
        adapter_row(&doc, "codex-cli")["reason"].as_str(),
        Some("source_missing"),
        "the dry run must name the kind, not only the prose: {doc}",
    );
    assert!(
        adapter_row(&doc, "claude-code")["reason"].is_null(),
        "a healthy row has no failure kind: {doc}",
    );
}

/// Status short-circuits discovery so every adapter reports one absent-root shape.
#[test]
fn status_reports_absent_sources_consistently() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::OneFixtureSession);
    assert_exit_ok(&run(&temp, &["sync"]), "the store-seeding sync");
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.openclaw]\nenabled = true\npath = {:?}\n\n\
             [adapters.codex-cli]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            missing.display().to_string(),
        ),
    );
    let args = ["status", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "status over absent sources");
    let doc = json(&args, &out);
    for name in ["codex-cli", "openclaw"] {
        let row = adapter_row(&doc["local"], name);
        assert!(row["sessions"].is_null(), "{name}: {row}");
        assert!(row["plan"].is_null(), "{name}: {row}");
        assert_eq!(row["reason"].as_str(), Some("source_missing"), "{row}");
        assert_eq!(
            row["error"].as_str(),
            Some(format!("source missing: {}", missing.display()).as_str()),
            "{row}",
        );
    }

    let text = run(&temp, &["status"]);
    assert_exit_ok(&text, "text status over absent sources");
    let output = stdout(&text);
    for line in output
        .lines()
        .filter(|line| line.contains("codex-cli") || line.contains("openclaw"))
    {
        assert!(!line.contains("up to date"), "{line}");
        assert!(!line.contains("0 sessions"), "{line}");
    }
}

/// The narrowed bail costs nothing: a typo'd `pond sync <adapter>` must fail
/// before the store is created and before the sync flock is taken, so a wrong
/// invocation leaves the host exactly as it found it.
#[test]
fn narrowing_at_an_absent_source_creates_no_store_and_takes_no_lock() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::EmptyDir);
    let out = run(&temp, &["sync", "codex-cli"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a named absent source is a wrong invocation\nstderr: {}",
        stderr(&out),
    );
    assert!(
        !temp.path().join("store").exists(),
        "the bail must precede `open_store`, which would create the destination",
    );
    let locks: Vec<_> = state_files(&temp)
        .into_iter()
        .filter(|name| name.starts_with("sync-"))
        .collect();
    assert!(
        locks.is_empty(),
        "the bail must precede the sync flock, so a typo never queues behind a real sync: {locks:?}",
    );
}

/// The narrowed bail is only the source-missing check's to make: every other
/// resolve failure keeps the error document that carries the run's counters,
/// so a consumer reading `sessions_inserted` never finds the key simply gone.
#[test]
fn a_resolve_error_keeps_the_counted_error_document() {
    let temp = TempDir::new().expect("temp");
    fleet_config(&temp, Healthy::EmptyDir);
    for adapter in ["definitely-not-an-adapter", "agy"] {
        let args = ["sync", adapter, "--format", "json"];
        let out = run(&temp, &args);
        assert_eq!(out.status.code(), Some(1), "{adapter}: {}", stderr(&out));
        let doc = json(&args, &out);
        assert_eq!(doc["outcome"].as_str(), Some("error"), "{doc}");
        for key in ["sessions_inserted", "messages_inserted", "duration_secs"] {
            assert!(
                doc.get(key).is_some(),
                "{adapter}: a resolve error must keep {key} on the document: {doc}",
            );
        }
    }
}

/// Every directory under `root` gets `mode`; files keep theirs (denying dir
/// writes is what makes the store read-only).
#[cfg(unix)]
fn chmod_dirs(root: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(mode)).expect("chmod dir");
    for entry in std::fs::read_dir(root).expect("read_dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            chmod_dirs(&path, mode);
        }
    }
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

#[test]
fn a_config_blob_with_an_empty_path_fails_with_a_fix() {
    let temp = TempDir::new().expect("temp");
    let root = claude_code_root(&temp, &Healthy::EmptyDir);
    // Every spelling of "empty", including inside an array: a fanned pass with
    // an empty element reported `source missing:` with no path to act on.
    for spelling in [
        "\"\"".to_owned(),
        "[\"\"]".to_owned(),
        format!("[\"\", {:?}]", root.display().to_string()),
    ] {
        write_config(
            &temp,
            &format!(
                "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n\
                 [adapters.codex-cli]\nenabled = true\npath = {}\n",
                root.display().to_string(),
                spelling,
            ),
        );
        for args in [vec!["sync"], vec!["sync", "--dry-run"]] {
            let out = run(&temp, &args);
            assert!(
                !out.status.success(),
                "{args:?} must reject path = {spelling}",
            );
            let error = stderr(&out);
            assert!(error.contains("[adapters.codex-cli]"), "{args:?}: {error}");
            assert!(error.contains("empty `path`"), "{args:?}: {error}");
            assert!(
                error.contains("set it to a directory") && error.contains("pond adapters disable"),
                "{args:?} must name the fixes: {error}",
            );
        }
    }
}

/// `--no-wait` is what the scheduled run passes so ticks never queue, and spec
/// 7.8 promises it the `skipped` document. An all-absent run must not answer
/// `ok` just because it had nothing to import.
#[test]
fn an_all_absent_sync_still_reports_skipped_when_the_store_is_busy() {
    let temp = TempDir::new().expect("temp");
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.codex-cli]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
        ),
    );
    // One run to create the lock file, whose name carries the store key.
    assert_exit_ok(&run(&temp, &["sync"]), "the seeding all-absent sync");
    let root = temp.path().join("state");
    let dir = if cfg!(windows) {
        root
    } else {
        root.join("pond")
    };
    let lock_name = state_files(&temp)
        .into_iter()
        .find(|name| name.starts_with("sync-") && name.ends_with(".lock"))
        .expect("the all-absent run takes the flock");

    let held = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join(lock_name))
        .expect("open the lock file");
    held.try_lock().expect("hold the flock for this test");

    let args = ["sync", "--no-wait", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a --no-wait sync against a busy store");
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("skipped"), "{doc}");
    drop(held);
}

/// openclaw's deletion-reconciliation pass runs after ingest and resolves every
/// ambiguity to PRESERVE. A `sessions` path it cannot enumerate is ambiguity,
/// not a run failure - aborting there would throw away a healthy adapter's
/// committed work, which is the shape issue #236 exists to remove.
#[test]
fn an_unenumerable_openclaw_sessions_path_does_not_fail_the_run() {
    let temp = TempDir::new().expect("temp");
    let claude = claude_code_root(&temp, &Healthy::OneFixtureSession);
    // `list_agents` manufactures an AgentDir per dir under `agents/` without
    // probing `sessions/`, so a `sessions` that is a regular file reaches the
    // scan as a non-directory.
    let agent = temp.path().join("oc").join("agents").join("a1");
    std::fs::create_dir_all(&agent).expect("agent dir");
    std::fs::write(agent.join("sessions"), b"not a directory").expect("sessions file");
    write_config(
        &temp,
        &format!(
            "[adapters.claude-code]\nenabled = true\npath = {:?}\n\n             [adapters.openclaw]\nenabled = true\npath = {:?}\n",
            claude.display().to_string(),
            temp.path().join("oc").display().to_string(),
        ),
    );

    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(
        &out,
        "a sync whose openclaw archive scan cannot read one agent",
    );
    let doc = json(&args, &out);
    assert_eq!(doc["outcome"].as_str(), Some("ok"), "{doc}");
    assert_eq!(
        doc["sessions_inserted"].as_u64(),
        Some(1),
        "the healthy adapter's committed session must survive: {doc}"
    );
}

/// `skipped_unimportable` is attached to the real summary document, not just
/// formatted correctly in isolation: the unit test drives the helper directly,
/// so deleting the one call site that reaches the document leaves it green.
#[test]
fn contract_excluded_sessions_surface_as_a_benign_json_count() {
    let temp = TempDir::new().expect("temp");
    let root = temp.path().join("nanoclaw");
    let v2_db = root.join("data").join("v2.db");
    std::fs::create_dir_all(v2_db.parent().expect("data dir")).expect("data dir");
    let conn = rusqlite::Connection::open(&v2_db).expect("open v2.db");
    conn.execute_batch(
        "CREATE TABLE agent_groups (id TEXT PRIMARY KEY, name TEXT NOT NULL, folder TEXT NOT NULL, agent_provider TEXT, created_at TEXT NOT NULL);
         CREATE TABLE messaging_groups (id TEXT PRIMARY KEY, channel_type TEXT NOT NULL, platform_id TEXT NOT NULL, instance TEXT NOT NULL, name TEXT, is_group INTEGER DEFAULT 0, created_at TEXT NOT NULL);
         CREATE TABLE container_configs (agent_group_id TEXT PRIMARY KEY, provider TEXT, model TEXT, assistant_name TEXT, updated_at TEXT NOT NULL);
         CREATE TABLE sessions (id TEXT PRIMARY KEY, agent_group_id TEXT NOT NULL, messaging_group_id TEXT, thread_id TEXT, agent_provider TEXT, status TEXT, created_at TEXT NOT NULL);
         INSERT INTO agent_groups VALUES ('ag-codex', 'Codex Group', 'codex', 'codex', '2026-04-01T00:00:00Z');
         INSERT INTO container_configs VALUES ('ag-codex', 'codex', 'gpt-5', 'Cod', '2026-04-01T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-codex-a', 'ag-codex', 'mg', 'thread', 'codex', 'active', '2026-04-27T00:00:00Z');
         INSERT INTO sessions VALUES ('sess-codex-b', 'ag-codex', 'mg', 'thread', 'codex', 'active', '2026-04-27T00:00:00Z');",
    )
    .expect("seed v2.db");
    drop(conn);
    write_config(
        &temp,
        &format!(
            "[adapters.nanoclaw]\nenabled = true\npath = {:?}\n",
            root.display().to_string(),
        ),
    );

    let args = ["sync", "--format", "json"];
    let out = run(&temp, &args);
    assert_exit_ok(&out, "a sync whose only sessions are contract-excluded");
    let doc = json(&args, &out);
    assert_eq!(doc["skipped_unimportable"].as_u64(), Some(2), "{doc}");
    // A documented non-ingest is not a health claim: it must not reach the
    // degraded array, and it must not print a warning.
    assert!(doc.get("degraded_adapters").is_none(), "{doc}");
    assert!(!stderr(&out).contains("could not read"), "{}", stderr(&out));
}

/// Pins the observable behavior, not the guard: since the all-absent CLI sync
/// short-circuits before the import stage, the notice is unreachable here by
/// construction. The guard itself now has only one live caller
/// (`serve --with-sync`), noted at its site in `main.rs`.
#[test]
fn an_all_absent_host_does_not_repeat_the_first_sync_notice() {
    let temp = TempDir::new().expect("temp");
    let missing = absent_source(&temp);
    write_config(
        &temp,
        &format!(
            "[adapters.codex-cli]\nenabled = true\npath = {:?}\n\n\
             [adapters.openclaw]\nenabled = true\npath = {:?}\n",
            missing.display().to_string(),
            missing.display().to_string(),
        ),
    );
    assert_exit_ok(&run(&temp, &["sync"]), "the first all-absent sync");
    let second = run(&temp, &["sync"]);
    assert_exit_ok(&second, "the second all-absent sync");
    assert!(
        !stderr(&second).contains("first sync from this host"),
        "an all-absent host never starts reading history: {}",
        stderr(&second),
    );
}
