//! An adapter that cannot read part of its source reports that, on every
//! surface an operator checks.
//!
//! The failure this pins: pond probed OpenClaw's agent database for one table
//! name, did not find it on any 2026.8.1+ host, and reported "0 sessions"
//! through `pond status` and `sync --dry-run` while the database held eight.
//! `session-movement-complete` calls that a skip that outruns durability, and
//! the adapter now returns a typed error instead of a count.
//!
//! These are CLI tests on purpose. The adapter-level guarantee is covered in
//! `adapter/openclaw.rs`, but the whole point of the change is what a person
//! SEES, and that lives in two rendering paths in `main.rs` that no test
//! touched - the exact thing a later refactor would quietly undo while every
//! adapter test still passed.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use assert_cmd::Command;
use serde_json::Value;
use tempfile::TempDir;

/// A state root whose agent DB carries transcript data under a schema no
/// reader claims: `transcript_events` with neither the v1 nor the v2 session
/// tables beside it. Shaped like a future OpenClaw rename, which is the case
/// that must never again read as "this host has nothing".
fn unreadable_openclaw_root(temp: &TempDir) -> std::path::PathBuf {
    let root = temp.path().join("openclaw");
    let db = root.join("agents").join("main").join("agent");
    std::fs::create_dir_all(&db).expect("agent dir");
    let conn = rusqlite::Connection::open(db.join("openclaw-agent.sqlite")).expect("open db");
    // Idempotent: a test that runs two commands builds this twice.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS transcript_events (
           session_id TEXT NOT NULL,
           seq INTEGER NOT NULL,
           event_json TEXT NOT NULL,
           created_at INTEGER NOT NULL,
           PRIMARY KEY (session_id, seq)
         ) STRICT;
         CREATE TABLE IF NOT EXISTS schema_meta (
           meta_key TEXT NOT NULL PRIMARY KEY,
           schema_version INTEGER NOT NULL,
           app_version TEXT
         ) STRICT;
         INSERT OR REPLACE INTO schema_meta VALUES ('primary', 999, '2099.1.1');",
    )
    .expect("write schema");
    root
}

fn run(temp: &TempDir, args: &[&str]) -> Value {
    let home = temp.path().join("home");
    let config_dir = temp.path().join("config").join("pond");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    let root = unreadable_openclaw_root(temp);
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "[storage]\npath = {:?}\n\n[adapters.openclaw]\nenabled = true\npath = {:?}\n",
            temp.path().join("store").display().to_string(),
            root.display().to_string(),
        ),
    )
    .expect("write config");

    let out = Command::new(env!("CARGO_BIN_EXE_pond"))
        .args(args)
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env_remove("RUST_LOG")
        .env("NO_COLOR", "1")
        .output()
        .expect("run pond");
    serde_json::from_slice(&out.stdout).unwrap_or_else(|error| {
        panic!(
            "{args:?} stdout was not one JSON document ({error}): {}",
            String::from_utf8_lossy(&out.stdout)
        )
    })
}

/// A count is a claim. When the source could not be read, the honest answer is
/// null plus a reason - never 0, which reads as "checked, and there is nothing
/// here".
#[test]
fn dry_run_json_reports_null_sessions_and_a_reason() {
    let temp = TempDir::new().expect("temp");
    let doc = run(&temp, &["sync", "--dry-run", "--format", "json"]);
    let adapter = &doc["adapters"][0];

    assert!(
        adapter["sessions"].is_null(),
        "an unreadable source must not report a session count: {adapter}"
    );
    let error = adapter["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no error explaining the null count: {adapter}"));
    // The operator has to be able to act on it: which tables were found, and
    // what version claimed them.
    for expected in ["transcript_events", "999", "2099.1.1"] {
        assert!(
            error.contains(expected),
            "the reason must name what it found; missing {expected:?} in {error:?}"
        );
    }
}

/// `pond status` is the surface people check first, and it used to answer this
/// case with "source path unreadable - check [adapters.openclaw] in config".
/// The path is fine and the config is correct; the schema is unknown and the
/// fix is to upgrade. A wrong diagnosis costs more than none.
#[test]
fn status_json_carries_the_same_reason_as_dry_run() {
    let temp = TempDir::new().expect("temp");
    // `status` reports `local: null` until a store exists, so the adapter rows
    // it renders are only reachable after one sync. The sync itself surfaces
    // the same schema error through its event stream - this test is about what
    // `status` says afterwards.
    let _ = run(&temp, &["sync", "--format", "json"]);
    let doc = run(&temp, &["status", "--format", "json"]);
    let adapter = doc["local"]["adapters"]
        .as_array()
        .and_then(|rows| rows.first())
        .unwrap_or_else(|| panic!("no adapter row in status: {doc}"));

    assert!(
        adapter["sessions"].is_null(),
        "status must not report a count for a source it could not read: {adapter}"
    );
    assert!(
        adapter["error"]
            .as_str()
            .is_some_and(|error| error.contains("transcript_events")),
        "status must explain the null count the same way dry-run does: {adapter}"
    );
}
