//! `pond resume` end to end: the verb the pi extension shells out to.
//!
//! Driven through the real binary rather than the library, because the
//! contract this suite protects IS the CLI one - the exit codes and the
//! `--format json` document a plugin branches on (spec.md 7.8). The store is
//! populated in-process first, then the binary is pointed at it with
//! `--storage-path`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use assert_cmd::Command;
use pond::{
    adapter::{NoopOracle, PiCodingAgentAdapter},
    handlers::ingest_adapter,
    sessions::Store,
};
use serde_json::Value;
use std::path::Path;
use tempfile::TempDir;

const PI_FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/pi-coding-agent/sessions"
);
const CLAUDE_FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/claude_code/projects"
);

/// A pi-origin v4 session that also has a child session in the same corpus, so
/// one store covers native fidelity and lineage.
const PI_SESSION: &str = "v4-main-session";
const PI_CHILD: &str = "v4-fork-session";

struct Sandbox {
    temp: TempDir,
}

impl Sandbox {
    /// A store holding one adapter's fixture corpus, plus the isolated HOME the
    /// binary runs under.
    async fn with(adapter: &dyn pond::adapter::Adapter) -> Self {
        let sandbox = Self {
            temp: TempDir::new().expect("temp dir"),
        };
        let store = Store::open_local(sandbox.store_path())
            .await
            .expect("open store");
        ingest_adapter(&store, adapter, &NoopOracle, |_| {})
            .await
            .expect("ingest fixtures");
        sandbox
    }

    async fn with_pi_corpus() -> Self {
        Self::with(&PiCodingAgentAdapter::new(PI_FIXTURES)).await
    }

    async fn with_claude_corpus() -> Self {
        Self::with(&pond::adapter::ClaudeCodeAdapter::new(CLAUDE_FIXTURES)).await
    }

    fn store_path(&self) -> std::path::PathBuf {
        self.temp.path().join("store")
    }

    fn out_dir(&self) -> std::path::PathBuf {
        self.temp.path().join("pi-home")
    }

    /// Run `pond resume ...` against this sandbox's store, in a sandboxed HOME
    /// so no real config or data dir is consulted. Returns (exit code, stdout).
    fn resume(&self, args: &[&str]) -> (i32, String) {
        let out = Command::new(env!("CARGO_BIN_EXE_pond"))
            .arg("resume")
            .args(args)
            .arg("--storage-path")
            .arg(self.store_path())
            .env("HOME", self.temp.path().join("home"))
            .env("XDG_DATA_HOME", self.temp.path().join("data"))
            .env("XDG_CONFIG_HOME", self.temp.path().join("config"))
            .env("XDG_CACHE_HOME", self.temp.path().join("cache"))
            .env_remove("POND_STORAGE_PATH")
            .env_remove("RUST_LOG")
            .env("NO_COLOR", "1")
            .output()
            .expect("run pond resume");
        (
            out.status.code().expect("pond exited with a code"),
            String::from_utf8(out.stdout).expect("stdout is utf-8"),
        )
    }

    fn resume_json(&self, args: &[&str]) -> (i32, Value) {
        let (code, stdout) = self.resume(args);
        let doc = serde_json::from_str(&stdout)
            .unwrap_or_else(|error| panic!("stdout is not JSON ({error}): {stdout}"));
        (code, doc)
    }
}

fn files_of(doc: &Value, session_id: &str) -> Vec<String> {
    doc["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .find(|entry| entry["session_id"] == session_id)
        .unwrap_or_else(|| panic!("no entry for {session_id} in {doc}"))["files"]
        .as_array()
        .expect("files array")
        .iter()
        .map(|value| value.as_str().expect("file path is a string").to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn native_resume_writes_a_pi_session_file_for_the_whole_lineage() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let out_dir = sandbox.out_dir();
    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 0, "a resumable session exits clean: {doc}");
    assert_eq!(doc["adapter"], "pi-coding-agent");

    // spec.md#adapter-lineage-complete-restore: the parent and its child come
    // back together, or not at all.
    let ids: Vec<&str> = doc["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .map(|entry| entry["session_id"].as_str().expect("session id"))
        .collect();
    assert!(
        ids.contains(&PI_SESSION) && ids.contains(&PI_CHILD),
        "got {ids:?}"
    );

    // spec.md#adapter-native-restore-lossless: same origin, so the system
    // decides native - the caller never asked for a fidelity.
    for entry in doc["sessions"].as_array().expect("sessions array") {
        assert_eq!(entry["actual_fidelity"], "native", "{entry}");
    }

    for path in files_of(&doc, PI_SESSION) {
        let file = Path::new(&path);
        assert!(file.is_file(), "{path} was reported but not written");
        assert!(
            file.starts_with(&out_dir),
            "{path} escaped --out-dir {}",
            out_dir.display(),
        );
        // The pi adapter's own layout, so pi's session list finds it.
        assert!(
            path.contains("/sessions/--"),
            "{path} is not in pi's layout"
        );
        let head: Value = serde_json::from_str(
            std::fs::read_to_string(file)
                .expect("read resumed file")
                .lines()
                .next()
                .expect("resumed file has a header"),
        )
        .expect("header parses");
        assert_eq!(head["kind"], "header");
        assert_eq!(head["version"], 4, "a v4-origin session resumes as v4");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_twice_is_refused_and_names_what_is_already_there() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let out_dir = sandbox.out_dir();
    let args = [
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ];
    let (first, doc) = sandbox.resume_json(&args);
    assert_eq!(first, 0);
    let written = files_of(&doc, PI_SESSION);

    let (second, refusal) = sandbox.resume_json(&args);
    assert_eq!(
        second, 3,
        "a collision has its own exit code, distinct from not-found",
    );
    assert_eq!(refusal["error"], "already_exists");
    let named: Vec<String> = refusal["existing"]
        .as_array()
        .expect("existing array")
        .iter()
        .map(|value| value.as_str().expect("path").to_owned())
        .collect();
    // The extension turns this into "already resumed - just open that file",
    // so the path has to be exactly the one already on disk.
    assert!(
        written.iter().all(|path| named.contains(path)),
        "wrote {written:?} but the refusal named {named:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_session_resumes_into_pi_as_a_readable_v3_transcript() {
    let sandbox = Sandbox::with_claude_corpus().await;
    let store = Store::open_local(sandbox.store_path())
        .await
        .expect("open store");
    let session_id = store
        .session_ids()
        .await
        .expect("list sessions")
        .into_iter()
        .next()
        .expect("claude fixtures have sessions");

    let out_dir = sandbox.out_dir();
    let (code, doc) = sandbox.resume_json(&[
        &session_id,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 0, "{doc}");
    let entry = &doc["sessions"][0];
    assert_eq!(
        entry["actual_fidelity"], "foreign",
        "a claude-code session cannot be replayed value-complete into pi",
    );
    assert_eq!(entry["source_agent"], "claude-code");

    let path = files_of(&doc, &session_id)
        .into_iter()
        .next()
        .expect("one file per session");
    let text = std::fs::read_to_string(&path).expect("read resumed file");
    let head: Value = serde_json::from_str(text.lines().next().expect("header")).expect("json");
    // Foreign restore targets v3 on purpose: it is what every shipped pi loads.
    assert_eq!(head["type"], "session");
    assert_eq!(head["version"], 3);
    assert!(
        text.lines()
            .skip(1)
            .any(|line| line.contains("\"role\":\"user\"")),
        "the reconstruction carries real conversation",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unstored_session_reports_not_found_and_teaches_the_next_step() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let out_dir = sandbox.out_dir();
    let (code, doc) = sandbox.resume_json(&[
        "no-such-session",
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 1);
    assert_eq!(doc["error"], "not_found");

    let (text_code, stdout) = sandbox.resume(&[
        "no-such-session",
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
    ]);
    assert_eq!(text_code, 1);
    assert!(
        stdout.is_empty(),
        "an error must not land on stdout: {stdout}"
    );
    assert!(!out_dir.exists(), "a failed resume writes nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_client_lists_the_ones_that_exist() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "pi",
        "--out-dir",
        sandbox.out_dir().to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 2, "an unanswerable request is not a not-found");
    assert_eq!(doc["error"], "unknown_adapter");
    let known: Vec<&str> = doc["known"]
        .as_array()
        .expect("known array")
        .iter()
        .map(|value| value.as_str().expect("adapter name"))
        .collect();
    assert!(known.contains(&"pi-coding-agent"), "got {known:?}");
}
