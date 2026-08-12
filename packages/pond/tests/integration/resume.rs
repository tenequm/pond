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
use walkdir::WalkDir;

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
/// A v3-origin session in the same corpus: the format every released pi writes,
/// and the one case where restoring onto the source file is correct.
const PI_V3_SESSION: &str = "019dd55d-99a4-7344-aa11-d1d71d2c80fb";

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
        sandbox.ingest(adapter).await;
        sandbox
    }

    async fn ingest(&self, adapter: &dyn pond::adapter::Adapter) {
        let store = Store::open_local(self.store_path())
            .await
            .expect("open store");
        ingest_adapter(&store, adapter, &NoopOracle, |_| {})
            .await
            .expect("ingest fixtures");
    }

    async fn with_pi_corpus() -> Self {
        Self::with(&PiCodingAgentAdapter::new(PI_FIXTURES)).await
    }

    /// The pi corpus copied INTO the sandbox and ingested from there, so a test
    /// can resume back into the very directory the sessions were captured from -
    /// what every user with a local pi install does.
    async fn with_pi_corpus_in_place() -> Self {
        let sandbox = Self {
            temp: TempDir::new().expect("temp dir"),
        };
        let sessions = sandbox.pi_agent_dir().join("sessions");
        copy_tree(Path::new(PI_FIXTURES), &sessions);
        sandbox.ingest(&PiCodingAgentAdapter::new(&sessions)).await;
        sandbox
    }

    /// The restore root a real pi install would be resumed into: the parent of
    /// `sessions/`, i.e. `~/.pi/agent`.
    fn pi_agent_dir(&self) -> std::path::PathBuf {
        self.out_dir_at("pi-agent")
    }

    async fn with_claude_corpus() -> Self {
        Self::with(&pond::adapter::ClaudeCodeAdapter::new(CLAUDE_FIXTURES)).await
    }

    fn store_path(&self) -> std::path::PathBuf {
        self.temp.path().join("store")
    }

    fn out_dir(&self) -> std::path::PathBuf {
        self.out_dir_at("pi-home")
    }

    /// A second restore root, for tests that need one destination tree seeded
    /// differently from another.
    fn out_dir_at(&self, name: &str) -> std::path::PathBuf {
        self.temp.path().join(name)
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

    /// The lowest-id session whose `source_agent` is exactly `claude-code`.
    /// `session_ids` is an unordered scan and the claude corpus also holds
    /// subagent and fork sessions (`claude-code/...`), so "the first id" is not
    /// a stable way to name a main session.
    async fn a_claude_code_session(&self) -> String {
        let store = Store::open_local(self.store_path())
            .await
            .expect("open store");
        let mut ids: Vec<String> = Vec::new();
        for id in store.session_ids().await.expect("list sessions") {
            let session = store.get_session(&id).await.expect("read session");
            if session.is_some_and(|found| found.session.source_agent == "claude-code") {
                ids.push(id);
            }
        }
        ids.sort();
        ids.into_iter()
            .next()
            .expect("claude fixtures have a main claude-code session")
    }

    fn resume_json(&self, args: &[&str]) -> (i32, Value) {
        let (code, stdout) = self.resume(args);
        let doc = serde_json::from_str(&stdout)
            .unwrap_or_else(|error| panic!("stdout is not JSON ({error}): {stdout}"));
        (code, doc)
    }
}

fn copy_tree(from: &Path, to: &std::path::Path) {
    for entry in WalkDir::new(from) {
        let entry = entry.expect("walk source dir");
        let dest = to.join(entry.path().strip_prefix(from).expect("entry under source"));
        if entry.file_type().is_dir() {
            std::fs::create_dir_all(&dest).expect("create destination");
        } else {
            std::fs::copy(entry.path(), &dest).expect("copy fixture");
        }
    }
}

/// Every `.jsonl` under a root with its bytes, so a test can prove a restore
/// left the files it found exactly as they were.
fn jsonl_snapshot(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_file()
                && entry.path().extension().and_then(|ext| ext.to_str()) == Some("jsonl")
        })
        .map(|entry| {
            let path = entry.into_path();
            let bytes = std::fs::read(&path).expect("read jsonl");
            (path, bytes)
        })
        .collect()
}

fn refusal_paths(doc: &Value) -> Vec<String> {
    doc["existing"]
        .as_array()
        .expect("existing array")
        .iter()
        .map(|value| value.as_str().expect("path").to_owned())
        .collect()
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

    // spec.md#adapter-native-restore-lossless: fidelity is the system's call and
    // it is reported honestly. These fixtures are v4-origin, and resume emits
    // the v3 pi can actually open, so a value-complete replay is impossible -
    // "foreign" is the truthful answer, not a bug.
    for entry in doc["sessions"].as_array().expect("sessions array") {
        assert_eq!(entry["actual_fidelity"], "foreign", "{entry}");
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
        // v3, not the v4 it was captured from: a byte-faithful file pi refuses
        // to load is not a restore (pi 0.84.1 rejects v4 outright).
        assert_eq!(head["type"], "session");
        assert_eq!(head["version"], 3, "resume emits the format pi can open");
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
    let named = refusal_paths(&refusal);
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
    let session_id = sandbox.a_claude_code_session().await;

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
    let entry = doc["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .find(|entry| entry["session_id"] == session_id.as_str())
        .unwrap_or_else(|| panic!("no entry for {session_id} in {doc}"));
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

/// An ingest-only client is an unanswerable request, not a failed write: the
/// capability is asked before any planning, so the caller gets exit 2 and a
/// document naming the alternative rather than a serialize error surfacing as
/// the generic exit 1 (which collides with "session is not stored").
#[tokio::test(flavor = "multi_thread")]
async fn an_ingest_only_client_refuses_with_the_alternative() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let out_dir = sandbox.out_dir();
    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "oh-my-pi",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);

    assert_eq!(code, 2, "ingest-only is unanswerable, not a not-found");
    assert_eq!(doc["error"], "restore_unsupported");
    assert_eq!(doc["adapter"], "oh-my-pi");
    let reason = doc["reason"]
        .as_str()
        .expect("reason names the alternative");
    assert!(reason.contains("pi-coding-agent"), "got {reason}");
    assert!(!out_dir.exists(), "a refused resume writes nothing");
}

/// The relative layout `--out-dir` gets filled with, learned by restoring once
/// into a throwaway root - the adapter owns that layout, so a test must not
/// hard-code it to seed a destination.
fn probe_relative_destination(sandbox: &Sandbox) -> std::path::PathBuf {
    let probe = sandbox.out_dir_at("probe");
    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        probe.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 0, "{doc}");
    Path::new(&files_of(&doc, PI_SESSION)[0])
        .strip_prefix(&probe)
        .expect("a restored file is under --out-dir")
        .to_path_buf()
}

/// An io failure is an outcome of `resume`, not a crash: it keeps the contract
/// that `--format json` answers on stdout, and it is its own exit code because
/// - unlike a collision - there is nothing on disk to open.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_reports_itself_as_json_and_leaves_nothing_behind() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let relative = probe_relative_destination(&sandbox);

    let out_dir = sandbox.out_dir();
    // A regular file where the restore needs a directory: an io failure every
    // user hits the same way, root included.
    let blocked = out_dir.join(relative.parent().expect("the layout nests the file"));
    std::fs::create_dir_all(blocked.parent().expect("nested layout")).expect("seed dirs");
    std::fs::write(&blocked, b"in the way").expect("seed blocker");

    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 4, "a write failure is not a collision: {doc}");
    assert_eq!(doc["error"], "write_failed");
    assert_eq!(doc["session_id"], PI_SESSION);
    assert_eq!(doc["out_dir"], out_dir.display().to_string());
    assert!(
        !doc["detail"]
            .as_str()
            .expect("detail is a string")
            .is_empty(),
        "the document carries the io detail: {doc}",
    );
    assert!(
        !out_dir.join(&relative).exists(),
        "the failed write left a file behind",
    );
    assert_eq!(
        std::fs::read(&blocked).expect("blocker survives"),
        b"in the way",
        "resume touched a path it did not create",
    );
}

/// A DANGLING symlink reads as absent to `Path::exists`, so a naive pre-check
/// waves it through and the writer's `O_EXCL` refuses it - turning a genuine
/// "already resumed" into a write failure. The gate is symlink-aware.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_dangling_symlink_at_a_destination_reports_already_exists() {
    let sandbox = Sandbox::with_pi_corpus().await;
    let relative = probe_relative_destination(&sandbox);

    let out_dir = sandbox.out_dir();
    let dest = out_dir.join(&relative);
    std::fs::create_dir_all(dest.parent().expect("nested layout")).expect("seed dirs");
    let outside = sandbox.out_dir_at("outside-the-root.jsonl");
    std::os::unix::fs::symlink(&outside, &dest).expect("dangling symlink");

    let (code, doc) = sandbox.resume_json(&[
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(code, 3, "something occupies the destination: {doc}");
    assert_eq!(doc["error"], "already_exists");
    let named = refusal_paths(&doc);
    assert!(
        named.contains(&dest.display().to_string()),
        "the refusal must name the occupied path: {named:?}",
    );
    assert!(
        !outside.exists(),
        "nothing may be written through the link, outside --out-dir",
    );
}

/// The case the distinct-reconstruction rule exists for
/// (spec.md#adapter-restore-distinct-reconstruction): a session captured from a
/// local pi, resumed back into the directory it was captured from. Its v4 source
/// file is one pi cannot open, so the v3 reconstruction has to be a file of its
/// own - naming it after the source turned every such resume into an
/// "already exists" refusal that sent the extension to the unloadable transcript.
#[tokio::test(flavor = "multi_thread")]
async fn resuming_into_the_capture_directory_writes_a_v3_file_beside_the_v4_source() {
    let sandbox = Sandbox::with_pi_corpus_in_place().await;
    let out_dir = sandbox.pi_agent_dir();
    let before = jsonl_snapshot(&out_dir);
    assert!(!before.is_empty(), "the corpus is inside the sandbox");

    let args = [
        PI_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ];
    let (code, doc) = sandbox.resume_json(&args);
    assert_eq!(
        code, 0,
        "resuming into the capture directory must not collide with the source: {doc}",
    );

    let written = files_of(&doc, PI_SESSION);
    let path = Path::new(written.first().expect("one file per session"));
    let tail = format!("_{PI_SESSION}.jsonl");
    assert!(
        !before.contains_key(path),
        "the reconstruction claimed the v4 source file's own name: {}",
        path.display(),
    );
    // The pi extension resolves which file to open by this suffix.
    assert!(
        path.to_str().unwrap().ends_with(&tail),
        "{} does not end with {tail}",
        path.display(),
    );
    let source = before
        .keys()
        .find(|existing| existing.to_str().unwrap().ends_with(&tail))
        .expect("the v4 source file is in the copied corpus");
    assert_eq!(
        path.parent(),
        source.parent(),
        "the reconstruction sits in the session's own project-slug directory",
    );
    let head: Value = serde_json::from_str(
        std::fs::read_to_string(path)
            .expect("read the reconstruction")
            .lines()
            .next()
            .expect("it has a header"),
    )
    .expect("header parses");
    assert_eq!(head["type"], "session");
    assert_eq!(head["version"], 3, "written in the format pi can open");
    for (existing, bytes) in &before {
        assert_eq!(
            &std::fs::read(existing).expect("a source file survives"),
            bytes,
            "{} was rewritten by the restore",
            existing.display(),
        );
    }

    // Idempotence: the name is derived from canonical, so the second attempt
    // refuses against the v3 file the first one wrote - the answer the extension
    // turns into "already resumed, open it".
    let (second, refusal) = sandbox.resume_json(&args);
    assert_eq!(second, 3, "a second resume is a collision: {refusal}");
    assert_eq!(refusal["error"], "already_exists");
    let named = refusal_paths(&refusal);
    assert!(
        named.contains(&path.display().to_string()),
        "the refusal names the v3 file: {named:?}",
    );
    assert!(
        !named
            .iter()
            .any(|existing| before.contains_key(Path::new(existing))),
        "a refusal must never point at a v4 source file pi cannot open: {named:?}",
    );
}

/// The mirror case, which must NOT change: a v3-origin session replays onto its
/// own source file, so resuming it into its capture directory is genuinely
/// "already resumed - open the file you have".
#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_v3_origin_session_into_its_capture_directory_is_already_resumed() {
    let sandbox = Sandbox::with_pi_corpus_in_place().await;
    let out_dir = sandbox.pi_agent_dir();
    let before = jsonl_snapshot(&out_dir);

    let (code, doc) = sandbox.resume_json(&[
        PI_V3_SESSION,
        "--to",
        "pi-coding-agent",
        "--out-dir",
        out_dir.to_str().unwrap(),
        "--format",
        "json",
    ]);
    assert_eq!(
        code, 3,
        "a v3 origin collides with its own transcript: {doc}"
    );
    assert_eq!(doc["error"], "already_exists");
    let source = before
        .keys()
        .find(|existing| {
            existing
                .to_str()
                .unwrap()
                .ends_with(&format!("_{PI_V3_SESSION}.jsonl"))
        })
        .expect("the v3 source file is in the copied corpus");
    assert_eq!(
        refusal_paths(&doc),
        vec![source.display().to_string()],
        "the refusal names the session's own source file, which pi can open",
    );
}
