#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Stdio;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use pond::sessions::Store;

use crate::support::{ChildGuard, sandboxed_pond};

const CLAUDE_CODE_FIXTURE: &str =
    "tests/fixtures/adapter/claude_code/projects/-Users-user-Projects-myproject-a";

fn copy_one_session(root: &Path) {
    let project = root.join("project");
    std::fs::create_dir_all(&project).expect("source project");
    let fixture = std::fs::read_dir(CLAUDE_CODE_FIXTURE)
        .expect("fixture directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "jsonl")
        })
        .expect("fixture session");
    std::fs::copy(
        &fixture,
        project.join(fixture.file_name().expect("fixture file name")),
    )
    .expect("copy fixture session");
}

fn write_config(temp: &TempDir, source: &Path) {
    let config_dir = temp.path().join("config").join("pond");
    std::fs::create_dir_all(&config_dir).expect("config directory");
    std::fs::write(
        config_dir.join("config.toml"),
        format!(
            "[storage]\npath = {:?}\n\n[adapters.claude-code]\nenabled = true\npath = {:?}\n",
            temp.path().join("store").display().to_string(),
            source.display().to_string(),
        ),
    )
    .expect("write config");
}

fn cursor_store_key(temp: &TempDir) -> String {
    std::fs::read_dir(state_dir(temp))
        .expect("state directory")
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .find_map(|name| {
            name.strip_prefix("sync-cursor-")
                .and_then(|name| name.strip_suffix(".json"))
                .map(str::to_owned)
        })
        .expect("sync cursor")
}

fn state_dir(temp: &TempDir) -> std::path::PathBuf {
    if cfg!(windows) {
        temp.path().join("state")
    } else {
        temp.path().join("state").join("pond")
    }
}

fn run_one_serve_sync(temp: &TempDir) -> String {
    let mut command = sandboxed_pond(temp);
    command
        .args([
            "serve",
            "--transport",
            "stdio",
            "--with-sync",
            "--sync-every",
            "60",
        ])
        .env("RUST_LOG", "pond::sync=info");
    let child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start serve");
    let mut child = ChildGuard(child);
    let stderr = child.0.stderr.take().expect("serve stderr");
    let (lines_tx, lines_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if lines_tx.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut logs = Vec::new();
    while Instant::now() < deadline {
        if let Some(status) = child.0.try_wait().expect("poll serve") {
            panic!(
                "serve exited before its first sync ({status}):\n{}",
                logs.join("\n")
            );
        }
        if let Ok(line) = lines_rx.recv_timeout(Duration::from_millis(200)) {
            let complete = line.contains("in-serve sync complete");
            logs.push(line);
            if complete {
                break;
            }
        }
    }

    child.0.kill().expect("stop serve");
    child.0.wait().expect("wait for serve");
    reader.join().expect("stderr reader");
    logs.join("\n")
}

#[test]
fn serve_restart_uses_persisted_cursor_when_rowmap_is_busy() {
    let temp = TempDir::new().expect("temp");
    let source = temp.path().join("source");
    copy_one_session(&source);
    write_config(&temp, &source);

    let initial = sandboxed_pond(&temp)
        .arg("sync")
        .output()
        .expect("run initial sync");
    assert!(
        initial.status.success(),
        "initial sync failed:\n{}",
        String::from_utf8_lossy(&initial.stderr),
    );

    let store_key = cursor_store_key(&temp);
    let cursor_path = state_dir(&temp).join(format!("sync-cursor-{store_key}.json"));
    std::fs::remove_file(&cursor_path).expect("remove cursor seeded by oneshot sync");
    let first_logs = run_one_serve_sync(&temp);
    assert!(
        first_logs.contains("in-serve sync complete"),
        "the first serve process must complete a sync:\n{first_logs}",
    );
    assert!(
        cursor_path.is_file(),
        "the first serve process must persist its cursor"
    );

    let cache = temp.path().join("cache").join("pond");
    if cache.exists() {
        std::fs::remove_dir_all(&cache).expect("remove rowmap cache");
    }
    std::fs::create_dir_all(&cache).expect("cache directory");
    let rowmap_lock =
        File::create(cache.join(format!("rowmetamap-{store_key}.lock"))).expect("rowmap lock file");
    rowmap_lock.lock().expect("hold rowmap build lock");

    // A no-op sync proves only that nothing was re-inserted, which an idempotent
    // full re-read also achieves. The preview resolves the same oracle and
    // reports the gate's verdict, so it is what proves the session was SKIPPED.
    let preview = sandboxed_pond(&temp)
        .args(["sync", "--dry-run"])
        .output()
        .expect("preview with the rowmap busy");
    let preview_out = String::from_utf8_lossy(&preview.stdout);
    assert!(
        preview.status.success() && preview_out.contains("up to date"),
        "the persisted cursor must mark the ingested session fresh:\n{preview_out}{}",
        String::from_utf8_lossy(&preview.stderr),
    );

    let logs = run_one_serve_sync(&temp);
    assert!(
        logs.contains("in-serve sync complete")
            && logs.contains("sessions=0")
            && logs.contains("messages=0"),
        "the restarted serve must complete an incremental no-op sync:\n{logs}",
    );
    assert!(
        !logs.contains("first sync from this host"),
        "the persisted cursor must prevent a full-read plan:\n{logs}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuilt_store_rejects_and_replaces_the_old_cursor() {
    let temp = TempDir::new().expect("temp");
    let source = temp.path().join("source");
    copy_one_session(&source);
    write_config(&temp, &source);

    let initial = sandboxed_pond(&temp)
        .arg("sync")
        .output()
        .expect("run initial sync");
    assert!(initial.status.success());

    let store_key = cursor_store_key(&temp);
    let cursor_path = state_dir(&temp).join(format!("sync-cursor-{store_key}.json"));
    std::fs::remove_dir_all(temp.path().join("store")).expect("replace store");
    let cache = temp.path().join("cache").join("pond");
    std::fs::create_dir_all(&cache).expect("cache directory");
    let rowmap_lock =
        File::create(cache.join(format!("rowmetamap-{store_key}.lock"))).expect("rowmap lock file");
    rowmap_lock.lock().expect("hold rowmap build lock");

    let rebuilt = sandboxed_pond(&temp)
        .arg("sync")
        .output()
        .expect("sync rebuilt store");
    assert!(
        rebuilt.status.success(),
        "rebuilt sync failed:\n{}",
        String::from_utf8_lossy(&rebuilt.stderr),
    );
    assert!(
        String::from_utf8_lossy(&rebuilt.stderr).contains("first sync from this host"),
        "the old cursor must not mark the replacement store fresh",
    );
    assert!(
        !cursor_path.exists(),
        "the rejected cursor stays absent until a current rowmap is available",
    );
    let store = Store::open_local(temp.path().join("store"))
        .await
        .expect("open rebuilt store");
    let (sessions, messages, _) = store.row_counts().await.expect("rebuilt row counts");
    assert!(sessions > 0 && messages > 0, "the source must be restored");
    drop(store);
    drop(rowmap_lock);

    let reseed = sandboxed_pond(&temp)
        .arg("sync")
        .output()
        .expect("reseed cursor");
    assert!(reseed.status.success());
    assert!(
        cursor_path.is_file(),
        "the replacement store gets a new cursor"
    );

    if cache.exists() {
        std::fs::remove_dir_all(&cache).expect("remove rowmap cache");
    }
    std::fs::create_dir_all(&cache).expect("cache directory");
    let rowmap_lock =
        File::create(cache.join(format!("rowmetamap-{store_key}.lock"))).expect("rowmap lock file");
    rowmap_lock.lock().expect("hold rowmap build lock");
    let resumed = sandboxed_pond(&temp)
        .arg("sync")
        .output()
        .expect("resume with replacement cursor");
    assert!(resumed.status.success());
    assert!(
        !String::from_utf8_lossy(&resumed.stderr).contains("first sync from this host"),
        "the replacement cursor must resume incrementally",
    );
}
