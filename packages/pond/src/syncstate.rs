//! Per-host sync coordination: the single-flight lock that keeps a manual
//! `pond sync` and the scheduled one from running concurrently against the
//! same store, the persisted freshness cursor, and the last-sync record that
//! `pond status` reports. Local-process coordination only - cross-host writers
//! stay pure OCC on the Lance store; nothing here ever touches store bytes.
//! Bin-only module: every artifact here is CLI-surface state.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Set once from the global `--state-dir` flag, before anything resolves a
/// state path. It is the argument form of `XDG_STATE_HOME`, and exists because
/// a Task Scheduler `Exec` action carries no environment block: the Windows
/// scheduler bakes the registration-time state dir into the task's arguments
/// where launchd and systemd pin an env var instead.
static STATE_DIR_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();

pub(crate) fn set_state_dir_override(dir: PathBuf) {
    let _ = STATE_DIR_OVERRIDE.set(dir);
}

/// The XDG state root pinned into scheduler jobs so the scheduled sync resolves
/// the same state dir as a manual run. `--state-dir` wins, then `XDG_STATE_HOME`
/// when set and absolute; otherwise the platform-native fallback applies.
pub(crate) fn state_root() -> PathBuf {
    if let Some(pinned) = STATE_DIR_OVERRIDE.get() {
        return pinned.clone();
    }
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        return xdg;
    }
    // Windows native: %LOCALAPPDATA%\pond\state.
    // pond_state_dir() is state_root() itself on Windows (no extra \pond level;
    // there are no sibling app state dirs here).
    #[cfg(windows)]
    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
    {
        return local_app_data.join("pond").join("state");
    }
    // Unix: $HOME/.local/state
    crate::config::home_dir()
        .map(|home| home.join(".local").join("state"))
        .unwrap_or_else(|| PathBuf::from(".pond-state"))
}

/// The directory where the scheduler log, sync lock, sync cursor, and last-sync
/// record live.
///
/// On all platforms, `XDG_STATE_HOME/pond` when `XDG_STATE_HOME` is set - except
/// on Windows, where the per-app `\pond` suffix is omitted because `state_root()`
/// already points to a pond-specific dir (`%LOCALAPPDATA%\pond\state`). This also
/// keeps the scheduler-pinning contract: the task bakes `--state-dir
/// <state_root()>` into its arguments, and the scheduled job calls
/// `pond_state_dir()` which on Windows returns `state_root()` = the same value,
/// with no double suffix.
///
/// On Unix: `$HOME/.local/state/pond` (one app under the shared XDG state home).
pub(crate) fn pond_state_dir() -> PathBuf {
    #[cfg(windows)]
    {
        state_root()
    }
    #[cfg(not(windows))]
    state_root().join("pond")
}

/// Who holds the sync lock. Written into a sibling `.holder.json` file (not the
/// lock file itself, which is whole-file-locked on Windows) on acquire so a
/// blocked sibling can name what it is waiting for.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SyncLockHolder {
    pub pid: u32,
    pub started_at: DateTime<Utc>,
}

pub(crate) enum SyncLockState {
    Acquired(SyncLockGuard),
    /// Another local pond process holds the lock. Holder info is best-effort:
    /// `None` when it could not be read back (e.g. a mid-write race).
    Busy(Option<SyncLockHolder>),
}

/// Held for the whole sync run. `Drop` unlocks explicitly, and the OS drops the
/// flock again when the file closes, so a killed sync can never leave a stale
/// lock. The lock file is never unlinked - unlink while a sibling holds the path
/// open hands out two locks. `SyncLockGuard::drop` also removes the sibling
/// `.holder.json` best-effort.
pub(crate) struct SyncLockGuard {
    file: File,
    holder_path: PathBuf,
}

impl Drop for SyncLockGuard {
    fn drop(&mut self) {
        // Release explicitly instead of relying on close. `flock` belongs to the
        // open file *description*, not the descriptor: any subprocess spawned
        // while this guard was alive inherited a duplicate, so dropping our
        // `File` leaves the lock held until that child execs or exits. The
        // window is small but real - it makes a later acquire report `Busy` with
        // no holder, so a sync refuses to start (or, under `--no-wait`, silently
        // skips) because of an unrelated child process. `unlock` clears the
        // description's lock outright, however many descriptors still reference
        // it. Best-effort: an unlock failure leaves the old close-time behavior.
        //
        // Order matters: remove the holder BEFORE unlocking. While we still
        // hold the lock nobody else can have written one, so the file we delete
        // is provably ours. Unlocking first opens a window where a successor
        // acquires, writes its own holder, and we delete it - leaving the next
        // blocked process reporting `Busy(None)` about a live holder it can no
        // longer name.
        //
        // Best-effort: the file may already be absent (concurrent stop, etc.).
        let _ = std::fs::remove_file(&self.holder_path);
        let _ = self.file.unlock();
    }
}

pub(crate) fn try_acquire_sync_lock(store_key: &str) -> Result<SyncLockState> {
    try_acquire_sync_lock_in(&pond_state_dir(), store_key)
}

fn try_acquire_sync_lock_in(dir: &Path, store_key: &str) -> Result<SyncLockState> {
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let path = dir.join(format!("sync-{store_key}.lock"));
    // Holder info lives in a sibling file, not the lock file itself: Windows
    // takes a mandatory whole-file lock, so a blocked sibling cannot read
    // bytes out of the locked file to name the holder. A plain unlocked file
    // is readable on every platform.
    let holder_path = dir.join(format!("sync-{store_key}.holder.json"));
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open sync lock {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => {
            let holder = SyncLockHolder {
                pid: std::process::id(),
                started_at: Utc::now(),
            };
            // Best-effort: the lock itself is the flock, not the holder bytes.
            let _ = write_json_atomic(dir, &holder_path, &holder);
            Ok(SyncLockState::Acquired(SyncLockGuard { file, holder_path }))
        }
        Err(std::fs::TryLockError::WouldBlock) => {
            let holder = std::fs::read_to_string(&holder_path)
                .ok()
                .and_then(|text| serde_json::from_str(&text).ok());
            Ok(SyncLockState::Busy(holder))
        }
        Err(std::fs::TryLockError::Error(error)) => {
            // A filesystem without flock semantics (some network mounts) can't
            // single-flight. The lock is best-effort local coordination that
            // never touches store bytes - cross-writer safety is OCC - so
            // degrade to running unlocked instead of failing the sync.
            tracing::warn!(
                %error,
                path = %path.display(),
                "sync lock unsupported on this filesystem; proceeding without single-flight"
            );
            Ok(SyncLockState::Acquired(SyncLockGuard { file, holder_path }))
        }
    }
}

/// Outcome of the most recent `pond sync` against one store on this host.
/// Written by every sync run (success or failure) and rendered by
/// `pond status`, so a silently failing scheduled sync becomes visible.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LastSyncRecord {
    pub finished_at: DateTime<Utc>,
    pub duration_secs: f64,
    pub sessions_inserted: u64,
    pub messages_inserted: u64,
    pub outcome: SyncOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SyncOutcome {
    Ok,
    Error,
}

/// The resident row-meta map's per-session watermarks, spilled to disk so a
/// restarted sync can still skip fresh sessions while another process owns the
/// map build. The three leading fields are lineage, not payload: the store must
/// be the same one and no older than the store these watermarks were read from,
/// or a watermark could outrun what that store actually holds and silently drop
/// messages (`usable_sync_cursor`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SyncCursor {
    pub messages_version: u64,
    pub row_count: usize,
    pub oldest_messages: Vec<(u64, String)>,
    pub watermarks: BTreeMap<String, i64>,
}

impl pond::adapter::SkipOracle for SyncCursor {
    fn session_max_ts(&self, session_id: &str) -> Option<i64> {
        self.watermarks.get(session_id).copied()
    }

    fn is_empty(&self) -> bool {
        self.watermarks.is_empty()
    }
}

fn sync_cursor_path(dir: &Path, store_key: &str) -> PathBuf {
    dir.join(format!("sync-cursor-{store_key}.json"))
}

pub(crate) fn write_sync_cursor(store_key: &str, cursor: &SyncCursor) {
    if let Err(error) = write_sync_cursor_in(&pond_state_dir(), store_key, cursor) {
        tracing::warn!(%error, "failed to write sync cursor");
    }
}

fn write_sync_cursor_in(dir: &Path, store_key: &str, cursor: &SyncCursor) -> Result<()> {
    write_json_atomic(dir, &sync_cursor_path(dir, store_key), cursor)
}

/// Temp + rename so a concurrent reader never sees a half-write. Every record
/// in this dir is read by a sibling process (`pond status`, a restarted sync),
/// so none of them may land in pieces.
fn write_json_atomic<T: Serialize>(dir: &Path, path: &Path, value: &T) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let bytes = serde_json::to_vec_pretty(value)
        .with_context(|| format!("serialize {}", path.display()))?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("failed to install {}", path.display()))?;
    Ok(())
}

pub(crate) fn read_sync_cursor(store_key: &str) -> Option<SyncCursor> {
    read_sync_cursor_in(&pond_state_dir(), store_key)
}

pub(crate) fn sync_cursor_exists(store_key: &str) -> bool {
    sync_cursor_path(&pond_state_dir(), store_key).is_file()
}

pub(crate) fn remove_sync_cursor(store_key: &str) {
    let path = sync_cursor_path(&pond_state_dir(), store_key);
    if let Err(error) = std::fs::remove_file(&path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(%error, path = %path.display(), "failed to remove stale sync cursor");
    }
}

fn read_sync_cursor_in(dir: &Path, store_key: &str) -> Option<SyncCursor> {
    let text = std::fs::read_to_string(sync_cursor_path(dir, store_key)).ok()?;
    serde_json::from_str(&text).ok()
}

fn last_sync_path(dir: &Path, store_key: &str) -> PathBuf {
    dir.join(format!("last-sync-{store_key}.json"))
}

/// Best-effort: a sync must never fail because its status breadcrumb could
/// not be written, so errors degrade to a tracing warning.
pub(crate) fn write_last_sync(store_key: &str, record: &LastSyncRecord) {
    if let Err(error) = write_last_sync_in(&pond_state_dir(), store_key, record) {
        tracing::warn!(%error, "failed to write last-sync record");
    }
}

fn write_last_sync_in(dir: &Path, store_key: &str, record: &LastSyncRecord) -> Result<()> {
    write_json_atomic(dir, &last_sync_path(dir, store_key), record)
}

pub(crate) fn read_last_sync(store_key: &str) -> Option<LastSyncRecord> {
    read_last_sync_in(&pond_state_dir(), store_key)
}

fn read_last_sync_in(dir: &Path, store_key: &str) -> Option<LastSyncRecord> {
    let text = std::fs::read_to_string(last_sync_path(dir, store_key)).ok()?;
    serde_json::from_str(&text).ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn sync_lock_excludes_a_sibling_and_frees_on_drop() {
        let dir = tempfile::TempDir::new().unwrap();
        let guard = match try_acquire_sync_lock_in(dir.path(), "k1").unwrap() {
            SyncLockState::Acquired(guard) => guard,
            SyncLockState::Busy(_) => panic!("fresh lock must acquire"),
        };
        match try_acquire_sync_lock_in(dir.path(), "k1").unwrap() {
            SyncLockState::Busy(holder) => {
                let holder = holder.expect("holder info written on acquire");
                assert_eq!(holder.pid, std::process::id());
            }
            SyncLockState::Acquired(_) => panic!("held lock must report busy"),
        }
        // A different store key is a different lock.
        assert!(matches!(
            try_acquire_sync_lock_in(dir.path(), "k2").unwrap(),
            SyncLockState::Acquired(_)
        ));
        drop(guard);
        assert!(matches!(
            try_acquire_sync_lock_in(dir.path(), "k1").unwrap(),
            SyncLockState::Acquired(_)
        ));
    }

    #[test]
    fn last_sync_record_round_trips_and_is_absent_before_first_write() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(read_last_sync_in(dir.path(), "k").is_none());
        let record = LastSyncRecord {
            finished_at: Utc::now(),
            duration_secs: 12.5,
            sessions_inserted: 3,
            messages_inserted: 41,
            outcome: SyncOutcome::Error,
            error: Some("boom".to_owned()),
        };
        write_last_sync_in(dir.path(), "k", &record).unwrap();
        let read = read_last_sync_in(dir.path(), "k").expect("record present");
        assert_eq!(read.sessions_inserted, 3);
        assert_eq!(read.outcome, SyncOutcome::Error);
        assert_eq!(read.error.as_deref(), Some("boom"));
    }

    #[test]
    fn sync_cursor_round_trips_and_is_keyed_by_store() {
        let dir = tempfile::TempDir::new().unwrap();
        let cursor = SyncCursor {
            messages_version: 17,
            row_count: 2,
            oldest_messages: vec![(3, "message-a".to_owned())],
            watermarks: BTreeMap::from([
                ("session-a".to_owned(), 1_700_000_000_000_000),
                ("session-b".to_owned(), 1_700_000_000_000_100),
            ]),
        };

        assert!(read_sync_cursor_in(dir.path(), "store-a").is_none());
        write_sync_cursor_in(dir.path(), "store-a", &cursor).unwrap();
        assert_eq!(read_sync_cursor_in(dir.path(), "store-a"), Some(cursor));
        assert!(read_sync_cursor_in(dir.path(), "store-b").is_none());
        assert!(
            !sync_cursor_path(dir.path(), "store-a")
                .with_extension("json.tmp")
                .exists()
        );
    }

    #[test]
    fn sync_cursor_gates_freshness_on_its_watermarks() {
        let cursor = SyncCursor {
            messages_version: 3,
            row_count: 1,
            oldest_messages: Vec::new(),
            watermarks: BTreeMap::from([("session-a".to_owned(), 1_700_000_000_000_000)]),
        };

        // The seam rule, not the getter: fresh iff the source has nothing newer.
        assert!(pond::adapter::is_session_fresh(
            &cursor,
            "session-a",
            Some(1_700_000_000_000_000)
        ));
        assert!(!pond::adapter::is_session_fresh(
            &cursor,
            "session-a",
            Some(1_700_000_000_000_001)
        ));
        // A session the cursor never saw is never fresh.
        assert!(!pond::adapter::is_session_fresh(
            &cursor,
            "session-b",
            Some(1)
        ));

        assert!(!pond::adapter::SkipOracle::is_empty(&cursor));
        assert!(pond::adapter::SkipOracle::is_empty(&SyncCursor {
            watermarks: BTreeMap::new(),
            ..cursor
        }));
    }
}

#[cfg(all(test, unix))]
mod lock_release_regression {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;

    /// Guards the fix in `SyncLockGuard::drop`. `flock` belongs to the open file
    /// description, so a subprocess spawned while the guard is alive inherits a
    /// duplicate descriptor and keeps the lock alive past the guard - unless the
    /// guard unlocks explicitly. Without the `unlock()` call this reproduces
    /// within a handful of rounds; the loop is generous only so a slow or busy
    /// machine cannot pass it by luck.
    ///
    /// unix-only: the mechanism is `flock` plus fd inheritance across fork.
    /// Windows locks are mandatory byte-range locks with different handle
    /// inheritance, so the same shape proves nothing there.
    #[test]
    fn lock_frees_on_drop_even_while_subprocesses_are_spawning() {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Counted, not assumed. A spawn that never succeeds turns this test
        // green without ever opening the fork/exec window it exists to probe -
        // which is exactly what a hardcoded /usr/bin/true would do on NixOS or
        // a minimal container, where that path does not exist. `/bin/sh` is
        // present on every unix, and the count is asserted below.
        let spawned = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let spawner = {
            let stop = std::sync::Arc::clone(&stop);
            let spawned = std::sync::Arc::clone(&spawned);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    if std::process::Command::new("/bin/sh")
                        .args(["-c", ":"])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .is_ok()
                    {
                        spawned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            })
        };

        let dir = tempfile::TempDir::new().unwrap();
        let mut result = Ok(());
        for round in 0..2_000 {
            let guard = match try_acquire_sync_lock_in(dir.path(), "k").unwrap() {
                SyncLockState::Acquired(guard) => guard,
                SyncLockState::Busy(_) => {
                    result = Err(format!("round {round}: fresh lock reported busy"));
                    break;
                }
            };
            drop(guard);
            if let SyncLockState::Busy(holder) = try_acquire_sync_lock_in(dir.path(), "k").unwrap()
            {
                result = Err(format!(
                    "round {round}: lock still held after drop (holder={holder:?}) - \
                     an inherited descriptor is keeping the flock alive"
                ));
                break;
            }
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        spawner.join().unwrap();
        if let Err(message) = result {
            panic!("{message}");
        }
        assert!(
            spawned.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "no subprocess ever spawned - this test proved nothing",
        );
    }
}
