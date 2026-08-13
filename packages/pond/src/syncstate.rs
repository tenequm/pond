//! Per-host sync coordination: the single-flight lock that keeps a manual
//! `pond sync` and the scheduled one from running concurrently against the
//! same store, and the last-sync record `pond status` reports. Local-process
//! coordination only - cross-host writers stay pure OCC on the Lance store;
//! nothing here ever touches store bytes. Bin-only module: both artifacts are
//! CLI-surface state.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The XDG state root pinned into scheduler job environments so the scheduled
/// sync resolves the same state dir as a manual run. `XDG_STATE_HOME` wins on
/// all platforms when set and absolute; otherwise the platform-native fallback
/// applies.
pub(crate) fn state_root() -> PathBuf {
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

/// The directory where the scheduler log, sync lock, and last-sync record live.
///
/// On all platforms, `XDG_STATE_HOME/pond` when `XDG_STATE_HOME` is set.
/// On Windows (native): `%LOCALAPPDATA%\pond\state` - `state_root()` already
/// points here, so no extra `\pond` subdirectory is added (there are no sibling
/// app state dirs to separate from under that root).
/// On Unix: `$HOME/.local/state/pond` (one app under the shared XDG state home).
pub(crate) fn pond_state_dir() -> PathBuf {
    // When XDG_STATE_HOME is set, state_root() returns it; we add \pond to
    // stay in the XDG-convention "one subdir per app" lane on all platforms.
    if std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .is_some()
    {
        return state_root().join("pond");
    }
    // Native Windows: state_root() IS the pond state dir already.
    #[cfg(windows)]
    return state_root();
    // Unix: state_root() is the XDG state home; add the app subdirectory.
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

/// Held for the whole sync run. The OS drops the flock when the file closes,
/// so a killed sync can never leave a stale lock. The lock file is never
/// unlinked - unlink while a sibling holds the path open hands out two locks.
/// `SyncLockGuard::drop` removes the sibling `.holder.json` best-effort.
pub(crate) struct SyncLockGuard {
    _file: File,
    holder_path: PathBuf,
}

impl Drop for SyncLockGuard {
    fn drop(&mut self) {
        // Best-effort: the file may already be absent (concurrent stop, etc.).
        let _ = std::fs::remove_file(&self.holder_path);
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
            // Use temp + rename so a concurrent reader never sees a partial
            // write (same pattern as write_last_sync_in). Best-effort: the
            // lock itself is the flock, not the holder bytes.
            if let Ok(bytes) = serde_json::to_vec(&holder) {
                let tmp = holder_path.with_extension("json.tmp");
                if std::fs::write(&tmp, &bytes).is_ok() {
                    let _ = std::fs::rename(&tmp, &holder_path);
                }
            }
            Ok(SyncLockState::Acquired(SyncLockGuard {
                _file: file,
                holder_path,
            }))
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
            Ok(SyncLockState::Acquired(SyncLockGuard {
                _file: file,
                holder_path,
            }))
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
    std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    let path = last_sync_path(dir, store_key);
    let bytes = serde_json::to_vec_pretty(record).context("serialize last-sync record")?;
    // Temp + rename so a concurrent `pond status` never reads a half-write.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("failed to install {}", path.display()))?;
    Ok(())
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
}
