//! Cross-process probe for the one rowmap cycle that can rename over a live
//! file (windows plan section 3.3 probe 2, contingency in section 6).
//!
//! Segments are generation-named (`rowmetamap-<key>-v{N}.rmm`), so a fresh
//! publish never collides with a mapped file. The single cycle that can is:
//! `purge_rowmaps` fails to delete a segment another process still has mapped,
//! and the rebuild then writes the SAME version filename over it. On unix that
//! is a non-event - unlink detaches the name and the sibling keeps its inode.
//! On Windows a mapped file cannot be deleted at all (`STATUS_CANNOT_DELETE`;
//! `FILE_SHARE_DELETE` does not help, and memmap2 holds a duplicated handle
//! until `Drop`), so what the rebuild's rename does there is the open question
//! this probe closes.
//!
//! The invariant asserted on every platform: however the cycle goes, it leaves
//! a segment pond can still read, and any temp left behind is shaped so
//! `sweep_orphan_temps` reclaims it on a later run. pond must never end up with
//! a segment it can neither open nor replace.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use pond::rowmap::{RowMetaEntry, RowMetaMap};
use tempfile::TempDir;

/// Set on the re-executed test binary to select the child role.
const CHILD_SEGMENT_ENV: &str = "POND_ROWMAP_PROBE_SEGMENT";
/// The child touches this once its mapping is live. A marker file rather than a
/// line on stdout: libtest writes its own banner there before the test body, so
/// the first line the parent reads is never the child's.
const CHILD_READY_ENV: &str = "POND_ROWMAP_PROBE_READY";

const STORE_KEY: &str = "probekey";
const VERSION: u64 = 7;

fn entries(count: u64, marker: &str) -> Vec<RowMetaEntry> {
    (0..count)
        .map(|row_id| RowMetaEntry {
            row_id,
            session_id: format!("01HXY{row_id:08}"),
            message_id: format!("msg-{row_id}"),
            role: "user".to_owned(),
            project: "/tmp/probe".to_owned(),
            source_agent: "claude-code".to_owned(),
            timestamp_micros: 1_700_000_000_000_000 + row_id as i64,
            search_text: format!("{marker} row {row_id}"),
        })
        .collect()
}

/// Leftover build temps must match what `sweep_orphan_temps` looks for:
/// the store prefix plus a `.tmp-` infix.
fn orphan_temps(cache_dir: &Path) -> Vec<String> {
    let prefix = format!("rowmetamap-{STORE_KEY}-");
    std::fs::read_dir(cache_dir)
        .expect("read cache dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .filter(|name| name.starts_with(&prefix) && name.contains(".tmp-"))
        .collect()
}

#[test]
fn purge_then_same_version_rebuild_under_a_live_mapping_leaves_a_readable_segment()
-> anyhow::Result<()> {
    let cache = TempDir::new()?;
    let segment = RowMetaMap::path_for(cache.path(), STORE_KEY, VERSION);
    RowMetaMap::build(&segment, VERSION, entries(64, "original"))?;

    // Hold the segment mapped from a second process for the whole cycle. A
    // mapping inside this process would not probe anything: the platform rule
    // under test is about a *foreign* handle.
    let ready = cache.path().join("child.ready");
    let mut child = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "rowmap_purge_probe::child_holds_a_mapping",
            "--ignored",
        ])
        .env(CHILD_SEGMENT_ENV, &segment)
        .env(CHILD_READY_ENV, &ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()?;

    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            Instant::now() < deadline,
            "child never mapped the segment (exit: {:?})",
            child.try_wait()?,
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // The cycle: purge (best-effort, `let _ =` in sessions.rs) then a rebuild
    // at the same version, which renames a fresh temp over the same name.
    let purged = std::fs::remove_file(&segment);
    let rebuilt = RowMetaMap::build(&segment, VERSION, entries(96, "rebuilt"));
    let rebuild_failed = rebuilt.is_err();

    if cfg!(unix) {
        // Pins the POSIX assumption the purge/sweep comments rely on.
        purged.expect("unix: unlinking a mapped file succeeds");
        rebuilt.expect("unix: renaming over a mapped file succeeds");
    }

    // The platform-independent contract. On unix both steps succeed and the
    // segment holds the rebuilt rows; on Windows a blocked purge may leave the
    // original in place - either is survivable, an unreadable or missing
    // segment is not.
    let reopened = RowMetaMap::open(&segment).expect("segment readable after the cycle");
    assert_eq!(reopened.version(), VERSION);

    // A rebuild that could not rename leaves its temp behind. That is fine
    // only because the name is shaped for `sweep_orphan_temps` to reclaim.
    if rebuild_failed {
        assert!(
            !orphan_temps(cache.path()).is_empty(),
            "a failed rebuild must leave a sweepable temp, not an untracked file",
        );
    }

    drop(child.stdin.take());
    child.wait()?;
    Ok(())
}

/// Child role, spawned by the probe above. Maps the segment, marks itself
/// ready, and holds the mapping until the parent closes stdin.
#[test]
#[ignore = "spawned as a child process by the purge probe"]
fn child_holds_a_mapping() {
    let Some(segment) = std::env::var_os(CHILD_SEGMENT_ENV) else {
        panic!("{CHILD_SEGMENT_ENV} must name the segment to map");
    };
    let ready = std::env::var_os(CHILD_READY_ENV).expect("ready-marker path");
    let mapped = RowMetaMap::open(Path::new(&segment)).expect("child maps the segment");

    std::fs::write(Path::new(&ready), b"1").expect("write ready marker");

    let mut sink = String::new();
    let _ = std::io::stdin().read_line(&mut sink);
    drop(mapped);
}
