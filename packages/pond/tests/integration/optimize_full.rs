//! `pond optimize` diagnoses, `pond optimize --full` heals (#305), end to end
//! through the CLI: a store planted with the pre-#285 tiny-page layout and an
//! orphaned index must be reported by `pond status` and bare `pond optimize`
//! without being touched, healed by `--full`, and a second `--full` must find
//! nothing left to do.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;

use chrono::Utc;
use lance::Dataset;
use lance::dataset::optimize::{CompactionMode, CompactionOptions, compact_files};
use lance::index::DatasetIndexExt;
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use pond::{
    sessions::{MaintenanceFinding, Store},
    substrate::Table,
    wire::{ProviderOptions, Session},
};
use tempfile::TempDir;

const ORPHAN: &str = "sessions_retired_btree";
const APPENDS: usize = 16;
/// Over 32 rows per page, the case a rows/page threshold missed on `messages`.
const SESSIONS_PER_APPEND: usize = 40;
const SESSIONS: usize = APPENDS * SESSIONS_PER_APPEND;

fn session(id: &str) -> Session {
    Session {
        id: id.to_owned(),
        parent_session_id: None,
        parent_message_id: None,
        source_agent: "claude-code".to_owned(),
        created_at: Utc::now(),
        project: pond::adapter::extract_str(&serde_json::json!({"x": "/tmp/full"}), "x").unwrap(),
        options: ProviderOptions::new(),
    }
}

/// One commit per append, then the binary-copy compaction old pond ran: one
/// fragment holding one page per append. Plus an index no intent names.
async fn plant_legacy_store(store_dir: &Path) {
    let store = Store::open_local(store_dir).await.unwrap();
    for append in 0..APPENDS {
        let batch: Vec<Session> = (0..SESSIONS_PER_APPEND)
            .map(|index| session(&format!("session-{append}-{index}")))
            .collect();
        store.upsert_sessions(&batch).await.unwrap();
    }
    store.build_indices_only(None).await.unwrap();
    assert_eq!(
        store.diagnose().await.unwrap(),
        vec![],
        "a fresh store is clean"
    );
    drop(store);

    let mut sessions = Dataset::open(store_dir.join("sessions.lance").to_str().unwrap())
        .await
        .unwrap();
    compact_files(
        &mut sessions,
        CompactionOptions {
            compaction_mode: Some(CompactionMode::TryBinaryCopy),
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    sessions
        .create_index_builder(
            &["source_agent"],
            IndexType::BTree,
            &ScalarIndexParams::for_builtin(BuiltinIndexType::BTree),
        )
        .name(ORPHAN.to_owned())
        .await
        .unwrap();
}

fn pond(temp: &TempDir, args: &[&str]) -> String {
    let out = assert_cmd::Command::new(env!("CARGO_BIN_EXE_pond"))
        .arg("--storage-path")
        .arg(temp.path().join("store"))
        .args(args)
        .env("HOME", temp.path().join("home"))
        .env("USERPROFILE", temp.path().join("home"))
        .env("APPDATA", temp.path().join("config"))
        .env("LOCALAPPDATA", temp.path().join("data"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_STATE_HOME", temp.path().join("state"))
        .env_remove("POND_STORAGE_PATH")
        .env("NO_COLOR", "1")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "`pond {args:?}` exited {:?}: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8(out.stdout).unwrap()
}

async fn findings(temp: &TempDir) -> Vec<MaintenanceFinding> {
    Store::open_local(temp.path().join("store"))
        .await
        .unwrap()
        .diagnose()
        .await
        .unwrap()
}

#[tokio::test]
async fn bare_optimize_reports_and_full_heals() {
    let temp = TempDir::new().unwrap();
    std::fs::create_dir_all(temp.path().join("home")).unwrap();
    plant_legacy_store(&temp.path().join("store")).await;

    let planted = findings(&temp).await;
    assert!(
        planted.contains(&MaintenanceFinding::OrphanIndex {
            table: Table::Sessions,
            index: ORPHAN.to_owned(),
        }),
        "{planted:?}",
    );
    assert!(
        planted.iter().any(|finding| matches!(
            finding,
            MaintenanceFinding::LegacyLayout { table: Table::Sessions, layout }
                if layout.fragment_ids.len() == 1 && layout.bytes > 0
        )),
        "{planted:?}",
    );

    let status = pond(&temp, &["status"]);
    assert!(
        status.contains("sessions: legacy page layout detected in 1 fragment ->"),
        "{status}"
    );
    assert!(
        status.contains(&format!("sessions: orphaned index {ORPHAN}")),
        "{status}"
    );
    let status_json: serde_json::Value =
        serde_json::from_str(&pond(&temp, &["status", "--format", "json"])).unwrap();
    let kinds: Vec<&str> = status_json["maintenance"]
        .as_array()
        .expect("maintenance findings in status JSON")
        .iter()
        .map(|finding| finding["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["orphan_index", "legacy_layout"]);

    let bare = pond(&temp, &["optimize"]);
    assert!(bare.contains("legacy page layout detected"), "{bare}");
    assert!(bare.contains("run `pond optimize --full`"), "{bare}");
    assert_eq!(findings(&temp).await, planted, "bare optimize never heals");

    let full = pond(&temp, &["optimize", "--full"]);
    assert!(
        full.contains(&format!("dropped orphaned index {ORPHAN}")),
        "{full}"
    );
    assert!(full.contains("1 sessions fragments"), "{full}");
    assert_eq!(findings(&temp).await, vec![], "--full heals every finding");
    let store = Store::open_local(temp.path().join("store")).await.unwrap();
    assert_eq!(store.row_counts().await.unwrap().0, SESSIONS);
    assert!(store.get_session("session-7-7").await.unwrap().is_some());
    drop(store);

    let again = pond(&temp, &["optimize", "--full"]);
    assert!(again.contains("nothing to heal"), "{again}");
}
