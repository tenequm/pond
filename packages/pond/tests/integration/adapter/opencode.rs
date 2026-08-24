//! opencode adapter integration suite: the shared conformance checks over the
//! committed fixture data dir (`opencode.db` + the legacy `storage/` tree),
//! plus the opt-in real-corpus restore oracle below.
//!
//! Real-corpus restore oracle (spec.md#adapter-native-restore-lossless): ingest
//! a REAL opencode data dir into a scratch store, native-serialize every
//! DB-backed session, and assert each emitted `opencode import` envelope is
//! value-equal to the `message.data` / `part.data` rows in the source SQLite
//! DB reconstructed per the adapter's documented injection (`{...data, id,
//! sessionID[, messageID]}` - DB-native rows carry their ids only in columns),
//! modulo the spec'd `<pond:truncated N bytes>` leaf sentinel
//! (spec.md#adapter-bounded-values). Personal data cannot be a committed
//! fixture, so the suite is opt-in:
//!
//!   POND_OPENCODE_DATA_DIR=~/.local/share/opencode \
//!     cargo test --test integration -- adapter::opencode:: --ignored --nocapture

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use pond::{
    adapter::{AdapterFactory, NoopOracle, OpencodeAdapter, OpencodeFactory, RestoreFidelity},
    handlers::ingest_adapter,
    sessions::Store,
};
use serde_json::Value;
use tempfile::TempDir;

use super::{Conformance, RoundTrip, path_config};

const FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/adapter/opencode"
);

fn conformance() -> Conformance<'static> {
    Conformance {
        factory: &OpencodeFactory,
        fixture_root: Path::new(FIXTURE_ROOT),
        // 10 DB-resident sessions plus 4 legacy split-file-tree sessions; the
        // two source eras carry disjoint ids, so nothing is superseded.
        expected_sessions: 14,
        resync_rereads: &[],
        // Native restore emits `opencode import` envelopes for an external
        // tool, not files this adapter re-reads.
        round_trip: RoundTrip::ExternalImport {
            verified_by: "native_restore_conformance_against_db_fixture and \
                          native_restore_emits_import_shape_from_tree in \
                          src/adapter/opencode.rs (CI), plus the opt-in \
                          native_restore_is_value_equal_to_real_db_corpus below",
        },
        config: path_config,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn full_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance().assert_ingest_counts_and_searchable().await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    conformance().assert_resync_is_noop().await
}

#[tokio::test(flavor = "multi_thread")]
async fn native_restore_serves_full_fidelity_import_envelopes() -> anyhow::Result<()> {
    conformance().assert_round_trip().await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "real-corpus oracle: set POND_OPENCODE_DATA_DIR and run with --ignored"]
#[allow(clippy::print_stdout)] // the oracle's coverage summary is its evidence
async fn native_restore_is_value_equal_to_real_db_corpus() -> anyhow::Result<()> {
    let data_dir = PathBuf::from(std::env::var("POND_OPENCODE_DATA_DIR").expect(
        "set POND_OPENCODE_DATA_DIR to an opencode data dir (e.g. ~/.local/share/opencode)",
    ));

    let temp = TempDir::new()?;
    let store = Store::open_local(temp.path()).await?;
    ingest_adapter(
        &store,
        &OpencodeAdapter::new(data_dir.clone()),
        &NoopOracle,
        |_| {},
    )
    .await?;

    let mut checked_sessions = 0usize;
    let mut skipped_empty = 0usize;
    let mut checked_messages = 0usize;
    let mut checked_parts = 0usize;
    let mut tolerated_truncations = 0usize;

    for db_path in db_paths(&data_dir)? {
        let conn = rusqlite::Connection::open_with_flags(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let session_ids: Vec<String> = conn
            .prepare("SELECT id FROM session ORDER BY id")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;

        for session_id in &session_ids {
            let db_messages = rows_json(
                &conn,
                "SELECT id, data FROM message WHERE session_id = ?1",
                session_id,
            )?;
            let Some(session) = store.get_session(session_id).await? else {
                // An Empty source session (spec.md#session-movement-complete)
                // is legitimately absent from the store; anything else missing
                // is a dropped session.
                anyhow::ensure!(
                    db_messages.is_empty(),
                    "{session_id}: has {} DB messages but is missing from the store",
                    db_messages.len()
                );
                skipped_empty += 1;
                continue;
            };

            let files = OpencodeFactory.serialize(&session, RestoreFidelity::Native)?;
            anyhow::ensure!(
                files.len() == 1,
                "{session_id}: expected 1 restored file, got {}",
                files.len()
            );
            let file = &files[0];
            anyhow::ensure!(
                file.relative_path == Path::new(&format!("{session_id}.json")),
                "{session_id}: unexpected restore path {}",
                file.relative_path.display()
            );
            anyhow::ensure!(
                file.actual_fidelity == RestoreFidelity::Native,
                "{session_id}: native restore downgraded to foreign"
            );

            let envelope: Value = serde_json::from_slice(&file.bytes)?;
            anyhow::ensure!(
                envelope["info"]["id"].as_str() == Some(session_id),
                "{session_id}: envelope info.id mismatch"
            );
            let messages = envelope["messages"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("{session_id}: envelope has no messages array"))?;

            let envelope_ids: BTreeSet<&str> = messages
                .iter()
                .filter_map(|m| m["info"]["id"].as_str())
                .collect();
            let db_ids: BTreeSet<&str> = db_messages.iter().map(|(id, _)| id.as_str()).collect();
            anyhow::ensure!(
                envelope_ids == db_ids,
                "{session_id}: message id sets differ\n  missing from envelope: {:?}\n  extra in envelope: {:?}",
                db_ids.difference(&envelope_ids).collect::<Vec<_>>(),
                envelope_ids.difference(&db_ids).collect::<Vec<_>>()
            );

            for message in messages {
                let message_id = message["info"]["id"].as_str().unwrap_or_default();
                let (_, db_info) = db_messages
                    .iter()
                    .find(|(id, _)| id == message_id)
                    .expect("id set equality proved membership");
                let expected_info =
                    with_injected(db_info, &[("id", message_id), ("sessionID", session_id)]);
                anyhow::ensure!(
                    value_equal(&message["info"], &expected_info, &mut tolerated_truncations),
                    "{session_id}/{message_id}: message info differs from DB"
                );
                checked_messages += 1;

                let db_parts = rows_json(
                    &conn,
                    "SELECT id, data FROM part WHERE message_id = ?1",
                    message_id,
                )?;
                let empty = Vec::new();
                let envelope_parts = message["parts"].as_array().unwrap_or(&empty);
                let envelope_part_ids: BTreeSet<&str> = envelope_parts
                    .iter()
                    .filter_map(|p| p["id"].as_str())
                    .collect();
                let db_part_ids: BTreeSet<&str> =
                    db_parts.iter().map(|(id, _)| id.as_str()).collect();
                anyhow::ensure!(
                    envelope_part_ids == db_part_ids,
                    "{session_id}/{message_id}: part id sets differ\n  missing from envelope: {:?}\n  extra in envelope: {:?}",
                    db_part_ids
                        .difference(&envelope_part_ids)
                        .collect::<Vec<_>>(),
                    envelope_part_ids
                        .difference(&db_part_ids)
                        .collect::<Vec<_>>()
                );
                for part in envelope_parts {
                    let part_id = part["id"].as_str().unwrap_or_default();
                    let (_, db_part) = db_parts
                        .iter()
                        .find(|(id, _)| id == part_id)
                        .expect("id set equality proved membership");
                    let expected_part = with_injected(
                        db_part,
                        &[
                            ("id", part_id),
                            ("sessionID", session_id),
                            ("messageID", message_id),
                        ],
                    );
                    anyhow::ensure!(
                        value_equal(part, &expected_part, &mut tolerated_truncations),
                        "{session_id}/{message_id}/{part_id}: part differs from DB"
                    );
                    checked_parts += 1;
                }
            }
            checked_sessions += 1;
        }
    }

    anyhow::ensure!(checked_sessions > 0, "no DB sessions found to check");
    println!(
        "restore oracle: {checked_sessions} sessions ({skipped_empty} empty skipped), \
         {checked_messages} messages, {checked_parts} parts value-equal to the source DB \
         ({tolerated_truncations} truncation sentinels tolerated)"
    );
    Ok(())
}

fn db_paths(data_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(data_dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("opencode") && name.ends_with(".db"))
        })
        .collect();
    paths.sort();
    anyhow::ensure!(
        !paths.is_empty(),
        "no opencode*.db in {}",
        data_dir.display()
    );
    Ok(paths)
}

fn rows_json(
    conn: &rusqlite::Connection,
    sql: &str,
    key: &str,
) -> anyhow::Result<Vec<(String, Value)>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let rows = stmt.query_map([key], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, data) = row?;
        let value = serde_json::from_str(&data)
            .map_err(|error| anyhow::anyhow!("bad JSON in DB row {id}: {error}"))?;
        out.push((id, value));
    }
    Ok(out)
}

/// The adapter's documented DB-row reconstruction: `{...data, <injected ids>}`
/// (the row's identity lives in columns; `opencode import` needs it inline).
fn with_injected(source: &Value, inject: &[(&str, &str)]) -> Value {
    let mut value = source.clone();
    if let Value::Object(map) = &mut value {
        for (key, id) in inject {
            map.insert((*key).to_owned(), Value::String((*id).to_owned()));
        }
    }
    value
}

/// Deep value equality where a restored string leaf may be the spec'd
/// truncation sentinel of the source leaf: a head-preserving prefix plus
/// `<pond:truncated N bytes>` (spec.md#adapter-bounded-values).
fn value_equal(restored: &Value, source: &Value, tolerated: &mut usize) -> bool {
    match (restored, source) {
        (Value::String(restored), Value::String(source)) => {
            restored == source || is_truncation_of(restored, source, tolerated)
        }
        (Value::Array(restored), Value::Array(source)) => {
            restored.len() == source.len()
                && restored
                    .iter()
                    .zip(source)
                    .all(|(r, s)| value_equal(r, s, tolerated))
        }
        (Value::Object(restored), Value::Object(source)) => {
            restored.len() == source.len()
                && restored.iter().all(|(key, value)| {
                    source
                        .get(key)
                        .is_some_and(|s| value_equal(value, s, tolerated))
                })
        }
        _ => restored == source,
    }
}

fn is_truncation_of(restored: &str, source: &str, tolerated: &mut usize) -> bool {
    let marker = format!("<pond:truncated {} bytes>", source.len());
    let truncated = restored
        .strip_suffix(&marker)
        .is_some_and(|head| source.starts_with(head));
    if truncated {
        *tolerated += 1;
    }
    truncated
}
