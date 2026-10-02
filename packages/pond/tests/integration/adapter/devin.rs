//! devin adapter integration suite: the shared conformance checks over the
//! committed data roots (macOS, Windows and Local Fusion sidekick captures),
//! the subagent, sidekick and fork lineage the store must hold after a real
//! ingest, what successive syncs of a store store (the mid-run pair, cuts of
//! the sidekick capture, a re-read adding images). Single-module mapping
//! behavior (the forest partition, provenance, tool outcomes, the watermark)
//! stays in the `src/adapter/devin.rs` unit tests.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use pond::{
    adapter::{DevinAdapter, DevinFactory, NoopOracle},
    handlers::ingest_adapter,
    sessions::{IngestSummary, RowmapOracle, Store},
    wire::{Message, Part, PartKind, Session},
};
use serde_json::{Value, json};
use tempfile::TempDir;

use super::{Conformance, RoundTrip, ensure_clean_ingest, ingest_into_temp_store, path_config};

const MACOS_ROOT: &str = "tests/fixtures/adapter/devin/macos/cli";
const WINDOWS_ROOT: &str = "tests/fixtures/adapter/devin/windows/cli";

// 5 sessions rows plus 3 subagent children (one in amplified-color, two in
// chalk-twig); the fork power-almandine copies a subagent link but no
// subagent nodes, so it yields no child.
const MACOS_SESSIONS: usize = 8;
// 2 sessions rows plus the explore subagent of viridian-bear.
const WINDOWS_SESSIONS: usize = 3;

const SUBAGENT_PARENT: &str = "amplified-color";
const SUBAGENT_CHILD: &str = "amplified-color/agent-51d1dc31-ee2a-44d7-b6f8-3be0f0cf5d6e";
const FORK: &str = "power-almandine";

// One session captured twice: `before` while its background subagent ran (no
// link row yet), `after` once it reported back, two compactions and a
// whole-forest re-save later. `after` holds 69 distinct messages, each placed
// in either the root's trees or the subagent's, so 69 messages are stored.
const MIDRUN_ROOT: &str = "tests/fixtures/adapter/devin/midrun";
const MIDRUN_PARENT: &str = "gilded-orca";
const MIDRUN_CHILD: &str = "gilded-orca/agent-e9b73e40-5526-42b0-acae-45389ecfe004";
const MIDRUN_MESSAGES: usize = 69;

// One Local Fusion session: two sidekick handoffs and an explore subagent. The
// sidekick is one child across both handoffs, so 3 sessions hold its 70
// distinct messages, each in exactly one of them.
const SIDEKICK_ROOT: &str = "tests/fixtures/adapter/devin/sidekick/cli";
const SIDEKICK_SESSIONS: usize = 3;
const SIDEKICK_MESSAGES: usize = 70;
const EXPLORE_CHILD: &str = "third-hourglass/agent-3fc38d38-f601-4976-8786-4d13059aa112";

fn conformance(root: &'static str, sessions: usize) -> Conformance<'static> {
    Conformance {
        factory: &DevinFactory,
        fixture_root: Path::new(root),
        expected_sessions: sessions,
        resync_rereads: &[],
        round_trip: RoundTrip::IngestOnly,
        config: path_config,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn macos_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_ingest_counts_and_searchable()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn windows_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance(WINDOWS_ROOT, WINDOWS_SESSIONS)
        .assert_ingest_counts_and_searchable()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn sidekick_fixture_ingest_counts_and_is_searchable() -> anyhow::Result<()> {
    conformance(SIDEKICK_ROOT, SIDEKICK_SESSIONS)
        .assert_ingest_counts_and_searchable()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_sync_skips_every_unchanged_session() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_resync_is_noop()
        .await?;
    conformance(WINDOWS_ROOT, WINDOWS_SESSIONS)
        .assert_resync_is_noop()
        .await?;
    conformance(SIDEKICK_ROOT, SIDEKICK_SESSIONS)
        .assert_resync_is_noop()
        .await
}

/// A real ingest stores each of the sidekick capture's distinct messages
/// exactly once across its three sessions.
#[tokio::test(flavor = "multi_thread")]
async fn a_sidekick_ingest_stores_every_message_once() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&DevinAdapter::new(SIDEKICK_ROOT)).await?;
    let stored = contents(&store).await?;
    let suffixes: Vec<&str> = stored
        .values()
        .flat_map(|(_, messages)| messages.keys())
        .filter_map(|id| id.rsplit_once(':').map(|(_, id)| id))
        .collect();
    anyhow::ensure!(suffixes.len() == SIDEKICK_MESSAGES);
    anyhow::ensure!(suffixes.iter().collect::<HashSet<_>>().len() == SIDEKICK_MESSAGES);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_is_declared_ingest_only() -> anyhow::Result<()> {
    conformance(MACOS_ROOT, MACOS_SESSIONS)
        .assert_round_trip()
        .await
}

/// The subagent child points at its parent under the kind subpath and holds
/// its own messages, none of the parent's; the fork stays a root, because the
/// writer records no link for it.
#[tokio::test(flavor = "multi_thread")]
async fn subagent_lineage_resolves_and_forks_stay_roots() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&DevinAdapter::new(MACOS_ROOT)).await?;

    let child = store
        .get_session(SUBAGENT_CHILD)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent child not stored"))?;
    anyhow::ensure!(child.session.source_agent == "devin/subagent");
    anyhow::ensure!(child.session.parent_session_id.as_deref() == Some(SUBAGENT_PARENT));
    anyhow::ensure!(child.session.parent_message_id.is_none());
    let parent = store
        .get_session(SUBAGENT_PARENT)
        .await?
        .ok_or_else(|| anyhow::anyhow!("subagent parent not stored"))?;
    let suffixes = |session: &pond::sessions::SessionWithMessages| -> HashSet<String> {
        session
            .messages
            .iter()
            .filter_map(|message| {
                message
                    .message
                    .id()
                    .rsplit_once(':')
                    .map(|(_, id)| id.to_owned())
            })
            .collect()
    };
    anyhow::ensure!(
        suffixes(&parent).is_disjoint(&suffixes(&child)),
        "a subagent message is also stored on the parent",
    );

    let fork = store
        .get_session(FORK)
        .await?
        .ok_or_else(|| anyhow::anyhow!("fork not stored"))?;
    anyhow::ensure!(fork.session.source_agent == "devin");
    anyhow::ensure!(fork.session.parent_session_id.is_none());
    Ok(())
}

/// A sync that lands while a subagent runs, then one after it reports back,
/// store the same messages in every session, and the same child row, as one
/// sync after the report would. Additive sync never rewrites a stored row, so
/// this holds only because nothing the adapter emits for a message or a child
/// depends on when it read.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_during_a_subagent_run_stores_what_a_later_sync_would() -> anyhow::Result<()> {
    let stage = |name: &str| DevinAdapter::new(Path::new(MIDRUN_ROOT).join(name).join("cli"));

    let staged_dir = TempDir::new()?;
    let staged = Store::open_local(staged_dir.path().join("store")).await?;
    let first = ingest_adapter(&staged, &stage("before"), &NoopOracle, |_| {}).await?;
    ensure_clean_ingest("devin", &first)?;
    staged
        .ensure_rowmap(&staged_dir.path().join("cache"))
        .await?;
    let oracle = RowmapOracle(staged.rowmap_snapshot());
    let second = ingest_adapter(&staged, &stage("after"), &oracle, |_| {}).await?;
    ensure_clean_ingest("devin", &second)?;

    let (once, _guard) = ingest_into_temp_store(&stage("after")).await?;
    let (staged, once) = (contents(&staged).await?, contents(&once).await?);
    anyhow::ensure!(messages(&staged) == messages(&once));
    anyhow::ensure!(staged[MIDRUN_CHILD].0 == once[MIDRUN_CHILD].0);

    let (child, messages) = &once[MIDRUN_CHILD];
    anyhow::ensure!(child.parent_session_id.as_deref() == Some(MIDRUN_PARENT));
    anyhow::ensure!(messages.len() > 1);
    anyhow::ensure!(
        once.values()
            .map(|(_, messages)| messages.len())
            .sum::<usize>()
            == MIDRUN_MESSAGES
    );
    Ok(())
}

type Contents = BTreeMap<String, (Session, BTreeMap<String, Value>)>;

/// Each session's messages alone: the root row's first-sync snapshot of
/// unmatched tool state legitimately differs between sync histories.
fn messages(store: &Contents) -> Vec<(&String, &BTreeMap<String, Value>)> {
    store
        .iter()
        .map(|(id, (_, messages))| (id, messages))
        .collect()
}

/// Every stored session row with each message's tool state.
async fn contents(store: &Store) -> anyhow::Result<Contents> {
    let mut out = BTreeMap::new();
    for id in store.session_ids().await? {
        let stored = store
            .get_session(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{id} listed but not stored"))?;
        let messages = stored
            .messages
            .iter()
            .map(|message| {
                (
                    message.message.id().to_owned(),
                    message.message.options()["devin"]["tool_call_state"].clone(),
                )
            })
            .collect();
        out.insert(id, (stored.session, messages));
    }
    Ok(out)
}

/// A copy of the sidekick capture holding only its nodes up to `cut`: what a
/// sync landing at that point of the write would read.
fn sidekick_cut(cut: i64) -> anyhow::Result<TempDir> {
    let dir = TempDir::new()?;
    let db = dir.path().join("sessions.db");
    std::fs::copy(Path::new(SIDEKICK_ROOT).join("sessions.db"), &db)?;
    rusqlite::Connection::open(&db)?
        .execute("DELETE FROM message_nodes WHERE node_id > ?1", [cut])?;
    Ok(dir)
}

/// One plain `pond sync` of the data root into `store`, gated by what the
/// store already holds.
async fn plain_sync(store: &Store, cache: &Path, root: &Path) -> anyhow::Result<IngestSummary> {
    store.ensure_rowmap(cache).await?;
    let oracle = RowmapOracle(store.rowmap_snapshot());
    ingest_adapter(store, &DevinAdapter::new(root), &oracle, |_| {}).await
}

/// A sync landing between the sidekick's parentless first brief (85) and its
/// prefix copy's brief (88) holds nothing back, so the next sync leaves the
/// store exactly as one sync after node 88 would.
#[tokio::test(flavor = "multi_thread")]
async fn a_sync_inside_a_sidekick_handoff_leaves_nothing_behind() -> anyhow::Result<()> {
    let (early, late) = (sidekick_cut(87)?, sidekick_cut(88)?);
    let dir = TempDir::new()?;
    let (store, cache) = (
        Store::open_local(dir.path().join("store")).await?,
        dir.path().join("cache"),
    );
    ensure_clean_ingest("devin", &plain_sync(&store, &cache, early.path()).await?)?;
    ensure_clean_ingest("devin", &plain_sync(&store, &cache, late.path()).await?)?;
    let (once, _guard) = ingest_into_temp_store(&DevinAdapter::new(late.path())).await?;
    anyhow::ensure!(messages(&contents(&store).await?) == messages(&contents(&once).await?));
    Ok(())
}

/// Pins a known residual (docs/adapters/devin.md row 10). A sync landing
/// after the explore subagent's parentless task prompt (186) and its prefix
/// copy (187-188) but before that copy's prompt (189) holds the copy's two
/// system messages back. The next sync names them, but they are older than
/// what the child already stored, so it gates fresh and skips them; the
/// child's next newer message or a full re-read (`pond sync --verify`)
/// stores them.
#[tokio::test(flavor = "multi_thread")]
async fn released_held_messages_wait_for_a_newer_message_or_a_verify() -> anyhow::Result<()> {
    let (early, late) = (sidekick_cut(188)?, sidekick_cut(189)?);
    let (once, _guard) = ingest_into_temp_store(&DevinAdapter::new(late.path())).await?;
    let once = contents(&once).await?;
    let (full, _full_guard) = ingest_into_temp_store(&DevinAdapter::new(SIDEKICK_ROOT)).await?;
    let full = contents(&full).await?;

    for recover_with_newer in [false, true] {
        let dir = TempDir::new()?;
        let store = Store::open_local(dir.path().join("store")).await?;
        let cache = dir.path().join("cache");
        let held = plain_sync(&store, &cache, early.path()).await?;
        anyhow::ensure!(held.dropped_events == 1, "the held-back error: {held:?}");
        ensure_clean_ingest("devin", &plain_sync(&store, &cache, late.path()).await?)?;
        let staged = contents(&store).await?;
        let skipped: Vec<&String> = once[EXPLORE_CHILD]
            .1
            .keys()
            .filter(|id| !staged[EXPLORE_CHILD].1.contains_key(*id))
            .collect();
        anyhow::ensure!(skipped.len() == 2, "{skipped:?}");

        if recover_with_newer {
            let root = Path::new(SIDEKICK_ROOT);
            ensure_clean_ingest("devin", &plain_sync(&store, &cache, root).await?)?;
            anyhow::ensure!(messages(&contents(&store).await?) == messages(&full));
        } else {
            let adapter = DevinAdapter::new(late.path());
            let verify = ingest_adapter(&store, &adapter, &NoopOracle, |_| {}).await?;
            ensure_clean_ingest("devin", &verify)?;
            anyhow::ensure!(messages(&contents(&store).await?) == messages(&once));
        }
    }
    Ok(())
}

/// Every stored message with its stored parts, keyed by message id.
async fn stored_parts(store: &Store) -> anyhow::Result<BTreeMap<String, (Message, Vec<Part>)>> {
    let mut out = BTreeMap::new();
    for id in store.session_ids().await? {
        let stored = store
            .get_session(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{id} listed but not stored"))?;
        for message in stored.messages {
            out.insert(
                message.message.id().to_owned(),
                (message.message, message.parts),
            );
        }
    }
    Ok(out)
}

/// A store synced before images were read gains them on a full re-read
/// (`pond sync --verify`): every stored message and part stays as it was,
/// and each image lands as a File part after them. Part ids are keyed by
/// ordinal and an already-stored id is skipped, so this holds only because
/// images come after every other part.
#[tokio::test(flavor = "multi_thread")]
async fn a_verify_reread_appends_images_after_the_stored_parts() -> anyhow::Result<()> {
    let (store, _guard) = ingest_into_temp_store(&DevinAdapter::new(MACOS_ROOT)).await?;
    let before = stored_parts(&store).await?;

    let forged = TempDir::new()?;
    let db = forged.path().join("sessions.db");
    std::fs::copy(Path::new(MACOS_ROOT).join("sessions.db"), &db)?;
    let conn = rusqlite::Connection::open(&db)?;
    let images = [
        ("4efb7e3f-b402-4e54-be24-f83d5771d2aa", 1),
        ("520f2475-34a4-4c06-b0a4-082c7b1b32d1", 1),
        ("f3e4968f-c6dc-4108-ad89-dfa3dac613d5", 2),
    ];
    for (message_id, count) in images {
        let image =
            json!({"width": 1, "height": 1, "base64_data": "aGk=", "mime_type": "image/png"});
        conn.execute(
            "UPDATE message_nodes SET chat_message = json_set(chat_message, '$.images', json(?1))
             WHERE chat_message ->> '$.message_id' = ?2",
            rusqlite::params![json!(vec![image; count]).to_string(), message_id],
        )?;
    }
    drop(conn);
    let summary = ingest_adapter(
        &store,
        &DevinAdapter::new(forged.path()),
        &NoopOracle,
        |_| {},
    )
    .await?;
    ensure_clean_ingest("devin", &summary)?;

    let after = stored_parts(&store).await?;
    anyhow::ensure!(before.keys().eq(after.keys()), "the message set changed");
    for (id, (message, parts)) in &before {
        let (stored, now) = &after[id];
        anyhow::ensure!(stored == message, "{id}: the stored message changed");
        anyhow::ensure!(now.starts_with(parts), "{id}: a stored part changed");
        let added = &now[parts.len()..];
        let expected = images
            .iter()
            .find(|(message_id, _)| id == &format!("branch-candy:{message_id}"))
            .map_or(0, |&(_, count)| count);
        anyhow::ensure!(added.len() == expected, "{id}: {} parts added", added.len());
        anyhow::ensure!(
            added
                .iter()
                .all(|part| matches!(part.kind, PartKind::File { .. })),
            "{id}: a non-File part was added"
        );
    }
    Ok(())
}
