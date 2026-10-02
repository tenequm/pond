//! The erase vocabulary every enforcement point shares
//! (spec.md#session-append-only-exception): the portable intent keys that
//! denylist an erased session, their merge rule, and the store-level erase
//! epoch that invalidates derived "already ingested" signals. The verb that
//! writes them ships separately; everything here is what a binary must honor
//! before it.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::adapter::SkipOracle;

/// Config key prefix of the portable intent: `pond.erased.<session_id>`,
/// written on both `sessions` and `messages` and enforced as their union, so a
/// rollback of either table alone cannot lift it.
pub(crate) const ERASED_KEY_PREFIX: &str = "pond.erased.";

/// The value of an intent key. Enforcement keys on presence alone; the value
/// only records when and through which named session the id was erased.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErasedIntent {
    pub at: DateTime<Utc>,
    pub root: String,
}

impl ErasedIntent {
    /// The `(key, value)` config entry denylisting `session_id`.
    pub fn entry(&self, session_id: &str) -> (String, String) {
        (
            intent_key(session_id),
            serde_json::to_string(self).unwrap_or_default(),
        )
    }
}

/// The config key denylisting `session_id`.
pub(crate) fn intent_key(session_id: &str) -> String {
    format!("{ERASED_KEY_PREFIX}{session_id}")
}

/// Every intent entry across `configs`, keyed by config key. The first config
/// wins on a key present in several - callers pass the destination first.
/// Operation records and the epoch share the `pond.erase.` namespace but not
/// this prefix, so they never travel.
pub fn intent_entries<'a>(
    configs: impl IntoIterator<Item = &'a HashMap<String, String>>,
) -> BTreeMap<String, String> {
    let mut entries = BTreeMap::new();
    for config in configs {
        for (key, value) in config {
            if key.starts_with(ERASED_KEY_PREFIX) {
                entries.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }
    }
    entries
}

/// The session ids enforced by `configs`: an id is erased when any of them
/// carries its intent key.
pub fn enforced_ids<'a>(
    configs: impl IntoIterator<Item = &'a HashMap<String, String>>,
) -> HashSet<String> {
    intent_ids(&intent_entries(configs))
}

/// The session ids named by intent `entries` (see [`intent_entries`]).
pub fn intent_ids(entries: &BTreeMap<String, String>) -> HashSet<String> {
    entries
        .keys()
        .filter_map(|key| key.strip_prefix(ERASED_KEY_PREFIX).map(str::to_owned))
        .collect()
}

/// The merge rule for intent arriving by copy or restore: only entries whose
/// key the destination's `existing` intent (both tables, see
/// [`intent_entries`]) lacks are imported. A present key is kept unchanged, so
/// an import never weakens, lifts or rewrites the destination's denylist.
pub fn intent_to_import(
    existing: &BTreeMap<String, String>,
    incoming: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    incoming
        .iter()
        .filter(|(key, _)| key.starts_with(ERASED_KEY_PREFIX) && !existing.contains_key(*key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// `messages` manifest config key holding the erase epoch.
pub(crate) const EPOCH_KEY: &str = "pond.erase.epoch";

/// The store's erase epoch, as recorded by a derived signal (a rowmap chain,
/// a sync cursor) or read from the live `messages` config.
///
/// A signal is valid only while the epoch it recorded equals the store's and
/// the store's is not in flight ([`Self::admits`]); anything else is discarded
/// and rebuilt from stored data, never extended. `Never` is a store no erase
/// has touched, so every pre-erase signal stays valid until the first one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EraseEpoch {
    #[default]
    Never,
    Settled([u8; 16]),
    InFlight,
}

impl EraseEpoch {
    /// The epoch a manifest config carries. Only a uuid is settled: anything
    /// else - `<op>:inflight`, or a form a later binary writes - reads as in
    /// flight, so an unrecognized value can never admit a stale signal.
    pub fn from_config(config: &HashMap<String, String>) -> Self {
        config
            .get(EPOCH_KEY)
            .map_or(Self::Never, |value| Self::from_value(value))
    }

    /// The epoch a present config value names.
    pub fn from_value(value: &str) -> Self {
        match uuid::Uuid::parse_str(value) {
            Ok(uuid) => Self::Settled(*uuid.as_bytes()),
            Err(_) => Self::InFlight,
        }
    }

    /// Whether a signal that recorded `recorded` may be used against a store
    /// whose current epoch is `self`.
    pub fn admits(self, recorded: Self) -> bool {
        self != Self::InFlight && recorded == self
    }
}

/// Any [`SkipOracle`] plus the store's erased ids - how a sync hands the
/// freshness gate its denylist whichever watermark source it settled on.
pub struct ErasedOracle {
    pub inner: Box<dyn SkipOracle>,
    pub erased: HashSet<String>,
}

impl SkipOracle for ErasedOracle {
    fn session_max_ts(&self, session_id: &str) -> Option<i64> {
        self.inner.session_max_ts(session_id)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn is_erased(&self, session_id: &str) -> bool {
        self.erased.contains(session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(root: &str) -> ErasedIntent {
        ErasedIntent {
            at: DateTime::from_timestamp(1_790_000_000, 0).unwrap_or_default(),
            root: root.to_owned(),
        }
    }

    #[test]
    fn intent_entry_round_trips_through_its_key_and_value() {
        let (key, value) = intent("root-a").entry("child-b");
        assert_eq!(key, "pond.erased.child-b");
        assert_eq!(
            serde_json::from_str::<ErasedIntent>(&value).ok(),
            Some(intent("root-a"))
        );
    }

    #[test]
    fn enforcement_is_the_union_of_both_tables() {
        let sessions = HashMap::from([intent("a").entry("a")]);
        let messages = HashMap::from([
            intent("b").entry("b"),
            (EPOCH_KEY.to_owned(), "op:inflight".to_owned()),
            ("pond.erase.op.x".to_owned(), "{}".to_owned()),
            ("pond.fts.stems".to_owned(), "s".to_owned()),
        ]);
        assert_eq!(
            enforced_ids([&sessions, &messages]),
            HashSet::from(["a".to_owned(), "b".to_owned()])
        );
        // A rollback of either table alone keeps the id enforced.
        let replicated = HashMap::from([intent("a").entry("a")]);
        assert!(enforced_ids([&HashMap::new(), &replicated]).contains("a"));
    }

    #[test]
    fn import_never_weakens_rewrites_or_carries_op_records() {
        let existing = intent_entries([&HashMap::from([intent("dest").entry("shared")])]);
        let incoming = intent_entries([&HashMap::from([
            intent("source").entry("shared"),
            intent("source").entry("fresh"),
            ("pond.erase.op.x".to_owned(), "{}".to_owned()),
            (EPOCH_KEY.to_owned(), "e".to_owned()),
        ])]);
        let imported = intent_to_import(&existing, &incoming);
        assert_eq!(imported, vec![intent("source").entry("fresh")]);
    }

    fn config(value: &str) -> HashMap<String, String> {
        HashMap::from([(EPOCH_KEY.to_owned(), value.to_owned())])
    }

    #[test]
    fn epoch_parses_every_config_form() {
        assert_eq!(EraseEpoch::from_config(&HashMap::new()), EraseEpoch::Never);
        assert_eq!(
            EraseEpoch::from_config(&config("op-7:inflight")),
            EraseEpoch::InFlight
        );
        let uuid = uuid::Uuid::now_v7();
        assert_eq!(
            EraseEpoch::from_config(&config(&uuid.to_string())),
            EraseEpoch::Settled(*uuid.as_bytes())
        );
    }

    /// Fail closed: a value this binary does not recognize - a malformed one,
    /// or an in-flight form a later binary writes - admits no signal.
    #[test]
    fn an_unrecognized_epoch_reads_as_in_flight() {
        for value in ["not-a-uuid", "", "op-7:running", "inflight:op-7"] {
            let epoch = EraseEpoch::from_config(&config(value));
            assert_eq!(epoch, EraseEpoch::InFlight, "{value:?}");
            assert!(!epoch.admits(epoch));
        }
    }

    #[test]
    fn only_an_equal_settled_or_never_epoch_admits_a_signal() {
        let first = EraseEpoch::Settled([1; 16]);
        let second = EraseEpoch::Settled([2; 16]);
        assert!(EraseEpoch::Never.admits(EraseEpoch::Never));
        assert!(first.admits(first));
        assert!(!second.admits(first));
        assert!(!first.admits(EraseEpoch::Never));
        assert!(!EraseEpoch::Never.admits(first));
        // In flight admits nothing, not even a signal built during the erase.
        assert!(!EraseEpoch::InFlight.admits(EraseEpoch::InFlight));
        assert!(!EraseEpoch::InFlight.admits(EraseEpoch::Never));
    }
}
