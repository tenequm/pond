//! What the desk knows about sessions, and the bounded file that carries it
//! between opens (`desk-cache.json` in the plugin state dir). A missing or
//! unreadable file is an empty cache, never an error.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{log_line, write_atomic};
use crate::types::{SessionRow, SessionStats};

const CACHE_FILE: &str = "desk-cache.json";
const LOG_FILE: &str = "desk.log";
/// A format change bumps this, and older files read as empty.
const VERSION: u32 = 1;
const MAX_SESSIONS: usize = 2000;
const MAX_LISTINGS: usize = 8;
/// Owner-only: it holds prompt titles, project paths and host names.
const CACHE_MODE: u32 = 0o600;

/// Titles and hosts never change once read; a missing title holds only for
/// the `last_ts` it was read at, a count until activity past its own.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct Known {
    /// The newest `last_ts` a listing reported.
    #[serde(default)]
    last_ts: Option<DateTime<Utc>>,
    /// The session's first message: its host is the origin host, and a
    /// listing window starting at or before it holds the whole session.
    #[serde(default)]
    first_ts: Option<DateTime<Utc>>,
    #[serde(default)]
    count: Option<Counted>,
    #[serde(default)]
    title: Option<Title>,
    #[serde(default)]
    host: Option<Host>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
struct Counted {
    last_ts: Option<DateTime<Utc>>,
    messages: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Title {
    Text(String),
    Missing { as_of: Option<DateTime<Utc>> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Host {
    Stamped(String),
    /// Pre-stamp rows: unknown provenance, never "this machine".
    Unstamped,
}

impl Known {
    /// The whole-session count, if one is known for the current `last_ts`.
    pub(super) fn count(&self) -> Option<u64> {
        self.count
            .filter(|counted| counted.last_ts >= self.last_ts)
            .map(|counted| counted.messages)
    }

    /// `None` until known; `Some(None)` for a session with no user message.
    pub(super) fn title(&self) -> Option<Option<&str>> {
        match &self.title {
            Some(Title::Text(text)) => Some(Some(text)),
            Some(Title::Missing { as_of }) if *as_of == self.last_ts => Some(None),
            _ => None,
        }
    }

    pub(super) fn set_title(&mut self, title: Option<String>) {
        self.title = Some(title.map_or(
            Title::Missing {
                as_of: self.last_ts,
            },
            Title::Text,
        ));
    }

    pub(super) fn first_ts(&self) -> Option<DateTime<Utc>> {
        self.first_ts
    }

    pub(super) fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    /// `None` is a first message with no host stamp.
    pub(super) fn set_host(&mut self, host: Option<String>) {
        self.host = Some(host.map_or(Host::Unstamped, Host::Stamped));
    }

    /// The count holds for the activity the stats read saw, which a listing
    /// that landed meanwhile may already have moved past.
    pub(super) fn set_stats(&mut self, stats: &SessionStats) {
        self.first_ts = Some(stats.first_ts);
        self.count_as_of(Some(stats.last_ts), stats.message_count);
    }

    /// Keeps whichever count saw the newer activity.
    fn count_as_of(&mut self, last_ts: Option<DateTime<Utc>>, messages: u64) {
        if self.count.is_none_or(|counted| counted.last_ts <= last_ts) {
            self.count = Some(Counted { last_ts, messages });
        }
    }

    /// Takes what a listing row proves. The row counts only its window, so
    /// its count is the session's only when the window reaches the session's
    /// start: always for the all-time listing (`since` is `None`), else only
    /// once that start is known from an earlier all-time row or stats read.
    pub(super) fn observe(&mut self, row: &SessionRow, since: Option<DateTime<Utc>>) {
        if self.last_ts.is_some_and(|last| last > row.last_ts) {
            return;
        }
        self.last_ts = Some(row.last_ts);
        if since.is_none() {
            self.first_ts = Some(row.first_ts);
        }
        let whole = self
            .first_ts
            .is_some_and(|first| since.is_none_or(|since| first >= since));
        if whole {
            self.count_as_of(self.last_ts, row.message_count);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct SavedListing {
    pub(super) project: Option<String>,
    pub(super) all_time: bool,
    pub(super) saved_at: DateTime<Utc>,
    pub(super) rows: Vec<SessionRow>,
    /// Landed during this desk run, as opposed to restored from the file.
    #[serde(skip)]
    pub(super) fresh: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct Snapshot {
    #[serde(default)]
    version: u32,
    pub(super) sessions: HashMap<String, Known>,
    pub(super) listings: Vec<SavedListing>,
}

impl Snapshot {
    pub(super) fn new(sessions: HashMap<String, Known>, listings: Vec<SavedListing>) -> Self {
        Self {
            version: VERSION,
            sessions,
            listings,
        }
    }

    /// Keeps the newest listings, and the sessions with the newest activity.
    fn bound(&mut self) {
        self.listings
            .sort_by_key(|listing| std::cmp::Reverse(listing.saved_at));
        self.listings.truncate(MAX_LISTINGS);
        if self.sessions.len() > MAX_SESSIONS {
            let mut newest: Vec<(Option<DateTime<Utc>>, String)> = self
                .sessions
                .iter()
                .map(|(id, known)| (known.last_ts, id.clone()))
                .collect();
            newest.sort_by(|a, b| b.cmp(a));
            for (_, id) in newest.split_off(MAX_SESSIONS) {
                self.sessions.remove(&id);
            }
        }
    }
}

pub(super) fn load(state_dir: &Path) -> Snapshot {
    let path = state_dir.join(CACHE_FILE);
    let loaded = match std::fs::read(&path) {
        Ok(bytes) => serde_json::from_slice::<Snapshot>(&bytes).map_err(|error| error.to_string()),
        Err(error) if error.kind() == ErrorKind::NotFound => return Snapshot::default(),
        Err(error) => Err(error.to_string()),
    };
    let log = state_dir.join(LOG_FILE);
    match loaded {
        Ok(snapshot) if snapshot.version == VERSION => snapshot,
        Ok(snapshot) => {
            log_line(
                &log,
                &format!(
                    "ignoring {}: version {}, expected {VERSION}",
                    path.display(),
                    snapshot.version
                ),
            );
            Snapshot::default()
        }
        Err(error) => {
            log_line(
                &log,
                &format!("ignoring unreadable {}: {error}", path.display()),
            );
            Snapshot::default()
        }
    }
}

pub(super) fn save(state_dir: &Path, mut snapshot: Snapshot) {
    snapshot.bound();
    let path = state_dir.join(CACHE_FILE);
    let written = serde_json::to_vec(&snapshot)
        .map_err(std::io::Error::other)
        .and_then(|json| write_atomic(&path, &json, CACHE_MODE));
    if let Err(error) = written {
        log_line(
            &state_dir.join(LOG_FILE),
            &format!("cannot write {}: {error}", path.display()),
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::os::unix::fs::PermissionsExt;

    use chrono::TimeDelta;

    use super::*;
    use crate::fake_pond::{Sandbox, ts};

    fn row(first: &str, last: &str, messages: u64) -> SessionRow {
        SessionRow {
            session_id: "s".to_owned(),
            last_ts: ts(last),
            first_ts: ts(first),
            message_count: messages,
            source_agent: "codex-cli".to_owned(),
            project: "/p".to_owned(),
        }
    }

    #[test]
    fn a_windowed_count_is_whole_only_when_the_session_start_is_inside() {
        let since = Some(ts("2026-09-11T00:00:00Z"));
        let straddling = row("2026-09-12T00:00:00Z", "2026-09-20T00:00:00Z", 5);

        let mut unknown_start = Known::default();
        unknown_start.observe(&straddling, since);
        assert_eq!(
            unknown_start.count(),
            None,
            "a later in-window first row does not prove the session started there"
        );

        let mut started_before = Known {
            first_ts: Some(ts("2026-09-01T00:00:00Z")),
            ..Known::default()
        };
        started_before.observe(&straddling, since);
        assert_eq!(started_before.count(), None);

        let mut started_inside = Known {
            first_ts: Some(ts("2026-09-12T00:00:00Z")),
            ..Known::default()
        };
        started_inside.observe(&straddling, since);
        assert_eq!(started_inside.count(), Some(5));

        let mut all_time = Known::default();
        all_time.observe(&straddling, None);
        assert_eq!(all_time.count(), Some(5));
        assert_eq!(all_time.first_ts, Some(ts("2026-09-12T00:00:00Z")));
    }

    #[test]
    fn new_activity_invalidates_counts_and_a_missing_title_but_not_a_title() {
        let mut known = Known::default();
        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-20T00:00:00Z", 5),
            None,
        );
        known.set_title(None);
        assert_eq!(known.title(), Some(None));

        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-21T00:00:00Z", 9),
            Some(ts("2026-09-15T00:00:00Z")),
        );
        assert_eq!(known.count(), None, "the window misses the start");
        assert_eq!(
            known.title(),
            None,
            "a resumed session may have a title now"
        );

        known.set_title(Some("fix it".to_owned()));
        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-22T00:00:00Z", 12),
            None,
        );
        assert_eq!(known.title(), Some(Some("fix it")));
        assert_eq!(known.count(), Some(12));

        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-21T00:00:00Z", 9),
            None,
        );
        assert_eq!(known.count(), Some(12), "an older row is ignored");
    }

    #[test]
    fn a_saved_cache_loads_back_bounded() {
        let sandbox = Sandbox::new();
        let dir = sandbox.state_dir();
        let base = ts("2026-01-01T00:00:00Z");
        let sessions = (0..MAX_SESSIONS + 5)
            .map(|i| {
                let mut known = Known::default();
                let at = base + TimeDelta::minutes(i64::try_from(i).unwrap());
                known.observe(&row("2025-12-31T00:00:00Z", &at.to_rfc3339(), 1), None);
                known.set_title(Some(format!("title {i}")));
                (format!("s{i}"), known)
            })
            .collect();
        let listings = (0..MAX_LISTINGS + 2)
            .map(|i| SavedListing {
                project: Some(format!("/p{i}")),
                all_time: false,
                saved_at: base + TimeDelta::hours(i64::try_from(i).unwrap()),
                rows: Vec::new(),
                fresh: true,
            })
            .collect();
        save(&dir, Snapshot::new(sessions, listings));
        let mode = std::fs::metadata(dir.join(CACHE_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, CACHE_MODE);

        let loaded = load(&dir);
        assert!(loaded.listings.iter().all(|listing| !listing.fresh));
        assert_eq!(loaded.sessions.len(), MAX_SESSIONS);
        assert!(!loaded.sessions.contains_key("s0"), "the oldest is dropped");
        assert_eq!(
            loaded.sessions[&format!("s{}", MAX_SESSIONS + 4)].title(),
            Some(Some(format!("title {}", MAX_SESSIONS + 4).as_str()))
        );
        assert_eq!(loaded.listings.len(), MAX_LISTINGS);
        assert_eq!(
            loaded.listings[0].project.as_deref(),
            Some(format!("/p{}", MAX_LISTINGS + 1).as_str())
        );
    }

    #[test]
    fn a_missing_corrupt_or_foreign_cache_is_empty() {
        let sandbox = Sandbox::new();
        let dir = sandbox.state_dir();
        assert_eq!(load(&dir), Snapshot::default());
        assert!(!dir.join(LOG_FILE).exists(), "a missing cache is not news");

        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(CACHE_FILE), b"{\"sessions\": [tru").unwrap();
        assert_eq!(load(&dir), Snapshot::default());
        let log = std::fs::read_to_string(dir.join(LOG_FILE)).unwrap();
        assert!(log.contains("ignoring unreadable"), "{log}");

        std::fs::write(
            dir.join(CACHE_FILE),
            br#"{"version":99,"sessions":{},"listings":[]}"#,
        )
        .unwrap();
        assert_eq!(load(&dir), Snapshot::default());
        let log = std::fs::read_to_string(dir.join(LOG_FILE)).unwrap();
        assert!(
            log.contains(&format!("version 99, expected {VERSION}")),
            "{log}"
        );
    }

    fn stats(last: &str, messages: u64) -> SessionStats {
        SessionStats {
            session_id: "s".to_owned(),
            message_count: messages,
            first_ts: ts("2026-09-01T00:00:00Z"),
            last_ts: ts(last),
        }
    }

    #[test]
    fn a_stats_count_holds_for_the_activity_it_saw() {
        let mut known = Known::default();
        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-20T00:00:00Z", 5),
            Some(ts("2026-09-15T00:00:00Z")),
        );
        known.set_stats(&stats("2026-09-19T00:00:00Z", 4));
        assert_eq!(
            known.count(),
            None,
            "a read from before the listing's activity undercounts"
        );

        known.set_stats(&stats("2026-09-20T00:00:00Z", 9));
        assert_eq!(known.count(), Some(9));
        known.set_stats(&stats("2026-09-19T00:00:00Z", 4));
        assert_eq!(known.count(), Some(9), "an older read is ignored");

        known.set_stats(&stats("2026-09-21T00:00:00Z", 12));
        assert_eq!(known.count(), Some(12), "a read past the listing counts");
        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-20T00:00:00Z", 9),
            None,
        );
        assert_eq!(known.count(), Some(12), "an older listing row is ignored");
    }

    #[test]
    fn a_stats_read_leaves_a_missing_title_standing() {
        let mut known = Known::default();
        known.observe(
            &row("2026-09-12T00:00:00Z", "2026-09-20T00:00:00Z", 5),
            Some(ts("2026-09-15T00:00:00Z")),
        );
        known.set_title(None);
        known.set_stats(&stats("2026-09-21T00:00:00Z", 6));
        assert_eq!(known.title(), Some(None));
        assert_eq!(known.count(), Some(6));
    }
}
