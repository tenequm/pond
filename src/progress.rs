//! Live progress for long-running operations (today: `pond copy`).
//!
//! Two halves, with a deliberate seam between them:
//!
//! * [`CopyState`] is the lock-free atomic counter bag that the copy worker
//!   tasks write into. Every field is an independent `Atomic*`, so the three
//!   concurrent `merge_scanner` futures in `Store::copy_delta_from` never
//!   serialize on a shared lock.
//! * [`Reporter`] owns the rendering side: an `indicatif::MultiProgress` (for
//!   the human surface), the NDJSON encoder (`--json`), and the OSC 9;4
//!   taskbar emitter. A single background ticker reads `CopyState::snapshot`
//!   every 100 ms and fans the snapshot out to whichever renderer is active.
//!
//! That split is what lets new output modes land without touching the copy
//! code: every renderer reads from the same snapshot, so the JSON shape and
//! the human display are always coherent by construction.

use std::collections::VecDeque;
use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::Serialize;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::substrate::{MergeStats, Table, WriteStats};

/// Phases of `pond copy`, in execution order. Termination is signaled
/// separately via [`CopyState::finished`]; mixing a `Done` sentinel into
/// this enum would force `match` arms onto a value that is not actually a
/// phase of work, so the two concerns stay split.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Plan = 0,
    Stream = 1,
    Indexes = 2,
    Verify = 3,
}

impl Phase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Plan,
            1 => Self::Stream,
            2 => Self::Indexes,
            _ => Self::Verify,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Stream => "stream",
            Self::Indexes => "indexes",
            Self::Verify => "verify",
        }
    }
}

/// Read-once snapshot of the live counters. Rendering reads this; nobody
/// writes through it.
#[derive(Debug, Clone)]
pub struct CopySnapshot {
    pub phase: Phase,
    pub finished: bool,
    pub sessions_total: u64,
    pub sessions_done: u64,
    pub messages_done: u64,
    pub parts_done: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub fragments_written: u64,
    pub commit_attempts_max: u64,
    pub skipped_duplicates: u64,
    /// Sessions already fully present on the destination when the copy began
    /// (`source_sessions - plan.total()`). The gauge starts here, not at zero,
    /// so a resumed copy reflects the ground already covered.
    pub sessions_baseline: u64,
    /// Sessions this run must touch (`plan.total()`).
    pub sessions_to_copy: u64,
    /// Total message + part rows this run must write (remaining = source - dest,
    /// summed). The denominator that actually moves on a resumed/grown copy,
    /// where `sessions_done` (new session rows) can stay zero throughout.
    pub rows_target: u64,
    pub elapsed: Duration,
}

impl CopySnapshot {
    /// Rows written so far this run (the gauge's numerator unit). Session rows
    /// are excluded: a resumed copy writes none, yet moves millions of
    /// message/part rows.
    pub fn rows_done(&self) -> u64 {
        self.messages_done + self.parts_done
    }

    fn rows_remaining(&self) -> u64 {
        self.rows_target.saturating_sub(self.rows_done())
    }

    /// Fraction of this run's row work done, in `[0, 1]`. With nothing to copy
    /// (`rows_target == 0`) the run is trivially complete.
    fn progress_fraction(&self) -> f64 {
        if self.rows_target == 0 {
            1.0
        } else {
            (self.rows_done() as f64 / self.rows_target as f64).clamp(0.0, 1.0)
        }
    }

    /// Sessions fully synced on the destination, estimated by mapping the row
    /// progress onto the session count: `baseline + fraction * to_copy`. Smooth
    /// (advances every tick rather than stepping per completed session) and lands
    /// exactly on `sessions_total` at completion.
    pub fn synced_sessions(&self) -> u64 {
        self.sessions_baseline
            + (self.progress_fraction() * self.sessions_to_copy as f64).floor() as u64
    }
}

/// Atomic counter bag shared across the copy worker tasks. All loads use
/// `Relaxed` because a single-tick snapshot may briefly straddle a write
/// without consequence: the visual cost is invisible at 10 Hz, and the
/// end-of-run summary reads *after* all writers join, so its numbers are
/// exact.
#[derive(Debug)]
pub struct CopyState {
    started: Instant,
    phase: AtomicU8,
    finished: AtomicBool,
    sessions_total: AtomicU64,
    sessions_done: AtomicU64,
    messages_done: AtomicU64,
    parts_done: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
    fragments_written: AtomicU64,
    commit_attempts_max: AtomicU64,
    skipped_duplicates: AtomicU64,
    sessions_baseline: AtomicU64,
    sessions_to_copy: AtomicU64,
    rows_target: AtomicU64,
}

impl CopyState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            phase: AtomicU8::new(Phase::Plan as u8),
            finished: AtomicBool::new(false),
            sessions_total: AtomicU64::new(0),
            sessions_done: AtomicU64::new(0),
            messages_done: AtomicU64::new(0),
            parts_done: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            fragments_written: AtomicU64::new(0),
            commit_attempts_max: AtomicU64::new(0),
            skipped_duplicates: AtomicU64::new(0),
            sessions_baseline: AtomicU64::new(0),
            sessions_to_copy: AtomicU64::new(0),
            rows_target: AtomicU64::new(0),
        })
    }

    pub fn set_sessions_total(&self, total: u64) {
        self.sessions_total.store(total, Ordering::Relaxed);
    }

    /// Seed the destination-fill gauge once the plan is known: how many sessions
    /// already match (`baseline`), how many this run touches (`to_copy`), and
    /// the total message + part rows it must write (`rows_target`).
    pub fn set_gauge(&self, baseline: u64, to_copy: u64, rows_target: u64) {
        self.sessions_baseline.store(baseline, Ordering::Relaxed);
        self.sessions_to_copy.store(to_copy, Ordering::Relaxed);
        self.rows_target.store(rows_target, Ordering::Relaxed);
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Relaxed);
    }

    /// Marks the run as terminated. The ticker stops on its next wake-up
    /// after observing this, and `Reporter::finish` joins it cleanly.
    pub fn mark_finished(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }

    /// Roll one Lance `MergeStats` into the live counters. Called from
    /// `sessions::merge_scanner` per batch. Per-session progress advances
    /// only on the `Sessions` table; messages and parts have their own
    /// counters but do not pretend to advance "sessions done" (that would
    /// double-count and confuse the ETA).
    pub fn record_merge(&self, table: Table, stats: &MergeStats) {
        let inserted = stats.num_inserted_rows + stats.num_updated_rows;
        self.done_bucket(table)
            .fetch_add(inserted, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(stats.bytes_written, Ordering::Relaxed);
        self.fragments_written
            .fetch_add(stats.num_files_written, Ordering::Relaxed);
        self.skipped_duplicates
            .fetch_add(stats.num_skipped_duplicates, Ordering::Relaxed);
        bump_max(&self.commit_attempts_max, stats.num_attempts as u64);
    }

    /// Fold one cumulative `WriteStats` tick from `append_stream` into the live
    /// counters. The append (absent-session) path produces no `MergeStats`, so
    /// without this it would show zero progress.
    ///
    /// `WriteStats` is cumulative *per stream*, but `CopyState` is shared
    /// across the three tables appending in parallel, so we must add only this
    /// stream's increment, not the raw cumulative. `cum` is the per-stream
    /// high-water mark (one per `append_stream` call): `fetch_max` advances it
    /// and the `saturating_sub` yields the new ground gained. This also makes
    /// the fold exact across OCC retries - a failed attempt writes orphaned
    /// fragments and ticks the counter up to some high-water; the retry's fresh
    /// per-stream cumulative restarts from zero, so its early ticks fall below
    /// the high-water (delta 0, the bar briefly stalls) until it passes the
    /// prior mark, after which the shared total lands at exactly the final
    /// written size. (A *per-attempt* `cum` would instead re-add the full
    /// retry and double-count.) `rows_written` advances the per-table done
    /// bucket; bytes/files advance the wire totals.
    pub fn record_write_progress(&self, table: Table, cum: &WriteCumulative, stats: &WriteStats) {
        let (rows, bytes, files) = cum.observe(stats);
        self.done_bucket(table).fetch_add(rows, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.fragments_written.fetch_add(files, Ordering::Relaxed);
    }

    /// Final bookkeeping for one `append_stream` call: only the OCC attempt
    /// count. Bytes/rows/files already streamed in live via
    /// [`Self::record_write_progress`] (including Lance's final cumulative
    /// tick), so folding them here would double-count.
    pub fn record_append(&self, attempts: u32) {
        bump_max(&self.commit_attempts_max, attempts as u64);
    }

    fn done_bucket(&self, table: Table) -> &AtomicU64 {
        match table {
            Table::Sessions => &self.sessions_done,
            Table::Messages => &self.messages_done,
            Table::Parts => &self.parts_done,
        }
    }

    /// Lance `ExecutionSummaryCounts.bytes_read` from a finished scan. One
    /// call per source-table scan; the value is the bytes Lance actually
    /// pulled from the source object store, post-coalescing.
    pub fn record_scan_summary(&self, bytes_read: u64) {
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CopySnapshot {
        CopySnapshot {
            phase: Phase::from_u8(self.phase.load(Ordering::Relaxed)),
            finished: self.finished.load(Ordering::Relaxed),
            sessions_total: self.sessions_total.load(Ordering::Relaxed),
            sessions_done: self.sessions_done.load(Ordering::Relaxed),
            messages_done: self.messages_done.load(Ordering::Relaxed),
            parts_done: self.parts_done.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            fragments_written: self.fragments_written.load(Ordering::Relaxed),
            commit_attempts_max: self.commit_attempts_max.load(Ordering::Relaxed),
            skipped_duplicates: self.skipped_duplicates.load(Ordering::Relaxed),
            sessions_baseline: self.sessions_baseline.load(Ordering::Relaxed),
            sessions_to_copy: self.sessions_to_copy.load(Ordering::Relaxed),
            rows_target: self.rows_target.load(Ordering::Relaxed),
            elapsed: self.started.elapsed(),
        }
    }
}

/// Per-stream high-water mark for one `append_stream` call, shared across its
/// OCC attempts. Each field tracks the max cumulative `WriteStats` value seen
/// so that [`CopyState::record_write_progress`] folds only newly-written
/// ground into the shared counters (see that method for the retry exactness
/// argument). After the append commits, these hold the call's final totals.
#[derive(Debug, Default)]
pub struct WriteCumulative {
    rows: AtomicU64,
    bytes: AtomicU64,
    files: AtomicU64,
}

impl WriteCumulative {
    /// Advance the high-water marks to `stats` and return the newly-written
    /// `(rows, bytes, files)` deltas - the increment beyond what this stream
    /// had already accounted for. `fetch_max` makes it monotonic, so a retry's
    /// from-zero cumulative contributes nothing until it passes the prior mark.
    pub fn observe(&self, stats: &WriteStats) -> (u64, u64, u64) {
        let rows = stats
            .rows_written
            .saturating_sub(self.rows.fetch_max(stats.rows_written, Ordering::Relaxed));
        let bytes = stats
            .bytes_written
            .saturating_sub(self.bytes.fetch_max(stats.bytes_written, Ordering::Relaxed));
        let files = (stats.files_written as u64).saturating_sub(
            self.files
                .fetch_max(stats.files_written as u64, Ordering::Relaxed),
        );
        (rows, bytes, files)
    }

    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }
    pub fn files(&self) -> u64 {
        self.files.load(Ordering::Relaxed)
    }
}

/// Lock-free monotonic max via CAS. Cheap (one extra load on the rare
/// "raise the bar" path), branch-free in the common case.
fn bump_max(target: &AtomicU64, candidate: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while candidate > current {
        match target.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    bytes_read: u64,
    bytes_written: u64,
    rows_done: u64,
}

/// Sliding window over the last 30 s of samples. Cumulative averages lie
/// badly after a phase transition (see indicatif #580 / restic PR #3563);
/// a window the size of "what just happened" re-converges in its own
/// length. Window width is fixed at construction; renderers push one
/// sample per tick.
#[derive(Debug)]
struct RateWindow {
    samples: VecDeque<Sample>,
    window: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct Rates {
    read_bps: f64,
    write_bps: f64,
    rows_per_sec: f64,
}

impl RateWindow {
    fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
        }
    }

    fn push(&mut self, snap: &CopySnapshot) {
        self.samples.push_back(Sample {
            elapsed: snap.elapsed,
            bytes_read: snap.bytes_read,
            bytes_written: snap.bytes_written,
            rows_done: snap.rows_done(),
        });
        while let Some(front) = self.samples.front() {
            if snap.elapsed.saturating_sub(front.elapsed) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn rates(&self) -> Rates {
        let (Some(head), Some(tail)) = (self.samples.front(), self.samples.back()) else {
            return Rates::default();
        };
        let spread = tail.elapsed.saturating_sub(head.elapsed).as_secs_f64();
        if spread < 0.1 {
            return Rates::default();
        }
        Rates {
            read_bps: tail.bytes_read.saturating_sub(head.bytes_read) as f64 / spread,
            write_bps: tail.bytes_written.saturating_sub(head.bytes_written) as f64 / spread,
            rows_per_sec: tail.rows_done.saturating_sub(head.rows_done) as f64 / spread,
        }
    }
}

/// Output mode for `pond copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Phased receipts + multi-metric live block on stderr when stderr is a
    /// TTY; one stats line every 5 s otherwise (rclone `--stats` shape).
    Human,
    /// NDJSON on stderr, one `status` event per tick + a closing `summary`.
    /// Field names match `restic --json` so existing parsers Just Work.
    Json,
    /// No human output at all - but the ticker still runs to drive OSC 9;4
    /// when stderr is a TTY (terminal-taskbar progress).
    Silent,
}

#[derive(Debug, Serialize)]
struct StatusEvent<'a> {
    message_type: &'a str,
    phase: &'a str,
    seconds_elapsed: u64,
    seconds_remaining: Option<u64>,
    percent_done: f64,
    total_files: u64,
    files_done: u64,
    /// Bytes written to the destination so far. There is no honest
    /// destination-side `total_bytes` to compare against for store-to-store
    /// (Lance writes can compress, dedupe, and produce more or fewer files
    /// per source row), so we omit `total_bytes` rather than emit a lie.
    bytes_done: u64,
    bytes_read_per_second: u64,
    bytes_written_per_second: u64,
    /// Worst OCC retry count seen so far across all commits (append + merge).
    /// Non-zero is a destination-contention warning. No restic analog;
    /// added as a pond-native field.
    commit_retries_max: u64,
    skipped_duplicates: u64,
}

#[derive(Debug, Serialize)]
struct SummaryEvent<'a> {
    message_type: &'a str,
    total_duration: f64,
    sessions_copied: u64,
    messages_copied: u64,
    parts_copied: u64,
    bytes_read: u64,
    bytes_written: u64,
    fragments_written: u64,
    skipped_duplicates: u64,
    commit_retries_max: u64,
}

/// Owns the rendering side: indicatif `MultiProgress` (human TTY), NDJSON
/// writer (`--json`), and the OSC 9;4 emitter. Dropping the reporter
/// aborts the ticker; explicit [`Reporter::finish`] is the supported exit
/// path because it also flushes the final `summary` event and the cleared
/// OSC sequence.
pub struct Reporter {
    state: Arc<CopyState>,
    mode: Mode,
    multi: Option<MultiProgress>,
    live_bar: Option<ProgressBar>,
    /// Second line of the human TTY surface: throughput / sub-state under the
    /// `live_bar`'s what-and-how-far headline. No spinner of its own (the
    /// headline animates for both); refreshed every tick alongside `live_bar`.
    detail_bar: Option<ProgressBar>,
    ticker: Option<JoinHandle<()>>,
    stop: Arc<Notify>,
    /// Receipts pushed via `receipt()`. Held only for tests; the live
    /// surface already shows them in scrollback.
    receipts: Arc<Mutex<Vec<String>>>,
}

impl Reporter {
    pub fn new(state: Arc<CopyState>, mode: Mode) -> Self {
        let stderr_is_tty = std::io::stderr().is_terminal();
        let (multi, live_bar, detail_bar) = if matches!(mode, Mode::Human) && stderr_is_tty {
            let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(10));
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("{spinner:.green} {msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            pb.enable_steady_tick(Duration::from_millis(120));
            // No spinner token: the headline's steady tick redraws the whole
            // MultiProgress, and the ticker sets this message every 100 ms.
            let detail = mp.add(ProgressBar::new_spinner());
            detail.set_style(
                ProgressStyle::with_template("{msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner()),
            );
            (Some(mp), Some(pb), Some(detail))
        } else {
            (None, None, None)
        };
        Self {
            state,
            mode,
            multi,
            live_bar,
            detail_bar,
            ticker: None,
            stop: Arc::new(Notify::new()),
            receipts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Spawn the background ticker. Reads `CopyState::snapshot()` every
    /// 100 ms, updates the live bar (human TTY), emits one NDJSON `status`
    /// (json), and drives OSC 9;4 across all modes when stderr is a TTY.
    pub fn start(&mut self) {
        let state = self.state.clone();
        let mode = self.mode;
        let bar = self.live_bar.clone();
        let detail = self.detail_bar.clone();
        let stop = self.stop.clone();
        let osc_enabled = std::io::stderr().is_terminal();
        let tick = Duration::from_millis(100);
        let non_tty_stats = Duration::from_secs(5);
        self.ticker = Some(tokio::spawn(async move {
            let mut window = RateWindow::new(Duration::from_secs(30));
            let mut next_stats_at = Instant::now() + non_tty_stats;
            let mut osc = OscEmitter::new(osc_enabled);
            loop {
                let stop_fut = stop.notified();
                tokio::pin!(stop_fut);
                let sleep = tokio::time::sleep(tick);
                tokio::pin!(sleep);
                tokio::select! {
                    _ = &mut stop_fut => break,
                    _ = &mut sleep => {}
                }
                let snap = state.snapshot();
                window.push(&snap);
                let rates = window.rates();
                match mode {
                    Mode::Human => {
                        if let Some(bar) = bar.as_ref() {
                            bar.set_message(format_line1(&snap, &rates));
                            if let Some(detail) = detail.as_ref() {
                                detail.set_message(format_line2(&snap, &rates));
                            }
                        } else if Instant::now() >= next_stats_at {
                            let _ = writeln!(
                                std::io::stderr(),
                                "{}",
                                format_non_tty_stats(&snap, &rates)
                            );
                            next_stats_at = Instant::now() + non_tty_stats;
                        }
                    }
                    Mode::Json => emit_status_json(&snap, &rates),
                    Mode::Silent => {}
                }
                osc.tick(&snap);
                if snap.finished {
                    break;
                }
            }
            osc.clear();
        }));
    }

    /// Print a receipt line above the live bar (TTY) or to stderr (non-TTY
    /// human). JSON / silent modes record it for tests but emit nothing.
    pub fn receipt(&self, line: &str) {
        if let Ok(mut r) = self.receipts.lock() {
            r.push(line.to_string());
        }
        match self.mode {
            Mode::Human => {
                if let Some(mp) = self.multi.as_ref() {
                    let _ = mp.println(line);
                } else {
                    let _ = writeln!(std::io::stderr(), "{line}");
                }
            }
            Mode::Json | Mode::Silent => {}
        }
    }

    /// Hand the terminal to a sub-stage that draws its *own* progress bar (the
    /// index-optimize spinner in `run_update_indexes_stage`): clear our lines and
    /// hide the `MultiProgress` so two indicatif draw targets don't fight over
    /// the cursor - the cause of the flicker during the `Indexes` phase. The
    /// ticker keeps running (OSC + state), but its `set_message` calls now land
    /// on a hidden target. No-op off the human TTY surface.
    pub fn suspend_live(&self) {
        if let Some(mp) = &self.multi {
            let _ = mp.clear();
            mp.set_draw_target(ProgressDrawTarget::hidden());
        }
    }

    /// Reclaim the terminal after a suspended sub-stage finishes, so the next
    /// phase (verify) renders again. Pairs with [`Self::suspend_live`].
    pub fn resume_live(&self) {
        if let Some(mp) = &self.multi {
            mp.set_draw_target(ProgressDrawTarget::stderr_with_hz(10));
        }
    }

    /// Stop the ticker, flush the final receipt (human) or `summary` event
    /// (json), and clear the OSC sequence.
    pub async fn finish(mut self, final_line: Option<&str>) {
        self.state.mark_finished();
        self.stop.notify_waiters();
        if let Some(t) = self.ticker.take() {
            let _ = t.await;
        }
        if let Some(bar) = self.live_bar.take() {
            bar.finish_and_clear();
        }
        if let Some(detail) = self.detail_bar.take() {
            detail.finish_and_clear();
        }
        let snap = self.state.snapshot();
        match self.mode {
            Mode::Human => {
                if let Some(line) = final_line {
                    if let Some(mp) = self.multi.as_ref() {
                        let _ = mp.println(line);
                    } else {
                        let _ = writeln!(std::io::stderr(), "{line}");
                    }
                }
            }
            Mode::Json => emit_summary_json(&snap),
            Mode::Silent => {}
        }
    }
}

impl Drop for Reporter {
    fn drop(&mut self) {
        // Best-effort cancel: a Reporter dropped without `finish()` (panic
        // path) should not leak the ticker task indefinitely.
        if let Some(handle) = self.ticker.take() {
            handle.abort();
        }
    }
}

/// Headline (first TTY line): the phase, and for `Stream` the destination-fill
/// gauge + ETA. The non-`Stream` phases scan or rebuild without a row gauge
/// (plan compares both stores; indexes folds fragments; verify diffs id-sets),
/// so they show a "working" line with elapsed rather than a frozen 100% gauge
/// or zeroed metrics that read as stuck. The bar template supplies the spinner.
fn format_line1(snap: &CopySnapshot, rates: &Rates) -> String {
    let el = format_duration(snap.elapsed);
    match snap.phase {
        Phase::Plan => format!("plan  comparing source <-> destination  [{el}]"),
        Phase::Indexes => format!("indexes  folding text + semantic on destination  [{el}]"),
        Phase::Verify => format!("verify  comparing id-sets  [{el}]"),
        Phase::Stream => {
            let synced = snap.synced_sessions();
            let pct = if snap.sessions_total > 0 {
                (synced as f64 / snap.sessions_total as f64) * 100.0
            } else {
                0.0
            };
            let eta = format_eta(snap, rates.rows_per_sec);
            format!(
                "stream  destination {synced}/{total} synced ({pct:.0}%)  ETA {eta}  [{el}]",
                synced = format_count(synced),
                total = format_count(snap.sessions_total),
            )
        }
    }
}

/// Detail (second TTY line, indented under the headline): rows + throughput
/// during `Stream`, a "why it's slow" note during `Plan`, blank for the
/// gauge-less rebuild phases.
fn format_line2(snap: &CopySnapshot, rates: &Rates) -> String {
    match snap.phase {
        Phase::Plan => "    remote scan over the destination, can take a while".into(),
        Phase::Indexes | Phase::Verify => String::new(),
        Phase::Stream => {
            // Between a merge's source scan and its commit, no progress callback
            // fires, so both rates read ~0 while a (often large) merge_insert is
            // in flight on the remote store - the only thing that zeroes both in
            // this phase. Name that state instead of a frozen "0 B/s" that reads
            // as stuck. spec.md#session-durable-copy: the merge tail is the cost.
            let stalled = rates.read_bps < 1.0
                && rates.write_bps < 1.0
                && snap.rows_done() < snap.rows_target;
            if stalled {
                return "    committing to destination...  (large merge on S3, no new bytes yet)"
                    .into();
            }
            let retries = if snap.commit_attempts_max > 1 {
                format!("  -  retries {}", snap.commit_attempts_max)
            } else {
                String::new()
            };
            format!(
                "    +{rows} rows  -  read {rb}/s  write {wb}/s{retries}",
                rows = format_count(snap.rows_done()),
                rb = format_bytes(rates.read_bps as u64),
                wb = format_bytes(rates.write_bps as u64),
            )
        }
    }
}

fn format_non_tty_stats(snap: &CopySnapshot, rates: &Rates) -> String {
    if snap.phase == Phase::Plan {
        return format!(
            "[{el}] phase=plan comparing source and destination",
            el = format_duration(snap.elapsed),
        );
    }
    format!(
        "[{el}] phase={phase} destination={synced}/{total} rows={rows} read={rb}/s write={wb}/s retries={ret}",
        el = format_duration(snap.elapsed),
        phase = snap.phase.label(),
        synced = snap.synced_sessions(),
        total = snap.sessions_total,
        rows = snap.rows_done(),
        rb = format_bytes(rates.read_bps as u64),
        wb = format_bytes(rates.write_bps as u64),
        ret = snap.commit_attempts_max,
    )
}

fn format_eta(snap: &CopySnapshot, rows_per_sec: f64) -> String {
    eta_secs(snap, rows_per_sec)
        .map(|s| format_duration(Duration::from_secs(s)))
        .unwrap_or_else(|| "--".into())
}

fn eta_secs(snap: &CopySnapshot, rows_per_sec: f64) -> Option<u64> {
    if snap.rows_target == 0 || snap.rows_done() >= snap.rows_target || rows_per_sec < 0.01 {
        return None;
    }
    Some((snap.rows_remaining() as f64 / rows_per_sec).round() as u64)
}

fn format_count(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_bytes(b: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    if b >= GIB {
        format!("{:.1} GiB", b as f64 / GIB as f64)
    } else if b >= MIB {
        format!("{:.1} MiB", b as f64 / MIB as f64)
    } else if b >= KIB {
        format!("{:.1} KiB", b as f64 / KIB as f64)
    } else {
        format!("{b} B")
    }
}

fn format_duration(d: Duration) -> String {
    let total = d.as_secs();
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    if h > 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

fn emit_status_json(snap: &CopySnapshot, rates: &Rates) {
    let synced = snap.synced_sessions();
    let percent = if snap.sessions_total > 0 {
        (synced as f64 / snap.sessions_total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let evt = StatusEvent {
        message_type: "status",
        phase: snap.phase.label(),
        seconds_elapsed: snap.elapsed.as_secs(),
        seconds_remaining: eta_secs(snap, rates.rows_per_sec),
        percent_done: percent,
        total_files: snap.sessions_total,
        files_done: synced,
        bytes_done: snap.bytes_written,
        bytes_read_per_second: rates.read_bps as u64,
        bytes_written_per_second: rates.write_bps as u64,
        commit_retries_max: snap.commit_attempts_max,
        skipped_duplicates: snap.skipped_duplicates,
    };
    if let Ok(line) = serde_json::to_string(&evt) {
        let _ = writeln!(std::io::stderr(), "{line}");
    }
}

fn emit_summary_json(snap: &CopySnapshot) {
    let evt = SummaryEvent {
        message_type: "summary",
        total_duration: snap.elapsed.as_secs_f64(),
        sessions_copied: snap.sessions_done,
        messages_copied: snap.messages_done,
        parts_copied: snap.parts_done,
        bytes_read: snap.bytes_read,
        bytes_written: snap.bytes_written,
        fragments_written: snap.fragments_written,
        skipped_duplicates: snap.skipped_duplicates,
        commit_retries_max: snap.commit_attempts_max,
    };
    if let Ok(line) = serde_json::to_string(&evt) {
        let _ = writeln!(std::io::stderr(), "{line}");
    }
}

/// OSC 9;4 emitter for terminal-taskbar progress. Three-tier behavior:
///   * non-TTY: no-op (constructed disabled).
///   * unknown total (sessions_total == 0): pulse (state 3) until known.
///   * known total: state 1 with percentage; throttled to >= 200 ms between
///     writes and only when the percent actually changes.
struct OscEmitter {
    enabled: bool,
    last_pct: Option<u8>,
    last_emit: Option<Instant>,
}

impl OscEmitter {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            last_pct: None,
            last_emit: None,
        }
    }

    fn tick(&mut self, snap: &CopySnapshot) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        if let Some(last) = self.last_emit
            && now.duration_since(last) < Duration::from_millis(200)
        {
            return;
        }
        let seq = if snap.sessions_total == 0 {
            termpulse_core::OscSequence::indeterminate("")
        } else {
            let pct = ((snap.synced_sessions() as f64 / snap.sessions_total as f64) * 100.0)
                .clamp(0.0, 100.0) as u8;
            if self.last_pct == Some(pct) {
                return;
            }
            self.last_pct = Some(pct);
            termpulse_core::OscSequence::normal(pct)
        };
        let mut buf = [0u8; 64];
        if let Ok(n) = seq.write_to(&mut buf) {
            let mut stderr = std::io::stderr();
            let _ = stderr.write_all(&buf[..n]);
            let _ = stderr.flush();
        }
        self.last_emit = Some(now);
    }

    fn clear(&mut self) {
        if !self.enabled {
            return;
        }
        let seq = termpulse_core::OscSequence::clear();
        let mut buf = [0u8; 64];
        if let Ok(n) = seq.write_to(&mut buf) {
            let mut stderr = std::io::stderr();
            let _ = stderr.write_all(&buf[..n]);
            let _ = stderr.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_count_boundaries() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1_000), "1.0k");
        assert_eq!(format_count(1_500_000), "1.5M");
        assert_eq!(format_count(2_000_000_000), "2.0B");
    }

    #[test]
    fn format_bytes_uses_binary_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.0 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GiB");
    }

    #[test]
    fn format_duration_short_and_long() {
        assert_eq!(format_duration(Duration::from_secs(0)), "00:00");
        assert_eq!(format_duration(Duration::from_secs(65)), "01:05");
        assert_eq!(format_duration(Duration::from_secs(3725)), "01:02:05");
    }

    #[test]
    fn eta_secs_returns_none_without_target_or_progress() {
        assert_eq!(eta_secs(&snap_rows(0, 0), 1.0), None, "no row target");
        assert_eq!(
            eta_secs(&snap_rows(100, 100), 1.0),
            None,
            "already complete"
        );
        assert_eq!(eta_secs(&snap_rows(100, 0), 0.0), None, "no rate yet");
    }

    #[test]
    fn eta_secs_from_row_rate() {
        // 60 rows left at 2/sec -> 30s
        assert_eq!(eta_secs(&snap_rows(100, 40), 2.0), Some(30));
    }

    #[test]
    fn rate_window_returns_zero_until_two_samples() {
        let mut w = RateWindow::new(Duration::from_secs(30));
        let snap = make_snap_with_bytes(0, 0, 0, 0);
        w.push(&snap);
        let r = w.rates();
        assert_eq!(r.read_bps, 0.0);
        assert_eq!(r.write_bps, 0.0);
        assert_eq!(r.rows_per_sec, 0.0);
    }

    #[test]
    fn rate_window_computes_bps_over_spread() {
        let mut w = RateWindow::new(Duration::from_secs(30));
        let s0 = sample(0, 0, 0, 0);
        let s1 = sample(2, 2_000, 1_000, 4);
        push_sample(&mut w, s0);
        push_sample(&mut w, s1);
        let r = w.rates();
        assert!((r.read_bps - 1000.0).abs() < 0.01);
        assert!((r.write_bps - 500.0).abs() < 0.01);
        assert!((r.rows_per_sec - 2.0).abs() < 0.01);
    }

    #[test]
    fn rate_window_evicts_old_samples() {
        let mut w = RateWindow::new(Duration::from_secs(5));
        push_sample(&mut w, sample(0, 0, 0, 0));
        push_sample(&mut w, sample(2, 100, 100, 1));
        push_sample(&mut w, sample(10, 1_000, 1_000, 5));
        // Only the two most recent samples should remain (spread > 5s).
        assert_eq!(w.samples.len(), 1);
    }

    #[test]
    fn bump_max_is_monotonic() {
        let m = AtomicU64::new(2);
        bump_max(&m, 1);
        assert_eq!(m.load(Ordering::Relaxed), 2);
        bump_max(&m, 5);
        assert_eq!(m.load(Ordering::Relaxed), 5);
    }

    fn make_snap_with_bytes(
        sessions_total: u64,
        sessions_done: u64,
        bytes_read: u64,
        bytes_written: u64,
    ) -> CopySnapshot {
        CopySnapshot {
            phase: Phase::Stream,
            finished: false,
            sessions_total,
            sessions_done,
            messages_done: 0,
            parts_done: 0,
            bytes_read,
            bytes_written,
            fragments_written: 0,
            commit_attempts_max: 0,
            skipped_duplicates: 0,
            sessions_baseline: 0,
            sessions_to_copy: 0,
            rows_target: 0,
            elapsed: Duration::from_secs(0),
        }
    }

    /// A stream-phase snapshot with the row gauge set: `rows_done` messages
    /// written toward a `rows_target` total.
    fn snap_rows(rows_target: u64, rows_done: u64) -> CopySnapshot {
        CopySnapshot {
            rows_target,
            messages_done: rows_done,
            ..make_snap_with_bytes(0, 0, 0, 0)
        }
    }

    fn sample(secs: u64, bytes_read: u64, bytes_written: u64, rows_done: u64) -> CopySnapshot {
        CopySnapshot {
            elapsed: Duration::from_secs(secs),
            messages_done: rows_done,
            ..make_snap_with_bytes(0, 0, bytes_read, bytes_written)
        }
    }

    fn push_sample(w: &mut RateWindow, snap: CopySnapshot) {
        w.push(&snap);
    }
}
