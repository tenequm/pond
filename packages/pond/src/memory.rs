//! Allocator maintenance and the memory guardrail for the long-lived serving
//! processes.
//!
//! glibc parks freed pages on its per-arena free lists rather than returning
//! them, so a process that peaked stays resident at that peak long after the
//! peak's memory is free (#61, #245); `malloc_trim` asks for the top of each
//! arena back. That is a mitigation, not a cure: a process whose live heap
//! keeps growing still walks to an OOM kill (#111, #245). The ceiling below is
//! the backstop - a bounded, drained self-stop at a configured resident-set
//! limit, so unexplained growth degrades into a restart instead of a kill that
//! the kernel may aim at an unrelated process.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// Ask glibc to return freed pages to the OS. Call only from a quiescent
/// point: glibc walks and locks every arena in turn, so a caller mid-workload
/// pays for the walk and stalls every concurrently allocating thread.
///
/// The `released` flag is glibc's own report of whether anything came back; it
/// is logged rather than acted on, because "nothing to release" is the normal
/// steady-state answer, not a failure.
// `target_env = "gnu"` and not bare `target_os = "linux"`: `malloc_trim` is a
// glibc extension that the `libc` crate declares only for the gnu environment,
// so a musl target would not even compile against the branch below.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[allow(unsafe_code)]
pub fn trim_allocator() {
    // SAFETY: malloc_trim has no caller-side safety invariants.
    let released = unsafe { libc::malloc_trim(0) != 0 };
    tracing::debug!(released, "trimmed glibc allocator");
}

/// No-op off glibc - musl, the macOS allocator and the Windows CRT have no
/// equivalent entry point, and none of them retain pages the way glibc does.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
#[inline]
pub fn trim_allocator() {}

/// Linux `/proc/self/status` resident-set fields, in KiB. `vm_hwm` is the
/// kernel's own high-water mark - a true peak, unlike anything sampled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RssStats {
    pub vm_rss_kb: u64,
    pub vm_hwm_kb: u64,
    pub rss_anon_kb: u64,
}

/// `None` off Linux (no `/proc`); the bench lane uses `ru_maxrss` there.
#[must_use]
pub fn rss() -> Option<RssStats> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let field = |name: &str| -> u64 {
            status
                .lines()
                .find_map(|line| {
                    line.strip_prefix(name)?
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()
                })
                .unwrap_or(0)
        };
        Some(RssStats {
            vm_rss_kb: field("VmRSS:"),
            vm_hwm_kb: field("VmHWM:"),
            rss_anon_kb: field("RssAnon:"),
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Current resident set in KiB, or `None` off Linux.
#[must_use]
pub fn current_rss_kb() -> Option<u64> {
    rss().map(|stats| stats.vm_rss_kb)
}

/// Exit code a serving process uses when it stops itself at the memory
/// ceiling. 75 is sysexits' `EX_TEMPFAIL`: the run failed for a transient
/// reason and retrying is the right response - which is exactly what a
/// supervisor or an MCP client should do with it. Distinct from every other
/// code pond returns, so a supervisor can tell a ceiling stop from a crash.
pub const EXIT_MEMORY_CEILING: i32 = 75;

/// The ceiling applied when `[runtime].memory_ceiling` is unset. Generous next
/// to the measured steady state (157-600 MiB across the reported deployments)
/// and above the largest legitimate transient on record - a 3.8 GiB cold sync
/// over a 3.8M-message store (#245) - so only sustained growth reaches it.
pub const DEFAULT_MEMORY_CEILING_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Floor on a configured ceiling. Below the largest legitimate transient a
/// serving process takes at startup, every restart would immediately breach
/// again; refusing the value beats shipping a restart loop.
pub const MIN_MEMORY_CEILING_BYTES: u64 = 256 * 1024 * 1024;

/// How often a serving process measures itself against the ceiling.
pub const CEILING_CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// Consecutive over-ceiling samples - each taken after a trim - before a
/// restart. Two samples an interval apart is what separates sustained growth
/// from a peak already on its way back down.
pub const CEILING_CONFIRMATIONS: u32 = 2;

/// How long a breach waits for in-flight work before exiting anyway. Bounded
/// so a request wedged on an unreachable object store cannot turn the ceiling
/// into a no-op.
pub const CEILING_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// The `oom_score_adj` a long-lived serving process gives itself when nothing
/// else has chosen one. #245 recorded the inversion this fixes: the kernel
/// killed unrelated ~30 MB processes while multi-GiB `pond mcp` processes
/// survived, because those carried the lower score. +200 is the value two
/// field deployments confirmed.
pub const DEFAULT_OOM_SCORE_ADJ: i32 = 200;

/// The range the kernel accepts for `oom_score_adj`.
pub const OOM_SCORE_ADJ_MIN: i32 = -1000;
pub const OOM_SCORE_ADJ_MAX: i32 = 1000;

/// Reads this process's resident set. Behind a trait so the ceiling's decision
/// logic is testable against a scripted sequence rather than the kernel's
/// bookkeeping.
pub trait ResidentMeter {
    /// Resident bytes, or `None` where the platform exposes no cheap reading.
    fn resident_bytes(&self) -> Option<u64>;
}

/// The real meter: `VmRSS` from `/proc/self/status`.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcMeter;

impl ResidentMeter for ProcMeter {
    fn resident_bytes(&self) -> Option<u64> {
        current_rss_kb().map(|kb| kb.saturating_mul(1024))
    }
}

/// True once `resident` has reached `limit`. Inclusive on purpose: a ceiling
/// is the first value that is too much, not the last one still allowed.
#[must_use]
pub fn over_ceiling(resident_bytes: u64, limit_bytes: u64) -> bool {
    resident_bytes >= limit_bytes
}

/// The outcome of one [`MemoryCeiling::sample`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CeilingVerdict {
    /// No resident-set probe on this platform; the guard is inert.
    Unmeasured,
    /// Under the ceiling - including "over until the trim, then under".
    Clear { resident_bytes: u64 },
    /// Over the ceiling after a trim, but not yet for long enough to act.
    Over {
        resident_bytes: u64,
        consecutive: u32,
    },
    /// Over the ceiling for `confirmations` consecutive samples: restart.
    Breached {
        resident_bytes: u64,
        consecutive: u32,
    },
}

/// Samples a process against its resident-set ceiling and decides when the
/// breach is real.
pub struct MemoryCeiling<M> {
    meter: M,
    limit_bytes: u64,
    confirmations: u32,
    consecutive: u32,
}

impl<M: ResidentMeter> MemoryCeiling<M> {
    pub fn new(meter: M, limit_bytes: u64, confirmations: u32) -> Self {
        Self {
            meter,
            limit_bytes,
            confirmations,
            consecutive: 0,
        }
    }

    /// Take one sample. An over-ceiling reading is never believed on its own:
    /// glibc's retained arenas can hold a process above the ceiling while it
    /// owns nothing, so the trim runs first and only the second reading counts
    /// - a restart must answer live growth, not a reclaimable floor.
    pub fn sample(&mut self) -> CeilingVerdict {
        let Some(resident_bytes) = self.meter.resident_bytes() else {
            return CeilingVerdict::Unmeasured;
        };
        if !over_ceiling(resident_bytes, self.limit_bytes) {
            self.consecutive = 0;
            return CeilingVerdict::Clear { resident_bytes };
        }
        trim_allocator();
        let Some(resident_bytes) = self.meter.resident_bytes() else {
            return CeilingVerdict::Unmeasured;
        };
        if !over_ceiling(resident_bytes, self.limit_bytes) {
            self.consecutive = 0;
            return CeilingVerdict::Clear { resident_bytes };
        }
        self.consecutive = self.consecutive.saturating_add(1);
        let consecutive = self.consecutive;
        if consecutive >= self.confirmations {
            CeilingVerdict::Breached {
                resident_bytes,
                consecutive,
            }
        } else {
            CeilingVerdict::Over {
                resident_bytes,
                consecutive,
            }
        }
    }
}

/// Work a ceiling restart should not cut in half: every request in flight and
/// every in-serve sync cycle. A breach waits for this to reach zero before it
/// exits - bounded by [`CEILING_DRAIN_TIMEOUT`], so a wedged holder cannot turn
/// the ceiling into a no-op. Past that bound the process leaves anyway, which
/// costs the unfinished tail of a cycle and never a half-written row: every
/// pond write lands as one append-only Lance commit, so the next sync redoes
/// exactly what did not commit (`lance-append-only`).
#[derive(Clone, Default)]
pub struct InFlight(Arc<AtomicUsize>);

impl InFlight {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enter a critical section; the returned guard leaves it when dropped -
    /// including on a cancelled future, which is what makes a cancelled
    /// request stop holding the drain open.
    #[must_use]
    pub fn enter(&self) -> InFlightGuard {
        self.0.fetch_add(1, Ordering::AcqRel);
        InFlightGuard(Arc::clone(&self.0))
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

pub struct InFlightGuard(Arc<AtomicUsize>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// How a drain ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Nothing is in flight; stopping now interrupts no work.
    Idle,
    /// The deadline passed with work still outstanding.
    TimedOut { outstanding: usize },
}

/// Wait for every critical section to finish, bounded by `timeout`.
pub async fn drain(in_flight: &InFlight, timeout: Duration) -> DrainOutcome {
    const POLL: Duration = Duration::from_millis(50);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let outstanding = in_flight.count();
        if outstanding == 0 {
            return DrainOutcome::Idle;
        }
        if tokio::time::Instant::now() >= deadline {
            return DrainOutcome::TimedOut { outstanding };
        }
        tokio::time::sleep(POLL.min(timeout)).await;
    }
}

/// The value to write to `oom_score_adj`, or `None` to leave the inherited one
/// alone. A configured value always wins - including `0`, the documented way to
/// opt out - and an inherited non-zero score means the service manager or the
/// operator already chose, so pond does not overrule it.
#[must_use]
pub fn oom_score_adj_target(configured: Option<i32>, inherited: i32) -> Option<i32> {
    match configured {
        Some(value) => Some(value.clamp(OOM_SCORE_ADJ_MIN, OOM_SCORE_ADJ_MAX)),
        None if inherited == 0 => Some(DEFAULT_OOM_SCORE_ADJ),
        None => None,
    }
}

/// Apply [`oom_score_adj_target`] to this process so the kernel prefers the
/// large pond process when it has to choose. Raising the score never needs
/// privilege; best effort, and a failure is logged rather than fatal.
pub fn apply_oom_score_adj(configured: Option<i32>) {
    #[cfg(target_os = "linux")]
    {
        const PATH: &str = "/proc/self/oom_score_adj";
        let inherited = std::fs::read_to_string(PATH)
            .ok()
            .and_then(|raw| raw.trim().parse::<i32>().ok());
        let Some(inherited) = inherited else {
            tracing::debug!("oom_score_adj unreadable; leaving it alone");
            return;
        };
        let Some(target) = oom_score_adj_target(configured, inherited) else {
            tracing::debug!(inherited, "oom_score_adj already set by the supervisor");
            return;
        };
        if target == inherited {
            return;
        }
        match std::fs::write(PATH, format!("{target}\n")) {
            Ok(()) => tracing::info!(inherited, target, "raised oom_score_adj"),
            // Lowering the score needs CAP_SYS_RESOURCE; raising it never does.
            Err(error) => tracing::warn!(%error, target, "could not set oom_score_adj"),
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = configured;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// The FFI branch only has to exist and return; asserting on RSS here
    /// would be asserting on the kernel's bookkeeping, not on this code.
    #[test]
    fn allocator_trim_is_callable() {
        trim_allocator();
    }

    /// The one place a real reading is touched: that the probe exists exactly
    /// where the platform supports it. Every decision test below runs on a
    /// scripted meter instead.
    #[test]
    fn the_rss_probe_matches_platform_support() {
        if cfg!(target_os = "linux") {
            let stats = rss().unwrap_or_default();
            assert!(stats.vm_rss_kb > 0);
            assert!(stats.vm_hwm_kb >= stats.vm_rss_kb);
            assert!(ProcMeter.resident_bytes().unwrap_or_default() > 0);
        } else {
            assert!(rss().is_none());
            assert!(ProcMeter.resident_bytes().is_none());
        }
    }

    /// Replays a scripted sequence of readings, so no test depends on the real
    /// resident set. The last reading repeats once the script runs out.
    struct FakeMeter(Mutex<Vec<u64>>);

    impl FakeMeter {
        fn new(readings: &[u64]) -> Self {
            let mut readings = readings.to_vec();
            readings.reverse();
            Self(Mutex::new(readings))
        }
    }

    impl ResidentMeter for FakeMeter {
        fn resident_bytes(&self) -> Option<u64> {
            let mut readings = self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if readings.len() > 1 {
                readings.pop()
            } else {
                readings.last().copied()
            }
        }
    }

    struct BlindMeter;

    impl ResidentMeter for BlindMeter {
        fn resident_bytes(&self) -> Option<u64> {
            None
        }
    }

    /// The comparison is inclusive, and this is the test a flipped `>=`/`>`
    /// has to fail: exactly at the limit is already a breach.
    #[test]
    fn the_ceiling_is_the_first_value_that_is_too_much() {
        assert!(!over_ceiling(999, 1000));
        assert!(over_ceiling(1000, 1000));
        assert!(over_ceiling(1001, 1000));
    }

    #[test]
    fn a_process_under_the_ceiling_is_clear() {
        let mut ceiling = MemoryCeiling::new(FakeMeter::new(&[10]), 1000, 2);
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Clear { resident_bytes: 10 }
        );
    }

    /// The first over-ceiling sample is a warning, not a restart.
    #[test]
    fn one_sample_over_the_ceiling_does_not_restart() {
        let mut ceiling = MemoryCeiling::new(FakeMeter::new(&[2000]), 1000, 2);
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Over {
                resident_bytes: 2000,
                consecutive: 1,
            }
        );
    }

    /// Exactly `confirmations` consecutive over-ceiling samples breach - the
    /// test a flipped `>=`/`>` on the confirmation count has to fail.
    #[test]
    fn consecutive_samples_over_the_ceiling_breach() {
        let mut ceiling = MemoryCeiling::new(FakeMeter::new(&[2000]), 1000, 2);
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Over {
                resident_bytes: 2000,
                consecutive: 1,
            }
        );
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Breached {
                resident_bytes: 2000,
                consecutive: 2,
            }
        );
    }

    /// Memory the trim gives back is not growth: an over-ceiling reading that
    /// drops below the limit after the trim resets the count.
    #[test]
    fn a_trim_that_reclaims_the_breach_clears_it() {
        // Readings alternate pre-trim / post-trim within one sample.
        let mut ceiling = MemoryCeiling::new(FakeMeter::new(&[2000, 500, 2000, 500]), 1000, 2);
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Clear {
                resident_bytes: 500
            }
        );
        assert_eq!(
            ceiling.sample(),
            CeilingVerdict::Clear {
                resident_bytes: 500
            }
        );
    }

    /// A process that dips back under between two breaches starts over, so
    /// only *consecutive* growth restarts.
    #[test]
    fn a_clear_sample_resets_the_breach_count() {
        let mut ceiling =
            MemoryCeiling::new(FakeMeter::new(&[2000, 2000, 10, 2000, 2000]), 1000, 2);
        assert!(matches!(ceiling.sample(), CeilingVerdict::Over { .. }));
        assert!(matches!(ceiling.sample(), CeilingVerdict::Clear { .. }));
        assert!(matches!(
            ceiling.sample(),
            CeilingVerdict::Over { consecutive: 1, .. }
        ));
    }

    /// Off Linux there is no cheap resident-set reading, and an unmeasured
    /// process must never be restarted on a guess.
    #[test]
    fn an_unmeasurable_process_never_breaches() {
        let mut ceiling = MemoryCeiling::new(BlindMeter, 1000, 1);
        assert_eq!(ceiling.sample(), CeilingVerdict::Unmeasured);
        assert_eq!(ceiling.sample(), CeilingVerdict::Unmeasured);
    }

    #[tokio::test]
    async fn an_idle_process_drains_immediately() {
        let in_flight = InFlight::new();
        assert_eq!(
            drain(&in_flight, Duration::from_secs(30)).await,
            DrainOutcome::Idle
        );
    }

    #[tokio::test]
    async fn a_finished_request_releases_the_drain() {
        let in_flight = InFlight::new();
        let guard = in_flight.enter();
        assert_eq!(in_flight.count(), 1);
        drop(guard);
        assert_eq!(
            drain(&in_flight, Duration::from_secs(30)).await,
            DrainOutcome::Idle
        );
    }

    /// A wedged request must not make the ceiling a no-op.
    #[tokio::test]
    async fn a_stuck_request_bounds_the_drain() {
        let in_flight = InFlight::new();
        let _wedged = in_flight.enter();
        assert_eq!(
            drain(&in_flight, Duration::from_millis(20)).await,
            DrainOutcome::TimedOut { outstanding: 1 }
        );
    }

    #[test]
    fn an_unset_oom_score_adj_gets_ponds_default() {
        assert_eq!(oom_score_adj_target(None, 0), Some(DEFAULT_OOM_SCORE_ADJ));
    }

    /// A supervisor that already chose a score owns it.
    #[test]
    fn an_inherited_oom_score_adj_is_left_alone() {
        assert_eq!(oom_score_adj_target(None, 500), None);
        assert_eq!(oom_score_adj_target(None, -500), None);
    }

    #[test]
    fn a_configured_oom_score_adj_wins_and_is_clamped() {
        assert_eq!(oom_score_adj_target(Some(0), 200), Some(0));
        assert_eq!(oom_score_adj_target(Some(750), 0), Some(750));
        assert_eq!(oom_score_adj_target(Some(5000), 0), Some(1000));
        assert_eq!(oom_score_adj_target(Some(-5000), 0), Some(-1000));
    }
}
