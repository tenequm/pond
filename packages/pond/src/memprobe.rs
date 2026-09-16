//! Memory instrumentation for the bench/profiling lanes (#245).
//!
//! Compiled only under `mem-probe` or `dhat-heap`; a default build has no
//! `#[global_allocator]` item, no statics, and no probe code at all.
//!
//! Three layers, so a number always points somewhere:
//!   * heap (`mem-probe`): a counting wrapper over `System` - live/peak/total
//!     bytes the program asked the allocator for.
//!   * RSS (Linux): `VmRSS`/`VmHWM`/`RssAnon` from `/proc/self/status`, plus
//!     `ru_maxrss` via `getrusage` everywhere else. The heap-vs-RSS gap IS the
//!     allocator-retention signal (#61: ~636 MiB freed-but-retained).
//!   * a 200 ms sampler, so a peak is caught even between explicit reads.
//!
//! Counting every allocation costs three atomic RMWs per `alloc`, so wall time
//! measured under `mem-probe` is not comparable to an uninstrumented run.
//!
//! macOS `phys_footprint` probes stay in `benches/serve_mem_bench.rs` for now;
//! docs/plans/2609-16-memory-instrumentation.md moves them here in hardening.

#[cfg(all(feature = "mem-probe", feature = "dhat-heap"))]
compile_error!(
    "mem-probe and dhat-heap both install a #[global_allocator]; enable exactly one \
     (mem-probe for the gate rows, dhat-heap for allocation-site attribution)"
);

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

/// Sampling period of [`RssSampler`]. Fine enough to catch a multi-second sync
/// spike, coarse enough that the `/proc` read is noise.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

#[cfg(feature = "mem-probe")]
mod counting {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicU64, Ordering};

    pub(super) static LIVE: AtomicU64 = AtomicU64::new(0);
    pub(super) static PEAK: AtomicU64 = AtomicU64::new(0);
    pub(super) static TOTAL: AtomicU64 = AtomicU64::new(0);

    /// Global atomics, not thread-locals: a thread-local scheme undercounts a
    /// multi-threaded peak, and the true global peak is the metric (#245).
    fn record_alloc(bytes: u64) {
        TOTAL.fetch_add(bytes, Ordering::Relaxed);
        let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
        PEAK.fetch_max(live, Ordering::Relaxed);
    }

    pub(super) struct CountingAlloc;

    // The crate denies `unsafe_code` rather than forbidding it precisely so a
    // wrapper like this can opt in with its reasoning stated (see `embed.rs`).
    #[allow(unsafe_code)]
    // SAFETY: every method forwards its arguments to `System` unchanged and
    // returns what `System` returned, so the `GlobalAlloc` contract is whatever
    // `System` already guarantees; the counters only observe sizes.
    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size() as u64);
            }
            ptr
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size() as u64);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
            if !new_ptr.is_null() {
                if new_size >= layout.size() {
                    record_alloc((new_size - layout.size()) as u64);
                } else {
                    LIVE.fetch_sub((layout.size() - new_size) as u64, Ordering::Relaxed);
                }
            }
            new_ptr
        }
    }
}

#[cfg(feature = "mem-probe")]
#[global_allocator]
static GLOBAL: counting::CountingAlloc = counting::CountingAlloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

/// Hold the returned profiler for the region to attribute; dropping it writes
/// `dhat-heap.json` for the online DHAT viewer. Diagnosis lane only - dhat is
/// slow and cannot reset mid-run.
#[cfg(feature = "dhat-heap")]
#[must_use]
pub fn start_dhat_profiler() -> dhat::Profiler {
    dhat::Profiler::new_heap()
}

/// What the counting allocator saw. `live` is retained bytes at the moment of
/// the read, `peak` the high-water mark since process start (or the last
/// [`reset_heap_peak`]), `total` every byte ever handed out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapStats {
    pub live_bytes: u64,
    pub peak_bytes: u64,
    pub total_alloc_bytes: u64,
}

/// `None` unless built with `mem-probe`.
#[must_use]
pub fn heap_stats() -> Option<HeapStats> {
    #[cfg(feature = "mem-probe")]
    {
        Some(HeapStats {
            live_bytes: counting::LIVE.load(Ordering::Relaxed),
            peak_bytes: counting::PEAK.load(Ordering::Relaxed),
            total_alloc_bytes: counting::TOTAL.load(Ordering::Relaxed),
        })
    }
    #[cfg(not(feature = "mem-probe"))]
    {
        None
    }
}

/// Drop the heap high-water mark to the currently-live bytes, so the next peak
/// read covers one scenario phase instead of the whole process.
pub fn reset_heap_peak() {
    #[cfg(feature = "mem-probe")]
    {
        let live = counting::LIVE.load(Ordering::Relaxed);
        counting::PEAK.store(live, Ordering::Relaxed);
    }
}

/// Linux `/proc/self/status` resident-set fields, in KiB. `vm_hwm` is the
/// kernel's own high-water mark - a true peak, unlike anything sampled.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RssStats {
    pub vm_rss_kb: u64,
    pub vm_hwm_kb: u64,
    pub rss_anon_kb: u64,
}

/// `None` off Linux (no `/proc`); use [`ru_maxrss_kb`] there.
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
    rss().map(|r| r.vm_rss_kb)
}

/// Reset the kernel's `VmHWM` watermark to the current `VmRSS`. Linux exposes
/// this only as a side effect of `/proc/self/clear_refs` mode 5, which is
/// otherwise a no-op (it does not touch page tables or reclaim anything).
/// Returns false where the mechanism does not exist.
pub fn reset_peak_rss() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::write("/proc/self/clear_refs", "5").is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// `getrusage(RUSAGE_SELF).ru_maxrss` normalized to KiB: it is bytes on macOS
/// (Apple's deviation from BSD) and KiB on Linux.
#[cfg(unix)]
#[must_use]
pub fn ru_maxrss_kb() -> Option<u64> {
    // Narrow opt-in past the crate's `unsafe_code = "deny"`, same as the FFI in
    // `embed.rs` and the bench harnesses.
    #[allow(unsafe_code)]
    let usage = {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: `getrusage` either fills the caller-owned `rusage` it is
        // handed and returns 0, or touches nothing and returns -1. The struct
        // is only read on the success arm.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        unsafe { usage.assume_init() }
    };
    let raw = u64::try_from(usage.ru_maxrss).ok()?;
    Some(if cfg!(target_os = "macos") {
        raw / 1024
    } else {
        raw
    })
}

#[cfg(not(unix))]
#[must_use]
pub fn ru_maxrss_kb() -> Option<u64> {
    None
}

/// Background `VmRSS` sampler, keeping the running max. It backstops `VmHWM`
/// where the kernel watermark is unavailable; the sampler itself allocates, so
/// it deliberately keeps no per-sample series.
pub struct RssSampler {
    peak_kb: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl RssSampler {
    #[must_use]
    pub fn start(interval: Duration) -> Self {
        let peak_kb = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = {
            let (peak, stop) = (Arc::clone(&peak_kb), Arc::clone(&stop));
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(kb) = current_rss_kb() {
                        peak.fetch_max(kb, Ordering::Relaxed);
                    }
                    thread::sleep(interval);
                }
            })
        };
        Self {
            peak_kb,
            stop,
            handle: Some(handle),
        }
    }

    /// Stop the thread and return the peak.
    pub fn finish(mut self) -> u64 {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
        self.peak_kb.load(Ordering::Relaxed)
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.join().ok();
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    #[test]
    fn rss_probe_matches_platform_support() {
        // Linux must report a non-zero resident set for a running process;
        // elsewhere the probe is absent by design.
        if cfg!(target_os = "linux") {
            let stats = rss().expect("/proc/self/status readable on linux");
            assert!(stats.vm_rss_kb > 0);
            assert!(stats.vm_hwm_kb >= stats.vm_rss_kb);
        } else {
            assert!(rss().is_none());
        }
    }

    #[cfg(feature = "mem-probe")]
    #[test]
    fn counting_allocator_tracks_live_and_peak() {
        let before = heap_stats().expect("mem-probe enabled");
        let big: Vec<u8> = vec![7; 8 << 20];
        let during = heap_stats().expect("mem-probe enabled");
        assert!(during.live_bytes >= before.live_bytes + (8 << 20));
        drop(big);
        let after = heap_stats().expect("mem-probe enabled");
        assert!(after.live_bytes < during.live_bytes);
        assert!(after.peak_bytes >= during.live_bytes);
    }
}
