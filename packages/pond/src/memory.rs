//! Allocator maintenance. glibc parks freed pages on its per-arena free lists
//! rather than returning them, so a process that peaked stays resident at that
//! peak long after the peak's memory is free (#61, #245). `malloc_trim` asks
//! for the top of each arena back.

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

#[cfg(test)]
mod tests {
    /// The FFI branch only has to exist and return; asserting on RSS here
    /// would be asserting on the kernel's bookkeeping, not on this code.
    #[test]
    fn allocator_trim_is_callable() {
        super::trim_allocator();
    }
}
