#[cfg(target_os = "linux")]
#[inline]
#[allow(unsafe_code)]
pub fn trim_allocator() {
    // SAFETY: malloc_trim has no caller-side safety invariants.
    let released = unsafe { libc::malloc_trim(0) != 0 };
    tracing::debug!(released, "trimmed glibc allocator");
}

#[cfg(not(target_os = "linux"))]
#[inline]
pub fn trim_allocator() {}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    #[test]
    fn allocator_trim_is_callable() {
        super::trim_allocator();
    }
}
