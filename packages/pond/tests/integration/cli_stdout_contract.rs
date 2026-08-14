//! stdout is a machine channel: `--format json` documents and MCP JSON-RPC
//! frames both travel on it, so one diagnostic line there corrupts a parse on
//! the other end. Nothing enforced that until this test. The `print_stdout`
//! clippy lint only sees the `print!` family, not the writer a tracing layer
//! resolves at runtime, which is how a `fmt::layer()` still carrying its
//! default stdout writer reached review (#129).
//!
//! The property asserted is the one that holds for every command and survives
//! output being reworded: raising verbosity changes stderr and leaves stdout
//! byte-for-byte identical.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use assert_cmd::Command;
use tempfile::TempDir;

/// `pond status` against a virgin data dir is the cheapest command that still
/// runs `init_tracing`: no network, no embedding model, no store to populate.
/// Its stdout is byte-stable across repeat runs in one environment, which is
/// what lets the two invocations below be compared directly - so both calls
/// must share `temp`.
fn status(temp: &TempDir, args: &[&str]) -> (String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_pond"))
        .arg("status")
        .args(args)
        .env("HOME", temp.path().join("home"))
        .env("USERPROFILE", temp.path().join("home"))
        .env("APPDATA", temp.path().join("config"))
        .env("LOCALAPPDATA", temp.path().join("data"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        // RUST_LOG replaces the whole CLI-level filter, which would make `-vv`
        // a no-op and the guard below fire. NO_COLOR keeps the human surface
        // unstyled so the two stdouts are comparable as bytes.
        .env_remove("RUST_LOG")
        .env("NO_COLOR", "1")
        .output()
        .expect("run pond status");

    assert!(
        out.status.success(),
        "`pond status {args:?}` exited {:?}: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    (
        String::from_utf8(out.stdout).expect("stdout is utf-8"),
        String::from_utf8(out.stderr).expect("stderr is utf-8"),
    )
}

#[test]
fn verbose_logging_never_reaches_stdout() {
    let temp = TempDir::new().expect("temp dir");
    std::fs::create_dir_all(temp.path().join("home")).expect("create home");

    let (quiet_stdout, quiet_stderr) = status(&temp, &[]);
    let (verbose_stdout, verbose_stderr) = status(&temp, &["-vv"]);

    // Guard against a vacuous pass: if `-vv` ever stops emitting records, every
    // assertion below holds for the wrong reason.
    assert!(
        verbose_stderr.len() > quiet_stderr.len(),
        "`-vv` added no stderr output, so this test proves nothing"
    );
    assert!(
        verbose_stderr.contains("DEBUG"),
        "`-vv` emitted no DEBUG records on stderr: {verbose_stderr}"
    );

    assert_eq!(
        quiet_stdout, verbose_stdout,
        "`-vv` changed stdout; diagnostics belong on stderr"
    );
    for marker in ["DEBUG", "TRACE", "INFO", "pond::"] {
        assert!(
            !verbose_stdout.contains(marker),
            "stdout carries the log marker {marker:?}:\n{verbose_stdout}"
        );
    }
}
