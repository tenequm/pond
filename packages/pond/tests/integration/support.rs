//! Helpers for the suites that drive the compiled `pond` binary.
#![allow(clippy::expect_used)]

use std::process::{Child, Command};
use tempfile::TempDir;

/// A `pond` invocation confined to `temp`: no host config, store, state, or
/// sources, and plain output.
pub(crate) fn sandboxed_pond(temp: &TempDir) -> Command {
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("create home");
    let mut command = Command::new(env!("CARGO_BIN_EXE_pond"));
    // clap's env fallbacks and the config's `POND_*` mirror would otherwise
    // hand a dev shell's store (possibly the real remote one) to the test.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("POND_") {
            command.env_remove(key);
        }
    }
    command
        .env("HOME", &home)
        .env("USERPROFILE", &home)
        .env("APPDATA", temp.path().join("config"))
        .env("LOCALAPPDATA", temp.path().join("data"))
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_STATE_HOME", temp.path().join("state"))
        // A host RUST_LOG replaces the CLI's own `-v` filter and adds stderr
        // lines the tests do not expect.
        .env_remove("RUST_LOG")
        .env("NO_COLOR", "1");
    command
}

/// Kills and reaps the child on drop, so a failed assertion never leaks a
/// running `pond`.
pub(crate) struct ChildGuard(pub(crate) Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
