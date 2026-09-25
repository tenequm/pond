//! The plugin's own files: `HERDR_PLUGIN_CONFIG_DIR/config.toml` (re-read per
//! run, malformed falls back to defaults - plan 5.3) and the state-dir logs,
//! locks and atomic writes every headless leg shares.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, bail};
use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::Deserialize;

pub(crate) const CONFIG_FILE: &str = "config.toml";

/// Headless logs are the only record of what a detached leg did, so they are
/// kept but bounded: past this size the next writer starts the file over.
const LOG_CAP_BYTES: u64 = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Config {
    pub sync_on_idle: bool,
    pub pond_bin: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync_on_idle: true,
            pond_bin: None,
        }
    }
}

impl Config {
    /// Never fails: a missing file is the defaults, a malformed one is logged
    /// to `log` and also the defaults.
    pub(crate) fn load(config_dir: &Path, log: &Path) -> Self {
        let path = config_dir.join(CONFIG_FILE);
        let text = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Self::default(),
            Err(error) => {
                log_line(
                    log,
                    &format!("cannot read {}: {error}; using defaults", path.display()),
                );
                return Self::default();
            }
        };
        toml::from_str(&text).unwrap_or_else(|error| {
            log_line(
                log,
                &format!("malformed {}: {error}; using defaults", path.display()),
            );
            Self::default()
        })
    }

    /// [`Self::load`] then [`Self::resolve_pond`]: the `pond` to spawn right now.
    pub(crate) fn pond(config_dir: &Path, log: &Path) -> anyhow::Result<PathBuf> {
        Self::load(config_dir, log).resolve_pond(config_dir)
    }

    /// The `pond` every spawn runs: `pond_bin` when set, else a PATH lookup.
    /// herdr's PATH is the server's from whenever it started, so a lookup
    /// failure names the config key that fixes it.
    pub(crate) fn resolve_pond(&self, config_dir: &Path) -> anyhow::Result<PathBuf> {
        let config = config_dir.join(CONFIG_FILE);
        if let Some(pond) = &self.pond_bin {
            if !pond.is_absolute() {
                bail!(
                    "pond_bin = {:?} in {} must be an absolute path",
                    pond.display(),
                    config.display()
                );
            }
            if !is_executable(pond) {
                bail!(
                    "pond_bin = {:?} in {} is not an executable file",
                    pond.display(),
                    config.display()
                );
            }
            return Ok(pond.clone());
        }
        let path = std::env::var_os("PATH").unwrap_or_default();
        find_on_path("pond", &path).with_context(|| {
            format!(
                "pond not found on herdr's PATH; set pond_bin = \"/absolute/path/to/pond\" in {}",
                config.display()
            )
        })
    }
}

fn find_on_path(name: &str, path: &std::ffi::OsStr) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_absolute() && is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// Opens `path` for appending, creating parents, and starts it over once it
/// passes the cap. Children handed this file append at its live end.
pub(crate) fn open_log(path: &Path) -> io::Result<File> {
    ensure_parent(path)?;
    if fs::metadata(path).is_ok_and(|meta| meta.len() > LOG_CAP_BYTES) {
        OpenOptions::new().write(true).open(path)?.set_len(0)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

/// Points every stdio end of `command` at /dev/null or `log`: an inherited
/// pipe would pin a herdr command slot, or corrupt the desk's terminal.
pub(crate) fn log_stdio<'a>(command: &'a mut Command, log: &Path) -> io::Result<&'a mut Command> {
    let out = open_log(log)?;
    let err = out.try_clone()?;
    Ok(command.stdin(Stdio::null()).stdout(out).stderr(err))
}

/// Best effort: a headless leg has nowhere else to report a failed log write.
pub(crate) fn log_line(path: &Path, message: &str) {
    if let Ok(mut file) = open_log(path) {
        let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let _ = writeln!(file, "{now} [{}] {message}", std::process::id());
    }
}

/// A non-blocking exclusive flock on `path`, held until the guard drops.
/// `None` means another process holds it.
pub(crate) fn try_lock(path: &Path) -> io::Result<Option<Flock<File>>> {
    ensure_parent(path)?;
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => Ok(Some(lock)),
        Err((_, Errno::EWOULDBLOCK)) => Ok(None),
        Err((_, errno)) => Err(io::Error::from(errno)),
    }
}

/// Temp file + rename, so a reader never sees a half-written file.
pub(crate) fn write_atomic(path: &Path, contents: &[u8]) -> io::Result<()> {
    ensure_parent(path)?;
    let mut temp = path.as_os_str().to_owned();
    temp.push(format!(".tmp.{}", std::process::id()));
    let temp = PathBuf::from(temp);
    fs::write(&temp, contents)?;
    fs::rename(&temp, path).inspect_err(|_| {
        let _ = fs::remove_file(&temp);
    })
}

fn ensure_parent(path: &Path) -> io::Result<()> {
    path.parent().map_or(Ok(()), fs::create_dir_all)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::fake_pond::{Sandbox, write_script};

    #[test]
    fn missing_config_is_the_defaults() {
        let sandbox = Sandbox::new();
        let config = Config::load(&sandbox.config_dir(), &sandbox.path("log"));
        assert_eq!(config, Config::default());
        assert!(config.sync_on_idle);
    }

    #[test]
    fn config_keys_are_read() {
        let sandbox = Sandbox::new();
        sandbox.write_config("sync_on_idle = false\npond_bin = \"/opt/pond\"\n");
        let config = Config::load(&sandbox.config_dir(), &sandbox.path("log"));
        assert!(!config.sync_on_idle);
        assert_eq!(config.pond_bin, Some(PathBuf::from("/opt/pond")));
    }

    #[test]
    fn malformed_config_is_logged_and_defaulted() {
        let sandbox = Sandbox::new();
        let log = sandbox.path("state/sync.log");
        for text in [
            "sync_on_idle = \"yes\"",
            "not toml [",
            "sync_on_idel = false",
        ] {
            sandbox.write_config(text);
            assert_eq!(Config::load(&sandbox.config_dir(), &log), Config::default());
        }
        let logged = fs::read_to_string(&log).unwrap();
        assert_eq!(logged.matches("malformed").count(), 3, "{logged}");
    }

    #[test]
    fn pond_bin_must_be_absolute_and_executable() {
        let sandbox = Sandbox::new();
        let relative = Config {
            pond_bin: Some(PathBuf::from("bin/pond")),
            ..Config::default()
        };
        let error = relative.resolve_pond(&sandbox.config_dir()).unwrap_err();
        assert!(error.to_string().contains("absolute"), "{error}");

        let missing = Config {
            pond_bin: Some(sandbox.path("nope/pond")),
            ..Config::default()
        };
        assert!(missing.resolve_pond(&sandbox.config_dir()).is_err());

        let pond = write_script(&sandbox.path("bin/pond"), "exit 0");
        let set = Config {
            pond_bin: Some(pond.clone()),
            ..Config::default()
        };
        assert_eq!(set.resolve_pond(&sandbox.config_dir()).unwrap(), pond);
    }

    #[test]
    fn path_lookup_skips_non_executables_and_relative_dirs() {
        let sandbox = Sandbox::new();
        fs::create_dir_all(sandbox.path("plain")).unwrap();
        fs::write(sandbox.path("plain/pond"), "").unwrap();
        let pond = write_script(&sandbox.path("exec/pond"), "exit 0");
        let path = std::env::join_paths([
            PathBuf::from("relative"),
            sandbox.path("plain"),
            sandbox.path("exec"),
        ])
        .unwrap();
        assert_eq!(find_on_path("pond", &path), Some(pond));
        assert_eq!(find_on_path("pond", std::ffi::OsStr::new("")), None);
    }

    #[test]
    fn log_starts_over_past_the_cap() {
        let sandbox = Sandbox::new();
        let log = sandbox.path("state/sync.log");
        fs::create_dir_all(sandbox.path("state")).unwrap();
        fs::write(
            &log,
            vec![b'x'; usize::try_from(LOG_CAP_BYTES).unwrap() + 1],
        )
        .unwrap();
        log_line(&log, "fresh");
        let text = fs::read_to_string(&log).unwrap();
        assert!(
            text.ends_with("fresh\n") && text.len() < 200,
            "{}",
            text.len()
        );
    }

    #[test]
    fn lock_is_exclusive_until_dropped() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("state/worker.lock");
        let held = try_lock(&path).unwrap().expect("first lock");
        assert!(try_lock(&path).unwrap().is_none());
        drop(held);
        assert!(try_lock(&path).unwrap().is_some());
    }

    #[test]
    fn atomic_write_replaces_whole_file() {
        let sandbox = Sandbox::new();
        let path = sandbox.path("state/endpoint");
        write_atomic(&path, b"one").unwrap();
        write_atomic(&path, b"two").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "two");
        assert_eq!(fs::read_dir(sandbox.path("state")).unwrap().count(), 1);
    }
}
