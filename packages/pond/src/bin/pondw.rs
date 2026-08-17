//! `pondw.exe --log <file> -- <program> [args...]` - the windowless launcher
//! the Windows scheduled task Execs, and pond's own plumbing rather than a
//! general-purpose runner.
//!
//! Task Scheduler runs its action in the interactive session, so a
//! console-subsystem binary flashes a window every tick; this one is in the
//! `windows` subsystem. It waits and exits with the child's code, so the task's
//! `Last Result` is the sync's own. `--log` exists because an Exec action has
//! no `StandardOutPath` equivalent.
//!
//! Non-Windows builds compile to an empty `main`: cargo discovers `src/bin` on
//! every platform, and this ships only in the Windows zip.

#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
fn main() {
    use std::ffi::OsStr;
    use std::io::Write;
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    // The parent has no console, so a console-subsystem child would allocate
    // and show one of its own.
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    // Distinct so Task Scheduler's Last Result tells the two apart.
    const USAGE: i32 = 2;
    const SPAWN_FAILED: i32 = 3;

    let mut args = std::env::args_os().skip(1);
    let (Some(flag), Some(log_path), Some(sep), Some(program)) =
        (args.next(), args.next(), args.next(), args.next())
    else {
        std::process::exit(USAGE);
    };
    if flag != OsStr::new("--log") || sep != OsStr::new("--") {
        std::process::exit(USAGE);
    }

    // An unwritable log must not stop the sync: losing diagnostics is worse
    // than losing the run it was diagnosing. The child still gets its exit code
    // back to Task Scheduler either way.
    let mut log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .ok();
    let (out, err) = match log.as_ref().map(|f| (f.try_clone(), f.try_clone())) {
        Some((Ok(out), Ok(err))) => (Stdio::from(out), Stdio::from(err)),
        _ => (Stdio::null(), Stdio::null()),
    };

    match Command::new(&program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .creation_flags(CREATE_NO_WINDOW)
        .status()
    {
        // A child killed by a signal-equivalent has no code; report failure
        // rather than the success a bare `unwrap_or(0)` would invent.
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            if let Some(log) = log.as_mut() {
                let _ = writeln!(
                    log,
                    "pondw: failed to run {}: {error}",
                    program.to_string_lossy()
                );
            }
            std::process::exit(SPAWN_FAILED);
        }
    }
}

#[cfg(not(windows))]
fn main() {}
