//! `pondw.exe` - the windowless launcher the Windows scheduled task runs.
//!
//! Task Scheduler executes its `Exec` action in the user's interactive session,
//! so a console-subsystem binary flashes a window on every tick. This one is
//! linked into the `windows` subsystem and has no console at all. It waits on
//! the child and exits with the child's code, so the task's `Last Result` is
//! pond's own status - a fire-and-forget shim reports its own success and hides
//! a failing sync.
//!
//! The task bakes exactly one shape:
//!
//! ```text
//! pondw.exe --log <file> -- <program> [args...]
//! ```
//!
//! Everything after `--` runs verbatim with stdout and stderr appended to
//! `<file>`, which is how `pond schedule logs` has anything to read: a
//! Task Scheduler action has no output redirection of its own, the way a
//! launchd plist has `StandardOutPath`. Not a general-purpose runner - it is
//! pond's own plumbing and takes no other form.
//!
//! Non-Windows builds compile to an empty `main`: this is a `src/bin` target,
//! which cargo discovers on every platform, and it ships only in the Windows
//! zip.

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
    const USAGE: i32 = 2;

    let mut args = std::env::args_os().skip(1);
    let (Some(flag), Some(log_path), Some(sep), Some(program)) =
        (args.next(), args.next(), args.next(), args.next())
    else {
        std::process::exit(USAGE);
    };
    if flag != OsStr::new("--log") || sep != OsStr::new("--") {
        std::process::exit(USAGE);
    }

    let Ok(mut log) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    else {
        // No console and no log: the exit code is the whole diagnostic.
        std::process::exit(USAGE);
    };
    let (Ok(out), Ok(err)) = (log.try_clone(), log.try_clone()) else {
        let _ = writeln!(log, "pondw: cannot duplicate the log handle");
        std::process::exit(1);
    };

    match Command::new(&program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(out))
        .stderr(Stdio::from(err))
        .creation_flags(CREATE_NO_WINDOW)
        .status()
    {
        // A child killed by a signal-equivalent has no code; report failure
        // rather than the success a bare `unwrap_or(0)` would invent.
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            let _ = writeln!(
                log,
                "pondw: failed to run {}: {error}",
                program.to_string_lossy()
            );
            std::process::exit(1);
        }
    }
}

#[cfg(not(windows))]
fn main() {}
