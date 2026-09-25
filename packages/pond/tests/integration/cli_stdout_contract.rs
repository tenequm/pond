//! stdout is a machine channel: `--format json` documents and MCP JSON-RPC
//! frames both travel on it, so one diagnostic line there corrupts a parse on
//! the other end. Nothing enforced that until this test. The `print_stdout`
//! clippy lint only sees the `print!` family, not the writer a tracing layer
//! resolves at runtime, which is how a `fmt::layer()` still carrying its
//! default stdout writer reached review (#129).
//!
//! The property asserted is the one that holds for every command and survives
//! output being reworded: raising verbosity changes stderr and leaves stdout
//! byte-for-byte identical. The pipe tests pin the other end of the channel: a
//! closed stdout is a quiet exit, and SIGPIPE never kills a running server.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use tempfile::TempDir;

use crate::support::sandboxed_pond;

/// `pond status` against a virgin data dir is the cheapest command that still
/// runs `init_tracing`: no network, no embedding model, no store to populate.
/// Its stdout is byte-stable across repeat runs in one environment, which is
/// what lets the two invocations below be compared directly - so both calls
/// must share `temp`.
fn status(temp: &TempDir, args: &[&str]) -> (String, String) {
    let out = sandboxed_pond(temp)
        .arg("status")
        .args(args)
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

/// `pond completions zsh | head` once aborted through human_panic, because
/// clap_complete unwraps its writes. A reader that hung up is ordinary Unix:
/// exit 0 and say nothing.
#[test]
fn closed_stdout_exits_quietly() {
    let temp = TempDir::new().expect("temp dir");
    // Closed before pond starts, so even output that fits the pipe buffer
    // hits the broken pipe.
    let (reader, writer) = std::io::pipe().expect("pipe");
    drop(reader);
    let out = sandboxed_pond(&temp)
        .args(["completions", "zsh"])
        .stdout(writer)
        .output()
        .expect("run pond completions");

    assert!(
        out.status.success() && out.stderr.is_empty(),
        "closed stdout was not a quiet exit ({:?}): {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A socket peer that hangs up mid-write raises SIGPIPE, and under the Unix
/// default disposition that killed a long-lived `pond serve` outright instead
/// of failing one connection. Raising the signal directly proves the
/// disposition without racing a real disconnect.
#[cfg(unix)]
#[test]
fn serve_survives_sigpipe() {
    use std::{
        io::{BufRead, BufReader, Read, Write},
        os::unix::process::ExitStatusExt,
        process::{Command, Stdio},
        sync::mpsc::{self, Receiver},
        time::{Duration, Instant},
    };

    use crate::support::ChildGuard;

    const DEADLINE: Duration = Duration::from_secs(30);

    fn lines(stream: impl Read + Send + 'static) -> Receiver<String> {
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        receiver
    }

    let temp = TempDir::new().expect("temp dir");
    let mut child = ChildGuard(
        sandboxed_pond(&temp)
            .args(["serve", "--transport", "stdio"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn pond serve"),
    );
    let stderr = lines(child.0.stderr.take().expect("stderr"));
    let stdout = lines(child.0.stdout.take().expect("stdout"));

    // Signal only after the ready line, so every disposition pond sets at
    // startup is already in effect.
    let started = Instant::now();
    loop {
        let line = stderr
            .recv_timeout(DEADLINE.saturating_sub(started.elapsed()))
            .expect("pond serve never reported ready");
        if line.contains("stdio MCP ready") {
            break;
        }
    }

    let kill = Command::new("kill")
        .args(["-PIPE", &child.0.id().to_string()])
        .status()
        .expect("run kill");
    assert!(kill.success(), "kill -PIPE failed: {kill:?}");

    let mut stdin = child.0.stdin.take().expect("stdin");
    writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"2025-06-18","capabilities":{{}},"clientInfo":{{"name":"sigpipe-test","version":"0"}}}}}}"#
    )
    .expect("send initialize");
    let response = stdout.recv_timeout(DEADLINE).unwrap_or_default();
    drop(stdin);

    let started = Instant::now();
    let exit = loop {
        if let Some(status) = child.0.try_wait().expect("poll pond serve") {
            break status;
        }
        assert!(
            started.elapsed() < DEADLINE,
            "pond serve did not exit after stdin closed"
        );
        std::thread::sleep(Duration::from_millis(50));
    };

    assert_eq!(exit.signal(), None, "pond serve was killed: {exit:?}");
    assert!(
        response.contains(r#""result""#),
        "pond serve stopped answering after SIGPIPE: {response:?}"
    );
}
