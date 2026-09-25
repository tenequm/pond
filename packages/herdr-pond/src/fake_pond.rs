//! A canned-response stand-in for `pond serve --socket`, so the HTTP client
//! is tested against real bytes on a real Unix socket - trait mocks alone
//! would let the client's serialization drift while every test stays green.
//! Also the sandbox dirs and fake `pond`/`herdr` scripts the shell-level
//! tests run.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use nix::sys::signal::kill;
use nix::unistd::Pid;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::api::{SEARCH_PATH, SQL_PATH, Socket};
use crate::config::CONFIG_FILE;
use crate::serve::{Endpoint, Origin, ServeDir};

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A fresh path under the temp dir: short, since a socket path is capped
/// near 100 bytes.
fn temp_path(kind: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-pond-{kind}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Golden bodies: the frozen `/v1/x/sql` and `/v1/search` contract.
pub(crate) mod golden {
    pub(crate) const SQL_READY: &str = r#"{"columns":["ready"],"rows":[{"ready":1}],"row_count":1,"truncated":false,"elapsed_ms":1}"#;

    pub(crate) const SQL_LISTING: &str = r#"{"columns":["session_id","last_ts","first_ts","message_count","source_agent","project"],"rows":[
        {"session_id":"s-live","last_ts":"2026-09-25T04:00:02.384123Z","first_ts":"2026-09-24T21:10:00.000000Z","message_count":94,"source_agent":"claude-code","project":"/home/me/pj/pond"},
        {"session_id":"s-old","last_ts":"2026-09-23T19:29:20.100000Z","first_ts":"2026-09-23T19:20:00.000000Z","message_count":3,"source_agent":"codex-cli","project":"/home/me/pj/pond/packages/pond"}
    ],"row_count":2,"truncated":false,"elapsed_ms":1712}"#;

    /// `s-old` has no user message, so it has no row.
    pub(crate) const SQL_TITLES: &str = r#"{"columns":["session_id","title"],"rows":[
        {"session_id":"s-live","title":"fix the timer re-arm"}
    ],"row_count":1,"truncated":false,"elapsed_ms":910}"#;

    pub(crate) const SQL_STATS: &str = r#"{"columns":["session_id","message_count","first_ts","last_ts"],"rows":[
        {"session_id":"s-live","message_count":94,"first_ts":"2026-09-24T21:10:00.000000Z","last_ts":"2026-09-25T04:00:02.384123Z"},
        {"session_id":"s-old","message_count":3,"first_ts":"2026-09-23T19:20:00.000000Z","last_ts":"2026-09-23T19:29:20.100000Z"}
    ],"row_count":2,"truncated":false,"elapsed_ms":380}"#;

    /// Nulls are omitted: `s-old`'s first message carries no host stamp.
    pub(crate) const SQL_HOSTS: &str = r#"{"columns":["session_id","host"],"rows":[
        {"session_id":"s-live","host":"ws-pond-01.lan"},
        {"session_id":"s-old"}
    ],"row_count":2,"truncated":false,"elapsed_ms":760}"#;

    /// Two rows share a timestamp: the pager must order and seek on the pair.
    pub(crate) const SQL_PAGE: &str = r#"{"columns":["message_id","timestamp","role","search_text"],"rows":[
        {"message_id":"m-a","timestamp":"2026-09-22T14:24:45.991000Z","role":"user","search_text":"check open issues\r\n\u001b[31mred\u001b[0m\tdone"},
        {"message_id":"m-b","timestamp":"2026-09-22T14:24:45.991000Z","role":"assistant","search_text":"I'll check the open issues."}
    ],"row_count":2,"truncated":false,"elapsed_ms":270}"#;

    pub(crate) const SQL_EMPTY: &str = r#"{"columns":["message_id","timestamp","role","search_text"],"rows":[],"row_count":0,"truncated":false,"elapsed_ms":90}"#;

    pub(crate) const SQL_ERROR: &str = r#"{"error":{"code":"validation_failed","message":"sql error: query exceeded the 30s limit; add a narrower WHERE or a LIMIT, or raise timeout_seconds","details":{}}}"#;

    pub(crate) const SEARCH: &str = r#"{"sessions":[{"session_id":"s-live","project":"/home/me/pj/pond","source_agent":"claude-code","session_messages_count":94,"matched_message_count":2,"matches":[
        {"message_id":"m-a","role":"user","timestamp":"2026-09-22T14:24:45.991Z","text":"the systemd timer stops re-arming","score":7.25},
        {"message_id":"m-c","role":"assistant","timestamp":"2026-09-22T14:30:00Z","text":"timer fixed","score":3.5,"parts_summary":[{"kind":"text"}]}
    ]}],"matched_total":2,"searchable_in_scope":4120,"has_more":false}"#;

    pub(crate) const SEARCH_OUT_OF_SCOPE: &str =
        r#"{"sessions":[],"matched_total":0,"searchable_in_scope":0,"has_more":false}"#;

    /// What axum sends for a body it cannot parse: plain text, not an envelope.
    pub(crate) const AXUM_REJECTION: &str =
        "Failed to deserialize the JSON body into the target type: missing field `query`";
}

/// One canned reply, chosen per request by [`FakePond`]'s router closure.
#[derive(Debug, Clone)]
pub(crate) struct Reply {
    status: u16,
    body: String,
    content_type: &'static str,
    delay: Duration,
}

impl Reply {
    pub(crate) fn json(body: &str) -> Self {
        Self::status(200, body)
    }

    pub(crate) fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
            content_type: "application/json",
            delay: Duration::ZERO,
        }
    }

    pub(crate) fn plain(status: u16, body: &str) -> Self {
        Self {
            content_type: "text/plain; charset=utf-8",
            ..Self::status(status, body)
        }
    }

    pub(crate) fn delayed(self, delay: Duration) -> Self {
        Self { delay, ..self }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recorded {
    pub path: String,
    pub host: String,
    pub body: String,
}

type Router = dyn Fn(&str, &str) -> Reply + Send + Sync;

/// An HTTP/1.1 server on a Unix socket answering each request through
/// `router(path, body)`. Every request is recorded, so tests can assert on
/// the SQL sent.
pub(crate) struct FakePond {
    pub socket: PathBuf,
    requests: Arc<Mutex<Vec<Recorded>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakePond {
    pub(crate) async fn start(
        router: impl Fn(&str, &str) -> Reply + Send + Sync + 'static,
    ) -> Self {
        let socket = temp_path("fake");
        let listener = UnixListener::bind(&socket).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let router: Arc<Router> = Arc::new(router);
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve_one(
                    stream,
                    Arc::clone(&router),
                    Arc::clone(&recorded),
                ));
            }
        });
        Self {
            socket,
            requests,
            task,
        }
    }

    /// Routes `/v1/x/sql` by a substring of the SQL and `/v1/search` to one
    /// body; anything else is a 404 like an old pond.
    pub(crate) async fn with_sql(routes: Vec<(&'static str, Reply)>, search: Reply) -> Self {
        Self::start(move |path, body| match path {
            SQL_PATH => routes
                .iter()
                .find(|(needle, _)| body.contains(needle))
                .map_or_else(
                    || Reply::status(400, golden::SQL_ERROR),
                    |(_, reply)| reply.clone(),
                ),
            SEARCH_PATH => search.clone(),
            _ => Reply::plain(404, ""),
        })
        .await
    }

    pub(crate) fn recorded(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    pub(crate) fn connect(&self) -> Socket {
        Socket::new(self.socket.clone()).unwrap()
    }
}

impl Drop for FakePond {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket);
    }
}

async fn serve_one(
    mut stream: UnixStream,
    router: Arc<Router>,
    recorded: Arc<Mutex<Vec<Recorded>>>,
) {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 8192];
    let (head_end, content_length) = loop {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buffer[..end]).to_ascii_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (end + 4, length);
        }
    };
    while buffer.len() < head_end + content_length {
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    }
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let path = head
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    let host = head
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("host:")
                .map(str::to_owned)
        })
        .unwrap_or_default()
        .trim()
        .to_owned();
    let body = String::from_utf8_lossy(&buffer[head_end..head_end + content_length]).into_owned();
    let reply = router(&path, &body);
    recorded.lock().unwrap().push(Recorded { path, host, body });
    tokio::time::sleep(reply.delay).await;
    let response = format!(
        "HTTP/1.1 {} X\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        reply.status,
        reply.content_type,
        reply.body.len(),
        reply.body
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// A throwaway directory standing in for the plugin's config and state dirs,
/// removed on drop.
pub(crate) struct Sandbox {
    root: PathBuf,
}

impl Sandbox {
    pub(crate) fn new() -> Self {
        let root = temp_path("test");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        Self { root }
    }

    pub(crate) fn path(&self, relative: &str) -> PathBuf {
        self.root.join(relative)
    }

    pub(crate) fn config_dir(&self) -> PathBuf {
        self.path("config")
    }

    pub(crate) fn state_dir(&self) -> PathBuf {
        self.path("state")
    }

    pub(crate) fn write_config(&self, text: &str) {
        std::fs::create_dir_all(self.config_dir()).unwrap();
        std::fs::write(self.config_dir().join(CONFIG_FILE), text).unwrap();
    }

    pub(crate) fn origin(&self) -> Origin {
        Origin {
            dir: ServeDir::new(&self.state_dir(), &self.path("herdr.sock")),
            config_dir: self.config_dir(),
        }
    }

    /// The lines of a sandbox file; a missing file has none.
    pub(crate) fn lines(&self, relative: &str) -> Vec<String> {
        std::fs::read_to_string(self.path(relative))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// A fake `pond serve` at `bin/pond`, set as `pond_bin`: records its argv
    /// in `calls` and its pid in `pid`, prints to both streams, and when
    /// given a `target` socket answers at its `--socket` path through a
    /// symlink to it (connect follows symlinks), then runs `after`.
    pub(crate) fn fake_serve(&self, target: Option<&Path>, after: &str) -> PathBuf {
        let publish = target.map_or_else(String::new, |target| {
            format!(r#"ln -s '{}' "$socket""#, target.display())
        });
        let pond = write_script(
            &self.path("bin/pond"),
            &format!(
                r#"printf '%s\n' "$*" >> '{calls}'
echo $$ > '{pid}'
echo "serve stdout"; echo "serve stderr" >&2
eval "socket=\${{$#}}"
{publish}
{after}"#,
                calls = self.path("calls").display(),
                pid = self.path("pid").display(),
            ),
        );
        self.write_config(&format!("pond_bin = \"{}\"\n", pond.display()));
        pond
    }

    /// The pid [`Self::fake_serve`] recorded.
    pub(crate) fn serve_pid(&self) -> u32 {
        self.lines("pid")[0].parse().unwrap()
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub(crate) fn ts(raw: &str) -> DateTime<Utc> {
    raw.parse().unwrap()
}

pub(crate) fn alive(pid: u32) -> bool {
    kill(Pid::from_raw(i32::try_from(pid).unwrap()), None).is_ok()
}

/// A socket path with nothing at it (connect fails with ENOENT).
pub(crate) fn missing_socket() -> Socket {
    Socket::new(temp_path("missing")).unwrap()
}

/// A socket file left by a listener that is gone (connect fails with
/// ECONNREFUSED), as a SIGKILLed serve leaves it.
pub(crate) fn stale_socket(path: &Path) -> Socket {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    drop(std::os::unix::net::UnixListener::bind(path).unwrap());
    Socket::new(path.to_path_buf()).unwrap()
}

pub(crate) fn endpoint(socket: &Path, token: &str) -> Endpoint {
    Endpoint {
        socket: socket.to_path_buf(),
        token: token.to_owned(),
    }
}

/// Writes an executable `/bin/sh` script, the stand-in for `pond` or `herdr`.
/// A child another test forks while the script is open for writing holds
/// that fd until it execs, and exec fails with ETXTBSY meanwhile - so the
/// script is dry-run (it exits at once under [`DRY_RUN`]) until it execs.
pub(crate) fn write_script(path: &Path, body: &str) -> PathBuf {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        format!("#!/bin/sh\n[ -z \"${DRY_RUN}\" ] || exit 0\n{body}\n"),
    )
    .unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    for _ in 0..100 {
        match std::process::Command::new(path).env(DRY_RUN, "1").status() {
            Err(error) if error.raw_os_error() == Some(nix::libc::ETXTBSY) => {
                std::thread::sleep(Duration::from_millis(10));
            }
            status => {
                assert!(status.unwrap().success());
                break;
            }
        }
    }
    path.to_path_buf()
}

const DRY_RUN: &str = "HERDR_POND_TEST_DRY_RUN";
