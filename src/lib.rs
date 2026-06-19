pub mod adapter;
pub mod config;
pub mod embed;
pub mod handlers;
pub mod render;
pub mod rowmap;
pub mod sessions;
pub mod sql;
pub mod substrate;
pub mod transport;
pub mod wire;

pub const PROTOCOL_VERSION: u16 = 1;

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub attempts: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// Symmetric jitter factor applied to the exponential backoff: the sleep
    /// is multiplied by `1.0 + jitter * uniform(-1.0, 1.0)` before clamping
    /// to `max_backoff`. De-correlates concurrent retriers on a contended
    /// Lance manifest (spec.md#lance-retry-jitter).
    pub jitter: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_backoff: Duration::from_millis(300),
            max_backoff: Duration::from_secs(5),
            jitter: 0.2,
        }
    }
}

pub mod output {
    use std::{
        io::{self, IsTerminal, Write},
        sync::OnceLock,
    };

    use anstyle::{AnsiColor, Style};
    use anyhow::Context;

    /// Whether the user-facing CLI surface should emit ANSI styling. Honors
    /// `NO_COLOR` (no-color.org) and falls back to plain text when stdout is
    /// not a TTY (piped to a file, captured by tests, ...). Cached so per-call
    /// overhead is a single pointer load.
    fn use_color() -> bool {
        static USE: OnceLock<bool> = OnceLock::new();
        *USE.get_or_init(|| std::env::var_os("NO_COLOR").is_none() && io::stdout().is_terminal())
    }

    /// Wrap `text` in `style`'s SGR sequence when color is enabled; return the
    /// raw text otherwise. The caller writes the result to stdout via
    /// [`line`].
    pub fn paint(text: &str, style: Style) -> String {
        if use_color() {
            format!("{}{text}{}", style.render(), style.render_reset())
        } else {
            text.to_owned()
        }
    }

    pub fn bold() -> Style {
        Style::new().bold()
    }
    pub fn dim() -> Style {
        Style::new().dimmed()
    }
    pub fn green() -> Style {
        Style::new().fg_color(Some(AnsiColor::Green.into()))
    }
    pub fn yellow() -> Style {
        Style::new().fg_color(Some(AnsiColor::Yellow.into()))
    }
    pub fn red() -> Style {
        Style::new().fg_color(Some(AnsiColor::Red.into()))
    }
    pub fn cyan() -> Style {
        Style::new().fg_color(Some(AnsiColor::Cyan.into()))
    }

    #[allow(clippy::print_stdout)]
    pub fn line(message: &str) -> anyhow::Result<()> {
        let mut stdout = io::stdout().lock();
        match writeln!(stdout, "{message}") {
            // Downstream (`pond ... | head`) closed the pipe after reading
            // what it wanted; the CLI convention is to stop quietly, not
            // surface "Broken pipe" as an error.
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => std::process::exit(0),
            result => result.context("failed to write command output"),
        }
    }

    /// Stderr counterpart to [`line`]. Per rust-cli/book stdout-vs-stderr
    /// discipline, anything that is not "the result" (progress, prompts,
    /// disclaimers, hints, interim status) belongs here so piping stdout to a
    /// file or another command yields the machine-readable view alone.
    pub fn line_err(message: &str) -> anyhow::Result<()> {
        let mut stderr = io::stderr().lock();
        match writeln!(stderr, "{message}") {
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => std::process::exit(0),
            result => result.context("failed to write command meta"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("validation failed: {message}")]
    Validation {
        message: String,
        field: Option<String>,
        value: Option<Value>,
        expected: Option<String>,
    },
    #[error("not found: {message}")]
    NotFound {
        message: String,
        kind: String,
        pk: Value,
    },
    #[error("namespace unknown: {namespace}")]
    NamespaceUnknown { namespace: String },
    #[error("commit conflict after {attempts} attempt(s)")]
    Conflict { attempts: u8 },
    #[error("storage unavailable: {0}")]
    Storage(#[from] anyhow::Error),
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
            field: None,
            value: None,
            expected: None,
        }
    }

    pub fn validation_field(
        message: impl Into<String>,
        field: impl Into<String>,
        value: Option<Value>,
        expected: Option<String>,
    ) -> Self {
        Self::Validation {
            message: message.into(),
            field: Some(field.into()),
            value,
            expected,
        }
    }

    pub fn not_found(kind: impl Into<String>, pk: Value, message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
            kind: kind.into(),
            pk,
        }
    }

    pub fn namespace_unknown(namespace: impl Into<String>) -> Self {
        Self::NamespaceUnknown {
            namespace: namespace.into(),
        }
    }

    pub fn conflict(attempts: u8) -> Self {
        Self::Conflict { attempts }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }
}
