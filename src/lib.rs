pub mod adapter;
pub mod config;
pub mod embed;
pub mod handlers;
pub mod sessions;
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
    /// Lance manifest (design.md 2.3 inv 3).
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
    use std::io::{self, Write};

    use anyhow::Context;

    #[allow(clippy::print_stdout)]
    pub fn line(message: &str) -> anyhow::Result<()> {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "{message}").context("failed to write command output")
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
