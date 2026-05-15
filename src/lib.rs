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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub attempts: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            initial_backoff: Duration::from_millis(300),
            max_backoff: Duration::from_secs(5),
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
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("namespace unknown: {0}")]
    NamespaceUnknown(String),
    #[error("storage unavailable: {0}")]
    Storage(#[from] anyhow::Error),
    #[error("internal error: {0}")]
    Internal(String),
}
