//! herdr plugin for pond: sync-on-idle and a read-only session desk.
//!
//! The desk draws with ratatui over crossterm directly - pond's CLI output
//! stack rule covers the pond binary, not this crate. `unsafe_code` is denied,
//! so process groups, signals and locks go through `nix`'s safe wrappers.

mod api;
mod config;
mod daemon;
mod desk;
#[cfg(test)]
mod fake_pond;
mod herdr;
mod hook;
mod serve;
mod types;

use std::process::ExitCode;
use std::sync::Arc;

use crate::types::{DeskContext, DeskExit};

const USAGE: &str = "usage: herdr-pond open|tui|hook [--worker <adapter>]|serve-daemon [--owner]";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (command, rest) = args
        .split_first()
        .map_or(("", &[][..]), |(c, r)| (c.as_str(), r));
    let result = match command {
        "open" => herdr::open_desk(),
        "tui" => desk_main(),
        "hook" => hook::run(rest),
        "serve-daemon" => daemon::run(rest),
        _ => Err(anyhow::anyhow!(USAGE)),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("herdr-pond {command}: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// The single-threaded runtime the daemon and the desk build by hand: the hook
/// path must never pay for one.
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

/// Runs the desk, then performs a jump only after it has restored the
/// terminal. The api (and any fallback serve it owns) is dropped when
/// `desk::run` returns, before herdr's CLI runs.
fn desk_main() -> anyhow::Result<()> {
    let context = DeskContext {
        project: herdr::context_project(),
    };
    let api = Arc::new(api::HttpApi::from_env()?);
    match desk::run(api, context)? {
        DeskExit::Quit => Ok(()),
        DeskExit::Jump { pane_id } => herdr::Herdr::from_env().agent_focus(&pane_id),
    }
}
