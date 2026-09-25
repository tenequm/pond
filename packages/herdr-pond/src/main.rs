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
        _ => Err(anyhow::anyhow!(
            "usage: herdr-pond open|tui|hook|serve-daemon"
        )),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("herdr-pond {command}: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Runs the desk, then performs a jump only after it has restored the terminal.
fn desk_main() -> anyhow::Result<()> {
    anyhow::bail!("not implemented")
}
