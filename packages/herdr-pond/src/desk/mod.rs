//! The session desk TUI (plan section 6). Owner: agent C - everything under
//! `desk/` and nothing outside it.

use std::sync::Arc;

use crate::types::{Api, DeskContext, DeskExit};

/// Builds its own current-thread runtime and owns the terminal until it
/// returns; the terminal is restored on every return path.
pub(crate) fn run(_api: Arc<dyn Api>, _context: DeskContext) -> anyhow::Result<DeskExit> {
    anyhow::bail!("not implemented")
}
