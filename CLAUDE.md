# Pond - project instructions

## Documentation

- **Prefer ASCII.** Default to plain ASCII in Markdown and other repo docs - it keeps diffs clean, greps simple, and rendering predictable across terminals.

## Process

- Don't write migration notes or compatibility shims; pond is pre-release and breaking changes are free.
- Don't add or maintain changelog entries; pond has no changelog and doesn't need one.

## CLI output stack

- User-facing CLI output uses `clap` (parsing) + `indicatif` (progress + spinners) + `dialoguer` (interactive prompts) + `comfy-table` (width-adaptive tables) + `anstyle` (color, gated through `pond::output::paint` which honors `NO_COLOR` and non-TTY stdout). Don't reach for `crossterm`, `terminal_size`, `owo-colors`, `colored`, or `tabled`; pond standardizes on the stack above.
- New tabular surfaces should go through `pond::output` and the `new_table()` helper in `src/main.rs` so every command renders the same: borderless, dynamic-width, dim-bold headers.
