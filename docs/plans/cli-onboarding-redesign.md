# CLI onboarding redesign: zero-to-hero UX for humans and agents

Status: IMPLEMENTED, validated green (fmt + clippy -D warnings + full test suite), awaiting the single combined commit to main (user-approved scope and commit shape). This doc was the handoff; it is kept as the design record.

## Progress checkpoint (update as you go)

- [x] Cargo.toml: added clap_complete, cliclack, ctrlc, shellexpand; dialoguer removed
- [x] config.rs: `expand_home_under` now shellexpand::full_with_context_no_errors (`~` + `$VAR`), added `contract_home_under` + `contract_home`, `config::display()` contracts `$HOME` to `~` for local URLs
- [x] config.rs: legacy-storage error recipe no longer echoes real creds (spec.md#storage-redaction); placeholder assertions updated
- [x] Sections 1-8 below: clap polish + completions + init wizard + schedule cluster + storage cluster + CI/packaging + SKILL.md/spec/README + help snapshots
- [x] Post-implementation polish pass (4-agent review): 7 findings fixed
- [x] Full-surface UX verification against the release binary; 7 further findings fixed: non-circular `--yes` legacy repair, virtual-hosted endpoint guidance + `legacy_url_guess` arm ordering (plain `s3://` + endpoint folds the host in), empty-store `status`/`storage` render a "no data yet" state via `Store::initialized()` (parts-table probe), `CredentialsNotLoaded` classifies as auth-class (exit 3 + creds fix naming), bare `pond storage` title, MCP serverInfo = pond/CARGO_PKG_VERSION, aws_config IMDS + lance throttle WARNs silenced at default verbosity

## Decisions already made (do not relitigate)

- Approach A from the analysis: `pond init` is the single idempotent front door (setup + repair). NO `pond doctor` (signals fragility; `pond status` carries fix-naming diagnostics). NO `pond skill add` (pond is MCP-native; MCP instructions/resources + flawless --help are the agent surface; SKILL.md is discoverability-only).
- Wizard stack: cliclack 0.5 (clack-style intro/outro/log; built on console+indicatif already in tree). dialoguer is removed entirely. inquire rejected (crossterm). Custom cliclack theme skipped: default clack theme is cyan-accent and console honors NO_COLOR.
- Cancel contract: Esc (and Ctrl-C via a no-op `ctrlc::set_handler` installed ONLY in `pond init`, never in sync, which must stay killable) surfaces as io::ErrorKind::Interrupted from .interact(); handler does cliclack::outro_cancel("Cancelled - nothing written") + exit(1). Config is written only at the very end of the wizard.
- Paths: store as-written (`~/...`) in config; expand at read (`expand_home_under`, now also `$VAR`); contract `$HOME` to `~` on display and on every path `init` writes. Machine output (JSON/wire) stays absolute. `std::env::home_dir` is un-deprecated since 1.86 but pond reads HOME env directly (existing pattern, keep).
- Subagent model preference (session-wide): Agent calls use model opus by default, sonnet for mechanical tasks, never fable.

## Full scope (user picked "all of it")

### 1. clap help polish (main.rs derive attrs)

- Doc-comment discipline: first line = `-h` summary (short!); blank line; detail paragraph = `--help`. The `Sql` and `Migrate` variants currently have no blank line so their whole paragraph leaks into the root command list - fix all variants this way.
- `#[command(after_long_help = "Examples:\n  ...")]` per visible command, 2-5 copy-pasteable examples each; root gets a Getting-started block (init -> sync -> search) + the MCP add command (`claude mcp add -s user pond -- pond mcp`).
- Root `#[command(styles = STYLES, max_term_width = 100)]`; STYLES = const clap::builder::styling::Styles tying to pond palette: header/usage bold, literal cyan, placeholder dim (mirrors pond::output bold/cyan/dim).
- `long_version`: `LazyLock<String>` = `CARGO_PKG_VERSION (POND_BUILD_COMMIT? std::env::consts::ARCH-OS)` via option_env!("POND_BUILD_COMMIT"); `-V` stays short. Dockerfile gets `ARG POND_BUILD_COMMIT=` + ENV before the zigbuild RUN; moon build-dist passes `--build-arg POND_BUILD_COMMIT="$(git rev-parse --short HEAD)"`.
- New `#[derive(clap::Args)] struct StoreArgs { storage_path, config }` with doc comments and `#[command(next_help_heading = "Global options")]`, flattened as the LAST field of every data-touching variant (Status, Sync, Embed, Serve, Mcp, ConfigCmd::Show, Search, Index, Get, Export, Import, Sql; Storage cluster). Migrate-under-storage and ConfigCmd::Path keep bespoke `config` field. Match arms destructure `store` and use `store.storage_path` / `store.config`.
- Cli root: `#[command(flatten)] #[command(next_help_heading = "Global options")]` on the verbosity field so -v/-q group under the same heading.
- Subcommand order = declaration order in clap derive; reorder Command enum to workflow order: Init, Sync, Status, Search, Get, Sql, Serve, Mcp, Schedule, Storage, Export, Import, Config, Completions, then hidden Embed, Index.
- `flatten_help = true` on the Config variant (small parent). Not on root.
- `serve` about is "Run the server" - rewrite: "Run the HTTP API server (or MCP over stdio with --transport stdio)".

### 2. completions

- `clap_complete = "4.6"` dep (done). New visible command: `Completions { #[arg(value_enum)] shell: clap_complete::Shell }`, about "Generate shell completions"; handler: `use clap::CommandFactory; clap_complete::generate(shell, &mut Cli::command(), "pond", &mut io::stdout())`. after_long_help shows install lines per shell (bash: `pond completions bash > ~/.local/share/bash-completion/completions/pond`, zsh: `pond completions zsh > "${fpath[1]}/_pond"`, fish: `pond completions fish > ~/.config/fish/completions/pond.fish`).

### 3. pond init (the wizard)

Clap variant (no env binding on storage-path - init WRITES config, env would silently persist ephemeral state):

```
Init {
    /// Storage destination to write into config (skips the storage prompt).
    #[arg(long, value_parser = parse_storage_path)] storage_path: Option<StorageUrl>,
    /// Comma-separated adapter names to enable (skips the source picker).
    #[arg(long, value_delimiter = ',')] adapters: Option<Vec<String>>,
    /// Register pond sync on a schedule (15m|1h|6h|1d). Opt-in: --yes alone never schedules.
    #[arg(long, value_enum)] schedule: Option<ScheduleEvery>,
    /// Skip MCP registration.
    #[arg(long)] skip_mcp: bool,
    /// Accept defaults for everything not covered by a flag (non-interactive).
    #[arg(long, short = 'y')] yes: bool,
    /// Ignore existing config values and start from built-in defaults.
    #[arg(long)] force: bool,
    /// Config file to write (default: ~/.config/pond/config.toml).
    #[arg(long, env = "POND_CONFIG")] config: Option<PathBuf>,
}
```

Flow (implement as src/init.rs, bin-only module declared `mod init;` in main.rs; justify: interactive-wizard subsystem, keeps main.rs from bloating):

1. config_file = config_path(config). Non-TTY + !yes + no flags -> bail naming the fix: "stdin is not a terminal; run `pond init --yes` or pass --storage-path/--adapters". Any flag present + non-TTY -> remaining sections take defaults (the next.js anti-hang lesson).
2. Load existing DocumentMut (toml_edit; tolerate absent file). --force = treat as empty for prefill but still single-write at end. Legacy-format config: Config::load errors with the rewrite recipe; init catches that specific case and offers to REWRITE the [storage] map into the new shape itself (parse old keys, build new doc) - this is the repair path the error message can now point at ("or run `pond init`").
3. ctrlc::set_handler(|| {}) (once, init only). intro("pond init").
4. Storage section: current = flag > existing [storage].path > contracted default "~/.local/share/pond". Interactive: cliclack::input("Where should pond store its data?").default_input(current).validate(StorageUrl::parse). Remote URL chosen -> inline end-to-end probe via pond::substrate::storage_check with cliclack::spinner; NoCreds/Auth failure -> log::warning naming [creds.default] + POND_CREDS_* + confirm("Keep this destination anyway?") default false.
5. Sources section: items = union over registry order of configured entries (prefill enabled state, hint = contracted path) and fresh probe_default candidates (preselected). cliclack::multiselect, required(false). --adapters validates against known_names; a name that probe finds nothing for and has no config entry -> bail "not detected; pass a path via `pond sync <name> --source-dir <path>` or add [sources.<name>] manually". Writes: picked+new -> insert table (contract path values under home) + enabled=true first; picked+existing-disabled -> flip enabled=true preserving other keys; unpicked+configured-enabled -> enabled=false; unpicked+fresh -> enabled=false stub (sticky decline, mirrors sync semantics).
6. Embeddings: informational log only ("embeddings  intfloat/multilingual-e5-small (default) - override under [embeddings]"). No prompt: a model swap forces re-embedding, too heavy for a wizard default.
7. MCP section (unless --skip-mcp): detect agent CLIs on PATH. claude: `claude mcp get pond` exit 0 -> "already registered"; else confirm + run `claude mcp add -s user pond -- pond mcp`, report. codex detected: do NOT auto-write; cliclack::note with the exact command (`codex mcp add pond -- pond mcp`). Other adapters: nothing.
8. Schedule section: opt-in. Interactive: confirm("Run pond sync automatically?") default FALSE; on yes, select every (15m/1h/6h/1d, initial 1h) and call the schedule-start routine AFTER config write succeeds. --schedule flag = non-interactive opt-in. --yes alone NEVER schedules.
9. Summary: cliclack::note("Plan", storage line + per-source enable/disable lines + config path); confirm("Write config?") default true (skipped by --yes). Decline -> outro_cancel + exit(1).
10. Single write of the DocumentMut. If doc string unchanged -> outro "Already set up - nothing to change." Else outro "Config written to <contracted path>" + note next steps: pond sync; claude mcp line if skipped; pond --help.
11. Wizard error helper: fn wiz<T>(io::Result<T>) -> anyhow::Result<T>; Interrupted -> outro_cancel + exit(1); other -> context("prompt failed").

discovery.rs changes: extract `pub(crate) fn apply_to_doc(doc: &mut DocumentMut, accepts: &[Candidate], declines: &[&str])` from persist_accept/persist_decline so init shares the exact table-shaping (enabled-first) logic with one write. Migrate prompt_and_persist (MultiSelect) and prompt_each (Confirms) to cliclack equivalents: multiselect.item(name.clone(), name, hint).initial_values(all).required(false is NOT wanted here - sync picker keeps requiring >=1? current code bails on empty selection, keep that); confirm(...).initial_value(true). Update the non-TTY bail to name the fix: "[sources] is empty and stdin is not a terminal; run `pond init --yes` to enable detected sources, or add a [sources.<adapter>] entry to {path} (known adapters: ...)". hint_for displays contracted paths.

### 4. pond schedule cluster

New file src/schedule.rs, bin-only (`mod schedule;` in main.rs only; OS-scheduler integration, unit tests inside). Windows: runtime bail "pond schedule is not supported on Windows yet".

```
Schedule { #[command(subcommand)] command: ScheduleCmd }
ScheduleCmd::Start { #[arg(long, value_enum, default_value_t = ScheduleEvery::H1)] every: ScheduleEvery }
ScheduleCmd::Stop
ScheduleCmd::Status
ScheduleCmd::Logs { #[arg(long, default_value_t = 50)] lines: usize }
```

ScheduleEvery { M15, H1, H6, D1 } with #[value(name = "15m")] etc.

- Scheduled job: `<stable-pond-path> sync -q` (NOT --yes: cron must never auto-enable fresh adapters). Stable path: search PATH entries for an executable `pond` (std::env::split_paths), fallback std::env::current_exe(). This lands on /opt/homebrew/bin/pond (survives brew upgrades; the versioned-Cellar-path cron breakage is a known past incident) or ~/.cargo/bin/pond or the nix profile path.
- Log file: $XDG_STATE_HOME/pond/sync.log else ~/.local/state/pond/sync.log (create parent).
- macOS: launchd ONLY (cron is refused on macOS: no user context, TCC breakage, sleep-spanning jobs dropped). Plist ~/Library/LaunchAgents/sh.pond.sync.plist, Label sh.pond.sync, ProgramArguments [bin, sync, -q], StartInterval secs(every), StandardOutPath/StandardErrorPath = log, ProcessType Background, XML comment "created and maintained by pond; edits may be replaced". Register: `launchctl bootout gui/$UID/sh.pond.sync` (ignore failure) then `launchctl bootstrap gui/$UID <plist>` (fail loud). UID via `id -u` (pond denies unsafe, no libc::getuid).
- Idempotency: byte-compare existing plist/unit content; identical + registered -> no-op "already scheduled (every X)".
- Linux: probe systemd with `systemctl --user list-timers` success; write ~/.config/systemd/user/pond-sync.service (Type=oneshot, ExecStart=<bin> sync -q) + pond-sync.timer (OnBootSec=2m, OnUnitActiveSec=<interval>, Persistent=true, WantedBy=timers.target), both stamped; `systemctl --user daemon-reload` + `enable --now pond-sync.timer`. No systemd -> cron fence in crontab: `# BEGIN POND SYNC (maintained by pond; do not edit)` ... `# END POND SYNC`, entry with randomized minute (fastrand) `M */1 * * *`-style mapping (15m -> `M,M+15,M+30,M+45 * * * *` with M in 0..15; 1h -> `M * * * *`; 6h -> `M */6 * * *`; 1d -> `M 3 * * *`), command `<bin> sync -q >> <log> 2>&1`. Strip-then-append on rewrite; switching scheduler removes the other first.
- Stop: bootout+rm plist / disable --now + rm units + daemon-reload / strip fence. Succeeds (exit 0) when nothing was registered.
- Status: derive truth live (launchctl print gui/$UID/sh.pond.sync exit code; systemctl --user is-enabled pond-sync.timer; crontab fence present). Print "schedule  active (launchd, every 1h)" + log path, or "schedule  not configured - run `pond schedule start`". Exit 0 active / 1 not.
- Logs: macOS/cron -> print log path + last N lines; systemd -> exec `journalctl --user -u pond-sync.service -n N --no-pager`.
- `pond status` gains a schedule line using the same probe (active/not configured; not an error when absent).

### 5. pond storage cluster

```
Storage { #[command(subcommand)] command: Option<StorageCmd>, #[command(flatten)] store: StoreArgs }
```

Bare `pond storage` = show: resolved URL (contracted display), creds binding (existing describe()), TableSizes, RowTotals - i.e. the current render_status_header data, extracted into a shared helper.

- `StorageCmd::Check { url: Option<String> }` - MOVED from `pond config check` verbatim (same probe, same exit codes 0/2/3/4/5 documented in help). ConfigCmd::Check is DELETED (pre-release, breaking ok; spec 7.8 updated).
- `StorageCmd::Migrate { --from, --to }` - MOVED from top-level `pond migrate` verbatim. Top-level Migrate DELETED.
- `StorageCmd::Use { url: String, #[arg(long)] migrate: bool, #[arg(long)] no_migrate: bool, -y/--yes }` - the guided switch (the turso validate-then-activate pattern):
  1. parse + resolve + warn_unmatched_sets; storage_check the destination (spinner); failure -> exit with the check's exit code and its fix-naming error.
  2. Open current store and destination; if current has rows and destination is missing them: interactive confirm "Copy existing data to the new destination first?" default true (--migrate/--no-migrate/-y for non-TTY); on yes run migrate_between_stores + `optimize_indices` on destination + verify: re-read destination RowTotals, must be >= source totals, else abort WITHOUT flipping config ("destination row counts do not reconcile; config not changed").
  3. Flip [storage].path in config.toml via toml_edit (preserve comments), single write, contracted display in output.
  4. Outro lines: what changed, "previous data at <old> is untouched", and `pond storage check` / `pond status` as verification next steps.
- `pond sync --import-from` REMOVED (struct field, the only/skip interaction bail, did_archive_import plumbing). `pond import <archive>` is the one way. Check tests/ for --import-from references.

### 6. CI / packaging (.github/workflows/ci.yml + moon.yml + Dockerfile)

- moon.yml build-dist, after docker build, before the tar loop: chmod +x target/dist/pond-x86_64-unknown-linux-gnu; rm -rf completions && mkdir completions; run it 3x (`completions bash > completions/pond.bash`, `zsh > completions/_pond`, `fish > completions/pond.fish`); tar line becomes `tar -cJf "dist/pond-$t.tar.xz" pond completions`; cleanup adds rm -rf completions. Comment WHY: completion scripts are target-independent; nix cannot execute the binary pre-patchelf so the files must ship in the tarball. Windows zip unchanged.
- moon.yml build-dist docker invocation adds `--build-arg POND_BUILD_COMMIT="$(git rev-parse --short HEAD)"`.
- Dockerfile: `ARG POND_BUILD_COMMIT=` + `ENV POND_BUILD_COMMIT=$POND_BUILD_COMMIT` after COPY . . (before the zigbuild RUN).
- ci.yml brew formula heredoc: `def install` adds `generate_completions_from_executable(bin/"pond", "completions")` (Homebrew-blessed; runs the installed native binary); `test do` adds `assert_match "_pond", shell_output("#{bin}/pond completions zsh")`.
- ci.yml nix derivation heredoc: function args add `installShellFiles`; `nativeBuildInputs = [ installShellFiles ] ++ lib.optionals hostPlatform.isLinux [ autoPatchelfHook ];` installPhase adds `installShellCompletion --bash completions/pond.bash --zsh completions/_pond --fish completions/pond.fish` (escape $ as \$ ONLY where the existing heredoc does - check \${system} pattern; installShellCompletion lines have no $). Do NOT execute the binary in installPhase (autoPatchelf runs in fixup, after).

### 7. SKILL.md + spec + README

- Root SKILL.md: thin discoverability pointer per agentskills spec (frontmatter name: pond, description). Content ~30 lines ASCII: what pond is, install (brew/nix/cargo-binstall), `pond init` once, `claude mcp add -s user pond -- pond mcp`, the three MCP tools, "run `pond --help` - every command's --help carries examples". It must NOT duplicate MCP instructions; it points at surfaces.
- docs/spec.md section 7.8 (CLI verbs): add init, schedule, storage (with check/use/migrate under it); remove top-level migrate and config check; note import-from removal. Keep the edit minimal and factual.
- README: quickstart becomes init-first (pond init; pond sync; claude mcp add line stays).

### 8. Snapshot tests (Phase 4)

- main.rs `#[cfg(test)] mod tests`: help snapshots via clap::CommandFactory: render_long_help().to_string() for root + each visible subcommand (init, sync, status, search, get, sql, serve, mcp, schedule, storage, export, import, config, completions), insta::assert_snapshot! named help_<name>; snapshots land in src/snapshots/. max_term_width=100 on root keeps them deterministic (test stdout is not a tty). NO version string appears in long help, so release bumps do not churn snapshots.
- indicatif in_memory progress snapshots: SKIPPED deliberately - status/sync write to process stdout, not an injectable writer; retrofitting a sink is out of scope. Say so in the commit message if asked.
- config.rs: update legacy-recipe test (placeholder creds); extend expand_home tests with a $VAR case (use a var that figment::Jail can set, or read an always-present var like HOME - prefer Jail).
- discovery.rs non-tty test message assertion may need updating for the new `pond init` wording.

## Validation + commit (the contract)

- cargo fmt; cargo clippy -- -D warnings; cargo test - full output, NEVER piped through head/tail/grep. All green BEFORE commit.
- ONE combined commit, conventional: suggested `feat(cli)!: init wizard, schedule + storage clusters, completions, agent-grade help` with body listing the breaking moves (migrate -> storage migrate, config check -> storage check, sync --import-from removed) - the `!` matters for release-plz minor bump.
- Push to main directly (user-approved). Never force-push, never --no-verify, new commit (not amend) after any hook failure.

## Known sharp edges for the implementer

- clap derive: `#[command(next_help_heading = ...)]` on a flattened field is supported; if compilation disagrees, fall back to setting help_heading on each arg of StoreArgs.
- cliclack multiselect requires at least one pick unless .required(false); sync picker keeps the >=1 behavior (bails "no sources selected"), init uses required(false).
- cliclack .interact() cancel = io::ErrorKind::Interrupted; the no-op ctrlc handler is what converts Ctrl-C into that path (otherwise SIGINT default-kills mid-raw-mode). Install it ONLY in init.
- StorageUrl::parse accepts `~/...` already (goes through Lance uri_to_url after pond-side expansion? verify: substrate.rs:118 routes bare paths through uri_to_url - confirm ~ paths expand correctly when written to config and read back through resolve_storage_location; resolve_storage_location parses [storage].path via StorageUrl::parse, and StorageUrl::parse must expand ~ itself - CHECK substrate.rs handling of `~` and route through config::expand_home_under if it does not).
- The figment env-mirror tests run inside figment::Jail; $VAR expansion tests should too.
- main.rs tests module at ~3101 already exists; add to it, mind the #![allow] style at module top.
- Existing tests referencing moved/removed commands: grep tests/ for `migrate`, `config check`, `--import-from`, `prompt_and_persist` wording.
- comfy-table/indicatif/anstyle untouched. figment untouched. Lance pinned 7.0.0 untouched.
