//! `pond init`: the idempotent setup-and-repair wizard.
//!
//! One pass over four sections - storage, adapters, MCP registration (which
//! also installs the bundled agent skill), sync schedule - then a single
//! `config.toml` write at the end. Every section is
//! answerable by a flag for non-interactive use; re-running against an
//! existing setup proposes only what would change. Bin-only module: the
//! wizard is a CLI surface, not library behavior.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail};
use pond::adapter::{self, Candidate};
use pond::config::{self, Config, CredsSet};
use pond::substrate::StorageUrl;
use toml_edit::{DocumentMut, Item, Table, Value, value};

use crate::schedule::{self, ScheduleEvery};

#[derive(Debug, clap::Args)]
pub(crate) struct InitArgs {
    /// Comma-separated adapter names to enable (skips the adapter picker).
    #[arg(long, value_delimiter = ',', value_name = "NAMES")]
    adapters: Option<Vec<String>>,
    /// Register `pond sync` on a schedule. Opt-in: `--yes` alone never schedules.
    #[arg(long = "every", value_enum, value_name = "EVERY")]
    schedule: Option<ScheduleEvery>,
    /// Skip MCP registration and the skill install.
    #[arg(long)]
    skip_mcp: bool,
    /// Accept defaults for everything not covered by a flag (non-interactive).
    #[arg(long, short = 'y')]
    yes: bool,
    /// Ignore existing config values and start from built-in defaults.
    #[arg(long)]
    force: bool,
}

/// The stock clack theme, except a cancelled prompt's footer renders as a
/// bare frame bar instead of "Operation cancelled." - every wizard cancel is
/// followed by `outro_cancel("Cancelled - nothing written")`, so the stock
/// text always doubled the message.
struct WizardTheme;

impl cliclack::Theme for WizardTheme {
    fn format_footer_with_message(&self, state: &cliclack::ThemeState, message: &str) -> String {
        use cliclack::ThemeState;
        format!(
            "{}\n",
            self.bar_color(state).apply_to(match state {
                ThemeState::Active => format!("└  {message}"),
                ThemeState::Cancel | ThemeState::Submit => "│".to_owned(),
                ThemeState::Error(err) => format!("└  {err}"),
            })
        )
    }
}

/// Whether the wizard's prompts still own the terminal. While true, the ctrlc
/// handler stays a no-op so cliclack can surface the interrupt through its own
/// raw-mode read; once the wizard hands off to the first sync it flips false
/// and Ctrl-C kills the process again - a long first sync must stay
/// interruptible.
static WIZARD_PROMPTS_ACTIVE: AtomicBool = AtomicBool::new(true);

/// The schedule the user opted into, parked here before the first sync runs
/// so the Ctrl-C handler can register it on the way out. The first-sync
/// banner promises "Ctrl-C is safe: the next sync resumes where this one
/// stopped" - that is only true if the interrupt path still installs the
/// schedule. `take()` keeps registration single-shot between the handler and
/// the normal path.
/// `(every, config_file)`: the registration pins the config path init
/// resolved, so a `--config-file` init writes to is the one the unit reads.
static PENDING_SCHEDULE: Mutex<Option<(ScheduleEvery, PathBuf)>> = Mutex::new(None);

/// Unwrap a prompt result. Esc and Ctrl-C surface from cliclack as
/// `Interrupted` (the wizard-scoped ctrlc handler in [`run`] is what keeps
/// SIGINT from killing the process mid-raw-mode); both cancel the whole
/// wizard - nothing has been written yet, so there is nothing to roll back.
fn wiz<T>(result: std::io::Result<T>) -> Result<T> {
    match result {
        Ok(inner) => Ok(inner),
        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
            let _ = cliclack::outro_cancel("Cancelled - nothing written");
            std::process::exit(1);
        }
        Err(error) => Err(error).context("prompt failed"),
    }
}

pub(crate) async fn run(
    args: InitArgs,
    storage_path: Option<StorageUrl>,
    config: Option<PathBuf>,
) -> Result<()> {
    let config_file = crate::config_path(config);
    // init writes config, so an env-sourced storage path would silently persist
    // ephemeral state. Honor `--storage-path` only when it came from argv, not
    // from `POND_STORAGE_PATH` (the global flag's env mirror).
    let storage_path = storage_path.filter(|_| {
        std::env::args().any(|a| a == "--storage-path" || a.starts_with("--storage-path="))
    });
    let interactive = std::io::stdin().is_terminal();
    let any_flag = storage_path.is_some()
        || args.adapters.is_some()
        || args.schedule.is_some()
        || args.skip_mcp;
    if !interactive && !args.yes && !any_flag {
        bail!(
            "stdin is not a terminal; run `pond init --yes` to accept defaults, or answer \
             sections with --storage-path / --adapters / --every"
        );
    }
    // With any flag (or --yes) present, unflagged sections take defaults
    // instead of prompting - a partially-flagged non-TTY run must never hang.
    let prompts = interactive && !args.yes;
    if prompts {
        WIZARD_PROMPTS_ACTIVE.store(true, Ordering::SeqCst);
        let _ = ctrlc::set_handler(|| {
            if !WIZARD_PROMPTS_ACTIVE.load(Ordering::SeqCst) {
                if let Ok(mut pending) = PENDING_SCHEDULE.lock()
                    && let Some((every, config_file)) = pending.take()
                    && let Err(error) = schedule::start(every, config_file)
                {
                    let _ =
                        pond::output::line_err(&format!("schedule registration failed: {error:#}"));
                }
                std::process::exit(130);
            }
        });
        cliclack::set_theme(WizardTheme);
    }

    let existing_text = if config_file.exists() {
        std::fs::read_to_string(&config_file)
            .with_context(|| format!("failed to read {}", config_file.display()))?
    } else {
        String::new()
    };
    let mut doc: DocumentMut = existing_text
        .parse()
        .with_context(|| format!("failed to parse {} as TOML", config_file.display()))?;

    cliclack::intro("pond init")?;

    // ---- repair: pre-redesign [storage] passthrough map -------------------
    let legacy_prefill = match extract_legacy_storage(&doc) {
        Some(legacy) => {
            // Whether the derived URL is complete enough to use as-is. A
            // virtual-hosted endpoint folds the bucket into the hostname, so the
            // guess comes out bucketless and the user must supply bucket/prefix.
            let usable = legacy_url_guess(&legacy)
                .as_deref()
                .is_some_and(|url| StorageUrl::parse(url).is_ok());
            if prompts {
                cliclack::log::warning(format!(
                    "{} uses the old [storage] passthrough format",
                    display_path(&config_file),
                ))?;
                let rewrite = wiz(
                    cliclack::confirm(
                        "Rewrite it now? (keys move to [creds.default]; the endpoint folds into the storage URL)",
                    )
                    .initial_value(true)
                    .interact(),
                )?;
                if !rewrite {
                    cliclack::outro_cancel(
                        "Cancelled - the old [storage] format must be rewritten first (`pond config schema` shows the new shape)",
                    )?;
                    std::process::exit(1);
                }
                if !usable {
                    cliclack::log::warning(
                        "the old endpoint folds the bucket into the hostname; add the bucket and prefix to the URL below: s3+https://<host>/<bucket>/<prefix>",
                    )?;
                }
            } else if storage_path.is_none() && !usable {
                // Non-interactive and the bucket can't be derived: bail with the
                // fix instead of re-raising the recipe (which would point back at
                // `pond init`, the command already running - a loop).
                bail!(
                    "the old [storage] map folds the bucket into the endpoint hostname, so the destination URL can't be derived automatically; re-run as `pond init --storage-path s3+https://<host>/<bucket>/<prefix>` (this same run then moves the credentials into [creds.default])"
                );
            }
            Some(apply_legacy_rewrite(&mut doc, &legacy))
        }
        None => None,
    };

    // ---- repair: pre-rename [sources.*] adapter map -> [adapters.*] ---------
    let migrated_adapters = rewrite_legacy_sources(&mut doc);
    if !migrated_adapters.is_empty() {
        cliclack::log::info(format!(
            "migrated [sources.*] -> [adapters.*]: {}",
            migrated_adapters.join(", "),
        ))?;
    }

    // ---- storage -----------------------------------------------------------
    let default_storage = if args.force {
        platform_default_storage()
    } else {
        legacy_prefill
            .flatten()
            .or_else(|| {
                doc.get("storage")
                    .and_then(Item::as_table_like)
                    .and_then(|table| table.get("path"))
                    .and_then(Item::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(platform_default_storage)
    };
    let chosen = pick_storage(storage_path.as_ref(), &mut doc, &default_storage, prompts).await?;
    let chosen_display = crate::storage_config_value(&chosen);
    let current_path = doc
        .get("storage")
        .and_then(Item::as_table_like)
        .and_then(|table| table.get("path"))
        .and_then(Item::as_str)
        .map(str::to_owned);
    // Assign only on change: re-running with the same answer must leave the
    // file byte-identical so the outro can say "nothing to change".
    if current_path.as_deref() != Some(chosen_display.as_str()) {
        crate::set_storage_path(&mut doc, &chosen_display);
    }

    // ---- adapters ----------------------------------------------------------
    let rows = adapter_rows(&doc, args.force);
    let picked = pick_adapters(&args, &rows, prompts)?;
    let mut fresh_accepts: Vec<Candidate> = Vec::new();
    let mut fresh_declines: Vec<&str> = Vec::new();
    for row in &rows {
        let want = picked.contains(&row.name);
        match &row.state {
            RowState::Fresh(candidate) => {
                if want {
                    fresh_accepts.push(candidate.clone());
                } else {
                    // Sticky decline, mirroring `pond sync` semantics: an
                    // explicit "no" persists so later runs stop re-asking.
                    fresh_declines.push(row.name.as_str());
                }
            }
            RowState::Configured { enabled } => {
                if *enabled != want {
                    doc["adapters"][row.name.as_str()]["enabled"] = value(want);
                }
            }
        }
    }
    adapter::apply_to_doc(&mut doc, &fresh_accepts, &fresh_declines)?;

    // ---- schedule (creating one is opt-in: --yes alone never schedules) ----
    // An ACTIVE registration is repair territory instead: re-registering
    // rewrites the unit with the current template (the config-file pin,
    // absolutized paths), which is how re-running init after an upgrade heals
    // a stale unit - so the prompt defaults to yes with the current cadence
    // preselected. Interactive only: a --yes run may be sandboxed (e2e drives
    // one), and a sandboxed repair would repoint the user's real unit at the
    // sandbox config.
    let active_schedule = schedule::status_snapshot();
    let schedule_choice: Option<ScheduleEvery> = match args.schedule {
        Some(every) => Some(every),
        None if prompts => {
            let prompt = if active_schedule.active {
                "Sync schedule found - re-register it? (refreshes the unit after an upgrade; No leaves it unchanged)"
            } else {
                "Run pond sync automatically on a schedule?"
            };
            let wanted = wiz(cliclack::confirm(prompt)
                .initial_value(active_schedule.active)
                .interact())?;
            if wanted {
                // cliclack renders hints only on the focused item, so the
                // recommendation rides in the label to stay visible.
                Some(wiz(cliclack::select("How often?")
                    .item(ScheduleEvery::M5, "every 5 minutes (recommended)", "")
                    .item(ScheduleEvery::M15, "every 15 minutes", "")
                    .item(ScheduleEvery::H1, "every hour", "")
                    .item(ScheduleEvery::H6, "every 6 hours", "")
                    .item(ScheduleEvery::D1, "daily", "")
                    .initial_value(active_schedule.every.unwrap_or(ScheduleEvery::M5))
                    .interact())?)
            } else {
                None
            }
        }
        None => None,
    };

    // ---- summary + the single write ----------------------------------------
    let mut plan = format!("storage    {chosen_display}");
    let enabled: Vec<&str> = picked.iter().map(String::as_str).collect();
    let disabled: Vec<&str> = rows
        .iter()
        .filter(|row| !picked.contains(&row.name))
        .map(|row| row.name.as_str())
        .collect();
    plan.push_str(&format!(
        "\nadapters   {}",
        if enabled.is_empty() {
            "(none)".to_owned()
        } else {
            enabled.join(", ")
        },
    ));
    if !disabled.is_empty() {
        plan.push_str(&format!("\ndisabled   {}", disabled.join(", ")));
    }
    if let Some(every) = schedule_choice {
        plan.push_str(&format!("\nschedule   pond sync every {}", every.label()));
        // A schedule pins the config path into the OS unit; a path the
        // templates cannot embed must fail here, before the config write and
        // the first sync - not inside registration after both already ran.
        schedule::reject_unembeddable(
            "config file",
            &config_file,
            "--config-file or POND_CONFIG_FILE, falling back to $XDG_CONFIG_HOME/pond/config.toml",
        )?;
    }
    plan.push_str(&format!("\nconfig     {}", display_path(&config_file)));
    cliclack::note("Plan", plan)?;
    if prompts {
        let write = wiz(cliclack::confirm("Write config?")
            .initial_value(true)
            .interact())?;
        if !write {
            cliclack::outro_cancel("Cancelled - nothing written")?;
            std::process::exit(1);
        }
    }

    let new_text = doc.to_string();
    let changed = new_text != existing_text;
    if changed {
        if let Some(parent) = config_file.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        crate::config::write_config_file(&config_file, &new_text)?;
    }

    // External side effects (MCP registration, the first sync, OS-scheduler
    // registration) run only AFTER the write gate: declining "Write config?"
    // exits above, so a cancelled wizard never mutates another tool's config
    // or the scheduler.
    if !args.skip_mcp {
        mcp_section(prompts, args.yes)?;
    }

    // ---- first sync, then the schedule --------------------------------------
    // The scheduler registers only AFTER the first sync completes: a fresh
    // systemd timer's OnBootSec elapse is already in the past, so it fires
    // ~immediately on registration - racing the (long, first) manual sync it
    // was set up to automate, and the two only slow each other down. Running
    // the first sync inside init also gives it the full progress UI.
    let run_first_sync = prompts
        && !picked.is_empty()
        && wiz(cliclack::confirm(
            "Run the first sync now? (recommended - it reads your full history)",
        )
        .initial_value(true)
        .interact())?;
    if run_first_sync && schedule_choice.is_some() {
        cliclack::log::info("the sync schedule will be registered once this first sync completes")?;
    }
    let next_steps = if run_first_sync {
        "pond status    check health\npond --help    explore the rest"
    } else if picked.is_empty() {
        // No adapters enabled: `pond sync` would dead-end, so don't list it.
        "pond adapters discover    enable an adapter once an agent CLI has history\npond status               check health\npond --help               explore the rest"
    } else {
        "pond sync      import your sessions\npond status    check health\npond --help    explore the rest"
    };
    cliclack::note("Next steps", next_steps)?;
    // The outro is the wizard UI's closing element, but the first sync still
    // runs after it - say so, or "Config written" reads as "done" and the
    // sync output looks like a second program starting.
    let outro_tail = if run_first_sync {
        " - starting the first sync..."
    } else {
        ""
    };
    if changed {
        cliclack::outro(format!(
            "Config written to {}{outro_tail}",
            display_path(&config_file)
        ))?;
    } else {
        cliclack::outro(format!("Already set up - nothing to change{outro_tail}"))?;
    }

    // Park the schedule for the Ctrl-C handler BEFORE the sync re-arms it, so
    // no window exists where an interrupt exits without registering.
    if let Ok(mut pending) = PENDING_SCHEDULE.lock() {
        *pending = schedule_choice.map(|every| (every, config_file.clone()));
    }
    let first_sync = if run_first_sync {
        WIZARD_PROMPTS_ACTIVE.store(false, Ordering::SeqCst);
        // Reload from disk so the sync sees exactly what this wizard wrote.
        let reloaded = Config::load(&config_file)?;
        crate::run_sync(
            &reloaded,
            &config_file,
            None,
            crate::SyncInvocation::defaults(),
        )
        .await
    } else {
        Ok(())
    };
    // Register the schedule even when the first sync failed: the scheduled
    // retry is the recovery path, and `pond status` now surfaces the failure.
    // take-and-register happens under one lock hold: the Ctrl-C handler
    // blocks on this mutex, so an interrupt landing mid-registration waits
    // for it to finish instead of exiting between the take and the start.
    let registration = match PENDING_SCHEDULE.lock() {
        Ok(mut pending) => pending
            .take()
            .map(|(every, config_file)| schedule::start(every, config_file)),
        Err(_) => None,
    };
    if let Some(outcome) = registration {
        match outcome {
            Ok(()) => {
                if !run_first_sync {
                    pond::output::line_err(&pond::output::paint(
                        "note: the first scheduled sync can start within minutes and takes a while on a full history; `pond sync` runs it in the foreground instead",
                        pond::output::dim(),
                    ))?;
                }
            }
            // A registration failure must not clobber the first sync's error.
            Err(error) if first_sync.is_ok() => return Err(error),
            Err(error) => {
                let _ = pond::output::line_err(&pond::output::paint(
                    &format!(
                        "schedule registration failed: {error:#} - run `pond schedule start` to retry"
                    ),
                    pond::output::red(),
                ));
            }
        }
    }
    first_sync
}

fn display_path(path: &Path) -> String {
    config::contract_home(path).display().to_string()
}

/// The platform-local default destination, contracted for display and for
/// writing into config (`~/.local/share/pond` rather than the expansion).
fn platform_default_storage() -> String {
    config::default_storage_path(
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from),
        config::home_dir(),
    )
    .ok()
    .and_then(|url| config::local_path(&url))
    .map(|path| config::contract_home(&path).display().to_string())
    .unwrap_or_else(|| "~/.local/share/pond".to_owned())
}

/// Why a local destination can never become a data dir: the target (or its
/// nearest existing ancestor) is a file. `None` for remote URLs and viable
/// paths. The path is NOT required to exist - `create_dir_all` at first use
/// handles that - and writability is not probed (there is no reliable check
/// without side effects); this rejects only what is structurally impossible.
fn structural_error(url: &StorageUrl) -> Option<String> {
    // Re-collect components: `uri_to_url` leaves a trailing slash on the
    // path, and `stat("/etc/hosts/")` is ENOTDIR (not "exists as a file").
    let path: PathBuf = config::local_path(url.canonical())?.components().collect();
    let mut probe = path.as_path();
    loop {
        if probe.exists() {
            if probe.is_dir() {
                return None;
            }
            let shown = config::contract_home(probe).display().to_string();
            return Some(if probe == path {
                format!("{shown} is an existing file, not a directory")
            } else {
                format!("{shown} is a file, so a data dir can't be created under it")
            });
        }
        probe = probe.parent()?;
    }
}

/// Resolve the storage section: flag > prompt > default. The interactive
/// prompt is a select with the local default always one keypress away;
/// free-text URL entry is the opt-in branch, not the first question. Remote
/// destinations are probed BEFORE they can land in config (a failing one
/// needs an explicit "keep anyway"); local ones get the structural check
/// instead - a file collision is permanent, so it is a hard reject with no
/// keep-anyway escape.
async fn pick_storage(
    storage_path: Option<&StorageUrl>,
    doc: &mut DocumentMut,
    default: &str,
    prompts: bool,
) -> Result<StorageUrl> {
    // Creds for the probe come from the in-progress document (plus the
    // POND_* env mirror) - nothing has been written yet. Mutable because an
    // inline capture below can add `[creds.default]` and re-probe.
    let mut creds = Config::load_str(&doc.to_string())
        .map(|config| config.creds)
        .unwrap_or_default();
    if let Some(chosen) = storage_path.cloned() {
        if let Some(reason) = structural_error(&chosen) {
            bail!(
                "--storage-path {}: {reason}; pick a directory",
                chosen.display()
            );
        }
        if !chosen.is_local()
            && let Err(reason) = probe_destination(&chosen, &creds).await
        {
            // On a TTY, offer to capture credentials inline and re-probe - so
            // `pond init --storage-path <bucket>` is one-command remote setup
            // rather than a bail telling you to add creds elsewhere first.
            if prompts
                && wiz(cliclack::confirm(format!(
                    "{} failed the check ({reason}). Enter credentials for it now?",
                    chosen.display()
                ))
                .initial_value(true)
                .interact())?
            {
                creds = capture_default_creds(doc)?;
                if let Err(reason) = probe_destination(&chosen, &creds).await {
                    bail!(
                        "--storage-path {} still failed after entering credentials: {reason}",
                        chosen.display()
                    );
                }
            } else {
                bail!(
                    "--storage-path {} failed the end-to-end check: {reason}; add credentials with `pond creds add` (or POND_CREDS_DEFAULT_*), then re-run",
                    chosen.display(),
                );
            }
        }
        return Ok(chosen);
    }
    if !prompts {
        let chosen = StorageUrl::parse(default)
            .with_context(|| format!("existing storage path {default:?} does not parse"))?;
        if let Some(reason) = structural_error(&chosen) {
            bail!(
                "configured storage {}: {reason}; re-run `pond init --storage-path <dir>` (or fix [storage].path in config)",
                chosen.display(),
            );
        }
        if !chosen.is_local()
            && let Err(reason) = probe_destination(&chosen, &creds).await
        {
            // The destination was already configured; a non-interactive
            // re-init must not brick on a transient outage - warn and keep.
            cliclack::log::warning(format!(
                "configured storage {} failed the end-to-end check: {reason}",
                chosen.display(),
            ))?;
        }
        return Ok(chosen);
    }
    let local = platform_default_storage();
    // "Keep current" earns its slot only when the carried-forward value is
    // distinct from the local default and could actually work; a broken one
    // stays reachable through the free-text branch, where the validator says
    // why it is broken instead of silently re-offering it.
    let keep = (default != local)
        .then(|| StorageUrl::parse(default).ok())
        .flatten()
        .filter(|url| structural_error(url).is_none())
        .map(|_| default.to_owned());
    // An unparseable carry-forward (the bucketless legacy guess) skips the
    // select and lands in the input, so "add the bucket and prefix to the
    // URL below" points at an actual URL input.
    let mut prefill =
        (default != local && StorageUrl::parse(default).is_err()).then(|| default.to_owned());
    loop {
        let text: String = match prefill.take() {
            Some(value) => wiz(cliclack::input("Storage path or URL")
                .default_input(&value)
                .validate(|input: &String| match StorageUrl::parse(input) {
                    Err(error) => Err(format!("{error:#}")),
                    Ok(url) => structural_error(&url).map_or(Ok(()), Err),
                })
                .interact())?,
            None => {
                let mut select = cliclack::select("Where should pond store its data?");
                // cliclack renders hints only on the focused item, so the
                // recommendation rides in the label to stay visible.
                select = match &keep {
                    Some(current) => select
                        .item('k', format!("Keep current ({current})"), "")
                        .item('l', format!("Locally ({local})"), "")
                        .initial_value('k'),
                    None => select
                        .item('l', format!("Locally ({local}) - recommended"), "")
                        .initial_value('l'),
                };
                select = select.item('o', "Somewhere else (path or S3 URL)", "");
                match wiz(select.interact())? {
                    'l' => local.clone(),
                    'k' => keep.clone().unwrap_or_else(|| local.clone()),
                    _ => {
                        prefill = Some(default.to_owned());
                        continue;
                    }
                }
            }
        };
        let chosen = StorageUrl::parse(&text)?;
        if chosen.is_local() {
            // Reachable for select-sourced values only; the free-text
            // validator already ran the same check inline.
            match structural_error(&chosen) {
                None => return Ok(chosen),
                Some(reason) => {
                    cliclack::log::warning(reason)?;
                    prefill = Some(text);
                    continue;
                }
            }
        }
        match probe_destination(&chosen, &creds).await {
            Ok(()) => return Ok(chosen),
            Err(reason) => {
                cliclack::log::warning(format!(
                    "{reason}\nCreds bind via [creds.default] in config or POND_CREDS_DEFAULT_* env (spec: creds scope match)."
                ))?;
                let action = wiz(cliclack::select("What now?")
                    .item(
                        'c',
                        "Enter credentials for this destination",
                        "saved as [creds.default]",
                    )
                    .item(
                        'l',
                        format!("Store locally instead ({local})"),
                        "safe default",
                    )
                    .item('e', "Edit the URL / fix creds and retry", "")
                    .item(
                        'k',
                        "Keep this destination anyway",
                        "writes a failing config",
                    )
                    .interact())?;
                match action {
                    'c' => {
                        creds = capture_default_creds(doc)?;
                        match probe_destination(&chosen, &creds).await {
                            Ok(()) => return Ok(chosen),
                            Err(reason) => {
                                cliclack::log::warning(format!(
                                    "still failing with those credentials: {reason}"
                                ))?;
                                prefill = Some(text);
                            }
                        }
                    }
                    'l' => {
                        return StorageUrl::parse(&local).with_context(|| {
                            format!("platform default storage path {local:?} does not parse")
                        });
                    }
                    'k' => return Ok(chosen),
                    _ => prefill = Some(text),
                }
            }
        }
    }
}

/// Capture an access key + hidden secret and write them as `[creds.default]`
/// (the catch-all set) into the in-progress doc, returning the refreshed creds
/// map for an immediate re-probe. init's one inline credential path - the
/// secret comes from a masked prompt, never argv (spec.md#storage-redaction).
fn capture_default_creds(doc: &mut DocumentMut) -> Result<BTreeMap<String, CredsSet>> {
    let access_key_id: String = wiz(cliclack::input("Access key ID").interact())?;
    let secret_access_key: String =
        wiz(cliclack::password("Secret access key").mask('*').interact())?;
    crate::set_creds_set(
        doc,
        "default",
        &access_key_id,
        &secret_access_key,
        None,
        None,
    );
    Ok(Config::load_str(&doc.to_string())
        .map(|config| config.creds)
        .unwrap_or_default())
}

/// End-to-end probe (same primitive as `pond storage check`) with a wizard
/// spinner. `Err` carries the operator-facing reason.
async fn probe_destination(
    url: &StorageUrl,
    creds: &BTreeMap<String, CredsSet>,
) -> std::result::Result<(), String> {
    let resolved = url.resolve(creds).map_err(|error| format!("{error:#}"))?;
    let spinner = cliclack::spinner();
    spinner.start(format!("Probing {}...", url.display()));
    match pond::substrate::storage_check(&resolved).await {
        Ok(()) => {
            spinner.stop(format!(
                "storage: {} reachable - conditional writes (OCC) supported",
                url.display(),
            ));
            Ok(())
        }
        Err(failure) => {
            spinner.error("storage probe failed");
            tracing::debug!("full probe failure: {failure:?}");
            Err(match failure.concise_cause() {
                Some(cause) => format!("{failure} ({cause})"),
                None => failure.to_string(),
            })
        }
    }
}

enum RowState {
    Configured { enabled: bool },
    Fresh(Candidate),
}

struct AdapterRow {
    name: String,
    hint: String,
    state: RowState,
    preselected: bool,
}

/// Union of configured `[adapters.*]` entries and fresh probe candidates, in
/// registry order (configured-but-unknown names append at the end so they
/// are never silently dropped). `force` resets preselection to "what the
/// probe detects", ignoring saved enables/declines.
fn adapter_rows(doc: &DocumentMut, force: bool) -> Vec<AdapterRow> {
    let configured = doc.get("adapters").and_then(Item::as_table_like);
    let candidates = adapter::discover(None);
    let candidate_for = |name: &str| candidates.iter().find(|c| c.name == name);
    let mut rows = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    for factory in adapter::registry() {
        let name = factory.name();
        seen.push(name);
        let entry = configured.and_then(|table| table.get(name));
        match (entry, candidate_for(name)) {
            (Some(item), candidate) => {
                let enabled = item
                    .as_table_like()
                    .and_then(|table| table.get("enabled"))
                    .and_then(Item::as_bool)
                    .unwrap_or(false);
                let contract =
                    |path: &str| config::contract_home(Path::new(path)).display().to_string();
                // A multi-path entry must show its own dirs: falling through
                // to the probed default would display a directory that is not
                // in the config at all.
                let hint = item
                    .as_table_like()
                    .and_then(|table| table.get("path"))
                    .and_then(|path| match path {
                        Item::Value(Value::Array(paths)) => Some(
                            paths
                                .iter()
                                .filter_map(|element| element.as_str())
                                .map(contract)
                                .collect::<Vec<_>>()
                                .join(", "),
                        ),
                        other => other.as_str().map(contract),
                    })
                    .or_else(|| candidate.map(|c| c.hint.clone()))
                    .unwrap_or_default();
                rows.push(AdapterRow {
                    name: name.to_owned(),
                    hint,
                    state: RowState::Configured { enabled },
                    preselected: if force { candidate.is_some() } else { enabled },
                });
            }
            (None, Some(candidate)) => rows.push(AdapterRow {
                name: name.to_owned(),
                hint: candidate.hint.clone(),
                state: RowState::Fresh(candidate.clone()),
                preselected: true,
            }),
            (None, None) => {}
        }
    }
    if let Some(table) = configured {
        for (name, item) in table.iter() {
            if seen.contains(&name) {
                continue;
            }
            let enabled = item
                .as_table_like()
                .and_then(|t| t.get("enabled"))
                .and_then(Item::as_bool)
                .unwrap_or(false);
            rows.push(AdapterRow {
                name: name.to_owned(),
                hint: "(unknown adapter)".to_owned(),
                state: RowState::Configured { enabled },
                preselected: !force && enabled,
            });
        }
    }
    rows
}

/// Resolve the adapters section: `--adapters` list (validated against known
/// names and against what is actually detectable) > interactive multiselect
/// (zero picks allowed) > the preselection defaults.
fn pick_adapters(args: &InitArgs, rows: &[AdapterRow], prompts: bool) -> Result<Vec<String>> {
    if let Some(requested) = &args.adapters {
        let known = adapter::known_names();
        for name in requested {
            if !known.contains(&name.as_str()) {
                bail!("unknown adapter {name:?}; known: {}", known.join(", "));
            }
            if !rows.iter().any(|row| &row.name == name) {
                bail!(
                    "adapter {name:?} was not detected on this machine and has no [adapters.{name}] entry; pass a path via `pond sync {name} --path <path>` or add the section manually"
                );
            }
        }
        return Ok(requested.clone());
    }
    if rows.is_empty() {
        // Say what pond was looking for and the command that retries - "add
        // TOML entries manually" assumes knowledge a first-run user lacks.
        cliclack::log::info(
            "adapters: none detected - pond reads local session history from agent CLIs \
             (Claude Code, Codex, OpenCode, Pi, Claude Cowork) and found none on this machine.\n\
             Once you have used one, run `pond adapters discover` to enable it; a custom \
             location can be added as an [adapters.<name>] entry (see `pond config schema`).",
        )?;
        return Ok(Vec::new());
    }
    if !prompts {
        return Ok(rows
            .iter()
            .filter(|row| row.preselected)
            .map(|row| row.name.clone())
            .collect());
    }
    let mut picker = cliclack::multiselect("Which adapters should pond sync?")
        .required(false)
        .initial_values(
            rows.iter()
                .filter(|row| row.preselected)
                .map(|row| row.name.clone())
                .collect(),
        );
    for row in rows {
        picker = picker.item(row.name.clone(), &row.name, &row.hint);
    }
    wiz(picker.interact())
}

/// Run an agent CLI at its resolved path, never by bare name: Windows
/// `CreateProcess` appends only `.exe`, so a `claude.cmd` shim is unspawnable
/// and goes through the interpreter instead. `raw_arg` because MSVCRT argument
/// quoting is not what cmd.exe parses (the trap `substrate::run_command`
/// documents); the outer quote pair survives cmd's strip-first-and-last rule
/// whether or not the path has spaces. `args` reach cmd unquoted, which both
/// call sites satisfy by passing fixed literals.
fn agent_command(bin: &Path, args: &[&str]) -> Command {
    #[cfg(windows)]
    if bin
        .extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"))
    {
        use std::os::windows::process::CommandExt as _;
        let shell = std::env::var_os("COMSPEC").unwrap_or_else(|| "cmd".into());
        let mut command = Command::new(shell);
        command.raw_arg(format!("/C \"\"{}\" {}\"", bin.display(), args.join(" ")));
        return command;
    }
    let mut command = Command::new(bin);
    command.args(args);
    command
}

/// Detect agent CLIs and offer MCP registration plus the bundled skill.
/// claude has an idempotent CLI surface (`mcp get` / `mcp add`), so pond
/// drives it directly; codex gets the exact command to run instead - pond
/// never edits another tool's config files behind the user's back.
fn mcp_section(prompts: bool, auto: bool) -> Result<()> {
    let claude = crate::find_on_path("claude");
    let codex = crate::find_on_path("codex");
    if claude.is_none() && codex.is_none() {
        cliclack::log::info(
            "mcp: no agent CLI detected - register later with `claude mcp add -s user pond -- pond mcp`",
        )?;
        return Ok(());
    }
    if let Some(claude) = &claude {
        let registered = agent_command(claude, &["mcp", "get", "pond"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        // One consent covers registration and the skill; a fresh install
        // that says yes here is not asked again for the skill write.
        let mut skill_consented = false;
        if registered {
            cliclack::log::success("mcp: pond is already registered in Claude Code")?;
        } else {
            let add = if prompts {
                wiz(cliclack::confirm(
                    "Register pond in Claude Code (MCP server + the pond skill)?",
                )
                .initial_value(true)
                .interact())?
            } else {
                auto
            };
            if add {
                let output = agent_command(
                    claude,
                    &["mcp", "add", "-s", "user", "pond", "--", "pond", "mcp"],
                )
                .output()
                .context("failed to run `claude mcp add`")?;
                if output.status.success() {
                    cliclack::log::success("mcp: registered in Claude Code (user scope)")?;
                } else {
                    cliclack::log::warning(format!(
                        "mcp: `claude mcp add` exited {}: {} - run `claude mcp add -s user pond -- pond mcp` manually",
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim(),
                    ))?;
                }
                skill_consented = true;
            } else {
                cliclack::log::info(
                    "mcp: skipped - register later with `claude mcp add -s user pond -- pond mcp`",
                )?;
            }
        }
        if registered || skill_consented {
            skill_section(prompts, auto, skill_consented)?;
        }
    }
    if codex.is_some() {
        cliclack::note(
            "codex detected",
            "register pond manually:\n  codex mcp add pond -- pond mcp",
        )?;
    }
    Ok(())
}

/// The skill pond installs for Claude Code, embedded so the installed copy
/// always matches the running binary (the same bytes `pond skill` prints).
const SKILL_MD: &str = include_str!("../SKILL.md");

const SKILL_DISPLAY_PATH: &str = "~/.claude/skills/pond/SKILL.md";

/// Sync the bundled skill into Claude Code's user skills dir.
fn skill_section(prompts: bool, auto: bool, consented: bool) -> Result<()> {
    let Some(home) = config::home_dir() else {
        cliclack::log::info(format!(
            "skill: no home dir (HOME/USERPROFILE unset) - install later by saving `pond skill` output to {SKILL_DISPLAY_PATH}",
        ))?;
        return Ok(());
    };
    let path = home
        .join(".claude")
        .join("skills")
        .join("pond")
        .join("SKILL.md");
    skill_sync(&path, prompts, auto, consented)
}

/// Three states: current (no-op), absent (install), differs (an older pond's
/// copy or a user edit - overwriting is asked about explicitly in prompt
/// mode, even when the combined registration consent already said yes).
fn skill_sync(path: &Path, prompts: bool, auto: bool, consented: bool) -> Result<()> {
    let existing = std::fs::read_to_string(path).ok();
    let (question, done) = match existing.as_deref() {
        Some(current) if current == SKILL_MD => {
            cliclack::log::success(format!("skill: up to date ({SKILL_DISPLAY_PATH})"))?;
            return Ok(());
        }
        Some(_) => (
            format!("Update the pond skill? ({SKILL_DISPLAY_PATH} differs from this pond version)"),
            "skill: updated",
        ),
        None => (
            format!("Install the pond skill for Claude Code? ({SKILL_DISPLAY_PATH})"),
            "skill: installed",
        ),
    };
    let write = if prompts && (existing.is_some() || !consented) {
        wiz(cliclack::confirm(question).initial_value(true).interact())?
    } else if prompts {
        true
    } else {
        auto || consented
    };
    if !write {
        cliclack::log::info(format!(
            "skill: skipped - install later by saving `pond skill` output to {SKILL_DISPLAY_PATH}",
        ))?;
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, SKILL_MD)
        .with_context(|| format!("failed to write {}", path.display()))?;
    cliclack::log::success(format!("{done} ({SKILL_DISPLAY_PATH})"))?;
    Ok(())
}

struct LegacyStorage {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    endpoint: Option<String>,
    path: Option<String>,
    /// The legacy addressing-style key. When true, the endpoint host carries
    /// the bucket as its leading label, which makes the URL guess exact.
    virtual_hosted: bool,
}

/// Recognize the pre-redesign `[storage]` passthrough map (the same shape
/// `config::detect_legacy_storage` errors on) and pull out the pieces the
/// rewrite needs. `None` when `[storage]` is absent or already in the new
/// path-only shape.
fn extract_legacy_storage(doc: &DocumentMut) -> Option<LegacyStorage> {
    let storage = doc.get("storage")?.as_table_like()?;
    let has_extra_keys = storage.iter().any(|(key, _)| key != "path");
    if !has_extra_keys {
        return None;
    }
    let get = |names: &[&str]| {
        storage.iter().find_map(|(key, item)| {
            names
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
                .then(|| item.as_str().unwrap_or_default().to_owned())
        })
    };
    // The legacy map held string-typed env values, but accept a TOML bool too.
    let truthy = |item: &Item| {
        item.as_bool()
            .or_else(|| {
                item.as_str()
                    .map(|text| text.eq_ignore_ascii_case("true") || text == "1")
            })
            .unwrap_or(false)
    };
    Some(LegacyStorage {
        access_key_id: get(config::LEGACY_ACCESS_KEY_KEYS),
        secret_access_key: get(config::LEGACY_SECRET_KEY_KEYS),
        endpoint: get(config::LEGACY_ENDPOINT_KEYS),
        path: storage
            .get("path")
            .and_then(Item::as_str)
            .map(str::to_owned),
        virtual_hosted: storage.iter().any(|(key, item)| {
            config::LEGACY_VIRTUAL_HOSTED_KEYS
                .iter()
                .any(|name| key.eq_ignore_ascii_case(name))
                && truthy(item)
        }),
    })
}

/// Rewrite the legacy map in place: keys move to `[creds.default]`, the
/// `[storage]` table resets to path-only (the storage section fills `path`
/// next). Returns the best-guess URL to prefill the storage prompt with -
/// the old format can't always say where the bucket ends and the host
/// begins (virtual-hosted endpoints fold the bucket into the hostname), so
/// the guess goes through the same validation + end-to-end probe as any
/// hand-typed URL. Region and addressing-style keys are dropped on purpose:
/// the new grammar autodetects or defaults both (spec.md#storage-url-grammar).
fn apply_legacy_rewrite(doc: &mut DocumentMut, legacy: &LegacyStorage) -> Option<String> {
    doc["storage"] = Item::Table(Table::new());
    if legacy.access_key_id.is_some() || legacy.secret_access_key.is_some() {
        // Explicit tables, not index-assignment: indexing would synthesize an
        // inline `creds = { default = {...} }` instead of a `[creds.default]`
        // section header.
        let mut set = Table::new();
        if let Some(key) = &legacy.access_key_id {
            set.insert("access_key_id", value(key));
        }
        if let Some(secret) = &legacy.secret_access_key {
            set.insert("secret_access_key", value(secret));
        }
        match doc.get_mut("creds").and_then(Item::as_table_mut) {
            Some(creds) => {
                creds.insert("default", Item::Table(set));
            }
            None => {
                let mut creds = Table::new();
                creds.set_implicit(true);
                creds.insert("default", Item::Table(set));
                doc.insert("creds", Item::Table(creds));
            }
        }
    }
    legacy_url_guess(legacy)
}

/// Best-effort new-grammar URL from the legacy map, for prefilling the storage
/// prompt. Pure (no mutation) so the caller can test the guess for usability
/// before committing to the rewrite. The old format can't always say where the
/// bucket ends and the host begins (virtual-hosted endpoints fold the bucket
/// into the hostname) - unless the addressing-style key pins it - so the guess
/// goes through the same validation + end-to-end probe as any hand-typed URL.
fn legacy_url_guess(legacy: &LegacyStorage) -> Option<String> {
    let host = legacy
        .endpoint
        .as_deref()
        .and_then(|endpoint| endpoint.split("://").nth(1))
        .map(|host| host.trim_end_matches('/'));
    match (host, legacy.path.as_deref()) {
        // Legacy `s3://bucket/prefix` + endpoint host: fold the host in. This
        // arm must come before the parse-wins arm below - a plain `s3://` URL
        // parses fine on its own but means "ambient AWS endpoint", which would
        // silently drop the configured custom endpoint. Under the declared
        // virtual-hosted style the endpoint host already leads with the
        // bucket; strip it or the new grammar (virtual-hosted by default)
        // folds the bucket in twice.
        (Some(host), Some(path)) if path.starts_with("s3://") => {
            let rest = path.trim_start_matches("s3://");
            let bucket = rest.split('/').next().unwrap_or_default();
            let host = (legacy.virtual_hosted)
                .then(|| host.strip_prefix(&format!("{bucket}.")))
                .flatten()
                .unwrap_or(host);
            Some(format!("s3+https://{host}/{rest}"))
        }
        // A path already carrying its own host (`s3+https://host/bucket/...`)
        // wins outright: the endpoint key is redundant once the URL has one.
        (_, Some(path)) if StorageUrl::parse(path).is_ok() => Some(path.to_owned()),
        // Endpoint only. With the virtual-hosted key declared, the bucket IS
        // the leading host label - de-fold it and the guess is exact. Without
        // it the guess is bucketless and fails to validate, which is the
        // signal the wizard uses to ask for the bucket/prefix.
        (Some(host), _) => match host.split_once('.') {
            Some((bucket, rest)) if legacy.virtual_hosted && rest.contains('.') => {
                Some(format!("s3+https://{rest}/{bucket}"))
            }
            _ => Some(format!("s3+https://{host}/")),
        },
        (None, Some(path)) => Some(path.to_owned()),
        (None, None) => None,
    }
}

/// Repair: the adapter config map was renamed `[sources.*]` -> `[adapters.*]`.
/// Move any legacy `[sources.<name>]` entry to `[adapters.<name>]`, preserving
/// values and comments and never clobbering an already-migrated entry; drop the
/// emptied `sources` table. Returns the moved names (empty when there is nothing
/// to migrate). Transitional - delete once live configs have migrated.
fn rewrite_legacy_sources(doc: &mut DocumentMut) -> Vec<String> {
    let names: Vec<String> = match doc.get("sources").and_then(Item::as_table) {
        Some(table) => table.iter().map(|(name, _)| name.to_owned()).collect(),
        None => return Vec::new(),
    };
    if !doc.contains_key("adapters") {
        let mut table = Table::new();
        table.set_implicit(true);
        doc.insert("adapters", Item::Table(table));
    }
    let mut moved = Vec::new();
    for name in names {
        let already = doc
            .get("adapters")
            .and_then(Item::as_table)
            .is_some_and(|table| table.contains_key(&name));
        let entry = doc
            .get_mut("sources")
            .and_then(Item::as_table_mut)
            .and_then(|table| table.remove(&name));
        if let Some(entry) = entry
            && !already
            && let Some(adapters) = doc.get_mut("adapters").and_then(Item::as_table_mut)
        {
            adapters.insert(&name, entry);
            moved.push(name);
        }
    }
    doc.remove("sources");
    moved
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;

    #[test]
    fn agent_command_spawns_the_resolved_binary() {
        let plain = Path::new("/opt/claude/bin/claude");
        let command = agent_command(plain, &["mcp", "get", "pond"]);
        assert_eq!(command.get_program(), plain.as_os_str());
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["mcp", "get", "pond"],
        );

        #[cfg(windows)]
        {
            let shim = Path::new(r"C:\Program Files\nodejs\claude.cmd");
            let command = agent_command(shim, &["mcp", "get", "pond"]);
            assert_ne!(command.get_program(), shim.as_os_str());
            assert_eq!(
                command.get_args().collect::<Vec<_>>(),
                [r#"/C ""C:\Program Files\nodejs\claude.cmd" mcp get pond""#],
            );
        }
    }

    #[test]
    fn skill_sync_installs_updates_and_respects_declines() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("skills").join("pond").join("SKILL.md");

        // Absent + non-interactive without consent: nothing written.
        skill_sync(&path, false, false, false).unwrap();
        assert!(!path.exists());

        // Absent + --yes: installed with the bundled bytes.
        skill_sync(&path, false, true, false).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), SKILL_MD);

        // Current: idempotent no-op.
        skill_sync(&path, false, true, false).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), SKILL_MD);

        // Differs (user edit) + non-interactive without consent: preserved.
        std::fs::write(&path, "user edit").unwrap();
        skill_sync(&path, false, false, false).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "user edit");

        // Differs + --yes: refreshed to the bundled version.
        skill_sync(&path, false, true, false).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), SKILL_MD);

        // Absent + combined-registration consent: installed without --yes.
        std::fs::remove_file(&path).unwrap();
        skill_sync(&path, false, false, true).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), SKILL_MD);
    }

    #[test]
    fn legacy_rewrite_moves_keys_and_prefills_the_url() {
        let mut doc: DocumentMut = r#"
[adapters.claude-code]
enabled = true
path = "/srv/claude"

[storage]
AWS_ACCESS_KEY_ID = "AKIA123"
AWS_SECRET_ACCESS_KEY = "shh"
AWS_REGION = "nbg1"
AWS_ENDPOINT = "https://nbg1.example.com"
"#
        .parse()
        .unwrap();
        let legacy = extract_legacy_storage(&doc).expect("legacy map detected");
        let prefill = apply_legacy_rewrite(&mut doc, &legacy);
        assert_eq!(prefill.as_deref(), Some("s3+https://nbg1.example.com/"));
        let body = doc.to_string();
        // Keys moved, region dropped, adapters untouched.
        assert!(body.contains("[creds.default]"), "got: {body}");
        assert!(body.contains("access_key_id = \"AKIA123\""), "got: {body}");
        assert!(!body.contains("AWS_ACCESS_KEY_ID"), "got: {body}");
        assert!(
            !body.contains("nbg1\""),
            "region must not carry over: {body}"
        );
        assert!(body.contains("[adapters.claude-code]"), "got: {body}");
        // The rewritten doc now loads under the new schema.
        Config::load_str(&body).expect("rewritten config loads");
    }

    #[test]
    fn rewrite_legacy_sources_renames_the_adapter_map() {
        let mut doc: DocumentMut = r#"
[sources.claude-code]
enabled = true
path = "/srv/claude"

[sources.codex-cli]
enabled = false

[storage]
path = "/srv/pond"
"#
        .parse()
        .unwrap();
        let moved = rewrite_legacy_sources(&mut doc);
        assert_eq!(moved, vec!["claude-code", "codex-cli"]);
        let body = doc.to_string();
        assert!(!body.contains("[sources."), "legacy block removed: {body}");
        assert!(body.contains("[adapters.claude-code]"), "got: {body}");
        assert!(body.contains("[adapters.codex-cli]"), "got: {body}");
        // Values and untouched sections survive the move.
        assert!(
            body.contains("path = \"/srv/claude\""),
            "values preserved: {body}"
        );
        assert!(
            body.contains("[storage]"),
            "other sections untouched: {body}"
        );
        // Idempotent: nothing left to migrate on a second pass.
        assert!(rewrite_legacy_sources(&mut doc).is_empty());
        // The migrated config loads under the new schema.
        Config::load_str(&doc.to_string()).expect("migrated config loads");
    }

    #[test]
    fn legacy_url_guess_prefers_a_full_url_path_over_the_endpoint() {
        // A path already in new-grammar form wins even when an endpoint key is
        // present (the endpoint becomes redundant) - it must not be shadowed by
        // the bucketless endpoint-only guess.
        let legacy = LegacyStorage {
            access_key_id: Some("AKIA123".to_owned()),
            secret_access_key: Some("shh".to_owned()),
            endpoint: Some("https://nbg1.example.com".to_owned()),
            path: Some("s3+https://nbg1.example.com/bucket/prefix".to_owned()),
            virtual_hosted: false,
        };
        assert_eq!(
            legacy_url_guess(&legacy).as_deref(),
            Some("s3+https://nbg1.example.com/bucket/prefix"),
        );
    }

    #[test]
    fn legacy_url_guess_folds_the_endpoint_into_a_plain_s3_path() {
        // `s3://bucket/prefix` parses on its own but means "ambient AWS
        // endpoint" - with an endpoint key present the host must fold in, or
        // the custom endpoint is silently dropped.
        let legacy = LegacyStorage {
            access_key_id: Some("AKIA123".to_owned()),
            secret_access_key: Some("shh".to_owned()),
            endpoint: Some("https://nbg1.example.com".to_owned()),
            path: Some("s3://mybucket/agent-sessions".to_owned()),
            virtual_hosted: false,
        };
        assert_eq!(
            legacy_url_guess(&legacy).as_deref(),
            Some("s3+https://nbg1.example.com/mybucket/agent-sessions"),
        );
        // Without an endpoint the plain s3:// path passes through unchanged.
        let ambient = LegacyStorage {
            endpoint: None,
            ..legacy
        };
        assert_eq!(
            legacy_url_guess(&ambient).as_deref(),
            Some("s3://mybucket/agent-sessions"),
        );
        // A virtual-hosted endpoint already leads with the bucket; folding the
        // s3:// path in must not double it.
        let folded = LegacyStorage {
            endpoint: Some("https://mybucket.nbg1.example.com".to_owned()),
            virtual_hosted: true,
            ..ambient
        };
        assert_eq!(
            legacy_url_guess(&folded).as_deref(),
            Some("s3+https://nbg1.example.com/mybucket/agent-sessions"),
        );
    }

    #[test]
    fn legacy_url_guess_defolds_a_declared_virtual_hosted_endpoint() {
        // Endpoint host carries the bucket as its leading label. Without the
        // addressing-style key the guess can't recover the split, so it comes
        // out bucketless and fails to parse - the signal the wizard uses to
        // ask for the bucket/prefix. With the key declared, the split is
        // exact and the guess validates as-is.
        let legacy = LegacyStorage {
            access_key_id: Some("AKIA123".to_owned()),
            secret_access_key: Some("shh".to_owned()),
            endpoint: Some("https://ttq.nbg1.your-objectstorage.com".to_owned()),
            path: None,
            virtual_hosted: false,
        };
        let guess = legacy_url_guess(&legacy).expect("a guess is produced");
        assert_eq!(guess, "s3+https://ttq.nbg1.your-objectstorage.com/");
        assert!(
            StorageUrl::parse(&guess).is_err(),
            "bucketless guess must not validate: {guess}"
        );
        let declared = LegacyStorage {
            virtual_hosted: true,
            ..legacy
        };
        let guess = legacy_url_guess(&declared).expect("a guess is produced");
        assert_eq!(guess, "s3+https://nbg1.your-objectstorage.com/ttq");
        StorageUrl::parse(&guess).expect("de-folded guess validates");
    }

    #[test]
    fn extract_legacy_storage_reads_the_virtual_hosted_key() {
        // String-typed "true" (the env-style legacy form) and a TOML bool both
        // count; absence means false.
        for (line, want) in [
            ("aws_virtual_hosted_style_request = \"true\"", true),
            ("aws_virtual_hosted_style_request = true", true),
            ("aws_virtual_hosted_style_request = \"false\"", false),
            ("", false),
        ] {
            let doc: DocumentMut = format!(
                "[storage]\nAWS_ACCESS_KEY_ID = \"AKIA123\"\nAWS_ENDPOINT = \"https://b.example.com\"\n{line}\n"
            )
            .parse()
            .unwrap();
            let legacy = extract_legacy_storage(&doc).expect("legacy map detected");
            assert_eq!(legacy.virtual_hosted, want, "for line {line:?}");
        }
    }

    #[test]
    fn structural_error_rejects_file_collisions_only() {
        let dir = tempfile::TempDir::new().unwrap();
        let file = dir.path().join("config.toml");
        std::fs::write(&file, "x").unwrap();
        let target_is_file = StorageUrl::parse(&file.display().to_string()).unwrap();
        assert!(
            structural_error(&target_is_file)
                .expect("file target rejected")
                .contains("existing file"),
        );
        let under_file = StorageUrl::parse(&file.join("data").display().to_string()).unwrap();
        assert!(
            structural_error(&under_file)
                .expect("ancestor file rejected")
                .contains("can't be created"),
        );
        // Not existing yet is fine (create_dir_all at first use), and remote
        // URLs are out of scope.
        let fresh = StorageUrl::parse(&dir.path().join("a/b/c").display().to_string()).unwrap();
        assert_eq!(structural_error(&fresh), None);
        let remote = StorageUrl::parse("s3+https://host.example.com/bucket/p").unwrap();
        assert_eq!(structural_error(&remote), None);
    }

    #[test]
    fn new_format_storage_is_not_flagged_as_legacy() {
        let doc: DocumentMut = "[storage]\npath = \"~/.local/share/pond\"\n"
            .parse()
            .unwrap();
        assert!(extract_legacy_storage(&doc).is_none());
        let empty: DocumentMut = "".parse().unwrap();
        assert!(extract_legacy_storage(&empty).is_none());
    }
}
