//! Source discovery and the interactive registration picker: probe the
//! environment for configurable sources, prompt the operator, and persist the
//! picks to `config.toml`. Config-time and interactive - kept off the
//! sync-time seam in `mod.rs`.

use std::path::Path;

use anyhow::{Context, bail};
use dialoguer::{Confirm, MultiSelect, theme::ColorfulTheme};
use serde_json::Value;
use toml_edit::{DocumentMut, Item, Table};

use super::{Env, by_name, known_names, probe_all};

/// One discovered adapter: its name, a hint to show the operator (typically
/// the probed path or endpoint), and the JSON config blob that will be
/// persisted under `[sources.<name>]` if the operator confirms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub name: String,
    pub hint: String,
    pub config: Value,
}

/// Probe every registered factory whose name is NOT already a key in
/// `configured`. Used by `pond sync` to spot freshly-detectable adapters
/// without re-prompting the operator about ones they already opted in or
/// out of. Returns `Candidate`s in registry order.
pub fn probe_unconfigured(
    configured: &std::collections::BTreeMap<String, Value>,
) -> Vec<Candidate> {
    let Some(env) = Env::from_env() else {
        return Vec::new();
    };
    super::registry()
        .iter()
        .filter(|factory| !configured.contains_key(factory.name()))
        .filter_map(|factory| {
            factory.probe_default(&env).map(|config| Candidate {
                name: factory.name().to_owned(),
                hint: hint_for(&config),
                config,
            })
        })
        .collect()
}

/// Probe every registered factory (or just the one named in `focus`) under
/// the current environment and shape results into [`Candidate`]s for the
/// picker. Returns an empty list when no factory's `probe_default` returned
/// anything - the caller surfaces a "configure manually" error in that case.
pub fn discover(focus: Option<&str>) -> Vec<Candidate> {
    let Some(env) = Env::from_env() else {
        return Vec::new();
    };
    let candidates: Vec<Candidate> = match focus {
        None => probe_all(&env)
            .into_iter()
            .map(|(name, config)| Candidate {
                name: name.to_owned(),
                hint: hint_for(&config),
                config,
            })
            .collect(),
        Some(name) => by_name(name)
            .and_then(|factory| factory.probe_default(&env))
            .map(|config| Candidate {
                name: name.to_owned(),
                hint: hint_for(&config),
                config,
            })
            .into_iter()
            .collect(),
    };
    candidates
}

/// Best-effort label for the picker. For filesystem configs that's the
/// `path`; for richer configs we fall back to a compact JSON dump so the
/// operator at least sees what they're confirming.
fn hint_for(config: &Value) -> String {
    if let Some(path) = config.get("path").and_then(Value::as_str) {
        return path.to_owned();
    }
    if let Some(endpoint) = config.get("endpoint").and_then(Value::as_str) {
        return endpoint.to_owned();
    }
    serde_json::to_string(config).unwrap_or_default()
}

/// Prompt the operator to pick which `candidates` to register, then persist
/// the chosen entries to `config.toml` and return them. Pre-checks every
/// candidate (the operator already opted in by running `pond sync`). When
/// `stdin_is_tty` is false we never prompt - we bail with a clear "configure
/// manually" message so CI and post-install scripts get a predictable error
/// instead of a hang. The caller injects TTY-ness so tests can drive the
/// non-tty branch deterministically regardless of how `cargo test` is invoked.
pub fn prompt_and_persist(
    config_path: &Path,
    candidates: &[Candidate],
    stdin_is_tty: bool,
) -> anyhow::Result<Vec<Candidate>> {
    if candidates.is_empty() {
        bail!(
            "no adapter sources detected in this environment; known adapters: {}",
            known_names().join(", "),
        );
    }
    if !stdin_is_tty {
        bail!(
            "[sources] is empty and stdin is not a terminal; add a [sources.<adapter>] \
             entry to {} (known adapters: {})",
            config_path.display(),
            known_names().join(", "),
        );
    }
    let labels = candidates
        .iter()
        .map(|c| format!("{} ({})", c.name, c.hint))
        .collect::<Vec<_>>();
    let defaults = vec![true; candidates.len()];
    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select sources to register (space toggles, enter confirms)")
        .items(&labels)
        .defaults(&defaults)
        .interact()
        .context("source picker prompt failed")?;
    if selections.is_empty() {
        bail!("no sources selected; nothing to sync");
    }
    let picks: Vec<Candidate> = selections
        .into_iter()
        .filter_map(|index| candidates.get(index).cloned())
        .collect();
    persist_accept(config_path, &picks)?;
    Ok(picks)
}

/// Write the picked sources back to `config.toml` under `[sources.<name>]`,
/// preserving any existing user comments/formatting via `toml_edit`. Each
/// pick's `config` JSON object is unpacked into TOML key/value pairs and
/// `enabled = true` is inserted as the first field so `resolve_sources`
/// picks it up.
pub fn persist_accept(config_path: &Path, picks: &[Candidate]) -> anyhow::Result<()> {
    let mut doc = open_or_init(config_path)?;
    let sources = sources_table_mut(&mut doc)?;
    for pick in picks {
        let mut entry = json_to_toml_table(&pick.config).with_context(|| {
            format!(
                "pick for {:?} did not produce a TOML-shaped table",
                pick.name
            )
        })?;
        prepend_enabled(&mut entry, true);
        sources.insert(&pick.name, Item::Table(entry));
    }
    std::fs::write(config_path, doc.to_string())
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    Ok(())
}

/// Outcome of a per-adapter `Confirm` prompt during `pond sync`. `enable`
/// is the answer to "should this adapter be enabled?"; `sync_now` is the
/// follow-up "should we sync it this run?" (only meaningful when
/// `enable == true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptOutcome {
    pub candidate: Candidate,
    pub enable: bool,
    pub sync_now: bool,
}

/// Prompt the operator about each freshly-detected unconfigured adapter:
/// "Enable X?" and, on accept, "Sync X now?". When `auto_accept` is true
/// (the operator passed `--yes`) every prompt is skipped and answered
/// yes/yes. Returns the per-adapter outcomes.
pub fn prompt_each(
    candidates: &[Candidate],
    auto_accept: bool,
) -> anyhow::Result<Vec<PromptOutcome>> {
    let mut out = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let label = if candidate.hint.is_empty() {
            candidate.name.clone()
        } else {
            format!("{} ({})", candidate.name, candidate.hint)
        };
        let (enable, sync_now) = if auto_accept {
            (true, true)
        } else {
            let enable = Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt(format!("Enable {label}?"))
                .default(true)
                .interact()
                .context("enable prompt failed")?;
            let sync_now = if enable {
                Confirm::with_theme(&ColorfulTheme::default())
                    .with_prompt(format!("Sync {} now?", candidate.name))
                    .default(true)
                    .interact()
                    .context("sync-now prompt failed")?
            } else {
                false
            };
            (enable, sync_now)
        };
        out.push(PromptOutcome {
            candidate: candidate.clone(),
            enable,
            sync_now,
        });
    }
    Ok(out)
}

/// Persist `[sources.<name>] enabled = false` for adapters the operator
/// declined during a probe prompt. Keeps the decline sticky across runs.
pub fn persist_decline(config_path: &Path, names: &[&str]) -> anyhow::Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut doc = open_or_init(config_path)?;
    let sources = sources_table_mut(&mut doc)?;
    for name in names {
        let mut entry = Table::new();
        prepend_enabled(&mut entry, false);
        sources.insert(name, Item::Table(entry));
    }
    std::fs::write(config_path, doc.to_string())
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    Ok(())
}

fn open_or_init(config_path: &Path) -> anyhow::Result<DocumentMut> {
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config dir {}", parent.display()))?;
    }
    let existing = if config_path.exists() {
        std::fs::read_to_string(config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?
    } else {
        String::new()
    };
    let mut doc: DocumentMut = existing
        .parse()
        .with_context(|| format!("failed to parse {} as TOML", config_path.display()))?;
    if !doc.contains_key("sources") {
        let mut table = Table::new();
        table.set_implicit(true);
        doc.insert("sources", Item::Table(table));
    }
    Ok(doc)
}

fn sources_table_mut(doc: &mut DocumentMut) -> anyhow::Result<&mut Table> {
    doc["sources"]
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("config.toml has a `sources` value that is not a table"))
}

fn prepend_enabled(table: &mut Table, value: bool) {
    use toml_edit::value as tv;
    let implicit = table.is_implicit();
    let keys: Vec<String> = table.iter().map(|(k, _)| k.to_owned()).collect();
    let mut existing: Vec<(String, Item)> = Vec::with_capacity(keys.len());
    for key in keys {
        if key == "enabled" {
            continue;
        }
        if let Some(item) = table.remove(&key) {
            existing.push((key, item));
        }
    }
    table.remove("enabled");
    let mut fresh = Table::new();
    fresh.set_implicit(implicit);
    fresh.insert("enabled", tv(value));
    for (key, item) in existing {
        fresh.insert(&key, item);
    }
    *table = fresh;
}

/// Convert a JSON object into a `toml_edit::Table`. Factories produce JSON
/// blobs (the seam contract); the picker persists them as TOML tables. Non-
/// object roots are rejected with an error rather than silently dropped.
fn json_to_toml_table(value: &Value) -> anyhow::Result<Table> {
    let Value::Object(map) = value else {
        bail!("config blob must be a JSON object, got {value}");
    };
    let mut table = Table::new();
    for (key, val) in map {
        table[key] = json_to_toml_item(val)?;
    }
    Ok(table)
}

fn json_to_toml_item(value: &Value) -> anyhow::Result<Item> {
    use toml_edit::{Array, InlineTable, Value as TomlValue, value as tv};
    Ok(match value {
        Value::Null => bail!("null is not representable in TOML"),
        Value::Bool(b) => tv(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                tv(i)
            } else if let Some(f) = n.as_f64() {
                tv(f)
            } else {
                bail!("number {n} is not representable in TOML");
            }
        }
        Value::String(s) => tv(s.clone()),
        Value::Array(values) => {
            let mut array = Array::new();
            for v in values {
                let item = json_to_toml_item(v)?;
                let toml_value: TomlValue = item.into_value().map_err(|_| {
                    anyhow::anyhow!("array element {v} is not a scalar; nested tables in arrays")
                })?;
                array.push(toml_value);
            }
            Item::Value(TomlValue::Array(array))
        }
        Value::Object(_) => {
            let table = json_to_toml_table(value)?;
            let mut inline = InlineTable::new();
            for (key, item) in table.iter() {
                if let Some(v) = item.as_value() {
                    inline.insert(key, v.clone());
                }
            }
            Item::Value(TomlValue::InlineTable(inline))
        }
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn prompt_and_persist_errors_on_non_tty_stdin() {
        // Drive the non-tty branch explicitly. This is the path CI and
        // package-install scripts hit; the picker must surface a clear
        // "configure manually" error instead of hanging on a prompt.
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("config.toml");
        let candidates = vec![Candidate {
            name: "claude-code".to_owned(),
            hint: "/tmp/dummy".to_owned(),
            config: json!({ "path": "/tmp/dummy" }),
        }];
        let err = prompt_and_persist(&config_path, &candidates, false)
            .expect_err("non-tty stdin must error rather than hang");
        let msg = err.to_string();
        assert!(
            msg.contains("not a terminal"),
            "error should mention the non-tty branch: {msg}",
        );
    }

    #[test]
    fn persist_accept_and_decline_write_enabled_first() {
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("config.toml");
        let accept = Candidate {
            name: "claude-code".to_owned(),
            hint: "/tmp/cc".to_owned(),
            config: json!({ "path": "/tmp/cc" }),
        };
        persist_accept(&config_path, &[accept]).unwrap();
        persist_decline(&config_path, &["opencode"]).unwrap();
        let body = std::fs::read_to_string(&config_path).unwrap();
        // Accepted entry: discriminator on top, then the blob.
        assert!(
            body.contains("[sources.claude-code]")
                && body.contains("enabled = true")
                && body.contains("path = \"/tmp/cc\""),
            "expected accepted entry; got: {body}",
        );
        // Declined entry: discriminator only, no path leak.
        assert!(
            body.contains("[sources.opencode]") && body.contains("enabled = false"),
            "expected declined entry; got: {body}",
        );
    }
}
