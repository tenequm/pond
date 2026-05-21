//! Source discovery and the interactive registration picker: probe the
//! environment for configurable sources, prompt the operator, and persist the
//! picks to `config.toml`. Config-time and interactive - kept off the
//! sync-time seam in `mod.rs`.

use std::path::Path;

use anyhow::{Context, bail};
use dialoguer::{MultiSelect, theme::ColorfulTheme};
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
    persist(config_path, &picks)?;
    Ok(picks)
}

/// Write the picked sources back to `config.toml` under `[sources.<name>]`,
/// preserving any existing user comments/formatting via `toml_edit`. Each
/// pick's `config` JSON object is unpacked into TOML key/value pairs.
fn persist(config_path: &Path, picks: &[Candidate]) -> anyhow::Result<()> {
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
    let Some(sources) = doc["sources"].as_table_mut() else {
        bail!("config.toml has a `sources` value that is not a table");
    };
    for pick in picks {
        let entry = json_to_toml_table(&pick.config).with_context(|| {
            format!(
                "pick for {:?} did not produce a TOML-shaped table",
                pick.name
            )
        })?;
        sources.insert(&pick.name, Item::Table(entry));
    }

    std::fs::write(config_path, doc.to_string())
        .with_context(|| format!("failed to write {}", config_path.display()))?;
    Ok(())
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
}
