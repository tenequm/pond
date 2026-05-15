//! Coverage for the `[sources.<adapter>]` config + per-factory discovery:
//! tilde expansion against an injected home (no `HOME` mutation - pond
//! forbids unsafe code under edition 2024), `resolve_sources` filtering,
//! per-factory `probe_default(env)` rules, and the non-tty branch of the
//! discovery prompt (we never block CI on stdin).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::{Path, PathBuf};

use pond::{
    adapter::{self, ClaudeCodeFactory, CodexFactory, Env},
    config::{Config, expand_home_under},
};
use serde_json::{Value, json};
use tempfile::TempDir;

fn write_config(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("config.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

#[test]
fn expand_home_under_handles_tilde_forms() {
    let home = Path::new("/srv/me");
    assert_eq!(
        expand_home_under(Path::new("~"), home),
        PathBuf::from("/srv/me")
    );
    assert_eq!(
        expand_home_under(Path::new("~/.codex/sessions"), home),
        PathBuf::from("/srv/me/.codex/sessions"),
    );
    // Absolute paths pass through unchanged.
    assert_eq!(
        expand_home_under(Path::new("/etc/passwd"), home),
        PathBuf::from("/etc/passwd"),
    );
    // A leading `~something` (no slash) is not the home form - leave it.
    assert_eq!(
        expand_home_under(Path::new("~user/elsewhere"), home),
        PathBuf::from("~user/elsewhere"),
    );
}

#[test]
fn resolve_sources_returns_one_or_all_or_errors() {
    let temp = TempDir::new().unwrap();
    let body = "\
[sources.claude-code]
path = \"/srv/claude\"

[sources.codex]
path = \"/srv/codex\"
";
    let path = write_config(temp.path(), body);
    let config = Config::load(&path).unwrap();

    // None -> everything in [sources.*]
    let all = config.resolve_sources(None).unwrap();
    assert_eq!(all.len(), 2);
    let names: Vec<_> = all.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"claude-code"));
    assert!(names.contains(&"codex"));

    // Some(name) -> one entry, opaque JSON blob
    let one = config.resolve_sources(Some("codex")).unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].0, "codex");
    assert_eq!(
        one[0].1.get("path").and_then(Value::as_str),
        Some("/srv/codex"),
    );

    // Unknown -> error
    assert!(config.resolve_sources(Some("nope")).is_err());
}

#[test]
fn each_factory_probes_its_default_under_an_injected_home() {
    // Per-adapter discovery lives on each factory's `probe_default`, not in
    // a central name->path table. Driving each one with an injected `home`
    // proves the rule lives where the format lives.
    use pond::adapter::AdapterFactory;

    let temp = TempDir::new().unwrap();
    let home = temp.path();
    let claude_dir = home.join(".claude").join("projects");
    let codex_dir = home.join(".codex").join("sessions");
    std::fs::create_dir_all(&claude_dir).unwrap();
    std::fs::create_dir_all(&codex_dir).unwrap();

    let env = Env::with_home(home);

    let claude_probe = ClaudeCodeFactory.probe_default(&env);
    assert_eq!(
        claude_probe
            .as_ref()
            .and_then(|v| v.get("path"))
            .and_then(Value::as_str),
        Some(claude_dir.to_str().unwrap()),
    );

    let codex_probe = CodexFactory.probe_default(&env);
    assert_eq!(
        codex_probe
            .as_ref()
            .and_then(|v| v.get("path"))
            .and_then(Value::as_str),
        Some(codex_dir.to_str().unwrap()),
    );

    // Removing the codex marker dir drops just that factory's probe.
    std::fs::remove_dir_all(&codex_dir).unwrap();
    assert!(CodexFactory.probe_default(&env).is_none());
    assert!(ClaudeCodeFactory.probe_default(&env).is_some());
}

#[test]
fn known_names_covers_every_registered_adapter() {
    // `adapter::registry` is the single source of truth for both `by_name`
    // dispatch and the discovery picker. A new adapter must show up here.
    let names = adapter::known_names();
    assert!(names.contains(&"claude-code"));
    assert!(names.contains(&"codex"));
}

#[test]
fn prompt_and_persist_errors_on_non_tty_stdin() {
    // `cargo test` runs with a piped stdin (non-tty), so this is the path
    // CI / package-install scripts hit. The picker must surface a clear
    // "configure manually" error instead of hanging on a prompt.
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.toml");
    let candidates = vec![adapter::Candidate {
        name: "claude-code".to_owned(),
        hint: "/tmp/dummy".to_owned(),
        config: json!({ "path": "/tmp/dummy" }),
    }];
    let err = adapter::prompt_and_persist(&config_path, &candidates)
        .expect_err("non-tty stdin must error rather than hang");
    let msg = err.to_string();
    assert!(
        msg.contains("not a terminal"),
        "error should mention the non-tty branch: {msg}",
    );
}
