---
title: Multiple source roots per adapter (multi-claude-dirs)
date: 2026-07-09
status: planned
owner: TBD
branch: feat/no-embed-sync-and-watch (or a fresh feat/multi-root branch)
supersedes: none
related: 2607-09-port-agentsview-formats-to-lance.md (its OpenClaude port + `claude_project_dirs` note are the second consumer of this primitive)
tags: [adapters, config, watch, sync, claude-code, multi-root]
---

# Multiple source roots per adapter

Let one filesystem adapter ingest from **more than one directory**. Driving case:
a user with two Claude Code homes (e.g. `~/.claude/projects` personal and a
work `~/work/.claude/projects`, or a machine with `CLAUDE_CONFIG_DIR` pointed
elsewhere) wants both pooled into one searchable pond corpus.

Today pond resolves **exactly one directory per adapter** and hardcodes the
claude-code default to `~/.claude/projects` (`src/adapter/claude_code.rs:84-86`).
There is no config, env var, or plural field that points it at a second dir.

## 0. Read first

Current one-dir-per-adapter machinery, all of which this plan touches:

- `src/adapter/claude_code.rs:84-86` — `probe_default` hardcodes `~/.claude/projects`; no `CLAUDE_CONFIG_DIR` is consulted anywhere in the tree.
- `src/adapter/mod.rs:561-570` — `config_path`: deserializes the adapter blob as `struct Cfg { path: PathBuf }` (singular), `~`-expands, returns one `PathBuf`. This is what every fs factory's `open` calls (e.g. `claude_code.rs:80-82`).
- `src/adapter/mod.rs:582-596` — `source_root(&Value) -> Option<PathBuf>`: the SAME parse `config_path` uses, so the watched dir can't drift from the imported one. Returns `None` for API-backed adapters (no local path).
- `src/adapter/mod.rs:499-514` — `registry()` / `by_name()`: adapter lookup is **exact factory-name match**. You cannot alias a second `[adapters.claude-code-work]` and have it dispatch to `ClaudeCodeFactory` — `by_name` returns `None`.
- `src/config.rs` — `adapters: BTreeMap<String, Value>` (one opaque blob per adapter **name**; TOML allows only one `[adapters.claude-code]` table); sample block ~180-201, `resolve_adapters` ~621-655.
- `src/watch.rs:101-121` — `resolve_watch_roots` builds `Vec<(String, PathBuf)>`, one `PathBuf` per adapter, straight from `adapter::source_root`.
- `src/main.rs` — `resolve_sync_adapters` (~4341-4344) requires the config section name to equal a registered factory name; `pond sync claude-code --path <dir>` override (~4325-4338).

Correctness backstops that make repeated/overlapping ingestion of new roots safe (unchanged by this plan, worth knowing):

- `src/adapter/mod.rs:262-286` — freshness is a stateless watermark (`source_last_ts <= stored_max_ts`), recomputed from the store; a newly-added root simply looks "all stale" and gets ingested next sync.
- `src/substrate.rs:4855-4866` + `2015-2048` — idempotent `merge_insert` keyed on `(session_id, id)` with `WhenMatched::DoNothing`. Session ids are globally unique, so two roots can never collide or duplicate.

**The gap is already documented** in `2607-09-port-agentsview-formats-to-lance.md`:
line 53 ("one config blob per adapter name, **no multi-root arrays today**"),
line 66 (agentsview's `claude_project_dirs`-style **multi-root array + env-var**
discovery pattern as prior art), line 321 (OpenClaude is "essentially a **second
root + label** on the existing `claude_code.rs` driver"). That plan is the second
consumer: build the primitive here once, reuse it there.

## 1. Decision: plural paths on one adapter (Option A), not named instances (Option B)

**Option A — `paths: Vec<PathBuf>` on the existing adapter blob (CHOSEN).**
```toml
[adapters.claude-code]
paths = ["~/.claude/projects", "~/work/.claude/projects"]
# path = "~/.claude/projects"   # still accepted; treated as a 1-element paths
```
All roots ingest under one adapter identity. Smallest change; matches the
driving case (pool everything into one corpus). This is the shape agentsview
already uses (`claude_project_dirs`).

**Option B — named instances (`[adapters."claude-code:work"]`), rejected for now.**
Cleaner per-source provenance (query "work vs personal"), but requires splitting
factory-name vs instance-name across `by_name`/`registry`/`resolve_sync_adapters`
and every 1:1 name assumption — far more surface for a need nobody has stated.
Kept as a roadmap item (§7); Option A does not preclude it.

Chosen because the goal is *one pooled corpus*, and every root shares the
claude-code parser — a per-instance identity buys nothing the driving case wants.

## 2. Config schema: `path` → `paths`, back-compat by construction

Accept **either** `path` (string, legacy) **or** `paths` (array). Normalize to a
`Vec<PathBuf>` at parse time so all downstream code sees one shape.

`src/adapter/mod.rs`, replace the singular `Cfg` in `config_path` (561-570) and
the ad-hoc `get("path")` in `source_root` (593-596) with one shared resolver:

```rust
#[derive(Deserialize)]
struct Cfg {
    #[serde(default)]
    path:  Option<PathBuf>,
    #[serde(default)]
    paths: Vec<PathBuf>,
}

/// Every local root a filesystem adapter reads from, `~`-expanded. Accepts the
/// legacy singular `path` and the plural `paths`; errors only if BOTH are set to
/// non-empty conflicting values, or NEITHER is present for a path-shaped adapter.
fn config_roots(adapter: &'static str, config: &Value) -> Result<Vec<PathBuf>, AdapterError> { ... }
```

Rules:
- `paths` present and non-empty → use it (each `~`-expanded).
- else `path` present → `vec![path]` (legacy path preserved verbatim).
- both present → error with a clear "set one of `path` / `paths`" message (don't silently merge; ambiguous intent).
- neither → the existing `None`/API-backed case for `source_root`; a hard config error for a factory that requires a root.
- de-dup + reject a root nested inside another (a parent+child pair would double-watch and double-scan the child); log and drop the redundant one.

`probe_default` (`claude_code.rs:84-86`) stays single-root (still emits `{ "path": ... }`) — discovery finds the one default home; multi-root is an explicit opt-in. **Optional stretch:** also probe `$CLAUDE_CONFIG_DIR` when set and distinct, emitting `{ "paths": [...] }`. Flagged, not required for v1.

## 3. Fan-out: three call sites consume the `Vec`

1. **`source_root` → `source_roots`** (`adapter/mod.rs:582-596`). Change the
   signature to return `Vec<PathBuf>` (empty for API-backed). Keep a thin
   `source_root` returning `.first().cloned()` only if some caller genuinely
   needs one — prefer migrating callers.

2. **Watch** (`watch.rs:101-121`, `resolve_watch_roots`). It already produces a
   `Vec<(String, PathBuf)>` and iterates `debouncer.watch(root, …)` per entry
   (`watch.rs:183-197`) — so it just flattens `source_roots` instead of pushing
   one. The "a root that doesn't exist yet is a warning, not fatal" logic already
   covers a work dir that isn't present on every machine. Near-zero change.

3. **Importer** (`claude_code.rs` open + the `JsonlTree` scan). `open` currently
   builds one `ClaudeCodeAdapter::new(config_path(...))`. Make the adapter hold
   `Vec<PathBuf>` and scan each root's JSONL tree, concatenating the record
   streams. Watermark/freshness is per-session (stateless, `mod.rs:262-286`), so
   interleaving roots needs no cursor bookkeeping. Confirm two roots never share
   a session id (they can't — ids are globally unique; the store is their union).

## 4. Files to touch

- `src/adapter/mod.rs` — `Cfg`/`config_path` → `config_roots`; `source_root` → `source_roots`; nesting/dup guard.
- `src/adapter/claude_code.rs` — adapter holds `Vec<PathBuf>`; scan each; `open` passes the vec. (`probe_default` optional `CLAUDE_CONFIG_DIR` stretch.)
- `src/adapter/{codex_cli,opencode,pi_coding_agent}.rs` — same `open` plumbing change if they route through `config_path` (audit; the primitive is generic, so they get multi-root for free).
- `src/watch.rs` — `resolve_watch_roots` flattens `source_roots`.
- `src/config.rs` — sample `[adapters.*]` block (~180-201) documents `paths`; `resolve_adapters` unchanged (still one blob per name).
- `src/main.rs` — audit `resolve_sync_adapters` (~4341) and the `--path` override (~4325-4338): decide whether `--path` appends or replaces (propose: replaces, one-shot, documented).
- Snapshots under `src/snapshots/` — help/config text if the sample block changes.

## 5. Tests

- **Unit** (`config_roots`): `path` only; `paths` only; both → error; neither → `None`/error per adapter kind; `~` expansion; nested-root de-dup; dup de-dup.
- **Integration**: two temp claude-code trees under distinct roots, one `[adapters.claude-code].paths` config, one `pond sync` → assert sessions from both roots land, counts add up, no dupes.
- **Watch**: two roots watched; a write under the *second* root triggers a no-embed tick (extends the existing watch test).
- **Back-compat**: an existing single-`path` config produces byte-identical ingestion to today (freshness watermark unchanged).
- **Idempotency**: sync twice → `merge_insert` no-ops, row counts stable (guards against a fan-out double-count bug).

## 6. Risks / open questions

- **`--path` semantics** with multi-root config: append or replace? (Proposed: replace, one-shot.)
- **Overlapping/nested roots** — guarded by the de-dup, but symlinked roots pointing at the same tree could still double-scan; watermark makes it *correct* (idempotent) but wasteful. Log when detected.
- **Per-root provenance** — Option A gives none. If "which machine/dir did this come from?" becomes a real query, that's the trigger to revisit Option B (named instances), not to bolt a label onto Option A.
- **Other adapters** — the primitive is generic; codex/opencode/pi get multi-root too. Confirm none of them assume a single root downstream of `open`.

## 7. Out of scope (roadmap)

- **Named adapter instances** (Option B, `[adapters."claude-code:work"]`) with per-instance identity/provenance and a factory-name/instance-name split in `by_name`.
- **`CLAUDE_CONFIG_DIR` auto-discovery** beyond the optional v1 stretch.
- The **OpenClaude "second root + label"** port — belongs to `2607-09-port-agentsview-formats-to-lance.md`; it consumes this primitive.

## 8. Milestones

1. `config_roots` + schema back-compat + unit tests. (No behavior change: single `path` still works.)
2. `source_roots` + watch fan-out + claude-code multi-scan + integration test.
3. Audit + wire the other fs adapters; snapshot/doc updates.
4. (Stretch) `CLAUDE_CONFIG_DIR` probe.
