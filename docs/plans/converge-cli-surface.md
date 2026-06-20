# Converge the CLI surface

Status: executing. Owner: this change. Supersedes the verb sprawl left after `cli-onboarding-redesign.md` and `improve-remote-storage-configs.md`.

## Why

The surface accreted three verbs for one concept (`migrate` + `export` + `import` are all "move canonical data") and two hidden verbs for another (`embed` + `index` are both "keep the lake queryable"). Flag names drifted across commands (`--config` vs the `config` subcommand; `--schedule` vs `--every`; `--out` vs `--output-file`; `pretty` vs `text`). This change converges to the smallest surface where every verb's role is unambiguous, informed by how the best-regarded modern data-movement CLIs (rclone, restic, kopia, dvc, gh, uv) shape theirs.

The settled model: corpus verbs are flat (`sync` in, `copy` across, `optimize` upkeep, `search`/`get`/`sql` out, `status` overview); admin concerns are noun-groups (`storage`, `adapters`, `creds`, `schedule`, `config`).

## Prior art that decided the names (from the design research)

- `copy` vs `sync` is a fixed industry contract: `copy` = additive/never-deletes, `sync` = mirror-with-delete. pond's movement is always an idempotent union (`lance-deterministic-pk` + merge-insert), so the cross-store verb is `copy`, not `sync`. `sync` is kept for adapter ingest only and MUST stay additive (never grow delete-on-source-removal).
- kopia is the structural cousin: content-addressed dedup repo, `repository sync-to` (= our `copy`), `snapshot verify` (= our `--verify-only`). We borrow its verbs, not its `repository` noun (pond is a queryable lake, not a backup repo; `storage` matches our `[storage]` config key and spec vocabulary).
- restic precedent: the whole store is a URL, no named remotes. pond already rejected a named-storage registry (`improve-remote-storage-configs.md`), so `copy --from <url> --to <url>` (URLs passed directly) is the ratified shape.
- gh/uv consensus: flat verbs for the hot path, noun-groups for admin, `config` for settings only - auth/creds stay their own peer (never folded under `config`).

## Converged surface

Corpus verbs (flat): `init  sync  optimize  copy  search  get  sql  status  serve  mcp`
Admin nouns: `storage{check,use}  adapters{list,discover,enable,disable}  creds{add,list,delete}  schedule{start,stop,status,logs}  config{show,path,schema}`
Plus: `completions`.

Removed top-level: `migrate`, `export`, `import` (-> `copy`). Removed hidden: `embed`, `index` (-> `optimize`).

## Decisions (resolved with the user)

1. `copy` routes by strict file-suffix sniff: `*.pond` -> archive, `*.jsonl` or `-` (stdout) -> JSONL wire stream, anything with a scheme or a plain dir path -> a pond store. No `--as` flag now (YAGNI; the sniff is unambiguous - add an override only if a real collision appears).
2. No standalone `export`/`import`/jsonl verb: the `.jsonl` wire stream and `.pond` archive are just `copy` targets/sources. The serializer already exists; only the verbs go.
3. `sync` keeps its name (additive ingest; not renamed to `pull`).
4. Membership verification is a flag: `copy --verify-only`, not a separate `verify` verb.
5. `optimize` = the maintenance verb. It runs the existing embed + index(update-indexes) stages; compaction and version GC already live *inside* the index stage via `[maintenance]`, so they are not separate `--only` values. Stages: `embed`, `index`.

## Flag/naming uniformity

| Concept | Before | After |
|---|---|---|
| config file selector | `--config` / `POND_CONFIG` | `--config-file` / `POND_CONFIG_FILE` |
| schedule cadence | init `--schedule`, `schedule start --every` | `--every` everywhere |
| one-off ingest path override | `sync --source-dir` | `sync --path` |
| index stage name | `update-indexes` | `index` |
| human output format | `OutputFormat::Pretty` (`pretty`) | `Text` (`text`) |
| defer index rebuild | `migrate --skip-indexes` | `copy --no-optimize` |
| export output path | `export --out` | (folded into `copy --to <file>`) |

Kept deliberately: per-command `--limit` defaults (search 10 / get 20 / sql 100 - different objects); `--force` (init) vs `--force-embed` (optimize) are scoped and do not collide.

Hidden (operator-only, removed from the visible surface): `search --mode` (benchmark/ablation), `search --namespace` and `get --namespace` (v1 is single-namespace).

## Final param reference

```
global:  --storage-path <URL> [POND_STORAGE_PATH]   --config-file <PATH> [POND_CONFIG_FILE]   -v/-q

pond init      [--adapters <NAMES>] [--every 5m|15m|1h|6h|1d] [--skip-mcp] [-y] [--force]
pond sync      [ADAPTER] [--path <DIR>] [--no-optimize]
pond optimize  [--only embed|index] [--skip embed|index] [--force-embed]
pond copy      --from <url|file> --to <url|file> [--verify-only] [--no-optimize]
pond search    <QUERY> [--limit] [--project] [--session-id] [--source-agent]
               [--include-subagents] [--from-date] [--to-date] [--min-score] [--explain] [--format text|json]
pond get       <--session-id|--message-id> [--context-depth] [--limit]
               [--response-mode] [--session-from] [--after-id] [--format text|json]
pond sql       <SQL> [--format text|json|ndjson|parquet] [--limit] [-o/--output-file]
pond status    [--adapters] [--include-subagents]
pond serve     [--transport http|stdio] [--host] [--port]
pond mcp
pond storage   check [URL] | use <URL>
pond adapters  list | discover | enable <NAME> | disable <NAME>
pond creds     add [NAME] [--scope] [--region] | list | delete <NAME>
pond schedule  start [--every ...] | stop | status | logs
pond config    show | path | schema
pond completions <SHELL>
```

## Execution phases (each ends on a green `cargo build`)

1. Mechanical renames: `--config`->`--config-file`/env; init `--schedule`->`--every`; `sync --source-dir`->`--path`; `SyncStage::UpdateIndexes`->`Index` (CLI `index`); `OutputFormat::Pretty`->`Text`.
2. Add `optimize`; remove hidden `embed` + `index` (and `IndexCommand`); route `--only embed|index` to `run_embed_stage` / `run_update_indexes_stage`; keep `--force-embed`.
3. `sync`: drop `--only/--skip/--force-embed`; add `--no-optimize`; after the import stage, run optimize on the store unless `--no-optimize`.
4. Add `copy` with the suffix-sniff dispatcher over the existing `run_migrate` (store->store), archive export, and archive import paths; `--from` and `--to` both required (each a store URL, `*.pond`, or - for `--to` - `*.jsonl`/`-`; `local` is the default-store keyword); `--verify-only`; `--no-optimize`. Remove `migrate`, `export`, `import`.
5. Internal doc-comment sweep: `pond embed`/`pond index optimize`/`pond migrate`/`pond export`/`pond import` -> new verbs across `src/**`.
6. Regenerate help snapshots (`cargo insta`/`INSTA_UPDATE`), rename the `migrate` integration module to `copy` where it tests the public path, add `--verify-only`/sniff coverage.
7. Docs sweep (below).
8. Verify: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`.

## Docs to update

- `docs/spec.md` 7.8 CLI verbs: rewrite the `migrate`/`export`/`import`/`storage`/`--config`/`--schedule`/`--source-dir`/`update-indexes` lines around `copy`/`optimize`/`--config-file`/`--every`/`--path`/`index`.
- `README.md`: `migrate`/`export`/`import`/`--source-dir`/`--only update-indexes` examples.
- `docs/src/quickstart.md`, `docs/src/migrate-local-to-remote.md` (retitle to copy), `docs/src/SUMMARY.md`.
- `AGENTS.md`, `SKILL.md`, `CHANGELOG.md`.
- `benches/*` flag refs (`--source-dir` is bench-driver-local; keep its own flag but note divergence, or rename to `--path` for consistency).

## Deferred (Phase 2, separate change)

Uniform `--format text|json` on the read/list commands that currently render tables only (`status`, `adapters list`, `creds list`, `storage check`). High value for agent/CI callers but a different kind of work (output-model serialization, especially the multi-section `status`), separable from the verb restructure. Tracked here so it is not lost.
