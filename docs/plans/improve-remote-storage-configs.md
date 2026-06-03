# Improve remote storage configs

Status: planned (not started)

## Summary

Replace pond's current single opaque `[storage]` map plus `--data-dir` URL with a typed registry of named storages, a fat-URL grammar that hides endpoint inside the path, mechanical `POND_STORAGE_*` env mirror, value-source variants for secrets (`_command`, `_file`), a documented precedence ladder, and a redacted `pond config show` affordance.

The result: one variable per storage in 95% of configs, no mixed casing, no secrets next to non-secrets in the same file by convention, credentials that survive switching, and a single command to move from local to remote.

Wire-breaking, pre-release. OK per CLAUDE.md ("breaking changes are free").

## Context

Why this redesign:

- Today `[storage]` is `BTreeMap<String, String>` passed verbatim to Lance's `with_storage_options`. No schema, no validation, no `--help`, no source attribution, no redaction. The opaque-map forward-compat seam was deliberate but the cost is paid on every user interaction.
- The bucket name hides inside the URL while region and endpoint live in another block. Three or four fields encode one logical destination.
- The user's live config currently mixes ENV-style keys (`AWS_ACCESS_KEY_ID`) and snake_case keys (`aws_virtual_hosted_style_request`) side-by-side because `object_store`'s schema spills through pond verbatim.
- Secrets sit in plaintext TOML next to non-secret knobs with no opt-in to env, no `_FILE` indirection, no command-based fetching, no redaction in any introspection surface.
- `pond status` reveals nothing about which config file is loaded, which storage is active, or whether the credentials work. First failure is mid-sync.
- Single-destination is fine until the user wants to migrate local-to-remote or alternate between archive and production. Both workflows require holding multiple storages addressable simultaneously without re-entering creds.

Reference research (summary; full notes in conversation history):

- Named-remote pattern is consensus across rclone, mc, kopia. Restic is the outlier with fat-URL + env only.
- DuckDB's scoped secrets shine when one process opens many destinations; pond does not, so longest-prefix matching is degenerate. Per-storage cred attachment is the simpler fit.
- `_FILE` suffix (Docker convention) and `--password-command` (restic) are the two highest-leverage secret affordances, free K8s and 1Password / sops / gpg / pass integration without integrating any of them.
- Figment is the modern Rust standard for layered config with provenance tracking. Used by Rocket. Composes clap-parsed CLI matches, prefixed env, and TOML into one typed deserialize.
- The rust-cli/book offers no guidance on this; its config-files chapter is a TODO. Community consensus on figment + clap-derive + per-field explicit `env =` is the actual best practice.

## Locked design

### Config schema

```toml
default = "local"

[storages.local]
path = "file:///Users/me/.local/share/pond"

[storages.prod]
path                       = "s3+https://ttq.nbg1.your-objectstorage.com/my-pond/prefix"
access_key_id              = "..."
secret_access_key_command  = "op read op://vault/pond/secret"

[storages.archive]
path                    = "s3+https://archive.example.com/cold-pond/2026"
access_key_id_file      = "/etc/pond/archive.id"
secret_access_key_file  = "/etc/pond/archive.secret"
```

Rules:

- `default = "<name>"` at the top names the storage used when no `--storage` flag and no `POND_STORAGE` env is set. Required when `[storages.*]` is non-empty.
- Each `[storages.<name>]` is a typed struct with `serde(deny_unknown_fields)`.
- `path` is always required. Local schemes need nothing else.
- Optional credential fields: `access_key_id`, `access_key_id_file`, `secret_access_key`, `secret_access_key_command`, `secret_access_key_file`. At most one variant per logical field (e.g. `secret_access_key` and `secret_access_key_command` together is a parse error).
- Unknown keys in `[storages.<name>]` are a hard error.
- `name` follows `[a-z][a-z0-9-]{0,31}`. Validated at parse.

No `extra = { ... }` opaque escape hatch in the file. The escape hatch is env (`POND_STORAGE_EXTRA__<KEY>=value`, double underscore as nesting separator) for the rare object_store knobs pond has not typed yet. Forces the file shape to stay clean.

### URL grammar

```
file:///abs/path                     local, absolute
./relative or /abs                   local, bare path treated as file://
~/path                               local, tilde-expanded against HOME
s3://bucket/prefix                   AWS S3, default endpoint
s3+https://host/bucket/prefix        S3-compatible, TLS, endpoint = host
s3+http://host:port/bucket/prefix    S3-compatible, no TLS, port allowed
gs://bucket/prefix                   Google Cloud Storage
az://account/container/prefix        Azure Blob
memory://name                        in-process (tests only)
```

The parser lives in `src/substrate.rs` next to where `with_storage_options` is called today. ~30 LOC on top of `url::Url`. Embedded credentials in the URL (`s3+https://user:pass@host/...`) are forbidden at parse.

Translation table (parser output -> Lance call):

| Input URL                                   | Lance URL              | Injected options                                                  |
|---------------------------------------------|------------------------|-------------------------------------------------------------------|
| `file:///abs/path`                          | `file:///abs/path`     | none                                                              |
| `s3://bucket/prefix`                        | `s3://bucket/prefix`   | none (object_store reads `AWS_*` env if present)                  |
| `s3+https://host/bucket/prefix`             | `s3://bucket/prefix`   | `endpoint=https://host`, `allow_http=false`, `virtual_hosted=true`|
| `s3+http://host:port/bucket/prefix`         | `s3://bucket/prefix`   | `endpoint=http://host:port`, `allow_http=true`, `virtual_hosted=true`|
| `gs://bucket/prefix`                        | `gs://bucket/prefix`   | none                                                              |
| `az://account/container/prefix`             | `az://container/prefix`| `account_name=account`                                            |

Region defaults:

- For `s3+https://` and `s3+http://`: not set. Most S3-compatibles (Hetzner, R2, B2, MinIO, Garage) accept any or ignore it.
- For `s3://`: sniffed from hostname (`s3.<region>.amazonaws.com`); falls back to `AWS_REGION` env; defaults to `us-east-1` if neither resolves.
- Always overridable via the env escape hatch `POND_STORAGE_EXTRA__REGION=...`.

`virtual_hosted_style_request` defaults to `true` for `s3+https` and `s3+http`. Matches Hetzner, R2, B2. MinIO and Garage users override via env (`POND_STORAGE_EXTRA__VIRTUAL_HOSTED_STYLE_REQUEST=false`).

### Env mirror

The active storage's fields project to a flat `POND_STORAGE_*` namespace. The map shape of the file is invisible at the env layer; one storage active at a time matches how env naturally works.

| TOML location                                    | Env                                            |
|--------------------------------------------------|------------------------------------------------|
| `default`                                        | `POND_STORAGE`                                 |
| `storages.<active>.path`                         | `POND_STORAGE_PATH`                            |
| `storages.<active>.access_key_id`                | `POND_STORAGE_ACCESS_KEY_ID`                   |
| `storages.<active>.access_key_id_file`           | `POND_STORAGE_ACCESS_KEY_ID_FILE`              |
| `storages.<active>.secret_access_key`            | `POND_STORAGE_SECRET_ACCESS_KEY`               |
| `storages.<active>.secret_access_key_command`    | `POND_STORAGE_SECRET_ACCESS_KEY_COMMAND`       |
| `storages.<active>.secret_access_key_file`       | `POND_STORAGE_SECRET_ACCESS_KEY_FILE`          |
| `storages.<active>.<extra-key>`                  | `POND_STORAGE_EXTRA__<KEY>`                    |

`POND_STORAGE=prod` selects the active registry entry. Subsequent `POND_STORAGE_*` env vars override that entry's fields for this process only. No mutation of the file.

The same `POND_STORAGE_PATH` env that overrides also acts as a complete substitute: if no config file exists and no `[storages.*]` is defined, `POND_STORAGE_PATH` alone is enough to run pond against any URL. This is the "zero config, one env var" deployment path (containers, CI).

### CLI surface

Top-level flags (clap derive, flattened into every command that opens storage):

```
--storage <NAME>                          # pick a named storage
--storage-path <URL_OR_PATH>              # override active path
--storage-access-key-id <ID>              # override active access key
--storage-access-key-id-file <PATH>       # override, from file
--storage-secret-access-key <SECRET>      # override active secret
--storage-secret-access-key-command <CMD> # override, exec for value
--storage-secret-access-key-file <PATH>   # override, from file
```

Each flag carries an explicit `env = "POND_STORAGE_..."` attribute so the env var name is visible in `--help`.

New subcommands:

```
pond storage list                         # show every defined storage, mark default, redact secrets
pond storage use <NAME>                   # persist default = "<NAME>"
pond storage add <NAME> --path <URL> [--access-key-id ...] [--secret-access-key-... ...]
pond storage rm <NAME>                    # error if NAME is the default and other entries exist
pond migrate --from <NAME> --to <NAME>    # cross-storage move via pond's export+import primitive
```

```
pond config show [--explain]              # redacted current config + source attribution per field
pond config path                          # absolute path of the loaded config file
pond config check                         # probe active storage (head object on bucket prefix)
```

Removed:

- `--data-dir` and `POND_DATA_DIR` (replaced by `--storage` / `--storage-path` and `POND_STORAGE` / `POND_STORAGE_PATH`).
- Old `[storage]` flat-map TOML block (replaced by `[storages.<name>]`).

### Precedence

```
CLI flag
  > POND_STORAGE_* env
  > [storages.<active>] in config.toml
  > AWS_* env (read natively by object_store)
  > IAM / EC2 / ECS instance metadata
  > built-in defaults
```

Documented as a single table in `pond config show --explain`. Documented again in the config.toml header comment. One source of truth.

### SDK credential chain fallback (Q3 = yes, explicit)

When the active storage's URL is an object-store URL and no credentials are set in any of CLI / env / file:

- Pond does NOT fail.
- Pond passes the URL to Lance with no cred fields.
- Lance hands it to object_store, which consults the standard cloud credential chain (env vars, shared credentials file, IMDS, container metadata).

This makes EC2 instance profiles, ECS task roles, GitHub Actions OIDC, and `aws sso login` ambient creds all "just work" with zero TOML. Documented as an explicit invariant in `--help` long form on every `--storage-*-key*` flag, and in the config.toml header comment.

### Secrets redaction (Q1 = yes, ships with this commit)

`pond config show` and `pond storage list` redact:

- Any field whose name contains, case-insensitively, one of: `key`, `secret`, `token`, `password`.
- Redaction prints `********` regardless of the underlying value length. Exception: `_command` and `_file` variants print their literal value (the path / command IS the safe part).

`pond config show --explain` adds a `source` column:

```
field                                        value         source
storages.prod.path                           s3+https://.. env POND_STORAGE_PATH
storages.prod.access_key_id                  AKIA...       file ~/.config/pond/config.toml
storages.prod.secret_access_key              ********      env POND_STORAGE_SECRET_ACCESS_KEY
storages.prod.secret_access_key_command      op read op... file ~/.config/pond/config.toml
default                                      prod          file ~/.config/pond/config.toml
```

Borderless `comfy-table`, dim-bold headers, matching pond's existing output stack per CLAUDE.md.

## Implementation

### Phase 1: schema and resolution

- Add `figment` to `Cargo.toml`.
- `src/config.rs`:
  - Remove `pub storage: BTreeMap<String, String>` and `pub fn parse_data_dir`.
  - Add `pub struct StorageEntry` with typed fields and `deny_unknown_fields`. One-of validator across the three secret-source variants; error names the offending pair.
  - Add `pub default: Option<String>` and `pub storages: BTreeMap<String, StorageEntry>` to `Config`.
  - Add `pub fn resolve_active_storage(&self, override_name: Option<&str>) -> Result<&StorageEntry>` and `pub fn validate(&self) -> Result<()>` (asserts `default` names an existing entry).
- New `src/storage_url.rs`:
  - `pub fn parse_storage_url(input: &str) -> Result<ParsedStorageUrl>`.
  - `pub struct ParsedStorageUrl { lance_url: Url, options: Vec<(String, String)> }`.
  - Handles all schemes in the grammar table; embedded creds rejected; trailing slash normalized.
- `src/substrate.rs`:
  - `fn resolve_secret_source(source: SecretSource) -> Result<String>` that handles `Inline`, `File(path)`, `Command(cmd)`. Command path uses `std::process::Command::new("sh").arg("-c").arg(cmd).output()`, strips a single trailing newline, surfaces non-zero exit codes with the command text.
  - Replace the current `[storage]` map plumbing with `parsed = parse_storage_url(entry.path)?; options = parsed.options.extend(entry.cred_options()?); store = Store::open(parsed.lance_url, options)`.
- `src/main.rs`:
  - Define `StorageArgs` clap struct with the seven `--storage-*` flags. `#[command(flatten)]` into every subcommand whose `data_dir` field gets removed.
  - Figment composition site: `Figment::new().merge(Toml::file(path)).merge(Env::prefixed("POND_").split("__")).merge(Serialized::defaults(&cli_args))`. Custom `Env::split("__")` so `POND_STORAGE_EXTRA__VIRTUAL_HOSTED_STYLE_REQUEST` splits on the double-underscore only and the rest stays flat.
  - Replace `--data-dir` / `POND_DATA_DIR` parsing.

### Phase 2: subcommands

- `src/main.rs` new subcommand modules:
  - `pond storage list` -> reuses `new_table()` from existing CLI output stack.
  - `pond storage use <name>` -> reads, mutates, writes via existing `persist_config` helper. Atomic temp-file + rename, chmod 600.
  - `pond storage add <name> ...` -> validates name regex, refuses overwrite without `--force`, writes via `persist_config`.
  - `pond storage rm <name>` -> errors if name is the default and other entries exist, instructs user to `storage use` first.
  - `pond migrate --from A --to B` -> opens both storages, drives the existing export+import code path between them. Inverse of "two `.pond` archives, two pond invocations." Confirms with `dialoguer` before deleting source data (default no).
- `pond config show` -> figment provenance walker, redaction, comfy-table render.
- `pond config path` -> one-liner.
- `pond config check` -> active-storage probe (list one object under the URL's prefix). Reports success, network error, auth error, missing-credentials error each as a distinct exit code.

### Phase 3: docs and live-config update

- `docs/spec.md` -> update the "Storage substrate" section to describe the registry + URL grammar.
- `docs/src/quickstart.md` -> update first-run flow to use `pond storage add`.
- Rewrite the auto-generated config.toml header comment (`src/config.rs:DEFAULT_CONFIG_BODY`) with the new schema and precedence table.
- Live user config (`~/.config/pond/config.toml`): manual one-time edit to convert the existing `[storage]` block. Not pond's job to migrate; documented in the breaking-changes section of the PR description.

## Tests

- `src/storage_url.rs`:
  - Every scheme in the grammar table: round-trip parse + translate.
  - Malformed inputs (missing bucket, embedded creds, unknown scheme).
  - Trailing-slash, leading-slash, percent-encoded prefix handling.
- `src/config.rs`:
  - `[storages.<name>]` round-trip (parse, serialize, parse again, equal).
  - `deny_unknown_fields` rejects typos with the offending key in the error.
  - One-of validator across three secret-source variants.
  - `default` naming a missing entry -> validate error.
  - Name regex `[a-z][a-z0-9-]{0,31}` enforced.
- `src/substrate.rs`:
  - Secret source: inline, file read, command exec (with a stub script).
  - Command exit code propagation; trailing newline stripping.
  - Resolver merges path-derived options + cred-source options without duplication.
- `src/main.rs` integration:
  - `pond storage add` writes, `pond storage list` shows, `pond storage rm` removes; atomic-rename observed.
  - `pond --storage prod ...` overrides default for one invocation.
  - `pond config show --explain` shows source attribution from figment metadata; secrets redacted.
  - `pond config check` probes a `shared-memory://` store; success path.
  - `pond migrate --from A --to B` round-trips a small dataset between two `shared-memory://` stores.
- Figment layering integration: file says `path=A`, env says `path=B`, CLI says `path=C`; resolved value is `C` with source `cli`. One test asserts the full ladder.

## Breaking changes

Pre-v1 per CLAUDE.md, no shim. The PR description enumerates the migration so any tester can apply it:

1. Rename `[storage]` to `[storages.<name>]` in `~/.config/pond/config.toml`. Pick a name (e.g. `local`, `prod`).
2. Set `default = "<name>"` at the top.
3. Rewrite each key:
   - `AWS_ACCESS_KEY_ID` -> `access_key_id`
   - `AWS_SECRET_ACCESS_KEY` -> `secret_access_key`
   - `AWS_REGION` -> drop (or `POND_STORAGE_EXTRA__REGION` env)
   - `AWS_ENDPOINT` -> drop; fold into `path` as `s3+https://host/bucket/prefix`
   - `allow_http` -> drop; controlled by scheme (`s3+http` vs `s3+https`)
   - `aws_virtual_hosted_style_request` -> drop (defaults to true) or `POND_STORAGE_EXTRA__VIRTUAL_HOSTED_STYLE_REQUEST=false`
4. Replace `--data-dir <URL>` calls with `--storage-path <URL>` or pre-register via `pond storage add`.
5. Replace `POND_DATA_DIR` env with `POND_STORAGE_PATH`.

After the rewrite, the user's `[storage]` block:

```toml
[storage]
AWS_ACCESS_KEY_ID = "..."
AWS_SECRET_ACCESS_KEY = "..."
AWS_REGION = "nbg1"
AWS_ENDPOINT = "https://ttq.nbg1.your-objectstorage.com"
aws_virtual_hosted_style_request = "true"
```

becomes:

```toml
default = "prod"

[storages.prod]
path              = "s3+https://ttq.nbg1.your-objectstorage.com/my-pond/prefix"
access_key_id     = "..."
secret_access_key = "..."
```

## Out of scope

- `pond config edit` (just open `$EDITOR` on the config path). Trivial follow-up; not load-bearing.
- OS keychain integration (macOS Keychain, Windows Credential Manager, Linux KeyRing). Adds a `keyring` crate dep and platform conditionals. Deferred; the `_command` value source already lets users get there via `op read` / `security find-generic-password` / `secret-tool lookup`.
- Multi-namespace pond (one process, many independent ponds in one Lance store). Spec already calls this out as v2; the registry shape is forward-compatible.
- Scoped secrets a la DuckDB (longest-URI-prefix match). Pond is single-active at a time; the registry binds creds to a name, not to a URL prefix. Forward-compatible if multi-active ever lands.
- `--no-prompt` flag on `pond storage add`. Non-interactive mode is implicit: if required flags are missing on a non-TTY stdin, error with the missing field. No extra flag needed.

## Risks and mitigations

- **Live config breaks for every existing pond user on first run after merge.** Mitigated by: the PR description includes the exact rewrite for the user's known config; `pond status` and `pond sync` both detect the legacy `[storage]` block and print a one-line migration hint pointing at the spec section.
- **`s3+https://` parser drifts from object_store's URL handling over time.** Mitigated by: the parser only translates the scheme prefix; everything after `://` goes through `url::Url` and the bucket extraction matches object_store's. One golden test per scheme in `storage_url.rs` plus a property test on randomized prefixes.
- **Figment env splitting on double-underscore is non-standard.** Most prior art uses single underscore. We need double specifically because `access_key_id` already contains underscores. Documented in code and in the config header comment with an example.
- **`pond config check` could leak path-prefix info if the bucket allows unauthenticated listing.** Mitigated by: probe uses a HEAD on a synthetic key (`__pond_config_check__`), not a list. 404 is a success signal for "auth works."

## Estimated footprint

- New code: ~600 LOC (`storage_url.rs` ~120, subcommands ~200, secret-source resolver ~80, figment composition ~50, redaction + provenance walker ~80, scattered ~70).
- Modified: `config.rs` ~150 LOC delta, `substrate.rs` ~80, `main.rs` ~200.
- Removed: `parse_data_dir`, opaque `[storage]` map plumbing, `--data-dir` flag everywhere.
- Tests: ~400 LOC (unit + integration).
- One commit if the diff stays under ~1500 LOC delta; otherwise split as phase 1 (schema + resolution + main wiring), phase 2 (subcommands + config show), phase 3 (docs + spec update).
