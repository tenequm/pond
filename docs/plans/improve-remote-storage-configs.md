# Improve remote storage configs

Status: planned (not started)

## Summary

Replace pond's opaque `[storage]` map and `--data-dir` URL with: a typed `[storage] path` (single default destination), URL-scoped `[creds.<name>]` credential sets resolved per URL by longest-prefix match, a fat-URL grammar that encodes endpoint + bucket + prefix in one token, a mechanical `POND_*` env mirror, figment-layered resolution with per-field provenance, `pond config show` / `pond config check` introspection, and an idempotent `pond migrate --from <URL> --to <URL>`.

The core invariant: every command works with no config file. URLs + env vars are always sufficient; the file is convenience, not a requirement.

Wire-breaking, pre-release. OK per CLAUDE.md ("breaking changes are free").

## Context

Why now: pond has its first external multi-machine user - two hosts cron-syncing into one Hetzner bucket. Multi-writer OCC already works (jittered `retry_lance`, `SkippedConflict` maintenance, `s3_backend.rs` proves the `If-None-Match` primitive). The config surface is the remaining friction: today the user hand-writes a raw `object_store` passthrough map (`AWS_ACCESS_KEY_ID` next to `aws_virtual_hosted_style_request`), duplicated per machine, with no introspection and no probe - first failure is mid-sync from cron.

Decisions that shaped this design, recorded so they are not relitigated:

- **Single default destination, no named-storage registry.** An earlier revision of this plan proposed `[storages.<name>]` + `default = "<name>"` + `pond storage use/add/rm`. Rejected: it reintroduces name-activated state ("the active storage") and per-invocation bookkeeping for what is a static infrastructure fact. Addresses are URLs, passed directly; commands that touch two storages take two URLs.
- **Credentials bind to URLs by scope, not by name.** The git `[credential "https://host"]` / DuckDB `CREATE SECRET ... SCOPE` model, not the AWS `[profile]` model. A profile is activated by name; pond's creds sets are never activated - they match. This is what makes multi-storage commands need zero extra syntax, lets one set cover N prefixes under one account, and keeps credential rotation out of argv (pond's primary callers are cron and MCP, where the invocation is frozen).
- **Section names.** `[storage]` is pond's own spec vocabulary (spec.md#substrate) and the infra convention (quickwit, Loki). `[creds.*]` is purpose-named like git's `[credential "url"]` and DuckDB secrets - both precedents carry non-secret connection fields under that name too. "remote" is wrong (the path can be local); "profile" imports the wrong (name-activation) mental model.
- **No credentials in URLs, ever.** RFC 3986 deprecates userinfo; litestream hard-rejects it; argv/history/ps/logs are one leak class. Parse error.
- **No secret-bearing CLI flags.** argv leaks exactly like creds-in-URL. Secrets travel via env, file, or command output only.
- **Endpoint lives inside the URL** (`s3+https://host/bucket/prefix`). Litestream's bare `s3://bucket` + out-of-band endpoint produced years of breakage (litestream #666, #811; #104 had credentials sent to AWS on misparse), patched three separate times. The fat-URL grammar avoids the class by construction.

## Locked design

### Config schema

```toml
# ~/.config/pond/config.toml
[storage]
path = "s3+https://nbg1.your-objectstorage.com/my-pond"

# Catch-all set: no scope = matches any URL (lowest precedence among sets).
[creds.default]
access_key_id             = "..."
secret_access_key_command = "op read op://vault/pond/secret"

# Scoped set: matches URLs under this prefix; longest match wins.
[creds.work]
scope             = "s3+https://fsn1.your-objectstorage.com/work-pond/"
access_key_id     = "..."
secret_access_key = "..."
region            = "fsn1"
```

Rules:

- `[storage].path` is optional; absent = the platform-local default data dir (today's behavior). Local schemes need nothing else.
- Set names match `[a-z][a-z0-9]{0,15}` - lowercase alphanumeric, no separators. Load-bearing: it makes the env grammar `POND_CREDS_<NAME>_<FIELD>` splittable at the first `_` after the name with zero ambiguity (field names contain underscores; names must not).
- Typed fields per set, `serde(deny_unknown_fields)`: `scope`, `access_key_id` / `access_key_id_file`, `secret_access_key` / `secret_access_key_file` / `secret_access_key_command`, `region`, `virtual_hosted_style_request`, and `extra` (inline table of verbatim `object_store` options for knobs pond has not typed; redaction rules apply to its keys by name).
- At most one variant per logical secret field; `secret_access_key` + `secret_access_key_command` together is a parse error naming the pair.
- At most one scope-less set; duplicate scopes across sets are a parse error.
- Unknown keys are a hard error (typos die loudly).

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
memory://name, shared-memory://name  tests only, never in production paths
```

- Embedded credentials (`s3+https://user:pass@host/...`) are rejected at parse with an error naming the env/file alternative.
- Recognized query params, stripped before the URL reaches Lance: `creds` (explicit set binding), `region`, `virtual_hosted_style_request`. A query param beats the matched set's field for the same knob.
- The parser sits on `url::Url`; only the scheme dispatch and translation are pond's (~120 LOC).

Translation table (parser output -> Lance call):

| Input URL                            | Lance URL               | Injected options                                                      |
|--------------------------------------|-------------------------|-----------------------------------------------------------------------|
| `file:///abs/path`                   | `file:///abs/path`      | none                                                                  |
| `s3://bucket/prefix`                 | `s3://bucket/prefix`    | none (ambient chain may apply)                                        |
| `s3+https://host/bucket/prefix`      | `s3://bucket/prefix`    | `endpoint=https://host`, `allow_http=false`, `virtual_hosted=true`    |
| `s3+http://host:port/bucket/prefix`  | `s3://bucket/prefix`    | `endpoint=http://host:port`, `allow_http=true`, `virtual_hosted=true` |
| `gs://bucket/prefix`                 | `gs://bucket/prefix`    | none                                                                  |
| `az://account/container/prefix`      | `az://container/prefix` | `account_name=account`                                                |

`virtual_hosted_style_request` defaults to `true` for `s3+https`/`s3+http` (Hetzner, R2, B2); MinIO/Garage users override via query param or creds-set field. `allow_http` is scheme-derived, never a config field.

### Per-URL resolution

Every storage URL, on every command, resolves independently - there is no "active storage" process state:

1. **Creds binding**: `?creds=<name>` (error if no such set) > longest-prefix scoped match > the scope-less set if any > none (object_store's ambient chain: `AWS_*` env, shared credentials file, IMDS/container metadata).
2. **Options assembly**, later wins: scheme-derived -> matched set's non-secret fields and `extra` -> URL query params.
3. **Secrets materialized**: inline / file / command. `_command` output is cached per set per process (runs once); a non-zero exit surfaces the command text. A single trailing newline is stripped.

Scope matching (normative; these become spec.md rules with anchors):

- URLs and scopes are compared canonicalized: scheme and host lowercased, default ports stripped, path compared as written.
- A scope matches a URL only at `/` segment boundaries: scope `.../pond` does not match `.../pond-2`.
- Longest matching scope wins. Ties are impossible: duplicate scopes are rejected at parse.
- No cross-scheme normalization in v1: a scope written `s3+https://host/bucket/` does not match a `s3://bucket/` URL.
- A defined set that matches no URL in the invocation prints a warning naming the set (misbinding must never be silent).

The SDK-chain fallback is an explicit, documented invariant: when a URL resolves to no creds set, pond passes no credential options and the standard cloud credential chain applies. EC2 instance profiles, ECS task roles, GitHub OIDC, and `aws sso login` all work with zero pond config.

### Env mirror

| TOML                       | Env                                          |
|----------------------------|----------------------------------------------|
| `storage.path`             | `POND_STORAGE_PATH`                          |
| `creds.<name>.scope`       | `POND_CREDS_<NAME>_SCOPE`                    |
| `creds.<name>.<field>`     | `POND_CREDS_<NAME>_<FIELD>` (name uppercased)|

- Sets are discovered by scanning the environment for `POND_CREDS_*`; an env set merges with a same-named file set field-by-field, env beating file.
- `extra` has no env form (use typed fields or query params; keeps the env grammar trivial).
- Zero-config deployment is `POND_STORAGE_PATH` plus either `POND_CREDS_*` or ambient `AWS_*`. This is the disaster-recovery invariant: restore and migrate must work on a fresh machine where the config file never existed.

### CLI surface

- `--storage-path <URL>` (env `POND_STORAGE_PATH` via clap `env =`, visible in `--help`) on every command that opens storage. Replaces `--data-dir` / `POND_DATA_DIR`.
- No secret-bearing flags exist anywhere.
- New subcommands:

```
pond config show              # resolved config: redacted values, source column (cli/env/file/default),
                              # and the active URL's creds binding ("-> creds work (file)" / "-> ambient chain")
pond config path              # absolute path of the loaded config file
pond config check [URL]       # probe (defaults to storage.path): parse, resolve creds (naming the set used),
                              # conditional-put probe (If-None-Match -> 412 expected; the OCC primitive,
                              # spec.md#substrate), small write/read/delete under a synthetic key.
                              # Distinct exit codes: ok / parse error / no creds / auth failed / OCC unsupported.
pond migrate --from <URL> --to <URL>
                              # copy canonical data between storages via the export+import path.
                              # Idempotent union merge (deterministic PKs + merge_insert): re-runnable,
                              # resumable, valid onto a populated destination. Never deletes the source.
                              # Destination indexes catch up via the normal optimize pass.
```

Multi-URL commands print the per-URL binding line before doing work, so a wrong scope match is visible immediately, not after an auth error.

### Precedence

```
CLI flag > POND_* env > config.toml > ambient cloud chain (object_store) > built-in defaults
```

One table; printed by `pond config show`, repeated in the generated config header comment. Auth failures name the creds set that was used and why it matched.

### Redaction

`pond config show` redacts any field whose name contains `key`, `secret`, `token`, or `password` (case-insensitive), printing `********` regardless of length - including keys inside `extra`. Exception: `_command` and `_file` variants print their literal value (the path / command IS the safe part).

### Legacy config detection

The pre-redesign `[storage]` map (ENV-style keys like `AWS_ACCESS_KEY_ID`) fails `deny_unknown_fields`. Catch that shape specifically and print the exact rewrite - old keys mapped onto `[storage].path` + `[creds.default]` - per CLAUDE.md "output names the fix". This is an error with a recipe, not a shim: old configs do not keep working.

## Implementation

### Phase 1 - schema, resolver, introspection (one commit, breaking: `feat(config)!:`)

- `Cargo.toml`: add `figment` (the one new dependency).
- `src/config.rs`: remove `storage: BTreeMap<String, String>`; add `StorageConfig { path: Option<String> }` and `creds: BTreeMap<String, CredsSet>`; validators (name charset, one-of secret variants, single scope-less set, duplicate scopes); legacy-shape detector with the rewrite recipe; figment composition (`Toml` -> `Env::prefixed("POND_")` -> `Serialized::defaults(cli)`) extracting one typed `Config` with provenance metadata retained for `config show`.
- `src/substrate.rs`: URL parser + scheme translation (inlined here, not a new file - split only if it grows past its welcome); scope matcher; per-URL resolver producing the `storage_options` map for `Handle::open_with_options`; secret-source materializer with per-process command cache.
- `src/main.rs`: rename `--data-dir` -> `--storage-path` everywhere; `pond config show|path|check` via the existing `pond::output` / `new_table()` stack.

### Phase 2 - migrate (own commit)

- `pond migrate --from <URL> --to <URL>` over the existing export+import primitive, `indicatif` progress, no deletion path. The rerun-is-a-no-op property is a test, not a promise.

### Phase 3 - docs (own commit)

- `docs/spec.md` storage-substrate section: URL grammar table, scope-matching rules with anchors (`spec.md#storage-url-grammar`, `spec.md#creds-scope-match`), the resolution ladder, the config-less invariant.
- Quickstart and the generated config header comment (`DEFAULT_CONFIG_BODY`) rewritten to the new schema, including the two-machine shared-bucket example.

## Tests

- Parser: golden round-trip per scheme; translation table asserted; userinfo rejected; trailing-slash / port / percent-encoded-prefix cases; query params stripped and applied.
- Config: schema round-trip; `deny_unknown_fields` names the offending key; one-of secret validator; name regex; single scope-less rule; duplicate-scope error; legacy shape produces the fix-naming error (golden).
- Resolver: scope table - segment boundary (`/pond` vs `/pond-2`), longest-match, `?creds=` pointer, pointer to missing set, scope-less fallback, ambient fallback, unmatched-set warning; option-assembly precedence (scheme < set < query).
- Env: `POND_CREDS_*` discovery; env-field-beats-file-field merge; the full ladder in one test (file says A, env says B, CLI says C -> C with source `cli`).
- Secrets: inline / file / command; exit-code propagation with command text; trailing-newline strip; command runs exactly once per process.
- `config show`: redaction (including `extra` keys); source attribution; binding line.
- `config check`: success and each distinct failure class against `shared-memory://` and the existing `s3s` fixture (the `If-None-Match` -> 412 path in `tests/integration/s3_backend.rs`).
- `migrate`: round-trip between two `shared-memory://` stores; immediate rerun is a no-op; union onto a populated destination.

## Breaking changes

Pre-v1, no shim. The migration for the known live config:

```toml
# Before
[storage]
AWS_ACCESS_KEY_ID = "..."
AWS_SECRET_ACCESS_KEY = "..."
AWS_REGION = "nbg1"
AWS_ENDPOINT = "https://ttq.nbg1.your-objectstorage.com"
aws_virtual_hosted_style_request = "true"

# After
[storage]
path = "s3+https://ttq.nbg1.your-objectstorage.com/my-pond/prefix"

[creds.default]
access_key_id     = "..."
secret_access_key = "..."
```

1. `--data-dir <URL>` -> `--storage-path <URL>`; `POND_DATA_DIR` -> `POND_STORAGE_PATH`.
2. `AWS_ENDPOINT` + bucket fold into the `s3+https://` path; `AWS_REGION` -> `region` field or `?region=`; `allow_http` is gone (scheme-derived); `aws_virtual_hosted_style_request` -> `virtual_hosted_style_request` (defaults true).
3. Release notes carry the same recipe the legacy-detection error prints.

## Out of scope

- Named storages / registry / `pond storage use|add|rm` - rejected, see Context; not deferred.
- Secret-bearing CLI flags - deliberate, not deferred.
- OS keychain; interactive connect wizard (`config check` covers probing; `_command` reaches keychains via `security` / `op` / `secret-tool`).
- gs/az typed creds fields - ambient chain only in v1; `extra` covers stragglers.
- Host/user provenance on ingested sessions (the multi-machine attribution ask) - next feature, separate PR.
- Live-write path, MemWAL, storage tiering, read caching - separate roadmap items; nothing here blocks them (the resolver is per-`Handle` input, no global state).

## Risks and mitigations

- **Scope misbinding is silent by nature.** Mitigated three ways: `config show` / multi-URL binding lines, unmatched-set warnings, and auth errors naming the set used and why it matched. These ship in phase 1, not as follow-up.
- **Parser drift vs object_store's URL handling.** Golden test per scheme plus a property test on randomized prefixes; everything after `://` stays `url::Url`'s problem.
- **Env grammar ambiguity.** Killed structurally by the alphanumeric-only set-name rule; no `__` separator conventions needed anywhere.
- **The one real user breaks on upgrade.** The legacy detector prints the exact rewrite; the release notes repeat it; the interim raw-map recipe he runs today maps 1:1 onto the example above.

## Estimated footprint

`config.rs` ~200 LOC delta, `substrate.rs` ~250, `main.rs` ~250, tests ~450. Three commits as phased above; phase 1 carries the `!`.
