# Migrate from local to remote storage

This guide moves an existing local pond install onto an S3-compatible object store (Hetzner, Cloudflare R2, Backblaze B2, MinIO, AWS S3, ...). It is written to be run by a person at a terminal, and to be executed by an agent or CI job with no terminal - every interactive step carries an `Agents / CI` note with its non-interactive equivalent, and the whole thing is repeated as one script at the end.

The shape of the migration is four steps that each do exactly one thing: **add credentials -> probe -> copy -> switch**. The copy step verifies itself and rebuilds the destination's indexes, so a clean `migrate` exits with the destination provably complete and ready to query - you never reconcile row counts by hand. Your local data is never modified or deleted at any point; it stays as a full backup.

## What this does, and what it never does

- It copies your canonical sessions to a new storage URL and points pond at it.
- It never deletes or mutates the local store at `~/.local/share/pond`. That directory remains a complete, queryable copy - treat it as your backup, not a cleanup target.
- It never moves data as a side effect of switching. `pond storage use` only flips a pointer; copying is always the separate, explicit `pond migrate`.

## Before you start

You need three things: pond installed with local data, a bucket, and its S3 credentials. The non-interactive `Agents / CI` snippets below branch on command exit codes alone - no extra tooling to install.

1. Confirm the local store has data:

   ```sh
   pond status
   ```

   Confirm the `sessions / messages / parts` row counts are non-zero - that is all the baseline you need, because migrate verifies the copy's completeness itself at the end.

2. Create a bucket at your provider and get S3 credentials. Worked example for Hetzner:
   - Hetzner Cloud Console -> your project -> Object Storage -> create a bucket (pick a region, e.g. Nuremberg `nbg1`, Falkenstein `fsn1`, or Helsinki `hel1`). Note the bucket name, say `my-pond`.
   - Generate S3 credentials for it - the access key and secret are shown once; save both.
   - Your destination URL is then `s3+https://<region>.your-objectstorage.com/<bucket>`, e.g. `s3+https://nbg1.your-objectstorage.com/my-pond`.

The `s3+https://host/bucket` form carries the endpoint and bucket in one URL, so the endpoint can never desync from the bucket. pond writes its datasets at the bucket root; if you want them under a subpath instead (to share the bucket with other data), append one - `s3+https://host/bucket/some-prefix`. The region is auto-detected for real AWS buckets and defaulted for S3-compatible endpoints; you rarely need to set it - if a provider needs a specific one, override with `?region=<id>` on the URL or a `region` field on the credential set. Other schemes (`s3://`, `gs://`, `az://`) work too and fall back to the ambient cloud SDK credential chain when no credential set matches.

Throughout this guide, set the destination once so the commands are copy-paste:

```sh
DEST=s3+https://nbg1.your-objectstorage.com/my-pond
```

## Step 1 - Add credentials

```sh
pond creds add
```

It prompts for a set name (press Enter for `default`), your access key ID (visible), and your secret access key (hidden). It writes a `[creds.<name>]` block to `config.toml`. A single scope-less `default` set matches any URL, which is all one bucket needs.

> **Agents / CI:** `creds add` requires a terminal for the hidden secret prompt and will refuse to run without one. Instead, provide credentials via the environment (a complete configuration on its own, no config file needed):
>
> ```sh
> export POND_CREDS_DEFAULT_ACCESS_KEY_ID="<access-key>"
> export POND_CREDS_DEFAULT_SECRET_ACCESS_KEY="<secret-key>"
> ```
>
> Or keep the secret out of the environment with a file or command in `config.toml`: `secret_access_key_file = "/run/secrets/pond-s3"` or `secret_access_key_command = "op read op://vault/pond/secret"`. Secrets must never be passed as CLI flags or embedded in the URL - those leak into shell history, process listings, and logs, and pond rejects URL-embedded credentials at parse.

## Step 2 - Probe the destination

This is a gate. Do not continue until it passes.

```sh
pond storage check "$DEST"
```

It parses the URL, resolves credentials, performs a conditional put (the optimistic-concurrency primitive pond relies on), reads the object back, and deletes it. Exit code `0` means the destination is fully usable. (`check` takes an optional URL; with no argument it probes your currently configured store instead.)

> **Agents / CI:** branch on the exit code, do not parse the prose. `0` ok, `2` parse error, `3` no credentials matched, `4` auth failed, `5` the store lacks conditional-put. See the troubleshooting table for the fix per code.
>
> ```sh
> pond storage check "$DEST" || exit 1
> ```

## Step 3 - Copy, index, and verify

```sh
pond migrate --from ~/.local/share/pond --to "$DEST"
```

This is the only step that moves data. It is an idempotent union merge: re-runnable, resumable, and valid onto a populated destination; the source is never modified. When the copy finishes, migrate rebuilds the destination's search indexes and then compares the `id` set of every table end to end - it exits `0` only when the destination provably contains every source row (printing a `verify: SYNCED` line) and exits `6`, naming the short table, otherwise. You do not reconcile counts by hand.

> **Note:** if the copy is interrupted (network drop, timeout), just run the same command again. Rows already at the destination are skipped, not duplicated. Never wipe the destination to "retry clean" - re-running converges.

> **Large stores:** the index rebuild is the slow part. Pass `--skip-indexes` to defer it - the copy and verify still run - then build the indexes later with `pond sync --only update-indexes --storage-path "$DEST"`.

> **Agents / CI:** branch on the exit code - `0` synced and ready, `6` destination missing source rows (re-run migrate; it converges). No `jq`, no count parsing:
>
> ```sh
> pond migrate --from ~/.local/share/pond --to "$DEST" || exit 1
> ```

## Step 4 - Switch pond to the bucket

```sh
pond storage use "$DEST"
```

`use` re-probes the destination and then flips `[storage].path` in `config.toml`. It moves no data - the copy already happened in Step 3. It reads no store other than the destination probe; its closing hint prints the exact `migrate --from <old> --to <new>` command so copying the old data over later is one paste away.

> **Agents / CI:** instead of writing config, set the destination in the environment - `POND_STORAGE_PATH` overrides config everywhere, so containers and ephemeral agents need no `use` step at all:
>
> ```sh
> export POND_STORAGE_PATH="$DEST"
> ```

## Step 5 - Confirm

migrate already verified the copy and built the indexes, so this is just a final sanity check that the switch points where you expect:

```sh
pond status
pond search "something you remember discussing"
```

> **Agents / CI:** re-check membership at any time, read-only and without copying, with `pond migrate --verify-only` - exit `0` synced, `6` diverged:
>
> ```sh
> pond migrate --verify-only --from ~/.local/share/pond --to "$DEST"
> ```

## Roll back, keep your backup

Rolling back is the same switch in reverse, and it is instant and safe because `use` never touched any data. The keyword `local` resolves to the default local data dir, so you do not need to remember its path:

```sh
pond storage use local
```

If you took the agents/CI path and exported `POND_STORAGE_PATH`, `unset` it (or point it back at the local path) - the environment overrides `config.toml`, so `use` alone won't take effect while it is set.

Keep `~/.local/share/pond` after migrating. It is your only full local copy and the doc deliberately does not delete it. If you genuinely need the disk space, take a portable snapshot elsewhere first with `pond export -o ~/pond-backup.pond` rather than removing the live directory.

Switching storage is transparent to your agent clients: they talk to pond over `pond mcp`, which reads the same `config.toml`, so nothing in Claude Code / Codex / others needs reconfiguring.

## Several machines, one bucket

Point each machine's `config.toml` (or `POND_*` environment) at the same URL and credentials, and run `pond sync` on each. Concurrent writers are safe: Lance serializes commits through the object store's conditional writes (optimistic concurrency), so two machines syncing at once cannot corrupt the dataset.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `check` exits `1` | Generic I/O error - DNS failure, endpoint unreachable, bucket missing | Check connectivity and the endpoint host; run `pond -vv storage check "$DEST"` for the full error |
| `check` exits `2` | URL malformed, unknown query param, or embedded credentials | Fix the URL grammar; never put a secret in the URL |
| `check` exits `3` | No credential set matched the destination | Add `[creds.default]` (`pond creds add`) or export `POND_CREDS_DEFAULT_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY`; confirm the binding with `pond config show` |
| `check` exits `4` | Auth failed - wrong key/secret, or bucket policy denies write/read/delete | Re-check the credentials; ensure the key can put/get/delete/list the bucket; run `pond -vv storage check "$DEST"` for the full error |
| `check` exits `5` | The store does not support conditional put | Use a store with conditional writes (Hetzner, R2, B2, AWS S3, GCS, Azure; recent MinIO) |
| `migrate` stalls or is interrupted | Transient network/timeout | Re-run the same `migrate` - it resumes and de-duplicates |
| `migrate` (or `migrate --verify-only`) exits `6` | Destination is missing source rows (the copy did not finish) | Re-run `migrate` - the union merge converges; do not delete the destination. Re-check read-only with `pond migrate --verify-only --from <src> --to <dest>` |
| `pond status` still shows local data after `use` | `POND_STORAGE_PATH` is set and overrides config | Unset it, or set it to the new URL |
| `pond sync` auth-fails in cron after switching | The scheduler's environment does not inherit your shell exports | Put `POND_CREDS_*` in the launchd plist / systemd unit / crontab environment |

## One-shot script (agents / CI)

The whole migration, non-interactive, with gates. Needs only `pond`:

```sh
set -euo pipefail

export POND_CREDS_DEFAULT_ACCESS_KEY_ID="<access-key>"
export POND_CREDS_DEFAULT_SECRET_ACCESS_KEY="<secret-key>"
SRC=~/.local/share/pond
DEST=s3+https://nbg1.your-objectstorage.com/my-pond

pond storage check "$DEST"                                   # gate: exit 0 required

# migrate is resumable and self-verifying: exit 0 = destination provably holds
# every source row (indexes rebuilt); exit 6 = rows missing. Retry transient
# interruptions; both converge on re-run.
n=0; until pond migrate --from "$SRC" --to "$DEST"; do
  n=$((n + 1)); [ "$n" -ge 5 ] && { echo "migrate failed after $n attempts"; exit 1; }
  echo "migrate incomplete; retrying ($n)..."; sleep 5
done

pond storage use "$DEST"                                     # or: export POND_STORAGE_PATH="$DEST"
pond status
```

The local store at `~/.local/share/pond` is left intact as a backup.
