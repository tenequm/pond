# CLI end-to-end harness

Drives the compiled `pond` binary across the whole command surface against a
real remote corpus (reads) and isolated scratch contexts (writes), records
wall-clock time per operation, and prints a pass/fail + timing matrix.

Not run by CI or `cargo test`: it needs remote S3 credentials, mutates scratch
stores, and is slow (cold prewarms, full copies). Plan and rationale:
`docs/plans/2606-19-cli-e2e-real-corpus.md`.

## Run

```
python ops/e2e/run.py \
  --bin target/release/pond \
  --url s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy \
  --config ~/.config/pond/config.toml \
  --scratch-prefix s3+https://nbg1.your-objectstorage.com/pondarium/pond-e2e
```

`--skip-mutating` runs only the read + sandbox + wizard checks (no scratch
writes), useful for a fast surface check.

## Contexts (never crossed)

- read: `--url` + `--config` (real creds), read-only commands only.
- sandbox: a per-case temp `HOME`/`XDG_*` with a `config.toml` carrying a copy
  of the real `[creds.*]` block; every config-writing or interactive command
  runs here, so the real config and launchd are never touched.
- scratch: sibling bucket prefixes `<scratch-prefix>-sync` / `-copy-dest` for
  store-mutating commands; kept after the run (no teardown).

## What it covers

Reads (timed cold + warm): `status`, `status -v`, `search` (vector + fts),
`get` (session + message, ids discovered via `sql`), `sql` (count, group-by),
`config show/path/schema`, `completions`, `storage check`, `creds list`,
`schedule status`, `adapters list`, `mcp` initialize handshake, global-flag
parse-order, and the read-side failure exit codes (2 parse, 1 not-found,
6 verify-fail).

Sandbox config-mutating: `init --yes`, legacy `[sources]`->`[adapters]`
migration, `adapters enable/disable`, `storage use` + rollback, `creds delete`.

Wizards (PTY): `init`, `creds add`, `adapters discover`.

Store-mutating timing: `sync` appends, `copy` prefix->prefix, `copy
--verify-only`, `optimize`.

## Notes

- Interactive prompt anchors in `run.py` (`drive_wizard` scripts) are matched
  against ANSI-stripped output; if a cliclack prompt label changes, update the
  anchor substring there.
- The cold full sync into an empty bucket is run once as a seeding step outside
  this harness (it is a one-shot, not repeatable against a now-populated
  store); its time is reported separately. The harness exercises the repeatable
  surface.
