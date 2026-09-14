---
type: Decision
title: CI compiler-cache config is per-job and never at the repo root
description: A repo-root .kache.toml would point every contributor's local build at a credential-less bucket, so config lives under .github/kache/ reached only through KACHE_CONFIG, with one S3 prefix per build shape carrying its key-schema tag.
tags: [ci, caching, kache, windows, decision]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: ci-caching-plan
    resource: ../../plans/2608-24-ci-caching-architecture.md
    title: "CI caching architecture: fast, robust, and settled once (the long form)"
---

# CI compiler-cache config is per-job and never at the repo root

## Never a repo-root .kache.toml

kache's config discovery is `KACHE_CONFIG` > nearest `.kache.toml` > XDG. A
file at the repo root would therefore be found by **every contributor running
kache locally as `RUSTC_WRAPPER` anywhere inside the checkout**, silently
pointing them at the project's bucket with no credentials, so every lookup pays
a request that cannot hit.

Config instead lives in committed per-job files under `.github/kache/`, reached
only through a `KACHE_CONFIG` the bootstrap exports before the kache-action
step runs.[^ci-caching-plan]

Each file carries only `[cache.remote]`. Everything pond tunes - cache dir, max
size, prefetch caps, manifest key, namespace (the last two have no file form at
all) - stays in env. `[cache] ignore_env` is never set in these files, because
it would silence exactly those variables.

## Prefix and key-schema discipline

One S3 prefix per build shape, carrying its key-schema tag from the day it is
created (`kache/pond/windows-verify/k25`, and so on). Nothing is shared across
target triples anyway, since the rustc flags differ per unit. Tagging from day
one keeps the next rotation a **sibling** rather than a directory nested inside
the old prefix, which is what keeps prefix-level bucket GC unambiguous. A
prefix is rotated on a key-schema bump, and that rotation is the remote GC.

The file's `prefix` must equal the action's `s3-prefix` input, and the
bootstrap asserts it and fails the job if they differ. This is not cosmetic: at
one version the daemon loses the env prefix while the CLI keeps it, so a
mismatch means the daemon prefetches from and uploads to prefix A while
`save-manifest` writes prefix B. Prefetch then silently never lands, which is
the exact failure the architecture exists to remove.

## Warm in bulk, and mind MOON_FORCE

The store is warmed before cargo starts with `kache sync --pull --all` from the
bootstrap, not the action's `sync: true`: `--all` is one paginated LIST plus
concurrent downloads, against roughly 1450 demand fetches or a filtered
enumeration of hundreds of LISTs. It is only safe because the prefix is
per-job, which makes prefix size a rotation trigger too.

`MOON_FORCE` is the clap env binding for `--force`, not a presence check, so it
must be the literal `true`; moon dies with `error: invalid value '1' for
'--force'` on anything else. Every acceptance and prewarm run sets it so moon's
remote cache cannot skip the leg being measured.

Windows-specific: `CARGO_HOME` goes on the fast D: disk, while `RUSTUP_HOME`
stays on C:, because rustup's proxies resolve toolchains out of `RUSTUP_HOME`
and never read `CARGO_HOME`.

## Invalidation

The discovery-order reasoning holds as long as kache resolves config by walking
up from the working directory. The prefix-mismatch hazard is version-specific
and should be re-checked on a kache major bump.

[^ci-caching-plan]: `docs/plans/2608-24-ci-caching-architecture.md`
