---
type: Decision
title: Windows is msvc-only, and the msvc main-thread stack reserve is the trap
description: The gnu artifact shipped for releases without ever running in CI and was broken at runtime; msvc replaced it, but the 1 MiB msvc stack reserve overflows on pond's async state machine before argument parsing.
tags: [windows, msvc, ci, portability, decision]
status: stable
generated: { by: "claude-code/opus-5", at: "2026-09-11T13:28:09Z" }
sources:
  - id: windows-plan
    resource: ../../plans/2608-13-windows-support-plan.md
    title: "Windows support: staged native port (the long form, with all settled decisions)"
  - id: pr147
    resource: https://github.com/tenequm/pond/pull/147
    title: "PR #147, where the stack overflow was found"
  - id: protobuf-src-windows
    resource: https://github.com/MaterializeInc/rust-protobuf-native/issues/4
    title: "protobuf-src has been broken on Windows since 2022"
---

# Windows is msvc-only, and the msvc main-thread stack reserve is the trap

## What was shipping before

The release pipeline cross-compiled `x86_64-pc-windows-gnu` and uploaded a zip
to every release, but **no CI job had ever executed it**, and it was broken at
runtime in two independent ways.[^windows-plan]

All path resolution read `HOME`, which is unset on native Windows, so adapter
discovery found nothing, the store fell back to a cwd-relative `.pond`, config
to `.pond.toml`, and the state directory (sync lock, last-sync record) moved
with `cd`. Tests inject `HOME` and `XDG_*`, so CI could never have caught it -
the injection that makes tests hermetic is exactly what hid the bug.

## The stack-reserve trap

pond's whole CLI future runs on the main thread, and the main-thread stack
reserve is a **linker default**: 1 MiB under MSVC and lld, 2 MiB under binutils
ld, against 8 MiB for a unix main. Constructing and polling pond's async state
machine overflows the msvc build *before argument parsing*, so even `--version`
crashes.[^pr147] This is a portability trap with no compile-time signal and no
relation to any code the change touches.

## Settled decisions

- **Targets: `x86_64-pc-windows-msvc` only.** The gnu artifact was dropped
  immediately rather than deprecated, because it never worked at runtime and
  nobody could be depending on it. No arm64.
- **Directories** use the standard `dirs` platform mapping (config to
  `%APPDATA%\pond`, data/state/cache to `%LOCALAPPDATA%\pond\...`), with XDG
  env overrides still winning when set. No new crate: `dirs` was already in the
  graph transitively and was promoted to a direct dependency.
- **Windows CI** runs full `cargo test` on release PRs, `main` pushes, and
  manual dispatch, and is not PR-blocking on ordinary PRs. It must include the
  OCC and rowmap probes, since those are the rules a silent Windows regression
  would break.
- **No ACL code.** `%APPDATA%` inherited ACLs are the 0600 analog, and the code
  says so in a comment rather than growing a permissions layer.
- **`lance`'s `protoc` feature cannot be enabled on Windows**: it pulls
  `protobuf-src`, broken there since 2022 with CRT-mismatch link
  errors.[^protobuf-src-windows] Lance's own Windows CI does not enable the
  feature and installs a system `protoc` instead, which is the pattern to
  follow.

## Invalidation

The stack-reserve trap persists until pond stops running its whole future on
the main thread. The `protobuf-src` constraint lifts if that crate is ever
fixed upstream.

[^windows-plan]: `docs/plans/2608-13-windows-support-plan.md`
[^pr147]: [tenequm/pond#147](https://github.com/tenequm/pond/pull/147)
[^protobuf-src-windows]: [rust-protobuf-native#4](https://github.com/MaterializeInc/rust-protobuf-native/issues/4)
