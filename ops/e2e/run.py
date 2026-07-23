#!/usr/bin/env python3
"""End-to-end CLI test + timing harness for the real `pond` binary.

Drives the compiled binary (not library entry points) across the whole command
surface, against a real remote corpus for reads and isolated scratch contexts
for writes. Records wall-clock time per operation and prints a pass/fail +
timing matrix. Gated out of CI: needs remote S3 creds, mutates scratch stores,
and is slow. See docs/plans/2606-19-cli-e2e-real-corpus.md.

Contexts (never crossed):
  read     : --url (real corpus, read-only) + --config (real creds)
  sandbox  : per-case temp HOME/XDG + a seeded config.toml carrying a copy of
             the real [creds.default] block; every config-writing / interactive
             command runs here so the real config and launchd are never touched
  scratch  : sibling bucket prefixes <scratch-prefix>-sync / -copy-dest for the
             store-mutating commands; kept after the run (no teardown)

Usage:
  python ops/e2e/run.py \
    --bin target/release/pond \
    --url s3+https://nbg1.your-objectstorage.com/pondarium/pond-full-corpus-benchmarking-copy \
    --config ~/.config/pond/config.toml \
    --scratch-prefix s3+https://nbg1.your-objectstorage.com/pondarium/pond-e2e

Interactive wizards (init, adapters discover, creds add) are driven through a
pseudo-terminal (stdlib `pty`); everything else through plain subprocess.
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import pty
import re
import select
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\r")


@dataclass
class Result:
    name: str
    target: str
    ok: bool
    exit_code: int
    seconds: float
    note: str = ""


RESULTS: list[Result] = []


def strip_ansi(b: bytes) -> str:
    return ANSI.sub("", b.decode("utf-8", "replace"))


def record(name: str, target: str, ok: bool, code: int, secs: float, note: str = "") -> None:
    RESULTS.append(Result(name, target, ok, code, secs, note))
    flag = "PASS" if ok else "FAIL"
    extra = f"  {note}" if note else ""
    print(f"  [{flag}] {name:<34} {secs:7.2f}s  exit={code}{extra}", flush=True)


def run(name, argv, target, *, expect=0, anchor=None, env=None, timeout=3600, stdin=None):
    """Run a non-interactive command; record exit-code + optional output anchor."""
    print(f"-> {name}", flush=True)
    start = time.monotonic()
    try:
        p = subprocess.run(
            argv, env=env, input=stdin, capture_output=True, timeout=timeout
        )
        code, out = p.returncode, (p.stdout + p.stderr)
    except subprocess.TimeoutExpired as e:
        secs = time.monotonic() - start
        record(name, target, False, -1, secs, f"timeout; out={_tail(e.stdout)}")
        return None
    secs = time.monotonic() - start
    text = out.decode("utf-8", "replace")
    ok = code == expect and (anchor is None or anchor.lower() in text.lower())
    note = "" if ok else f"want exit={expect} anchor={anchor!r}; got: {_tail(out)}"
    record(name, target, ok, code, secs, note)
    return text


def _tail(b: bytes | None, n: int = 160) -> str:
    if not b:
        return ""
    s = b.decode("utf-8", "replace").strip().replace("\n", " | ")
    return s[-n:]


def drive_wizard(name, argv, script, env, *, target="sandbox", expect=0, deadline=120):
    """Run an interactive command under a PTY, feeding keystrokes when anchors
    appear. `script` is a list of (anchor_substring, keystrokes_bytes)."""
    print(f"-> {name} (pty)", flush=True)
    start = time.monotonic()
    pending = list(script)
    buf = ""
    pid, fd = pty.fork()
    if pid == 0:  # child
        os.environ.update(env)
        os.execv(argv[0], argv)
        os._exit(127)
    code = None
    while True:
        if time.monotonic() - start > deadline:
            try:
                os.write(fd, b"\x03")  # Ctrl-C so a missed anchor cannot hang
            except OSError:
                pass
            time.sleep(0.3)
            try:
                os.waitpid(pid, 0)
            except OSError:
                pass
            record(name, target, False, -1, time.monotonic() - start, "deadline; sent Ctrl-C")
            try:
                os.close(fd)
            except OSError:
                pass
            return
        r, _, _ = select.select([fd], [], [], 0.5)
        if fd in r:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                chunk = b""
            if not chunk:
                _, status = os.waitpid(pid, 0)
                code = os.waitstatus_to_exitcode(status)
                break
            buf += strip_ansi(chunk)
            if pending:
                anchor, keys = pending[0]
                if anchor.lower() in buf.lower():
                    time.sleep(0.2)
                    try:
                        os.write(fd, keys)
                    except OSError:
                        # Child already exited and closed the pty; stop feeding
                        # and let the exit code below decide pass/fail.
                        _, status = os.waitpid(pid, 0)
                        code = os.waitstatus_to_exitcode(status)
                        break
                    pending.pop(0)
                    buf = ""
    secs = time.monotonic() - start
    try:
        os.close(fd)
    except OSError:
        pass
    ok = code == expect and not pending
    note = "" if ok else f"want exit={expect}; got {code}; unfed={len(pending)}"
    record(name, target, ok, code if code is not None else -1, secs, note)


def seed_sandbox(base: Path, real_config: Path, storage_path: str, legacy=False) -> dict:
    """A fresh HOME/XDG sandbox with a config.toml carrying the real creds block."""
    home = base
    for sub in ("config/pond", "data", "state"):
        (home / sub).mkdir(parents=True, exist_ok=True)
    creds_block = extract_creds_block(real_config)
    table = "sources" if legacy else "adapters"
    cfg = (home / "config/pond/config.toml")
    cfg.write_text(
        f"[storage]\npath = \"{storage_path}\"\n\n"
        f"[{table}.claude-code]\nenabled = false\npath = \"{home}/empty-claude\"\n\n"
        f"{creds_block}\n"
    )
    env = dict(os.environ)
    env.update(
        HOME=str(home),
        XDG_CONFIG_HOME=str(home / "config"),
        XDG_DATA_HOME=str(home / "data"),
        XDG_STATE_HOME=str(home / "state"),
        NO_COLOR="1",
    )
    return env


def extract_creds_block(real_config: Path) -> str:
    """Pull the [creds.*] block(s) verbatim so the sandbox can authenticate to
    the same bucket. Secrets stay on disk in the sandbox only."""
    text = real_config.read_text()
    out, keep = [], False
    for line in text.splitlines():
        if line.strip().startswith("[creds"):
            keep = True
        elif line.strip().startswith("[") and not line.strip().startswith("[creds"):
            keep = False
        if keep:
            out.append(line)
    return "\n".join(out) if out else "[creds.default]\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--url", required=True, help="real corpus URL (read-only)")
    ap.add_argument("--config", required=True, help="real config with creds")
    ap.add_argument("--scratch-prefix", required=True, help="bucket prefix base for scratch stores")
    ap.add_argument("--skip-mutating", action="store_true")
    ap.add_argument("--skip-sync", action="store_true",
                    help="skip the from-scratch sync seeding; still run copy + optimize "
                         "(optimize then targets the copy-dest the copy test populates)")
    args = ap.parse_args()

    B = str(Path(args.bin).resolve())
    URL = args.url
    CFG = str(Path(os.path.expanduser(args.config)).resolve())
    SYNC = f"{args.scratch_prefix}-sync"
    COPY_DEST = f"{args.scratch_prefix}-copy-dest"
    # A prefix nothing ever writes to, so the verify-empty check stays exit-6 even
    # across re-runs (using COPY_DEST would flip to SYNCED once the copy test fills it).
    EMPTY = f"{args.scratch_prefix}-verify-empty"
    read = ["--storage-path", URL, "--config-file", CFG]
    workdir = Path(tempfile.mkdtemp(prefix="pond-e2e-"))
    print(f"sandbox root: {workdir}\nbinary: {B}\n")
    # Run from inside the sandbox so any relative path a command resolves (e.g. a
    # bare-string --storage-path that parses as a local path) materializes here,
    # never in the repo root. Tear the sandbox down on exit; the S3 scratch
    # prefixes are kept by design.
    origin = Path.cwd()
    os.chdir(workdir)

    def _teardown():
        os.chdir(origin)
        shutil.rmtree(workdir, ignore_errors=True)

    atexit.register(_teardown)

    # --- read-only on the real corpus (timed cold then warm) -------------------
    print("== reads on the real corpus ==")
    run("status", [B, "status", *read], "real", anchor="storage")
    run("status (warm)", [B, "status", *read], "real", anchor="storage")
    run("status -v", [B, "status", "-v", *read], "real", anchor="searchable")
    run("config show", [B, "config", "show", *read], "real")
    run("config path", [B, "config", "path", "--config-file", CFG], "real")
    run("config schema", [B, "config", "schema"], "n/a", anchor="storage")
    run("completions zsh", [B, "completions", "zsh"], "n/a", anchor="_pond")
    run("storage check (real)", [B, "storage", "check", URL, "--config-file", CFG], "real")
    run("creds list", [B, "creds", "list", "--config-file", CFG], "real")
    # `schedule status` exits 1 when nothing is registered (documented: schedule.rs
    # "Exit 0 when active, 1 when not configured"). Run it in a fresh sandbox HOME
    # so the launchd state is deterministically unconfigured.
    sched_env = seed_sandbox(workdir / "sched", Path(CFG), str(workdir / "store-sched"))
    run("schedule status (unconfigured)", [B, "schedule", "status"], "sandbox",
        env=sched_env, expect=1)
    run("adapters list", [B, "adapters", "list", "--config-file", CFG], "real")

    # search: cold (pays index prewarm) then warm, both modes
    run("search vector (cold)", [B, "search", "memory leak", *read], "real", anchor=None, timeout=900)
    run("search vector (warm)", [B, "search", "auth refactor", *read], "real", timeout=600)
    run("search fts", [B, "search", "--mode", "fts", "timeout", *read], "real", timeout=600)

    # discover a real session/message id via sql, then time get
    sid = _first_value(run("sql sample session", [B, "sql",
        "SELECT session_id FROM messages LIMIT 1", "--format", "ndjson", *read], "real"))
    mid = _first_value(run("sql sample message", [B, "sql",
        "SELECT message_id FROM messages LIMIT 1", "--format", "ndjson", *read], "real"))
    run("sql count", [B, "sql", "SELECT count(*) FROM messages", *read], "real")
    run("sql group-by", [B, "sql",
        "SELECT role, count(*) FROM messages GROUP BY role", *read], "real")
    if sid:
        run("get-session", [B, "get-session", sid, *read], "real", timeout=600)
    if mid:
        run("get-message", [B, "get-message", mid, *read], "real", timeout=600)

    # mcp initialize handshake over stdio
    init = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "e2e", "version": "0"}}}) + "\n"
    run("mcp initialize", [B, "mcp", *read], "real", anchor="pond",
        stdin=init.encode(), timeout=120)

    # global-flag parse order: before vs after the subcommand, byte-identical
    a = run("flags before subcmd", [B, "--storage-path", URL, "--config-file", CFG, "status"], "real")
    b = run("flags after subcmd", [B, "status", "--storage-path", URL, "--config-file", CFG], "real")
    record("flag-order identical", "real", bool(a) and a == b, 0, 0.0,
           "" if a == b else "stdout differs")

    # failure exit codes (read side)
    # A bare string is a valid local path (exit 0); a real parse error needs an
    # actually-malformed URL - here a local URL carrying query params, which the
    # storage parser rejects.
    run("bad url -> exit 2", [B, "status", "--storage-path", "file:///tmp/x?a=b"], "n/a", expect=2)
    run("missing id -> exit 1", [B, "get-session", "nope", *read], "real", expect=1)
    run("verify empty -> exit 6",
        [B, "copy", "--verify-only", "--from", URL, "--to", EMPTY, "--config-file", CFG],
        "scratch", expect=6, timeout=1800)

    # --- config-mutating in sandbox HOME/XDG ----------------------------------
    print("\n== config-mutating (sandbox) ==")
    s1 = seed_sandbox(workdir / "init", Path(CFG), str(workdir / "store-init"))
    run("init --yes", [B, "init", "--yes"], "sandbox", env=s1, anchor="config")
    s2 = seed_sandbox(workdir / "legacy", Path(CFG), str(workdir / "store-legacy"), legacy=True)
    run("init legacy-migrate", [B, "init", "--yes"], "sandbox", env=s2)
    legacy_cfg = (workdir / "legacy/config/pond/config.toml").read_text()
    record("legacy [sources]->[adapters]", "sandbox",
           "[adapters." in legacy_cfg and "[sources." not in legacy_cfg, 0, 0.0)
    run("adapters enable", [B, "adapters", "enable", "claude-code"], "sandbox", env=s2)
    run("adapters disable", [B, "adapters", "disable", "claude-code"], "sandbox", env=s2)
    run("storage use scratch", [B, "storage", "use", SYNC], "sandbox", env=s2, timeout=300)
    run("storage use local", [B, "storage", "use", "local"], "sandbox", env=s2)
    run("creds delete", [B, "creds", "delete", "default"], "sandbox", env=s2)

    # --- interactive wizards (PTY) --------------------------------------------
    print("\n== wizards (pty) ==")
    w1 = seed_sandbox(workdir / "wiz-init", Path(CFG), str(workdir / "store-wiz"))
    # Anchor on the first interactive PROMPT, not the intro banner: cliclack only
    # consumes Ctrl-C once it is in its raw-mode read loop, so a Ctrl-C sent at
    # the intro is dropped and the wizard hangs.
    drive_wizard("init wizard (cancel)", [B, "init"],
                 [("store its data", b"\x03")], w1, expect=1)
    w2 = seed_sandbox(workdir / "wiz-creds", Path(CFG), str(workdir / "store-creds2"))
    # The sandbox seeds a scope-less [creds.default]; the wizard never prompts for
    # a scope, so adding a second catch-all set is rejected ("at most one catch-all
    # set is allowed"). The valid wizard path is to replace the default: accept the
    # default name, confirm the replace.
    drive_wizard("creds add wizard (replace default)", [B, "creds", "add"],
                 [("set name", b"\r"), ("Replace existing", b"y\r"),
                  ("Access key", b"AKIAEXAMPLE\r"), ("Secret access key", b"secret123\r")],
                 w2, deadline=60)
    w3 = seed_sandbox(workdir / "wiz-disc", Path(CFG), str(workdir / "store-disc"))
    # An empty sandbox detects no adapters and bails (exit 1) on its own; there is
    # no prompt to drive, so feed nothing and assert the bail.
    drive_wizard("adapters discover (empty)", [B, "adapters", "discover"],
                 [], w3, expect=1, deadline=40)

    # --- store-mutating timing (scratch) --------------------------------------
    if not args.skip_mutating:
        print("\n== store-mutating timing (scratch) ==")
        if not args.skip_sync:
            run("sync appends", [B, "sync", "--storage-path", SYNC, "--config-file", CFG],
                "scratch:sync", timeout=3600)
        run("copy prefix->prefix",
            [B, "copy", "--from", URL, "--to", COPY_DEST, "--config-file", CFG],
            "scratch:copy-dest", anchor=None, timeout=7200)
        run("copy verify (synced)",
            [B, "copy", "--verify-only", "--from", URL, "--to", COPY_DEST, "--config-file", CFG],
            "scratch:copy-dest", timeout=1800)
        # With --skip-sync the -sync store is never seeded, so optimize runs
        # against the copy-dest the copy test just populated (real data, not a
        # trivial no-op against an empty store).
        opt_target = COPY_DEST if args.skip_sync else SYNC
        run("optimize", [B, "optimize", "--storage-path", opt_target, "--config-file", CFG],
            "scratch:copy-dest" if args.skip_sync else "scratch:sync", timeout=3600)

    print_matrix()
    return 0 if all(r.ok for r in RESULTS) else 1


def _first_value(ndjson_text: str | None):
    if not ndjson_text:
        return None
    for line in ndjson_text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                obj = json.loads(line)
                return next(iter(obj.values()))
            except (json.JSONDecodeError, StopIteration):
                continue
    return None


def print_matrix() -> None:
    print("\n=== RESULT MATRIX ===")
    print(f"{'op':<34} {'target':<16} {'time':>9}  {'exit':>4}  result")
    for r in RESULTS:
        print(f"{r.name:<34} {r.target:<16} {r.seconds:8.2f}s  {r.exit_code:>4}  "
              f"{'PASS' if r.ok else 'FAIL'}")
    n_fail = sum(1 for r in RESULTS if not r.ok)
    print(f"\n{len(RESULTS)} checks, {n_fail} failed")


if __name__ == "__main__":
    sys.exit(main())
