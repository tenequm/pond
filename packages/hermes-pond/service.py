"""Managed-mode supervisor for the local pond process, plus the shared call seam.

Managed (default): owns a `pond serve --transport stdio --with-sync` child -
lazy start on first tool call, health-check via tools/list, reconnect with
exponential backoff after an unexpected exit, `nice` on POSIX so background sync
never competes with interactive work, a file log, and a SIGTERM teardown ladder.
On a completely unconfigured pond it starts with `--bootstrap hermes` so the
first sync ingests the user's existing hermes history; it never mutates an
existing pond config.

url: attaches to an external `pond serve` over streamable HTTP, no supervision.

Both modes go through one controller and expose a single ``call_tool`` seam the
tool handlers use. In-tree template for the pid/state file is
plugins/google_meet/process_manager.py; the in-process supervision loop mirrors
packages/openclaw-pond/src/service.ts.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import threading
import time
from pathlib import Path

from .config import PondPluginConfig
from .mcp_client import HttpTransport, McpError, PondMcpClient, StdioTransport

# Local-sidecar deadlines: the initialize handshake and each tool call. Short
# relative to the MCP default (60s) because this is a same-host child.
DIAL_TIMEOUT_S = 10.0
CALL_TIMEOUT_S = 120.0
# A pond_sql call carries its own server-side timeout (up to 600s); the transport
# deadline must outlive it, or a long query would trip McpError and tear down the
# warm child. Grant the requested budget plus room for transfer + rendering.
SQL_CALL_TIMEOUT_MARGIN_S = 30.0
RESTART_BASE_DELAY_S = 0.5
RESTART_MAX_DELAY_S = 30.0
# pond's own JSON-RPC application error codes (spec 7.4): validation_failed,
# not_found, and their siblings, i.e. -32010 through -32016.
POND_APP_ERROR_CODES = range(-32016, -32009)
VERSION_PROBE_TIMEOUT_S = 5.0

INSTALL_HINT = (
    "install pond, then restart hermes: `brew install tenequm/tap/pond`, "
    "`cargo install pond`, or download a binary from "
    "https://github.com/tenequm/pond/releases. The plugin bootstraps the hermes "
    "adapter itself - no `pond init` is needed."
)

# Well-known install locations checked after PATH.
_WELL_KNOWN = (
    "~/.cargo/bin/pond",
    "~/.local/bin/pond",
    "/opt/homebrew/bin/pond",
    "/usr/local/bin/pond",
)


def resolve_pond_binary(binary_path: str | None) -> str:
    """Locate the pond binary: explicit config, then PATH, then well-known dirs.

    Raises with the exact install command so a missing binary fails with a named
    fix, never an opaque spawn error deep in the transport.
    """
    if binary_path:
        expanded = os.path.expanduser(binary_path)
        if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
            return expanded
        raise McpError(
            f"configured pond binary_path is not executable: {binary_path}. {INSTALL_HINT}"
        )
    found = shutil.which("pond")
    if found:
        return found
    for candidate in _WELL_KNOWN:
        expanded = os.path.expanduser(candidate)
        if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
            return expanded
    raise McpError(f"pond binary not found on PATH. {INSTALL_HINT}")


def is_pond_app_error(exc: McpError) -> bool:
    """Did pond reject the REQUEST, rather than the connection failing?

    An app error is the caller's problem and the warm child is still healthy, so
    tearing it down would respawn `pond serve` on every such call. A transport
    fault carries neither a pond code nor a `retryable` marker and must drop.
    """
    code = exc.error.code
    if isinstance(code, int) and code in POND_APP_ERROR_CODES:
        return True
    data = exc.error.data
    return isinstance(data, dict) and data.get("retryable") is False


def _call_timeout(name: str, args: dict) -> float:
    """Transport deadline for one tool call. pond_sql carries its own server-side
    timeout, so the transport must wait at least that long plus a transfer margin;
    every other tool uses the flat sidecar deadline."""
    if name == "pond_sql":
        requested = args.get("timeout_seconds")
        if isinstance(requested, int) and requested > 0:
            return float(requested) + SQL_CALL_TIMEOUT_MARGIN_S
    return CALL_TIMEOUT_S


def _log_dir() -> Path:
    try:
        from hermes_constants import get_hermes_home

        base = Path(get_hermes_home())
    except Exception:
        base = Path(os.path.expanduser(os.environ.get("HERMES_HOME", "~/.hermes")))
    d = base / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


class PondController:
    """Lazy, self-healing MCP connection to pond, shared by all tool handlers."""

    def __init__(self, config: PondPluginConfig, logger=None):
        self._config = config
        self._log = logger
        self._client: PondMcpClient | None = None
        self._lock = threading.Lock()
        self._attempt = 0
        self._next_retry_at = 0.0
        self._stopped = False
        self._version_logged = False

    # -- logging -----------------------------------------------------------
    def _info(self, msg: str) -> None:
        if self._log:
            self._log.info(f"[pond] {msg}")

    def _warn(self, msg: str) -> None:
        if self._log:
            self._log.warning(f"[pond] {msg}")

    def _error(self, msg: str) -> None:
        if self._log:
            self._log.error(f"[pond] {msg}")

    # -- public seam -------------------------------------------------------
    def call_tool(self, name: str, args: dict) -> tuple[bool, str]:
        """Forward one tool call to pond, connecting lazily. ``(ok, text)``."""
        with self._lock:
            if self._stopped:
                return False, "pond service is stopped"
            client = self._ensure_connected()
            if client is None:
                return (
                    False,
                    "pond is not connected yet; the pond service is starting or "
                    "unavailable. Check the hermes logs.",
                )
            try:
                return client.call_tool(name, args, timeout=_call_timeout(name, args))
            except McpError as exc:
                if is_pond_app_error(exc):
                    return False, f"pond: {exc.error.message}"
                self._drop(f"tool call failed: {exc}")
                return False, f"pond call failed: {exc}"

    def stop(self) -> None:
        """Idempotent teardown - tears down the child / connection."""
        with self._lock:
            self._stopped = True
            self._drop("stop requested")

    # -- connection lifecycle ---------------------------------------------
    def _ensure_connected(self) -> PondMcpClient | None:
        if self._client is not None and self._client.alive():
            return self._client
        if self._client is not None and not self._client.alive():
            self._drop("child exited")
        now = time.monotonic()
        if now < self._next_retry_at:
            return None
        return self._dial()

    def _dial(self) -> PondMcpClient | None:
        try:
            transport = self._dial_http() if self._config.mode == "url" else self._dial_stdio()
        except (McpError, OSError) as exc:
            self._on_dial_failure(exc)
            return None
        client = PondMcpClient(transport)
        try:
            client.initialize(DIAL_TIMEOUT_S)
            # Probe so a dead transport fails here, not on the first tool call.
            client.list_tool_names(DIAL_TIMEOUT_S)
        except (McpError, OSError) as exc:
            # Handshake/probe failed after the transport (and, in stdio mode, the
            # child) was created - close it so a spawned `--with-sync` pond is not
            # orphaned to keep syncing while the retry loop spawns another.
            try:
                transport.close()
            except Exception:
                pass
            self._on_dial_failure(exc)
            return None
        self._attempt = 0
        self._client = client
        self._info(f"connected ({self._config.mode} mode).")
        return client

    def _on_dial_failure(self, exc: Exception) -> None:
        if self._config.mode == "url":
            self._error(f"connection to {self._config.url or '(no url)'} failed: {exc}")
        else:
            self._error(f"failed to start: {exc}")
        self._schedule_retry()

    def _log_version(self, pond_bin: str) -> None:
        """Record which pond this session is actually talking to, once. The tool
        text is version-neutral, so the log is the only place the binary's
        behaviour (default search arm, embeddings) can be traced back to.
        Fire-and-forget: the dial never waits on it and never fails because of
        it (matches the openclaw-pond / pi-pond probes)."""
        if self._version_logged:
            return
        self._version_logged = True
        threading.Thread(
            target=self._probe_version, args=(pond_bin,), daemon=True
        ).start()

    def _probe_version(self, pond_bin: str) -> None:
        try:
            probe = subprocess.run(
                [pond_bin, "--version"],
                capture_output=True,
                text=True,
                stdin=subprocess.DEVNULL,
                timeout=VERSION_PROBE_TIMEOUT_S,
            )
            self._info(f"{pond_bin}: {(probe.stdout or probe.stderr).strip()}")
        except Exception as exc:
            self._warn(f"could not read `{pond_bin} --version`: {exc}")

    def _dial_stdio(self) -> StdioTransport:
        pond_bin = resolve_pond_binary(self._config.binary_path)
        self._log_version(pond_bin)
        args = [
            "serve",
            "--transport",
            "stdio",
            "--with-sync",
            "--sync-every",
            str(self._config.sync_interval_minutes),
            # First-run only: enables the hermes adapter when pond has NO
            # adapters configured. Never touches an existing pond config.
            "--bootstrap",
            "hermes",
        ]
        # nice execs pond in place (same pid), so the teardown ladder still
        # signals the right process. POSIX only.
        if os.name == "posix":
            command = ["nice", "-n", "19", pond_bin, *args]
        else:
            command = [pond_bin, *args]
        log_path = str(_log_dir() / "pond-serve.log")
        return StdioTransport(command, env=os.environ.copy(), log_path=log_path)

    def _dial_http(self) -> HttpTransport:
        if not self._config.url:
            raise McpError("pond.mode=url requires pond.url to be set")
        return HttpTransport(self._config.url, self._config.headers)

    def _drop(self, reason: str) -> None:
        if self._client is not None:
            self._warn(f"connection closed ({reason}).")
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def _schedule_retry(self) -> None:
        self._attempt += 1
        delay = min(RESTART_MAX_DELAY_S, RESTART_BASE_DELAY_S * (2 ** (self._attempt - 1)))
        self._next_retry_at = time.monotonic() + delay
