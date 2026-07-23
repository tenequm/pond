"""A dependency-free MCP client for pond, in two dial modes.

Pond speaks JSON-RPC 2.0 over the standard MCP transports: newline-delimited
JSON on stdio (managed mode, a child `pond serve --transport stdio`) and
streamable HTTP (url mode, an external `pond serve`). The client is pure
stdlib so enabling the hermes plugin needs no `pip install` - the whole
onboarding cost is installing the `pond` binary itself.

Transports expose a request/response seam (`roundtrip` + `notify`) rather than
a raw byte stream, because that is the shape both stdio and HTTP share and the
shape tests drive against a fake pond endpoint.
"""

from __future__ import annotations

import json
import os
import select
import signal
import subprocess
import threading
import urllib.request
from typing import Protocol


class McpError(Exception):
    """A JSON-RPC error envelope or a transport failure."""


PROTOCOL_VERSION = "2025-06-18"
CLIENT_INFO = {"name": "hermes-pond", "version": "0.1.0"}


def extract_text(content: object) -> str:
    """Join the text parts of an MCP tool result's content array.

    Pond renders agent-facing transcript text (not structured hits), so a tool
    result is one or more ``{"type": "text", "text": ...}`` parts.
    """
    if not isinstance(content, list):
        return ""
    out: list[str] = []
    for part in content:
        if (
            isinstance(part, dict)
            and part.get("type") == "text"
            and isinstance(part.get("text"), str)
        ):
            out.append(part["text"])
    return "\n".join(out)


class Transport(Protocol):
    def roundtrip(self, message: dict, timeout: float) -> dict: ...
    def notify(self, message: dict) -> None: ...
    def alive(self) -> bool: ...
    def close(self) -> None: ...


class StdioTransport:
    """A child process spoken to over newline-delimited JSON-RPC on stdio.

    Reads raw bytes off the child's stdout fd with a ``select`` deadline so a
    hung child fails the call instead of blocking the gateway. stderr is routed
    to a log file (pond writes its readiness banner and warnings there; stdout
    is reserved for JSON-RPC).
    """

    def __init__(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        log_path: str | None = None,
    ):
        self._log_fh = open(log_path, "ab", buffering=0) if log_path else subprocess.DEVNULL
        self._proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self._log_fh,
            env=env,
            bufsize=0,
        )
        self._buf = b""
        self._lock = threading.Lock()

    def _read_line(self, deadline: float) -> bytes:
        import time

        assert self._proc.stdout is not None
        fd = self._proc.stdout.fileno()
        while b"\n" not in self._buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise McpError("timed out waiting for pond response")
            ready, _, _ = select.select([fd], [], [], remaining)
            if not ready:
                continue
            chunk = os.read(fd, 65536)
            if not chunk:
                raise McpError("pond stdio closed (child exited)")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\n", 1)
        return line

    def _write(self, message: dict) -> None:
        assert self._proc.stdin is not None
        data = (json.dumps(message) + "\n").encode("utf-8")
        try:
            self._proc.stdin.write(data)
            self._proc.stdin.flush()
        except (BrokenPipeError, ValueError) as exc:
            raise McpError(f"pond stdin unavailable: {exc}") from exc

    def roundtrip(self, message: dict, timeout: float) -> dict:
        import time

        deadline = time.monotonic() + timeout
        want = message.get("id")
        with self._lock:
            self._write(message)
            while True:
                line = self._read_line(deadline).strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict) and parsed.get("id") == want:
                    return parsed
                # A notification or an out-of-band frame: skip and keep reading.

    def notify(self, message: dict) -> None:
        with self._lock:
            self._write(message)

    def alive(self) -> bool:
        return self._proc.poll() is None

    def close(self) -> None:
        proc = self._proc
        if proc.poll() is None:
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except Exception:
                pass
            try:
                proc.terminate()  # SIGTERM
            except ProcessLookupError:
                pass
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    proc.send_signal(signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for stream in (proc.stdin, proc.stdout):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass
        if self._log_fh is not subprocess.DEVNULL:
            try:
                self._log_fh.close()
            except Exception:
                pass


class HttpTransport:
    """Streamable-HTTP transport over an external `pond serve` (url mode).

    No supervision - the operator owns the endpoint and any auth (passed as
    headers). Handles both response shapes the MCP streamable-HTTP spec allows:
    a single ``application/json`` body or a ``text/event-stream`` (SSE) body of
    JSON-RPC frames. The server's ``Mcp-Session-Id`` (returned on initialize) is
    echoed on later requests.
    """

    def __init__(self, url: str, headers: dict[str, str] | None = None):
        self._url = url
        self._headers = dict(headers or {})
        self._session_id: str | None = None

    def _post(self, message: dict, timeout: float) -> tuple[dict | None, dict]:
        body = json.dumps(message).encode("utf-8")
        req = urllib.request.Request(self._url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json, text/event-stream")
        for key, value in self._headers.items():
            req.add_header(key, value)
        if self._session_id:
            req.add_header("Mcp-Session-Id", self._session_id)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            sid = resp.headers.get("Mcp-Session-Id")
            if sid:
                self._session_id = sid
            content_type = (resp.headers.get("Content-Type") or "").lower()
            raw = resp.read()
        parsed = parse_http_body(raw, content_type, want=message.get("id"))
        return parsed, {"content_type": content_type}

    def roundtrip(self, message: dict, timeout: float) -> dict:
        try:
            parsed, _ = self._post(message, timeout)
        except Exception as exc:
            raise McpError(f"pond http request failed: {exc}") from exc
        if parsed is None:
            raise McpError("pond http response carried no matching JSON-RPC frame")
        return parsed

    def notify(self, message: dict) -> None:
        try:
            self._post(message, timeout=10.0)
        except Exception:
            # Notifications (e.g. initialized) are fire-and-forget; a 202 with
            # an empty body is normal and parse_http_body returns None for it.
            pass

    def alive(self) -> bool:
        return True

    def close(self) -> None:
        return None


def parse_http_body(raw: bytes, content_type: str, want: object) -> dict | None:
    """Extract the JSON-RPC response matching ``want`` from an HTTP body.

    ``application/json`` -> parse directly. ``text/event-stream`` -> collect the
    ``data:`` payloads, JSON-parse each, and return the one whose id matches.
    """
    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return None
    if "text/event-stream" in content_type:
        for payload in _sse_data_frames(text):
            try:
                frame = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if isinstance(frame, dict) and frame.get("id") == want:
                return frame
        return None
    try:
        frame = json.loads(text)
    except json.JSONDecodeError:
        return None
    return frame if isinstance(frame, dict) else None


def _sse_data_frames(text: str) -> list[str]:
    frames: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if line.startswith("data:"):
            current.append(line[len("data:") :].lstrip())
        elif line == "":
            if current:
                frames.append("\n".join(current))
                current = []
    if current:
        frames.append("\n".join(current))
    return frames


class PondMcpClient:
    """MCP handshake + tool calls over an injected transport."""

    def __init__(self, transport: Transport):
        self._t = transport
        self._id = 0

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _request(self, method: str, params: dict | None, timeout: float) -> dict:
        message = {"jsonrpc": "2.0", "id": self._next_id(), "method": method}
        if params is not None:
            message["params"] = params
        resp = self._t.roundtrip(message, timeout)
        if isinstance(resp.get("error"), dict):
            raise McpError(str(resp["error"].get("message") or resp["error"]))
        return resp.get("result") or {}

    def initialize(self, timeout: float) -> None:
        self._request(
            "initialize",
            {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": CLIENT_INFO,
            },
            timeout,
        )
        self._t.notify({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def list_tool_names(self, timeout: float) -> list[str]:
        result = self._request("tools/list", {}, timeout)
        tools = result.get("tools") or []
        return [t.get("name", "") for t in tools if isinstance(t, dict)]

    def call_tool(self, name: str, args: dict, timeout: float) -> tuple[bool, str]:
        """Return ``(ok, text)``. ``ok=False`` carries pond's error text."""
        result = self._request("tools/call", {"name": name, "arguments": args}, timeout)
        text = extract_text(result.get("content"))
        if result.get("isError") is True:
            return False, text or f"pond tool {name} reported an error"
        return True, text

    def alive(self) -> bool:
        return self._t.alive()

    def close(self) -> None:
        self._t.close()
