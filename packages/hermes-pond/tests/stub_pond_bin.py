#!/usr/bin/env python3
"""A stand-in for `pond serve --transport stdio`, for the service lifecycle test.

Speaks newline-delimited JSON-RPC on stdio (the MCP stdio framing pond uses via
rmcp). Ignores all CLI args (serve/--transport/--with-sync/--bootstrap ...) and
just answers initialize / tools/list / tools/call. Records tool calls to the
file named by HERMES_POND_STUB_LOG when set.
"""

import json
import os
import sys

TOOL_NAMES = ["pond_search", "pond_get_session", "pond_get_message", "pond_sql"]


def _write(msg):
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def _record(entry):
    path = os.environ.get("HERMES_POND_STUB_LOG")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def main():
    sys.stderr.write("stub-pond: stdio MCP ready\n")
    sys.stderr.flush()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = req.get("method")
        rid = req.get("id")
        if method == "initialize":
            _write(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "result": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "stub-pond", "version": "0.0.0"},
                    },
                }
            )
        elif method == "notifications/initialized":
            continue
        elif method == "tools/list":
            _write(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "result": {
                        "tools": [
                            {"name": n, "inputSchema": {"type": "object"}} for n in TOOL_NAMES
                        ]
                    },
                }
            )
        elif method == "tools/call":
            params = req.get("params") or {}
            name = params.get("name")
            _record({"name": name, "arguments": params.get("arguments")})
            _write(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "result": {
                        "content": [{"type": "text", "text": f"stub:{name}"}],
                        "isError": False,
                    },
                }
            )
        elif rid is not None:
            _write(
                {
                    "jsonrpc": "2.0",
                    "id": rid,
                    "error": {"code": -32601, "message": f"unknown method {method}"},
                }
            )


if __name__ == "__main__":
    main()
