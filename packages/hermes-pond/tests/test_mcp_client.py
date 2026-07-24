"""MCP client: handshake/call over a fake transport, SSE + JSON body parsing."""

from __future__ import annotations

import pytest
from hermes_pond.mcp_client import (
    McpError,
    PondMcpClient,
    extract_text,
    parse_http_body,
)


class FakeTransport:
    """Answers roundtrip() from a per-method responder; records notifications."""

    def __init__(self, responder):
        self._responder = responder
        self.notifications = []
        self.closed = False

    def roundtrip(self, message, timeout):
        result = self._responder(message)
        return {"jsonrpc": "2.0", "id": message.get("id"), **result}

    def notify(self, message):
        self.notifications.append(message)

    def alive(self):
        return not self.closed

    def close(self):
        self.closed = True


def test_initialize_sends_initialized_notification():
    t = FakeTransport(lambda msg: {"result": {"protocolVersion": "x", "capabilities": {}}})
    client = PondMcpClient(t)
    client.initialize(timeout=1.0)
    assert t.notifications == [{"jsonrpc": "2.0", "method": "notifications/initialized"}]


def test_call_tool_extracts_text():
    def responder(msg):
        if msg["method"] == "tools/call":
            return {"result": {"content": [{"type": "text", "text": "hello"}]}}
        return {"result": {}}

    client = PondMcpClient(FakeTransport(responder))
    ok, text = client.call_tool("pond_search", {"query": "x"}, timeout=1.0)
    assert ok is True
    assert text == "hello"


def test_call_tool_is_error_returns_ok_false():
    def responder(msg):
        return {"result": {"content": [{"type": "text", "text": "boom"}], "isError": True}}

    client = PondMcpClient(FakeTransport(responder))
    ok, text = client.call_tool("pond_sql", {"query": "x"}, timeout=1.0)
    assert ok is False
    assert text == "boom"


def test_jsonrpc_error_raises():
    def responder(msg):
        return {"error": {"code": -32602, "message": "validation_failed"}}

    client = PondMcpClient(FakeTransport(responder))
    with pytest.raises(McpError, match="validation_failed"):
        client.call_tool("pond_search", {}, timeout=1.0)


def test_extract_text_joins_text_parts_only():
    content = [
        {"type": "text", "text": "a"},
        {"type": "image", "data": "..."},
        {"type": "text", "text": "b"},
    ]
    assert extract_text(content) == "a\nb"


def test_parse_http_body_json():
    frame = parse_http_body(b'{"jsonrpc":"2.0","id":7,"result":{}}', "application/json", want=7)
    assert frame is not None and frame["id"] == 7


def test_parse_http_body_sse_selects_matching_id():
    body = (
        b"event: message\n"
        b'data: {"jsonrpc":"2.0","id":1,"result":{"a":1}}\n'
        b"\n"
        b"event: message\n"
        b'data: {"jsonrpc":"2.0","id":2,"result":{"b":2}}\n'
        b"\n"
    )
    frame = parse_http_body(body, "text/event-stream", want=2)
    assert frame is not None and frame["result"] == {"b": 2}


def test_parse_http_body_empty_returns_none():
    assert parse_http_body(b"", "application/json", want=1) is None
