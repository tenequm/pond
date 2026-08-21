"""Golden request/response for the four tools against a recording pond caller."""

from __future__ import annotations

import json

from hermes_pond.tools import (
    RESPONSE_MAX_BYTES,
    SEARCH_MAX_LIMIT,
    SEARCH_SCHEMA,
    make_handlers,
)


class RecordingCaller:
    def __init__(self, responses=None):
        self.calls = []
        self._responses = responses or {}

    def __call__(self, name, args):
        self.calls.append((name, args))
        resp = self._responses.get(name)
        if callable(resp):
            return resp(args)
        if resp is not None:
            return resp
        return True, f"ok:{name}"


def _decode(raw):
    return json.loads(raw)


def test_search_forwards_query_limit_and_source_agent():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_search"]

    out = _decode(handle({"query": "  auth flow  ", "mode": "fts"}))

    assert out == {"status": "ok", "text": "ok:pond_search"}
    name, args = caller.calls[0]
    assert name == "pond_search"
    assert args["query"] == "auth flow"
    assert args["limit"] == 10  # default
    assert args["mode"] == "fts"
    assert args["source_agent"] == "hermes"


def test_search_caps_limit_and_empty_query_errors():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_search"]

    handle({"query": "x", "limit": 999})
    assert caller.calls[-1][1]["limit"] == SEARCH_MAX_LIMIT

    err = _decode(handle({"query": "   "}))
    assert err["status"] == "error"
    # empty query must not reach pond
    assert len(caller.calls) == 1


def test_star_source_omits_source_agent_filter():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["*"])
    _schema, handle = handlers["pond_search"]
    handle({"query": "x"})
    assert "source_agent" not in caller.calls[0][1]


def test_get_session_forwards_paging_fields():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_get_session"]
    handle({"id": "sess-1", "from": "end", "limit": 50, "after_message_id": "m5"})
    args = caller.calls[0][1]
    assert args == {"id": "sess-1", "from": "end", "limit": 50, "after_message_id": "m5"}


def test_get_message_forwards_context_window():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_get_message"]
    handle({"id": "sess-1:42", "context_before": 3, "context_after": 2})
    assert caller.calls[0][1] == {"id": "sess-1:42", "context_before": 3, "context_after": 2}


def test_sql_forwards_format_and_timeout():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_sql"]
    handle({"query": "SELECT 1", "format": "ndjson", "timeout_seconds": 30})
    assert caller.calls[0][1] == {
        "query": "SELECT 1",
        "format": "ndjson",
        "timeout_seconds": 30,
    }


def test_error_relayed_as_typed_envelope():
    caller = RecordingCaller(responses={"pond_search": (False, "not_found: nope")})
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_search"]
    out = _decode(handle({"query": "x"}))
    assert out == {"status": "error", "error": "not_found: nope"}


def test_response_is_byte_bounded():
    big = "A" * (RESPONSE_MAX_BYTES * 2)
    caller = RecordingCaller(responses={"pond_search": (True, big)})
    handlers = make_handlers(caller, sources=["hermes"])
    _schema, handle = handlers["pond_search"]
    out = _decode(handle({"query": "x"}))
    assert out["status"] == "ok"
    assert "truncated" in out["text"]
    assert len(out["text"].encode("utf-8")) <= RESPONSE_MAX_BYTES + 200


def test_schemas_are_well_formed():
    caller = RecordingCaller()
    handlers = make_handlers(caller, sources=["hermes"])
    for name, (schema, _handle) in handlers.items():
        assert schema["name"] == name
        params = schema["parameters"]
        assert params["type"] == "object"
        assert params["additionalProperties"] is False
        assert schema["description"]


def test_mode_description_is_version_neutral():
    # The description must read correctly against ANY pond binary: the default
    # arm is the running instance's business, so naming one here would go stale
    # on the first upgrade that flips it.
    description = SEARCH_SCHEMA["description"]
    assert '"fts"' in description
    assert '"vector"' in description
    assert "default," not in description
