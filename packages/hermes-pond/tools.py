"""The four projected pond tools, as hermes native tools.

Each forwards to pond over MCP and returns a JSON string (the hermes tool
handler contract). pond renders its own agent-facing transcript text, so the
handler relays that text byte-bounded inside a typed ok/error envelope. Read-only
holds by construction: pond's MCP surface never exposes a write path.

Response budgets are copied verbatim from packages/openclaw-pond/src/schemas.ts
rather than reinvented. Tool descriptions carry the routing: pond is the durable
CROSS-HARNESS archive of past sessions (survives resets and disk-budget pruning),
distinct from hermes' live same-harness `session_search`. Kept short on purpose
(names + short descriptions route; detail lives in pond's own resources).
"""

from __future__ import annotations

import json
from collections.abc import Callable

SEARCH_DEFAULT_LIMIT = 10
SEARCH_MAX_LIMIT = 25
SEARCH_MAX_QUERY_CHARS = 4096
SQL_MAX_QUERY_CHARS = 8192
RESPONSE_MAX_BYTES = 32 * 1024
SQL_MAX_TIMEOUT_SECONDS = 600

TOOLSET = "pond"

PondCaller = Callable[[str, dict], tuple[bool, str]]


def _json(obj: object) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _ok(text: str) -> str:
    return _json({"status": "ok", "text": text})


def _error(message: str) -> str:
    return _json({"status": "error", "error": message})


def _bounded(text: str) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= RESPONSE_MAX_BYTES:
        return text
    clipped = encoded[:RESPONSE_MAX_BYTES].decode("utf-8", errors="ignore")
    return (
        f"{clipped}\n\n[pond: response truncated to {RESPONSE_MAX_BYTES} bytes; "
        "narrow the query or lower limit]"
    )


SEARCH_SCHEMA = {
    "name": "pond_search",
    "description": (
        "Find relevant messages in your durable pond corpus of PAST agent sessions "
        "(cross-harness: Claude Code, OpenClaw, hermes, and every other ingested "
        "source; survives resets and disk-budget pruning). Not the live conversation "
        "- for the current session use session_search. Returns pond's rendered "
        'transcript grouped by session, best first. mode: "vector" (default, '
        'meaning) or "fts" (exact words). Pass a hit\'s session_id to '
        "pond_get_session or its message_id to pond_get_message."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "maxLength": SEARCH_MAX_QUERY_CHARS},
            "mode": {"type": "string", "enum": ["vector", "fts"]},
            "sort_by": {"type": "string", "enum": ["relevance", "recency"]},
            "limit": {"type": "integer", "minimum": 1, "maximum": SEARCH_MAX_LIMIT},
            "project": {"type": "string", "maxLength": 512},
            "session_id": {"type": "string", "maxLength": 512},
            "from_date": {"type": "string", "maxLength": 32},
            "to_date": {"type": "string", "maxLength": 32},
        },
        "required": ["query"],
        "additionalProperties": False,
    },
}

GET_SESSION_SCHEMA = {
    "name": "pond_get_session",
    "description": (
        "Read a whole past session from pond as a chronological transcript - the tool "
        "for analyzing, reviewing, or summarizing a past session. Pass an id from "
        "pond_search (a message_id also works: it resolves to its parent session "
        'anchored at that message). from="end" reads the most recent turns; '
        "after_message_id / before_message_id page from a page marker."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "maxLength": 512},
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            "from": {"type": "string", "enum": ["start", "end"]},
            "after_message_id": {"type": "string", "maxLength": 512},
            "before_message_id": {"type": "string", "maxLength": 512},
        },
        "required": ["id"],
        "additionalProperties": False,
    },
}

GET_MESSAGE_SCHEMA = {
    "name": "pond_get_message",
    "description": (
        "Expand one pond message with its full part bodies (tool_call / tool_result / "
        "reasoning) plus conversational neighbors; context_before / context_after size "
        "the window (like grep -B/-A). Pass a message_id from pond_search; for the "
        "whole session use pond_get_session."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "maxLength": 512},
            "context_before": {"type": "integer", "minimum": 0, "maximum": 25},
            "context_after": {"type": "integer", "minimum": 0, "maximum": 25},
        },
        "required": ["id"],
        "additionalProperties": False,
    },
}

SQL_SCHEMA = {
    "name": "pond_sql",
    "description": (
        "Advanced escape hatch: run ONE read-only SQL SELECT over pond's corpus as "
        "three tables (sessions, messages, parts) for analytics pond_search and the "
        "get tools cannot express - counts, group-by, joins, exact strings inside tool "
        "bodies, bulk export (parquet/ndjson). Read the schema://pond-sql resource "
        "before writing SQL."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "maxLength": SQL_MAX_QUERY_CHARS},
            "format": {"type": "string", "enum": ["text", "parquet", "ndjson"]},
            "timeout_seconds": {
                "type": "integer",
                "minimum": 1,
                "maximum": SQL_MAX_TIMEOUT_SECONDS,
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    },
}


def _resolve_source_agent(sources: list[str]) -> str | None:
    """pond's source_agent filter takes one source. "*" opts into the whole corpus."""
    if not sources or "*" in sources:
        return None
    return sources[0]


def make_handlers(call_pond: PondCaller, sources: list[str]):
    """Build the four tool handlers bound to a pond caller and the source filter.

    Handlers take ``(args: dict, **kwargs)`` and return a JSON string, per the
    hermes registry contract.
    """
    source_agent = _resolve_source_agent(sources)

    def _relay(name: str, pond_args: dict) -> str:
        ok, text = call_pond(name, pond_args)
        if not ok:
            return _error(text)
        return _ok(_bounded(text))

    def handle_search(args: dict, **_kw) -> str:
        query = str(args.get("query") or "").strip()
        if not query:
            return _error("query must not be empty")
        limit = args.get("limit")
        limit = SEARCH_DEFAULT_LIMIT if not isinstance(limit, int) else limit
        limit = min(SEARCH_MAX_LIMIT, max(1, limit))
        pond_args: dict = {"query": query, "limit": limit}
        if source_agent:
            pond_args["source_agent"] = source_agent
        for key in ("mode", "sort_by", "project", "session_id", "from_date", "to_date"):
            value = args.get(key)
            if isinstance(value, str) and value:
                pond_args[key] = value
        return _relay("pond_search", pond_args)

    def handle_get_session(args: dict, **_kw) -> str:
        ident = str(args.get("id") or "").strip()
        if not ident:
            return _error("id must not be empty")
        pond_args: dict = {"id": ident}
        if isinstance(args.get("limit"), int):
            pond_args["limit"] = args["limit"]
        if isinstance(args.get("from"), str) and args["from"]:
            pond_args["from"] = args["from"]
        for key in ("after_message_id", "before_message_id"):
            value = args.get(key)
            if isinstance(value, str) and value:
                pond_args[key] = value
        return _relay("pond_get_session", pond_args)

    def handle_get_message(args: dict, **_kw) -> str:
        ident = str(args.get("id") or "").strip()
        if not ident:
            return _error("id must not be empty")
        pond_args: dict = {"id": ident}
        for key in ("context_before", "context_after"):
            value = args.get(key)
            if isinstance(value, int):
                pond_args[key] = value
        return _relay("pond_get_message", pond_args)

    def handle_sql(args: dict, **_kw) -> str:
        query = str(args.get("query") or "").strip()
        if not query:
            return _error("query must not be empty")
        pond_args: dict = {"query": query}
        if isinstance(args.get("format"), str) and args["format"]:
            pond_args["format"] = args["format"]
        if isinstance(args.get("timeout_seconds"), int):
            pond_args["timeout_seconds"] = args["timeout_seconds"]
        return _relay("pond_sql", pond_args)

    return {
        "pond_search": (SEARCH_SCHEMA, handle_search),
        "pond_get_session": (GET_SESSION_SCHEMA, handle_get_session),
        "pond_get_message": (GET_MESSAGE_SCHEMA, handle_get_message),
        "pond_sql": (SQL_SCHEMA, handle_sql),
    }
