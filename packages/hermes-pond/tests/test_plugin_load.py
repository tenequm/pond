"""Plugin registration: register(ctx) wires the tools and the CLI command.

The FakePluginContext mirrors hermes_cli.plugins.PluginContext's register_tool /
register_cli_command signatures so a passing test here means the real loader
would wire the same surface.
"""

from __future__ import annotations

import argparse

import hermes_pond


class FakePluginContext:
    def __init__(self):
        self.tools = {}
        self.cli_commands = {}

    def register_tool(
        self,
        name,
        toolset,
        schema,
        handler,
        check_fn=None,
        requires_env=None,
        is_async=False,
        description="",
        emoji="",
        override=False,
    ):
        assert override is False, "pond must never override a built-in tool"
        self.tools[name] = {
            "toolset": toolset,
            "schema": schema,
            "handler": handler,
            "description": description,
        }

    def register_cli_command(self, name, help, setup_fn, handler_fn=None, description=""):
        self.cli_commands[name] = {
            "help": help,
            "setup_fn": setup_fn,
            "handler_fn": handler_fn,
        }


def test_register_wires_four_tools_one_toolset():
    ctx = FakePluginContext()
    hermes_pond.register(ctx)
    assert set(ctx.tools) == {
        "pond_search",
        "pond_get_session",
        "pond_get_message",
        "pond_sql",
    }
    assert {t["toolset"] for t in ctx.tools.values()} == {"pond"}
    for name, t in ctx.tools.items():
        assert t["schema"]["name"] == name
        assert callable(t["handler"])
        assert t["description"]


def test_register_wires_pond_cli_command():
    ctx = FakePluginContext()
    hermes_pond.register(ctx)
    assert "pond" in ctx.cli_commands
    cmd = ctx.cli_commands["pond"]
    parser = argparse.ArgumentParser(prog="hermes pond")
    cmd["setup_fn"](parser)
    ns = parser.parse_args(["setup"])
    assert getattr(ns, "pond_command") == "setup"


def test_tool_handlers_return_json_strings_without_pond(monkeypatch):
    # Force "no pond binary" so the handler cannot spawn a real pond serve
    # against the developer's live store; it must return a typed JSON error
    # string (never raise), satisfying the hermes handler contract.
    import hermes_pond.service as service

    def _no_binary(_path):
        raise service.McpError("pond binary not found on PATH.")

    monkeypatch.setattr(service, "resolve_pond_binary", _no_binary)

    ctx = FakePluginContext()
    hermes_pond.register(ctx)
    import json

    out = ctx.tools["pond_search"]["handler"]({"query": "hello"})
    parsed = json.loads(out)
    assert parsed["status"] in {"ok", "error"}
