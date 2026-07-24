"""hermes-pond: read-only pond recall tools for the Hermes agent.

Registers pond_search / pond_get_session / pond_get_message / pond_sql under the
`pond` toolset and the `hermes pond` CLI command. Managed mode supervises a
local `pond serve`; url mode attaches to an external one. Tools-only, read-only,
by construction - no memory-provider slot, no ambient recall, no session_search
override. pond coexists with hermes' live `session_search`: pond is the durable
cross-harness archive of PAST sessions; session_search is live same-harness.

The runtime has zero third-party Python dependencies (pure-stdlib MCP client),
so enabling the plugin needs no pip install.

Sibling modules are imported lazily inside the functions below (never at module
top level) so this file can be imported standalone - relative imports resolve
only under a real package parent (hermes' loader, or the test conftest).
"""

from __future__ import annotations

import atexit
import logging

logger = logging.getLogger(__name__)

# One controller per process: tool handlers share the lazily-started pond
# connection, and it survives across tool calls within the gateway process.
_controller = None


def _get_controller():
    global _controller
    if _controller is None:
        from .config import load_plugin_config
        from .service import PondController

        _controller = PondController(load_plugin_config(), logger=logger)
        atexit.register(_controller.stop)
    return _controller


def register(ctx) -> None:
    """Register the tools and the CLI command.

    Called once by the hermes plugin loader when `pond` is enabled in
    config.yaml (plugins.enabled).
    """
    from .config import load_plugin_config
    from .tools import TOOLSET, make_handlers

    config = load_plugin_config()
    controller = _get_controller()
    handlers = make_handlers(
        call_pond=lambda name, args: controller.call_tool(name, args),
        sources=config.sources,
    )

    for name, (schema, handler) in handlers.items():
        ctx.register_tool(
            name=name,
            toolset=TOOLSET,
            schema=schema,
            handler=handler,
            description=schema.get("description", ""),
        )

    from . import setup_cmd

    ctx.register_cli_command(
        name="pond",
        help="Manage pond recall (setup, status)",
        setup_fn=setup_cmd.register_cli,
        handler_fn=setup_cmd.pond_command,
        description="Locate pond and expose its read-only recall tools on hermes surfaces.",
    )
