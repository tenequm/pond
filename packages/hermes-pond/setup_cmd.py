"""`hermes pond setup` - the onboarding command.

Install must be: get the plugin -> enable -> `hermes pond setup` -> done. This
command does the two things a hermes user cannot do for themselves:

1. Locate the pond binary (PATH, then well-known dirs); if absent, fail with the
   exact install command - no silent download.
2. Write the per-platform tool allowlist so the pond tools actually appear on
   the headless surfaces. Gateway messaging platforms, the API server, and cron
   resolve their tools from ``platform_toolsets.<platform>`` in config.yaml
   (hermes_cli/tools_config.py::_get_platform_tools). A plugin toolset is
   default-on for a platform the operator has never configured, but default-OFF
   once that platform has a saved toolset list that omits it ("known but
   absent"). So setup adds ``pond`` explicitly to every already-configured
   platform plus api_server and cron, preserving each platform's native tools.

Pond config bootstrap is delegated to the managed child (`pond serve
--bootstrap hermes`), which enables the hermes adapter only when pond has no
adapters configured and never mutates an existing pond config.

All hermes imports are lazy so this module loads without a hermes install.
"""

from __future__ import annotations

import argparse

from .service import INSTALL_HINT, resolve_pond_binary
from .tools import TOOLSET

# api_server and cron are headless surfaces users reach without ever opening
# `hermes tools`, so always ensure the pond toolset is listed for them.
_FORCED_PLATFORMS = ("api_server", "cron")


def register_cli(subparser: argparse.ArgumentParser) -> None:
    """Build the ``hermes pond`` argparse tree (called at plugin load)."""
    subs = subparser.add_subparsers(dest="pond_command")
    setup_p = subs.add_parser("setup", help="Locate pond and expose its tools on chat surfaces")
    setup_p.add_argument(
        "--all-platforms",
        action="store_true",
        help="Add the pond toolset to every gateway platform, not only configured ones.",
    )
    subs.add_parser("status", help="Report pond binary + allowlist state")
    subparser.set_defaults(func=pond_command)


def pond_command(args: argparse.Namespace) -> int:
    sub = getattr(args, "pond_command", None)
    if sub == "setup":
        return _cmd_setup(all_platforms=bool(getattr(args, "all_platforms", False)))
    if sub == "status":
        return _cmd_status()
    print("usage: hermes pond {setup,status}")
    return 2


def _locate_binary(binary_path: str | None = None) -> str | None:
    try:
        return resolve_pond_binary(binary_path)
    except Exception as exc:
        print(f"pond binary: NOT FOUND\n  {exc}")
        return None


def _target_platforms(config: dict, all_platforms: bool) -> list[str]:
    from hermes_cli.platforms import PLATFORMS

    if all_platforms:
        targets = [p for p in PLATFORMS if p != "cli"]
    else:
        configured = [
            p for p in (config.get("platform_toolsets") or {}) if p != "cli" and p in PLATFORMS
        ]
        targets = sorted(set(configured) | set(_FORCED_PLATFORMS))
    return targets


def _ensure_pond_listed(config: dict, platform: str) -> bool:
    """Add ``pond`` to ``platform_toolsets.<platform>``, preserving native tools.

    Returns True when the config changed. A brand-new entry is seeded with the
    platform's default composite so its native tools are not dropped.
    """
    from hermes_cli.platforms import PLATFORMS

    table = config.setdefault("platform_toolsets", {})
    existing = table.get(platform)
    if isinstance(existing, list):
        entries = [str(e) for e in existing]
        if TOOLSET in entries:
            return False
        entries.append(TOOLSET)
        table[platform] = entries
        return True
    info = PLATFORMS.get(platform)
    default_ts = info.default_toolset if info else f"hermes-{platform}"
    table[platform] = [default_ts, TOOLSET]
    return True


def _cmd_setup(*, all_platforms: bool) -> int:
    from .config import load_plugin_config

    plugin = load_plugin_config()
    if plugin.mode == "url":
        # url mode attaches to an external `pond serve`; no local binary is needed
        # or spawned, so requiring one here would make setup impossible for it.
        if not plugin.url:
            print("pond.mode=url is set but pond.url is missing; add the endpoint url")
            return 1
        print(f"pond endpoint: {plugin.url} (url mode - no local binary needed)")
    else:
        # Validate the binary that will actually be spawned (config binaryPath),
        # not a bare PATH lookup that could resolve to a different pond.
        pond_bin = _locate_binary(plugin.binary_path)
        if pond_bin is None:
            print(f"\nfix: {INSTALL_HINT}")
            return 1
        print(f"pond binary: {pond_bin}")

    try:
        from hermes_cli.config import load_config, save_config
    except Exception as exc:
        print(f"could not load hermes config: {exc}")
        return 1

    config = load_config() or {}
    targets = _target_platforms(config, all_platforms)
    changed: list[str] = []
    for platform in targets:
        if _ensure_pond_listed(config, platform):
            changed.append(platform)

    if changed:
        save_config(config)
        print("added the pond toolset to: " + ", ".join(sorted(changed)))
    else:
        print("pond toolset already listed on every targeted platform")

    print(
        "\nunconfigured chat platforms already include the pond tools by default; "
        "the CLI and TUI include them too."
    )
    print(
        "pond config is bootstrapped by the managed process on first run "
        "(`--bootstrap hermes`); an existing pond config is left untouched."
    )
    print("\nrestart the gateway for the tools to take effect: hermes gateway restart")
    return 0


def _cmd_status() -> int:
    from .config import load_plugin_config

    plugin = load_plugin_config()
    if plugin.mode == "url":
        reachable = bool(plugin.url)
        print(f"pond endpoint: {plugin.url or 'MISSING (pond.mode=url needs pond.url)'}")
    else:
        pond_bin = _locate_binary(plugin.binary_path)
        reachable = pond_bin is not None
        if pond_bin is not None:
            print(f"pond binary: {pond_bin}")
    try:
        from hermes_cli.config import load_config

        config = load_config() or {}
    except Exception as exc:
        print(f"could not load hermes config: {exc}")
        return 1
    table = config.get("platform_toolsets") or {}
    listed = sorted(p for p, v in table.items() if isinstance(v, list) and TOOLSET in v)
    if listed:
        print("pond toolset explicitly listed on: " + ", ".join(listed))
    else:
        print("pond toolset not explicitly listed on any platform (default-on where unconfigured)")
    return 0 if reachable else 1
