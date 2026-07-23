"""`hermes pond setup` allowlist writing, against fake hermes config modules."""

from __future__ import annotations

import sys
import types
from collections import namedtuple

import hermes_pond.setup_cmd as setup_cmd
import pytest

_Info = namedtuple("_Info", ["default_toolset"])

# A trimmed platform registry mirroring hermes_cli/platforms.py shape.
_FAKE_PLATFORMS = {
    "cli": _Info("hermes-cli"),
    "telegram": _Info("hermes-telegram"),
    "discord": _Info("hermes-discord"),
    "api_server": _Info("hermes-api-server"),
    "cron": _Info("hermes-cron"),
}


@pytest.fixture
def fake_hermes(monkeypatch):
    """Install fake hermes_cli.config + hermes_cli.platforms; capture saved cfg."""
    state = {"config": {}, "saved": None}

    hermes_cli = types.ModuleType("hermes_cli")
    hermes_cli.__path__ = []  # mark as package

    platforms_mod = types.ModuleType("hermes_cli.platforms")
    platforms_mod.PLATFORMS = _FAKE_PLATFORMS

    config_mod = types.ModuleType("hermes_cli.config")
    config_mod.load_config = lambda: state["config"]

    def _save(cfg):
        state["saved"] = cfg

    config_mod.save_config = _save

    monkeypatch.setitem(sys.modules, "hermes_cli", hermes_cli)
    monkeypatch.setitem(sys.modules, "hermes_cli.platforms", platforms_mod)
    monkeypatch.setitem(sys.modules, "hermes_cli.config", config_mod)
    return state


def test_ensure_pond_listed_new_platform_seeds_default(fake_hermes):
    config = {}
    changed = setup_cmd._ensure_pond_listed(config, "telegram")
    assert changed is True
    assert config["platform_toolsets"]["telegram"] == ["hermes-telegram", "pond"]


def test_ensure_pond_listed_appends_to_existing(fake_hermes):
    config = {"platform_toolsets": {"discord": ["hermes-discord", "spotify"]}}
    changed = setup_cmd._ensure_pond_listed(config, "discord")
    assert changed is True
    assert config["platform_toolsets"]["discord"] == ["hermes-discord", "spotify", "pond"]


def test_ensure_pond_listed_idempotent(fake_hermes):
    config = {"platform_toolsets": {"cron": ["hermes-cron", "pond"]}}
    changed = setup_cmd._ensure_pond_listed(config, "cron")
    assert changed is False


def test_target_platforms_default_is_configured_plus_forced(fake_hermes):
    config = {"platform_toolsets": {"telegram": ["hermes-telegram"], "cli": ["hermes-cli"]}}
    targets = setup_cmd._target_platforms(config, all_platforms=False)
    # configured (telegram) + forced (api_server, cron); cli excluded
    assert set(targets) == {"telegram", "api_server", "cron"}


def test_target_platforms_all_excludes_cli(fake_hermes):
    targets = setup_cmd._target_platforms({}, all_platforms=True)
    assert "cli" not in targets
    assert {"telegram", "discord", "api_server", "cron"}.issubset(set(targets))


def test_cmd_setup_writes_allowlist(fake_hermes, monkeypatch, capsys):
    fake_hermes["config"] = {"platform_toolsets": {"telegram": ["hermes-telegram"]}}
    monkeypatch.setattr(setup_cmd, "_locate_binary", lambda: "/usr/local/bin/pond")

    rc = setup_cmd._cmd_setup(all_platforms=False)
    assert rc == 0
    saved = fake_hermes["saved"]
    assert saved is not None
    pt = saved["platform_toolsets"]
    assert "pond" in pt["telegram"]
    assert pt["api_server"] == ["hermes-api-server", "pond"]
    assert pt["cron"] == ["hermes-cron", "pond"]


def test_cmd_setup_missing_binary_fails_without_writing(fake_hermes, monkeypatch):
    monkeypatch.setattr(setup_cmd, "_locate_binary", lambda: None)
    rc = setup_cmd._cmd_setup(all_platforms=False)
    assert rc == 1
    assert fake_hermes["saved"] is None
