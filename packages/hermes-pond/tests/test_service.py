"""PondController lifecycle against a stub pond binary (real subprocess)."""

from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest
from hermes_pond.config import PondPluginConfig
from hermes_pond.service import PondController, resolve_pond_binary

STUB = Path(__file__).resolve().parent / "stub_pond_bin.py"


@pytest.fixture(autouse=True)
def _ensure_stub_executable():
    mode = STUB.stat().st_mode
    STUB.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    return tmp_path


def _managed_config():
    return PondPluginConfig(mode="managed", binary_path=str(STUB), sources=["hermes"])


def test_resolve_binary_missing_names_the_fix():
    with pytest.raises(Exception) as exc:
        resolve_pond_binary("/does/not/exist/pond")
    assert "install pond" in str(exc.value)


def test_managed_dial_call_and_stop(hermes_home, monkeypatch):
    log = hermes_home / "stub-calls.jsonl"
    monkeypatch.setenv("HERMES_POND_STUB_LOG", str(log))
    controller = PondController(_managed_config())
    try:
        ok, text = controller.call_tool("pond_search", {"query": "hi", "limit": 5})
        assert ok is True
        assert text == "stub:pond_search"
        recorded = [json.loads(x) for x in log.read_text().splitlines() if x.strip()]
        assert recorded[0]["name"] == "pond_search"
        assert recorded[0]["arguments"] == {"query": "hi", "limit": 5}
    finally:
        controller.stop()


def test_second_call_reuses_connection(hermes_home):
    controller = PondController(_managed_config())
    try:
        controller.call_tool("pond_search", {"query": "a"})
        controller.call_tool("pond_get_session", {"id": "s1"})
        # A single long-lived child: verify the pond-serve log has exactly one
        # readiness banner (one process spawned).
        serve_log = (hermes_home / "logs" / "pond-serve.log").read_text()
        assert serve_log.count("stdio MCP ready") == 1
    finally:
        controller.stop()


def test_stop_is_idempotent(hermes_home):
    controller = PondController(_managed_config())
    controller.call_tool("pond_search", {"query": "a"})
    controller.stop()
    controller.stop()
    ok, text = controller.call_tool("pond_search", {"query": "a"})
    assert ok is False
    assert "stopped" in text


def test_binary_missing_returns_typed_error(hermes_home):
    controller = PondController(
        PondPluginConfig(mode="managed", binary_path="/no/such/pond", sources=["hermes"])
    )
    ok, text = controller.call_tool("pond_search", {"query": "a"})
    assert ok is False
    # Not connected yet -> the starting/unavailable message; the install hint is
    # in the logs. A second attempt is gated by backoff.
    assert "pond is not connected" in text
