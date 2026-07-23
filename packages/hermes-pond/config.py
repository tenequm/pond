"""Plugin-owned configuration, read from ``plugins.entries.pond`` in config.yaml.

Mirrors openclaw-pond's config.ts: connection mode plus the corpus-source knob.
Hermes has no per-agent session-visibility SDK equivalent to OpenClaw's, so
there is no visibility clamp here - the tools expose whatever the operator's
pond store holds (the operator chooses what pond ingests). See the README.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_SYNC_INTERVAL_MINUTES = 5
DEFAULT_SOURCES = ("hermes",)


@dataclass
class PondPluginConfig:
    mode: str = "managed"  # "managed" | "url"
    sync_interval_minutes: int = DEFAULT_SYNC_INTERVAL_MINUTES
    binary_path: str | None = None
    url: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    sources: list[str] = field(default_factory=lambda: list(DEFAULT_SOURCES))


def _as_record(value: object) -> dict:
    return value if isinstance(value, dict) else {}


def _as_str_list(value: object, fallback: tuple[str, ...]) -> list[str]:
    if not isinstance(value, list):
        return list(fallback)
    items = [v for v in value if isinstance(v, str) and v]
    return items if items else list(fallback)


def _as_str_map(value: object) -> dict[str, str]:
    record = _as_record(value)
    return {k: v for k, v in record.items() if isinstance(v, str)}


def parse_plugin_config(raw: object) -> PondPluginConfig:
    """Parse the ``plugins.entries.pond`` subtree into a typed config.

    Unknown / malformed fields fall back to defaults rather than raising, so a
    hand-edited config never blocks the gateway from starting.
    """
    root = _as_record(raw)
    pond = _as_record(root.get("pond"))

    mode = "url" if pond.get("mode") == "url" else "managed"

    raw_interval = pond.get("syncIntervalMinutes", pond.get("sync_interval_minutes"))
    if isinstance(raw_interval, (int, float)) and raw_interval == raw_interval:
        sync_interval = max(1, int(raw_interval))
    else:
        sync_interval = DEFAULT_SYNC_INTERVAL_MINUTES

    binary_path = pond.get("binaryPath", pond.get("binary_path"))
    binary_path = binary_path if isinstance(binary_path, str) and binary_path else None

    url = pond.get("url")
    url = url if isinstance(url, str) and url else None

    return PondPluginConfig(
        mode=mode,
        sync_interval_minutes=sync_interval,
        binary_path=binary_path,
        url=url,
        headers=_as_str_map(pond.get("headers")),
        sources=_as_str_list(root.get("sources"), DEFAULT_SOURCES),
    )


def load_plugin_config() -> PondPluginConfig:
    """Read the live plugin config from hermes' config.yaml.

    Imports hermes lazily so this module stays importable (and unit-testable)
    without a hermes install. Returns defaults when config cannot be read.
    """
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        entry = ((cfg.get("plugins") or {}).get("entries") or {}).get("pond")
        return parse_plugin_config(entry)
    except Exception:
        return PondPluginConfig()
