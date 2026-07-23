"""Load the plugin package the way hermes' loader does.

Hermes imports a directory plugin via importlib with the plugin dir on
``submodule_search_locations`` and a synthetic package name, so relative imports
(`from .tools import ...`) resolve. We reproduce that exactly here, registering
the package as ``hermes_pond`` - which also proves the package is loadable the
way hermes will load it (see test_plugin_load.py).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent.parent
PKG_NAME = "hermes_pond"


def _load_package():
    if PKG_NAME in sys.modules:
        return sys.modules[PKG_NAME]
    spec = importlib.util.spec_from_file_location(
        PKG_NAME,
        PKG_DIR / "__init__.py",
        submodule_search_locations=[str(PKG_DIR)],
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[PKG_NAME] = module
    spec.loader.exec_module(module)
    return module


_load_package()
