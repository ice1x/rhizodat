"""The core in Rust (``rhizodat_native``, built from ``crates/rhizodat-py``), when installed.

The module holds the Rust implementation of the pure parts of the core (UUIDv7
ids, the hybrid logical clock, branch gids, migration files); both pass the
vectors in ``conformance/``. The package uses it for ``new_id`` and keeps the
Python implementation when the module is missing or ``RHIZODAT_NATIVE=0``.
Install it with ``uv sync --group native`` (needs a Rust toolchain).
"""

import importlib
import os
from types import ModuleType

__all__ = ["ENV", "NATIVE", "load"]

ENV = "RHIZODAT_NATIVE"


def load() -> ModuleType | None:
    """``rhizodat_native`` if it is installed and not turned off, else ``None``."""
    if os.environ.get(ENV) == "0":
        return None
    try:
        return importlib.import_module("rhizodat_native")
    except ImportError:
        return None


NATIVE = load()
