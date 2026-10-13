"""datorium-MCP configuration from the environment.

MCP clients start the server with an environment, so DSNs (and their secrets)
stay out of command lines:

- ``DATORIUM_STORE_<NAME>=<dsn>``: one per store; the name is lower-cased;
- ``DATORIUM_JOURNAL=<dsn>``: the journal (optional; the journal tools need it);
- ``DATORIUM_MCP_MAX_ROWS=<n>``: the most rows one query returns (default 1000);
- ``DATORIUM_MCP_RECOVERY=1``: offer the ``run_recovery`` tool, which writes (off by
  default: the other tools only read);
- ``DATORIUM_MCP_RECOVERY_STALE_S=<seconds>``: how long a record stays unchanged before
  recovery takes it over (default 30).
"""

import math
from collections.abc import Mapping
from dataclasses import dataclass, field

__all__ = ["Config", "ConfigError", "config_from_env"]

_STORE_PREFIX = "DATORIUM_STORE_"
DEFAULT_MAX_ROWS = 1000
DEFAULT_STALE_AFTER_S = 30.0
_ON = {"1", "true", "yes", "on"}
_OFF = {"", "0", "false", "no", "off"}


class ConfigError(ValueError):
    """The environment does not configure a usable server."""


@dataclass(frozen=True, slots=True)
class Config:
    stores: dict[str, str] = field(default_factory=dict)
    journal: str | None = None
    max_rows: int = DEFAULT_MAX_ROWS
    recovery: bool = False
    stale_after_s: float = DEFAULT_STALE_AFTER_S


def config_from_env(environ: Mapping[str, str]) -> Config:
    stores = {
        key.removeprefix(_STORE_PREFIX).lower(): dsn
        for key, dsn in sorted(environ.items())
        if key.startswith(_STORE_PREFIX) and len(key) > len(_STORE_PREFIX)
    }
    if not stores:
        raise ConfigError(f"no stores configured: set {_STORE_PREFIX}<NAME>=<dsn> for each store")
    raw = environ.get("DATORIUM_MCP_MAX_ROWS", str(DEFAULT_MAX_ROWS))
    try:
        max_rows = int(raw)
    except ValueError:
        raise ConfigError(f"DATORIUM_MCP_MAX_ROWS must be a number, not {raw!r}") from None
    if max_rows < 1:
        raise ConfigError(f"DATORIUM_MCP_MAX_ROWS must be at least 1, not {max_rows}")
    flag = environ.get("DATORIUM_MCP_RECOVERY", "").strip().lower()
    if flag not in _ON | _OFF:
        raise ConfigError(f"DATORIUM_MCP_RECOVERY must be 1 or 0, not {flag!r}")
    raw = environ.get("DATORIUM_MCP_RECOVERY_STALE_S", str(DEFAULT_STALE_AFTER_S))
    try:
        stale_after_s = float(raw)
    except ValueError:
        stale_after_s = math.nan
    if not stale_after_s > 0 or math.isinf(stale_after_s):
        raise ConfigError(f"DATORIUM_MCP_RECOVERY_STALE_S must be seconds > 0, not {raw!r}")
    return Config(
        stores,
        environ.get("DATORIUM_JOURNAL") or None,
        max_rows,
        recovery=flag in _ON,
        stale_after_s=stale_after_s,
    )
