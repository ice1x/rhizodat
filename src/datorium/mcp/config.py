"""datorium-MCP configuration from the environment.

MCP clients start the server with an environment, so DSNs (and their secrets)
stay out of command lines:

- ``DATORIUM_STORE_<NAME>=<dsn>``: one per store; the name is lower-cased;
- ``DATORIUM_JOURNAL=<dsn>``: the journal (optional until writes need it);
- ``DATORIUM_MCP_MAX_ROWS=<n>``: the most rows one query returns (default 1000);
- ``DATORIUM_MCP_ENTITIES=<module:Class>,...``: the ``@entity`` classes offered
  for CRUD (the application's package must be importable);
- ``DATORIUM_MCP_WRITES=1``: enable creating, updating, deleting and restoring
  entities (off by default: a delete is a hard erase unless the type says soft).

Transactions and recovery are not the agent's business: the former recovery
settings are refused, so a stale configuration does not pass silently.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

__all__ = ["Config", "ConfigError", "config_from_env"]

_STORE_PREFIX = "DATORIUM_STORE_"
DEFAULT_MAX_ROWS = 1000
_YES = {"1", "true", "yes", "on"}
_NO = {"", "0", "false", "no", "off"}
_REMOVED = ("DATORIUM_MCP_RECOVERY", "DATORIUM_MCP_RECOVERY_STALE_S")


class ConfigError(ValueError):
    """The environment does not configure a usable server."""


@dataclass(frozen=True, slots=True)
class Config:
    stores: dict[str, str] = field(default_factory=dict)
    journal: str | None = None
    max_rows: int = DEFAULT_MAX_ROWS
    entities: tuple[str, ...] = ()
    writes: bool = False


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
    for removed in _REMOVED:
        if removed in environ:
            raise ConfigError(
                f"{removed} was removed: datorium-MCP no longer runs recovery or shows"
                " transactions; run Datorium.recovery() from an operator process instead"
            )
    entities = tuple(
        e.strip() for e in environ.get("DATORIUM_MCP_ENTITIES", "").split(",") if e.strip()
    )
    flag = environ.get("DATORIUM_MCP_WRITES", "").strip().lower()
    if flag not in _YES | _NO:
        raise ConfigError(f"DATORIUM_MCP_WRITES must be 1 or 0, not {flag!r}")
    return Config(
        stores,
        environ.get("DATORIUM_JOURNAL") or None,
        max_rows,
        entities=entities,
        writes=flag in _YES,
    )
