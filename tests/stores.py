"""Test store discovery: an explicit environment variable, else the compose default.

``.env.test`` holds the DSNs of the stores ``compose.yaml`` starts. A DSN set in
the environment is trusted as is (CI sets them, so a store that is down fails the
run); a DSN taken from the file is probed first, and a store that is not running
skips its tests with a hint instead of failing them.
"""

import socket
from collections.abc import Callable, Mapping
from pathlib import Path

from datorium.core.dsn import parse_dsn

__all__ = ["ENV_FILE", "StoreUnavailable", "load_env_file", "reachable", "resolve_dsn"]

ENV_FILE = Path(__file__).resolve().parent.parent / ".env.test"


class StoreUnavailable(Exception):
    """No DSN for a store, or its compose default is not running."""


def load_env_file(path: Path) -> dict[str, str]:
    """``KEY=VALUE`` lines; blank lines and ``#`` comments are skipped, no quoting."""
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(f"{path} line {number}: expected KEY=VALUE")
        values[key.strip()] = value.strip()
    return values


def reachable(dsn: str, timeout_s: float = 0.5) -> bool:
    """Whether something accepts TCP connections at the DSN's host and port."""
    parsed = parse_dsn(dsn)
    if parsed.host is None or parsed.port is None:
        return True
    try:
        socket.create_connection((parsed.host, parsed.port), timeout=timeout_s).close()
    except OSError:
        return False
    return True


def resolve_dsn(
    var: str,
    environ: Mapping[str, str],
    defaults: Mapping[str, str],
    probe: Callable[[str], bool] = reachable,
) -> str:
    if explicit := environ.get(var):
        return explicit
    default = defaults.get(var)
    if not default:
        raise StoreUnavailable(f"set {var} to run these tests")
    if not probe(default):
        raise StoreUnavailable(f"{var} from .env.test is not running: docker compose up -d --wait")
    return default
