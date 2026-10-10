"""Connection strings (DSNs) for stores and the journal.

One URL syntax for every store, parsed without importing any driver:

==================================================  ==========  ===========
DSN                                                 store       transport
==================================================  ==========  ===========
``postgresql://user:pass@host:5432/db?sslmode=…``   postgres    —
``clickhouse://user:pass@host:8123/db``             clickhouse  —
``drevo:///abs/path.redb``                          drevo       embedded
``drevo+mem://``                                    drevo       memory
``drevo+bolt://user:pass@host:7687``                drevo       bolt
``iceberg+rest://host:8181/warehouse``              iceberg     rest
``iceberg+sql://…``, ``iceberg+glue://…``           iceberg     sql, glue
``file:///abs/path``                                file        —
==================================================  ==========  ===========

``postgres://`` is accepted as an alias. Scheme matching ignores case. User,
password and path are percent-decoded. Passwords and secret-looking query
parameters never appear in ``repr``/``str``; use ``unredacted()`` to get the
original string for a driver.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, unquote, urlsplit

__all__ = ["DSN", "DSNError", "parse_dsn"]


class DSNError(ValueError):
    """The connection string is malformed or names an unsupported store."""


# scheme -> (store, transport, default port)
_SCHEMES: dict[str, tuple[str, str | None, int | None]] = {
    "postgresql": ("postgres", None, 5432),
    "postgres": ("postgres", None, 5432),
    "clickhouse": ("clickhouse", None, 8123),
    "drevo": ("drevo", "embedded", None),
    "drevo+mem": ("drevo", "memory", None),
    "drevo+bolt": ("drevo", "bolt", 7687),
    "iceberg+rest": ("iceberg", "rest", None),
    "iceberg+sql": ("iceberg", "sql", None),
    "iceberg+glue": ("iceberg", "glue", None),
    "file": ("file", None, None),
}
_HOST_REQUIRED = {"clickhouse", "drevo+bolt", "iceberg+rest"}
_SECRET_MARKERS = ("pass", "secret", "token", "key", "credential")
_MASK = "***"


@dataclass(frozen=True, slots=True)
class DSN:
    store: str
    transport: str | None
    host: str | None
    port: int | None
    user: str | None
    password: str | None = field(repr=False)
    path: str | None
    _params: tuple[tuple[str, str], ...] = field(repr=False)
    _raw: str = field(repr=False)

    @property
    def database(self) -> str | None:
        """The path without its leading slash: database, warehouse or catalog name."""
        if self.path is None:
            return None
        return self.path.lstrip("/") or None

    @property
    def params(self) -> Mapping[str, str]:
        return dict(self._params)

    def unredacted(self) -> str:
        """The original string, secrets included. Pass it to drivers only."""
        return self._raw

    def redacted(self) -> str:
        parts = urlsplit(self._raw)
        netloc = parts.netloc
        if self.password is not None:
            userinfo, _, hostinfo = netloc.rpartition("@")
            name = userinfo.partition(":")[0]
            netloc = f"{name}:{_MASK}@{hostinfo}"
        query = "&".join(
            f"{k}={_MASK if _is_secret(k) else v}"
            for k, v in parse_qsl(parts.query, keep_blank_values=True)
        )
        out = f"{parts.scheme}://{netloc}{parts.path}"
        return f"{out}?{query}" if query else out

    def __str__(self) -> str:
        return self.redacted()

    def __repr__(self) -> str:
        return f"DSN({self.redacted()!r})"


def _is_secret(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in _SECRET_MARKERS)


def parse_dsn(raw: str) -> DSN:
    parts = urlsplit(raw)
    scheme = parts.scheme.lower()
    if scheme not in _SCHEMES or not raw.lower().startswith(f"{scheme}://"):
        supported = ", ".join(f"{s}://" for s in _SCHEMES)
        raise DSNError(
            f"unsupported connection string {_scheme_hint(raw)!r}; supported: {supported}"
        )
    store, transport, default_port = _SCHEMES[scheme]

    try:
        port = parts.port
    except ValueError as exc:
        raise DSNError(f"invalid port in {scheme}:// connection string") from exc
    if port == 0:
        raise DSNError(f"invalid port 0 in {scheme}:// connection string")

    host = parts.hostname or None
    path = unquote(parts.path) or None

    if scheme in ("drevo", "file"):
        if parts.netloc:
            raise DSNError(
                f"{scheme}:/// takes a local absolute path, not a host: {scheme}:///abs/path"
            )
        if path is None:
            raise DSNError(f"{scheme}:/// needs a path: {scheme}:///abs/path")
    if scheme == "drevo+mem" and (parts.netloc or path):
        raise DSNError("drevo+mem:// takes no host or path")
    if scheme in _HOST_REQUIRED and host is None:
        raise DSNError(f"{scheme}:// needs a host")

    params: dict[str, str] = {}
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key in params:
            raise DSNError(f"query parameter {key!r} is given more than once")
        params[key] = value

    return DSN(
        store=store,
        transport=transport,
        host=host,
        port=port if port is not None else (default_port if host else None),
        user=unquote(parts.username) if parts.username else None,
        password=unquote(parts.password) if parts.password is not None else None,
        path=path,
        _params=tuple(params.items()),
        _raw=raw,
    )


def _scheme_hint(raw: str) -> str:
    """The scheme part only, so an error never echoes credentials."""
    return raw.partition("://")[0] + "://" if "://" in raw else raw.partition(":")[0]
