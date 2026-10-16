"""Connection strings (DSNs) for stores and the journal.

One URL syntax for every store, parsed without importing any driver. The schemes
and their rules (host, path, default port) are declared by the registered
adapters (``rhizodat.core.adapter``); rhizodat's own are:

==================================================  ==========  ===========
DSN                                                 store       transport
==================================================  ==========  ===========
``postgresql://user:pass@host:5432/db?sslmode=…``   postgres    —
``clickhouse://user:pass@host:8123/db``             clickhouse  —
``drevo:///abs/path.drevo``                         drevo       embedded
``drevo+mem://`` (tests only)                       drevo       memory
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

from rhizodat.core.adapter import load_errors
from rhizodat.core.adapter import scheme as registered_scheme
from rhizodat.core.adapter import schemes as registered_schemes

__all__ = ["DSN", "DSNError", "parse_dsn"]


class DSNError(ValueError):
    """The connection string is malformed or names an unsupported store."""


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
        return redact(self._raw)

    def __str__(self) -> str:
        return self.redacted()

    def __repr__(self) -> str:
        return f"DSN({self.redacted()!r})"


def redact(raw: str) -> str:
    """``raw`` with its password and secret-looking query parameters masked.

    Needs no registered adapter: for connection strings that may not parse.
    """
    try:
        parts = urlsplit(raw)
    except ValueError:
        return f"{_scheme_hint(raw)}{_MASK}"
    netloc = parts.netloc
    userinfo, at, hostinfo = netloc.rpartition("@")
    if at and ":" in userinfo:
        netloc = f"{userinfo.partition(':')[0]}:{_MASK}@{hostinfo}"
    query = "&".join(
        f"{k}={_MASK if _is_secret(k) else v}"
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
    )
    out = f"{parts.scheme}://{netloc}{parts.path}"
    return f"{out}?{query}" if query else out


def _is_secret(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in _SECRET_MARKERS)


def parse_dsn(raw: str) -> DSN:
    try:
        parts = urlsplit(raw)
    except ValueError as exc:  # an unbalanced IPv6 bracket
        raise DSNError(f"invalid host in {_scheme_hint(raw)!r} connection string") from exc
    scheme = parts.scheme.lower()
    found = registered_scheme(scheme) if raw.lower().startswith(f"{scheme}://") else None
    if found is None:
        supported = ", ".join(f"{s}://" for s in registered_schemes())
        broken = "".join(
            f"; plug-in {name!r} failed to load ({error})" for name, error in load_errors().items()
        )
        raise DSNError(
            f"unsupported connection string {_scheme_hint(raw)!r}; supported: {supported}{broken}"
        )
    adapter, rules = found

    try:
        port = parts.port
    except ValueError as exc:
        raise DSNError(f"invalid port in {scheme}:// connection string") from exc
    if port == 0:
        raise DSNError(f"invalid port 0 in {scheme}:// connection string")

    host = parts.hostname or None
    path = unquote(parts.path) or None

    if rules.host == "none" and parts.netloc:
        if rules.path == "required":
            raise DSNError(
                f"{scheme}:/// takes a local absolute path, not a host: {scheme}:///abs/path"
            )
        raise DSNError(f"{scheme}:// takes no host" + (" or path" if rules.path == "none" else ""))
    if rules.path == "required" and path is None:
        raise DSNError(f"{scheme}:/// needs a path: {scheme}:///abs/path")
    if rules.path == "none" and path:
        raise DSNError(f"{scheme}:// takes no host or path")
    if rules.host == "required" and host is None:
        raise DSNError(f"{scheme}:// needs a host")

    params: dict[str, str] = {}
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key in params:
            raise DSNError(f"query parameter {key!r} is given more than once")
        params[key] = value

    return DSN(
        store=adapter.kind,
        transport=rules.transport,
        host=host,
        port=port if port is not None else (rules.default_port if host else None),
        user=unquote(parts.username) if parts.username else None,
        password=unquote(parts.password) if parts.password is not None else None,
        path=path,
        _params=tuple(params.items()),
        _raw=raw,
    )


def _scheme_hint(raw: str) -> str:
    """The scheme part only, so an error never echoes credentials."""
    return raw.partition("://")[0] + "://" if "://" in raw else raw.partition(":")[0]
