"""The datorium-MCP tools as plain functions over a ``Datorium`` (no MCP dependency).

Every result is a JSON value: store values are converted by ``jsonable``.
"""

import base64
import datetime as dt
import decimal
import itertools
import math
import uuid
from collections.abc import Mapping
from typing import Any

from datorium.api import Datorium
from datorium.core.adapter import ReadOnlyQueries, adapter_for
from datorium.core.dsn import parse_dsn

__all__ = ["Tools", "jsonable"]

DEFAULT_LIMIT = 100


def jsonable(value: Any) -> Any:  # noqa: ANN401 - any store value
    """``value`` as JSON: text for UUIDs, decimals (exact), dates and times (ISO 8601) and
    non-finite floats, seconds for durations, ``{"base64": ...}`` for bytes, lists for
    sequences and sets; anything else becomes its text."""
    match value:
        case None | bool() | int() | str():
            return value
        case float():
            return value if math.isfinite(value) else str(value)
        case uuid.UUID() | decimal.Decimal():
            return str(value)
        case dt.datetime() | dt.date() | dt.time():
            return value.isoformat()
        case dt.timedelta():
            return value.total_seconds()
        case bytes() | bytearray() | memoryview():
            return {"base64": base64.b64encode(bytes(value)).decode()}
        case Mapping():
            return {str(k): jsonable(v) for k, v in value.items()}
        case list() | tuple() | set() | frozenset():
            return [jsonable(v) for v in value]
    return str(value)


class Tools:
    """What datorium-MCP offers, over one ``Datorium``.

    ``dsns`` are the stores' connection strings, shown with secrets masked;
    ``max_rows`` caps what one query returns. Transactions, the journal and
    recovery are datorium's own business and are not offered.
    """

    def __init__(
        self,
        dat: Datorium,
        *,
        dsns: Mapping[str, str] | None = None,
        max_rows: int = 1000,
    ) -> None:
        self._dat = dat
        self._dsns = dict(dsns or {})
        self._max_rows = max_rows

    @classmethod
    def open(
        cls,
        stores: Mapping[str, str],
        *,
        journal: str | None = None,
        max_rows: int = 1000,
    ) -> "Tools":
        """Open every store (and the journal) named by its DSN."""
        return cls(Datorium(stores=stores, journal=journal), dsns=stores, max_rows=max_rows)

    def close(self) -> None:
        self._dat.close()

    def stores(self) -> list[dict[str, Any]]:
        out = []
        for name in self._dat.store_names:
            store = self._dat.store(name)
            dsn = self._dsns.get(name)
            parsed = parse_dsn(dsn) if dsn else None
            out.append(
                {
                    "name": name,
                    "kind": parsed.store if parsed else type(store).__name__,
                    "two_phase": bool(store.two_phase),
                    "read_only_queries": isinstance(store, ReadOnlyQueries),
                    "dsn": parsed.redacted() if parsed else None,
                }
            )
        return out

    def query_languages(self) -> dict[str, str]:
        """Each configured store kind's native query language, from its adapter."""
        out: dict[str, str] = {}
        for dsn in self._dsns.values():
            kind = parse_dsn(dsn).store
            help_ = adapter_for(kind).query_help
            if help_:
                out.setdefault(kind, help_)
        return out

    def query(
        self,
        store: str,
        query: str,
        params: Any = None,  # noqa: ANN401 - the store's own parameters
        limit: int = DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        """A native read through the store's ``read_only`` access, at most ``limit`` rows."""
        if limit < 1:
            raise ValueError(f"limit must be at least 1, not {limit}")
        limit = min(limit, self._max_rows)
        target = self._dat.store(store)
        if not isinstance(target, ReadOnlyQueries):
            raise ValueError(f"store {store!r} offers no read-only queries")
        result = target.read_only(query, params)
        try:
            rows = list(itertools.islice(result, limit + 1))
        finally:
            result.close()
        columns = result.columns
        return {
            "store": store,
            "columns": list(columns) if columns is not None else None,
            "rows": [jsonable(r) for r in rows[:limit]],
            "truncated": len(rows) > limit,
        }
