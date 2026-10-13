"""The datorium-MCP tools as plain functions over a ``Datorium`` (no MCP dependency).

Every result is a JSON value: store values are converted by ``jsonable``.
"""

import base64
import datetime as dt
import decimal
import itertools
import math
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any

from datorium.api import Datorium
from datorium.core.dsn import parse_dsn
from datorium.core.journal import Journal, JournalRecord

__all__ = ["Tools", "jsonable"]

DEFAULT_LIMIT = 100


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


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
    ``max_rows`` caps what one query returns.
    """

    def __init__(
        self,
        dat: Datorium,
        *,
        dsns: Mapping[str, str] | None = None,
        max_rows: int = 1000,
        wall_ms: Callable[[], int] = _system_ms,
    ) -> None:
        self._dat = dat
        self._dsns = dict(dsns or {})
        self._max_rows = max_rows
        self._wall_ms = wall_ms

    @classmethod
    def open(
        cls, stores: Mapping[str, str], *, journal: str | None = None, max_rows: int = 1000
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
                    "read_only_queries": callable(getattr(store, "read_only", None)),
                    "dsn": parsed.redacted() if parsed else None,
                }
            )
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
        read_only = getattr(target, "read_only", None)
        if not callable(read_only):
            raise ValueError(f"store {store!r} offers no read-only queries")
        result = read_only(query, params)
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

    def transaction(self, dat_txn: str) -> dict[str, Any] | None:
        """One meta-transaction's journal record, or ``None`` if there is none."""
        journal = self._journal()
        try:
            txn = uuid.UUID(dat_txn)
        except ValueError:
            raise ValueError(f"dat_txn must be a UUID, not {dat_txn!r}") from None
        record = journal.get(txn)
        return None if record is None else self._record(record, self._wall_ms())

    def pending_transactions(
        self, older_than_s: float = 0, limit: int = DEFAULT_LIMIT
    ) -> dict[str, Any]:
        """Unfinished meta-transactions (ACTIVE, PREPARED, COMMITTED) unchanged for at least
        ``older_than_s`` seconds, oldest first: in flight, or left for recovery."""
        journal = self._journal()
        now = self._wall_ms()
        records = journal.pending(
            updated_before_ms=now - int(older_than_s * 1000) + 1, limit=min(limit, self._max_rows)
        )
        return {"now_ms": now, "transactions": [self._record(r, now) for r in records]}

    def _journal(self) -> Journal:
        if self._dat.journal is None:
            raise ValueError("no journal configured: set DATORIUM_JOURNAL")
        return self._dat.journal

    @staticmethod
    def _record(record: JournalRecord, now: int) -> dict[str, Any]:
        return {
            "dat_txn": str(record.dat_txn),
            "state": str(record.state),
            "dat_tx_time": record.dat_tx_time,
            "created_ms": record.created_ms,
            "updated_ms": record.updated_ms,
            "age_ms": now - record.updated_ms,
            "branches": [
                {
                    "store": b.store,
                    "gid": b.gid,
                    "two_phase": b.two_phase,
                    "payload_bytes": len(b.payload) if b.payload is not None else 0,
                }
                for b in record.branches
            ],
        }
