"""The datorium-MCP tools as plain functions over a ``Datorium`` (no MCP dependency).

Every result is a JSON value: store values are converted by ``jsonable``.
"""

import base64
import datetime as dt
import decimal
import itertools
import math
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from datorium.api import Datorium
from datorium.core.adapter import ReadOnlyQueries, adapter_for
from datorium.core.coordinator import CommittedPending
from datorium.core.dsn import parse_dsn
from datorium.core.mapping import mapping_of
from datorium.core.parts import hlc_bound

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
    ``max_rows`` caps what one query returns. ``entities`` are the ``@entity``
    classes offered for CRUD; ``writes`` enables creating, updating, deleting and
    restoring them. Each entity call is one Repository call, so one Unit of Work:
    transactions, the journal and recovery are datorium's own business and are
    not offered.
    """

    def __init__(
        self,
        dat: Datorium,
        *,
        dsns: Mapping[str, str] | None = None,
        max_rows: int = 1000,
        entities: Sequence[type] = (),
        writes: bool = False,
    ) -> None:
        self._dat = dat
        self._dsns = dict(dsns or {})
        self._max_rows = max_rows
        self._types: dict[str, type] = {}
        for cls in entities:
            name = mapping_of(cls).type_name
            if name in self._types:
                raise ValueError(f"two entity classes are named {name!r}")
            self._types[name] = cls
        self.writes = writes

    @classmethod
    def open(
        cls,
        stores: Mapping[str, str],
        *,
        journal: str | None = None,
        max_rows: int = 1000,
        entities: Sequence[type] = (),
        writes: bool = False,
    ) -> "Tools":
        """Open every store (and the journal) named by its DSN."""
        return cls(
            Datorium(stores=stores, journal=journal),
            dsns=stores,
            max_rows=max_rows,
            entities=entities,
            writes=writes,
        )

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

    # --- entities ------------------------------------------------------------------------

    @property
    def entity_type_names(self) -> tuple[str, ...]:
        return tuple(self._types)

    def entity_types(self) -> list[dict[str, Any]]:
        """Each offered entity type: its fields (how to write each), stores and delete mode."""
        from datorium.mcp.entities import describe, field_types, fields

        out = []
        for name, cls in self._types.items():
            mapping = mapping_of(cls)
            types_ = field_types(mapping)
            window = mapping.soft_delete_window or self._dat.soft_delete_window
            out.append(
                {
                    "name": name,
                    "fields": {f: describe(mapping, f, types_[f]) for f in fields(mapping)},
                    "valid_time": mapping.valid_time,
                    "stores": {s: list(fs) for s, fs in mapping.stores.items()},
                    "delete": "soft" if mapping.soft_delete else "hard",
                    "restorable_for_s": window.total_seconds() if mapping.soft_delete else None,
                }
            )
        return out

    def get_entity(
        self,
        type_name: str,
        dat_id: str,
        *,
        as_of: str | None = None,
        known_at: str | None = None,
        include_deleted: bool = False,
    ) -> dict[str, Any] | None:
        """The entity known at ``known_at`` (now if ``None``); for a valid-time type the
        version holding at ``as_of``. Times are ISO 8601 with a time zone."""
        from datorium.mcp.entities import entity_json

        found = self._repo(type_name).get(
            _id(dat_id),
            as_of=None if as_of is None else _moment(as_of),
            known_at=None if known_at is None else _moment(known_at),
            include_deleted=include_deleted,
        )
        return None if found is None else entity_json(found)

    def entity_history(self, type_name: str, dat_id: str) -> list[dict[str, Any]]:
        """Every committed version, oldest first (soft deletes included)."""
        from datorium.mcp.entities import entity_json

        return [entity_json(v) for v in self._repo(type_name).history(_id(dat_id))]

    def create_entity(self, type_name: str, values: Mapping[str, Any]) -> dict[str, Any]:
        """A new entity from every field's value; one Unit of Work over its stores."""
        from datorium.mcp.entities import entity_json, values_from_json

        self._check_writes()
        cls = self._type(type_name)
        obj = cls(**values_from_json(mapping_of(cls), values, partial=False))
        outcome = self._repo(type_name).add(obj)
        return {"entity": entity_json(outcome.value), "pending": _pending(outcome)}

    def update_entity(
        self, type_name: str, dat_id: str, values: Mapping[str, Any]
    ) -> dict[str, Any]:
        """A new version with ``values`` changed; fields not given are kept."""
        from datorium.mcp.entities import entity_json, replace, values_from_json

        self._check_writes()
        cls = self._type(type_name)
        changes = values_from_json(mapping_of(cls), values, partial=True)
        repo = self._repo(type_name)
        current = _latest(repo, mapping_of(cls), _id(dat_id))
        if current is None:
            raise LookupError(f"no {type_name} {dat_id}")
        outcome = repo.update(replace(current, changes))
        return {"entity": entity_json(outcome.value), "pending": _pending(outcome)}

    def delete_entity(self, type_name: str, dat_id: str) -> dict[str, Any]:
        """Delete: hidden and restorable for a soft-delete type, else erased for good."""
        self._check_writes()
        soft = mapping_of(self._type(type_name)).soft_delete
        outcome = self._repo(type_name).delete(_id(dat_id))
        return {
            "dat_id": dat_id,
            "deleted": "soft" if soft else "hard",
            "pending": _pending(outcome),
        }

    def restore_entity(self, type_name: str, dat_id: str) -> dict[str, Any]:
        """Bring a soft-deleted entity back within its window."""
        from datorium.core.repository import NotRestorable
        from datorium.mcp.entities import entity_json

        self._check_writes()
        try:
            outcome = self._repo(type_name).restore(_id(dat_id))
        except NotRestorable as exc:
            raise LookupError(str(exc)) from exc
        return {"entity": entity_json(outcome.value), "pending": _pending(outcome)}

    def _type(self, type_name: str) -> type:
        try:
            return self._types[type_name]
        except KeyError:
            raise KeyError(
                f"no entity type {type_name!r}; offered: {sorted(self._types)}"
            ) from None

    def _repo(self, type_name: str) -> Any:  # noqa: ANN401 - EntityRepository of that type
        return self._dat.repository(self._type(type_name))

    def _check_writes(self) -> None:
        if not self.writes:
            raise PermissionError(
                "entity writes are off: the server was started without DATORIUM_MCP_WRITES=1"
            )


def _latest(repo: Any, mapping: Any, dat_id: uuid.UUID) -> object | None:  # noqa: ANN401
    """The version an update starts from: the latest, whatever period it holds for."""
    if not mapping.valid_time:
        current: object | None = repo.get(dat_id)
        return current
    versions: list[object] = repo.history(dat_id)
    if not versions or getattr(versions[-1], "dat_deleted_at", None) is not None:
        return None
    return versions[-1]


def _id(raw: str) -> uuid.UUID:
    try:
        return uuid.UUID(raw)
    except (ValueError, AttributeError, TypeError):
        raise ValueError(f"not a dat_id (uuid): {raw!r}") from None


def _moment(raw: str) -> dt.datetime:
    try:
        at = dt.datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"as_of must be ISO 8601, got {raw!r}") from None
    hlc_bound(at)  # refuses a moment without a time zone, with its own message
    return at


def _pending(outcome: object) -> bool:
    return isinstance(outcome, CommittedPending)
