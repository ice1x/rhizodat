"""Entity parts: what a store keeps of an entity, and how the repository asks for it.

An entity's fields are split by store (``@entity(stores=...)``); each store's
share is a **part**. A store keeps every version of a part: one per write,
tagged with the meta-transaction (``dat_txn``) and its transaction time
(``dat_tx_time``, HLC). A soft delete is one more version marked ``deleted``;
a hard delete erases every version.

Stores join by implementing two capabilities, checked with ``isinstance``:

- ``PartBranch`` on a branch: writes inside a Unit of Work, and reads that see
  them. ``dat_tx_time`` is only known at stage, so the branch fills it (and a
  missing creation time) then;
- ``PartStore`` on a store: committed reads outside any Unit of Work;
- ``ScansParts`` on a store: every entity's committed versions at once, for
  searches over all entities of a type (``similar``).

Values reach the store as plain data: references are their ``dat_id``.
"""

import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from rhizodat.core.mapping import Ref, mapping_of

__all__ = [
    "VALIDITY",
    "Part",
    "PartBranch",
    "PartStore",
    "ScansParts",
    "Version",
    "from_storage",
    "hlc_bound",
    "hlc_datetime",
    "to_storage",
]

VALIDITY = ("valid_from", "valid_to")
"""The validity fields every part of a valid-time type carries."""

_LOGICAL_BITS = 16
_NOT_IDENT = re.compile(r"[^a-z0-9_]")


@dataclass(frozen=True, slots=True)
class Part:
    """One store's share of one entity type."""

    type_name: str
    store: str
    fields: tuple[str, ...]
    types: Mapping[str, Any] = field(default_factory=dict, compare=False)
    """Each field's resolved type; ``object`` when unknown."""
    refs: frozenset[str] = frozenset()

    @classmethod
    def of(cls, entity_type: type, store: str) -> "Part":
        mapping = mapping_of(entity_type)
        if store not in mapping.stores:
            raise KeyError(f"{mapping.type_name} has no part in store {store!r}")
        types = mapping.field_types()
        fields = mapping.stores[store]
        field_types = {f: types[f] for f in fields}
        if mapping.valid_time:  # every part carries the period its version holds for
            fields = (*fields, *VALIDITY)
            field_types |= dict.fromkeys(VALIDITY, datetime | None)
        return cls(
            type_name=mapping.type_name,
            store=store,
            fields=fields,
            types=field_types,
            refs=frozenset(f for f in fields if f in mapping.refs),
        )

    @property
    def table(self) -> str:
        """A storage name derived from the type name: ``tracker.Issue`` → ``tracker_issue``."""
        name = _NOT_IDENT.sub("_", self.type_name.lower())
        if not name[:1].isalpha():
            name = f"e_{name}"
        return name[:63]


@dataclass(frozen=True, slots=True)
class Version:
    """One stored version of one part."""

    dat_txn: uuid.UUID
    dat_tx_time: int | None
    """HLC; ``None`` for a write of the reading branch that is not staged yet."""
    created: int | None
    """Transaction time of the entity's first version."""
    deleted: bool
    values: Mapping[str, Any]


@runtime_checkable
class PartBranch(Protocol):
    def write_part(
        self,
        part: Part,
        dat_id: uuid.UUID,
        values: Mapping[str, Any],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        """Add a version. ``created=None``: this version is the first (its own time)."""
        ...

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        """Physically remove every version of the part."""
        ...

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        """The latest version, this branch's own writes included."""
        ...


@runtime_checkable
class PartStore(Protocol):
    def install_part(self, part: Part) -> None:
        """Create what the part needs (table, indexes) if missing. Idempotent."""
        ...

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        """The latest committed version with ``dat_tx_time <= known_at`` (any, if ``None``)."""
        ...

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        """Every committed version, oldest first."""
        ...

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        """Entities whose latest version is a soft delete older than ``deleted_before``."""
        ...


@runtime_checkable
class ScansParts(Protocol):
    def part_versions(
        self, part: Part, *, known_at: int | None = None
    ) -> list[tuple[uuid.UUID, Version]]:
        """Every committed version of every entity with ``dat_tx_time <= known_at``
        (all, if ``None``), oldest first."""
        ...


# --- time -------------------------------------------------------------------------------


def hlc_bound(at: datetime) -> int:
    """The largest HLC timestamp within ``at``'s millisecond: ``as_of`` is inclusive."""
    if at.tzinfo is None:
        raise ValueError("as_of / known_at need a time zone (e.g. datetime.now(UTC))")
    ms = int(at.timestamp() * 1000)
    return (ms << _LOGICAL_BITS) | ((1 << _LOGICAL_BITS) - 1)


def hlc_datetime(ts: int) -> datetime:
    """The wall-clock time of an HLC timestamp, in UTC."""
    return datetime.fromtimestamp((ts >> _LOGICAL_BITS) / 1000, UTC)


# --- values -----------------------------------------------------------------------------


def to_storage(value: Any) -> Any:  # noqa: ANN401 - any field value
    """References become their ``dat_id``, inside lists and dicts too."""
    if isinstance(value, Ref):
        return value.dat_id
    if isinstance(value, list | tuple):
        return [to_storage(v) for v in value]
    if isinstance(value, dict):
        return {k: to_storage(v) for k, v in value.items()}
    return value


def from_storage(value: Any, *, ref: bool) -> Any:  # noqa: ANN401 - any stored value
    """The inverse of ``to_storage``: in a reference field every id is a ``Ref``."""
    if not ref:
        return value
    if isinstance(value, uuid.UUID):
        return Ref(value)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [from_storage(v, ref=True) for v in value]
    if isinstance(value, Mapping):
        return {k: from_storage(v, ref=True) for k, v in value.items()}
    return value
