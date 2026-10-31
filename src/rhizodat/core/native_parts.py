"""Entity parts on the native core: ``rhizodat_native`` stores behind the
repository's part protocols.

A native facade's repositories run the same logic as the Python facade's
(:mod:`rhizodat.core.repository`); only the parts move to the core.
:class:`NativePartStore` is a :class:`~rhizodat.core.parts.PartStore` (and
``ScansParts``) over a native ``PostgresStore`` or ``DrevoStore``: committed
versions are read by the Rust adapter. :class:`NativePartBranch` is a
:class:`~rhizodat.core.parts.PartBranch` over a Unit of Work's native unit: writes
are buffered on the unit and applied inside the meta-transaction when it commits;
a read sees the unit's own writes first, then the committed versions.

The layout is the Python adapters' (PostgreSQL tables with the same DDL and
``jsonb`` encoding; drevo nodes with the same properties and reference anchors),
so either facade reads the other's versions. Parts live in PostgreSQL and drevo
on this path so far; ClickHouse and Iceberg keep theirs on the Python facade.
"""

import uuid
from collections.abc import Mapping
from types import ModuleType
from typing import Any

from rhizodat.adapters import drevo, postgres
from rhizodat.core.parts import Part, Version

__all__ = ["NativePartBranch", "NativePartStore", "part_spec"]


def part_spec(native: ModuleType, part: Part, store: Any = None) -> Any:  # noqa: ANN401 - specs
    """``part`` as the spec of ``store``'s kind: a ``rhizodat_native.PartSpec`` typed like
    the PostgreSQL adapter's table (the default), or a ``DrevoPartSpec`` stored like
    the drevo adapter's nodes."""
    if isinstance(store, native.DrevoStore):
        return native.DrevoPartSpec(part.type_name, part.table, drevo.part_fields(part))
    return native.PartSpec(part.type_name, part.table, postgres.part_columns(part))


def _version(raw: Mapping[str, Any], dat_txn: uuid.UUID | None = None) -> Version:
    return Version(
        dat_txn=raw["dat_txn"] if dat_txn is None else dat_txn,
        dat_tx_time=raw.get("dat_tx_time"),
        created=raw["created"],
        deleted=raw["deleted"],
        values=raw["values"],
    )


class NativePartStore:
    """Committed parts in a native ``PostgresStore`` or ``DrevoStore``."""

    def __init__(self, native: ModuleType, store: Any) -> None:  # noqa: ANN401 - native store
        if not isinstance(store, native.PostgresStore | native.DrevoStore):
            raise NotImplementedError(
                f"entity parts in a {type(store).__name__} are not on the native core path"
                " yet (PostgreSQL and drevo only); use the Python facade for this type"
            )
        self._native = native
        self._store = store
        self._specs: dict[Part, Any] = {}

    def spec(self, part: Part) -> Any:  # noqa: ANN401 - rhizodat_native.PartSpec
        spec = self._specs.get(part)
        if spec is None:
            spec = self._specs[part] = part_spec(self._native, part, self._store)
        return spec

    def install_part(self, part: Part) -> None:
        self._store.install_part(self.spec(part))

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        raw = self._store.read_part(self.spec(part), dat_id, known_at=known_at)
        return None if raw is None else _version(raw)

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        return [_version(raw) for raw in self._store.part_history(self.spec(part), dat_id)]

    def part_versions(
        self, part: Part, *, known_at: int | None = None
    ) -> list[tuple[uuid.UUID, Version]]:
        versions = self._store.part_versions(self.spec(part), known_at=known_at)
        return [(dat_id, _version(raw)) for dat_id, raw in versions]

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        ids: list[uuid.UUID] = self._store.expired_parts(
            self.spec(part), deleted_before=deleted_before, limit=limit
        )
        return ids


class NativePartBranch:
    """One Unit of Work's parts on a store's native unit."""

    def __init__(
        self,
        unit: Any,  # noqa: ANN401 - the store's native unit
        parts: NativePartStore,
        dat_txn: uuid.UUID,
    ) -> None:
        self._unit = unit
        self._parts = parts
        self._dat_txn = dat_txn

    def write_part(
        self,
        part: Part,
        dat_id: uuid.UUID,
        values: Mapping[str, Any],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        self._unit.write_part(
            self._parts.spec(part), dat_id, dict(values), created=created, deleted=deleted
        )

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        self._unit.erase_part(self._parts.spec(part), dat_id)

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        """This Unit of Work's own latest version, else the latest committed one."""
        touched, own = self._unit.own_part(self._parts.spec(part), dat_id)
        if not touched:
            return self._parts.read_part(part, dat_id)
        return None if own is None else _version(own, self._dat_txn)
