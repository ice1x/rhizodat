"""Repository: entity CRUD across the mapped stores.

Each store keeps its part of an entity (``rhizodat.core.parts``). A write adds a
version of **every** part in one Unit of Work, so the parts of one version always
share a ``dat_txn``; a read assembles the parts of one version.

- ``Repository`` works inside a Unit of Work (``uow.repository(T)``): its writes
  commit with the rest of the work, its reads see them.
- ``EntityRepository`` is the facade's (``dat.repository(T)``): each write is
  one Unit of Work of its own, with retries; reads go to the committed data and
  take ``as_of`` (transaction time).

Delete is a hard delete, erasing every version in every store, unless the type
is declared with ``soft_delete``: then it adds a version marked deleted, hidden
from reads, restorable within the window and erased by ``purge_expired`` after.
"""

import dataclasses
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from rhizodat.core.coordinator import Committed, CommittedPending
from rhizodat.core.mapping import EntityMapping, mapping_of
from rhizodat.core.parts import (
    VALIDITY,
    Part,
    PartBranch,
    PartStore,
    Version,
    from_storage,
    hlc_bound,
    hlc_datetime,
    to_storage,
)

if TYPE_CHECKING:
    from rhizodat.api import Rhizodat
    from rhizodat.core.unit_of_work import UnitOfWork

__all__ = ["EntityExists", "EntityNotFound", "EntityRepository", "NotRestorable", "Repository"]

Outcome = Committed | CommittedPending

_MS = 1 << 16  # one millisecond in HLC units
_CONSISTENT_READ_ATTEMPTS = 8


class EntityExists(Exception):
    """``add`` of an entity that already has versions (live or soft-deleted)."""


class EntityNotFound(LookupError):
    """The entity has no live version."""


class NotRestorable(Exception):
    """``restore`` of an entity that is not soft-deleted, or left its window."""


@dataclass(frozen=True, slots=True)
class _Snapshot:
    """The parts of one version of one entity, by store."""

    dat_id: uuid.UUID
    versions: Mapping[str, Version]

    @property
    def any(self) -> Version:
        return next(iter(self.versions.values()))


class _Kind:
    """What a repository needs to know about one entity type."""

    def __init__(self, entity_type: type, window: timedelta) -> None:
        self.mapping: EntityMapping = mapping_of(entity_type)
        self.parts = {store: Part.of(entity_type, store) for store in self.mapping.stores}
        self.window = self.mapping.soft_delete_window or window

    @property
    def window_hlc(self) -> int:
        return int(self.window / timedelta(milliseconds=1)) * _MS

    def split(self, obj: object) -> dict[str, dict[str, Any]]:
        if not isinstance(obj, self.mapping.cls):
            raise TypeError(f"expected a {self.mapping.type_name}, got {type(obj).__name__}")
        validity = self._validity(obj)
        return {
            store: {f: to_storage(v) for f, v in values.items()} | validity
            for store, values in self.mapping.split(obj).items()
        }

    def _validity(self, obj: object) -> dict[str, Any]:
        """The period ``obj`` holds for, checked; empty for a type without valid time."""
        if not self.mapping.valid_time:
            return {}
        start, end = obj.valid_from, obj.valid_to  # type: ignore[attr-defined]
        for name, at in (("valid_from", start), ("valid_to", end)):
            if at is not None and (not isinstance(at, datetime) or at.tzinfo is None):
                raise ValueError(
                    f"{self.mapping.type_name}.{name} needs a time zone (e.g. UTC), got {at!r}"
                )
        if start is not None and end is not None and start >= end:
            raise ValueError(
                f"{self.mapping.type_name}: valid_from must be before valid_to"
                f" ({start.isoformat()} >= {end.isoformat()})"
            )
        return {"valid_from": start, "valid_to": end}

    def holds_at(self, snap: "_Snapshot", at: datetime) -> bool:
        """Whether the version's period contains ``at`` (from inclusive, to exclusive)."""
        values = snap.any.values
        start, end = values.get("valid_from"), values.get("valid_to")
        return (start is None or start <= at) and (end is None or at < end)

    def assemble(self, snap: _Snapshot) -> object:
        parts = {
            store: {
                f: from_storage(v, ref=f in self.parts[store].refs)
                for f, v in version.values.items()
                if f not in VALIDITY
            }
            for store, version in snap.versions.items()
        }
        v = snap.any
        updated = v.dat_tx_time
        validity = {f: v.values.get(f) for f in VALIDITY} if self.mapping.valid_time else {}
        return self.mapping.assemble(
            snap.dat_id,
            parts,
            **validity,
            dat_created_at=hlc_datetime(v.created) if v.created is not None else None,
            dat_updated_at=hlc_datetime(updated) if updated is not None else None,
            dat_deleted_at=hlc_datetime(updated) if v.deleted and updated is not None else None,
        )

    def stamped(self, obj: object, created: int | None, at: int | None) -> object:
        """``obj`` with the times of the version written at ``at``."""
        if at is None:
            return obj
        values = {f: getattr(obj, f) for f in self.mapping.fields}
        extra = {}
        if self.mapping.valid_time:
            extra = {"valid_from": obj.valid_from, "valid_to": obj.valid_to}  # type: ignore[attr-defined]
        return self.mapping.cls(
            dat_id=obj.dat_id,  # type: ignore[attr-defined]
            **values,
            **extra,
            dat_created_at=hlc_datetime(created if created is not None else at),
            dat_updated_at=hlc_datetime(at),
        )


class Repository[T]:
    """Entity reads and writes inside one Unit of Work."""

    def __init__(self, uow: "UnitOfWork", kind: _Kind, now: Callable[[], int]) -> None:
        self._uow = uow
        self._kind = kind
        self._now = now
        self.created: dict[uuid.UUID, int | None] = {}
        """Creation time of each entity this repository wrote; ``None``: created now."""

    # --- reads ---------------------------------------------------------------------------

    def get(self, dat_id: uuid.UUID, *, include_deleted: bool = False) -> T | None:
        """The latest version, this Unit of Work's writes included."""
        snap = self._latest(dat_id)
        if snap is None or (snap.any.deleted and not include_deleted):
            return None
        return cast("T", self._kind.assemble(snap))

    # --- writes --------------------------------------------------------------------------

    def add(self, obj: T) -> None:
        dat_id = _id_of(obj)
        if self._latest(dat_id) is not None:
            raise EntityExists(f"{self._kind.mapping.type_name} {dat_id} already exists")
        self._write(dat_id, self._kind.split(obj), created=None)

    def update(self, obj: T) -> None:
        dat_id = _id_of(obj)
        snap = self._live(dat_id)
        self._write(dat_id, self._kind.split(obj), created=snap.any.created)

    def save(self, obj: T) -> None:
        """``add`` or ``update``, whichever applies."""
        dat_id = _id_of(obj)
        snap = self._latest(dat_id)
        if snap is not None and snap.any.deleted:
            raise EntityExists(
                f"{self._kind.mapping.type_name} {dat_id} is soft-deleted: restore it first"
            )
        created = snap.any.created if snap is not None else None
        self._write(dat_id, self._kind.split(obj), created=created)

    def delete(self, dat_id: uuid.UUID) -> None:
        snap = self._live(dat_id)
        if self._kind.mapping.soft_delete:
            self._write(dat_id, _values(snap), created=snap.any.created, deleted=True)
        else:
            self._erase(dat_id)

    def restore(self, dat_id: uuid.UUID) -> None:
        name = f"{self._kind.mapping.type_name} {dat_id}"
        if not self._kind.mapping.soft_delete:
            raise NotRestorable(f"{name}: the type has no soft delete; a delete erases it")
        snap = self._latest(dat_id)
        if snap is None:
            raise NotRestorable(f"{name} does not exist (or was purged)")
        version = snap.any
        if not version.deleted:
            raise NotRestorable(f"{name} is not deleted")
        deleted_at = version.dat_tx_time
        if deleted_at is not None and self._now() - deleted_at > self._kind.window_hlc:
            raise NotRestorable(f"{name} left its restore window of {self._kind.window}")
        self._write(dat_id, _values(snap), created=version.created)

    def purge(self, dat_id: uuid.UUID, *, deleted_before: int) -> bool:
        """Erase a soft-deleted entity if it is still deleted and old enough."""
        snap = self._latest(dat_id)
        if snap is None or not snap.any.deleted:
            return False
        at = snap.any.dat_tx_time
        if at is None or at >= deleted_before:
            return False
        self._erase(dat_id)
        return True

    # --- internals -----------------------------------------------------------------------

    def _branch(self, store: str) -> PartBranch:
        branch = self._uow.store(store)
        if not isinstance(branch, PartBranch):
            raise TypeError(f"store {store!r} cannot hold entity parts (no PartBranch)")
        return branch

    def _latest(self, dat_id: uuid.UUID) -> _Snapshot | None:
        versions = {
            store: v
            for store, part in self._kind.parts.items()
            if (v := self._branch(store).read_part(part, dat_id)) is not None
        }
        return _Snapshot(dat_id, versions) if len(versions) == len(self._kind.parts) else None

    def _live(self, dat_id: uuid.UUID) -> _Snapshot:
        snap = self._latest(dat_id)
        if snap is None or snap.any.deleted:
            raise EntityNotFound(f"{self._kind.mapping.type_name} {dat_id} not found")
        return snap

    def _write(
        self,
        dat_id: uuid.UUID,
        parts: Mapping[str, Mapping[str, Any]],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        for store, part in self._kind.parts.items():
            self._branch(store).write_part(
                part, dat_id, parts[store], created=created, deleted=deleted
            )
        self.created[dat_id] = created

    def _erase(self, dat_id: uuid.UUID) -> None:
        for store, part in self._kind.parts.items():
            self._branch(store).erase_part(part, dat_id)
        self.created.pop(dat_id, None)


class EntityRepository[T]:
    """``dat.repository(T)``: committed reads, and one Unit of Work per write."""

    def __init__(self, dat: "Rhizodat", entity_type: type[T], kind: _Kind) -> None:
        self._dat = dat
        self._type = entity_type
        self._kind = kind

    # --- reads ---------------------------------------------------------------------------

    def get(
        self,
        dat_id: uuid.UUID,
        *,
        as_of: datetime | None = None,
        known_at: datetime | None = None,
        include_deleted: bool = False,
    ) -> T | None:
        """The entity as known at ``known_at`` (now if ``None``).

        For a type with valid time, the version that holds at ``as_of`` (now if
        ``None``): among the versions known then, the latest one whose period
        contains it. Without valid time ``as_of`` and ``known_at`` both name the
        moment of knowledge; give one of them.
        """
        if not self._kind.mapping.valid_time:
            if as_of is not None and known_at is not None:
                raise ValueError(
                    f"{self._kind.mapping.type_name} has no valid time:"
                    " as_of and known_at are the same moment, give one"
                )
            at = as_of if as_of is not None else known_at
            snap = self._consistent(dat_id, None if at is None else hlc_bound(at))
            if snap is None or (snap.any.deleted and not include_deleted):
                return None
            return cast("T", self._kind.assemble(snap))
        known = self._snapshots(dat_id, None if known_at is None else hlc_bound(known_at))
        if not known or (known[-1].any.deleted and not include_deleted):
            return None
        valid_at = as_of if as_of is not None else datetime.now(UTC)
        hlc_bound(valid_at)  # refuses a moment without a time zone
        holding = [s for s in known if self._kind.holds_at(s, valid_at)]
        return cast("T", self._kind.assemble(holding[-1])) if holding else None

    def history(self, dat_id: uuid.UUID) -> list[T]:
        """Every committed version, oldest first (soft deletes included)."""
        return [cast("T", self._kind.assemble(s)) for s in self._snapshots(dat_id, None)]

    def _snapshots(self, dat_id: uuid.UUID, known_at: int | None) -> list[_Snapshot]:
        """Every version every store has (and knew at ``known_at``), oldest first."""
        by_txn: dict[uuid.UUID, dict[str, Version]] = {}
        for store, part in self._kind.parts.items():
            for v in self._store(store).part_history(part, dat_id):
                if known_at is None or (v.dat_tx_time is not None and v.dat_tx_time <= known_at):
                    by_txn.setdefault(v.dat_txn, {})[store] = v
        complete = [
            _Snapshot(dat_id, parts)
            for parts in by_txn.values()
            if len(parts) == len(self._kind.parts)
        ]
        complete.sort(key=lambda s: s.any.dat_tx_time or 0)
        return complete

    # --- writes --------------------------------------------------------------------------

    def add(self, obj: T) -> Outcome:
        return self._write(lambda repo: repo.add(obj), obj)

    def update(self, obj: T) -> Outcome:
        return self._write(lambda repo: repo.update(obj), obj)

    def save(self, obj: T) -> Outcome:
        return self._write(lambda repo: repo.save(obj), obj)

    def delete(self, dat_id: uuid.UUID) -> Outcome:
        outcome, _, _ = self._run(lambda repo: repo.delete(dat_id))
        return outcome

    def restore(self, dat_id: uuid.UUID) -> Outcome:
        """Bring a soft-deleted entity back; ``value`` is the restored entity."""

        def work(repo: Repository[T]) -> T:
            repo.restore(dat_id)
            restored = repo.get(dat_id)
            assert restored is not None
            return restored

        outcome, restored, created = self._run(work)
        return _with_value(
            outcome, self._kind.stamped(restored, created.get(dat_id), outcome.dat_tx_time)
        )

    def purge_expired(self, *, limit: int = 1000) -> int:
        """Erase soft-deleted entities whose window is over; how many were erased."""
        if not self._kind.mapping.soft_delete:
            return 0
        before = self._dat.clock.now().encode() - self._kind.window_hlc
        ids: set[uuid.UUID] = set()
        for store, part in self._kind.parts.items():
            ids.update(self._store(store).expired_parts(part, deleted_before=before, limit=limit))
        if not ids:
            return 0

        def work(repo: Repository[T]) -> int:
            return sum(repo.purge(i, deleted_before=before) for i in sorted(ids)[:limit])

        _, purged, _ = self._run(work)
        return cast("int", purged)

    # --- internals -----------------------------------------------------------------------

    def _store(self, name: str) -> PartStore:
        return cast("PartStore", self._dat.store(name))

    def _run(
        self, work: Callable[[Repository[T]], Any]
    ) -> tuple[Outcome, Any, dict[uuid.UUID, int | None]]:
        """Run ``work`` as one Unit of Work: the outcome (``value`` cleared), what
        ``work`` returned and the creation times of what it wrote."""

        def unit(uow: "UnitOfWork") -> tuple[Any, dict[uuid.UUID, int | None]]:
            repo = uow.repository(self._type)
            return work(repo), dict(repo.created)

        outcome = self._dat.transactional(unit)()
        value, created = cast("tuple[Any, dict[uuid.UUID, int | None]]", outcome.value)
        return _with_value(outcome, None), value, created

    def _write(self, work: Callable[[Repository[T]], None], obj: T) -> Outcome:
        outcome, _, created = self._run(work)
        stamped = self._kind.stamped(obj, created.get(_id_of(obj)), outcome.dat_tx_time)
        return _with_value(outcome, stamped)

    def _consistent(self, dat_id: uuid.UUID, known_at: int | None) -> _Snapshot | None:
        """The parts of one version. Stores finish a meta-transaction one after
        another, so a part may already show a newer version than another: read
        again at the oldest of the latest times until every part agrees."""
        for _ in range(_CONSISTENT_READ_ATTEMPTS):
            versions = {
                store: self._store(store).read_part(part, dat_id, known_at=known_at)
                for store, part in self._kind.parts.items()
            }
            found = {s: v for s, v in versions.items() if v is not None}
            if not found:
                return None
            if len(found) < len(versions):
                return None  # a hard delete or a first write that is still finishing
            txns = {v.dat_txn for v in found.values()}
            if len(txns) == 1:
                return _Snapshot(dat_id, found)
            known_at = min(v.dat_tx_time or 0 for v in found.values())
        return None


def _id_of(obj: object) -> uuid.UUID:
    dat_id = getattr(obj, "dat_id", None)
    if not isinstance(dat_id, uuid.UUID):
        raise TypeError(f"not an entity: {type(obj).__name__}")
    return dat_id


def _values(snap: _Snapshot) -> dict[str, Mapping[str, Any]]:
    return {store: v.values for store, v in snap.versions.items()}


def _with_value(outcome: Outcome, value: object) -> Outcome:
    return dataclasses.replace(outcome, value=value)
