"""An in-memory two-phase store that holds entity parts, for tests of code on datorium.

``MemoryEntityStore`` implements ``Store`` with ``PartStore``, and its branches
``PartBranch``: what a branch writes is invisible to others until it commits.
``hold_finish`` keeps prepared work invisible after the decision until
``release``, to test reads while a meta-transaction is still finishing.
"""

import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from datorium.core.journal import BranchRecord
from datorium.core.parts import Part, Version

__all__ = ["MemoryEntityStore"]

_Key = tuple[str, uuid.UUID]  # (part table, dat_id)


@dataclass(frozen=True, slots=True)
class _Op:
    key: _Key
    version: Version | None  # None: erase every version


class MemoryEntityStore:
    two_phase = True

    def __init__(self, name: str = "memory") -> None:
        self.name = name
        self.hold_finish = False
        self._lock = threading.Lock()
        self._versions: dict[_Key, list[Version]] = {}
        self._prepared: dict[str, list[_Op]] = {}
        self._held: list[list[_Op]] = []
        self._applied: set[uuid.UUID] = set()
        self.installed: set[str] = set()

    # --- Store -------------------------------------------------------------------------

    def branch(self, dat_txn: uuid.UUID) -> "_Branch":
        return _Branch(self, dat_txn)

    def participant(self) -> "_Participant":
        return _Participant(self)

    def execute(self, query: str, params: Any = None) -> Any:  # noqa: ANN401 - unused
        raise NotImplementedError("MemoryEntityStore has no query language")

    def close(self) -> None:
        pass

    # --- PartStore ---------------------------------------------------------------------

    def install_part(self, part: Part) -> None:
        self.installed.add(part.table)

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        with self._lock:
            versions = [
                v
                for v in self._versions.get((part.table, dat_id), [])
                if known_at is None or (v.dat_tx_time or 0) <= known_at
            ]
        return versions[-1] if versions else None

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        with self._lock:
            return list(self._versions.get((part.table, dat_id), []))

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        with self._lock:
            found = [
                dat_id
                for (table, dat_id), versions in self._versions.items()
                if table == part.table
                and versions
                and versions[-1].deleted
                and (versions[-1].dat_tx_time or 0) < deleted_before
            ]
        return found[:limit]

    # --- for tests ---------------------------------------------------------------------

    def release(self) -> None:
        """Apply what ``hold_finish`` kept back."""
        with self._lock:
            held, self._held = self._held, []
        self.hold_finish = False
        for ops in held:
            self._apply(ops)

    def _apply(self, ops: list[_Op]) -> None:
        with self._lock:
            for op in ops:
                if op.version is None:
                    self._versions.pop(op.key, None)
                else:
                    self._versions.setdefault(op.key, []).append(op.version)
                    self._versions[op.key].sort(key=lambda v: v.dat_tx_time or 0)

    def _finish(self, ops: list[_Op]) -> None:
        if self.hold_finish:
            with self._lock:
                self._held.append(ops)
            return
        self._apply(ops)


class _Branch:
    two_phase = True

    def __init__(self, db: MemoryEntityStore, dat_txn: uuid.UUID) -> None:
        self.db = db
        self.store = db.name
        self.dat_txn = dat_txn
        self._ops: list[_Op] = []
        self._gid: str | None = None

    def write_part(
        self,
        part: Part,
        dat_id: uuid.UUID,
        values: Mapping[str, Any],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        unknown = set(values) - set(part.fields)
        if unknown:
            raise ValueError(f"{part.type_name}: {sorted(unknown)} are not fields of this part")
        version = Version(self.dat_txn, None, created, deleted, dict(values))
        self._ops.append(_Op((part.table, dat_id), version))

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        self._ops.append(_Op((part.table, dat_id), None))

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        key = (part.table, dat_id)
        latest = self.db.read_part(part, dat_id)
        for op in self._ops:
            if op.key == key:
                latest = op.version
        return latest

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        self._ops = [
            op
            if op.version is None
            else _Op(
                op.key,
                replace(
                    op.version,
                    dat_tx_time=dat_tx_time,
                    created=op.version.created if op.version.created is not None else dat_tx_time,
                ),
            )
            for op in self._ops
        ]
        return None

    def prepare(self, gid: str) -> None:
        with self.db._lock:
            self.db._prepared[gid] = self._ops
        self._gid = gid

    def commit_one_phase(self) -> None:
        self.db._applied.add(self.dat_txn)
        self.db._finish(self._ops)

    def rollback(self) -> None:
        if self._gid is not None:
            with self.db._lock:
                self.db._prepared.pop(self._gid, None)
        self._ops = []

    def is_retryable(self, error: BaseException) -> bool:
        return False


class _Participant:
    def __init__(self, db: MemoryEntityStore) -> None:
        self.db = db

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        with self.db._lock:
            ops = self.db._prepared.pop(branch.gid, None)
        if ops is not None:
            self.db._applied.add(dat_txn)
            self.db._finish(ops)

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        return dat_txn in self.db._applied

    def abort(self, branch: BranchRecord) -> None:
        with self.db._lock:
            self.db._prepared.pop(branch.gid, None)

    def prepared_gids(self) -> list[str]:
        with self.db._lock:
            return list(self.db._prepared)

    def is_retryable(self, error: BaseException) -> bool:
        return False
