"""In-memory stores, branches and participants with fault injection.

A ``FakeStore`` behaves like one database: an open transaction buffers rows,
``prepare`` moves them to a prepared slot under a gid (2PC stores only),
``commit`` makes them visible. Non-2PC stores apply a staged payload after the
decision, deduplicated by ``dat_txn``. ``Faults`` raises at named points so
tests can fail or "crash" the coordinator anywhere.
"""

import json
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from datorium.core.errors import ErrorKind
from datorium.core.journal import BranchRecord, JournalRecord, MemoryJournal, State
from datorium.core.result import Result


class Transient(Exception):
    """A retryable store error (serialization failure, prepared fence, network)."""


class Permanent(Exception):
    """A non-retryable store error (constraint violation)."""


class Full(Exception):
    """The store's own "no space left" error (PostgreSQL 53100, ClickHouse 243)."""


class Crash(BaseException):
    """The coordinator process dies here. Not an ``Exception``: nothing may catch it."""


@dataclass
class Faults:
    """Raise ``error`` the first ``times`` times a point is reached."""

    plan: dict[str, tuple[BaseException, int]] = field(default_factory=dict)
    seen: list[str] = field(default_factory=list)

    def add(self, point: str, error: BaseException, times: int = 1) -> None:
        self.plan[point] = (error, times)

    def hit(self, point: str) -> None:
        self.seen.append(point)
        if point in self.plan:
            error, times = self.plan[point]
            if times > 0:
                self.plan[point] = (error, times - 1)
                raise error


@dataclass
class FakeStore:
    name: str
    two_phase: bool
    faults: Faults
    committed: dict[uuid.UUID, list[str]] = field(default_factory=dict)
    prepared: dict[str, tuple[uuid.UUID, list[str]]] = field(default_factory=dict)
    lose_commit_ack: int = 0  # commits that apply but then report a timeout
    free: int | None = None  # bytes left, as Capacity reports them (None: unknown)

    def rows_of(self, dat_txn: uuid.UUID) -> list[str] | None:
        return self.committed.get(dat_txn)

    def _apply(self, dat_txn: uuid.UUID, rows: list[str]) -> None:
        self.committed.setdefault(dat_txn, list(rows))  # idempotent
        if self.lose_commit_ack > 0:
            self.lose_commit_ack -= 1
            raise TimeoutError(f"{self.name}: commit acknowledgement lost")


class FakeBranch:
    def __init__(self, store: FakeStore, rows: list[str]) -> None:
        self.db = store
        self.store = store.name
        self.two_phase = store.two_phase
        self.rows = rows
        self.state = "open"
        self.gid: str | None = None
        self.dat_txn: uuid.UUID | None = None

    def execute(self, row: str, params: object = None) -> None:
        """Buffer a row in this branch's open transaction."""
        self.db.faults.hit(f"{self.store}.execute")
        self.rows.append(row)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        self.db.faults.hit(f"{self.store}.stage")
        self.dat_txn = dat_txn
        if self.two_phase:
            return None
        return json.dumps(self.rows).encode()

    def prepare(self, gid: str) -> None:
        assert self.two_phase
        assert self.dat_txn is not None
        self.db.faults.hit(f"{self.store}.prepare")
        self.db.prepared[gid] = (self.dat_txn, self.rows)
        self.gid = gid
        self.state = "prepared"
        self.db.faults.hit(f"{self.store}.prepare.after")

    def commit_one_phase(self) -> None:
        assert self.dat_txn is not None
        self.db.faults.hit(f"{self.store}.commit_one_phase")
        self.db._apply(self.dat_txn, self.rows)
        self.state = "committed"

    def rollback(self) -> None:
        self.db.faults.hit(f"{self.store}.rollback")
        if self.gid is not None:
            self.db.prepared.pop(self.gid, None)
        self.state = "rolled back"

    def is_retryable(self, error: BaseException) -> bool:
        return isinstance(error, Transient | TimeoutError)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)


def _classify(error: BaseException) -> ErrorKind:
    if isinstance(error, Full):
        return ErrorKind.STORE_FULL
    if isinstance(error, Transient | TimeoutError):
        return ErrorKind.TRANSIENT
    return ErrorKind.PERMANENT


class FakeParticipant:
    def __init__(self, store: FakeStore) -> None:
        self.db = store

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        self.db.faults.hit(f"{self.db.name}.finish")
        if branch.two_phase:
            entry = self.db.prepared.pop(branch.gid, None)
            if entry is None:
                return  # unknown gid: already resolved
            self.db._apply(dat_txn, entry[1])
        else:
            assert branch.payload is not None
            self.db._apply(dat_txn, json.loads(branch.payload))
        self.db.faults.hit(f"{self.db.name}.finish.after")

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        self.db.faults.hit(f"{self.db.name}.is_applied")
        return dat_txn in self.db.committed

    def abort(self, branch: BranchRecord) -> None:
        self.db.faults.hit(f"{self.db.name}.abort")
        self.db.prepared.pop(branch.gid, None)

    def prepared_gids(self) -> list[str]:
        return list(self.db.prepared)

    def is_retryable(self, error: BaseException) -> bool:
        return isinstance(error, Transient | TimeoutError)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)


class FakeCapacity:
    """``Capacity`` of a fake store: reports ``FakeStore.free``, or raises ``fail``."""

    def __init__(self, store: FakeStore, fail: BaseException | None = None) -> None:
        self.db = store
        self.fail = fail

    def free_space(self) -> int | None:
        if self.fail is not None:
            raise self.fail
        return self.db.free


@dataclass
class World:
    """Several fake stores sharing one fault plan."""

    faults: Faults = field(default_factory=Faults)
    stores: dict[str, FakeStore] = field(default_factory=dict)

    def store(self, name: str, *, two_phase: bool) -> FakeStore:
        self.stores[name] = FakeStore(name, two_phase, self.faults)
        return self.stores[name]

    def capacity(self) -> dict[str, FakeCapacity]:
        return {name: FakeCapacity(s) for name, s in self.stores.items()}

    def participants(self) -> dict[str, FakeParticipant]:
        return {name: FakeParticipant(s) for name, s in self.stores.items()}

    def branches(
        self, rows: Callable[[str], list[str]] = lambda s: [f"{s}-row"]
    ) -> list[FakeBranch]:
        return [FakeBranch(s, rows(name)) for name, s in self.stores.items()]

    def applied(self, dat_txn: uuid.UUID) -> dict[str, bool]:
        return {name: s.rows_of(dat_txn) is not None for name, s in self.stores.items()}

    def leftover_prepared(self) -> dict[str, list[str]]:
        return {name: list(s.prepared) for name, s in self.stores.items() if s.prepared}


class FaultyJournal(MemoryJournal):
    """MemoryJournal with fault points ``journal.<state>`` and ``journal.<state>.after``."""

    def __init__(self, faults: Faults, wall_ms: Callable[[], int]) -> None:
        super().__init__(wall_ms=wall_ms)
        self.faults = faults

    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord:
        self.faults.hit("journal.ACTIVE")
        record = super().begin(dat_txn, branches)
        self.faults.hit("journal.ACTIVE.after")
        return record

    def transition(
        self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
    ) -> JournalRecord:
        self.faults.hit(f"journal.{new}")
        record = super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)
        self.faults.hit(f"journal.{new}.after")
        return record


class FakeStoreHandle:
    """A FakeStore behind the ``Store`` protocol, as the facade sees real stores."""

    def __init__(self, store: FakeStore) -> None:
        self.db = store
        self.name = store.name
        self.two_phase = store.two_phase
        self.closed = False

    def branch(self, dat_txn: uuid.UUID) -> FakeBranch:
        self.db.faults.hit(f"{self.name}.begin")
        return FakeBranch(self.db, [])

    def participant(self) -> FakeParticipant:
        return FakeParticipant(self.db)

    def free_space(self) -> int | None:
        return self.db.free

    def execute(self, query: str, params: object = None) -> Result:
        rows = [{"row": row} for rows in self.db.committed.values() for row in rows]
        return Result(iter(rows), columns=("row",))

    def close(self) -> None:
        self.closed = True
