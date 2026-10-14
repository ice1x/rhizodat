"""The contract every store adapter must pass, run against the real store.

datorium's guarantees hold only if an adapter's ``Branch`` and ``Participant``
keep their promises: nothing is visible before the decision, ``finish`` is
idempotent, ``abort`` really discards, and a crash at any step of the commit
protocol ends all-or-nothing after one recovery run. An adapter author writes a
``Harness`` for the store and subclasses ``AdapterContract``::

    from datorium.testing.contract import AdapterContract

    class TestMySqlContract(AdapterContract):
        @pytest.fixture
        def harness(self) -> Iterator[MySqlHarness]:
            h = MySqlHarness(...)
            yield h
            h.cleanup()

The crash tests commit one meta-transaction over the adapter's store and an
in-memory two-phase store, crash the coordinator at each step, simulate the
process dying (``Harness.abandon``) and run ``Recovery`` once.
"""

import contextlib
import errno
import uuid
from collections.abc import Callable, Iterator
from typing import Any, Protocol

import pytest

from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import Branch, Coordinator, Participant, Recovery, gid_for
from datorium.core.errors import Capacity, ErrorKind, classify
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, JournalRecord, MemoryJournal, State
from datorium.core.retry import Retry
from datorium.core.unit_of_work import Store

__all__ = ["CRASH_POINTS", "AdapterContract", "Crash", "Harness", "MemoryStore"]

TX_TIME = 1_760_000_000_000 << 16


class Harness(Protocol):
    """What the contract needs to know about one store kind."""

    def open_store(self, name: str) -> Store:
        """A store named ``name`` in a place of its own (schema, database, graph,
        namespace), installed and ready for ``write``. The harness cleans it up."""
        ...

    def write(self, branch: Branch, key: str) -> None:
        """Write one record identified by ``key`` in the branch's open transaction."""
        ...

    def count(self, store: Store, key: str) -> int:
        """Committed records with ``key``, read through native access."""
        ...

    # Optional: ``store_full_error(self) -> BaseException``, an instance of the error the
    # store's driver raises when the store has no room (PostgreSQL ``DiskFull``,
    # ClickHouse code 243). The contract then checks it is classified ``STORE_FULL``.


class Crash(BaseException):
    """The coordinator's process dies here (not an ``Exception``: nothing catches it)."""


def abandon(harness: Harness, branch: "_Probe") -> None:
    """What a dying process leaves: open transactions die with their connections,
    prepared ones and durable staged data stay. A harness may define
    ``abandon(branch)`` for a store that needs something else."""
    custom = getattr(harness, "abandon", None)
    if callable(custom):
        custom(branch.inner)
        return
    if branch.state == "open" or (branch.state == "staged" and branch.two_phase):
        branch.inner.rollback()


# --- an in-memory two-phase store, the second branch of the crash tests ---------------


class MemoryStore:
    """A two-phase store in memory: rows by key, prepared transactions by gid."""

    two_phase = True

    def __init__(self, name: str = "memory") -> None:
        self.name = name
        self.committed: dict[uuid.UUID, list[str]] = {}
        self.prepared: dict[str, tuple[uuid.UUID, list[str]]] = {}

    def branch(self, dat_txn: uuid.UUID) -> "_MemoryBranch":
        return _MemoryBranch(self, dat_txn)

    def participant(self) -> "_MemoryParticipant":
        return _MemoryParticipant(self)

    def execute(self, query: str, params: Any = None) -> Any:  # noqa: ANN401 - unused
        raise NotImplementedError

    def close(self) -> None:
        pass

    def count(self, key: str) -> int:
        return sum(rows.count(key) for rows in self.committed.values())


class _MemoryBranch:
    def __init__(self, db: MemoryStore, dat_txn: uuid.UUID) -> None:
        self.db = db
        self.store = db.name
        self.two_phase = True
        self.dat_txn = dat_txn
        self.rows: list[str] = []
        self.gid: str | None = None

    def write(self, key: str) -> None:
        self.rows.append(key)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        return None

    def prepare(self, gid: str) -> None:
        self.db.prepared[gid] = (self.dat_txn, self.rows)
        self.gid = gid

    def commit_one_phase(self) -> None:
        self.db.committed.setdefault(self.dat_txn, list(self.rows))

    def rollback(self) -> None:
        if self.gid is not None:
            self.db.prepared.pop(self.gid, None)

    def is_retryable(self, error: BaseException) -> bool:
        return False


class _MemoryParticipant:
    def __init__(self, db: MemoryStore) -> None:
        self.db = db

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        entry = self.db.prepared.pop(branch.gid, None)
        if entry is not None:
            self.db.committed.setdefault(dat_txn, list(entry[1]))

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        return dat_txn in self.db.committed

    def abort(self, branch: BranchRecord) -> None:
        self.db.prepared.pop(branch.gid, None)

    def prepared_gids(self) -> list[str]:
        return list(self.db.prepared)

    def is_retryable(self, error: BaseException) -> bool:
        return False


# --- fault points -----------------------------------------------------------------------


class _Faults:
    def __init__(self, point: str | None) -> None:
        self.point = point
        self.fired = False

    def hit(self, name: str) -> None:
        if name == self.point and not self.fired:
            self.fired = True
            raise Crash(name)


class _Probe:
    """A branch with fault points ``<store>.<step>`` and ``<store>.<step>.after``."""

    def __init__(self, inner: Branch, role: str, faults: _Faults) -> None:
        self.inner = inner
        self.store = inner.store
        self.two_phase = inner.two_phase
        self.role = role
        self.faults = faults
        self.state = "open"

    def _step(self, step: str, run: Callable[[], Any], state: str) -> Any:  # noqa: ANN401
        self.faults.hit(f"{self.role}.{step}")
        out = run()
        self.state = state
        self.faults.hit(f"{self.role}.{step}.after")
        return out

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        out: bytes | None = self._step(
            "stage", lambda: self.inner.stage(dat_txn, dat_tx_time), "staged"
        )
        return out

    def prepare(self, gid: str) -> None:
        self._step("prepare", lambda: self.inner.prepare(gid), "prepared")

    def commit_one_phase(self) -> None:
        self._step("commit_one_phase", self.inner.commit_one_phase, "committed")

    def rollback(self) -> None:
        self.inner.rollback()
        self.state = "rolled back"

    def is_retryable(self, error: BaseException) -> bool:
        return self.inner.is_retryable(error)


class _ProbeParticipant:
    def __init__(self, role: str, inner: Participant, faults: _Faults) -> None:
        self.role = role
        self.inner = inner
        self.faults = faults

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        self.faults.hit(f"{self.role}.finish")
        self.inner.finish(branch, dat_txn, dat_tx_time)
        self.faults.hit(f"{self.role}.finish.after")

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        return self.inner.is_applied(branch, dat_txn)

    def abort(self, branch: BranchRecord) -> None:
        self.inner.abort(branch)

    def prepared_gids(self) -> list[str]:
        return self.inner.prepared_gids()

    def is_retryable(self, error: BaseException) -> bool:
        return self.inner.is_retryable(error)


class _ProbeJournal(MemoryJournal):
    """Fault points ``journal.<STATE>`` (before the write) and ``journal.<STATE>.after``."""

    def __init__(self, faults: _Faults, wall_ms: Callable[[], int]) -> None:
        super().__init__(wall_ms=wall_ms)
        self.faults = faults

    def begin(self, dat_txn: uuid.UUID, branches: Any) -> JournalRecord:  # noqa: ANN401
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


class _Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


# The crash points of a meta-transaction over the adapter's store (role "store") and
# an in-memory two-phase store (role "memory"), in protocol order; None: no crash. Steps a
# store does not take (prepare in a one-phase store) never fire: the commit completes.
CRASH_POINTS: tuple[str | None, ...] = (
    "journal.ACTIVE",
    "journal.ACTIVE.after",
    "store.stage",
    "store.stage.after",
    "memory.prepare",
    "memory.prepare.after",
    "store.prepare",
    "store.prepare.after",
    "journal.PREPARED",
    "journal.PREPARED.after",
    "journal.COMMITTED",
    "journal.COMMITTED.after",
    "store.finish",
    "store.finish.after",
    "memory.finish",
    "memory.finish.after",
    "journal.DONE",
    "journal.DONE.after",
    None,
)


def _key() -> str:
    return f"k{uuid.uuid4().hex}"


def _record(store: Store, txn: uuid.UUID, payload: bytes | None) -> BranchRecord:
    return BranchRecord(store.name, gid_for(txn, store.name), store.two_phase, payload)


def _ours(store: Store, txn: uuid.UUID) -> list[str]:
    gid = gid_for(txn, store.name)
    return [g for g in store.participant().prepared_gids() if g == gid]


def _staged(harness: Harness, store: Store, key: str) -> tuple[Branch, uuid.UUID, BranchRecord]:
    """A branch with one record, staged and (for two-phase stores) prepared."""
    txn = new_id()
    branch = store.branch(txn)
    harness.write(branch, key)
    payload = branch.stage(txn, TX_TIME)
    record = _record(store, txn, payload)
    if store.two_phase:
        branch.prepare(record.gid)
    return branch, txn, record


class AdapterContract:
    """The contract as pytest tests: subclass it and provide the ``harness`` fixture."""

    @pytest.fixture
    def harness(self) -> Iterator[Harness]:
        raise NotImplementedError("provide a `harness` fixture for your store")

    # --- one store, no coordinator ------------------------------------------------------

    def test_a_single_branch_commits_natively(self, harness: Harness) -> None:
        store = harness.open_store("single")
        key, txn = _key(), new_id()
        branch = store.branch(txn)
        harness.write(branch, key)
        payload = branch.stage(txn, TX_TIME)
        assert harness.count(store, key) == 0, "visible before the commit"
        branch.commit_one_phase()
        assert harness.count(store, key) == 1
        assert store.participant().is_applied(_record(store, txn, payload), txn)

    def test_rollback_before_the_decision_discards_the_branch(self, harness: Harness) -> None:
        store = harness.open_store("rollback")
        key = _key()
        branch = store.branch(new_id())
        harness.write(branch, key)
        branch.rollback()
        assert harness.count(store, key) == 0

    def test_nothing_is_visible_or_applied_before_finish(self, harness: Harness) -> None:
        store = harness.open_store("invisible")
        key = _key()
        _, txn, record = _staged(harness, store, key)
        assert harness.count(store, key) == 0, "visible before the decision"
        assert not store.participant().is_applied(record, txn)
        if store.two_phase:
            assert _ours(store, txn) == [record.gid], "the prepared gid is not listed"
        store.participant().abort(record)

    def test_finish_commits_once_however_often_it_runs(self, harness: Harness) -> None:
        store = harness.open_store("finish")
        key = _key()
        _, txn, record = _staged(harness, store, key)
        participant = store.participant()
        for _ in range(3):  # the coordinator, then recovery workers, may all finish it
            participant.finish(record, txn, TX_TIME)
        assert harness.count(store, key) == 1
        assert participant.is_applied(record, txn)
        assert _ours(store, txn) == []

    def test_abort_discards_and_is_idempotent(self, harness: Harness) -> None:
        store = harness.open_store("abort")
        key = _key()
        _, txn, record = _staged(harness, store, key)
        participant = store.participant()
        participant.abort(record)
        participant.abort(record)
        assert harness.count(store, key) == 0
        assert _ours(store, txn) == []
        assert not participant.is_applied(record, txn)

    def test_an_aborted_branch_never_becomes_visible(self, harness: Harness) -> None:
        store = harness.open_store("aborted")
        key = _key()
        _, txn, record = _staged(harness, store, key)
        participant = store.participant()
        participant.abort(record)
        if store.two_phase:
            # The prepared transaction is gone: finishing it must not invent one.
            with contextlib.suppress(Exception):  # an error is allowed, visibility is not
                participant.finish(record, txn, TX_TIME)
            assert harness.count(store, key) == 0

    def test_plain_errors_are_not_retryable(self, harness: Harness) -> None:
        store = harness.open_store("retryable")
        branch = store.branch(new_id())
        error = ValueError("a bug, not a transient failure")
        assert not branch.is_retryable(error)
        assert not store.participant().is_retryable(error)
        branch.rollback()

    def test_errors_are_classified(self, harness: Harness) -> None:
        store = harness.open_store("classified")
        branch = store.branch(new_id())
        no_room = OSError(errno.ENOSPC, "No space left on device")
        for source in (branch, store.participant()):
            assert classify(source, ValueError("a bug")) is ErrorKind.PERMANENT
            assert classify(source, no_room) is ErrorKind.STORE_FULL
            assert not source.is_retryable(no_room)
        branch.rollback()

    def test_the_stores_own_full_error_is_store_full(self, harness: Harness) -> None:
        sample = getattr(harness, "store_full_error", None)
        if sample is None:
            return  # the harness names no driver error for a full store: nothing to check
        store = harness.open_store("full")
        branch = store.branch(new_id())
        error = sample()
        for source in (branch, store.participant()):
            assert classify(source, error) is ErrorKind.STORE_FULL
            assert not source.is_retryable(error)
        branch.rollback()

    def test_free_space_is_bytes_or_unknown(self, harness: Harness) -> None:
        store = harness.open_store("capacity")
        if not isinstance(store, Capacity):
            return  # optional: a store that cannot tell does not implement it
        free = store.free_space()
        assert free is None or (isinstance(free, int) and free >= 0)

    # --- the commit protocol under crashes ----------------------------------------------

    @pytest.mark.parametrize("point", CRASH_POINTS, ids=lambda p: p or "no-crash")
    def test_a_crash_at_any_step_ends_all_or_nothing(
        self, harness: Harness, point: str | None
    ) -> None:
        # A name of its own: recovery rolls back unknown gids of this store name on
        # the whole server, and must never touch another process's transactions.
        name = f"contract_{uuid.uuid4().hex[:12]}"
        store = harness.open_store(name)
        assert store.name == name, "open_store must name the store as asked"
        memory = MemoryStore(f"{name}_memory")
        faults, wall = _Faults(point), _Wall()
        journal = _ProbeJournal(faults, wall)
        participants: dict[str, Participant] = {
            store.name: _ProbeParticipant("store", store.participant(), faults),
            memory.name: _ProbeParticipant("memory", memory.participant(), faults),
        }
        coordinator = Coordinator(
            journal=journal,
            participants=participants,
            clock=HybridLogicalClock(),
            finish=Retry(attempts=1, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
            sleep=lambda s: None,
        )
        key, txn = _key(), new_id()
        branches = [
            _Probe(store.branch(txn), "store", faults),
            _Probe(memory.branch(txn), "memory", faults),
        ]
        harness.write(branches[0].inner, key)
        branches[1].inner.write(key)  # type: ignore[attr-defined]
        try:
            coordinator.commit(txn, branches)
        except Crash:
            for branch in branches:
                abandon(harness, branch)
        record = journal.get(txn)
        decided = record is not None and record.state in (State.COMMITTED, State.DONE)
        if point is not None and not faults.fired:
            decided = True  # the step does not exist for this store: the commit completed

        wall.ms += 60_000
        report = Recovery(
            journal=journal,
            participants={store.name: store.participant(), memory.name: memory.participant()},
            stale_after_ms=1,
            wall_ms=wall,
        ).run_once()

        expected = 1 if decided else 0
        assert harness.count(store, key) == expected, f"store after {point}: {report}"
        assert memory.count(key) == expected, f"memory after {point}: {report}"
        assert _ours(store, txn) == [], f"{point} left a prepared transaction"
        assert memory.prepared == {}
        final = journal.get(txn)
        assert final is None or final.state in (State.ABORTED, State.DONE), (
            f"{point}: journal left {final.state}"
        )
