"""The adapter contract kit itself: a correct store passes it, broken ones fail it."""

import uuid
from collections.abc import Callable

import pytest

from rhizodat.core.coordinator import Branch
from rhizodat.core.journal import BranchRecord
from rhizodat.core.unit_of_work import Store
from rhizodat.testing.contract import CRASH_POINTS, AdapterContract, MemoryStore
from rhizodat.testing.contract import _MemoryParticipant as MemoryParticipant


class MemoryHarness:
    def __init__(self, factory: Callable[[str], MemoryStore] = MemoryStore) -> None:
        self.factory = factory

    def open_store(self, name: str) -> Store:
        return self.factory(name)

    def write(self, branch: Branch, key: str) -> None:
        branch.write(key)  # type: ignore[attr-defined]

    def count(self, store: Store, key: str) -> int:
        assert isinstance(store, MemoryStore)
        return store.count(key)


class TestMemoryStoreContract(AdapterContract):
    """The reference: rhizodat's in-memory two-phase store keeps the contract."""

    @pytest.fixture
    def harness(self) -> MemoryHarness:
        return MemoryHarness()


# --- broken stores ----------------------------------------------------------------------


class DoubleFinish(MemoryParticipant):
    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        entry = self.db.prepared.get(branch.gid)  # never forgets: applies again each time
        if entry is not None:
            self.db.committed.setdefault(dat_txn, []).extend(entry[1])


class NoAbort(MemoryParticipant):
    def abort(self, branch: BranchRecord) -> None:
        pass


class VisibleAtPrepare(MemoryStore):
    def branch(self, dat_txn: uuid.UUID):  # type: ignore[no-untyped-def]  # noqa: ANN201
        inner = super().branch(dat_txn)
        original = inner.prepare

        def prepare(gid: str) -> None:
            original(gid)
            self.committed[dat_txn] = list(inner.rows)  # leaks before the decision

        inner.prepare = prepare  # type: ignore[method-assign]
        return inner


def with_participant(cls: type[MemoryParticipant]) -> Callable[[str], MemoryStore]:
    def factory(name: str) -> MemoryStore:
        store = MemoryStore(name)
        store.participant = lambda: cls(store)  # type: ignore[method-assign]
        return store

    return factory


def fails(test: str, harness: MemoryHarness, **kwargs: object) -> None:
    with pytest.raises(AssertionError):
        getattr(AdapterContract(), test)(harness, **kwargs)


def test_a_finish_that_applies_twice_fails_the_contract() -> None:
    fails(
        "test_finish_commits_once_however_often_it_runs",
        MemoryHarness(with_participant(DoubleFinish)),
    )


def test_an_abort_that_keeps_the_prepared_transaction_fails_the_contract() -> None:
    fails("test_abort_discards_and_is_idempotent", MemoryHarness(with_participant(NoAbort)))


def test_rows_visible_before_the_decision_fail_the_contract() -> None:
    harness = MemoryHarness(VisibleAtPrepare)
    fails("test_nothing_is_visible_or_applied_before_finish", harness)
    fails("test_a_crash_at_any_step_ends_all_or_nothing", harness, point="journal.PREPARED")


def test_every_protocol_step_has_a_crash_point() -> None:
    points = {p for p in CRASH_POINTS if p is not None}
    for state in ("ACTIVE", "PREPARED", "COMMITTED", "DONE"):
        assert {f"journal.{state}", f"journal.{state}.after"} <= points
    for store in ("store", "memory"):
        for step in ("prepare", "finish"):
            assert {f"{store}.{step}", f"{store}.{step}.after"} <= points
