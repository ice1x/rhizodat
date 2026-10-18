"""The async Rust coordinator driven by Python ``transactional`` callbacks.

The open question of the bindings (plan step 5): a Unit of Work written in
Python hands its branches to the coordinator in the Rust core, which runs the
commit protocol on its own async runtime and calls back into each branch's
Python methods (``stage``, ``prepare``, ``finish`` ...) along the way.

The branches here are fakes, like the Rust ``tests/coordinator.rs`` fakes and
the Python ``tests/core/test_coordinator.py`` ones: no real store, so the whole
protocol -- one-phase, two-phase through the in-memory journal, roll back
before the decision, the all-or-nothing property, and a callback that raises --
is exercised without Docker. Install the module with ``uv sync --group native``
(needs a Rust toolchain).
"""

import threading
import uuid

import pytest
from faker import Faker

native = pytest.importorskip("rhizodat_native")


class Recorder:
    """Every branch method call, in order, across every branch."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.lock = threading.Lock()

    def note(self, store: str, method: str) -> None:
        with self.lock:
            self.calls.append((store, method))

    def of(self, store: str) -> list[str]:
        with self.lock:
            return [m for s, m in self.calls if s == store]


class Boom(Exception):
    """A branch callback failure; ``retryable`` is read by the coordinator."""

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


class FakeBranch:
    """A branch whose phases record themselves and may be told to fail.

    ``fail`` maps a method name to the ``Boom`` it raises the first time it is
    called; ``applied`` is what ``is_applied`` reports.
    """

    def __init__(
        self,
        store: str,
        *,
        two_phase: bool,
        recorder: Recorder,
        payload: bytes | None = None,
        fail: dict[str, Boom] | None = None,
        applied: bool = False,
    ) -> None:
        self._store = store
        self._two_phase = two_phase
        self._recorder = recorder
        self._payload = payload
        self._fail = dict(fail or {})
        self._applied = applied

    # identity ----------------------------------------------------------------
    def store(self) -> str:
        return self._store

    def two_phase(self) -> bool:
        return self._two_phase

    # before the decision -----------------------------------------------------
    def _maybe_fail(self, method: str) -> None:
        self._recorder.note(self._store, method)
        boom = self._fail.pop(method, None)
        if boom is not None:
            raise boom

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        assert isinstance(dat_txn, uuid.UUID)
        assert isinstance(dat_tx_time, int)
        self._maybe_fail("stage")
        return self._payload

    def prepare(self, gid: str) -> None:
        assert gid.startswith("dat:")
        self._maybe_fail("prepare")

    def commit_one_phase(self) -> None:
        self._maybe_fail("commit_one_phase")

    def rollback(self) -> None:
        self._recorder.note(self._store, "rollback")

    # after the decision ------------------------------------------------------
    def finish(self, payload: bytes | None, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        assert payload == self._payload
        assert isinstance(dat_txn, uuid.UUID)
        self._maybe_fail("finish")
        self._applied = True

    def is_applied(self, dat_txn: uuid.UUID) -> bool:
        assert isinstance(dat_txn, uuid.UUID)
        return self._applied

    def abort(self) -> None:
        self._recorder.note(self._store, "abort")

    def prepared_gids(self) -> list[str]:
        return []


@pytest.fixture
def faker() -> Faker:
    fake = Faker()
    Faker.seed(20261018)
    return fake


def new_txn() -> uuid.UUID:
    txn = native.new_id()
    assert isinstance(txn, uuid.UUID)
    return txn


# --- nothing, and a single branch ------------------------------------------------------


def test_nothing_to_commit() -> None:
    outcome = native.Coordinator(native.MemoryJournal()).commit(new_txn(), [])
    assert outcome["status"] == "committed"
    assert outcome["dat_tx_time"] is None


def test_single_branch_commits_natively_without_a_journal(faker: Faker) -> None:
    rec = Recorder()
    branch = FakeBranch("clickhouse", two_phase=False, recorder=rec, payload=faker.binary(16))
    txn = new_txn()
    outcome = native.Coordinator().commit(txn, [branch])
    assert outcome["status"] == "committed"
    assert isinstance(outcome["dat_tx_time"], int)
    assert outcome["dat_txn"] == txn
    assert rec.of("clickhouse") == ["stage", "commit_one_phase"]


def test_single_branch_failure_rolls_back_and_reports_retryable() -> None:
    rec = Recorder()
    branch = FakeBranch(
        "pg",
        two_phase=True,
        recorder=rec,
        fail={"commit_one_phase": Boom("deadlock", retryable=True)},
    )
    with pytest.raises(native.AbortedError) as excinfo:
        native.Coordinator().commit(new_txn(), [branch])
    assert excinfo.value.retryable is True
    assert "rollback" in rec.of("pg")


# --- several branches through the journal ----------------------------------------------


def test_two_phase_commit_runs_the_whole_protocol(faker: Faker) -> None:
    rec = Recorder()
    journal = native.MemoryJournal()
    branches = [
        FakeBranch("pg", two_phase=True, recorder=rec, payload=faker.binary(8)),
        FakeBranch("graph", two_phase=True, recorder=rec, payload=faker.binary(8)),
    ]
    txn = new_txn()
    outcome = native.Coordinator(journal).commit(txn, branches)
    assert outcome["status"] == "committed"
    assert isinstance(outcome["dat_tx_time"], int)
    for store in ("pg", "graph"):
        assert rec.of(store) == ["stage", "prepare", "finish"]
    record = journal.get(txn)
    assert record["state"] == "DONE"


def test_a_prepare_failure_before_the_decision_commits_nothing() -> None:
    rec = Recorder()
    journal = native.MemoryJournal()
    branches = [
        FakeBranch("pg", two_phase=True, recorder=rec),
        FakeBranch("graph", two_phase=True, recorder=rec, fail={"prepare": Boom("no")}),
    ]
    txn = new_txn()
    with pytest.raises(native.AbortedError):
        native.Coordinator(journal).commit(txn, branches)
    # All-or-nothing: every branch rolled back, nobody finished.
    for store in ("pg", "graph"):
        assert "finish" not in rec.of(store)
        assert "rollback" in rec.of(store)
    record = journal.get(txn)
    assert record is None or record["state"] == "ABORTED"


def test_a_callback_that_raises_is_chained_as_the_cause() -> None:
    # Like the Python reference: a branch failure before the decision raises
    # Aborted, with the store's own exception kept as ``__cause__``.
    rec = Recorder()
    journal = native.MemoryJournal()
    boom = Boom("kaboom")
    branches = [
        FakeBranch("pg", two_phase=True, recorder=rec),
        FakeBranch("graph", two_phase=True, recorder=rec, fail={"stage": boom}),
    ]
    with pytest.raises(native.AbortedError) as excinfo:
        native.Coordinator(journal).commit(new_txn(), branches)
    assert excinfo.value.__cause__ is boom


def test_two_branches_on_one_store_are_invalid() -> None:
    rec = Recorder()
    branches = [
        FakeBranch("pg", two_phase=True, recorder=rec),
        FakeBranch("pg", two_phase=True, recorder=rec),
    ]
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator(native.MemoryJournal()).commit(new_txn(), branches)


def test_a_meta_transaction_needs_a_journal() -> None:
    rec = Recorder()
    branches = [
        FakeBranch("pg", two_phase=True, recorder=rec),
        FakeBranch("graph", two_phase=True, recorder=rec),
    ]
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator().commit(new_txn(), branches)


# --- the GIL under concurrency ---------------------------------------------------------


def test_many_python_threads_commit_without_deadlock(faker: Faker) -> None:
    done: list[str] = []
    lock = threading.Lock()

    def work() -> None:
        rec = Recorder()
        journal = native.MemoryJournal()
        branches = [
            FakeBranch("pg", two_phase=True, recorder=rec, payload=faker.binary(4)),
            FakeBranch("graph", two_phase=True, recorder=rec, payload=faker.binary(4)),
        ]
        outcome = native.Coordinator(journal).commit(new_txn(), branches)
        with lock:
            done.append(outcome["status"])

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert done == ["committed"] * 8
