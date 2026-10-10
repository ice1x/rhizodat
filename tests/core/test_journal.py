"""Journal of multi-branch meta-transactions: states, compare-and-set, scans."""

import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from datorium.core.identity import new_id
from datorium.core.journal import (
    BranchRecord,
    Journal,
    JournalConflict,
    JournalRecord,
    MemoryJournal,
    SqliteJournal,
    State,
    open_journal,
)

BRANCHES = (
    BranchRecord(store="pg", gid="dat:1:pg", two_phase=True),
    BranchRecord(store="clickhouse", gid="dat:1:clickhouse", two_phase=False, payload=b"\x00rows"),
)


class Wall:
    def __init__(self, ms: int = 1_000_000) -> None:
        self.ms = ms

    def __call__(self) -> int:
        return self.ms


JournalFactory = Callable[[Wall], Journal]


@pytest.fixture(params=["memory", "sqlite"])
def make_journal(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[JournalFactory]:
    opened: list[Journal] = []

    def factory(wall: Wall) -> Journal:
        if request.param == "memory":
            journal: Journal = MemoryJournal(wall_ms=wall)
        else:
            journal = SqliteJournal(tmp_path / f"journal-{len(opened)}.db", wall_ms=wall)
        opened.append(journal)
        return journal

    yield factory
    for journal in opened:
        journal.close()


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def journal(make_journal: JournalFactory, wall: Wall) -> Journal:
    return make_journal(wall)


def begin(journal: Journal) -> JournalRecord:
    return journal.begin(new_id(), BRANCHES)


# --- begin / get -------------------------------------------------------------


def test_begin_records_an_active_transaction(journal: Journal, wall: Wall) -> None:
    txn = new_id()
    record = journal.begin(txn, BRANCHES)
    assert record.dat_txn == txn
    assert record.state is State.ACTIVE
    assert record.branches == BRANCHES
    assert record.dat_tx_time is None
    assert record.created_ms == record.updated_ms == wall.ms
    assert journal.get(txn) == record


def test_get_of_an_unknown_transaction_is_none(journal: Journal) -> None:
    assert journal.get(new_id()) is None


def test_begin_twice_with_the_same_dat_txn_is_a_conflict(journal: Journal) -> None:
    record = begin(journal)
    with pytest.raises(JournalConflict, match="already"):
        journal.begin(record.dat_txn, BRANCHES)


def test_branch_payload_survives_the_round_trip(journal: Journal) -> None:
    record = begin(journal)
    stored = journal.get(record.dat_txn)
    assert stored is not None
    assert stored.branches[1].payload == b"\x00rows"
    assert stored.branches[0].payload is None


# --- transitions -------------------------------------------------------------


def test_happy_path_active_prepared_committed_done(journal: Journal, wall: Wall) -> None:
    record = begin(journal)
    wall.ms += 5
    prepared = journal.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=42)
    assert (prepared.state, prepared.dat_tx_time, prepared.updated_ms) == (
        State.PREPARED,
        42,
        wall.ms,
    )
    committed = journal.transition(record.dat_txn, State.PREPARED, State.COMMITTED)
    assert committed.dat_tx_time == 42
    done = journal.transition(record.dat_txn, State.COMMITTED, State.DONE)
    assert done.state is State.DONE
    assert done.created_ms == record.created_ms


@pytest.mark.parametrize("from_state", [State.ACTIVE, State.PREPARED])
def test_abort_is_allowed_before_the_decision(journal: Journal, from_state: State) -> None:
    record = begin(journal)
    if from_state is State.PREPARED:
        journal.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
    assert journal.transition(record.dat_txn, from_state, State.ABORTED).state is State.ABORTED


@pytest.mark.parametrize(
    ("path", "illegal"),
    [
        ([], State.COMMITTED),  # ACTIVE -> COMMITTED skips prepare
        ([], State.DONE),
        ([State.PREPARED, State.COMMITTED], State.ABORTED),  # never abort after the decision
        ([State.PREPARED, State.COMMITTED], State.PREPARED),
        ([State.ABORTED], State.PREPARED),
        ([State.PREPARED, State.COMMITTED, State.DONE], State.COMMITTED),
    ],
)
def test_illegal_transitions_are_rejected(
    journal: Journal, path: list[State], illegal: State
) -> None:
    record = begin(journal)
    current = State.ACTIVE
    for nxt in path:
        journal.transition(
            record.dat_txn, current, nxt, dat_tx_time=1 if nxt is State.PREPARED else None
        )
        current = nxt
    with pytest.raises(ValueError, match="illegal"):
        journal.transition(record.dat_txn, current, illegal)


def test_prepared_requires_a_transaction_time(journal: Journal) -> None:
    record = begin(journal)
    with pytest.raises(ValueError, match="dat_tx_time"):
        journal.transition(record.dat_txn, State.ACTIVE, State.PREPARED)


def test_transition_from_a_stale_state_is_a_conflict_and_changes_nothing(journal: Journal) -> None:
    record = begin(journal)
    journal.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=7)
    journal.transition(record.dat_txn, State.PREPARED, State.ABORTED)  # recovery won the race
    with pytest.raises(JournalConflict, match="ABORTED"):
        journal.transition(record.dat_txn, State.PREPARED, State.COMMITTED)
    stored = journal.get(record.dat_txn)
    assert stored is not None
    assert stored.state is State.ABORTED


def test_transition_of_an_unknown_transaction_is_a_conflict(journal: Journal) -> None:
    with pytest.raises(JournalConflict, match="unknown"):
        journal.transition(new_id(), State.ACTIVE, State.PREPARED, dat_tx_time=1)


def race(journal: Journal, txn: object) -> list[State]:
    """Commit and abort the same PREPARED record from two threads at once."""
    winners: list[State] = []
    barrier = threading.Barrier(2)

    def attempt(new: State) -> None:
        barrier.wait()
        try:
            journal.transition(txn, State.PREPARED, new)  # type: ignore[arg-type]
            winners.append(new)
        except JournalConflict:
            pass

    threads = [
        threading.Thread(target=attempt, args=(s,)) for s in (State.COMMITTED, State.ABORTED)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return winners


def test_concurrent_decide_and_abort_have_exactly_one_winner(journal: Journal) -> None:
    for _ in range(20):
        record = begin(journal)
        journal.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
        winners = race(journal, record.dat_txn)
        stored = journal.get(record.dat_txn)
        assert stored is not None
        assert winners == [stored.state]


# --- scans and compaction ----------------------------------------------------


def test_pending_lists_undecided_and_unfinished_transactions_older_than_a_cutoff(
    journal: Journal, wall: Wall
) -> None:
    old_active = begin(journal)
    old_committed = begin(journal)
    journal.transition(old_committed.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
    journal.transition(old_committed.dat_txn, State.PREPARED, State.COMMITTED)
    old_done = begin(journal)
    journal.transition(old_done.dat_txn, State.ACTIVE, State.ABORTED)
    wall.ms += 10_000
    fresh = begin(journal)

    pending = journal.pending(updated_before_ms=wall.ms - 5_000)
    assert {r.dat_txn for r in pending} == {old_active.dat_txn, old_committed.dat_txn}
    assert fresh.dat_txn not in {r.dat_txn for r in journal.pending(updated_before_ms=wall.ms)}
    assert {r.dat_txn for r in journal.pending(updated_before_ms=wall.ms + 1)} >= {fresh.dat_txn}


def test_pending_is_ordered_oldest_first_and_limited(journal: Journal, wall: Wall) -> None:
    txns = []
    for _ in range(5):
        txns.append(begin(journal).dat_txn)
        wall.ms += 1
    assert [r.dat_txn for r in journal.pending(updated_before_ms=wall.ms, limit=3)] == txns[:3]


def test_forget_removes_only_finished_transactions(journal: Journal) -> None:
    active = begin(journal)
    aborted = begin(journal)
    journal.transition(aborted.dat_txn, State.ACTIVE, State.ABORTED)

    journal.forget(aborted.dat_txn)
    assert journal.get(aborted.dat_txn) is None
    with pytest.raises(ValueError, match="ACTIVE"):
        journal.forget(active.dat_txn)
    assert journal.get(active.dat_txn) is not None


# --- durability and opening ----------------------------------------------------


def test_sqlite_journal_survives_reopening(tmp_path: Path, wall: Wall) -> None:
    path = tmp_path / "journal.db"
    first = SqliteJournal(path, wall_ms=wall)
    record = first.begin(new_id(), BRANCHES)
    first.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=9)
    first.close()

    second = SqliteJournal(path, wall_ms=wall)
    stored = second.get(record.dat_txn)
    second.close()
    assert stored is not None
    assert (stored.state, stored.dat_tx_time, stored.branches) == (State.PREPARED, 9, BRANCHES)


def test_two_sqlite_handles_share_state(tmp_path: Path, wall: Wall) -> None:
    path = tmp_path / "shared.db"
    a, b = SqliteJournal(path, wall_ms=wall), SqliteJournal(path, wall_ms=wall)
    record = a.begin(new_id(), BRANCHES)
    b.transition(record.dat_txn, State.ACTIVE, State.ABORTED)
    with pytest.raises(JournalConflict):
        a.transition(record.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
    a.close()
    b.close()


def test_open_journal_from_a_file_dsn(tmp_path: Path) -> None:
    journal = open_journal(f"file://{tmp_path / 'j.db'}")
    assert isinstance(journal, SqliteJournal)
    journal.close()


def test_open_journal_requires_a_dsn() -> None:
    with pytest.raises(ValueError, match="journal"):
        open_journal(None)


def test_open_journal_rejects_a_store_that_cannot_be_a_journal() -> None:
    with pytest.raises(ValueError, match="ACID"):
        open_journal("clickhouse://localhost/x")


@pytest.mark.parametrize("dsn", ["postgresql://localhost/app", "drevo:///tmp/x.redb"])
def test_open_journal_for_stores_not_implemented_yet(dsn: str) -> None:
    with pytest.raises(NotImplementedError):
        open_journal(dsn)


# --- model-based check -------------------------------------------------------

LEGAL = {
    State.ACTIVE: {State.PREPARED, State.ABORTED},
    State.PREPARED: {State.COMMITTED, State.ABORTED},
    State.COMMITTED: {State.DONE},
    State.ABORTED: set(),
    State.DONE: set(),
}


class JournalMachine(RuleBasedStateMachine):
    """Random operations against MemoryJournal agree with a dict model."""

    def __init__(self) -> None:
        super().__init__()
        self.journal = MemoryJournal(wall_ms=Wall())
        self.model: dict[object, State] = {}

    @rule()
    def begin(self) -> None:
        txn = new_id()
        self.journal.begin(txn, BRANCHES)
        self.model[txn] = State.ACTIVE

    @precondition(lambda self: bool(self.model))
    @rule(data=st.data(), claimed=st.sampled_from(list(State)), new=st.sampled_from(list(State)))
    def transition(self, data: st.DataObject, claimed: State, new: State) -> None:
        txn = data.draw(st.sampled_from(sorted(self.model, key=str)))
        actual = self.model[txn]
        # Argument errors (an illegal edge) are checked before the stored state.
        try:
            self.journal.transition(txn, claimed, new, dat_tx_time=1)  # type: ignore[arg-type]
        except ValueError:
            assert new not in LEGAL[claimed]
        except JournalConflict:
            assert new in LEGAL[claimed]
            assert claimed is not actual
        else:
            assert new in LEGAL[claimed]
            assert claimed is actual
            self.model[txn] = new

    @invariant()
    def journal_matches_model(self) -> None:
        for txn, state in self.model.items():
            stored = self.journal.get(txn)  # type: ignore[arg-type]
            assert stored is not None
            assert stored.state is state


TestJournalMachine = JournalMachine.TestCase
TestJournalMachine.settings = settings(max_examples=50, stateful_step_count=30)
