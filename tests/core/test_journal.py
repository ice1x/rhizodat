"""Journal of multi-branch meta-transactions: states, compare-and-set, scans."""

import threading
import uuid
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


JournalFactory = Callable[..., Journal]  # (wall, index=None): index reopens a journal


@pytest.fixture(params=["memory", "sqlite", "postgres", "drevo", "drevo-bolt"])
def make_journal(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[JournalFactory]:
    opened: list[Journal] = []
    if request.param == "postgres":
        from datorium.adapters.postgres import PostgresJournal

        pg_dsn: str = request.getfixturevalue("pg_dsn")
        pg_schema: str = request.getfixturevalue("pg_schema")
    if request.param.startswith("drevo"):
        from conftest import drevo_embedded_available
        from datorium.adapters.drevo import DrevoJournal

        if request.param == "drevo-bolt":
            drevo_dsn: str = request.getfixturevalue("drevo_bolt_dsn")
        elif drevo_embedded_available():
            drevo_dsn = f"drevo://{tmp_path / 'journal.drevo'}"
        else:
            pytest.skip("drevo-py with 2PC is not installed")
        # A label per journal keeps tests apart on a shared drevo server.
        label = f"DatJournal{uuid.uuid4().hex[:10]}"

    def factory(wall: Wall, index: int | None = None) -> Journal:
        n = len(opened) if index is None else index
        if request.param == "memory":
            journal: Journal = MemoryJournal(wall_ms=wall)
        elif request.param == "postgres":
            journal = PostgresJournal(pg_dsn, schema=pg_schema, table=f"journal_{n}", wall_ms=wall)
        elif request.param.startswith("drevo"):
            journal = DrevoJournal(drevo_dsn, label=f"{label}_{n}", wall_ms=wall)
        else:
            journal = SqliteJournal(tmp_path / f"journal-{n}.db", wall_ms=wall)
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


def test_pending_orders_by_last_update_not_by_creation(journal: Journal, wall: Wall) -> None:
    first = begin(journal)
    wall.ms += 1
    second = begin(journal)
    wall.ms += 1
    journal.transition(first.dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
    wall.ms += 1
    assert [r.dat_txn for r in journal.pending(updated_before_ms=wall.ms)] == [
        second.dat_txn,
        first.dat_txn,
    ]


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


def test_open_journal_for_drevo(tmp_path: Path) -> None:
    from conftest import drevo_embedded_available
    from datorium.adapters.drevo import DrevoJournal

    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed")
    journal = open_journal(f"drevo://{tmp_path / 'journal.drevo'}")
    assert isinstance(journal, DrevoJournal)
    record = begin(journal)
    assert journal.get(record.dat_txn) == record
    journal.close()


def test_drevo_journal_label_is_validated() -> None:
    from datorium.adapters.drevo import DrevoJournal

    with pytest.raises(ValueError, match="label"):
        DrevoJournal("drevo+bolt://127.0.0.1:1", label="X) DETACH DELETE (n")


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


# --- the migration history ----------------------------------------------------------------


def test_every_journal_keeps_the_migration_history(journal: Journal) -> None:
    from datorium.core.journal import AppliedMigration, MigrationHistory

    assert isinstance(journal, MigrationHistory)
    assert journal.applied_migrations() == []
    second = AppliedMigration(2, "graph_index", "b" * 64, (2_000 << 16) | 3)
    first = AppliedMigration(1, "bugs", "a" * 64, 1_000 << 16)
    journal.record_migration(second)
    journal.record_migration(first)
    assert journal.applied_migrations() == [first, second]

    with pytest.raises(JournalConflict, match="migration 1 is already recorded"):
        journal.record_migration(AppliedMigration(1, "bugs", "c" * 64, 3_000 << 16))
    assert journal.applied_migrations() == [first, second]


def test_the_migration_history_survives_reopening(make_journal: JournalFactory) -> None:
    from datorium.core.journal import AppliedMigration, MemoryJournal, MigrationHistory

    wall = Wall()
    journal = make_journal(wall)
    if isinstance(journal, MemoryJournal):
        return  # not durable: nothing to reopen
    assert isinstance(journal, MigrationHistory)
    applied = AppliedMigration(1, "bugs", "a" * 64, 1_000 << 16)
    journal.record_migration(applied)
    journal.close()
    reopened = make_journal(wall, index=0)
    assert isinstance(reopened, MigrationHistory)
    assert reopened.applied_migrations() == [applied]
