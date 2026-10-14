"""Commit protocol: one-phase, multi-branch 2PC through the journal, ambiguity."""

import uuid

import pytest

from datorium import StoreFull
from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import (
    Aborted,
    Committed,
    CommittedPending,
    Coordinator,
    InDoubt,
    gid_for,
    parse_gid,
)
from datorium.core.identity import new_id
from datorium.core.journal import JournalConflict, JournalRecord, MemoryJournal, State
from datorium.core.retry import Retry
from fakes import Crash, FakeBranch, FakeCapacity, Faults, Full, Permanent, Transient, World


class Clock:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


@pytest.fixture
def world() -> World:
    w = World()
    w.store("pg", two_phase=True)
    w.store("graph", two_phase=True)
    w.store("clickhouse", two_phase=False)
    return w


@pytest.fixture
def journal() -> MemoryJournal:
    return MemoryJournal()


@pytest.fixture
def clock() -> Clock:
    return Clock()


def coordinator(
    world: World, journal: MemoryJournal | None, clock: Clock, **kw: object
) -> Coordinator:
    return Coordinator(
        journal=journal,
        participants=world.participants(),
        clock=HybridLogicalClock(),
        finish=Retry(attempts=5, deadline_s=2.0, base_delay_s=0.01, max_delay_s=0.1),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        **kw,  # type: ignore[arg-type]
    )


# --- gid -----------------------------------------------------------------------


def test_gid_round_trips_and_names_the_store() -> None:
    txn = new_id()
    gid = gid_for(txn, "orders:pg")
    assert gid == f"dat:{txn}:orders:pg"
    assert parse_gid(gid) == (txn, "orders:pg")


@pytest.mark.parametrize("gid", ["foreign-xid", "dat:not-a-uuid:pg", "dat:"])
def test_foreign_gids_are_not_ours(gid: str) -> None:
    assert parse_gid(gid) is None


# --- empty and single branch -----------------------------------------------------


def test_nothing_to_commit(world: World, journal: MemoryJournal, clock: Clock) -> None:
    txn = new_id()
    assert coordinator(world, journal, clock).commit(txn, []) == Committed(txn, None)


def test_single_branch_commits_natively_without_the_journal(clock: Clock) -> None:
    world = World()
    world.store("clickhouse", two_phase=False)
    txn = new_id()
    outcome = coordinator(world, None, clock).commit(txn, world.branches())
    assert isinstance(outcome, Committed)
    assert outcome.dat_tx_time is not None
    assert world.applied(txn) == {"clickhouse": True}
    assert "clickhouse.prepare" not in world.faults.seen


def test_single_branch_failure_rolls_back_and_says_if_retry_helps(clock: Clock) -> None:
    world = World()
    world.store("pg", two_phase=True)
    world.faults.add("pg.commit_one_phase", Transient("40001"))
    txn = new_id()
    branches = world.branches()
    with pytest.raises(Aborted) as err:
        coordinator(world, None, clock).commit(txn, branches)
    assert err.value.retryable is True
    assert err.value.dat_txn == txn
    assert isinstance(err.value.__cause__, Transient)
    assert branches[0].state == "rolled back"


def test_single_branch_lost_ack_is_resolved_by_asking_the_store(clock: Clock) -> None:
    world = World()
    store = world.store("pg", two_phase=True)
    store.lose_commit_ack = 1
    txn = new_id()
    outcome = coordinator(world, None, clock).commit(txn, world.branches())
    assert isinstance(outcome, Committed)
    assert world.applied(txn) == {"pg": True}


def test_single_branch_lost_ack_check_sees_the_staged_payload(clock: Clock) -> None:
    """A non-2PC store (Iceberg) needs its payload to know where to look."""
    world = World()
    store = world.store("lake", two_phase=False)
    store.lose_commit_ack = 1
    seen: list[bytes | None] = []
    participant = world.participants()["lake"]
    original = participant.is_applied

    def spy(branch: object, dat_txn: uuid.UUID) -> bool:
        seen.append(branch.payload)  # type: ignore[attr-defined]
        return original(branch, dat_txn)  # type: ignore[arg-type]

    participant.is_applied = spy  # type: ignore[method-assign]
    txn = new_id()
    outcome = Coordinator(
        journal=None, participants={"lake": participant}, clock=HybridLogicalClock()
    ).commit(txn, world.branches())
    assert isinstance(outcome, Committed)
    assert len(seen) == 1
    assert seen[0] is not None


# --- several branches: happy path --------------------------------------------------


def test_multi_branch_commit_follows_the_protocol(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    txn = new_id()
    outcome = coordinator(world, journal, clock).commit(txn, world.branches())

    assert isinstance(outcome, Committed)
    assert world.applied(txn) == {"pg": True, "graph": True, "clickhouse": True}
    assert world.leftover_prepared() == {}
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.DONE
    assert record.dat_tx_time == outcome.dat_tx_time
    assert [b.store for b in record.branches] == ["pg", "graph", "clickhouse"]

    seen = world.faults.seen
    # Slow lock-free staging first, lock-holding prepare last, then commit 2PC first.
    assert seen.index("clickhouse.stage") < seen.index("pg.prepare")
    assert max(seen.index("pg.prepare"), seen.index("graph.prepare")) < seen.index("pg.finish")
    assert max(seen.index("pg.finish"), seen.index("graph.finish")) < seen.index(
        "clickhouse.finish"
    )


def test_lock_holders_commit_first_whatever_order_the_branches_come_in(
    journal: MemoryJournal, clock: Clock
) -> None:
    world = World()
    world.store("clickhouse", two_phase=False)
    world.store("iceberg", two_phase=False)
    world.store("pg", two_phase=True)
    txn = new_id()
    coordinator(world, journal, clock).commit(txn, world.branches())
    finishes = [p for p in world.faults.seen if p.endswith(".finish")]
    assert finishes[0] == "pg.finish"


def test_multi_branch_without_a_journal_is_refused_up_front(world: World, clock: Clock) -> None:
    txn = new_id()
    branches = world.branches()
    with pytest.raises(ValueError, match="journal"):
        coordinator(world, None, clock).commit(txn, branches)
    assert world.applied(txn) == dict.fromkeys(world.stores, False)
    assert all(b.state == "rolled back" for b in branches)


def test_duplicate_store_branches_are_rejected(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    pg = world.stores["pg"]
    with pytest.raises(ValueError, match="'pg'"):
        coordinator(world, journal, clock).commit(
            new_id(), [FakeBranch(pg, []), FakeBranch(pg, [])]
        )


def test_branch_without_a_participant_is_rejected(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    other = World()
    lonely = other.store("iceberg", two_phase=False)
    with pytest.raises(ValueError, match="'iceberg'"):
        coordinator(world, journal, clock).commit(
            new_id(), [*world.branches(), FakeBranch(lonely, [])]
        )


# --- several branches: failures before the decision --------------------------------


@pytest.mark.parametrize("point", ["clickhouse.stage", "pg.stage", "pg.prepare", "graph.prepare"])
@pytest.mark.parametrize(
    ("error", "retryable"), [(Transient("conflict"), True), (Permanent("constraint"), False)]
)
def test_failure_before_the_decision_aborts_everything(
    world: World,
    journal: MemoryJournal,
    clock: Clock,
    point: str,
    error: Exception,
    retryable: bool,
) -> None:
    world.faults.add(point, error)
    txn = new_id()
    branches = world.branches()
    with pytest.raises(Aborted) as err:
        coordinator(world, journal, clock).commit(txn, branches)

    assert err.value.retryable is retryable
    assert err.value.__cause__ is error
    assert world.applied(txn) == dict.fromkeys(world.stores, False)
    assert world.leftover_prepared() == {}
    assert all(b.state == "rolled back" for b in branches)
    record = journal.get(txn)
    assert record is None or record.state is State.ABORTED


def test_journal_unreachable_before_the_decision_is_a_retryable_abort(
    world: World, clock: Clock
) -> None:
    class DownJournal(MemoryJournal):
        def begin(self, *a: object, **k: object) -> object:  # type: ignore[override]
            raise ConnectionError("journal down")

    txn = new_id()
    with pytest.raises(Aborted) as err:
        coordinator(world, DownJournal(), clock).commit(txn, world.branches())
    assert err.value.retryable is True
    assert world.leftover_prepared() == {}


def test_recovery_won_the_race_so_the_late_decision_aborts(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    class RacingJournal(MemoryJournal):
        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED:
                super().transition(dat_txn, expected, State.ABORTED)
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

    racing = RacingJournal()
    txn = new_id()
    with pytest.raises(Aborted) as err:
        coordinator(world, racing, clock).commit(txn, world.branches())
    assert isinstance(err.value.__cause__, JournalConflict)
    assert world.applied(txn) == dict.fromkeys(world.stores, False)
    assert world.leftover_prepared() == {}


# --- the decision write is ambiguous ----------------------------------------------


def test_decision_timeout_that_did_apply_is_read_back_and_finished(
    world: World, clock: Clock
) -> None:
    class LossyJournal(MemoryJournal):
        lost = False

        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            record = super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)
            if new is State.COMMITTED and not self.lost:
                self.lost = True
                raise TimeoutError("ack lost")
            return record

    txn = new_id()
    outcome = coordinator(world, LossyJournal(), clock).commit(txn, world.branches())
    assert isinstance(outcome, Committed)
    assert world.applied(txn) == dict.fromkeys(world.stores, True)


def test_decision_timeout_that_did_not_apply_is_retried(world: World, clock: Clock) -> None:
    class FlakyJournal(MemoryJournal):
        failures = 1

        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED and self.failures:
                self.failures -= 1
                raise TimeoutError("not written")
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

    txn = new_id()
    assert isinstance(
        coordinator(world, FlakyJournal(), clock).commit(txn, world.branches()), Committed
    )
    assert world.applied(txn) == dict.fromkeys(world.stores, True)


def test_unreadable_decision_is_in_doubt_and_left_to_recovery(world: World, clock: Clock) -> None:
    class DarkJournal(MemoryJournal):
        dark = False

        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED:
                self.dark = True
                raise TimeoutError("no answer")
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

        def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
            if self.dark:
                raise ConnectionError("journal unreachable")
            return super().get(dat_txn)

    txn = new_id()
    with pytest.raises(InDoubt) as err:
        coordinator(world, DarkJournal(), clock).commit(txn, world.branches())
    assert err.value.dat_txn == txn
    # Nothing was rolled back: recovery decides from the journal later.
    assert set(world.leftover_prepared()) == {"pg", "graph"}


def test_decision_still_unwritten_after_every_attempt_is_settled_as_aborted(
    world: World, clock: Clock
) -> None:
    class SlowJournal(MemoryJournal):
        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED:
                raise TimeoutError("slow")
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

    journal = SlowJournal()
    txn = new_id()
    with pytest.raises(Aborted) as err:
        coordinator(world, journal, clock).commit(txn, world.branches())
    assert err.value.retryable
    assert journal.get(txn).state is State.ABORTED  # type: ignore[union-attr]
    assert world.leftover_prepared() == {}
    assert len(clock.sleeps) == 4  # five attempts, four pauses


def test_settling_an_undecidable_transaction_when_the_journal_goes_dark_is_in_doubt(
    world: World, clock: Clock
) -> None:
    """The give-up compare-and-set conflicts, then the read-back fails: in doubt,
    never an unclassified error."""

    class DarkJournal(MemoryJournal):
        dark = False

        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED:
                raise TimeoutError("slow")
            if new is State.ABORTED and expected is State.PREPARED:
                # Someone else decided in between; the record is no longer PREPARED.
                super().transition(dat_txn, State.PREPARED, State.COMMITTED)
                self.dark = True
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

        def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
            if self.dark:
                raise ConnectionError("journal unreachable")
            return super().get(dat_txn)

    with pytest.raises(InDoubt):
        coordinator(world, DarkJournal(), clock).commit(new_id(), world.branches())


# --- after the decision -------------------------------------------------------------


def test_finish_retries_a_transient_failure(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.faults.add("clickhouse.finish", Transient("too many parts"), times=2)
    txn = new_id()
    assert isinstance(coordinator(world, journal, clock).commit(txn, world.branches()), Committed)
    assert world.applied(txn)["clickhouse"]
    assert len(clock.sleeps) == 2


def test_finish_with_a_lost_ack_checks_the_store_instead_of_reapplying(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.stores["graph"].lose_commit_ack = 1
    txn = new_id()
    assert isinstance(coordinator(world, journal, clock).commit(txn, world.branches()), Committed)
    assert "graph.is_applied" in world.faults.seen


def test_finish_that_keeps_failing_returns_committed_pending_never_raises(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.faults.add("clickhouse.finish", Transient("down"), times=100)
    txn = new_id()
    outcome = coordinator(world, journal, clock).commit(txn, world.branches())

    assert outcome == CommittedPending(txn, outcome.dat_tx_time)
    assert world.applied(txn) == {"pg": True, "graph": True, "clickhouse": False}
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.COMMITTED  # handed to recovery
    assert clock.t <= 2.0 + 0.1


def test_finish_stops_at_the_deadline_even_with_attempts_left(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.faults.add("clickhouse.finish", Transient("down"), times=1000)
    slow = Coordinator(
        journal=journal,
        participants=world.participants(),
        clock=HybridLogicalClock(),
        finish=Retry(attempts=100, deadline_s=1.0, base_delay_s=0.4, max_delay_s=0.4),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        rand=lambda: 0.999,
    )
    outcome = slow.commit(new_id(), world.branches())
    assert isinstance(outcome, CommittedPending)
    tries = world.faults.seen.count("clickhouse.finish")
    assert 2 <= tries <= 4, f"{tries} attempts within a one-second deadline"
    assert clock.t <= 1.0


def test_finish_failure_of_any_kind_after_the_decision_does_not_raise(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.faults.add("pg.finish", Permanent("weird"), times=100)
    txn = new_id()
    outcome = coordinator(world, journal, clock).commit(txn, world.branches())
    assert isinstance(outcome, CommittedPending)
    assert world.applied(txn)["clickhouse"]  # the other branches still finished


def test_done_write_failure_still_reports_committed_pending(world: World, clock: Clock) -> None:
    class NoDone(MemoryJournal):
        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.DONE:
                raise ConnectionError("journal down")
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

    txn = new_id()
    outcome = coordinator(world, NoDone(), clock).commit(txn, world.branches())
    assert isinstance(outcome, CommittedPending)
    assert world.applied(txn) == dict.fromkeys(world.stores, True)


# --- crashes ------------------------------------------------------------------------


@pytest.mark.parametrize("point", ["pg.prepare", "graph.finish", "clickhouse.finish"])
def test_a_crash_is_not_swallowed(
    world: World, journal: MemoryJournal, clock: Clock, point: str
) -> None:
    world.faults.add(point, Crash())
    with pytest.raises(Crash):
        coordinator(world, journal, clock).commit(new_id(), world.branches())


def test_uow_rollback_releases_every_branch(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    branches = world.branches()
    world.faults.add("graph.rollback", Transient("connection reset"))
    coordinator(world, journal, clock).rollback(uuid.uuid4(), branches)
    assert all(b.state == "rolled back" for b in branches if b.store != "graph")


def test_faults_helper_counts_down() -> None:
    faults = Faults()
    faults.add("x", Transient(), times=2)
    for _ in range(2):
        with pytest.raises(Transient):
            faults.hit("x")
    faults.hit("x")


# --- a full store ------------------------------------------------------------------


def test_a_full_store_aborts_one_branch_with_store_full(clock: Clock) -> None:
    world = World()
    world.store("pg", two_phase=True)
    world.faults.add("pg.commit_one_phase", Full("53100 disk full"))
    txn, branches = new_id(), world.branches()
    with pytest.raises(StoreFull) as err:
        coordinator(world, None, clock).commit(txn, branches)
    assert (err.value.store, err.value.retryable) == ("pg", False)
    assert branches[0].state == "rolled back"


@pytest.mark.parametrize("point", ["clickhouse.stage", "graph.prepare"])
def test_a_full_store_before_the_decision_aborts_everything(
    world: World, journal: MemoryJournal, clock: Clock, point: str
) -> None:
    world.faults.add(point, Full("no space"))
    txn = new_id()
    with pytest.raises(StoreFull) as err:
        coordinator(world, journal, clock).commit(txn, world.branches())
    assert err.value.store == point.split(".")[0]
    assert not any(world.applied(txn).values())
    assert world.leftover_prepared() == {}


def test_a_store_below_min_free_is_refused_before_anything_is_written(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.stores["clickhouse"].free = 10
    world.stores["pg"].free = 10_000
    txn = new_id()
    with pytest.raises(StoreFull) as err:
        coordinator(world, journal, clock, capacity=world.capacity(), min_free=100).commit(
            txn, world.branches()
        )
    assert (err.value.store, err.value.free) == ("clickhouse", 10)
    assert not [p for p in world.faults.seen if p.endswith((".stage", ".prepare"))]
    assert journal.get(txn) is None


def test_stores_above_min_free_or_unknown_commit(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.stores["pg"].free = 100
    world.stores["clickhouse"].free = None  # the store cannot tell
    capacity = world.capacity()
    del capacity["graph"]  # no Capacity at all
    txn = new_id()
    outcome = coordinator(world, journal, clock, capacity=capacity, min_free=100).commit(
        txn, world.branches()
    )
    assert isinstance(outcome, Committed)


def test_a_capacity_check_that_fails_does_not_block_the_commit(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    capacity = {"pg": FakeCapacity(world.stores["pg"], fail=TimeoutError("system.disks"))}
    txn = new_id()
    outcome = coordinator(world, journal, clock, capacity=capacity, min_free=100).commit(
        txn, world.branches()
    )
    assert isinstance(outcome, Committed)


def test_without_min_free_capacity_is_not_asked(
    world: World, journal: MemoryJournal, clock: Clock
) -> None:
    world.stores["pg"].free = 0
    outcome = coordinator(world, journal, clock, capacity=world.capacity()).commit(
        new_id(), world.branches()
    )
    assert isinstance(outcome, Committed)


def test_min_free_must_not_be_negative(world: World, clock: Clock) -> None:
    with pytest.raises(ValueError, match="min_free"):
        coordinator(world, None, clock, min_free=-1)
