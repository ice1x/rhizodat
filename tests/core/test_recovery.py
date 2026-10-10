"""Recovery: presumed abort, roll-forward, orphan prepared gids, crash anywhere."""

import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import (
    Aborted,
    Committed,
    CommittedPending,
    Coordinator,
    InDoubt,
    Recovery,
    gid_for,
)
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, MemoryJournal, State
from datorium.core.retry import Retry
from fakes import Crash, Faults, FaultyJournal, Permanent, Transient, World

STALE_MS = 30_000


class Wall:
    def __init__(self) -> None:
        self.ms = 1_000_000

    def __call__(self) -> int:
        return self.ms


def make_world() -> World:
    world = World()
    world.store("pg", two_phase=True)
    world.store("graph", two_phase=True)
    world.store("clickhouse", two_phase=False)
    return world


def make_coordinator(world: World, journal: MemoryJournal) -> Coordinator:
    return Coordinator(
        journal=journal,
        participants=world.participants(),
        clock=HybridLogicalClock(),
        finish=Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
        sleep=lambda s: None,
    )


def make_recovery(world: World, journal: MemoryJournal, wall: Wall, **kw: int) -> Recovery:
    return Recovery(
        journal=journal,
        participants=world.participants(),
        stale_after_ms=STALE_MS,
        wall_ms=wall,
        **kw,
    )


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def world() -> World:
    return make_world()


@pytest.fixture
def journal(world: World, wall: Wall) -> FaultyJournal:
    return FaultyJournal(world.faults, wall)


def crash_commit(world: World, journal: MemoryJournal, point: str) -> uuid.UUID:
    world.faults.add(point, Crash())
    txn = new_id()
    with pytest.raises(Crash):
        make_coordinator(world, journal).commit(txn, world.branches())
    return txn


# --- presumed abort --------------------------------------------------------------


@pytest.mark.parametrize(
    "point", ["pg.prepare.after", "graph.prepare", "journal.PREPARED.after", "journal.COMMITTED"]
)
def test_crash_before_the_decision_is_rolled_back(
    world: World, journal: FaultyJournal, wall: Wall, point: str
) -> None:
    txn = crash_commit(world, journal, point)
    recovery = make_recovery(world, journal, wall)

    assert recovery.run_once().aborted == []  # not stale yet: the coordinator may be alive
    wall.ms += STALE_MS + 1
    report = recovery.run_once()

    assert report.aborted == [txn]
    assert world.applied(txn) == dict.fromkeys(world.stores, False)
    assert world.leftover_prepared() == {}
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.ABORTED


def test_abort_is_written_before_branches_are_rolled_back(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    txn = crash_commit(world, journal, "journal.COMMITTED")
    wall.ms += STALE_MS + 1
    make_recovery(world, journal, wall).run_once()
    seen = world.faults.seen
    assert seen.index("journal.ABORTED") < seen.index("pg.abort")
    assert journal.get(txn) is not None


# --- roll forward ----------------------------------------------------------------


@pytest.mark.parametrize(
    "point",
    [
        "journal.COMMITTED.after",
        "pg.finish",
        "graph.finish.after",
        "clickhouse.finish",
        "journal.DONE",
    ],
)
def test_crash_after_the_decision_is_rolled_forward(
    world: World, journal: FaultyJournal, wall: Wall, point: str
) -> None:
    txn = crash_commit(world, journal, point)
    wall.ms += STALE_MS + 1
    report = make_recovery(world, journal, wall).run_once()

    assert report.finished == [txn]
    assert world.applied(txn) == dict.fromkeys(world.stores, True)
    assert world.leftover_prepared() == {}
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.DONE


def test_recovery_rolls_forward_lock_holders_first(wall: Wall) -> None:
    world = World()
    world.store("clickhouse", two_phase=False)
    world.store("pg", two_phase=True)
    journal = FaultyJournal(world.faults, wall)
    crash_commit(world, journal, "journal.COMMITTED.after")
    world.faults.seen.clear()
    wall.ms += STALE_MS + 1
    make_recovery(world, journal, wall).run_once()
    assert [p for p in world.faults.seen if p.endswith(".finish")] == [
        "pg.finish",
        "clickhouse.finish",
    ]


def test_committed_pending_is_finished_by_recovery(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    world.faults.add("clickhouse.finish", Transient("down"), times=2)
    txn = new_id()
    assert isinstance(
        make_coordinator(world, journal).commit(txn, world.branches()), CommittedPending
    )

    wall.ms += STALE_MS + 1
    report = make_recovery(world, journal, wall).run_once()
    assert report.finished == [txn]
    assert world.applied(txn) == dict.fromkeys(world.stores, True)


def test_unfinished_branch_stays_pending_and_alerts_when_stuck(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    world.faults.add("clickhouse.finish", Permanent("schema mismatch"), times=1_000)
    txn = new_id()
    make_coordinator(world, journal).commit(txn, world.branches())
    recovery = make_recovery(world, journal, wall, alert_after_ms=600_000)

    wall.ms += STALE_MS + 1
    first = recovery.run_once()
    assert (first.finished, first.unfinished, first.stuck) == ([], [txn], [])

    wall.ms += 600_000
    later = recovery.run_once()
    assert later.unfinished == [txn]
    assert later.stuck == [txn]  # never silently dropped
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.COMMITTED


def test_recovery_and_a_live_coordinator_cannot_both_decide(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    """Recovery aborts a slow PREPARED transaction; the coordinator's decision then loses."""
    recovery = make_recovery(world, journal, wall)

    def recovery_runs_now() -> None:
        wall.ms += STALE_MS + 1
        recovery.run_once()

    class Interleave(Faults):
        def hit(self, point: str) -> None:
            if point == "journal.COMMITTED":
                recovery_runs_now()
            super().hit(point)

    world.faults.__class__ = Interleave
    txn = new_id()
    with pytest.raises(Aborted):
        make_coordinator(world, journal).commit(txn, world.branches())
    assert world.applied(txn) == dict.fromkeys(world.stores, False)
    assert world.leftover_prepared() == {}


# --- orphans in the stores --------------------------------------------------------


def test_prepared_gid_without_a_journal_record_is_rolled_back(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    txn = new_id()
    gid = gid_for(txn, "pg")
    world.stores["pg"].prepared[gid] = (txn, ["row"])
    world.stores["pg"].prepared["someone-elses-xid"] = (uuid.uuid4(), ["theirs"])

    report = make_recovery(world, journal, wall).run_once()
    assert report.orphans == [gid]
    assert list(world.stores["pg"].prepared) == ["someone-elses-xid"]


def test_prepared_gid_left_after_a_failed_abort_is_swept(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    txn = crash_commit(world, journal, "journal.COMMITTED")
    world.faults.add("graph.abort", Transient("connection reset"))
    wall.ms += STALE_MS + 1
    recovery = make_recovery(world, journal, wall)

    recovery.run_once()
    assert world.leftover_prepared() == {"graph": [gid_for(txn, "graph")]}
    report = recovery.run_once()
    assert report.orphans == [gid_for(txn, "graph")]
    assert world.leftover_prepared() == {}


def test_prepared_gid_of_a_live_transaction_is_left_alone(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    txn = new_id()
    journal.begin(txn, [BranchRecord("pg", gid_for(txn, "pg"), True)])
    world.stores["pg"].prepared[gid_for(txn, "pg")] = (txn, ["row"])
    report = make_recovery(world, journal, wall).run_once()
    assert report.orphans == []
    assert world.leftover_prepared() == {"pg": [gid_for(txn, "pg")]}


def test_recovery_survives_a_broken_participant(
    world: World, journal: FaultyJournal, wall: Wall
) -> None:
    txn = crash_commit(world, journal, "pg.finish")

    class Broken(Exception):
        pass

    world.faults.add("pg.finish", Broken(), times=1)
    wall.ms += STALE_MS + 1
    recovery = make_recovery(world, journal, wall)
    assert recovery.run_once().unfinished == [txn]
    assert recovery.run_once().finished == [txn]


def test_invalid_recovery_settings_are_rejected(world: World, journal: FaultyJournal) -> None:
    with pytest.raises(ValueError, match="stale_after_ms"):
        Recovery(journal=journal, participants=world.participants(), stale_after_ms=0)


# --- crash anywhere: all or nothing ----------------------------------------------

POINTS = [
    *(f"{s}.stage" for s in ("pg", "graph", "clickhouse")),
    *(f"{s}.prepare{a}" for s in ("pg", "graph") for a in ("", ".after")),
    *(
        f"journal.{st}{a}"
        for st in ("ACTIVE", "PREPARED", "COMMITTED", "DONE")
        for a in ("", ".after")
    ),
    *(f"{s}.finish{a}" for s in ("pg", "graph", "clickhouse") for a in ("", ".after")),
]


@settings(max_examples=300, deadline=None)
@given(
    crash=st.none() | st.sampled_from(POINTS),
    transient=st.lists(st.tuples(st.sampled_from(POINTS), st.integers(1, 3)), max_size=3),
    lost_acks=st.lists(st.sampled_from(["pg", "graph", "clickhouse"]), max_size=2),
)
def test_any_crash_or_failure_ends_all_or_nothing_after_recovery(
    crash: str | None, transient: list[tuple[str, int]], lost_acks: list[str]
) -> None:
    world = make_world()
    wall = Wall()
    journal = FaultyJournal(world.faults, wall)
    for point, times in transient:
        world.faults.add(point, Transient(point), times)
    if crash is not None:
        world.faults.add(crash, Crash())
    for store in lost_acks:
        world.stores[store].lose_commit_ack += 1

    txn = new_id()
    outcome: str
    try:
        result = make_coordinator(world, journal).commit(txn, world.branches())
        outcome = "committed" if isinstance(result, Committed | CommittedPending) else "?"
    except Aborted:
        outcome = "aborted"
    except InDoubt:
        outcome = "in doubt"
    except Crash:
        outcome = "crashed"

    world.faults.plan.clear()  # the world heals; recovery runs until quiet
    for fake in world.stores.values():
        fake.lose_commit_ack = 0
    recovery = make_recovery(world, journal, wall)
    for _ in range(3):
        wall.ms += STALE_MS + 1
        recovery.run_once()

    applied = set(world.applied(txn).values())
    assert len(applied) == 1, f"partial commit after {outcome}: {world.applied(txn)}"
    assert world.leftover_prepared() == {}
    if outcome == "committed":
        assert applied == {True}
    if outcome == "aborted":
        assert applied == {False}
    record = journal.get(txn)
    assert record is None or record.state in (State.DONE, State.ABORTED)
    assert (record is not None and record.state is State.DONE) == (applied == {True})
