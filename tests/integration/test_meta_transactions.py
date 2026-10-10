"""Meta-transactions over fake stores with a durable SQLite journal and real threads."""

import threading
import uuid
from pathlib import Path

import pytest
from faker import Faker

from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import Aborted, Committed, Coordinator, Recovery
from datorium.core.identity import new_id
from datorium.core.journal import SqliteJournal, State
from datorium.core.retry import Retry
from fakes import Crash, Transient, World


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


def erp_world() -> World:
    world = World()
    world.store("orders_pg", two_phase=True)  # order header and lines
    world.store("graph", two_phase=True)  # customer -> order -> product links
    world.store("analytics", two_phase=False)  # revenue facts
    return world


def coordinator(world: World, journal: SqliteJournal) -> Coordinator:
    return Coordinator(
        journal=journal,
        participants=world.participants(),
        clock=HybridLogicalClock(),
        finish=Retry(attempts=3, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
        sleep=lambda s: None,
    )


def test_erp_order_survives_a_coordinator_crash_after_the_decision(
    tmp_path: Path, faker: Faker
) -> None:
    world = erp_world()
    wall = Wall()
    path = tmp_path / "journal.db"
    app_host = SqliteJournal(path, wall_ms=wall)

    order = faker.bothify("SO-#####")
    branches = world.branches(lambda store: [f"{store}:{order}"])
    world.faults.add("graph.finish", Crash())  # the app host dies while finishing
    txn = new_id()
    with pytest.raises(Crash):
        coordinator(world, app_host).commit(txn, branches)
    assert world.applied(txn) == {"orders_pg": True, "graph": False, "analytics": False}

    # Another host runs recovery against the same journal file.
    wall.ms += 60_000
    ops_host = SqliteJournal(path, wall_ms=wall)
    report = Recovery(journal=ops_host, participants=world.participants(), wall_ms=wall).run_once()

    assert report.finished == [txn]
    assert world.applied(txn) == dict.fromkeys(world.stores, True)
    assert world.stores["analytics"].rows_of(txn) == [f"analytics:{order}"]
    record = ops_host.get(txn)
    assert record is not None
    assert record.state is State.DONE
    app_host.close()
    ops_host.close()


def test_bug_tracker_concurrent_writers_never_half_commit(tmp_path: Path) -> None:
    """Many threads commit at once; some hit transient conflicts. Every one is all or nothing."""
    world = World()
    world.store("bugs_pg", two_phase=True)
    world.store("events", two_phase=False)
    journal = SqliteJournal(tmp_path / "journal.db")
    world.faults.add("bugs_pg.prepare", Transient("prepared fence"), times=5)

    lock = threading.Lock()  # FakeStore is not thread-safe; the coordinator is driven concurrently
    outcomes: dict[uuid.UUID, str] = {}

    class Locked:
        def __init__(self, inner: object) -> None:
            self.inner = inner

        def __getattr__(self, name: str) -> object:
            attr = getattr(self.inner, name)
            if not callable(attr):
                return attr

            def call(*a: object, **k: object) -> object:
                with lock:
                    return attr(*a, **k)

            return call

    participants = {n: Locked(p) for n, p in world.participants().items()}
    coord = Coordinator(
        journal=journal,
        participants=participants,  # type: ignore[arg-type]
        clock=HybridLogicalClock(),
        sleep=lambda s: None,
    )

    def work(i: int) -> None:
        txn = new_id()
        branches = [Locked(b) for b in world.branches(lambda s: [f"{s}:bug-{i}"])]
        try:
            result = coord.commit(txn, branches)  # type: ignore[arg-type]
            outcomes[txn] = "committed" if isinstance(result, Committed) else "pending"
        except Aborted as err:
            outcomes[txn] = "retry" if err.retryable else "aborted"

    threads = [threading.Thread(target=work, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(outcomes.values()).count("retry") == 5
    for txn, outcome in outcomes.items():
        applied = set(world.applied(txn).values())
        assert applied == ({True} if outcome == "committed" else {False})
    assert world.leftover_prepared() == {}
    journal.close()
