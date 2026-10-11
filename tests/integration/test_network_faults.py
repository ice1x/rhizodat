"""IT task manager under real network failures (toxiproxy, compose profile "faults").

A task lives in PostgreSQL, its activity feed in ClickHouse and its dependency
graph in drevo. The fault tests elsewhere raise simulated errors inside the
adapters; here the network to the journal or to a store is cut at a chosen step
of the commit protocol, so the drivers see what they would see in production:
dropped connections and refused reconnects.
"""

import uuid
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any

import pytest
from faker import Faker

from datorium import Committed, CommittedPending, Datorium, InDoubt, Retry, UnitOfWork
from datorium.adapters.clickhouse import ClickHouseStore
from datorium.adapters.drevo import DrevoStore
from datorium.adapters.postgres import PostgresJournal, PostgresStore
from datorium.core.journal import BranchRecord, State
from toxiproxy import Proxy, Toxiproxy, through


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


@dataclass
class Tracker:
    dat: Datorium
    pg: PostgresStore
    feed: ClickHouseStore
    graph: DrevoStore
    journal: PostgresJournal
    wall: Wall
    label: str
    proxies: dict[str, Proxy]

    def tasks(self) -> int:
        return int(self.pg.execute("SELECT count(*) AS n FROM tasks").one()["n"])

    def activity(self) -> int:
        return int(self.feed.execute("SELECT count() AS n FROM activity").one()["n"])

    def blockers(self) -> int:
        q = f"MATCH (:{self.label})-[:BLOCKED_BY]->(b) RETURN count(b) AS n"
        return int(self.graph.execute(q).one()["n"])

    def state(self, txn: uuid.UUID) -> State | None:
        record = self.journal.get(txn)
        return None if record is None else record.state

    def recover(self) -> Any:
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()

    def cut_at(self, proxy: str, state: State, *, after: bool) -> None:
        """Disable ``proxy`` just before or just after the journal moves to ``state``."""
        journal = self.journal
        original = journal.transition

        def transition(dat_txn: uuid.UUID, expected: State, new: State, **kw: Any) -> Any:
            if new is state and not after:
                self.proxies[proxy].disable()
            record = original(dat_txn, expected, new, **kw)
            if new is state and after:
                self.proxies[proxy].disable()
            return record

        journal.transition = transition  # type: ignore[method-assign]
        self._restore_transition: Callable[[], None] = lambda: setattr(
            journal, "transition", original
        )

    def heal(self) -> None:
        for proxy in self.proxies.values():
            proxy.enable()
        restore = getattr(self, "_restore_transition", None)
        if restore is not None:
            restore()


@pytest.fixture
def tracker(
    toxiproxy: Toxiproxy,
    pg_dsn: str,
    pg_schema: str,
    clickhouse_dsn: str,
    drevo_bolt_dsn: str,
) -> Iterator[Tracker]:
    with ExitStack() as stack:
        proxies = {
            "journal": stack.enter_context(toxiproxy.proxy("dat_journal", "postgres:5432", 58401)),
            "pg": stack.enter_context(toxiproxy.proxy("dat_pg", "postgres:5432", 58402)),
            "feed": stack.enter_context(toxiproxy.proxy("dat_feed", "clickhouse:8123", 58403)),
            "graph": stack.enter_context(toxiproxy.proxy("dat_graph", "drevo:7687", 58404)),
        }
        pg = PostgresStore(through(pg_dsn, 58402), name="pg", schema=pg_schema)
        pg.install()
        pg.execute("CREATE TABLE tasks (id uuid PRIMARY KEY, title text NOT NULL)")
        feed = ClickHouseStore(through(clickhouse_dsn, 58403), name="feed")
        feed.install()
        feed.execute(
            "CREATE TABLE activity (task_id UUID, event String, dat_txn UUID, dat_tx_time Int64)"
            " ENGINE = MergeTree ORDER BY task_id"
        )
        graph = DrevoStore(through(drevo_bolt_dsn, 58404), name="graph")
        wall = Wall()
        journal = PostgresJournal(through(pg_dsn, 58401), schema=pg_schema, wall_ms=wall)
        fast = Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0)
        dat = Datorium(
            stores={"pg": pg, "feed": feed, "graph": graph},
            journal=journal,
            finish=fast,
            sleep=lambda s: None,
        )
        t = Tracker(dat, pg, feed, graph, journal, wall, f"Task{uuid.uuid4().hex[:8]}", proxies)
        try:
            yield t
        finally:
            t.heal()
            for gid in graph.participant().prepared_gids():
                if gid.startswith("dat:"):
                    graph.participant().abort(BranchRecord("graph", gid, True))
            dat.close()


def open_task(uow: UnitOfWork, label: str, title: str, blockers: list[str]) -> uuid.UUID:
    task_id = uuid.uuid4()
    uow.store("pg").execute("INSERT INTO tasks (id, title) VALUES (%s, %s)", (task_id, title))
    uow.store("feed").insert("activity", [{"task_id": task_id, "event": "opened"}])
    uow.store("graph").execute(
        f"CREATE (t:{label} {{dat_id: $id}}) WITH t UNWIND $names AS name "
        f"CREATE (t)-[:BLOCKED_BY]->(:{label}Blocker {{name: name}})",
        {"id": str(task_id), "names": blockers},
    )
    return task_id


def test_healthy_network_commits_all_three(tracker: Tracker, faker: Faker) -> None:
    outcome = tracker.dat.transactional(open_task)(tracker.label, faker.sentence(), ["design"])
    assert isinstance(outcome, Committed)
    assert (tracker.tasks(), tracker.activity(), tracker.blockers()) == (1, 1, 1)


def test_journal_lost_at_the_decision_is_in_doubt_then_presumed_aborted(
    tracker: Tracker, faker: Faker
) -> None:
    tracker.cut_at("journal", State.COMMITTED, after=False)
    uow = tracker.dat.unit_of_work()
    open_task(uow, tracker.label, faker.sentence(), ["review"])
    with pytest.raises(InDoubt):
        uow.commit()
    # Prepared in PostgreSQL and drevo (drevo's fence is up), nothing decided.
    assert tracker.pg.participant().prepared_gids() != []
    tracker.heal()
    assert tracker.state(uow.dat_txn) is State.PREPARED

    report = tracker.recover()
    assert report.aborted == [uow.dat_txn]
    assert (tracker.tasks(), tracker.activity(), tracker.blockers()) == (0, 0, 0)
    assert [g for g in tracker.pg.participant().prepared_gids() if g.startswith("dat:")] == []
    assert [g for g in tracker.graph.participant().prepared_gids() if g.startswith("dat:")] == []


def test_journal_lost_right_after_the_decision_keeps_the_commit(
    tracker: Tracker, faker: Faker
) -> None:
    """The decision is durable; only DONE cannot be written. Nothing is undone."""
    tracker.cut_at("journal", State.COMMITTED, after=True)
    outcome = tracker.dat.transactional(open_task)(tracker.label, faker.sentence(), ["qa"])
    assert isinstance(outcome, CommittedPending)
    assert (tracker.tasks(), tracker.activity(), tracker.blockers()) == (1, 1, 1)
    tracker.heal()
    assert tracker.state(outcome.dat_txn) is State.COMMITTED
    assert tracker.recover().finished == [outcome.dat_txn]
    assert tracker.state(outcome.dat_txn) is State.DONE
    assert tracker.activity() == 1


@pytest.mark.parametrize("store", ["pg", "feed", "graph"])
def test_store_lost_after_the_decision_is_pending_then_rolled_forward(
    tracker: Tracker, faker: Faker, store: str
) -> None:
    tracker.cut_at(store, State.COMMITTED, after=True)
    outcome = tracker.dat.transactional(open_task)(tracker.label, faker.sentence(), ["ops"])
    assert isinstance(outcome, CommittedPending)
    assert tracker.state(outcome.dat_txn) is State.COMMITTED
    tracker.heal()

    report = tracker.recover()
    assert report.finished == [outcome.dat_txn]
    assert (tracker.tasks(), tracker.activity(), tracker.blockers()) == (1, 1, 1)
    assert tracker.state(outcome.dat_txn) is State.DONE
    tracker.graph.execute(f"CREATE (:{tracker.label}Note)")  # drevo's prepared fence is gone
