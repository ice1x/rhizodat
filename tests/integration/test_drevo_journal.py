"""Bug tracker with the journal in drevo: a bug in PostgreSQL, its duplicate links in a drevo graph.

The journal is a second drevo graph (a store's graph cannot hold it: its prepared
fence would block the coordinator), so the deployment needs no PostgreSQL for
coordination at all.
"""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from conftest import drevo_embedded_available
from datorium import Committed, Datorium, Retry, UnitOfWork
from datorium.adapters.drevo import DrevoJournal, DrevoStore
from datorium.adapters.postgres import PostgresStore
from datorium.core.journal import State
from fakes import Crash


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


@dataclass
class Tracker:
    dat: Datorium
    pg: PostgresStore
    graph: DrevoStore
    journal: DrevoJournal
    wall: Wall

    def bugs(self) -> int:
        return int(self.pg.execute("SELECT count(*) AS n FROM bugs").one()["n"])

    def links(self) -> int:
        return int(
            self.graph.execute("MATCH (:Bug)-[d:DUPLICATES]->() RETURN count(d) AS n").one()["n"]
        )

    def recover(self) -> Any:  # RecoveryReport
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()


@pytest.fixture
def tracker(pg_dsn: str, pg_schema: str, tmp_path: Path) -> Iterator[Tracker]:
    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed")
    pg = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    pg.install()
    pg.execute("CREATE TABLE bugs (id uuid PRIMARY KEY, summary text NOT NULL)")
    graph = DrevoStore(f"drevo://{tmp_path / 'bugs.drevo'}", name="graph")
    wall = Wall()
    journal = DrevoJournal(f"drevo://{tmp_path / 'journal.drevo'}", wall_ms=wall)
    dat = Datorium(
        stores={"pg": pg, "graph": graph},
        journal=journal,
        finish=Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
        sleep=lambda s: None,
    )
    yield Tracker(dat, pg, graph, journal, wall)
    dat.close()


def file_duplicate(uow: UnitOfWork, summary: str, original: str) -> uuid.UUID:
    bug_id = uuid.uuid4()
    uow.store("pg").execute("INSERT INTO bugs (id, summary) VALUES (%s, %s)", (bug_id, summary))
    uow.store("graph").execute(
        "CREATE (:Bug {dat_id: $id})-[:DUPLICATES]->(:Bug {dat_id: $original})",
        {"id": str(bug_id), "original": original},
    )
    return bug_id


def test_duplicate_report_commits_with_the_journal_in_drevo(tracker: Tracker, faker: Faker) -> None:
    outcome = tracker.dat.transactional(file_duplicate)(faker.sentence(), str(uuid.uuid4()))
    assert isinstance(outcome, Committed)
    assert (tracker.bugs(), tracker.links()) == (1, 1)
    record = tracker.journal.get(outcome.dat_txn)
    assert record is not None
    assert record.state is State.DONE


def test_crash_after_the_decision_is_rolled_forward_from_the_drevo_journal(
    tracker: Tracker, faker: Faker
) -> None:
    participant = tracker.graph.participant()
    original = participant.finish

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    uow = tracker.dat.unit_of_work()
    file_duplicate(uow, faker.sentence(), str(uuid.uuid4()))
    with pytest.raises(Crash):
        uow.commit()
    participant.finish = original  # type: ignore[method-assign]
    assert tracker.journal.get(uow.dat_txn).state is State.COMMITTED  # type: ignore[union-attr]

    report = tracker.recover()
    assert report.finished == [uow.dat_txn]
    assert (tracker.bugs(), tracker.links()) == (1, 1)
    assert tracker.journal.get(uow.dat_txn).state is State.DONE  # type: ignore[union-attr]


def test_crash_before_the_decision_is_presumed_aborted(tracker: Tracker, faker: Faker) -> None:
    journal = tracker.journal
    original = journal.transition

    def crash(dat_txn: uuid.UUID, expected: State, new: State, **kw: Any) -> Any:
        if new is State.COMMITTED:
            raise Crash()
        return original(dat_txn, expected, new, **kw)

    journal.transition = crash  # type: ignore[method-assign]
    uow = tracker.dat.unit_of_work()
    file_duplicate(uow, faker.sentence(), str(uuid.uuid4()))
    with pytest.raises(Crash):
        uow.commit()
    journal.transition = original  # type: ignore[method-assign]

    report = tracker.recover()
    assert report.aborted == [uow.dat_txn]
    assert (tracker.bugs(), tracker.links()) == (0, 0)
    tracker.graph.execute("CREATE (:Note)")  # the store's fence is gone
