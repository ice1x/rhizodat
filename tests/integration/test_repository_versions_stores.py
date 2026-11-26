"""Version preconditions against real stores: two writers that read the same
version of an entity race, and exactly one of them wins.

PostgreSQL serialises them with an advisory lock (the second waits, then reads
the first one's version); drevo with a write on the latest version node (the
second to commit conflicts, is retried, and its retry reads the new version).
Either way the loser gets ``VersionConflict`` and no write is lost.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined, call-arg, arg-type"

import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from drevo_parts import drevo_parts_dsn
from rhizodat import Rhizodat, VersionConflict, entity
from rhizodat.adapters.drevo import DrevoStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.parts import Part


@entity(stores={"main": ["title", "owner"]}, name="desk.VersionedTicket")
class Ticket:
    title: str
    owner: str


@dataclass
class Desk:
    dat: Rhizodat
    kind: str


def _clear(store: object) -> None:
    label = Part.of(Ticket, "main").table
    if isinstance(store, DrevoStore):
        store.execute(f"MATCH (v:{label}) DETACH DELETE v")


@pytest.fixture(params=["postgres", "file", "bolt"])
def desk(
    request: pytest.FixtureRequest, tmp_path: Path, pg_dsn: str, pg_schema: str
) -> Iterator[Desk]:
    main: Any
    if request.param == "postgres":
        main = PostgresStore(pg_dsn, name="main", schema=pg_schema)
    else:
        main = DrevoStore(drevo_parts_dsn(request, tmp_path), name="main")
    _clear(main)
    dat = Rhizodat(stores={"main": main}, journal=PostgresJournal(pg_dsn, schema=pg_schema))
    yield Desk(dat, request.param)
    _clear(main)
    dat.close()


def test_a_stale_update_is_refused(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    t = tickets.add(Ticket(title=faker.sentence(), owner=faker.name())).value
    stale = tickets.get(t.dat_id)
    assert stale is not None
    t.owner = faker.name()
    tickets.update(t)
    stale.title = faker.sentence()
    with pytest.raises(VersionConflict):
        tickets.update(stale)
    latest = tickets.get(t.dat_id)
    assert latest is not None
    assert (latest.title, latest.owner) == (t.title, t.owner)


def test_two_writers_of_one_version_race_and_one_wins(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    t = tickets.add(Ticket(title=faker.sentence(), owner=faker.name())).value
    first, second = tickets.get(t.dat_id), tickets.get(t.dat_id)
    assert first is not None
    assert second is not None
    first.owner, second.owner = "first", "second"

    locked, go = threading.Event(), threading.Event()
    results: dict[str, object] = {}

    @desk.dat.transactional
    def slow(uow: Any) -> None:
        uow.repository(Ticket).update(first)
        locked.set()
        go.wait(10)

    @desk.dat.transactional
    def fast(uow: Any) -> None:
        uow.repository(Ticket).update(second)

    def run(name: str, work: Any) -> None:
        try:
            work()
            results[name] = "committed"
        except VersionConflict as conflict:
            results[name] = conflict

    a = threading.Thread(target=run, args=("first", slow))
    a.start()
    assert locked.wait(10)
    b = threading.Thread(target=run, args=("second", fast))
    b.start()
    b.join(1.0)  # PostgreSQL: still waiting on the lock; drevo: done
    go.set()
    a.join(20)
    b.join(20)
    assert not a.is_alive()
    assert not b.is_alive()

    outcomes = sorted(type(r).__name__ for r in results.values())
    assert outcomes == ["VersionConflict", "str"], results
    winner = next(n for n, r in results.items() if r == "committed")
    latest = tickets.get(t.dat_id)
    assert latest is not None
    assert latest.owner == winner
    assert len(tickets.history(t.dat_id)) == 2
    if desk.kind == "postgres":
        assert winner == "first"  # the second waited for the first one's lock
