"""Version preconditions on the native core: the repository compares when an
entity is written, and the core checks again at commit, under the entity's lock
(PostgreSQL's advisory lock, a write on drevo's latest version node).

On the native path a Unit of Work's writes only reach the store when it commits,
so of two writers that read one version the first to commit wins and the other
gets ``VersionConflict`` from its commit: the check the repository made earlier
saw no newer version yet.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined, call-arg, arg-type, union-attr"

import threading
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from faker import Faker

from rhizodat import Rhizodat, VersionConflict, entity
from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore
from rhizodat.core.parts import Part

native = pytest.importorskip("rhizodat_native")


@entity(stores={"main": ["title", "owner"]}, name="desk.NativeVersionedTicket")
class Ticket:
    title: str
    owner: str


@dataclass
class Desk:
    dat: Rhizodat
    kind: str


def _clear(dsn: str) -> None:
    store = PyDrevoStore(dsn, name="cleaner")
    try:
        store.execute(f"MATCH (v:{Part.of(Ticket, 'main').table}) DETACH DELETE v")
    finally:
        store.close()


@pytest.fixture(params=["postgres", "drevo"])
def desk(request: pytest.FixtureRequest, pg_dsn: str, pg_schema: str) -> Iterator[Desk]:
    main: Any
    dsn = None
    if request.param == "postgres":
        main = native.PostgresStore(pg_dsn, "main", pg_schema)
    else:
        dsn = request.getfixturevalue("drevo_bolt_dsn")
        _clear(dsn)
        main = native.DrevoStore(dsn, "main")
    dat = Rhizodat(stores={"main": main})
    yield Desk(dat, request.param)
    dat.close()
    if dsn is not None:
        _clear(dsn)


def test_a_stale_update_is_refused(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    t = tickets.add(Ticket(title=faker.sentence(), owner=faker.name())).value
    stale = tickets.get(t.dat_id)
    assert stale is not None
    t.owner = faker.name()
    tickets.update(t)
    stale.title = faker.sentence()
    with pytest.raises(VersionConflict) as refused:
        tickets.update(stale)
    assert refused.value.actual == tickets.get(t.dat_id).dat_version


def test_the_core_refuses_a_version_that_moved_after_the_check(desk: Desk, faker: Faker) -> None:
    """The repository's check passes, another writer commits, then this commit
    is refused by the core's check under the lock."""
    tickets = desk.dat.repository(Ticket)
    t = tickets.add(Ticket(title=faker.sentence(), owner=faker.name())).value
    first, second = tickets.get(t.dat_id), tickets.get(t.dat_id)
    uow = desk.dat.unit_of_work()
    first.owner = "first"
    uow.repository(Ticket).update(first)  # checked: still the latest
    second.owner = "second"
    tickets.update(second)  # commits in between
    with pytest.raises(VersionConflict) as refused:
        uow.commit()
    assert refused.value.dat_id == t.dat_id
    assert refused.value.expected == first.dat_version
    latest = tickets.get(t.dat_id)
    assert latest.owner == "second"
    assert refused.value.actual == latest.dat_version
    assert len(tickets.history(t.dat_id)) == 2


def test_two_writers_of_one_version_race_and_one_wins(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    t = tickets.add(Ticket(title=faker.sentence(), owner=faker.name())).value
    first, second = tickets.get(t.dat_id), tickets.get(t.dat_id)
    first.owner, second.owner = "first", "second"
    written, go = threading.Event(), threading.Event()
    results: dict[str, object] = {}
    calls: list[str] = []

    @desk.dat.transactional
    def slow(uow: Any) -> None:
        calls.append("first")
        uow.repository(Ticket).update(first)
        written.set()
        go.wait(10)

    @desk.dat.transactional
    def fast(uow: Any) -> None:
        calls.append("second")
        uow.repository(Ticket).update(second)

    def run(name: str, work: Any) -> None:
        try:
            work()
            results[name] = "committed"
        except VersionConflict as conflict:
            results[name] = conflict

    a = threading.Thread(target=run, args=("first", slow))
    a.start()
    assert written.wait(10)
    b = threading.Thread(target=run, args=("second", fast))
    b.start()
    b.join(10)
    go.set()
    a.join(20)
    assert not a.is_alive()
    assert not b.is_alive()

    assert sorted(type(r).__name__ for r in results.values()) == ["VersionConflict", "str"]
    winner = next(n for n, r in results.items() if r == "committed")
    assert winner == "second", "the first to commit wins: writes apply at commit"
    assert tickets.get(t.dat_id).owner == "second"
    assert len(tickets.history(t.dat_id)) == 2
    assert calls.count("first") <= 2, "a conflict is not retried"


def test_the_facades_take_one_lock_per_entity(pg_dsn: str, pg_schema: str, faker: Faker) -> None:
    """A Python facade's Unit of Work holds the entity's advisory lock from its
    write on; a native commit of the same entity waits for it, then sees the
    Python one's version."""
    from rhizodat.adapters.postgres import PostgresStore as PyPostgresStore

    py = Rhizodat(stores={"main": PyPostgresStore(pg_dsn, name="main", schema=pg_schema)})
    nat = Rhizodat(stores={"main": native.PostgresStore(pg_dsn, "main", pg_schema)})
    try:
        t = nat.repository(Ticket).add(Ticket(title=faker.sentence(), owner=faker.name())).value
        mine, theirs = nat.repository(Ticket).get(t.dat_id), py.repository(Ticket).get(t.dat_id)
        held = py.unit_of_work()
        theirs.owner = "python"
        held.repository(Ticket).update(theirs)  # takes the lock now

        outcome: list[object] = []

        def commit_native() -> None:
            uow = nat.unit_of_work()
            mine.owner = "native"
            uow.repository(Ticket).update(mine)
            try:
                uow.commit()
                outcome.append("committed")
            except VersionConflict as conflict:
                outcome.append(conflict)

        waiter = threading.Thread(target=commit_native)
        waiter.start()
        waiter.join(1.0)
        assert waiter.is_alive(), "the native commit waits for the Python one's lock"
        held.commit()
        waiter.join(10)
        assert not waiter.is_alive()
        assert isinstance(outcome[0], VersionConflict), outcome
        assert nat.repository(Ticket).get(t.dat_id).owner == "python"
    finally:
        py.close()
        nat.close()
