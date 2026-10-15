"""IT task manager on the Repository: tasks in PostgreSQL, their activity counters in ClickHouse.

Each task's workflow fields live in PostgreSQL (2PC) and its activity fields in
ClickHouse (applied after the decision from the journal payload), so every
write is a meta-transaction over both. A crash after the decision leaves the
ClickHouse part behind until recovery; reads stay consistent meanwhile.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from faker import Faker

from fakes import Crash
from rhizodat import CommittedPending, Retry, Rhizodat, entity
from rhizodat.adapters.clickhouse import ClickHouseStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.repository import NotRestorable

DAY_MS = 86_400_000


@entity(
    stores={
        "work": ["summary", "status", "assignee"],
        "activity": ["views", "last_viewed", "watchers"],
    },
    name="it.Ticket",
    soft_delete=timedelta(days=7),
)
class Ticket:
    summary: str
    status: str
    assignee: str | None
    views: int
    last_viewed: datetime | None
    watchers: list[str]


@entity(stores={"work": ["title"], "activity": ["hits"]}, name="it.Board")
class Board:
    title: str
    hits: int


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 14, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@dataclass
class Desk:
    dat: Rhizodat
    pg: PostgresStore
    ch: ClickHouseStore
    wall: Wall

    def recover(self) -> object:
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()

    def ch_rows(self, table: str, dat_id: object) -> int:
        row = self.ch.execute(
            f"SELECT count() AS n FROM {table} WHERE dat_id = {{i:UUID}}", {"i": dat_id}
        ).one()
        return int(row["n"])


@pytest.fixture
def desk(pg_dsn: str, pg_schema: str, clickhouse_dsn: str) -> Iterator[Desk]:
    wall = Wall()
    pg = PostgresStore(pg_dsn, name="work", schema=pg_schema)
    ch = ClickHouseStore(clickhouse_dsn, name="activity")
    ch.install()
    no_wait = Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0)
    dat = Rhizodat(
        stores={"work": pg, "activity": ch},
        journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
        finish=no_wait,
        sleep=lambda s: None,
    )
    yield Desk(dat, pg, ch, wall)
    dat.close()


def new_ticket(faker: Faker) -> Ticket:
    return Ticket(
        summary=faker.bs(),
        status="new",
        assignee=None,
        views=0,
        last_viewed=None,
        watchers=[],
    )


def test_a_ticket_is_worked_on_across_both_stores(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    ticket = new_ticket(faker)
    tickets.add(ticket)
    opened = desk.wall.now()

    desk.wall.ms += 60_000
    ticket.status, ticket.assignee = "in progress", faker.user_name()
    ticket.views, ticket.last_viewed = 3, desk.wall.now()
    ticket.watchers = [ticket.assignee, faker.user_name()]
    tickets.update(ticket)

    current = tickets.get(ticket.dat_id)
    assert current == ticket
    assert current is not None
    assert (current.dat_created_at, current.dat_updated_at) == (opened, desk.wall.now())
    at_open = tickets.get(ticket.dat_id, as_of=opened)
    assert at_open is not None
    assert (at_open.status, at_open.views, at_open.watchers) == ("new", 0, [])
    assert [t.views for t in tickets.history(ticket.dat_id)] == [0, 3]
    assert desk.ch_rows("it_ticket", ticket.dat_id) == 2


def test_a_board_is_erased_from_both_stores(desk: Desk, faker: Faker) -> None:
    boards = desk.dat.repository(Board)
    board = Board(title=faker.catch_phrase(), hits=faker.random_int(1, 99))
    boards.add(board)
    board.hits += 1
    boards.update(board)

    boards.delete(board.dat_id)

    assert boards.get(board.dat_id) is None
    assert boards.history(board.dat_id) == []
    assert desk.ch_rows("it_board", board.dat_id) == 0
    with pytest.raises(NotRestorable):
        boards.restore(board.dat_id)


def test_a_closed_ticket_is_restorable_for_its_window_then_purged(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    spam, mistake = new_ticket(faker), new_ticket(faker)
    tickets.add(spam)
    tickets.add(mistake)
    tickets.delete(spam.dat_id)
    tickets.delete(mistake.dat_id)

    desk.wall.ms += DAY_MS
    assert tickets.restore(mistake.dat_id).value == mistake
    assert tickets.get(spam.dat_id) is None

    desk.wall.ms += 6 * DAY_MS  # the type's own window (7 days) wins over the default
    assert tickets.purge_expired() == 0
    desk.wall.ms += 1
    assert tickets.purge_expired() == 1
    assert tickets.get(spam.dat_id, include_deleted=True) is None
    assert desk.ch_rows("it_ticket", spam.dat_id) == 0
    assert tickets.get(mistake.dat_id) == mistake


def test_activity_left_behind_by_a_crash_is_rolled_forward(desk: Desk, faker: Faker) -> None:
    tickets = desk.dat.repository(Ticket)
    ticket = new_ticket(faker)
    tickets.add(ticket)
    participant = desk.ch.participant()
    original = participant.finish

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    desk.wall.ms += 60_000
    ticket.status, ticket.views = "done", 9
    with pytest.raises(Crash):
        tickets.update(ticket)
    participant.finish = original  # type: ignore[method-assign]

    # PostgreSQL has the new version, ClickHouse not yet: the read falls back to the
    # last version both stores agree on.
    seen = tickets.get(ticket.dat_id)
    assert seen is not None
    assert (seen.status, seen.views) == ("new", 0)

    desk.recover()
    done = tickets.get(ticket.dat_id)
    assert done is not None
    assert (done.status, done.views) == ("done", 9)
    assert desk.ch_rows("it_ticket", ticket.dat_id) == 2


def test_clickhouse_down_after_the_decision_is_pending(desk: Desk, faker: Faker) -> None:
    from clickhouse_connect.driver.exceptions import OperationalError

    tickets = desk.dat.repository(Ticket)
    participant = desk.ch.participant()
    original = participant.finish

    def unavailable(*a: object, **k: object) -> None:
        raise OperationalError("connection refused")

    participant.finish = unavailable  # type: ignore[method-assign]
    ticket = new_ticket(faker)
    outcome = tickets.add(ticket)
    participant.finish = original  # type: ignore[method-assign]

    assert isinstance(outcome, CommittedPending)
    assert tickets.get(ticket.dat_id) is None  # not every part is there yet
    desk.recover()
    assert tickets.get(ticket.dat_id) == ticket
