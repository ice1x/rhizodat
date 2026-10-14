"""IT task manager with a full analytics store: the write fails with StoreFull, and
nothing is written anywhere.

ClickHouse applies its rows after the commit decision, so once the decision is
taken a full disk there can only leave the meta-transaction pending. With
``min_free`` set, datorium asks the store first (``system.disks``) and refuses
the meta-transaction before anything is written. PostgreSQL cannot report its
free space; its own disk-full error (53100) aborts before the decision anyway.
"""

# @entity adds dat_id at runtime; mypy cannot see it.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator

import psycopg
import pytest
from faker import Faker

from datorium import Datorium, StoreFull, entity
from datorium.adapters.clickhouse import ClickHouseStore
from datorium.adapters.postgres import PostgresJournal, PostgresStore


@entity(stores={"work": ["summary", "status"], "activity": ["views"]}, name="full.Task")
class Task:
    summary: str
    status: str
    views: int


def open_dat(pg_dsn: str, pg_schema: str, clickhouse_dsn: str, min_free: int) -> Datorium:
    ch = ClickHouseStore(clickhouse_dsn, name="activity")
    ch.install()
    return Datorium(
        stores={"work": PostgresStore(pg_dsn, name="work", schema=pg_schema), "activity": ch},
        journal=PostgresJournal(pg_dsn, schema=pg_schema),
        min_free=min_free,
    )


@pytest.fixture
def full(pg_dsn: str, pg_schema: str, clickhouse_dsn: str) -> Iterator[Datorium]:
    """ClickHouse with less room than ``min_free`` asks for (more than any disk has)."""
    dat = open_dat(pg_dsn, pg_schema, clickhouse_dsn, min_free=1 << 62)
    yield dat
    dat.close()


def test_a_task_is_refused_when_the_activity_store_is_full(
    full: Datorium, faker: Faker, pg_dsn: str, pg_schema: str
) -> None:
    tasks = full.repository(Task)
    task = Task(summary=faker.bs(), status="new", views=0)

    with pytest.raises(StoreFull) as err:
        tasks.add(task)

    assert err.value.store == "activity"
    assert err.value.free is not None
    assert err.value.free < 1 << 62
    assert not err.value.retryable
    assert tasks.get(task.dat_id) is None
    with psycopg.connect(pg_dsn) as conn:
        rows = conn.execute(
            f'SELECT count(*) FROM "{pg_schema}".full_task WHERE dat_id = %s', (task.dat_id,)
        ).fetchone()
    assert rows == (0,)


def test_the_same_work_commits_when_there_is_room(
    pg_dsn: str, pg_schema: str, clickhouse_dsn: str, faker: Faker
) -> None:
    dat = open_dat(pg_dsn, pg_schema, clickhouse_dsn, min_free=1)
    try:
        tasks = dat.repository(Task)
        task = Task(summary=faker.bs(), status="new", views=0)
        tasks.add(task)
        assert tasks.get(task.dat_id) == task
    finally:
        dat.close()


def test_postgres_disk_full_inside_the_work_is_store_full(
    pg_dsn: str, pg_schema: str, clickhouse_dsn: str
) -> None:
    dat = open_dat(pg_dsn, pg_schema, clickhouse_dsn, min_free=1)
    calls = []

    @dat.transactional
    def write(uow: object) -> None:
        calls.append(1)
        uow.store("work").execute("SELECT 1")  # type: ignore[attr-defined]
        raise psycopg.errors.DiskFull("could not extend file: No space left on device")

    try:
        with pytest.raises(StoreFull) as err:
            write()
        assert (err.value.store, calls) == ("work", [1])
    finally:
        dat.close()
