"""IT task manager: workers claim tasks concurrently; serialization failures re-run the UoW."""

import threading
from collections.abc import Iterator

import pytest
from faker import Faker

from rhizodat import Committed, Retry, Rhizodat, UnitOfWork
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore


@pytest.fixture
def dat(pg_dsn: str, pg_schema: str, faker: Faker) -> Iterator[Rhizodat]:
    tasks = PostgresStore(pg_dsn, name="tasks", schema=pg_schema, isolation="serializable")
    tasks.install()
    tasks.execute("CREATE TABLE task (id int PRIMARY KEY, title text NOT NULL, claimed_by text)")
    for i in range(20):
        tasks.execute("INSERT INTO task (id, title) VALUES (%s, %s)", (i, faker.catch_phrase()))
    d = Rhizodat(
        stores={"tasks": tasks},
        journal=PostgresJournal(pg_dsn, schema=pg_schema),
        retry=Retry(attempts=30, deadline_s=30.0, base_delay_s=0.005, max_delay_s=0.05),
    )
    yield d
    d.close()


def test_concurrent_claims_never_double_book_a_task(dat: Rhizodat) -> None:
    attempts: dict[str, int] = {}
    lock = threading.Lock()

    @dat.transactional
    def claim(uow: UnitOfWork, worker: str) -> int:
        with lock:
            attempts[worker] = attempts.get(worker, 0) + 1
        branch = uow.store("tasks")
        row = branch.execute(
            "SELECT id FROM task WHERE claimed_by IS NULL ORDER BY id LIMIT 1"
        ).one()
        branch.execute("UPDATE task SET claimed_by = %s WHERE id = %s", (worker, row["id"]))
        return int(row["id"])

    results: dict[str, int] = {}
    barrier = threading.Barrier(8)

    def worker(name: str) -> None:
        barrier.wait()
        outcome = claim(worker=name)
        assert isinstance(outcome, Committed)
        results[name] = outcome.value  # type: ignore[assignment]

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 8
    assert len(set(results.values())) == 8  # every worker got its own task
    rows = (
        dat.store("tasks")
        .execute("SELECT id, claimed_by FROM task WHERE claimed_by IS NOT NULL ORDER BY id")
        .all()
    )
    assert {r["claimed_by"]: r["id"] for r in rows} == results
    assert sum(attempts.values()) > 8  # conflicts happened and were retried


def test_stores_opened_by_dsn_need_no_install_step(pg_dsn: str, pg_schema: str) -> None:
    """The service tables (dat_applied) appear on first use in a Unit of Work."""
    sep = "&" if "?" in pg_dsn else "?"
    dsn = f"{pg_dsn}{sep}schema={pg_schema}"
    with Rhizodat(stores={"a": dsn}, journal=PostgresJournal(pg_dsn, schema=pg_schema)) as d:
        d.store("a").execute("CREATE TABLE note (id int PRIMARY KEY)")

        @d.transactional
        def write(uow: UnitOfWork, n: int) -> None:
            uow.store("a").execute("INSERT INTO note VALUES (%s)", (n,))

        assert isinstance(write(1), Committed)
        assert isinstance(write(2), Committed)
        assert d.store("a").execute("SELECT count(*) AS n FROM note").one()["n"] == 2
