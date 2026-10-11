"""ERP ledger: an invoice in PostgreSQL, its ledger history in Iceberg, one meta-transaction.

The Iceberg catalog lives in the same PostgreSQL (``iceberg+sql://``) with a
local warehouse. Iceberg data files are written at stage and committed as a
snapshot after the decision; recovery on "another host" (a second store over
the same catalog) rolls forward what a crash left.
"""

import datetime as dt
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
from faker import Faker
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
    UUIDType,
)

from datorium import Aborted, Committed, Datorium, Retry, UnitOfWork
from datorium.adapters.iceberg import IcebergStore
from datorium.adapters.postgres import PostgresJournal, PostgresStore
from datorium.core.journal import State
from fakes import Crash

LEDGER = Schema(
    NestedField(1, "invoice_id", UUIDType(), required=True),
    NestedField(2, "account", StringType(), required=True),
    NestedField(3, "amount", DecimalType(14, 2), required=True),
    NestedField(4, "booked_at", TimestamptzType()),
    NestedField(5, "dat_txn", UUIDType()),
    NestedField(6, "dat_tx_time", LongType()),
)


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


@dataclass
class Ledger:
    dat: Datorium
    pg: PostgresStore
    lake: IcebergStore
    lake_dsn: str
    journal: PostgresJournal
    wall: Wall
    warehouse: Path
    table: str

    def invoices(self) -> int:
        return int(self.pg.execute("SELECT count(*) AS n FROM invoices").one()["n"])

    def entries(self) -> list[Mapping[str, Any]]:
        return self.lake.execute(self.table).all()

    def staged_files(self) -> list[Path]:
        return sorted(self.warehouse.rglob("dat-*.parquet"))

    def recover_elsewhere(self) -> Any:
        """Recovery with its own connections, as a worker on another host would run it."""
        self.wall.ms += 60_000
        other_lake = IcebergStore(self.lake_dsn, name="lake")
        other = Datorium(stores={"pg": self.pg, "lake": other_lake}, journal=self.journal)
        try:
            return other.recovery(wall_ms=self.wall).run_once()
        finally:
            other_lake.close()


@pytest.fixture
def ledger(pg_dsn: str, pg_schema: str, tmp_path: Path) -> Iterator[Ledger]:
    pg = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    pg.install()
    pg.execute("CREATE TABLE invoices (id uuid PRIMARY KEY, customer text NOT NULL)")
    server = urlsplit(pg_dsn)
    # The catalog tables are shared; a namespace per test keeps tests apart.
    lake_dsn = (
        f"iceberg+sql://{server.netloc}{server.path}?warehouse=file://{tmp_path / 'warehouse'}"
    )
    lake = IcebergStore(lake_dsn, name="lake")
    namespace = f"erp_{pg_schema}"
    lake.catalog.create_namespace(namespace)
    lake.catalog.create_table(f"{namespace}.ledger", LEDGER)
    wall = Wall()
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    no_wait = Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0)
    dat = Datorium(
        stores={"pg": pg, "lake": lake}, journal=journal, finish=no_wait, sleep=lambda s: None
    )
    table = f"{namespace}.ledger"
    yield Ledger(dat, pg, lake, lake_dsn, journal, wall, tmp_path / "warehouse", table)
    try:
        lake.catalog.drop_table(table)
        lake.catalog.drop_namespace(namespace)
    finally:
        dat.close()


def issue_invoice(
    uow: UnitOfWork, table: str, customer: str, lines: list[tuple[str, Decimal]]
) -> uuid.UUID:
    invoice_id = uuid.uuid4()
    uow.store("pg").execute(
        "INSERT INTO invoices (id, customer) VALUES (%s, %s)", (invoice_id, customer)
    )
    booked = dt.datetime.now(dt.UTC)
    uow.store("lake").append(
        table,
        [
            {"invoice_id": invoice_id, "account": a, "amount": m, "booked_at": booked}
            for a, m in lines
        ],
    )
    return invoice_id


def lines(faker: Faker, n: int = 2) -> list[tuple[str, Decimal]]:
    return [(faker.bothify("ACC-###"), Decimal(faker.numerify("###.##"))) for _ in range(n)]


def test_invoice_and_its_ledger_entries_commit_together(ledger: Ledger, faker: Faker) -> None:
    outcome = ledger.dat.transactional(issue_invoice)(
        ledger.table, faker.company(), lines(faker, 3)
    )
    assert isinstance(outcome, Committed)
    assert ledger.invoices() == 1
    entries = ledger.entries()
    assert len(entries) == 3
    assert {e["invoice_id"] for e in entries} == {outcome.value}
    assert {e["dat_txn"] for e in entries} == {outcome.dat_txn}
    assert ledger.journal.get(outcome.dat_txn).state is State.DONE  # type: ignore[union-attr]


def test_a_row_that_does_not_fit_aborts_and_leaves_no_files(ledger: Ledger, faker: Faker) -> None:
    def bad_amount(uow: UnitOfWork) -> None:
        uow.store("pg").execute(
            "INSERT INTO invoices (id, customer) VALUES (%s, %s)", (uuid.uuid4(), faker.company())
        )
        uow.store("lake").append(
            ledger.table, [{"invoice_id": uuid.uuid4(), "account": "A", "amount": "lots"}]
        )

    with pytest.raises(Aborted) as caught:
        ledger.dat.transactional(bad_amount)()
    assert not caught.value.retryable
    assert (ledger.invoices(), ledger.entries()) == (0, [])
    assert ledger.staged_files() == []
    assert ledger.pg.participant().prepared_gids() == []


def test_postgres_failing_to_prepare_deletes_the_staged_files(ledger: Ledger, faker: Faker) -> None:
    def duplicate(uow: UnitOfWork) -> None:
        same = uuid.uuid4()
        issue_invoice(uow, ledger.table, faker.company(), lines(faker))
        uow.store("pg").execute(
            "INSERT INTO invoices (id, customer) VALUES (%s, %s), (%s, %s)",
            (same, "a", same, "b"),
        )

    with pytest.raises(Exception, match="duplicate key"):
        ledger.dat.transactional(duplicate)()
    assert ledger.staged_files() == []
    assert ledger.entries() == []


def test_crash_before_the_snapshot_is_rolled_forward_elsewhere(
    ledger: Ledger, faker: Faker
) -> None:
    participant = ledger.lake.participant()

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    uow = ledger.dat.unit_of_work()
    issue_invoice(uow, ledger.table, faker.company(), lines(faker, 2))
    with pytest.raises(Crash):
        uow.commit()
    assert ledger.invoices() == 1
    assert ledger.entries() == []
    assert len(ledger.staged_files()) == 1  # durable before the decision, invisible

    report = ledger.recover_elsewhere()
    assert report.finished == [uow.dat_txn]
    assert len(ledger.entries()) == 2
    assert ledger.recover_elsewhere().finished == []
    assert len(ledger.entries()) == 2


def test_crash_before_the_decision_is_presumed_aborted_and_cleaned(
    ledger: Ledger, faker: Faker
) -> None:
    journal = ledger.journal
    original = journal.transition

    def crash_on_decision(dat_txn: uuid.UUID, expected: State, new: State, **kw: Any) -> Any:
        if new is State.COMMITTED:
            raise Crash()
        return original(dat_txn, expected, new, **kw)

    journal.transition = crash_on_decision  # type: ignore[method-assign]
    uow = ledger.dat.unit_of_work()
    issue_invoice(uow, ledger.table, faker.company(), lines(faker))
    with pytest.raises(Crash):
        uow.commit()
    journal.transition = original  # type: ignore[method-assign]
    assert len(ledger.staged_files()) == 1

    report = ledger.recover_elsewhere()
    assert report.aborted == [uow.dat_txn]
    assert (ledger.invoices(), ledger.entries()) == (0, [])
    assert ledger.staged_files() == []


def test_a_concurrent_catalog_commit_is_retried_after_the_decision(
    ledger: Ledger, faker: Faker
) -> None:
    from pyiceberg.exceptions import CommitFailedException

    participant = ledger.lake.participant()
    original = participant.finish
    conflicts = iter([CommitFailedException("metadata pointer moved")])

    def conflict_once(*a: Any, **k: Any) -> None:
        for exc in conflicts:
            raise exc
        original(*a, **k)

    participant.finish = conflict_once  # type: ignore[method-assign]
    outcome = ledger.dat.transactional(issue_invoice)(ledger.table, faker.company(), lines(faker))
    assert isinstance(outcome, Committed)
    assert len(ledger.entries()) == 2
