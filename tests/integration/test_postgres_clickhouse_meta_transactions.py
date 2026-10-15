"""ERP: an order in PostgreSQL, its sales facts in ClickHouse, one meta-transaction.

PostgreSQL commits through 2PC; ClickHouse rows are inserted after the decision
from the journal payload, by the coordinator or, after a crash, by recovery.
"""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from faker import Faker

from fakes import Crash
from rhizodat import Aborted, Committed, CommittedPending, Retry, Rhizodat, UnitOfWork
from rhizodat.adapters.clickhouse import ClickHouseStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.journal import State


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


@dataclass
class Erp:
    dat: Rhizodat
    pg: PostgresStore
    ch: ClickHouseStore
    journal: PostgresJournal
    wall: Wall

    def orders(self) -> int:
        return int(self.pg.execute("SELECT count(*) AS n FROM orders").one()["n"])

    def facts(self) -> int:
        return int(self.ch.execute("SELECT count() AS n FROM sales").one()["n"])

    def state(self, txn: uuid.UUID) -> State | None:
        record = self.journal.get(txn)
        return None if record is None else record.state

    def recover(self) -> Any:
        self.wall.ms += 60_000
        return self.dat.recovery(wall_ms=self.wall).run_once()


@pytest.fixture
def erp(pg_dsn: str, pg_schema: str, clickhouse_dsn: str) -> Iterator[Erp]:
    pg = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    pg.install()
    pg.execute("CREATE TABLE orders (id uuid PRIMARY KEY, customer text NOT NULL)")
    ch = ClickHouseStore(clickhouse_dsn, name="ch")
    ch.install()
    ch.execute(
        "CREATE TABLE sales (order_id UUID, sku String, qty UInt32, price Decimal(12, 2),"
        " dat_txn UUID, dat_tx_time Int64,"
        " INDEX dat_txn_idx dat_txn TYPE bloom_filter GRANULARITY 1)"
        " ENGINE = MergeTree ORDER BY (sku, order_id)"
    )
    wall = Wall()
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    no_wait = Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0)
    dat = Rhizodat(
        stores={"pg": pg, "ch": ch}, journal=journal, finish=no_wait, sleep=lambda s: None
    )
    yield Erp(dat, pg, ch, journal, wall)
    dat.close()


def place_order(uow: UnitOfWork, customer: str, lines: list[tuple[str, int, Decimal]]) -> uuid.UUID:
    order_id = uuid.uuid4()
    uow.store("pg").execute(
        "INSERT INTO orders (id, customer) VALUES (%s, %s)", (order_id, customer)
    )
    uow.store("ch").insert(
        "sales",
        [{"order_id": order_id, "sku": s, "qty": q, "price": p} for s, q, p in lines],
    )
    return order_id


def lines(faker: Faker, n: int = 3) -> list[tuple[str, int, Decimal]]:
    return [
        (faker.bothify("SKU-####"), faker.pyint(1, 9), Decimal(faker.numerify("##.##")))
        for _ in range(n)
    ]


def test_order_and_its_sales_facts_commit_together(erp: Erp, faker: Faker) -> None:
    outcome = erp.dat.transactional(place_order)(faker.name(), lines(faker))
    assert isinstance(outcome, Committed)
    assert (erp.orders(), erp.facts()) == (1, 3)
    assert erp.state(outcome.dat_txn) is State.DONE
    row = erp.ch.execute("SELECT DISTINCT dat_txn AS t FROM sales").one()
    assert row["t"] == outcome.dat_txn
    assert erp.pg.participant().prepared_gids() == []


def test_analytics_only_write_is_one_phase(erp: Erp, faker: Faker) -> None:
    def record_sale(uow: UnitOfWork) -> None:
        uow.store("ch").insert(
            "sales", [{"order_id": uuid.uuid4(), "sku": "X", "qty": 1, "price": Decimal("1.00")}]
        )

    outcome = erp.dat.transactional(record_sale)()
    assert isinstance(outcome, Committed)
    assert erp.facts() == 1
    assert erp.journal.get(outcome.dat_txn) is None  # one branch: no journal


def test_an_unknown_column_aborts_before_the_order_is_prepared(erp: Erp, faker: Faker) -> None:
    def with_colour(uow: UnitOfWork) -> None:
        uow.store("pg").execute(
            "INSERT INTO orders (id, customer) VALUES (%s, %s)", (uuid.uuid4(), faker.name())
        )
        uow.store("ch").insert(
            "sales",
            [
                {
                    "order_id": uuid.uuid4(),
                    "sku": "X",
                    "qty": 1,
                    "price": Decimal(1),
                    "colour": "red",
                }
            ],
        )

    with pytest.raises(Aborted, match="colour") as caught:
        erp.dat.transactional(with_colour)()
    assert not caught.value.retryable
    assert (erp.orders(), erp.facts()) == (0, 0)
    assert erp.pg.participant().prepared_gids() == []


def test_a_stage_failure_aborts_and_rolls_back_postgres(erp: Erp, faker: Faker) -> None:
    def to_missing_table(uow: UnitOfWork) -> None:
        place_order(uow, faker.name(), lines(faker))
        uow.store("ch").insert("returns", [{"order_id": uuid.uuid4()}])

    with pytest.raises(Aborted) as caught:
        erp.dat.transactional(to_missing_table)()
    assert not caught.value.retryable
    assert (erp.orders(), erp.facts()) == (0, 0)
    assert erp.pg.participant().prepared_gids() == []


def test_crash_while_inserting_facts_is_rolled_forward(erp: Erp, faker: Faker) -> None:
    participant = erp.ch.participant()
    original = participant.finish

    def die(*a: object, **k: object) -> None:
        raise Crash()

    participant.finish = die  # type: ignore[method-assign]
    uow = erp.dat.unit_of_work()
    place_order(uow, faker.name(), lines(faker, 4))
    with pytest.raises(Crash):
        uow.commit()
    participant.finish = original  # type: ignore[method-assign]
    assert (erp.orders(), erp.facts()) == (1, 0)  # the decision is durable, facts are not yet
    assert erp.state(uow.dat_txn) is State.COMMITTED

    report = erp.recover()
    assert report.finished == [uow.dat_txn]
    assert (erp.orders(), erp.facts()) == (1, 4)
    assert erp.state(uow.dat_txn) is State.DONE
    assert erp.recover().finished == []  # nothing left, nothing written twice
    assert erp.facts() == 4


def test_clickhouse_down_after_the_decision_is_pending_not_failed(erp: Erp, faker: Faker) -> None:
    from clickhouse_connect.driver.exceptions import OperationalError

    participant = erp.ch.participant()
    original = participant.finish

    def unavailable(*a: object, **k: object) -> None:
        raise OperationalError("connection refused")

    participant.finish = unavailable  # type: ignore[method-assign]
    outcome = erp.dat.transactional(place_order)(faker.name(), lines(faker, 2))
    participant.finish = original  # type: ignore[method-assign]

    assert isinstance(outcome, CommittedPending)
    assert outcome.value is not None
    assert (erp.orders(), erp.facts()) == (1, 0)
    assert erp.recover().finished == [outcome.dat_txn]
    assert erp.facts() == 2


def test_stores_open_from_connection_strings(clickhouse_dsn: str) -> None:
    dat = Rhizodat(stores={"ch": clickhouse_dsn})
    dat.store("ch").install()  # type: ignore[attr-defined]
    assert dat.store("ch").execute("SELECT 1 AS one").one() == {"one": 1}
    dat.close()
