"""ERP price agreements with valid time, in PostgreSQL plus each other store.

The period a price holds for (``valid_from`` / ``valid_to``) is stored with
every part, so each store must round-trip it exactly: PostgreSQL
``timestamptz``, ClickHouse ``DateTime64``, Iceberg ``timestamptz``, drevo
tagged JSON. A rebate entered later wins for its own days only, and what was
known before it is still readable.
"""

# @entity adds dat_id, the validity fields and the service times at runtime.
# mypy: disable-error-code="attr-defined, call-arg"

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from faker import Faker

from conftest import drevo_embedded_available
from rhizodat import Rhizodat, entity
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.unit_of_work import Store


@entity(
    stores={"books": ["customer", "price"], "other": ["currency", "note"]},
    name="erp.Price",
    valid_time=True,
)
class Price:
    customer: str
    price: Decimal
    currency: str
    note: str | None


def day(n: int) -> datetime:
    return datetime(2026, 1, 1, 8, 30, 15, 250_000, tzinfo=UTC) + timedelta(days=n)


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 15, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms


def other_store(kind: str, request: pytest.FixtureRequest, tmp_path: Path) -> Store:
    if kind == "clickhouse":
        from rhizodat.adapters.clickhouse import ClickHouseStore

        ch = ClickHouseStore(request.getfixturevalue("clickhouse_dsn"), name="other")
        ch.install()
        return ch
    if kind == "iceberg":
        from rhizodat.adapters.iceberg import IcebergStore

        lake = f"iceberg+sql://{tmp_path / 'c.db'}?warehouse=file://{tmp_path / 'wh'}"
        return IcebergStore(lake, name="other")
    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
    from rhizodat.adapters.drevo import DrevoStore

    return DrevoStore(f"drevo://{tmp_path / 'g.drevo'}", name="other")


@pytest.fixture(params=["clickhouse", "iceberg", "drevo"])
def dat(
    request: pytest.FixtureRequest, pg_dsn: str, pg_schema: str, tmp_path: Path
) -> Iterator[tuple[Rhizodat, Wall]]:
    wall = Wall()
    d = Rhizodat(
        stores={
            "books": PostgresStore(pg_dsn, name="books", schema=pg_schema),
            "other": other_store(request.param, request, tmp_path),
        },
        journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d, wall
    d.close()


def test_a_rebate_wins_for_its_days_and_the_past_stays_known(
    dat: tuple[Rhizodat, Wall], faker: Faker
) -> None:
    d, wall = dat
    prices = d.repository(Price)
    p = Price(
        customer=faker.company(),
        price=Decimal("10.00"),
        currency="EUR",
        note=None,
        valid_from=day(0),
        valid_to=None,
    )
    prices.add(p)
    before_rebate = datetime.fromtimestamp(wall.ms / 1000, UTC)
    wall.ms += 60_000

    rebate = Price(
        dat_id=p.dat_id,
        customer=p.customer,
        price=Decimal("8.00"),
        currency="EUR",
        note=faker.sentence(),
        valid_from=day(10),
        valid_to=day(20),
    )
    prices.update(rebate)

    on = {n: prices.get(p.dat_id, as_of=day(n)) for n in (5, 10, 19, 20)}
    assert {n: v.price if v else None for n, v in on.items()} == {
        5: Decimal("10.00"),
        10: Decimal("8.00"),
        19: Decimal("8.00"),
        20: Decimal("10.00"),
    }
    assert on[10] == rebate  # the period round-trips to the microsecond, in UTC
    assert on[10] is not None
    assert (on[10].valid_from, on[10].valid_to) == (day(10), day(20))
    then = prices.get(p.dat_id, as_of=day(15), known_at=before_rebate)
    assert then is not None
    assert then.price == Decimal("10.00")
    assert [v.valid_from for v in prices.history(p.dat_id)] == [day(0), day(10)]
