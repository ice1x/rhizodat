"""Scenario: an ERP over PostgreSQL and ClickHouse with valid time, on either facade.

Customers and orders keep their bookkeeping fields in PostgreSQL and their money in
ClickHouse, where the revenue is summed. A price agreement has valid time: the
period it holds for is stored with it, so an order is priced by the agreement
that holds on its delivery day, and a rebate entered later wins for its own days
only while what was known before it stays readable. The scenario runs end to end
the way an application would: migrate the schema, agree prices, place orders in
one transaction priced as of their delivery days (an interrupted batch places
none), sum the revenue per SKU natively in ClickHouse over the latest version of
each order, cancel an order, and work the same books from both facades in turn.

The same scenario runs on the Python facade and on the native facade (the core
in Rust under the same API), so both must agree.
"""

# @entity adds dat_id, the validity fields and the service times at runtime.
# mypy: disable-error-code="attr-defined,call-arg"

import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime, timedelta

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(stores={"books": ["name", "segment"]}, name="erp.Customer")
class Customer:
    name: str
    segment: str


@entity(
    stores={"books": ["customer", "sku", "currency"], "ledger": ["unit_cents"]},
    name="erp.PriceAgreement",
    valid_time=True,
)
class PriceAgreement:
    customer: Ref["Customer"]
    sku: str
    currency: str
    unit_cents: int


@entity(
    stores={
        "books": ["customer", "quantity", "delivery_on", "status"],
        "ledger": ["sku", "unit_cents", "total_cents"],
    },
    name="erp.Order",
    soft_delete=True,
)
class Order:
    customer: Ref["Customer"]
    quantity: int
    delivery_on: date
    status: str  # placed | shipped
    sku: str
    unit_cents: int
    total_cents: int


ORDERS_LEDGER = Part.of(Order, "ledger").table

SCHEMA = [
    Migration.of(
        1,
        "catalogue",
        [
            Step("books", "sql", "CREATE TABLE erp_sku (sku text PRIMARY KEY, name text NOT NULL)"),
            Step(
                "books", "sql", "INSERT INTO erp_sku VALUES ('A-100', 'anvil'), ('B-200', 'bolt')"
            ),
            Step(
                "ledger",
                "sql",
                "CREATE TABLE erp_fx (day Date, currency String, rate Float64)"
                " ENGINE = MergeTree ORDER BY (currency, day)",
            ),
        ],
        description="the SKU catalogue and the FX table",
    )
]


def day(n: int) -> date:
    return date(2026, 3, 1) + timedelta(days=n)


def at(d: date) -> datetime:
    """A valid-time moment on a delivery day (periods are stored as timestamps)."""
    return datetime(d.year, d.month, d.day, 12, tzinfo=UTC)


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def python_facade(pg_dsn: str, pg_schema: str, journal_schema: str, ch_dsn: str) -> Rhizodat:
    from rhizodat.adapters.clickhouse import ClickHouseStore
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    ledger = ClickHouseStore(ch_dsn, name="ledger")
    ledger.install()
    return Rhizodat(
        stores={"books": PostgresStore(pg_dsn, name="books", schema=pg_schema), "ledger": ledger},
        journal=PostgresJournal(pg_dsn, schema=journal_schema),
    )


def native_facade(pg_dsn: str, pg_schema: str, journal_schema: str, ch_dsn: str) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "books": native.PostgresStore(pg_dsn, "books", pg_schema),
            "ledger": native.ClickHouseStore(ch_dsn, "ledger"),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )


FACADES: dict[str, Callable[..., Rhizodat]] = {"python": python_facade, "native": native_facade}


@pytest.fixture(params=list(FACADES))
def dat(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    clickhouse_dsn: str,
) -> Iterator[Rhizodat]:
    d = FACADES[request.param](pg_dsn, pg_schema, journal_schema, clickhouse_dsn)
    d.migrate(SCHEMA)
    yield d
    d.close()


def agree(dat: Rhizodat, faker: Faker) -> tuple[Customer, PriceAgreement]:
    """A customer buying anvils at 50.00 from day 0 on, open-ended."""
    customer = Customer(name=faker.company(), segment="industry")
    dat.repository(Customer).add(customer)
    deal = PriceAgreement(
        customer=Ref.to(customer),
        sku="A-100",
        currency="EUR",
        unit_cents=5000,
        valid_from=at(day(0)),
        valid_to=None,
    )
    dat.repository(PriceAgreement).add(deal)
    return customer, deal


def rebate(dat: Rhizodat, deal: PriceAgreement, *, from_day: int, to_day: int, cents: int) -> None:
    """A new version of the agreement that holds for its own days only."""
    dat.repository(PriceAgreement).update(
        PriceAgreement(
            dat_id=deal.dat_id,
            customer=deal.customer,
            sku=deal.sku,
            currency=deal.currency,
            unit_cents=cents,
            valid_from=at(day(from_day)),
            valid_to=at(day(to_day)),
        )
    )


def place(
    dat: Rhizodat,
    customer: Customer,
    deal: PriceAgreement,
    deliveries: list[tuple[int, int]],
    *,
    fail_after: int | None = None,
) -> list[Order]:
    """One transaction: an order per (day, quantity), priced by the agreement that holds
    on its delivery day. A failure places none of them."""
    prices = dat.repository(PriceAgreement)
    orders = []
    for n, quantity in deliveries:
        holding = prices.get(deal.dat_id, as_of=at(day(n)))
        assert holding is not None
        orders.append(
            Order(
                customer=Ref.to(customer),
                quantity=quantity,
                delivery_on=day(n),
                status="placed",
                sku=holding.sku,
                unit_cents=holding.unit_cents,
                total_cents=holding.unit_cents * quantity,
            )
        )

    @dat.transactional
    def batch(uow: UnitOfWork) -> None:
        repo = uow.repository(Order)
        for i, order in enumerate(orders):
            if fail_after is not None and i == fail_after:
                raise RuntimeError("batch interrupted")
            repo.add(order)

    batch()
    return orders


def revenue_per_sku(dat: Rhizodat) -> dict[str, int]:
    """Native ClickHouse: the latest version of each order, cancelled ones excluded."""
    rows = (
        dat.store("ledger")
        .execute(
            "SELECT sku, sum(total) AS revenue FROM ("
            "  SELECT argMax(sku, dat_tx_time) AS sku, argMax(total_cents, dat_tx_time) AS total,"
            "         argMax(dat_deleted, dat_tx_time) AS gone"
            f"  FROM {ORDERS_LEDGER} GROUP BY dat_id) WHERE NOT gone GROUP BY sku ORDER BY sku"
        )
        .all()
    )
    return {str(r["sku"]): int(r["revenue"]) for r in rows}


def test_the_schema_is_migrated_once_and_the_history_says_so(dat: Rhizodat) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(SCHEMA)
    assert (report.applied, report.already) == ((), (1,))


def test_a_rebate_holds_for_its_days_and_the_past_stays_known(dat: Rhizodat, faker: Faker) -> None:
    _, deal = agree(dat, faker)
    before_rebate = datetime.now(UTC)
    time.sleep(0.01)
    rebate(dat, deal, from_day=10, to_day=20, cents=4000)

    prices = dat.repository(PriceAgreement)
    on = {n: prices.get(deal.dat_id, as_of=at(day(n))) for n in (5, 10, 19, 20)}
    assert {n: p.unit_cents if p else None for n, p in on.items()} == {
        5: 5000,
        10: 4000,
        19: 4000,
        20: 5000,
    }
    assert on[10] is not None
    assert (on[10].valid_from, on[10].valid_to) == (at(day(10)), at(day(20)))
    then = prices.get(deal.dat_id, as_of=at(day(15)), known_at=before_rebate)
    assert then is not None
    assert then.unit_cents == 5000
    assert [p.valid_from for p in prices.history(deal.dat_id)] == [at(day(0)), at(day(10))]


def test_orders_are_priced_as_of_their_delivery_day_in_one_batch_or_none(
    dat: Rhizodat, faker: Faker
) -> None:
    customer, deal = agree(dat, faker)
    rebate(dat, deal, from_day=10, to_day=20, cents=4000)
    deliveries = [(5, 2), (12, 3), (25, 1)]

    with pytest.raises(RuntimeError, match="interrupted"):
        place(dat, customer, deal, deliveries, fail_after=2)
    assert revenue_per_sku(dat) == {}

    orders = place(dat, customer, deal, deliveries)
    assert [o.total_cents for o in orders] == [10_000, 12_000, 5_000]
    repo = dat.repository(Order)
    for order in orders:
        assert repo.get(order.dat_id) == order
    assert revenue_per_sku(dat) == {"A-100": 27_000}


def test_a_cancelled_order_leaves_the_revenue_and_stays_in_the_history(
    dat: Rhizodat, faker: Faker
) -> None:
    customer, deal = agree(dat, faker)
    orders = place(dat, customer, deal, [(1, 1), (2, 4)])
    repo = dat.repository(Order)
    placed_at = datetime.now(UTC)
    time.sleep(0.01)

    orders[0].status = "shipped"
    repo.update(orders[0])
    repo.delete(orders[1].dat_id)

    assert revenue_per_sku(dat) == {"A-100": 5_000}
    assert repo.get(orders[1].dat_id) is None
    gone = repo.get(orders[1].dat_id, include_deleted=True)
    assert gone is not None
    assert gone.dat_deleted_at is not None
    as_placed = repo.get(orders[0].dat_id, as_of=placed_at)
    assert as_placed is not None
    assert as_placed.status == "placed"
    assert [o.status for o in repo.history(orders[0].dat_id)] == ["placed", "shipped"]


def test_both_facades_keep_the_same_books(
    pg_dsn: str, pg_schema: str, journal_schema: str, clickhouse_dsn: str, faker: Faker
) -> None:
    """Prices agreed on the Python facade, orders placed natively, revenue read on both."""
    args = (pg_dsn, pg_schema, journal_schema, clickhouse_dsn)
    with python_facade(*args) as python_side:
        python_side.migrate(SCHEMA)
        customer, deal = agree(python_side, faker)
        rebate(python_side, deal, from_day=10, to_day=20, cents=4000)
    with native_facade(*args) as native_side:
        assert [m.id for m in native_side.schema_versions()] == [1]
        orders = place(native_side, customer, deal, [(5, 1), (15, 1)])
        assert [o.unit_cents for o in orders] == [5000, 4000]
        assert revenue_per_sku(native_side) == {"A-100": 9_000}
    with python_facade(*args) as python_side:
        assert revenue_per_sku(python_side) == {"A-100": 9_000}
        read = python_side.repository(Order).get(orders[1].dat_id)
        assert read == orders[1]
