"""Entity parts and repositories on the native core, over PostgreSQL.

``rhizodat_native.PartSpec`` names one store's share of an entity type; a native
``PostgresStore`` reads committed versions (``read_part``, ``part_history``,
``part_versions``, ``expired_parts``) and a ``PostgresUnit`` buffers writes
(``write_part``, ``erase_part``) that its commit applies inside the
meta-transaction. On top, a native facade's ``repository(T)`` and
``uow.repository(T)`` run the same repository logic as the Python facade.

The tables are the Python adapter's, so a version written by either facade is
read by the other. Docker-free tests cover the buffer and the value conversion;
the rest run against the compose PostgreSQL. Install the module with
``uv sync --group native`` (needs a Rust toolchain).
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,union-attr,call-arg"

import time
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.postgres import PostgresStore as PyPostgresStore
from rhizodat.core.parts import Part
from rhizodat.core.repository import EntityExists, EntityNotFound

native = pytest.importorskip("rhizodat_native")


@entity(stores={"pg": ["name", "email"]}, name="crm.Customer")
class Customer:
    name: str
    email: str


@entity(
    stores={"pg": ["customer", "amount", "placed_on", "paid_at", "lines", "token"]},
    name="erp.Order",
    soft_delete=True,
)
class Order:
    customer: Ref["Customer"]
    amount: Decimal
    placed_on: date
    paid_at: datetime | None
    lines: list[dict[str, int]]
    token: bytes


@entity(stores={"pg": ["customer", "price"]}, name="erp.PriceAgreement", valid_time=True)
class PriceAgreement:
    customer: str
    price: Decimal


@entity(stores={"lake": ["v"]}, name="t.Lake")
class InLake:
    v: int


def spec(type_: type, store: str = "pg") -> object:
    from rhizodat.core.native_parts import part_spec

    return part_spec(native, Part.of(type_, store))


# --- Docker-free ------------------------------------------------------------------------


def test_a_spec_takes_plain_names_and_known_types() -> None:
    with pytest.raises(native.InvalidUnitOfWork, match="table"):
        native.PartSpec("t.T", "bad name", [])
    with pytest.raises(native.InvalidUnitOfWork, match="column type"):
        native.PartSpec("t.T", "t", [("a", "integer")])
    with pytest.raises(native.InvalidUnitOfWork, match="service column"):
        native.PartSpec("t.T", "t", [("dat_txn", "uuid")])
    s = native.PartSpec("t.T", "t", [("a", "text"), ("b", "jsonb")])
    assert (s.table, s.fields) == ("t", ["a", "b"])


def test_a_part_spec_follows_the_python_adapters_column_types() -> None:
    s = spec(Order)
    assert s.table == "erp_order"
    assert s.fields == ["customer", "amount", "placed_on", "paid_at", "lines", "token"]


def test_a_units_own_writes_read_back_with_their_python_types(faker: Faker) -> None:
    unit = native.PostgresStore("postgresql://u:p@127.0.0.1:5432/db", "pg").unit()
    s = spec(Order)
    dat_id, owner = uuid.uuid4(), uuid.uuid4()
    assert unit.own_part(s, dat_id) == (False, None)
    values = {
        "customer": owner,
        "amount": Decimal("12.3400"),
        "placed_on": date(2026, 10, 31),
        "paid_at": datetime(2026, 10, 31, 13, 14, 15, 123456, tzinfo=UTC),
        "lines": [{"sku": faker.random_int(1, 999)}],
        "token": b"\x00\xff",
    }
    unit.write_part(s, dat_id, values, created=None)
    touched, own = unit.own_part(s, dat_id)
    assert touched
    assert own == {"created": None, "deleted": False, "values": values}
    unit.erase_part(s, dat_id)
    assert unit.own_part(s, dat_id) == (True, None)


def test_a_write_names_only_the_parts_fields() -> None:
    unit = native.PostgresStore("postgresql://u:p@127.0.0.1:5432/db", "pg").unit()
    s = spec(Customer)
    with pytest.raises(native.InvalidUnitOfWork, match="are not fields"):
        unit.write_part(s, uuid.uuid4(), {"nickname": "x"}, created=None)
    with pytest.raises(native.InvalidUnitOfWork, match="not a list or a map"):
        unit.write_part(s, uuid.uuid4(), {"name": ["x"]}, created=None)
    assert unit.own_part(s, uuid.uuid4()) == (False, None)


def test_parts_in_other_native_stores_are_not_on_the_native_path_yet() -> None:
    stores = {
        "pg": native.PostgresStore("postgresql://u:p@127.0.0.1:5432/db", "pg"),
        "lake": native.IcebergStore("iceberg+rest://127.0.0.1:1/", "lake"),
    }
    with (
        Rhizodat(stores=stores, journal=native.MemoryJournal()) as dat,
        pytest.raises(NotImplementedError, match="IcebergStore"),
    ):
        dat.repository(InLake)


# --- against PostgreSQL -----------------------------------------------------------------


@pytest.fixture
def dat(pg_dsn: str, pg_schema: str) -> Iterator[Rhizodat]:
    d = Rhizodat(stores={"pg": native.PostgresStore(pg_dsn, "pg", pg_schema)})
    yield d
    d.close()


@pytest.fixture
def python_dat(pg_dsn: str, pg_schema: str) -> Iterator[Rhizodat]:
    """The Python facade over the same schema."""
    d = Rhizodat(stores={"pg": PyPostgresStore(pg_dsn, name="pg", schema=pg_schema)})
    yield d
    d.close()


def tick() -> datetime:
    """A moment strictly between two commits (HLC times are milliseconds)."""
    time.sleep(0.005)
    at = datetime.now(UTC)
    time.sleep(0.005)
    return at


def new_order(faker: Faker, customer: uuid.UUID) -> Order:
    return Order(
        customer=Ref(customer),
        amount=Decimal(faker.pydecimal(left_digits=4, right_digits=2, positive=True)),
        placed_on=faker.date_object(),
        paid_at=None,
        lines=[{"qty": faker.random_int(1, 9)}],
        token=faker.binary(length=8),
    )


def test_a_customer_is_added_read_and_updated_with_its_history(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    customer = Customer(name=faker.name(), email=faker.email())
    added = customers.add(customer)
    first_name = customer.name
    stored = customers.get(customer.dat_id)
    assert (stored.name, stored.email) == (customer.name, customer.email)
    assert stored.dat_created_at == added.value.dat_created_at
    with pytest.raises(EntityExists):
        customers.add(customer)

    between = tick()
    customer.name = faker.name()
    customers.update(customer)
    assert customers.get(customer.dat_id).name == customer.name
    assert customers.get(customer.dat_id, known_at=between).name == first_name
    history = customers.history(customer.dat_id)
    assert [c.name for c in history] == [first_name, customer.name]
    assert history[1].dat_created_at == history[0].dat_created_at
    with pytest.raises(EntityNotFound):
        customers.update(Customer(name="x", email="y"))


def test_every_field_type_round_trips(dat: Rhizodat, faker: Faker) -> None:
    orders = dat.repository(Order)
    order = new_order(faker, uuid.uuid4())
    order.paid_at = datetime(2026, 10, 31, 9, 30, 15, 123456, tzinfo=UTC)
    order.lines = [{"qty": 2}, {"qty": 3}]
    orders.add(order)
    stored = orders.get(order.dat_id)
    assert stored.customer == order.customer
    assert isinstance(stored.customer, Ref)
    assert (stored.amount, stored.placed_on, stored.paid_at) == (
        order.amount,
        order.placed_on,
        order.paid_at,
    )
    assert (stored.lines, stored.token) == (order.lines, order.token)


def test_a_soft_delete_restores_and_purges(dat: Rhizodat, faker: Faker) -> None:
    orders = dat.repository(Order)
    order = new_order(faker, uuid.uuid4())
    orders.add(order)
    orders.delete(order.dat_id)
    assert orders.get(order.dat_id) is None
    assert orders.get(order.dat_id, include_deleted=True).dat_deleted_at is not None
    orders.restore(order.dat_id)
    assert orders.get(order.dat_id) is not None
    orders.delete(order.dat_id)
    assert orders.purge_expired() == 0  # still inside its restore window


def test_a_unit_of_work_writes_entities_and_rows_atomically(
    dat: Rhizodat, faker: Faker, pg_dsn: str, pg_schema: str
) -> None:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            f'CREATE TABLE "{pg_schema}".audit (note text, dat_txn uuid, dat_tx_time bigint)'
        )
    customer = Customer(name=faker.name(), email=faker.email())

    @dat.transactional
    def onboard(uow: UnitOfWork) -> None:
        repo = uow.repository(Customer)
        repo.add(customer)
        assert repo.get(customer.dat_id).name == customer.name  # its own write
        uow.store("pg").insert("audit", ["note"], [["onboarded"]])

    onboard()
    assert dat.repository(Customer).get(customer.dat_id) is not None

    other = Customer(name=faker.name(), email=faker.email())

    @dat.transactional
    def broken(uow: UnitOfWork) -> None:
        uow.repository(Customer).add(other)
        uow.store("pg").insert("audit", ["note"], [["never"]])
        raise RuntimeError("the work fails")

    with pytest.raises(RuntimeError):
        broken()
    assert dat.repository(Customer).get(other.dat_id) is None
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        notes = conn.execute(f'SELECT note FROM "{pg_schema}".audit').fetchall()
    assert notes == [("onboarded",)]


def test_a_unit_of_work_reads_its_own_erase(dat: Rhizodat, faker: Faker) -> None:
    customer = Customer(name=faker.name(), email=faker.email())
    dat.repository(Customer).add(customer)

    @dat.transactional
    def erase(uow: UnitOfWork) -> None:
        repo = uow.repository(Customer)
        repo.delete(customer.dat_id)  # no soft delete: erased
        assert repo.get(customer.dat_id) is None

    erase()
    assert dat.repository(Customer).history(customer.dat_id) == []


def test_valid_time_reads_on_the_native_path(dat: Rhizodat, faker: Faker) -> None:
    agreements = dat.repository(PriceAgreement)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    agreement = PriceAgreement(
        customer=faker.company(),
        price=Decimal("10.00"),
        valid_from=start,
        valid_to=start + timedelta(days=30),
    )
    agreements.add(agreement)
    assert agreements.get(agreement.dat_id, as_of=start + timedelta(days=1)).price == Decimal(
        "10.00"
    )
    assert agreements.get(agreement.dat_id, as_of=start + timedelta(days=31)) is None


def test_the_python_facade_reads_what_the_native_one_wrote(
    dat: Rhizodat, python_dat: Rhizodat, faker: Faker
) -> None:
    order = new_order(faker, uuid.uuid4())
    dat.repository(Order).add(order)
    seen = python_dat.repository(Order).get(order.dat_id)
    assert (seen.customer, seen.amount, seen.placed_on, seen.lines, seen.token) == (
        order.customer,
        order.amount,
        order.placed_on,
        order.lines,
        order.token,
    )


def test_the_native_facade_reads_what_the_python_one_wrote(
    dat: Rhizodat, python_dat: Rhizodat, faker: Faker
) -> None:
    order = new_order(faker, uuid.uuid4())
    order.paid_at = datetime.now(UTC)
    python_dat.repository(Order).add(order)
    between = tick()
    order.amount += 1
    python_dat.repository(Order).update(order)
    orders = dat.repository(Order)
    seen = orders.get(order.dat_id)
    assert (seen.amount, seen.paid_at, seen.lines, seen.token) == (
        order.amount,
        order.paid_at,
        order.lines,
        order.token,
    )
    assert orders.get(order.dat_id, known_at=between).amount == order.amount - 1
    assert len(orders.history(order.dat_id)) == 2


def test_a_native_part_store_scans_every_version(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    a, b = (Customer(name=faker.name(), email=faker.email()) for _ in range(2))
    customers.add(a)
    between = tick()
    customers.add(b)
    from rhizodat.core.parts import hlc_bound

    parts = dat._part_store("pg")
    part = Part.of(Customer, "pg")
    assert [i for i, _ in parts.part_versions(part)] == [a.dat_id, b.dat_id]
    assert [i for i, _ in parts.part_versions(part, known_at=hlc_bound(between))] == [a.dat_id]
