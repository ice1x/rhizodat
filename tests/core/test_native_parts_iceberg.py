"""Entity parts and repositories on the native core, over Iceberg (and PostgreSQL).

``rhizodat_native.IcebergPartSpec`` names an Iceberg store's share of an entity
type, in the store's parts namespace; a native ``IcebergStore`` reads committed
versions and an ``IcebergUnit`` keeps one version per entity (the last write
wins) that the commit stages as a data file and appends after the decision; a
hard delete is erased copy-on-write. A native facade's repositories then hold a
type in Iceberg alone, or split it across PostgreSQL and Iceberg under a
PostgreSQL journal.

The tables are the Python adapter's, so either facade reads the other's
versions. Each test gets its own parts namespace on the compose REST catalog.
Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,union-attr"

import contextlib
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.iceberg import IcebergStore as PyIcebergStore
from rhizodat.adapters.postgres import PostgresJournal as PyPostgresJournal
from rhizodat.adapters.postgres import PostgresStore as PyPostgresStore
from rhizodat.core.parts import Part

native = pytest.importorskip("rhizodat_native")


@entity(
    stores={"lake": ["kind", "amount", "at", "day", "user", "payload", "raw", "ok", "score"]},
    name="audit.Event",
)
class Event:
    kind: str
    amount: Decimal
    at: datetime
    day: date
    user: Ref["Account"] | None
    payload: dict[str, int]
    raw: bytes
    ok: bool
    score: float


@entity(stores={"pg": ["owner"], "lake": ["balance"]}, name="erp.Account", soft_delete=True)
class Account:
    owner: str
    balance: int


def tick() -> datetime:
    """A moment strictly between two commits (HLC times are milliseconds)."""
    time.sleep(0.005)
    at = datetime.now(UTC)
    time.sleep(0.005)
    return at


@pytest.fixture
def parts_namespace(iceberg_rest_dsn: str) -> Iterator[str]:
    """A parts namespace of its own, dropped afterwards."""
    name = f"dat_np_{uuid.uuid4().hex[:10]}"
    yield name
    store = PyIcebergStore(iceberg_rest_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):  # best effort: a test namespace
            for ident in store.catalog.list_tables(name):
                store.catalog.purge_table(ident)
            store.catalog.drop_namespace(name)
    finally:
        store.close()


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def lake(iceberg_rest_dsn: str, parts_namespace: str) -> Iterator[Rhizodat]:
    store = native.IcebergStore(iceberg_rest_dsn, "lake", parts_namespace=parts_namespace)
    d = Rhizodat(stores={"lake": store})
    yield d
    d.close()


@pytest.fixture
def ledger(
    pg_dsn: str, pg_schema: str, journal_schema: str, iceberg_rest_dsn: str, parts_namespace: str
) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores={
            "pg": native.PostgresStore(pg_dsn, "pg", pg_schema),
            "lake": native.IcebergStore(iceberg_rest_dsn, "lake", parts_namespace=parts_namespace),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )
    yield d
    d.close()


@pytest.fixture
def python_ledger(
    pg_dsn: str, pg_schema: str, journal_schema: str, iceberg_rest_dsn: str, parts_namespace: str
) -> Iterator[Rhizodat]:
    """The Python facade over the same schema and namespace."""
    d = Rhizodat(
        stores={
            "pg": PyPostgresStore(pg_dsn, name="pg", schema=pg_schema),
            "lake": PyIcebergStore(iceberg_rest_dsn, name="lake", parts_namespace=parts_namespace),
        },
        journal=PyPostgresJournal(pg_dsn, schema=journal_schema),
    )
    yield d
    d.close()


def event(faker: Faker) -> Event:
    return Event(
        kind=faker.word(),
        amount=Decimal("12.3400"),
        at=datetime(2026, 10, 31, 9, 30, 15, 123456, tzinfo=UTC),
        day=faker.date_object(),
        user=Ref(uuid.uuid4()),
        payload={"n": faker.random_int(1, 9)},
        raw=faker.binary(length=6),
        ok=True,
        score=faker.pyfloat(min_value=0, max_value=1),
    )


# --- Docker-free ------------------------------------------------------------------------


def test_an_iceberg_spec_takes_a_namespaced_table_and_known_kinds() -> None:
    with pytest.raises(native.InvalidUnitOfWork, match="namespace"):
        native.IcebergPartSpec("t.T", "no_namespace", [])
    with pytest.raises(native.InvalidUnitOfWork, match="field kind"):
        native.IcebergPartSpec("t.T", "dat.t", [("a", "uuid")])
    with pytest.raises(native.InvalidUnitOfWork, match="service column"):
        native.IcebergPartSpec("t.T", "dat.t", [("dat_txn", "id")])
    from rhizodat.core.native_parts import part_spec

    store = native.IcebergStore("iceberg+rest://127.0.0.1:1/", "lake", parts_namespace="parts")
    assert store.parts_namespace == "parts"
    assert native.IcebergStore("iceberg+rest://127.0.0.1:1/", "lake").parts_namespace == "dat"
    spec = part_spec(native, Part.of(Event, "lake"), store)
    assert (spec.table, spec.fields[:2]) == ("parts.audit_event", ["kind", "amount"])


def test_an_iceberg_units_last_write_wins_and_an_erase_hides_it() -> None:
    from rhizodat.core.native_parts import part_spec

    store = native.IcebergStore("iceberg+rest://127.0.0.1:1/", "lake")
    unit = store.unit()
    spec = part_spec(native, Part.of(Account, "lake"), store)
    dat_id = uuid.uuid4()
    assert unit.own_part(spec, dat_id) == (False, None)
    unit.write_part(spec, dat_id, {"balance": 1}, created=None)
    unit.write_part(spec, dat_id, {"balance": 2}, created=None)
    assert unit.own_part(spec, dat_id) == (
        True,
        {"created": None, "deleted": False, "values": {"balance": 2}},
    )
    unit.erase_part(spec, dat_id)
    assert unit.own_part(spec, dat_id) == (True, None)
    with pytest.raises(native.InvalidUnitOfWork, match="are not fields"):
        unit.write_part(spec, dat_id, {"owner": "pg's"}, created=None)
    with pytest.raises(native.InvalidUnitOfWork, match="cannot hold"):
        unit.write_part(spec, dat_id, {"balance": "many"}, created=None)


# --- against Iceberg (and PostgreSQL) ----------------------------------------------------


def test_an_event_lives_in_iceberg_with_every_field_type(lake: Rhizodat, faker: Faker) -> None:
    events = lake.repository(Event)
    e = event(faker)
    events.add(e)
    stored = events.get(e.dat_id)
    assert (stored.kind, stored.amount, stored.at, stored.day) == (e.kind, e.amount, e.at, e.day)
    assert (stored.user, stored.payload, stored.raw, stored.ok, stored.score) == (
        e.user,
        e.payload,
        e.raw,
        True,
        e.score,
    )
    between = tick()
    e.kind = faker.word()
    events.update(e)
    assert [x.kind for x in events.history(e.dat_id)] == [stored.kind, e.kind]
    assert events.get(e.dat_id, known_at=between).kind == stored.kind
    events.delete(e.dat_id)  # no soft delete: erased copy-on-write after the decision
    assert events.history(e.dat_id) == []


def test_an_account_splits_across_postgresql_and_iceberg(ledger: Rhizodat, faker: Faker) -> None:
    account = Account(owner=faker.name(), balance=100)

    @ledger.transactional
    def open_account(uow: UnitOfWork) -> None:
        repo = uow.repository(Account)
        repo.add(account)
        assert repo.get(account.dat_id).balance == 100  # its own write

    open_account()
    accounts = ledger.repository(Account)
    assert accounts.get(account.dat_id).owner == account.owner
    account.balance = 70
    accounts.update(account)
    accounts.delete(account.dat_id)
    assert accounts.get(account.dat_id) is None
    assert accounts.get(account.dat_id, include_deleted=True).balance == 70
    accounts.restore(account.dat_id)
    assert accounts.get(account.dat_id).balance == 70

    lost = Account(owner=faker.name(), balance=1)

    @ledger.transactional
    def broken(uow: UnitOfWork) -> None:
        uow.repository(Account).add(lost)
        raise RuntimeError("the work fails")

    with pytest.raises(RuntimeError):
        broken()
    assert accounts.history(lost.dat_id) == []


def test_the_facades_read_each_others_versions(
    ledger: Rhizodat, python_ledger: Rhizodat, faker: Faker
) -> None:
    by_python = Account(owner=faker.name(), balance=5)
    python_ledger.repository(Account).add(by_python)
    seen = ledger.repository(Account).get(by_python.dat_id)
    assert (seen.owner, seen.balance) == (by_python.owner, 5)

    natively = Account(owner=faker.name(), balance=9)
    ledger.repository(Account).add(natively)
    natively.balance = 10
    ledger.repository(Account).update(natively)
    seen = python_ledger.repository(Account).get(natively.dat_id)
    assert (seen.owner, seen.balance) == (natively.owner, 10)
    assert len(python_ledger.repository(Account).history(natively.dat_id)) == 2
