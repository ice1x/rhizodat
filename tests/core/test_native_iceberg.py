"""The native coordinator over the real Rust Iceberg adapter (plan step 5).

The one-phase branch and participant of ``crates/rhizodat-iceberg`` behind
``rhizodat_native.Coordinator``: a Unit of Work's Arrow data crosses the boundary
through ``IcebergStore.unit().append(table, data)``, stage writes Parquet files and
the participant appends them in one tagged snapshot after the decision, with no
Python callbacks on the path. Iceberg has no journal of its own, so an Iceberg
unit and a PostgreSQL store commit together through the PostgreSQL journal.

Each test works in its own namespace of the compose REST catalog (profile
``iceberg``), set up and read back through the Python adapter. Install the module
with ``uv sync --group native`` (needs a Rust toolchain).
"""

import uuid
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pyarrow as pa
import pytest
from faker import Faker
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, UUIDType

from rhizodat.adapters.iceberg import IcebergStore

native = pytest.importorskip("rhizodat_native")

SALES = Schema(
    NestedField(1, "sku", StringType(), required=True),
    NestedField(2, "qty", LongType()),
    NestedField(3, "dat_txn", UUIDType()),
    NestedField(4, "dat_tx_time", LongType()),
)


@pytest.fixture
def lake(iceberg_rest_dsn: str) -> Iterator[IcebergStore]:
    """The Python adapter over the compose REST catalog, to set up and read back."""
    store = IcebergStore(iceberg_rest_dsn, name="lake")
    yield store
    store.close()


@pytest.fixture
def iceberg(iceberg_rest_dsn: str) -> Any:
    """The native store over the same catalog."""
    return native.IcebergStore(iceberg_rest_dsn, "lake")


@pytest.fixture
def sales(lake: IcebergStore) -> Iterator[str]:
    """A fresh ``<namespace>.sales`` table, dropped with its namespace afterwards."""
    namespace = f"dat_native_{uuid.uuid4().hex[:12]}"
    lake.catalog.create_namespace(namespace)
    table = f"{namespace}.sales"
    lake.catalog.create_table(table, SALES)
    yield table
    for identifier in lake.catalog.list_tables(namespace):
        lake.catalog.drop_table(identifier)
    lake.catalog.drop_namespace(namespace)


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh PostgreSQL schemas, dropped afterwards."""
    created: list[str] = []

    def make() -> str:
        name = f"dat_native_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{name}"')
        created.append(name)
        return name

    yield make

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute("SET lock_timeout = '10s'")
        for (gid,) in conn.execute(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchall():
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
        for name in created:
            conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def sale_rows(faker: Faker, n: int) -> list[dict[str, Any]]:
    return [{"sku": faker.bothify("SKU-####"), "qty": faker.pyint(1, 9)} for _ in range(n)]


def read(lake: IcebergStore, table: str) -> list[dict[str, Any]]:
    return [dict(row) for row in lake.execute(table).all()]


def snapshots(lake: IcebergStore, table: str) -> int:
    return len(lake.catalog.load_table(table).snapshots())


# --- Docker-free: the store and the unit check their input ------------------------------


def test_a_store_is_built_without_io_and_refuses_other_dsns() -> None:
    store = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake")
    assert store.name == "lake"
    for dsn in ["postgresql://u:p@127.0.0.1:5432/db", "iceberg+sql:///tmp/c.db"]:
        with pytest.raises(native.InvalidUnitOfWork):
            native.IcebergStore(dsn, "lake")


def test_a_unit_takes_only_arrow_data() -> None:
    unit = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake").unit()
    with pytest.raises(native.InvalidUnitOfWork, match="__arrow_c_stream__"):
        unit.append("erp.sales", [{"sku": "a"}])
    with pytest.raises(native.InvalidUnitOfWork, match="Arrow IPC stream"):
        unit.append("erp.sales", b"not arrow")


@pytest.mark.parametrize("reserved", ["dat_txn", "dat_tx_time"])
def test_a_unit_refuses_reserved_columns(reserved: str) -> None:
    unit = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake").unit()
    with pytest.raises(native.InvalidUnitOfWork, match=reserved):
        unit.append("erp.sales", pa.table({"sku": ["a"], reserved: [1]}))


def test_a_unit_needs_a_namespaced_table() -> None:
    unit = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake").unit()
    with pytest.raises(native.InvalidUnitOfWork, match=r"namespace\.table"):
        unit.append("sales", pa.table({"sku": ["a"]}))


def test_a_catalog_that_is_down_aborts_retryably() -> None:
    unit = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake").unit()
    unit.append("erp.sales", pa.table({"sku": ["a"]}))
    with pytest.raises(native.AbortedError) as caught:
        native.Coordinator().commit(native.new_id(), [unit])
    assert caught.value.retryable


# --- one Iceberg store ------------------------------------------------------------------


def test_a_unit_writes_its_rows(lake: IcebergStore, iceberg: Any, sales: str, faker: Faker) -> None:
    rows = sale_rows(faker, 3)
    unit = iceberg.unit()
    unit.append(sales, pa.Table.from_pylist(rows))
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [unit])
    assert outcome["status"] == "committed"
    assert outcome["dat_txn"] == txn
    written = read(lake, sales)
    assert sorted((r["sku"], r["qty"]) for r in written) == sorted(
        (r["sku"], r["qty"]) for r in rows
    )
    # Every row is stamped with the meta-transaction, in one snapshot.
    assert {r["dat_txn"] for r in written} == {txn}
    assert {r["dat_tx_time"] for r in written} == {outcome["dat_tx_time"]}
    assert snapshots(lake, sales) == 1


def test_a_unit_takes_record_batch_readers_and_ipc_bytes(
    lake: IcebergStore, iceberg: Any, sales: str, faker: Faker
) -> None:
    first, second = (
        pa.Table.from_pylist(sale_rows(faker, 2)),
        pa.Table.from_pylist(sale_rows(faker, 2)),
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, second.schema) as writer:
        writer.write_table(second)
    unit = iceberg.unit()
    unit.append(sales, pa.RecordBatchReader.from_batches(first.schema, first.to_batches()))
    unit.append(sales, sink.getvalue().to_pybytes())
    native.Coordinator().commit(native.new_id(), [unit])
    assert len(read(lake, sales)) == 4


def test_columns_are_cast_and_missing_ones_are_null(
    lake: IcebergStore, iceberg: Any, sales: str
) -> None:
    # qty arrives as int32 and is cast to the table's long; a batch without qty
    # leaves it null.
    unit = iceberg.unit()
    unit.append(sales, pa.table({"sku": ["a"], "qty": pa.array([7], pa.int32())}))
    unit.append(sales, pa.table({"sku": ["b"]}))
    native.Coordinator().commit(native.new_id(), [unit])
    assert sorted((r["sku"], r["qty"]) for r in read(lake, sales)) == [("a", 7), ("b", None)]


def test_a_unit_is_reusable(lake: IcebergStore, iceberg: Any, sales: str, faker: Faker) -> None:
    unit = iceberg.unit()
    unit.append(sales, pa.Table.from_pylist(sale_rows(faker, 2)))
    first, second = native.new_id(), native.new_id()
    native.Coordinator().commit(first, [unit])
    native.Coordinator().commit(second, [unit])
    # Each commit writes the buffered rows as its own snapshot.
    written = read(lake, sales)
    assert len(written) == 4
    assert {r["dat_txn"] for r in written} == {first, second}
    assert snapshots(lake, sales) == 2


def test_a_store_without_rows_commits_nothing(lake: IcebergStore, iceberg: Any, sales: str) -> None:
    outcome = native.Coordinator().commit(native.new_id(), [iceberg])
    assert outcome["status"] == "committed"
    assert snapshots(lake, sales) == 0


def test_a_missing_table_aborts_and_writes_nothing(
    lake: IcebergStore, iceberg: Any, sales: str
) -> None:
    namespace = sales.split(".")[0]
    unit = iceberg.unit()
    unit.append(sales, pa.table({"sku": ["a"]}))
    unit.append(f"{namespace}.missing", pa.table({"sku": ["b"]}))
    with pytest.raises(native.AbortedError) as caught:
        native.Coordinator().commit(native.new_id(), [unit])
    assert not caught.value.retryable
    assert snapshots(lake, sales) == 0


def test_rows_that_do_not_fit_abort(lake: IcebergStore, iceberg: Any, sales: str) -> None:
    unit = iceberg.unit()
    unit.append(sales, pa.table({"sku": ["a"], "colour": ["red"]}))
    with pytest.raises(native.AbortedError, match="colour"):
        native.Coordinator().commit(native.new_id(), [unit])
    assert snapshots(lake, sales) == 0


# --- an Iceberg unit and a PostgreSQL store, one meta-transaction ------------------------


def test_iceberg_and_postgres_commit_through_a_journal(
    lake: IcebergStore,
    iceberg: Any,
    sales: str,
    pg_dsn: str,
    make_schema: Callable[[], str],
    faker: Faker,
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            f'CREATE TABLE "{pg_schema}".orders (id text, dat_txn uuid, dat_tx_time bigint)'
        )
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    orders = pg.unit()
    order = faker.uuid4()
    orders.insert("orders", ["id"], [[order]])
    unit = iceberg.unit()
    unit.append(sales, pa.Table.from_pylist(sale_rows(faker, 2)))
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [orders, unit])
    assert outcome["status"] == "committed"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        ids = conn.execute(
            f'SELECT id FROM "{pg_schema}".orders WHERE dat_txn = %s', (txn,)
        ).fetchall()
    assert ids == [(order,)]
    assert {r["dat_txn"] for r in read(lake, sales)} == {txn}
    assert journal.get(txn)["state"] == "DONE"


def test_an_iceberg_failure_rolls_back_postgres(
    lake: IcebergStore, iceberg: Any, sales: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            f'CREATE TABLE "{pg_schema}".orders (id text, dat_txn uuid, dat_tx_time bigint)'
        )
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    orders = pg.unit()
    orders.insert("orders", ["id"], [["o-1"]])
    unit = iceberg.unit()
    unit.append(sales, pa.table({"sku": ["a"], "colour": ["red"]}))
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    with pytest.raises(native.AbortedError):
        native.Coordinator(journal).commit(native.new_id(), [orders, unit])
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        assert conn.execute(f'SELECT count(*) FROM "{pg_schema}".orders').fetchone() == (0,)
    assert snapshots(lake, sales) == 0
