"""Loading files into a store (``Loads``): the table from inferred columns, rows as one unit.

The same import runs against ClickHouse, PostgreSQL and Iceberg: create the
table through the store's own migration steps, append, replace, add a column,
and fail halfway, which must leave the table exactly as it was.
"""

import datetime as dt
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from iceberg_parts import drop_parts
from rhizodat.core.adapter import Loads, Migrates
from rhizodat.ingest import Column

COLUMNS = [
    Column("id", "int64", nullable=False),
    Column("Title With Space", "string", nullable=False),
    Column("kind", "string", nullable=True, low_cardinality=True),
    Column("price", "float64", nullable=True),
    Column("day", "date", nullable=True),
    Column("at", "timestamp", nullable=True),
    Column("at_utc", "timestamptz", nullable=True),
    Column("paid", "boolean", nullable=True),
]


@dataclass
class Target:
    store: Any
    table: str
    count: Callable[[], int]
    columns: Callable[[], list[str]]


def batches(faker: Faker, n: int, *, fail_at: int | None = None, size: int = 25) -> Iterator[Any]:
    import pyarrow as pa

    from rhizodat.ingest.schema import arrow_schema

    schema = arrow_schema(COLUMNS)
    rows = []
    for i in range(n):
        if fail_at is not None and i == fail_at:
            raise RuntimeError("the source broke off")
        rows.append(
            {
                "id": i,
                "Title With Space": faker.sentence(),
                "kind": faker.random_element(["a", "b", None]),
                "price": faker.pyfloat(right_digits=2, positive=True),
                "day": faker.date_object(),
                "at": faker.date_time().replace(microsecond=0),
                "at_utc": faker.date_time(tzinfo=dt.UTC).replace(microsecond=0),
                "paid": faker.pybool(),
            }
        )
        if len(rows) == size:
            yield pa.RecordBatch.from_pylist(rows, schema=schema)
            rows = []
    if rows:
        yield pa.RecordBatch.from_pylist(rows, schema=schema)


@pytest.fixture(params=["clickhouse", "postgres", "iceberg-sql", "iceberg-rest"])
def target(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Target]:
    kind = request.param
    if kind == "clickhouse":
        from rhizodat.adapters.clickhouse import ClickHouseStore

        ch = ClickHouseStore(request.getfixturevalue("clickhouse_dsn"), name="events")

        def ch_count() -> int:
            row = ch.execute("SELECT count() AS n FROM sales").first()
            return int(row["n"]) if row else 0

        def ch_columns() -> list[str]:
            return [r["name"] for r in ch.execute("DESCRIBE TABLE sales").all()]

        yield Target(ch, "sales", ch_count, ch_columns)
        ch.close()
    elif kind == "postgres":
        from rhizodat.adapters.postgres import PostgresStore

        pg = PostgresStore(
            request.getfixturevalue("pg_dsn"), schema=request.getfixturevalue("pg_schema")
        )

        def pg_count() -> int:
            row = pg.execute("SELECT count(*) AS n FROM sales").first()
            return int(row["n"]) if row else 0

        def pg_columns() -> list[str]:
            rows = pg.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema ="
                " current_schema() AND table_name = 'sales' ORDER BY ordinal_position"
            ).all()
            return [r["column_name"] for r in rows]

        yield Target(pg, "sales", pg_count, pg_columns)
        pg.close()
    else:
        from rhizodat.adapters.iceberg import IcebergStore

        dsn = (
            str(request.getfixturevalue("iceberg_rest_dsn"))
            if kind == "iceberg-rest"
            else f"iceberg+sql://{tmp_path / 'c.db'}?warehouse=file://{tmp_path / 'wh'}"
        )
        lake = IcebergStore(
            dsn,
            name="lake",
            parts_namespace=f"imp_{uuid.uuid4().hex[:12]}",
        )
        table = f"{lake.parts_namespace}.sales"

        def ice_count() -> int:
            return int(lake.catalog.load_table(table).scan().to_arrow().num_rows)

        def ice_columns() -> list[str]:
            return [f.name for f in lake.catalog.load_table(table).schema().fields]

        try:
            yield Target(lake, table, ice_count, ice_columns)
        finally:
            try:
                drop_parts(lake)
            finally:
                lake.close()


def create(t: Target, columns: Iterable[Column] = COLUMNS, **kwargs: Any) -> None:
    for step in t.store.create_table_steps(t.table, list(columns), **kwargs):
        t.store.run_migration_step(step.kind, step.body)


def test_the_store_loads(target: Target) -> None:
    assert isinstance(target.store, Loads)
    assert isinstance(target.store, Migrates)
    assert target.store.table_columns(target.table) is None


def test_create_append_and_replace(target: Target, faker: Faker) -> None:
    create(target)
    create(target)  # the steps are idempotent, like every migration step
    assert target.store.table_columns(target.table) == [c.name for c in COLUMNS]
    assert target.columns() == [c.name for c in COLUMNS]

    assert target.store.load(target.table, batches(faker, 60)) == 60
    assert target.store.load(target.table, batches(faker, 10)) == 10
    assert target.count() == 70
    assert target.store.load(target.table, batches(faker, 4), replace=True) == 4
    assert target.count() == 4


def test_a_load_that_fails_halfway_leaves_the_table_as_it_was(target: Target, faker: Faker) -> None:
    create(target)
    target.store.load(target.table, batches(faker, 5))
    with pytest.raises(RuntimeError, match="broke off"):
        target.store.load(target.table, batches(faker, 100, fail_at=60))
    assert target.count() == 5
    with pytest.raises(RuntimeError, match="broke off"):
        target.store.load(target.table, batches(faker, 100, fail_at=60), replace=True)
    assert target.count() == 5


def test_new_columns_are_added_nullable(target: Target, faker: Faker) -> None:
    create(target, COLUMNS[:2])
    target.store.load(
        target.table, (b.select(["id", "Title With Space"]) for b in batches(faker, 3))
    )
    for step in target.store.add_column_steps(target.table, COLUMNS[2:]):
        for _ in range(2):
            target.store.run_migration_step(step.kind, step.body)
    assert target.store.table_columns(target.table) == [c.name for c in COLUMNS]
    target.store.load(target.table, batches(faker, 2))
    assert target.count() == 5


def test_a_load_may_leave_out_nullable_columns(target: Target, faker: Faker) -> None:
    create(target)
    loaded = target.store.load(
        target.table, (b.select(["id", "Title With Space"]) for b in batches(faker, 3))
    )
    assert loaded == 3
    assert target.count() == 3


# --- store-specific ------------------------------------------------------------------------


def test_clickhouse_ddl_uses_low_cardinality_and_the_sort_key(clickhouse_dsn: str) -> None:
    from rhizodat.adapters.clickhouse import ClickHouseStore

    ch = ClickHouseStore(clickhouse_dsn)
    try:
        [step] = ch.create_table_steps("sales", COLUMNS, order_by=["id"])
        assert "`kind` LowCardinality(Nullable(String))" in step.body
        assert "`at_utc` Nullable(DateTime64(6, 'UTC'))" in step.body
        assert step.body.endswith("ENGINE = MergeTree ORDER BY (`id`)")
        [plain] = ch.create_table_steps("sales", COLUMNS)
        assert plain.body.endswith("ORDER BY tuple()")
        with pytest.raises(ValueError, match=r"sort key .*price.* nullable"):
            ch.create_table_steps("sales", COLUMNS, order_by=["price"])
        with pytest.raises(ValueError, match="no column nope"):
            ch.create_table_steps("sales", COLUMNS, order_by=["nope"])
        left = ch.execute("SHOW TABLES").all()
        assert left == []  # nothing was created, and no staging table is left behind
    finally:
        ch.close()


def test_clickhouse_leaves_no_staging_table(clickhouse_dsn: str, faker: Faker) -> None:
    from rhizodat.adapters.clickhouse import ClickHouseStore

    ch = ClickHouseStore(clickhouse_dsn)
    try:
        for step in ch.create_table_steps("sales", COLUMNS):
            ch.run_migration_step(step.kind, step.body)
        ch.load("sales", batches(faker, 10))
        with pytest.raises(RuntimeError):
            ch.load("sales", batches(faker, 10, fail_at=5))
        ch.load("sales", batches(faker, 3), replace=True)
        assert [r["name"] for r in ch.execute("SHOW TABLES").all()] == ["sales"]
    finally:
        ch.close()


def test_postgres_quotes_names_and_keeps_not_null(pg_dsn: str, pg_schema: str) -> None:
    from rhizodat.adapters.postgres import PostgresStore

    pg = PostgresStore(pg_dsn, schema=pg_schema)
    try:
        [step] = pg.create_table_steps("sales", COLUMNS)
        assert '"Title With Space" text NOT NULL' in step.body
        assert '"at_utc" timestamptz' in step.body
    finally:
        pg.close()
