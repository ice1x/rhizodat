"""``execute`` on native stores: a query's full result crosses the boundary as Arrow.

``rhizodat_native.IcebergStore.execute(table, params)`` scans a table like the Python
adapter's (``selected_fields``, ``snapshot_id``, ``limit``) and
``rhizodat_native.ClickHouseStore.execute(query, params)`` runs native SQL with
``{name:Type}`` placeholders. Either returns a :class:`~rhizodat.core.result.Result`
built from one Arrow IPC stream (the core's answer), so ``r.store(name).execute(...)``
works on the native facade as on the Python one, and ``to_arrow`` costs nothing more.
Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.adapters.iceberg import IcebergStore as PyIcebergStore
from rhizodat.core.result import Result

pa = pytest.importorskip("pyarrow")
native = pytest.importorskip("rhizodat_native")


# --- Iceberg ----------------------------------------------------------------------------


@pytest.fixture
def sales(iceberg_rest_dsn: str) -> Iterator[str]:
    """A fresh ``<namespace>.sales`` table, dropped with its namespace afterwards."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType, UUIDType

    lake = PyIcebergStore(iceberg_rest_dsn, name="setup")
    namespace = f"dat_exec_{uuid.uuid4().hex[:12]}"
    lake.catalog.create_namespace(namespace)
    table = f"{namespace}.sales"
    schema = Schema(
        NestedField(1, "sku", StringType(), required=True),
        NestedField(2, "qty", LongType()),
        NestedField(3, "dat_txn", UUIDType()),
        NestedField(4, "dat_tx_time", LongType()),
    )
    lake.catalog.create_table(table, schema)
    yield table
    for identifier in lake.catalog.list_tables(namespace):
        lake.catalog.drop_table(identifier)
    lake.catalog.drop_namespace(namespace)
    lake.close()


def sell(r: Rhizodat, table: str, skus: list[str], qty: list[int]) -> uuid.UUID:
    @r.transactional
    def write(uow: Any) -> None:
        uow.store("lake").append(table, pa.table({"sku": skus, "qty": qty}))

    return write().dat_txn


def test_iceberg_execute_scans_the_table_as_arrow(
    iceberg_rest_dsn: str, sales: str, faker: Faker
) -> None:
    skus = sorted({faker.bothify("SKU-####") for _ in range(4)})
    with Rhizodat(stores={"lake": native.IcebergStore(iceberg_rest_dsn, "lake")}) as r:
        dat_txn = sell(r, sales, skus, [1, 2, 3, 4])
        result = r.store("lake").execute(sales)
        assert isinstance(result, Result)
        assert set(result.columns or ()) == {"sku", "qty", "dat_txn", "dat_tx_time"}
        rows = sorted(result.all(), key=lambda row: row["sku"])
        assert [row["sku"] for row in rows] == skus
        assert {row["dat_txn"] for row in rows} == {dat_txn}

        table = r.store("lake").execute(sales).to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 4


def test_iceberg_execute_takes_the_scan_params(
    iceberg_rest_dsn: str, sales: str, faker: Faker
) -> None:
    skus = [faker.bothify("SKU-####") for _ in range(3)]
    with Rhizodat(stores={"lake": native.IcebergStore(iceberg_rest_dsn, "lake")}) as r:
        sell(r, sales, skus, [1, 2, 3])
        lake = r.store("lake")
        narrow = lake.execute(sales, {"selected_fields": ("sku",)})
        assert narrow.columns == ("sku",)
        assert len(narrow.all()) == 3
        assert len(lake.execute(sales, {"limit": 2}).all()) == 2
        filtered = lake.execute(sales, {"row_filter": "qty >= 2", "selected_fields": ("sku",)})
        assert filtered.columns == ("sku",)
        assert sorted(r["sku"] for r in filtered.all()) == sorted(skus[1:])
        by_sku = lake.execute(sales, {"row_filter": f"sku == '{skus[0]}' AND qty < 5"})
        assert [r["qty"] for r in by_sku.all()] == [1]
        several = lake.execute(
            sales, {"row_filter": f"sku IN ('{skus[0]}', '{skus[2]}') OR qty = 2"}
        )
        assert len(several.all()) == 3
        assert lake.execute(sales, {"row_filter": "qty IS NULL"}).all() == []
        assert lake.execute(sales, {"limit": 0}).all() == []

        first = PyIcebergStore(iceberg_rest_dsn, name="reader")
        try:
            snapshot = first.catalog.load_table(sales).current_snapshot()
        finally:
            first.close()
        assert snapshot is not None
        sell(r, sales, skus, [7, 8, 9])
        assert len(lake.execute(sales).all()) == 6
        then = lake.execute(sales, {"snapshot_id": snapshot.snapshot_id})
        assert len(then.all()) == 3


def test_iceberg_execute_of_an_empty_table_has_its_columns(
    iceberg_rest_dsn: str, sales: str
) -> None:
    result = native.IcebergStore(iceberg_rest_dsn, "lake").execute(sales)
    assert result.columns == ("sku", "qty", "dat_txn", "dat_tx_time")
    assert result.all() == []


def test_iceberg_execute_refuses_what_the_core_cannot_do(iceberg_rest_dsn: str) -> None:
    lake = native.IcebergStore(iceberg_rest_dsn, "lake")
    with pytest.raises(native.InvalidUnitOfWork, match=r"row_filter.*a literal"):
        lake.execute("ns.t", {"row_filter": "qty >"})
    with pytest.raises(native.InvalidUnitOfWork, match="filter's text"):
        lake.execute("ns.t", {"row_filter": 1})
    with pytest.raises(native.InvalidUnitOfWork, match="unknown"):
        lake.execute("ns.t", {"case_sensitive": False})
    with pytest.raises(native.InvalidUnitOfWork, match=r"namespace\.table"):
        lake.execute("sales")
    with pytest.raises(RuntimeError, match="sales"):
        lake.execute(f"dat_missing_{uuid.uuid4().hex[:8]}.sales")


# --- ClickHouse -------------------------------------------------------------------------


def _ch_client(clickhouse_dsn: str) -> Any:
    import clickhouse_connect

    from rhizodat.core.dsn import parse_dsn

    server = parse_dsn(clickhouse_dsn)
    return clickhouse_connect.get_client(
        host=server.host,
        port=server.port,
        username=server.user or "default",
        password=server.password or "",
        database=(server.path or "/").lstrip("/") or "default",
        autogenerate_session_id=False,
    )


def test_clickhouse_execute_runs_sql_and_binds_params(clickhouse_dsn: str, faker: Faker) -> None:
    store = native.ClickHouseStore(clickhouse_dsn, "ch")
    ddl = store.execute(
        "CREATE TABLE events (id Int64, name String, score Float64, ok Bool, at DateTime64(3))"
        " ENGINE = MergeTree ORDER BY id"
    )
    assert isinstance(ddl, Result)
    assert ddl.columns == ()
    assert ddl.all() == []

    names = [faker.name() for _ in range(3)]
    client = _ch_client(clickhouse_dsn)
    client.insert(
        "events",
        [[i, n, i / 2, i % 2 == 0, "2026-11-01 10:00:00.123"] for i, n in enumerate(names)],
        column_names=["id", "name", "score", "ok", "at"],
    )

    result = store.execute(
        "SELECT id, name, score, ok FROM events WHERE id >= {low:Int64} AND name != {skip:String}"
        " ORDER BY id",
        {"low": 1, "skip": names[1]},
    )
    assert result.columns == ("id", "name", "score", "ok")
    assert result.all() == [{"id": 2, "name": names[2], "score": 1.0, "ok": True}]

    table = store.execute("SELECT id, at FROM events ORDER BY id").to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column("id").to_pylist() == [0, 1, 2]
    assert pa.types.is_timestamp(table.schema.field("at").type)

    assert store.execute("SELECT id FROM events WHERE id < 0").all() == []
    assert store.execute("INSERT INTO events (id, name) VALUES (9, 'x')").all() == []
    assert store.execute("SELECT count() AS n FROM events").one() == {"n": 4}


def test_clickhouse_execute_reports_the_servers_error(clickhouse_dsn: str) -> None:
    store = native.ClickHouseStore(clickhouse_dsn, "ch")
    with pytest.raises(RuntimeError, match=r"(?i)unknown table|doesn't exist|does not exist"):
        store.execute("SELECT * FROM no_such_table")
    with pytest.raises(native.InvalidUnitOfWork, match="cannot bind"):
        store.execute("SELECT {x:String}", {"x": object()})


def test_clickhouse_execute_through_the_facade(clickhouse_dsn: str) -> None:
    with Rhizodat(stores={"ch": native.ClickHouseStore(clickhouse_dsn, "ch")}) as r:
        r.store("ch").execute(
            "CREATE TABLE t (n Int64, dat_txn UUID, dat_tx_time Int64)"
            " ENGINE = MergeTree ORDER BY n"
        )

        @r.transactional
        def write(uow: Any) -> None:
            uow.store("ch").insert("t", ["n"], [[1], [2]])

        write()
        assert [row["n"] for row in r.store("ch").execute("SELECT n FROM t ORDER BY n")] == [1, 2]


# --- PostgreSQL -------------------------------------------------------------------------


def test_postgres_execute_runs_sql_with_positional_params(
    pg_dsn: str, pg_schema: str, faker: Faker
) -> None:
    store = native.PostgresStore(pg_dsn, "pg", pg_schema)
    ddl = store.execute(
        "CREATE TABLE people (id bigint, small int, name text, score double precision,"
        " ok boolean, tag uuid, raw bytea, doc jsonb, at timestamptz, day date, price numeric)"
    )
    assert isinstance(ddl, Result)
    assert (ddl.columns, ddl.all()) == ((), [])

    name, tag = faker.name(), uuid.uuid4()
    inserted = store.execute(
        "INSERT INTO people VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb,"
        " '2026-10-31T09:30:15.123456+00:00', '2026-10-31', 12.5)",
        [1, 7, name, 0.5, True, tag, b"\x00\xff", '{"n": 1}'],
    )
    assert inserted.all() == []
    store.execute("INSERT INTO people (id, name) VALUES ($1, $2)", (2, None))

    result = store.execute(
        "SELECT id, small, name, score, ok, tag, raw, doc, at, day FROM people"
        " WHERE id = $1 OR name = $2 ORDER BY id",
        [2, name],
    )
    assert result.columns == (
        "id",
        "small",
        "name",
        "score",
        "ok",
        "tag",
        "raw",
        "doc",
        "at",
        "day",
    )
    first, second = result.all()
    assert first == {
        "id": 1,
        "small": 7,
        "name": name,
        "score": 0.5,
        "ok": True,
        "tag": tag,
        "raw": b"\x00\xff",
        "doc": {"n": 1},
        "at": datetime(2026, 10, 31, 9, 30, 15, 123456, tzinfo=UTC),
        "day": date(2026, 10, 31),
    }
    assert second["id"] == 2
    assert all(second[c] is None for c in ("small", "name", "score", "ok", "tag", "raw", "doc"))

    assert store.execute("SELECT count(*) AS n FROM people").one() == {"n": 2}
    assert store.execute("SELECT current_schema() AS s").one() == {"s": pg_schema}
    assert store.execute("SELECT price::text AS p FROM people WHERE id = 1").one() == {"p": "12.5"}
    table = store.execute("SELECT id, name FROM people ORDER BY id").to_arrow()
    assert table.column("id").to_pylist() == [1, 2]


def test_postgres_execute_refuses_what_it_cannot_bind_or_decode(
    pg_dsn: str, pg_schema: str
) -> None:
    store = native.PostgresStore(pg_dsn, "pg", pg_schema)
    with pytest.raises(RuntimeError, match=r"(?i)numeric.*::text"):
        store.execute("SELECT 1.5::numeric AS price")
    with pytest.raises(native.InvalidUnitOfWork, match="cannot bind"):
        store.execute("SELECT $1::text", [object()])
    with pytest.raises(native.InvalidUnitOfWork, match=r"\$1"):
        store.execute("SELECT $1::text", {"name": "x"})
    with pytest.raises(RuntimeError, match=r"(?i)no_such_table|does not exist"):
        store.execute("SELECT * FROM no_such_table")


def test_postgres_execute_through_the_facade(pg_dsn: str, pg_schema: str) -> None:
    with Rhizodat(stores={"pg": native.PostgresStore(pg_dsn, "pg", pg_schema)}) as r:
        r.store("pg").execute("CREATE TABLE t (n bigint, dat_txn uuid, dat_tx_time bigint)")

        @r.transactional
        def write(uow: Any) -> None:
            uow.store("pg").insert("t", ["n"], [[1], [2]])

        write()
        assert [row["n"] for row in r.store("pg").execute("SELECT n FROM t ORDER BY n")] == [1, 2]


# --- drevo ------------------------------------------------------------------------------


def test_drevo_execute_runs_cypher_with_params(drevo_bolt_dsn: str, faker: Faker) -> None:
    label = f"Exec{uuid.uuid4().hex[:10]}"
    store = native.DrevoStore(drevo_bolt_dsn, "graph")
    try:
        names = sorted({faker.first_name() for _ in range(3)})
        created = store.execute(
            f"UNWIND $names AS name CREATE (:{label} {{name: name, n: 1, ok: true, score: 0.5}})",
            {"names": names},
        )
        assert isinstance(created, Result)
        assert created.all() == []

        result = store.execute(
            f"MATCH (p:{label}) WHERE p.name <> $skip"
            " RETURN p.name AS name, p.n AS n, p.ok AS ok, p.score AS score, [p.n, p.name] AS pair"
            " ORDER BY name",
            {"skip": names[1]},
        )
        assert result.columns == ("n", "name", "ok", "pair", "score")
        assert result.all() == [
            {"name": names[0], "n": 1, "ok": True, "score": 0.5, "pair": [1, names[0]]},
            {"name": names[2], "n": 1, "ok": True, "score": 0.5, "pair": [1, names[2]]},
        ]
        assert store.execute(f"MATCH (p:{label}) RETURN count(p) AS c").one() == {"c": 3}
        assert store.execute(f"MATCH (p:{label}) WHERE p.n > 5 RETURN p.name").all() == []
        with pytest.raises(native.InvalidUnitOfWork, match="Cypher parameter"):
            store.execute("RETURN $x", {"x": object()})
        with pytest.raises(RuntimeError):
            store.execute("THIS IS NOT CYPHER")
    finally:
        store.execute(f"MATCH (p:{label}) DETACH DELETE p")


def test_drevo_execute_through_the_facade(drevo_bolt_dsn: str) -> None:
    label = f"Exec{uuid.uuid4().hex[:10]}"
    with Rhizodat(stores={"graph": native.DrevoStore(drevo_bolt_dsn, "graph")}) as r:
        try:

            @r.transactional
            def write(uow: Any) -> None:
                uow.store("graph").run(f"CREATE (:{label} {{n: 1}}), (:{label} {{n: 2}})")

            write()
            rows = r.store("graph").execute(f"MATCH (p:{label}) RETURN p.n AS n ORDER BY n")
            assert [row["n"] for row in rows] == [1, 2]
        finally:
            r.store("graph").execute(f"MATCH (p:{label}) DETACH DELETE p")
