"""Migration steps in each store's own language: ``sql`` for PostgreSQL and ClickHouse,
``cypher`` for drevo, declarative ``iceberg`` operations for Iceberg.

A body is one statement or a list of them. Every step here is written to be run
twice, as the runner does after a failure; the Iceberg operations are idempotent
by themselves.
"""

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from datorium.core.adapter import Migrates
from drevo_parts import drevo_parts_dsn
from iceberg_parts import drop_parts, iceberg_parts_dsn


def migrates(store: object) -> bool:
    return isinstance(store, Migrates)


# --- PostgreSQL ---------------------------------------------------------------------------


@pytest.fixture
def pg(pg_dsn: str, pg_schema: str) -> Iterator[Any]:
    from datorium.adapters.postgres import PostgresStore

    store = PostgresStore(pg_dsn, schema=pg_schema)
    yield store
    store.close()


def pg_columns(store: Any, table: str) -> list[str]:
    rows = store.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = current_schema() AND table_name = %s ORDER BY ordinal_position",
        (table,),
    ).all()
    return [r["column_name"] for r in rows]


def test_postgres_runs_sql_steps(pg: Any) -> None:
    assert migrates(pg)
    assert pg.migration_kinds == frozenset({"sql"})
    create = "CREATE TABLE IF NOT EXISTS task (id uuid PRIMARY KEY, title text NOT NULL)"
    add = "ALTER TABLE task ADD COLUMN IF NOT EXISTS due date"
    for _ in range(2):
        pg.run_migration_step("sql", create)
        pg.run_migration_step("sql", [add])
    assert pg_columns(pg, "task") == ["id", "title", "due"]


def test_a_postgres_step_is_one_transaction(pg: Any) -> None:
    with pytest.raises(Exception, match="nope"):
        pg.run_migration_step(
            "sql", ["CREATE TABLE IF NOT EXISTS task (id int)", "SELECT * FROM nope"]
        )
    assert pg_columns(pg, "task") == []  # the first statement was rolled back too


@pytest.mark.parametrize("body", [42, [], ["x", 1], {"sql": "x"}])
def test_a_sql_body_is_text_or_a_list_of_text(pg: Any, body: object) -> None:
    with pytest.raises(ValueError, match="sql step"):
        pg.run_migration_step("sql", body)


# --- ClickHouse ---------------------------------------------------------------------------


def test_clickhouse_runs_sql_steps_one_statement_at_a_time(clickhouse_dsn: str) -> None:
    from datorium.adapters.clickhouse import ClickHouseStore

    store = ClickHouseStore(clickhouse_dsn)
    try:
        assert migrates(store)
        assert store.migration_kinds == frozenset({"sql"})
        steps = [
            "CREATE TABLE IF NOT EXISTS events (id UUID, at DateTime64(3)) "
            "ENGINE = MergeTree ORDER BY (id, at)",
            "ALTER TABLE events ADD COLUMN IF NOT EXISTS kind LowCardinality(String)",
        ]
        for _ in range(2):
            store.run_migration_step("sql", steps)
        names = [r["name"] for r in store.execute("DESCRIBE TABLE events").all()]
        assert names == ["id", "at", "kind"]
    finally:
        store.close()


# --- drevo --------------------------------------------------------------------------------


@pytest.fixture(params=["memory", "bolt"])
def graph(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    from datorium.adapters.drevo import DrevoStore

    store = DrevoStore(drevo_parts_dsn(request, tmp_path), name="graph")
    yield store
    store.close()


def test_drevo_runs_cypher_steps(graph: Any, faker: Faker) -> None:
    assert migrates(graph)
    assert graph.migration_kinds == frozenset({"cypher"})
    label = f"Tag{uuid.uuid4().hex[:8]}"
    name = faker.word()
    seed = f"MERGE (t:{label} {{title: '{label}:{name}', name: '{name}'}})"
    try:
        for _ in range(2):
            graph.run_migration_step("cypher", [seed])
        count = graph.execute(f"MATCH (t:{label}) RETURN count(t) AS n").first()
        assert count is not None
        assert count["n"] == 1
    finally:
        graph.execute(f"MATCH (t:{label}) DELETE t")


def test_a_cypher_body_is_text_or_a_list_of_text(graph: Any) -> None:
    with pytest.raises(ValueError, match="cypher step"):
        graph.run_migration_step("cypher", [{"q": "RETURN 1"}])


# --- Iceberg ------------------------------------------------------------------------------


@pytest.fixture(params=["sql", "rest"])
def lake(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    from datorium.adapters.iceberg import IcebergStore

    store = IcebergStore(
        iceberg_parts_dsn(request, tmp_path),
        name="lake",
        parts_namespace=f"mig_{uuid.uuid4().hex[:12]}",
    )
    try:
        yield store
    finally:
        try:
            drop_parts(store)
        finally:
            store.close()


def lake_schema(store: Any, table: str) -> dict[str, tuple[str, bool]]:
    schema = store.catalog.load_table(table).schema()
    return {f.name: (str(f.field_type), f.required) for f in schema.fields}


def test_iceberg_runs_schema_operations(lake: Any) -> None:
    assert migrates(lake)
    assert lake.migration_kinds == frozenset({"iceberg"})
    table = f"{lake.parts_namespace}.tasks"
    ops = [
        {"op": "create_namespace", "namespace": lake.parts_namespace},
        {
            "op": "create_table",
            "table": table,
            "columns": [
                {"name": "id", "type": "string", "required": True},
                {"name": "title", "type": "string"},
                {"name": "estimate", "type": "decimal(10, 2)"},
                {"name": "opened", "type": "timestamptz"},
            ],
        },
        {"op": "add_column", "table": table, "name": "points", "type": "int"},
        {"op": "rename_column", "table": table, "from": "title", "to": "summary"},
        {"op": "drop_column", "table": table, "name": "estimate"},
        {"op": "add_partition", "table": table, "column": "opened", "transform": "day"},
    ]
    for _ in range(2):  # every operation is a no-op the second time
        lake.run_migration_step("iceberg", ops)

    assert lake_schema(lake, table) == {
        "id": ("string", True),
        "summary": ("string", False),
        "opened": ("timestamptz", False),
        "points": ("int", False),
    }
    spec = lake.catalog.load_table(table).spec()
    assert [(f.name, str(f.transform)) for f in spec.fields] == [("opened_day", "day")]


@pytest.mark.parametrize(
    ("op", "message"),
    [
        ({"op": "teleport", "table": "t"}, "unknown iceberg operation 'teleport'"),
        ({"op": "drop_column", "name": "x"}, "drop_column needs table"),
        ({"op": "drop_column", "table": "{ns}.t", "name": "x", "force": True}, "force"),
        ({"op": "add_column", "table": "{ns}.t", "name": "x", "type": "money"}, "type 'money'"),
        ({"op": "add_column", "table": "{ns}.t", "name": "x", "type": "long"}, "is int, not long"),
        ({"op": "rename_column", "table": "{ns}.t", "from": "a", "to": "b"}, "neither a nor b"),
        ("drop everything", "an iceberg step is a list of operations"),
    ],
)
def test_bad_iceberg_operations_are_refused(lake: Any, op: object, message: str) -> None:
    table = f"{lake.parts_namespace}.t"
    lake.run_migration_step(
        "iceberg",
        [
            {"op": "create_namespace", "namespace": lake.parts_namespace},
            {"op": "create_table", "table": table, "columns": [{"name": "x", "type": "int"}]},
        ],
    )
    if isinstance(op, dict):
        op = {
            k: v.format(ns=lake.parts_namespace) if isinstance(v, str) else v for k, v in op.items()
        }
        body: object = [op]
    else:
        body = op
    with pytest.raises(ValueError, match=message):
        lake.run_migration_step("iceberg", body)
