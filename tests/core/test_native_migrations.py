"""Migrations on the native facade: the history in a native journal, the steps on
native stores.

``rhizodat_native.PostgresJournal`` and ``MemoryJournal`` keep the migration history
(``applied_migrations`` / ``record_migration``, the Python adapter's table for the
PostgreSQL one, so either facade reads the other's history); the native
``PostgresStore`` and ``ClickHouseStore`` run ``sql`` steps and ``DrevoStore`` runs
``cypher`` steps, so ``r.migrate(...)`` and ``r.schema_versions()`` work on the native
facade as on the Python one; the native ``IcebergStore`` runs ``iceberg`` steps through the Python
adapter's declarative operations (iceberg-rust 0.10 has no rename or partition
update, so the step itself stays Python).
Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.adapters.postgres import PostgresJournal as PyPostgresJournal
from rhizodat.adapters.postgres import PostgresStore as PyPostgresStore
from rhizodat.core.adapter import Migrates
from rhizodat.core.journal import AppliedMigration, JournalConflict, MigrationHistory
from rhizodat.migrations import Migration, MigrationError, MigrationFailed, Step

native = pytest.importorskip("rhizodat_native")


def release(n: int, store: str, kind: str, body: Any) -> Migration:
    return Migration.of(n, f"release_{n}", [Step(store, kind, body)])


# --- Docker-free ------------------------------------------------------------------------


def test_native_journals_and_stores_have_the_capabilities() -> None:
    assert isinstance(native.MemoryJournal(), MigrationHistory)
    for store in (
        native.PostgresStore("postgresql://u:p@127.0.0.1:1/db", "pg", "s"),
        native.ClickHouseStore("clickhouse://127.0.0.1:1/db", "ch"),
    ):
        assert isinstance(store, Migrates)
        assert store.migration_kinds == frozenset({"sql"})
    graph = native.DrevoStore("drevo+bolt://127.0.0.1:1", "graph")
    assert isinstance(graph, Migrates)
    assert graph.migration_kinds == frozenset({"cypher"})
    lake = native.IcebergStore("iceberg+rest://127.0.0.1:1/wh", "lake")
    assert isinstance(lake, Migrates)
    assert lake.migration_kinds == frozenset({"iceberg"})
    with pytest.raises(ValueError, match="iceberg steps, not sql"):
        lake.run_migration_step("sql", "SELECT 1")
    with pytest.raises(ValueError, match="list of operations"):
        lake.run_migration_step("iceberg", {"op": "create_namespace"})


def test_a_memory_journal_keeps_the_history(faker: Faker) -> None:
    journal = native.MemoryJournal()
    assert journal.applied_migrations() == []
    first = AppliedMigration(2, "b", faker.sha256(), 20)
    second = AppliedMigration(1, "a", faker.sha256(), 10)
    journal.record_migration(first)
    journal.record_migration(second)
    assert journal.applied_migrations() == [second, first]
    assert all(isinstance(a, AppliedMigration) for a in journal.applied_migrations())
    with pytest.raises(JournalConflict, match="migration 1"):
        journal.record_migration(AppliedMigration(1, "again", faker.sha256(), 30))


def test_a_step_body_is_a_statement_or_a_list_of_them() -> None:
    store = native.PostgresStore("postgresql://u:p@127.0.0.1:1/db", "pg", "s")
    for body in (1, [], ["a", 2], {"x": "y"}):
        with pytest.raises(ValueError, match="statement or a list of statements"):
            store.run_migration_step("sql", body)
    with pytest.raises(ValueError, match="sql steps, not ddl"):
        store.run_migration_step("ddl", "CREATE TABLE t ()")


def test_the_facade_runs_migrations_with_a_memory_journal(faker: Faker) -> None:
    with Rhizodat(
        stores={"pg": native.PostgresStore("postgresql://u:p@127.0.0.1:1/db", "pg", "s")},
        journal=native.MemoryJournal(),
    ) as r:
        assert r.schema_versions() == []
        report = r.migrate([release(1, "pg", "sql", faker.word())], dry_run=True)
        assert (report.applied, report.pending, report.already) == ((), (1,), ())
        with pytest.raises(MigrationError, match="runs sql steps, not cypher"):
            r.migrate([release(1, "pg", "cypher", "RETURN 1")], dry_run=True)


# --- against PostgreSQL, ClickHouse and drevo ----------------------------------------------


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def test_postgres_migrations_run_and_are_recorded_for_both_facades(
    pg_dsn: str, pg_schema: str, journal_schema: str
) -> None:
    before = datetime.now(UTC)
    with Rhizodat(
        stores={"pg": native.PostgresStore(pg_dsn, "pg", pg_schema)},
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    ) as r:
        first = Migration.of(
            1,
            "orders",
            [
                Step(
                    "pg",
                    "sql",
                    [
                        "CREATE TABLE IF NOT EXISTS orders (id bigint PRIMARY KEY)",
                        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS sku text",
                    ],
                )
            ],
        )
        second = release(2, "pg", "sql", "CREATE INDEX IF NOT EXISTS orders_sku ON orders (sku)")
        report = r.migrate([second, first])
        assert (report.applied, report.already) == ((1, 2), ())
        assert [
            row["column_name"]
            for row in r.store("pg").execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = $1 AND table_name = 'orders' ORDER BY ordinal_position",
                [pg_schema],
            )
        ] == ["id", "sku"]

        history = r.schema_versions()
        assert [(h.id, h.name, h.checksum) for h in history] == [
            (1, "orders", first.checksum),
            (2, "release_2", second.checksum),
        ]
        assert all(isinstance(h, AppliedMigration) for h in history)
        assert history[0].applied_at >= before.replace(microsecond=0)
        assert r.schema_versions(as_of=before) == []
        assert r.migrate([first, second]).applied == ()

        # A step that fails is not recorded; earlier migrations stay.
        broken = Migration.of(
            3, "broken", [Step("pg", "sql", ["CREATE TABLE IF NOT EXISTS ok ()", "SELECT nope"])]
        )
        with pytest.raises(MigrationFailed, match="step 1 on store 'pg'") as caught:
            r.migrate([first, second, broken])
        assert "nope" in str(caught.value)
        assert [h.id for h in r.schema_versions()] == [1, 2]
        # The failed step ran in one transaction: its first statement is rolled back.
        assert r.store("pg").execute(
            "SELECT to_regclass($1)::text AS t", [f"{pg_schema}.ok"]
        ).one() == {"t": None}

    # The Python facade reads the same history and continues it.
    with Rhizodat(
        stores={"pg": PyPostgresStore(pg_dsn, name="pg", schema=pg_schema)},
        journal=PyPostgresJournal(pg_dsn, schema=journal_schema),
    ) as py:
        assert [h.id for h in py.schema_versions()] == [1, 2]
        third = release(3, "pg", "sql", "CREATE TABLE IF NOT EXISTS lines (n bigint)")
        assert py.migrate([first, second, third]).applied == (3,)
    with Rhizodat(
        stores={"pg": native.PostgresStore(pg_dsn, "pg", pg_schema)},
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    ) as r:
        assert [h.id for h in r.schema_versions()] == [1, 2, 3]
        with pytest.raises(MigrationError, match="changed since it was applied"):
            r.migrate([first, second, release(3, "pg", "sql", "SELECT 2")])


def test_clickhouse_and_drevo_steps_run_on_the_native_stores(
    pg_dsn: str, journal_schema: str, clickhouse_dsn: str, drevo_bolt_dsn: str
) -> None:
    label = f"Mig{uuid.uuid4().hex[:10]}"
    stores = {
        "ch": native.ClickHouseStore(clickhouse_dsn, "ch"),
        "graph": native.DrevoStore(drevo_bolt_dsn, "graph"),
    }
    with Rhizodat(stores=stores, journal=native.PostgresJournal(pg_dsn, journal_schema)) as r:
        try:
            migration = Migration.of(
                1,
                "events",
                [
                    Step(
                        "ch",
                        "sql",
                        [
                            "CREATE TABLE IF NOT EXISTS events (id Int64) ENGINE = MergeTree"
                            " ORDER BY id",
                            "INSERT INTO events VALUES (1)",
                        ],
                    ),
                    Step("graph", "cypher", f"MERGE (:{label} {{seed: 1}})"),
                ],
            )
            assert r.migrate([migration]).applied == (1,)
            assert r.store("ch").execute("SELECT count() AS n FROM events").one() == {"n": 1}
            assert r.store("graph").execute(f"MATCH (n:{label}) RETURN count(n) AS n").one() == {
                "n": 1
            }
            with pytest.raises(MigrationFailed, match="store 'ch'"):
                r.migrate([migration, release(2, "ch", "sql", "SELECT * FROM no_such")])
            with pytest.raises(MigrationFailed, match="store 'graph'"):
                r.migrate([migration, release(2, "graph", "cypher", "THIS IS NOT CYPHER")])
            assert [h.id for h in r.schema_versions()] == [1]
        finally:
            r.store("graph").execute(f"MATCH (n:{label}) DETACH DELETE n")


def test_iceberg_steps_run_on_the_native_store(
    pg_dsn: str, journal_schema: str, iceberg_rest_dsn: str
) -> None:
    from rhizodat.adapters.iceberg import IcebergStore as PyIcebergStore

    namespace = f"dat_mig_{uuid.uuid4().hex[:10]}"
    table = f"{namespace}.tasks"
    rename = {"op": "rename_column", "table": table, "from": "title", "to": "summary"}
    widen = {"op": "add_column", "table": table, "name": "points", "type": "long"}
    lake = native.IcebergStore(iceberg_rest_dsn, "lake")
    with Rhizodat(
        stores={"lake": lake}, journal=native.PostgresJournal(pg_dsn, journal_schema)
    ) as r:
        reader = PyIcebergStore(iceberg_rest_dsn, name="reader")
        try:
            migration = Migration.of(
                1,
                "tasks",
                [
                    Step(
                        "lake",
                        "iceberg",
                        [
                            {"op": "create_namespace", "namespace": namespace},
                            {
                                "op": "create_table",
                                "table": table,
                                "columns": [
                                    {"name": "id", "type": "string", "required": True},
                                    {"name": "title", "type": "string"},
                                ],
                            },
                            {"op": "add_column", "table": table, "name": "points", "type": "int"},
                            rename,
                        ],
                    )
                ],
            )
            assert r.migrate([migration]).applied == (1,)
            names = [f.name for f in reader.catalog.load_table(table).schema().fields]
            assert names == ["id", "summary", "points"]
            assert r.store("lake").execute(table).columns == ("id", "summary", "points")
            # Idempotent: the same step runs again without complaint.
            lake.run_migration_step("iceberg", migration.steps[0].body)
            with pytest.raises(MigrationFailed, match="store 'lake'") as caught:
                r.migrate(
                    [
                        migration,
                        release(
                            2,
                            "lake",
                            "iceberg",
                            [widen],
                        ),
                    ]
                )
            assert "points is int" in str(caught.value)
        finally:
            for ident in reader.catalog.list_tables(namespace):
                reader.catalog.drop_table(ident)
            reader.catalog.drop_namespace(namespace)
            reader.close()
