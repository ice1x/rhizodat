"""Importing a file: the inferred schema becomes a migration, then the rows load as one unit.

A bug tracker imports its legacy export. The first import creates the table
through a migration file written next to the others (so the schema change is
reviewed and recorded with its time); a later export with one more column adds
it through the next migration; an import into a table that already fits needs
no migration at all. A store that cannot load files is named up front.
"""

from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.core.adapter import Loads
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.journal import MemoryJournal
from rhizodat.ingest import Column, import_file, plan_import
from rhizodat.migrations import Step, load_migrations
from rhizodat.testing.memory import MemoryEntityStore


class TableStore(MemoryEntityStore):
    """A store whose tables are lists of rows; ``ddl`` steps create and widen them."""

    migration_kinds = frozenset({"ddl"})

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.tables: dict[str, list[str]] = {}
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.fail_after: int | None = None

    def run_migration_step(self, kind: str, body: Any) -> None:
        if body["op"] == "create":
            self.tables.setdefault(body["table"], list(body["columns"]))
        else:
            for c in body["columns"]:
                if c not in self.tables[body["table"]]:
                    self.tables[body["table"]].append(c)

    def table_columns(self, table: str) -> list[str] | None:
        return self.tables.get(table)

    def create_table_steps(
        self, table: str, columns: Sequence[Column], *, order_by: Sequence[str] = ()
    ) -> list[Step]:
        body = {"op": "create", "table": table, "columns": [c.name for c in columns]}
        return [Step(self.name, "ddl", body)]

    def add_column_steps(self, table: str, columns: Sequence[Column]) -> list[Step]:
        body = {"op": "add", "table": table, "columns": [c.name for c in columns]}
        return [Step(self.name, "ddl", body)]

    def load(self, table: str, batches: Iterable[Any], *, replace: bool = False) -> int:
        staged: list[dict[str, Any]] = []
        for batch in batches:
            staged.extend(batch.to_pylist())
            if self.fail_after is not None and len(staged) >= self.fail_after:
                raise RuntimeError("the store went away")
        if replace:
            self.rows[table] = []
        self.rows.setdefault(table, []).extend(staged)  # all or nothing
        return len(staged)


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 16, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms


@pytest.fixture
def store() -> TableStore:
    return TableStore("tracker")


@pytest.fixture
def dat(store: TableStore) -> Iterator[Rhizodat]:
    wall = Wall()
    d = Rhizodat(
        stores={"tracker": store, "mem": MemoryEntityStore("mem")},
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d
    d.close()


def export(path: Path, faker: Faker, n: int, *, with_points: bool = False) -> Path:
    header = "id,title,severity" + (",points" if with_points else "")
    rows = [header]
    for i in range(n):
        row = f'{i},"{faker.sentence()}",{faker.random_int(1, 4)}'
        rows.append(row + (f",{faker.random_int(0, 13)}" if with_points else ""))
    path.write_text("\n".join(rows) + "\n")
    return path


def test_the_store_capability() -> None:
    assert isinstance(TableStore("t"), Loads)
    assert not isinstance(MemoryEntityStore(), Loads)


def test_a_first_import_writes_a_migration_runs_it_and_loads(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "0001_base.toml").write_text(
        '[[steps]]\nstore = "tracker"\nddl = { op = "create", table = "users", columns = ["id"] }\n'
    )
    source = export(tmp_path / "bugs.csv", faker, 30)

    report = import_file(dat, source, store="tracker", table="bugs", migrations=migrations)

    assert report.rows == 30
    assert report.migration == 2  # the next number after the directory's own
    written = load_migrations(migrations)[-1]
    assert (written.id, written.name) == (2, "import_bugs")
    assert written.description is not None
    assert "bugs.csv" in written.description
    assert store.tables["bugs"] == ["id", "title", "severity"]
    assert [v.id for v in dat.schema_versions()] == [1, 2]
    assert len(store.rows["bugs"]) == 30


def test_a_wider_export_adds_its_columns_through_the_next_migration(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    import_file(
        dat,
        export(tmp_path / "a.csv", faker, 5),
        store="tracker",
        table="bugs",
        migrations=migrations,
    )
    report = import_file(
        dat,
        export(tmp_path / "b.csv", faker, 7, with_points=True),
        store="tracker",
        table="bugs",
        migrations=migrations,
    )
    assert report.migration == 2
    assert load_migrations(migrations)[-1].name == "import_bugs_columns"
    assert store.tables["bugs"] == ["id", "title", "severity", "points"]
    assert len(store.rows["bugs"]) == 12

    # Same columns again: nothing to migrate, and no directory needed.
    again = import_file(
        dat, export(tmp_path / "c.csv", faker, 3, with_points=True), store="tracker", table="bugs"
    )
    assert (again.migration, again.rows) == (None, 3)


def test_a_dry_run_plans_and_touches_nothing(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    plan = plan_import(dat, export(tmp_path / "a.csv", faker, 5), store="tracker", table="bugs")
    assert [c.name for c in plan.schema.columns] == ["id", "title", "severity"]
    assert plan.new_table
    assert plan.steps == store.create_table_steps("bugs", plan.schema.columns)
    assert store.tables == {}


def test_replace_swaps_the_contents(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    migrations = tmp_path / "m"
    migrations.mkdir()
    import_file(
        dat,
        export(tmp_path / "a.csv", faker, 5),
        store="tracker",
        table="bugs",
        migrations=migrations,
    )
    import_file(
        dat, export(tmp_path / "b.csv", faker, 2), store="tracker", table="bugs", replace=True
    )
    assert len(store.rows["bugs"]) == 2


def test_a_failed_load_leaves_the_table_as_it_was(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    migrations = tmp_path / "m"
    migrations.mkdir()
    store.fail_after = 3
    with pytest.raises(RuntimeError, match="went away"):
        import_file(
            dat,
            export(tmp_path / "a.csv", faker, 5),
            store="tracker",
            table="bugs",
            migrations=migrations,
        )
    assert store.rows.get("bugs", []) == []
    # The schema migration stays: it is recorded, and the next import only loads.
    store.fail_after = None
    assert (
        import_file(
            dat, export(tmp_path / "b.csv", faker, 4), store="tracker", table="bugs"
        ).migration
        is None
    )


def test_refusals_come_before_anything_runs(
    dat: Rhizodat, store: TableStore, tmp_path: Path, faker: Faker
) -> None:
    source = export(tmp_path / "a.csv", faker, 3)
    with pytest.raises(ValueError, match="pass migrations="):
        import_file(dat, source, store="tracker", table="bugs")
    with pytest.raises(ValueError, match=r"store 'mem' .*cannot load files"):
        import_file(dat, source, store="mem", table="bugs", migrations=tmp_path)
    with pytest.raises(KeyError, match="nowhere"):
        import_file(dat, source, store="nowhere", table="bugs", migrations=tmp_path)
    assert store.tables == {}
