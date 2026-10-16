"""An IT task manager evolves its schema across every store with one migration history.

Release 1 creates the task table in PostgreSQL, the activity log in ClickHouse,
the task graph's uniqueness in drevo and the lakehouse table in Iceberg. Release
2 adds a column everywhere it is needed. The history lives in the PostgreSQL
journal; a re-run applies nothing, a failed step is resumed, and the schema in
force at a past moment is known.
"""

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from iceberg_parts import drop_parts
from rhizodat import Rhizodat
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.migrations import MigrationError, MigrationFailed

RELEASE_1 = '''
description = "tasks, their activity log, the task graph and the lakehouse copy"

[[steps]]
store = "pg"
sql = """
CREATE TABLE IF NOT EXISTS task (
    id uuid PRIMARY KEY,
    title text NOT NULL,
    status text NOT NULL DEFAULT 'open'
)"""

[[steps]]
store = "events"
sql = [
  """CREATE TABLE IF NOT EXISTS activity (task UUID, at DateTime64(3), what String)
     ENGINE = MergeTree ORDER BY (task, at)""",
]

[[steps]]
store = "graph"
cypher = "MERGE (s:{label} {{title: '{label}:schema', version: 1}})"

[[steps]]
store = "lake"
iceberg = [
  {{ op = "create_namespace", namespace = "{ns}" }},
  {{ op = "create_table", table = "{ns}.tasks", columns = [
      {{ name = "id", type = "string", required = true }},
      {{ name = "title", type = "string" }},
      {{ name = "closed", type = "timestamptz" }},
  ] }},
  {{ op = "add_partition", table = "{ns}.tasks", column = "closed", transform = "month" }},
]
'''

RELEASE_2 = """
description = "story points"

[[steps]]
store = "pg"
sql = "ALTER TABLE task ADD COLUMN IF NOT EXISTS points int"

[[steps]]
store = "events"
sql = "ALTER TABLE activity ADD COLUMN IF NOT EXISTS points Nullable(Int32)"

[[steps]]
store = "lake"
iceberg = [{{ op = "add_column", table = "{ns}.tasks", name = "points", type = "int" }}]
"""


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 15, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def at(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def names() -> dict[str, str]:
    """A drevo label and an Iceberg namespace of this test only (servers are shared)."""
    tag = uuid.uuid4().hex[:10]
    return {"label": f"TaskSchema{tag}", "ns": f"tracker_{tag}"}


@pytest.fixture
def dat(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    pg_schema: str,
    clickhouse_dsn: str,
    iceberg_rest_dsn: str,
    tmp_path: Path,
    wall: Wall,
    names: dict[str, str],
) -> Iterator[Rhizodat]:
    from rhizodat.adapters.clickhouse import ClickHouseStore
    from rhizodat.adapters.drevo import DrevoStore
    from rhizodat.adapters.iceberg import IcebergStore

    graph = DrevoStore(request.getfixturevalue("drevo_bolt_dsn"), name="graph")
    lake = IcebergStore(iceberg_rest_dsn, name="lake", parts_namespace=names["ns"])
    d = Rhizodat(
        stores={
            "pg": PostgresStore(pg_dsn, schema=pg_schema),
            "events": ClickHouseStore(clickhouse_dsn, name="events"),
            "graph": graph,
            "lake": lake,
        },
        journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d
    try:
        graph.execute(f"MATCH (s:{names['label']}) DELETE s")
        drop_parts(lake)
    finally:
        d.close()


def write(directory: Path, names: dict[str, str], *releases: tuple[str, str]) -> Path:
    directory.mkdir(exist_ok=True)
    for filename, text in releases:
        (directory / filename).write_text(text.format(**names))
    return directory


def columns(dat: Rhizodat, store: str, query: str, key: str) -> list[str]:
    return [r[key] for r in dat.store(store).execute(query).all()]


def pg_columns(dat: Rhizodat) -> list[str]:
    return columns(
        dat,
        "pg",
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema()"
        " AND table_name = 'task' ORDER BY ordinal_position",
        "column_name",
    )


def lake_columns(dat: Rhizodat, ns: str) -> list[str]:
    lake: Any = dat.store("lake")
    return [f.name for f in lake.catalog.load_table(f"{ns}.tasks").schema().fields]


def test_two_releases_migrate_every_store_once(
    dat: Rhizodat, tmp_path: Path, wall: Wall, names: dict[str, str], faker: Faker
) -> None:
    folder = write(tmp_path / "migrations", names, ("0001_tasks.toml", RELEASE_1))
    assert dat.migrate(folder).applied == (1,)
    release_1_at = wall.at()
    wall.ms += 60_000

    assert pg_columns(dat) == ["id", "title", "status"]
    assert columns(dat, "events", "DESCRIBE TABLE activity", "name") == ["task", "at", "what"]
    graph_nodes = dat.store("graph").execute(f"MATCH (s:{names['label']}) RETURN s.version AS v")
    assert [r["v"] for r in graph_nodes.all()] == [1]
    assert lake_columns(dat, names["ns"]) == ["id", "title", "closed"]

    write(folder, names, ("0002_points.toml", RELEASE_2))
    assert dat.migrate(folder, dry_run=True).pending == (2,)
    report = dat.migrate(folder)
    assert (report.applied, report.already) == ((2,), (1,))
    assert pg_columns(dat)[-1] == "points"
    assert columns(dat, "events", "DESCRIBE TABLE activity", "name")[-1] == "points"
    assert lake_columns(dat, names["ns"])[-1] == "points"

    # Nothing left to do; the data written in between is untouched.
    dat.store("pg").execute(
        "INSERT INTO task (id, title, points) VALUES (%s, %s, 3)", (uuid.uuid4(), faker.sentence())
    )
    assert dat.migrate(folder).applied == ()
    assert [r["n"] for r in dat.store("pg").execute("SELECT count(*) AS n FROM task").all()] == [1]

    assert [v.id for v in dat.schema_versions(as_of=release_1_at)] == [1]
    assert [v.id for v in dat.schema_versions()] == [1, 2]


def test_a_failed_release_is_resumed_after_the_fix(
    dat: Rhizodat, tmp_path: Path, names: dict[str, str]
) -> None:
    folder = write(tmp_path / "migrations", names, ("0001_tasks.toml", RELEASE_1))
    broken = RELEASE_2.replace("ADD COLUMN IF NOT EXISTS points Nullable(Int32)", "ADD COLUMN")
    write(folder, names, ("0002_points.toml", broken))

    with pytest.raises(MigrationFailed) as failed:
        dat.migrate(folder)
    assert (failed.value.migration, failed.value.step, failed.value.store) == (2, 2, "events")
    assert [v.id for v in dat.schema_versions()] == [1]
    assert pg_columns(dat)[-1] == "points"  # step 1 of release 2 had run

    # Editing an unapplied migration is fine; the whole of it runs again.
    write(folder, names, ("0002_points.toml", RELEASE_2))
    assert dat.migrate(folder).applied == (2,)

    # Editing an applied one is not.
    write(folder, names, ("0002_points.toml", RELEASE_2.replace("points int", "points bigint")))
    with pytest.raises(MigrationError, match="changed since it was applied"):
        dat.migrate(folder)
