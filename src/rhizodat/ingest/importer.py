"""Importing a file into one store: infer, migrate, load.

The schema change an import needs (a new table, or columns the table lacks)
is a migration like any other: written as the next ``NNNN_import_<table>.toml``
in the application's migration directory, reviewed with it, run by
``Rhizodat.migrate`` and recorded with its time. The rows then load through
the store's ``Loads.load``, as one unit. A failed load leaves the table as it
was; the migration stays applied, so the next import only loads.

An import concerns one store, so it needs no meta-transaction: the store's
own atomicity (a transaction, an atomic partition attach, one table commit)
is what makes it all-or-nothing.
"""

import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rhizodat.core.adapter import Loads, Migrates
from rhizodat.ingest.schema import Column, TableSchema
from rhizodat.ingest.sources import DEFAULT_NULLS, _shown, infer_schema, read_batches
from rhizodat.migrations.format import Migration, Step, load_migrations, to_toml

if TYPE_CHECKING:
    from rhizodat.api import Rhizodat

__all__ = ["ImportPlan", "ImportReport", "import_file", "plan_import"]


@dataclass(frozen=True, slots=True)
class ImportPlan:
    source: str
    """The source as shown (secrets masked)."""
    store: str
    table: str
    schema: TableSchema
    new_table: bool
    new_columns: tuple[Column, ...]
    steps: list[Step]
    """The migration steps the import needs; empty when the table already fits."""


@dataclass(frozen=True, slots=True)
class ImportReport:
    plan: ImportPlan
    rows: int
    migration: int | None
    """The id of the migration written and run, if the import needed one."""


def _loader(dat: "Rhizodat", store: str) -> Any:  # noqa: ANN401 - Loads & Migrates
    target = dat.store(store)
    if not isinstance(target, Loads) or not isinstance(target, Migrates):
        raise ValueError(
            f"store {store!r} ({type(target).__name__}) cannot load files: its adapter"
            " implements no Loads capability"
        )
    return target


def plan_import(
    dat: "Rhizodat",
    source: str | Path,
    *,
    store: str,
    table: str,
    format: str | None = None,
    null_values: Sequence[str] = DEFAULT_NULLS,
    sample_rows: int | None = None,
    order_by: Sequence[str] = (),
) -> ImportPlan:
    """What importing ``source`` into ``store.table`` would do; nothing is written."""
    target = _loader(dat, store)
    schema = infer_schema(source, format=format, null_values=null_values, sample_rows=sample_rows)
    existing = target.table_columns(table)
    if existing is None:
        steps = target.create_table_steps(table, schema.columns, order_by=order_by)
        new, new_table = schema.columns, True
    else:
        new = tuple(c for c in schema.columns if c.name not in existing)
        steps = target.add_column_steps(table, new) if new else []
        new_table = False
    # Steps run on the store under its name here, whatever the adapter calls itself.
    named = [Step(store, s.kind, s.body) for s in steps]
    return ImportPlan(_shown(source), store, table, schema, new_table, new, named)


def _slug(table: str) -> str:
    """The table's own name (without its schema or namespace) as a migration name."""
    slug = re.sub(r"[^a-z0-9]+", "_", table.rpartition(".")[2].lower()).strip("_")
    return slug or "table"


def _next_id(dat: "Rhizodat", directory: Path) -> int:
    on_disk = [m.id for m in load_migrations(directory)]
    applied = [a.id for a in dat.schema_versions()]
    return max([0, *on_disk, *applied]) + 1


def _write_migration(dat: "Rhizodat", plan: ImportPlan, directory: Path) -> Migration:
    if plan.new_table:
        name = f"import_{_slug(plan.table)}"
        what = f"create {plan.table} ({len(plan.schema.columns)} columns)"
    else:
        name = f"import_{_slug(plan.table)}_columns"
        what = f"add {', '.join(c.name for c in plan.new_columns)} to {plan.table}"
    migration = Migration.of(
        _next_id(dat, directory),
        name,
        plan.steps,
        description=f"import of {plan.source} into {plan.store}: {what}",
    )
    path = directory / f"{migration.id:04}_{migration.name}.toml"
    path.write_text(to_toml(migration), encoding="utf-8")
    return migration


def import_file(
    dat: "Rhizodat",
    source: str | Path,
    *,
    store: str,
    table: str,
    migrations: str | Path | None = None,
    replace: bool = False,
    format: str | None = None,
    null_values: Sequence[str] = DEFAULT_NULLS,
    sample_rows: int | None = None,
    order_by: Sequence[str] = (),
) -> ImportReport:
    """Import ``source`` into ``store.table``: the migration it needs (written to the
    ``migrations`` directory and run), then the rows, as one unit; ``replace`` swaps
    the table's contents."""
    target = _loader(dat, store)
    plan = plan_import(
        dat,
        source,
        store=store,
        table=table,
        format=format,
        null_values=null_values,
        sample_rows=sample_rows,
        order_by=order_by,
    )
    migration_id = None
    if plan.steps:
        if migrations is None:
            what = "a new table" if plan.new_table else "new columns"
            raise ValueError(
                f"importing into {store}.{table} needs {what}, which is a migration:"
                " pass migrations=<the migration directory> (CLI: --migrations DIR)"
            )
        directory = Path(migrations)
        migration_id = _write_migration(dat, plan, directory).id
        dat.migrate(directory)
    rows = target.load(table, read_batches(source, plan.schema), replace=replace)
    return ImportReport(plan, rows, migration_id)
