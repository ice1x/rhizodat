# Schema migrations

One migration history for all stores, in a **language-neutral format**: a
directory of TOML files `NNNN_name.toml` (the number is the migration's id),
each a list of steps. A step names a store of the configuration and has
exactly one body, keyed by its kind:

```toml
# migrations/0001_tasks.toml
description = "tasks, their activity log and the lakehouse copy"

[[steps]]
store = "pg"
sql = """
CREATE TABLE IF NOT EXISTS task (
    id uuid PRIMARY KEY,
    title text NOT NULL
)"""

[[steps]]
store = "events"
sql = ["CREATE TABLE IF NOT EXISTS activity (task UUID, at DateTime64(3)) ENGINE = MergeTree ORDER BY (task, at)"]

[[steps]]
store = "graph"
cypher = "CREATE INDEX task_title IF NOT EXISTS FOR (t:Task) ON (t.title)"

[[steps]]
store = "lake"
iceberg = [
  { op = "create_namespace", namespace = "tracker" },
  { op = "create_table", table = "tracker.tasks", columns = [
      { name = "id", type = "string", required = true },
      { name = "closed", type = "timestamptz" },
  ] },
  { op = "add_partition", table = "tracker.tasks", column = "closed", transform = "month" },
]
```

| Store | Kind | Step body |
|---|---|---|
| PostgreSQL | `sql` | SQL statements, one transaction per step (DDL is transactional) |
| ClickHouse | `sql` | SQL statements, one request each (including `ENGINE`, `ORDER BY`, `TTL`) |
| drevo | `cypher` | Cypher statements, each in autocommit mode (e.g. `CREATE VECTOR INDEX`) |
| Iceberg | `iceberg` | declarative operations: `create_namespace`, `create_table`, `add_column`, `rename_column`, `drop_column`, `add_partition` |

A plug-in adapter runs its own kinds (`Migrates`: `migration_kinds`,
`run_migration_step`).

```python
report = dat.migrate("migrations")          # or a list of Migration objects
report.applied, report.pending, report.already
dat.migrate("migrations", dry_run=True)     # check and report only
dat.schema_versions(as_of=last_monday)      # the schema in force then
```

```
RHIZODAT_STORE_PG=postgresql://… RHIZODAT_JOURNAL=postgresql://… rhizodat migrate migrations
```

- The history lives in the journal store (`dat_journal_migrations` in
  PostgreSQL and SQLite, nodes in drevo): id, name, checksum and the HLC time
  of the record, so `schema_versions(as_of=...)` knows which schema applied
  at a moment.
- Before anything runs, the files are checked against the history: an applied
  migration must still be there unchanged (the checksum: sha256 of a canonical
  JSON form, the same in every implementation), a new one must be newer than
  every applied one, and every step must name a store that runs its kind.
- Steps must be idempotent (`IF NOT EXISTS`, checks before change; the
  Iceberg operations are by themselves): a migration across stores is not
  atomic, so a failing step stops the run with `MigrationFailed`, the
  migration stays unrecorded, and the next run repeats all of it.
- One runner at a time. A second runner that records a migration first is
  not an error: the steps ran twice, which they allow.
- The format is pinned by `conformance/migrations.json`; the Rust core
  parses it (`rhizodat_core::migrations`) with the same checksums.

---|---|
| PostgreSQL | SQL |
| ClickHouse | SQL (including `ENGINE`, `ORDER BY`, `TTL`) |
| drevo | Cypher (e.g. `CREATE VECTOR INDEX`) |
| Iceberg | declarative schema operations (add / rename / drop column, partition spec) |

- The version table lives in the journal store.
- Steps are idempotent (`IF NOT EXISTS`, checks before change): a migration
  across stores is not atomic, so a failed run must be safe to repeat.
- Schema versions are recorded with their transaction time, so `as_of` reads
  know which schema applied then.

Alembic may be used inside the Python prototype as a runner, but the step
format is rhizodat's own so other language ports can execute it.
