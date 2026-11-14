# The shipped stores and their limits

What each of the four stores carries on the Python facade and on the native
facade (`rhizodat[native]`, the core in Rust under the same API), and where it
stops. A limit listed here is a known one; anything else that fails is a bug,
please report it.

| Store | DSNs | Joins as | Journal here? | Native facade |
|---|---|---|---|---|
| PostgreSQL | `postgresql://`, `postgres://` (`?schema=`) | two-phase (`PREPARE TRANSACTION`) | yes | yes |
| drevo | `drevo:///path.drevo`, `drevo+mem://`, `drevo+bolt://host:7687` | two-phase (`drevo.tx.prepare`) | yes, when no branch is prepared there | over Bolt |
| ClickHouse | `clickhouse://user:pass@host:8123/db` | one-phase, rows in the journal | no | yes |
| Apache Iceberg | `iceberg+rest://`, `iceberg+sql://`, `iceberg+glue://` | one-phase, files staged before the decision | no | REST and Glue catalogs |

One-phase stores apply after the decision, so a reader may briefly see the
two-phase stores' share first; the Repository hides this by reading the newest
version every part already has.

## PostgreSQL

- The server needs `max_prepared_transactions > 0` for meta-transactions over
  several stores. In production the journal's database must replicate
  synchronously: the `COMMITTED` record is the commit point.
- A store or a journal with a `schema` works in that schema (search path
  `"<schema>", public`). `install` (and `install_part`) and opening the journal
  create the schema when it is missing, on both facades (0.2.0 and earlier: a missing
  schema put the tables in `public`). A role that may not create schemas works
  with one that exists. The commit markers go to `"<schema>".dat_applied`,
  never to another schema's through the search path.
- Entity parts: one table per type, a typed column per field, `jsonb` for
  lists, dicts and anything without a native type.
- Native facade, rows (`uow.store(name).insert`): scalars, decimals, bytes and
  temporal values, bound as `$n::text::<column type>` so the server parses each
  per its column. A list goes to an array column (`text[]`, `integer[][]`) or a
  `json(b)` one, a map to `json(b)`; decimals are JSON numbers, UUIDs and
  temporal values JSON strings. **Bytes and non-finite numbers have no JSON
  form, and a map cannot go into an array**: such a row aborts the transaction.
- Native facade, `execute`: parameters are positional only (`$1..$n`), checked
  against the statement's parameter types; result columns are decoded for
  `bool`, integers, floats, `numeric` (exactly, as a `Decimal` with its scale,
  `NaN` and the infinities included), `text`, `uuid`, `bytea`, `json(b)`,
  `timestamp(tz)` and `date`. **Other column types (`interval`, arrays, ranges,
  …) are refused** with the cast to use (`gap::text`). The Python facade binds
  and decodes whatever psycopg does.
- Migrations: `sql` steps; the native store runs a migration's statements in
  one transaction.

## drevo

- `read_only(..., timeout_s=)` (rhizodat-MCP's query timeout) is sent as the
  Bolt transaction timeout, which drevo enforces from 0.0.86. **On an older
  server and on embedded drevo a running query cannot be stopped**: a heavy
  read (a large cross product) runs to its end and holds its memory there. The
  test suite still pins drevo 0.0.43.
- Embedded (`drevo:///`, `drevo+mem://`) needs `drevo-py`, which is not on PyPI
  yet and is built from source with a Rust toolchain; one process at a time.
  Remote (`drevo+bolt://`) works with any Neo4j driver. The native facade
  reaches drevo over Bolt.
- The **prepared fence**: while a branch is prepared, every other write to that
  graph fails fast with a retryable error, so the journal must live elsewhere
  and graph write throughput is bounded by the prepare → commit window. The
  behaviour of a prepared transaction on failover to a replica is still to be
  confirmed with drevo.
- Entity parts: one node per version, labelled after the type; `str`, `int`,
  `float`, `bool` natively, references and UUIDs as strings, anything else as
  tagged JSON. Node titles are unique graph-wide, so a field named `title` is
  stored as `dat_title`; map other non-unique fields away from `title`.
  Every reference is also an edge from the version to the target's anchor
  (`:DatEntity {dat_id}`), so a graph query sees every version's edges; the
  latest version decides (see `tests/scenarios/test_task_manager.py`).
- Native facade, `execute`: Cypher with named parameters; **columns come back
  sorted by name, and a result with no rows has no columns**.
- Migrations: `cypher` steps, each in its own transaction.

## ClickHouse

- No transactions: a branch buffers rows, stage checks them against the table
  schema and `max_rows_per_table` (default 100 000), the rows travel in the
  journal payload and are inserted after the decision. Tables written through
  rhizodat need `dat_txn UUID` and `dat_tx_time Int64` columns, ideally with a
  `bloom_filter` skip index on `dat_txn`; use `ReplicatedMergeTree` where
  deduplication tokens matter.
- Entity parts: one `MergeTree` table per type, one row per version.
  **`Decimal`, `bytes`, lists and dicts are tagged JSON in a `String`
  column**, so the server cannot do arithmetic on them: keep money you want to
  sum in ClickHouse as integers (cents), as `tests/scenarios/test_erp.py` does.
- Native facade: rows travel as `JSONEachRow`, so **a `bytes` value must be
  valid UTF-8** and anything else is refused at stage. `execute` binds
  `{name:Type}` parameters server-side; `SELECT`-like statements answer with
  one Arrow stream (`Result.to_arrow()`), other statements run as commands.
- Migrations: `sql` steps, one statement per request.

## Apache Iceberg

- Catalogs: `iceberg+rest://`, `iceberg+sql://` (PostgreSQL or SQLite) and
  `iceberg+glue://<region>?warehouse=s3://bucket/prefix` (AWS Glue, with
  `rhizodat[glue]`; credentials as `glue.*` parameters or from the AWS
  environment). The native facade supports REST and Glue (not SQL), with S3
  through OpenDAL; the same DSN serves both facades. **On the native facade, a
  hard delete of entity parts needs the REST catalog.**
- Application tables need `dat_txn` (`uuid` or `string`) and `dat_tx_time`
  (`long`) columns, and **only appends** are supported for them. A branch over
  several tables commits one snapshot per table (Iceberg has no multi-table
  commit): atomic with the meta-transaction by roll-forward, not by Iceberg.
- Entity parts live in their own namespace (`parts_namespace`, `dat` by
  default), `dat_id` / `dat_txn` as text; a hard delete rewrites the data files
  that hold the entity's versions (copy-on-write). Older snapshots keep the old
  files until snapshot expiry and orphan-file removal, the usual Iceberg
  maintenance.
- Partitioned application tables: the native facade writes one file per
  partition of the table's default spec, every transform included (each row's
  partition computed by the spec's transforms); the Python facade writes what
  pyiceberg's append supports. The native hard delete of entity parts needs an
  unpartitioned table, which the parts namespace always has.
- Native facade: scans
  (`execute(table, {...})`) take `selected_fields`, `snapshot_id`, `limit`
  (applied client-side) and `row_filter` in pyiceberg's syntax; the result
  crosses as one Arrow stream, UUID columns tagged `arrow.uuid`.
- Migrations: `iceberg` steps are the adapter's declarative operations; the
  native store delegates them to pyiceberg (iceberg-rust 0.10 has no column
  rename and no partition-spec update), so `rhizodat[iceberg]` must be
  installed for them on either facade.

## Both facades

- A `Rhizodat` is either all-Python (DSN strings or Python store objects) or
  all-native (native store objects and a native journal, or none): mixing is
  refused.
- The layouts are the same, so a version written by one facade is read by the
  other, and either continues the other's migration history
  (`tests/scenarios/` closes each scenario that way).
