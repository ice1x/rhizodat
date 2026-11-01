# Development

`compose.yaml` runs the stores the tests need (ports on 127.0.0.1 only, data in
tmpfs, so every `up` starts empty):

```bash
docker compose up -d --wait   # PostgreSQL (2PC enabled) and a disposable drevo
docker compose --profile '*' up -d --wait   # every store, plus toxiproxy
uv sync --group drevo         # optional: embedded drevo-py from the pinned commit (Rust)
uv sync --group native        # optional: the core in Rust (crates/rhizodat-py, maturin)
uv run pytest
docker compose down
```

The tests read store DSNs from the environment, else from `.env.test`. A DSN from
`.env.test` whose store is not running skips that store's tests with a hint; a
DSN set in the environment is used as is, so a store that is down fails the run
(CI exports `.env.test` for that reason). Point the tests at other stores with
`RHIZODAT_TEST_PG_DSN`, `RHIZODAT_TEST_DREVO_BOLT`,
`RHIZODAT_TEST_CLICKHOUSE_DSN`, `RHIZODAT_TEST_ICEBERG_REST_DSN` and
`RHIZODAT_TEST_TOXIPROXY`. The drevo server must be
**disposable**: 2PC raises the prepared fence and blocks other writers.

```bash
uv sync                  # dev environment
uv run ruff check .      # lint
uv run ruff format .     # format
uv run mypy              # types (strict)
```

The Rust core is a Cargo workspace at the repository root (stable Rust, MSRV
1.94); its tests include the vectors in `conformance/`, and the adapter crates'
tests use the compose PostgreSQL, drevo, ClickHouse and Iceberg REST catalog
(skipped with a hint when a store is not running, unless its
`RHIZODAT_TEST_*` variable is set):

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`crates/rhizodat-py` is the Python module `rhizodat_native` (PyO3): UUIDv7,
the hybrid logical clock, gids and migration files from the Rust core.
`uv sync --group native` builds and installs it (`uv sync` alone removes it
again: list both groups, e.g. `--group drevo --group native`); the package
then makes `new_id` with it, and `RHIZODAT_NATIVE=0` turns it off.
`tests/conformance/test_native_vectors.py` runs the vectors through it.

It also exposes the commit protocol: `rhizodat_native.Coordinator(journal)`
runs a Unit of Work's branches (any Python objects with the branch methods)
through the core on a single-threaded executor, calling back into each object
along the way and raising the same `AbortedError` / `InDoubtError` /
`InvalidUnitOfWork` as the Python reference. The core runs with the GIL
released and each callback re-takes it for its one call.
`tests/core/test_native_coordinator.py` drives it with fake branches.

A branch may also be a native store — `rhizodat_native.PostgresStore(dsn, name,
schema)` with `rhizodat_native.PostgresJournal(dsn, schema)`, or
`rhizodat_native.DrevoStore(dsn, name)` over Bolt with
`rhizodat_native.DrevoJournal(dsn, label)`, or
`rhizodat_native.ClickHouseStore(dsn, name)` or
`rhizodat_native.IcebergStore(dsn, name)` over a REST catalog (both one-phase,
applied after the decision; no journal of their own) — whose Rust branch,
participant and journal (`crates/rhizodat-postgres`, `crates/rhizodat-drevo`,
`crates/rhizodat-clickhouse`, `crates/rhizodat-iceberg`) run the whole protocol against the real store with
no Python callbacks on the path. A Unit of Work may mix native stores (of
different kinds) and Python branch objects; a meta-transaction over several
native stores commits through one of the native journals. A commit with any
native store runs on a current-thread tokio runtime (so it stays single-threaded,
like the callback path) and needs the compose stores.
`tests/core/test_native_postgres.py`, `tests/core/test_native_drevo.py`,
`tests/core/test_native_clickhouse.py` and `tests/core/test_native_iceberg.py`
drive it.

A Unit of Work's own data reaches a store through `store.unit()`. For a
ClickHouse or a PostgreSQL store it is tabular — buffer rows with
`unit.insert(table, columns, rows)` (the core's row format, so no Arrow is
needed); the target table carries the user columns plus `dat_txn` and
`dat_tx_time`, which the commit stamps onto every row. A ClickHouse unit is
one-phase (its rows travel in the journal, applied after the decision) and is
spent by its commit; a PostgreSQL unit writes its rows inside the branch's
two-phase transaction and may be reused across a retry. PostgreSQL rows carry
scalars, decimals, bytes and temporal values (bound as `$n::text::<column type>`,
so the server parses each per its own column); lists and maps are not carried
yet.

A drevo store is a graph, so its unit buffers parameterized Cypher instead:
`unit.run(cypher, params)`. The statements run inside the branch's two-phase
transaction, so they commit atomically with the meta-transaction; each gets
`dat_txn` and `dat_tx_time` parameters injected, so a statement may stamp its
nodes with `$dat_txn` (those names are reserved). A drevo unit is reusable.
Hand any unit to `commit` like a store.

An Iceberg store takes Arrow: `unit.append(table, data)` with any object that has
`__arrow_c_stream__` (a pyarrow `Table` or `RecordBatchReader`, a polars frame)
or the bytes of an Arrow IPC stream, for a `namespace.table`. Columns are matched
by name and cast to the table's types at stage, which writes Parquet files; the
participant appends them in one snapshot tagged with the meta-transaction after
the decision. The data crosses as an IPC stream (pyarrow writes it: one copy) —
reading the C stream directly would need `unsafe`, which the workspace forbids.
The store connects to the catalog inside each commit, so building it does no
I/O. An Iceberg unit is reusable; each commit appends its batches anew.

The `rhizodat.Rhizodat` facade runs on this native core, opt-in: build it with
native store objects and a native journal (or none) —

```python
import rhizodat_native as native
from rhizodat import Rhizodat

r = Rhizodat(
    stores={"pg": native.PostgresStore(dsn, "pg", schema)},
    journal=native.PostgresJournal(dsn, journal_schema),  # or None for one store
)

@r.transactional
def write(uow):
    uow.store("pg").insert("people", ["name"], [["ada"]])
```

and its Units of Work commit through `rhizodat_native.Coordinator` instead of
the Python coordinator, with no Python callbacks on the path. A Unit of Work
buffers its data on each store's native unit (`uow.store(name).insert(...)` for a
tabular store, `uow.store(name).run(cypher, params)` for drevo,
`uow.store(name).append(table, arrow_data)` for Iceberg), and
`transactional`'s retry loop drives it unchanged. Every store must be a native
store object and the journal a native journal or none (mixing native and Python
stores, or a DSN-string journal, is refused). `r.recovery(...)` runs the core's
recovery worker (`rhizodat_native.Recovery(journal, stores)`) with the same
settings and `RecoveryReport` as the Python one: stale undecided records are
presumed aborted and rolled back, committed ones finished and marked `DONE`,
prepared gids without a decision rolled back; a participant that still fails is
reported (`unfinished`, then `stuck`) and retried by a later run.
`tests/core/test_native_recovery.py` crashes a child process (`os._exit`)
before and after the decision with PostgreSQL prepared, then recovers it.
`r.repository(T)` and `uow.repository(T)` run the Python facade's repository
logic with their parts on the core (`rhizodat.core.native_parts`): every native
store reads committed versions, its unit buffers part writes that the commit
applies inside the meta-transaction (ClickHouse and Iceberg: in the journal
payload, after the decision; an Iceberg erase copy-on-write through the REST
catalog). The layout is the Python adapters', so either facade reads the other's
versions (`tests/core/test_native_parts*.py` check both ways).
`r.store(name).execute(...)` on any native store runs on the core and answers
with the facade's `Result` (`rhizodat.core.native_result`,
`tests/core/test_native_execute.py`): ClickHouse (native SQL with `{name:Type}`
parameters) and Iceberg (a scan with `selected_fields`, `snapshot_id`, `limit`)
send the full result as one Arrow IPC stream, so `to_arrow()` is the stream
itself; PostgreSQL (native SQL with `$1..$n` parameters, bound by position and
checked against the statement's parameter types; `numeric` and other undecoded
column types are refused with the `col::text` cast to use) and drevo (Cypher
with named parameters) answer with rows. `r.migrate(...)` and
`r.schema_versions()` run on the native facade too (`tests/core/test_native_migrations.py`):
the native `PostgresJournal` keeps the history in the Python adapter's
`<table>_migrations` table (either facade continues the other's history) and
`MemoryJournal` in memory; the native PostgreSQL store runs `sql` steps in one
transaction, ClickHouse one statement per request, drevo `cypher` steps in their
own transactions, and the native Iceberg store runs `iceberg` steps through the
Python adapter's declarative operations (iceberg-rust 0.10 has no column rename
or partition update). Iceberg scans take `row_filter` in pyiceberg's syntax too
(`rhizodat_iceberg::filter`), so the native facade now covers the Python
facade's whole surface except what each adapter documents as its own.
