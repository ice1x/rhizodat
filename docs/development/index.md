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
`rhizodat_native.ClickHouseStore(dsn, name)` (one-phase, applied after the
decision; no journal of its own) — whose Rust branch, participant and journal
(`crates/rhizodat-postgres`, `crates/rhizodat-drevo`,
`crates/rhizodat-clickhouse`) run the whole protocol against the real store with
no Python callbacks on the path. A Unit of Work may mix native stores (of
different kinds) and Python branch objects; a meta-transaction over several
native stores commits through one of the native journals. A commit with any
native store runs on a current-thread tokio runtime (so it stays single-threaded,
like the callback path) and needs the compose stores.
`tests/core/test_native_postgres.py`, `tests/core/test_native_drevo.py` and
`tests/core/test_native_clickhouse.py` drive it.

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
