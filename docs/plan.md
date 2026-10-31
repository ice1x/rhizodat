# Plan

## v0.1 roadmap (Python, the executable specification)

1. ✔ Package skeleton, CI (pytest, ruff, mypy).
2. ✔ Core without stores: identity, HLC, DSN parsing, mapping, result model.
3. ✔ PostgreSQL + drevo adapters; meta-transactions with 2PC and retries; journal
   and recovery, including crash tests between prepare and commit; Unit of Work
   and `transactional`.
4. ✔ Docker Compose for development and tests (below).
   ✔ ClickHouse branch: buffered rows, schema check at stage, rows in the
   journal, idempotent insert after the decision, roll-forward by recovery.
   ✔ Iceberg branch: data files written at stage, their paths in the journal,
   one tagged snapshot per table after the decision, roll-forward by recovery,
   staged files deleted on abort.
5. Bitemporal reads (`as_of`, `known_at`, `history`), with the storage format of
   entity parts and repository persistence.
   ✔ Repository CRUD, versions, `as_of` in transaction time, hard and soft
   delete; parts in PostgreSQL, ClickHouse, drevo and Iceberg; the parts
   contract. ✔ Valid time: periods stored with every part, `as_of` /
   `known_at` reads.
6. ✔ Migrations: one TOML format for every store, the history in the journal
   with transaction time, `rhizodat migrate`.
7. Integration scenarios: CBT journal, story/book editor, IT task manager, ERP,
   bug tracker.

Out of scope for v0.1: async API, savepoints / nested transactions, cross-store
query planning.

## Docker Compose for development and tests

One `compose.yaml` runs every store the tests need, locally and in CI alike
(`docker compose up -d --wait`, then `uv run pytest`):

- ✔ PostgreSQL with `max_prepared_transactions > 0`;
- ✔ drevo from the pinned multi-arch image with the `drevo.tx.*` 2PC procedures
  (`ice1x/drevo:0.0.43`, [ice1x/drevo#591](https://github.com/ice1x/drevo/issues/591)),
  so CI now tests drevo over Bolt too;
- ✔ healthchecks, and `.env.test` with the test DSNs;
- ✔ ClickHouse behind the `clickhouse` profile, so a test run starts only what
  it needs;
- ✔ Iceberg behind the `iceberg` profile: the Apache REST catalog fixture over
  S3-compatible object storage (rustfs; the MinIO image is no longer on Docker
  Hub), with a one-shot container creating the bucket. The adapter tests run
  against it and against a SQLite catalog with a local warehouse;
- ✔ toxiproxy behind the `faults` profile: tests create proxies through its API
  and cut the network to the journal or a store at a chosen protocol step (the
  journal before / after the decision, PostgreSQL, ClickHouse or drevo after it)
  and check `InDoubt` / `CommittedPending` and what recovery does once the
  network is back.

Compose is for development, tests and demos on one host. It is not the
production deployment.

## Rust core

The parts that must behave identically everywhere move to one Rust
implementation instead of separate ports per language:

| Part | Why Rust |
|---|---|
| Commit protocol, journal, recovery | one implementation instead of N ports of a subtle protocol; states and outcomes enforced by types |
| HLC, UUIDv7, gid | shared by every binding, bit-for-bit identical |
| Parallel prepare / finish | async (tokio) shortens the lock window and drevo's prepared fence |
| Embedded drevo | drevo is Rust: use `drevo-core` directly, without drevo-py or Bolt |
| Recovery worker, server | long-running processes with predictable memory and latency |

Speed of the coordinator itself is not the reason: commit latency is set by
network round trips to the journal and the stores, not by CPU.

Steps:

1. **Conformance suite as data** (JSON / YAML), run by every implementation:
   ✔ UUIDv7, HLC, gid, the journal transition table, DSN vectors and the crash
   matrix (failure point × state left × outcome after recovery) in
   [`conformance/`](https://github.com/ice1x/rhizodat/tree/main/conformance), run against the Python reference.
2. ✔ **Driver check** (2026-10-12, spikes against the compose stores): every
   step the protocol needs works in all four drivers; none stays on Python.

   | Store | Crate | Verdict | Caveats for the port |
   |---|---|---|---|
   | PostgreSQL | `tokio-postgres` 0.7.18 | ready | gids are SQL literals in `PREPARE TRANSACTION` (quoted by hand); `SqlState` gives `42704` / `40001`; connection loss is `is_closed()` without a code |
   | drevo (Bolt) | `neo4rs` 0.9.0-rc.10 | ready with caveats | release candidate; Bolt 4.4; the prepared fence is a `Transient` error; an unknown gid is still recognised by its message only ([ice1x/drevo#590](https://github.com/ice1x/drevo/issues/590)); no default timeout on pool checkout, and result streams hold pooled connections until dropped |
   | ClickHouse | `clickhouse` 0.15.2 | ready with caveats | server error codes are parsed from the `Code: N.` message; journal batches need the formatted insert (`JSONEachRow`), not typed rows |
   | Iceberg | `iceberg` 0.10.1 + REST catalog + `iceberg-storage-opendal` | ready with caveats | 0.x API; snapshot summary properties, staged Parquet writes, fast append, snapshot listing, `FileIO::delete` and uuid columns work; building a `DataFile` from a path is crate-private, so the Rust journal payload carries the `DataFile` JSON (`serialize_data_file_to_json`, column statistics kept); the commit already rejects files referenced by the table; tables written by Rust read back in pyiceberg |

   Embedded drevo will use `drevo-core` directly and is checked with step 4.
3. **`rhizodat-core` crate** ([`crates/rhizodat-core`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-core)):
   protocol, journal, recovery, HLC, identity, tested on fake stores like the
   Python core.
   ✔ Identity (UUIDv7), HLC, gids, the journal state machine and DSN parsing,
   passing the same vectors as Python. Writing them found two gaps in the
   reference, now fixed and pinned by new vectors: another spelling of a
   `dat_txn` (upper case, braces, no hyphens) was taken for a rhizodat gid, and
   an unbalanced IPv6 bracket in a DSN raised a bare `ValueError`.
   ✔ The journal interface with an in-memory journal, the coordinator and
   recovery, async and runtime-agnostic (the binding passes a `Timer`). Their
   tests are the port of the Python ones on the same fakes, including the
   all-or-nothing property test, and `crash_matrix.json` replays with
   identical step traces. A crash is a panic (or a dropped future), which the
   core never catches. The port found one more gap, fixed in both: when the
   coordinator gave up deciding and the journal then could not be read, the
   reference raised the driver's error instead of `InDoubt`.
   Branches are still prepared and finished one by one, as the traces pin
   down; parallel prepare / finish comes with the Rust adapters.
4. ✔ **Rust adapters**: PostgreSQL, drevo (embedded, and Bolt), the
   PostgreSQL journal.
   ✔ [`crates/rhizodat-postgres`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-postgres) (tokio-postgres):
   the two-phase store, its participant and the PostgreSQL journal, on the
   same tables as the Python adapter. The journal's `branches` text is pinned
   in `conformance/journal.json`, so either implementation recovers the
   other's records. Tested against the compose PostgreSQL, including a
   meta-transaction over two stores, a crash after prepare (presumed abort)
   and a crash after the decision (roll-forward), both recovered by a worker
   with its own connections. Stores must not share a schema (see "One marker
   table per store"); the Python facade refuses it, the Rust crates document it
   until a Rust facade exists.
   ✔ [`crates/rhizodat-drevo`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-drevo) (neo4rs, Bolt): the
   two-phase graph store, its participant and the drevo journal, on the same
   nodes as the Python adapter; tested against the compose drevo, including
   the prepared fence and a bug tracker over PostgreSQL and drevo with crashes
   after prepare and after the decision. neo4rs retries a failed auto-commit
   statement on transient errors for up to 60 seconds, which would hide the
   fence from rhizodat's retry policy: writes therefore run in explicit
   transactions (never retried), while reads and the `drevo.tx.*` procedures
   stay in auto-commit mode, since under the fence drevo rejects the commit of
   any explicit transaction and accepts the procedures only outside one
   ([ice1x/drevo#608](https://github.com/ice1x/drevo/issues/608)).
   ✔ Embedded drevo (`drevo:///path`, `drevo+mem://`) behind the crate's
   `embedded` feature, on the native engine of the pinned drevo commit (the
   one drevo-py uses), with typed two-phase errors; every drevo scenario runs
   on Bolt and embedded, and a durable embedded graph keeps a prepared
   transaction across a restart. Step 4 is complete.

   Beyond step 4, so that no store stays on Python:
   ✔ [`crates/rhizodat-clickhouse`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-clickhouse) (`clickhouse`
   crate): the one-phase branch applied after the decision, with the same
   count / delete / reinsert idempotency, deduplication tokens and
   `dat_applied` marker as the Python adapter. Its journal payload is the
   shared row format, now in `rhizodat_core::rows` and pinned by
   [`conformance/rows.json`](https://github.com/ice1x/rhizodat/blob/main/conformance/rows.json): a Python payload is
   applied by the Rust participant and the reverse (checked both ways). Rows
   travel as `JSONEachRow` (columns are known only at run time) with
   `date_time_input_format = best_effort`; since JSON strings are UTF-8, a
   bytes value must be valid UTF-8 there and anything else is refused at
   stage. The ClickHouse crate needs Rust 1.89.
   ✔ [`crates/rhizodat-iceberg`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-iceberg) (iceberg-rust 0.10,
   REST catalog, S3 through OpenDAL): the one-phase branch committed after the
   decision, with the same tagged snapshots and idempotency checks as the
   Python adapter. A branch buffers Arrow record batches; stage conforms them
   to the table's schema (by name, missing optional columns null, types cast)
   and writes Parquet files. The journal payload is the shared staged-files
   format, now in `rhizodat_core::staged` and pinned by
   [`conformance/staged_files.json`](https://github.com/ice1x/rhizodat/blob/main/conformance/staged_files.json): paths,
   plus the `DataFile` JSON of each file that iceberg-rust needs to commit
   it. A payload with paths only (the Python adapter's) is committed from the
   Parquet footers, and the Python adapter commits a Rust payload by its paths
   (checked both ways). Tested on iceberg-rust's in-memory catalog over a local
   warehouse and on the compose REST catalog, including an order in
   PostgreSQL with its sales in Iceberg, committed together and rolled forward
   by recovery after a crash. Not yet in Rust: partitioned tables (refused at
   stage) and the SQL / Glue catalogs. iceberg-rust 0.10 needs Rust 1.94, now
   the workspace MSRV. Every store now has a Rust adapter.
5. **Bindings**: the `rhizodat` Python package becomes a PyO3 wrapper over the
   core without changing its public API; a C ABI (cbindgen) for Go. Open
   questions to settle first: Python callbacks (`transactional` functions) into
   an async core and the GIL, and results across the boundary (Arrow).
   ✔ First step, [`crates/rhizodat-py`](https://github.com/ice1x/rhizodat/tree/main/crates/rhizodat-py): the optional
   module `rhizodat_native` (PyO3, abi3, built with maturin) with the pure
   parts of the core: UUIDv7, the HLC, gids and migration files, passing the
   same vectors. The package makes `new_id` with it when it is installed and
   stays pure Python otherwise. Callbacks and the GIL, settled for synchronous
   callbacks: the core holds a mutex while it calls its sources, so a generator
   or clock over the system sources (no Python) is shared between threads and
   releases the GIL, while one over Python callables is bound to the thread
   that made it; a callback's exception is kept and raised once the core
   returns.
   ✔ The coordinator across the boundary (`rhizodat_native.Coordinator`): a
   Python Unit of Work hands it its branches (any objects with the branch
   methods) and the core runs the commit protocol — one-phase, two-phase
   through the in-memory journal, roll back before the decision, the
   all-or-nothing property — calling back into each object's `stage` /
   `prepare` / `finish` and so on. The async core runs on a single-threaded
   executor with the GIL released (`Python::detach`) and each callback re-takes
   it for its one call (`Python::attach`), so nothing leaves the calling
   thread and there is no cross-thread hazard; the first exception a callback
   raises is kept and chained as the `__cause__` of `AbortedError` /
   `InDoubtError`, as the Python reference chains the store's own error. Driven
   by fakes, like the Rust and Python coordinator tests, so no Docker.
   ✔ The first real adapter behind this surface (`rhizodat_native.PostgresStore`
   and `rhizodat_native.PostgresJournal`): a Unit of Work of native stores runs
   the whole protocol through the core against real PostgreSQL — one-phase for a
   single store, two-phase through the PostgreSQL journal for several, roll back
   before the decision, the all-or-nothing property, idempotent replay — with no
   Python callbacks on the path (a Unit of Work may still mix native stores and
   Python branch objects). A native commit runs on a current-thread tokio
   runtime (`TokioTimer` for the retry policy) that drives the adapter and its
   connection tasks on the calling thread, so it keeps the single-threaded
   property of the Python-callback path. `tests/core/test_native_postgres.py`
   drives it against the compose PostgreSQL.
   ✔ The drevo adapter behind the same surface (`rhizodat_native.DrevoStore`
   over Bolt, `rhizodat_native.DrevoJournal`): the stores now share one
   `NativeStore` path, so a Unit of Work mixes native adapters freely — a drevo
   store and a PostgreSQL store commit together as one meta-transaction through
   a journal (the first cross-adapter native Unit of Work), on the same
   current-thread runtime (neo4rs and tokio-postgres driven side by side).
   `tests/core/test_native_drevo.py` drives it against the compose drevo.
   ✔ ClickHouse behind the same surface (`rhizodat_native.ClickHouseStore`): the
   one-phase branch, applied after the decision. ClickHouse has no journal of its
   own, so a meta-transaction that includes it (for example with a PostgreSQL
   store) commits through another store's journal.
   `tests/core/test_native_clickhouse.py` drives it against the compose
   ClickHouse.
   ✔ The first data across the boundary (`rhizodat_native.ClickHouseUnit`,
   `store.unit()`): Python buffers a Unit of Work's rows into it with `insert`
   (the core's row format, so no Arrow dependency) and hands it to `commit` like
   a store, so a native ClickHouse commit writes real rows, not just the applied
   marker. A unit is spent by its commit. `tests/core/test_native_clickhouse.py`
   writes rows and reads them back, including a ClickHouse + PostgreSQL commit.
   ✔ The same row path for PostgreSQL (`PostgresStore.unit()`): a unit's rows are
   written inside the branch's own transaction at stage, so they commit atomically
   with the meta-transaction through the two-phase protocol. The target table
   carries the user columns plus `dat_txn`/`dat_tx_time`, stamped onto every row;
   values are bound as `$n::text::<column type>`, so the server parses each per its
   own column (scalars, decimals, bytes and temporal values; lists and maps not
   yet). A PostgreSQL unit may be reused across a retry.
   `tests/core/test_native_postgres.py` writes rows, reads them back, and replays a
   commit without duplicating them, including a unit + second-store meta-transaction.
   ✔ The data path for drevo (`DrevoStore.unit()`): a graph store buffers
   parameterized Cypher instead of rows (`unit.run(cypher, params)`), run inside the
   branch's two-phase transaction so the statements commit atomically with the
   meta-transaction. rhizodat injects `dat_txn`/`dat_tx_time` parameters into every
   statement, so a write may stamp its nodes with `$dat_txn` (those names are
   reserved). A drevo unit is reusable across meta-transactions.
   `tests/core/test_native_drevo.py` writes nodes, reads them back stamped, reuses a
   unit across two meta-transactions, and commits a unit together with a PostgreSQL
   store through a journal.
   ✔ The package's own `transactional` on the core (opt-in): a `Rhizodat` facade
   built with native store objects and a native journal (or none) runs its Units of
   Work through `rhizodat_native.Coordinator` — the whole protocol in Rust, no Python
   callbacks on the path. A Unit of Work buffers its data on each store's native unit
   (`uow.store(name).insert(...)` / `.run(cypher, params)`) and `transactional`'s
   retry loop drives the native Unit of Work unchanged. Repositories, parts,
   `execute` and migrations are not on this path yet. `tests/core/test_native_facade.py`
   writes through the facade to one PostgreSQL store, one ClickHouse store, and a
   PostgreSQL + drevo meta-transaction through a journal.
   ✔ Iceberg behind the same surface (`rhizodat_native.IcebergStore`, REST
   catalog): the first Arrow across the boundary. `store.unit().append(table, data)`
   takes any object with `__arrow_c_stream__` (or Arrow IPC stream bytes); stage
   writes Parquet files and the participant appends them after the decision, so an
   Iceberg unit and a PostgreSQL store commit together through the PostgreSQL
   journal, and the facade takes the native Iceberg store like the others. The data
   crosses as an Arrow IPC stream (one copy, safe Rust): importing the C stream
   directly needs `unsafe` (forbidden in the workspace) or an Arrow binding for
   pyo3 0.30 at iceberg-rust's Arrow 58, which does not exist yet. The store
   connects inside each commit's runtime (the REST client is bound to the runtime
   that made it), so it is built without I/O. `tests/core/test_native_iceberg.py`
   writes, casts, reuses a unit, aborts on a missing table or unfit rows, and
   rolls back PostgreSQL when Iceberg fails.
   ✔ Recovery on the core (`rhizodat_native.Recovery(journal, stores)`): the core's
   recovery worker over a native journal and the participants of native stores (or
   Python objects with the participant methods), rebuilt for each run on its own
   current-thread runtime; the native facade's `recovery()` returns the same
   `RecoveryReport` as the Python one. A participant's failure is reported and
   retried by a later run; a failing wall clock or journal fails the run.
   `tests/core/test_native_recovery.py` kills a child coordinator (`os._exit`)
   before the decision (rolled back) and after it (rolled forward, idempotently)
   with a PostgreSQL branch left prepared, and sweeps an orphaned prepared gid
   through the facade.
   ✔ Entity parts in PostgreSQL on the core (`rhizodat_postgres::parts`): the
   reference's part tables (same DDL, service columns and tagged JSON for `jsonb`
   fields, so either implementation reads the other's versions), writes buffered in
   `PostgresParts` and applied in order inside the branch's transaction at stage,
   and committed reads (`read_part` as of a time, `part_history`, `part_versions`,
   `expired_parts`). The first piece of repositories on the core.
   Next: parts across the boundary (`rhizodat_native`) and repositories on the
   native facade for PostgreSQL, then parts in the other stores, reads with Arrow
   for full results, and the C ABI for Go.
6. **CI**: `cargo test`, `cargo clippy`, `cargo fmt --check`, and the conformance
   suite against both implementations.

Order: after step 4 of the v0.1 roadmap (or at least after the Iceberg driver
check), so the port carries a finished protocol, including the non-2PC
branches, rather than a moving target.

## Production deployment

Every store is reached over the network, so clustered stores work unchanged; the
guarantees then depend on how each store fails over.

- **Journal**: the `COMMITTED` record is the atomic commit point. Its store must
  replicate **synchronously** (PostgreSQL `synchronous_commit = on` with a
  synchronous standby, or a quorum, e.g. Patroni). With asynchronous replication
  a failover can lose a decision, and recovery would presume abort for a
  transaction that is partly committed.
- **PostgreSQL**: two-phase commit goes to the primary. Prepared transactions are
  in the WAL and survive failover to a synchronous standby; with an asynchronous
  one a prepared branch can be lost, which the `dat_applied` check reports as a
  heuristic outcome.
- **ClickHouse**: `ReplicatedMergeTree` for deduplication tokens; the
  "already applied" check reads with sequential consistency or after quorum
  inserts.
- **drevo**: the server over Bolt, not the embedded mode. A prepared transaction
  survives a restart (restored from the WAL); its behaviour on failover to a
  replica is still to be confirmed with drevo.
- **Iceberg**: a highly available catalog; commit conflicts after the decision
  are retried.
- **Latency**: a commit over several stores costs four journal writes plus a
  prepare and a commit per two-phase store. Keep the journal close to the
  coordinators. While a drevo transaction is prepared, all other writes to that
  graph wait, so graph write throughput is bounded by the prepare→commit window.
- **Clocks**: NTP on every host; the HLC absorbs the remaining skew and refuses
  timestamps beyond `max_offset_ms`.
- **Operations**: per-call timeouts, connection pools, metrics and alerts for
  stuck transactions and fence duration, and recovery as its own process.
  Deployed with container images and a Helm chart; managed databases are fine.

## Server mode (after v0.1)

An optional `rhizodat-server` on top of the same core: clients in any language,
one place for coordination, recovery and connection pools, and no single-process
limit of embedded drevo. It adds a network hop and must run as several
instances (the journal's compare-and-set already makes concurrent coordinators
safe). The open design question is the transaction model over the network:

- **sessions** (`begin` / `execute` / `commit`): flexible, but an idle client
  holds locks and the drevo fence until a timeout;
- **one request per Unit of Work**: safe, but no application logic between
  statements.

The library stays the core; the server is built on it, not instead of it.
