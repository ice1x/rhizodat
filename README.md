# datorium

## A bitemporal polyglot data layer

datorium is a data access layer over several databases that behaves like **one
transactional store**:

- **PostgreSQL**: OLTP, SQL
- **ClickHouse**: OLAP, SQL
- **[drevo](https://github.com/ice1x/drevo)**: graph (Cypher), full-text search, vectors
- **Apache Iceberg**: lakehouse tables

Application code works with entities through a **Repository** and groups writes
in a **Unit of Work**. One Unit of Work is one **meta-transaction** that spans
every store it touches and gives ACID guarantees at the layer level. Every
record carries transaction time (and optionally valid time), so any entity can
be read **as of** any moment.

The Python implementation is the executable specification. The critical core
(commit protocol, journal, recovery, clock, identity) is to be implemented once
in Rust and shared by every language through bindings (see
[Plan](#plan)). Everything below is described in language-neutral terms on
purpose.

> Status: v0.1 in progress. Implemented: identity, HLC, DSNs, entity mapping,
> results, the journal, the commit coordinator and recovery, PostgreSQL, drevo,
> ClickHouse and Iceberg adapters, journals in PostgreSQL and drevo, Units of
> Work with retries. Not yet: repository persistence of entities, bitemporal
> reads, migrations. This README is
> the specification v0.1 is built against.

---

## Principles

1. **No store is primary.** An entity may live in a single store or be spread
   across all four. Identity, transactions, time and migrations never assume a
   master database.
2. **Native query languages stay.** SQL, Cypher and Iceberg scans are not
   unified into a DSL; the layer unifies identity, transactions, time and
   result access.
3. **ACID belongs to the layer**, not only to the stores, and is stated
   honestly where a store limits it (see [Guarantees](#guarantees)).
4. **Portable by design.** No Python-only mechanisms in the core contracts
   (identity, journal, protocol, migration format).

## Non-goals

- A universal query DSL
- Cross-store joins inside the database engines
- Schema unification across stores

---

## Stores and capabilities

| Store | Query | Native ACID | 2PC participant | Branch commit in a meta-transaction |
|---|---|---|---|---|
| PostgreSQL | SQL | ✔ | ✔ `PREPARE TRANSACTION` | prepare → `COMMIT PREPARED` |
| drevo | Cypher, FTS, vector | ✔ MVCC + WAL | ✔ `prepare` / `commit_prepared` | prepare → `commit_prepared` |
| Apache Iceberg | table scan | ✔ atomic snapshot commit | ✖ optimistic commit | stage data files → commit snapshot after the decision, retried |
| ClickHouse | SQL | ✖ | ✖ | insert after the decision with a dedup token, retried |

Iceberg's atomicity comes from the format plus its catalog: a commit swaps the
table's metadata pointer to a new snapshot. drevo's 2PC is exposed in
`drevo-py` (`prepare(gid)`, `commit_prepared`, `rollback_prepared`,
`list_prepared`) and in `drevo.dbapi` (PEP 249 TPC).


drevo takes part in a meta-transaction in two ways, with the same semantics:
embedded (`drevo:///path/graph.drevo`; `drevo+mem://` for tests only, it is drevo's
in-memory test oracle) through drevo-py, or remote
(`drevo+bolt://`) through any Neo4j driver, where 2PC is the `drevo.tx.prepare`,
`commitPrepared`, `rollbackPrepared` and `listPrepared` procedures. Node
`title`s are unique across a drevo graph, so map entity fields that are not
unique to another property name.

ClickHouse has no transactions, so its branch only buffers rows
(`uow.store("ch").insert(table, rows)`). Stage checks every batch against the
table schema and `max_rows_per_table` (default 100 000), adds `dat_txn` and
`dat_tx_time` to each row and puts the batch into the journal; the rows are
inserted after the decision, by the coordinator or by recovery. Tables written
through datorium need `dat_txn UUID` and `dat_tx_time Int64` columns, ideally
with a `bloom_filter` skip index on `dat_txn`; batches are JSON in the journal,
with tagged UUID, Decimal, bytes, date and datetime values (a time zone becomes
its UTC offset).

An Iceberg branch buffers appends (`uow.store("lake").append(table, rows)`,
mappings or a `pyarrow.Table`). Stage converts them to the table's Arrow schema,
adds `dat_txn` and `dat_tx_time`, and writes one Parquet data file per table
under the table's `data/` location; the journal carries the file paths, not the
rows. After the decision each table gets one snapshot adding the files
(`add_files`), tagged `dat.txn` / `dat.tx_time` in its summary. Tables need
`dat_txn` (`uuid` or `string`) and `dat_tx_time` (`long`) columns. Only appends
for now; catalogs: `iceberg+rest://` and `iceberg+sql://` (PostgreSQL or
SQLite), Glue later. A branch over several tables commits one snapshot per
table: atomic with the rest of the meta-transaction by roll-forward, not by a
multi-table Iceberg commit.

---

## Architecture

```
 Application (entities, domain logic)
        │  repo.get / add / remove / find / history
        ▼
 ┌──────────────────────────────────────────────┐
 │ Repository[T]        entity ↔ store mapping  │
 │ Unit of Work         one meta-transaction    │
 │ Coordinator          2PC + journal + recovery│
 │ Clock                hybrid logical clock    │
 └──────────────────────────────────────────────┘
        │  branches (one per touched store)
        ▼
 PostgreSQL │ drevo │ Iceberg │ ClickHouse      ← adapters
```

---

## Identity

Every record written by datorium carries service fields:

| Field | Type | Meaning |
|---|---|---|
| `dat_id` | UUIDv7 | Entity identity. Generated by the layer, never by a store. Every part of one entity has the same `dat_id` in every store. |
| `dat_type` | string | Entity type, for stores that hold several types in one table or graph. |
| `dat_txn` | UUIDv7 | The meta-transaction that wrote this version. Used for idempotent apply and for filtering uncommitted data. |
| `dat_tx_time` | HLC timestamp | Transaction time of this version (see [Time](#time)). |

UUIDv7 is time-ordered, which suits `ORDER BY` in ClickHouse, sorting in
Iceberg and B-tree locality in PostgreSQL.

UUIDv7 follows RFC 9562 with the 12-bit field used as a counter (section 6.2,
method 1): ids from one generator are strictly increasing, also within one
millisecond and when the wall clock steps back.

Storage per store: PostgreSQL `uuid`, ClickHouse `UUID`, drevo node property,
Iceberg `uuid` (falls back to `fixed[16]` where the engine lacks it).

**Links between entities** are references by `dat_id`. Foreign keys cannot
cross stores, so the Unit of Work checks referential integrity before commit.

### Entity mapping

Which part of an entity lives in which store is declared per entity type; there
is no central registry to look up.

```python
from datorium import entity, Ref

@entity(stores={
    "pg":         ["status", "amount", "customer"],
    "drevo":      ["related"],          # links as graph edges
    "clickhouse": ["events"],           # append-only facts
    "iceberg":    ["snapshots"],        # long-term history
})
class Order:
    status: str
    amount: Decimal
    customer: Ref["Customer"]
    related: list[Ref["Order"]]
    events: list[str]
    snapshots: list[bytes]
```

The narrow case is one store; the wide case is all four. Both use the same API.

- Every declared field maps to exactly one store, and every mapped name is a
  declared field. `ClassVar`s are not fields.
- `dat_*` names are reserved; so are `valid_from` / `valid_to` when valid time
  is on (`@entity(..., valid_time=True)`).
- The type name stored as `dat_type` defaults to the class name; set
  `name="tracker.Issue"` to keep it stable across renames and language ports.
- `@entity` makes the class a keyword-only dataclass with a `dat_id` field (a
  new UUIDv7 unless given).
- A reference is `Ref[T]`, a value holding the target's `dat_id`, also inside
  containers or `| None`. One field refers to one target type; a union of
  several is rejected in v0.1.

---

## Meta-transactions

A Unit of Work is one **meta-transaction** (`dat_txn`) with one **branch** per
store it touches. It is flat: one global transaction, branches underneath, no
nesting.

- **One branch** → the store's own transaction, one-phase commit. No journal.
- **Several branches** → the commit protocol below, coordinated through the journal.

Writes never have to cover a whole entity: updating only the ClickHouse part of
an `Order` is a meta-transaction with a single branch.

### Commit protocol (several branches)

1. **Begin.** Generate `dat_txn`.
2. **Work.** Run statements in all branches, in parallel: PostgreSQL and drevo
   in open transactions, ClickHouse rows buffered, Iceberg data files written
   (invisible until a snapshot references them).
3. **Stage** (slow, holds no locks). Assign `dat_tx_time` from the HLC. Iceberg:
   finish data files and pre-check conflicts. ClickHouse: prepare the payload.
   Failures here are the cheapest to abort, so everything likely to fail is
   checked here. Then journal record `ACTIVE` with every branch and its gid,
   plus staged payloads (e.g. the ClickHouse batch).
4. **Prepare** (fast, holds locks). PostgreSQL `PREPARE TRANSACTION` and drevo
   `prepare(gid)`, **last**. Journal record `PREPARED` with `dat_tx_time`. Any
   failure up to here → journal `ABORTED`, roll everything back.
5. **Decide.** Journal `PREPARED → COMMITTED` (compare-and-set). **This is the
   atomic commit point.**
6. **Finish.** First release locks: `COMMIT PREPARED` / `commit_prepared`. Then
   the Iceberg snapshot commit and the ClickHouse insert with
   `insert_deduplication_token = dat_txn:branch`. Every step is idempotent.
7. **Done.** Journal record `DONE`.

The journal record is written once the branch list is known (after staging),
not at begin: before it exists nothing is prepared, so a crash leaves only open
transactions that die with their connections and unreferenced Iceberg files.
A branch's gid is `dat:<dat_txn>:<store>`; recovery recognises its own
prepared transactions by that prefix.

**Ordering rule: slow, lock-free work before prepare; lock-holding branches
prepare last and commit first.** The window between prepare and commit must be
as short as possible:

- A prepared PostgreSQL transaction holds its row locks until `COMMIT PREPARED`.
- A prepared drevo transaction raises the graph's **prepared fence**: while any
  prepared transaction exists, every other commit to that graph fails fast with
  a retryable `TransactionConflict` (reads keep working). See drevo
  `docs/rfc-two-phase-commit.md`.
- drevo's opt-in `DREVO_PREPARED_TX_TIMEOUT_SECS` heuristically rolls back a
  transaction that stays prepared too long, overriding the coordinator. The
  coordinator must finish well inside that timeout, and treats a refused late
  commit as a heuristic outcome.

Order within step 6 does not affect correctness (the decision is already
durable); it only shortens how long other writers are blocked.

### Journal

- Required for meta-transactions with several branches: `journal=<dsn>`
  pointing to an ACID store (PostgreSQL or drevo). Missing → a clear error, not
  a silent unsafe default.
- Must be shared and durable: recovery may run on another host than the one
  that crashed.
- A local file (`journal="file:///…"`) is allowed only explicitly, for
  development and single-process embedded use.
- A drevo journal keeps one node per meta-transaction (label `DatJournal` by
  default, `title` = `<label>:<dat_txn>` for uniqueness); each compare-and-set
  is one drevo transaction, and drevo rejects the second of two concurrent
  writers. It must be a **separate graph** from every drevo store of the
  deployment: a prepared branch raises its graph's fence, which would block the
  coordinator's own `PREPARED` / `COMMITTED` writes. `Datorium` refuses such a
  configuration.
- The journal is a coordination role, not a data master: it holds transaction
  states, not entity data.

States and the only legal transitions:

```
ACTIVE ──► PREPARED ──► COMMITTED ──► DONE
   │           │
   └───────────┴──► ABORTED
```

- Every change is a **compare-and-set** from the expected state. A coordinator
  writing `COMMITTED` and a recovery worker writing `ABORTED` for the same
  `PREPARED` record cannot both succeed; the loser gets `JournalConflict`.
- `PREPARED` carries the `dat_tx_time` assigned at stage. `ABORTED` and `DONE`
  are final and may be compacted away.
- Table layout, the same for every SQL journal:

| Column | Type | |
|---|---|---|
| `dat_txn` | UUID | primary key |
| `state` | text | one of the states above |
| `dat_tx_time` | bigint | HLC, set at `PREPARED` |
| `branches` | text | JSON list of `{store, gid, two_phase, payload}` (payload base64, e.g. a staged ClickHouse batch) |
| `created_ms`, `updated_ms` | bigint | coordinator wall clock, used only to find stale records |

### Recovery

A recovery worker (any host, run periodically) first sweeps the prepared
transactions each store holds (`list_prepared` / `pg_prepared_xacts`): a
`dat:` gid whose journal record is missing or `ABORTED` is rolled back. Then it
handles journal records unchanged for `stale_after_ms` (default 30 s, well above
a normal commit, so it does not race live coordinators; the compare-and-set
settles it if it does):

| Journal state | Action |
|---|---|
| `ACTIVE` or `PREPARED` (no decision), stale | write `ABORTED` (compare-and-set), then roll back prepared branches (presumed abort) |
| `COMMITTED` (not `DONE`) | roll forward every branch, idempotently |
| `DONE` or `ABORTED` | nothing; eligible for journal compaction |

A decided transaction still unfinished after `alert_after_ms` (default 10 min)
is reported as stuck; it is never dropped.

### Retries

Retries follow one line: **before the decision a meta-transaction may be
aborted and re-run; after the decision it may only be finished.**

**Before the decision (steps 1–4): retry the whole Unit of Work.**

- A transient failure in any branch aborts every branch, and the Unit of Work
  is re-run from the start with a **new** `dat_txn` and a new `dat_tx_time`.
  Retrying a single statement is not enough: reads made earlier in the work may
  be stale.
- So a retryable Unit of Work is a **function**, not a `with` block (see the
  [API sketch](#api-sketch-v01)). A `with` block runs once and fails on the
  first transient error.
- Policy: maximum attempts, overall deadline, exponential backoff with **full
  jitter**. Jitter matters: drevo's prepared fence makes concurrent writers
  fail fast at the same moment, and without jitter they retry in lockstep.

| Store | Retryable (transient) | Not retryable |
|---|---|---|
| PostgreSQL | `40001` serialization failure, `40P01` deadlock, connection loss before prepare | constraint violations, syntax / schema errors |
| drevo | `TransactionConflict` (commit conflict or prepared fence), Bolt `TransientError`, HTTP `503` | constraint violations, Cypher errors |
| Iceberg | commit conflict found by the pre-check, catalog unavailable | schema mismatch, validation errors |
| ClickHouse | network errors, timeouts, too many parts | type / schema errors |
| datorium | journal unreachable before the decision (→ presumed abort) | invariant / reference validation failures |

**After the decision (steps 5–7): retry each branch until it is done, never abort.**

- The coordinator retries in-process up to a short deadline, then hands the
  transaction to the recovery worker, which keeps retrying with backoff.
- `commit()` then returns `CommittedPending(dat_txn)`, **not an error**. The
  transaction is committed; raising would make callers re-run it and write it
  twice.
- If the `COMMITTED` write cannot be confirmed and the journal cannot be read
  back, `commit()` raises `InDoubt(dat_txn)`: the outcome is unknown, the
  Unit of Work must **not** be re-run, and recovery decides from the journal.
- A transaction that stays unfinished past a threshold raises an alert; it is
  never silently dropped.

**Ambiguous outcomes.** A timeout on a commit call does not say whether it
applied. The coordinator never guesses; it checks the store before retrying:

Every two-phase branch writes an **applied marker** inside its own transaction
at stage: a row `(dat_txn, dat_tx_time)` in the store's `dat_applied` table
(PostgreSQL) or the equivalent in drevo. "Did this branch commit?" is then one
key lookup, whatever tables the application wrote, and works for one-phase
commits too.

| Store | How "already applied" is detected |
|---|---|
| PostgreSQL | gid absent from `pg_prepared_xacts` and a `dat_applied` marker row for `dat_txn` exists → done; gid absent and no marker → heuristic outcome, reported, never "done" |
| drevo | `commit_prepared` on an unknown gid returns `UnknownGid` → already resolved |
| Iceberg | a snapshot whose summary carries `dat.txn` exists, or the current snapshot already references the staged files (the tagged snapshot may have been expired) → skip. Staged files are deleted on abort, never while a snapshot may reference them; after an ambiguous one-phase commit they are kept for orphan-file maintenance |
| ClickHouse | an applied marker `(dat_txn, dat_tx_time)` in `dat_applied` (ReplacingMergeTree), written after every batch. Before a batch the participant counts the table's rows with this `dat_txn`: none → insert, all → skip, some (a crash left part of an insert) → lightweight `DELETE` of them and insert again. Each insert also carries `insert_deduplication_token = <gid>:<n>` for replicated tables; correctness does not depend on the dedup window |
| Journal | a failed or timed-out `COMMITTED` write is read back; if the state cannot be read, the transaction is left to recovery, which decides from the journal once it is reachable |

**One marker table per store.** Two stores of one deployment must not share
where their markers live: the same PostgreSQL server, database and schema (both
branches would insert the same `dat_txn` into one `dat_applied`, and the second
would wait for the first forever), the same drevo graph (the first branch's
prepared fence refuses the second's prepare on every retry), or the same
ClickHouse database (one store's marker would make the other's branch look
applied). `Datorium` refuses such a configuration up front; give each store its
own schema, database or graph (for PostgreSQL also by DSN:
`postgresql://…/erp?schema=orders`). Stores report the place as
`applied_location`.

### Guarantees

| | What datorium guarantees |
|---|---|
| **A** | All branches commit or none do. PostgreSQL and drevo commit atomically via 2PC; Iceberg and ClickHouse are rolled forward after the decision, so the outcome is atomic but may become visible later. |
| **C** | Invariants and cross-store references are validated in the Unit of Work before prepare; each store enforces its own constraints. |
| **I** | Native isolation inside each store. Across stores, readers that go through datorium filter by committed `dat_txn` and `dat_tx_time`, so they do not see half-applied meta-transactions. Direct reads that bypass datorium can. |
| **D** | Durability of each store plus the journal; a decided transaction survives crashes of the application, the coordinator host, or a store (recovery finishes it). |

Known limit (once `overwrite`/`delete` are supported; appends never conflict):
an Iceberg `overwrite`/`delete` may conflict with a concurrent writer after the
decision. Conflicts are pre-checked at prepare; a residual
conflict is retried and, if it cannot be resolved, reported as a heuristic
outcome for that branch.

---

## Time

datorium is **bitemporal**:

- **Transaction time** (always on): when the layer recorded a version.
  `dat_tx_time` is a hybrid logical clock (HLC) timestamp, assigned at prepare.
  Wall clocks of different hosts are never compared directly.
  Stored as one int64, `wall_ms << 16 | logical`, which sorts like the
  timestamp. A received timestamp more than `max_offset_ms` (default 500 ms)
  ahead of the local wall clock is refused rather than adopted.
- **Valid time** (opt-in per entity type): `valid_from` / `valid_to`, when the
  fact holds in the real world.

A write creates a new version; a delete closes the interval. Nothing is
overwritten in place.

```python
repo.get(order_id)                              # current
repo.get(order_id, as_of=t)                     # valid at t
repo.get(order_id, as_of=t, known_at=t2)        # valid at t, as known at t2
repo.history(order_id)                          # every version, every store
```

How each store keeps history: Iceberg snapshots and time travel; drevo facts
with validity windows; ClickHouse append-only versioned rows; PostgreSQL
versioned rows. A retention / compaction policy bounds the growth.

---

## Schema migrations

One migration history for all stores, in a **language-neutral format**:
numbered steps, each targeting one store.

| Store | Step body |
|---|---|
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
format is datorium's own so other language ports can execute it.

---

## API sketch (v0.1)

```python
from datorium import Datorium

dat = Datorium(
    stores={
        "pg":         "postgresql://user:pass@localhost/app",
        "drevo":      "drevo:///data/graph.drevo",
        "clickhouse": "clickhouse://localhost:8123/analytics",
        "iceberg":    "iceberg+rest://localhost:8181/warehouse",
    },
    journal="postgresql://user:pass@localhost/datorium",
)

orders = dat.repository(Order)

@dat.transactional(retry=Retry(attempts=5, deadline=timedelta(seconds=10)))
def place_order(uow, customer: Ref["Customer"]) -> Order:
    order = Order(status="new", amount=Decimal("10.00"), customer=customer)
    uow.repository(Order).add(order)
    return order                          # commit on return; re-run on a transient error

outcome = place_order(customer=ref)       # Committed | CommittedPending; raises Aborted

order = orders.get(outcome.value.dat_id, as_of=yesterday)
```

A plain `with dat.unit_of_work() as uow:` block is also available; it runs once
and does not retry.

Inside a Unit of Work, native statements run in that store's branch of the
meta-transaction; the branch is opened the first time the store is touched:

```python
@dat.transactional
def claim_task(uow, worker: str) -> int:
    tasks = uow.store("pg")
    row = tasks.execute("SELECT id FROM task WHERE claimed_by IS NULL LIMIT 1").one()
    tasks.execute("UPDATE task SET claimed_by = %s WHERE id = %s", (worker, row["id"]))
    uow.store("drevo").execute("MATCH (w:Worker {name: $w}) SET w.busy = true", {"w": worker})
    return row["id"]                      # outcome.value
```

A transient failure in the work itself (e.g. a serialization failure) or before
the decision re-runs the whole function with a new `dat_txn`; a domain error
from the function propagates unchanged.

Native access stays available through adapters:

```python
dat.store("pg").execute("SELECT count(*) FROM orders WHERE status = %(s)s", {"s": "new"})
dat.store("drevo").execute("MATCH (o:Order)-[:related]->(x) RETURN x LIMIT 10")
dat.store("iceberg").execute("analytics.orders", {"row_filter": "amount > 100"}).to_arrow()
```

### Adapter contracts

```python
class Adapter:
    capabilities: set[str]          # {"sql", "graph", "fts", "vector", "table"}
    def connect(self) -> None: ...
    def close(self) -> None: ...

class Branch:                        # one store's part of one Unit of Work
    store: str
    two_phase: bool                  # True for PostgreSQL and drevo
    def stage(self, dat_txn, dat_tx_time) -> bytes | None: ...  # slow, lock-free; payload for the journal
    def prepare(self, gid: str) -> None: ...   # lock-holding; called last
    def commit_one_phase(self) -> None: ...    # the only branch: native commit
    def rollback(self) -> None: ...            # before the decision
    def is_retryable(self, error: Exception) -> bool: ...

class Participant:                   # the store after the decision and for recovery
    def finish(self, branch, dat_txn, dat_tx_time) -> None: ...  # idempotent; unknown gid = done
    def is_applied(self, branch, dat_txn) -> bool: ...           # resolves ambiguous outcomes
    def abort(self, branch) -> None: ...                         # roll back a prepared gid
    def prepared_gids(self) -> list[str]: ...
    def is_retryable(self, error: Exception) -> bool: ...

class Result:                        # single pass over rows (mappings)
    columns: tuple[str, ...] | None
    def __iter__(self): ...
    def first(self): ...             # first row or None; consumes
    def one(self): ...               # exactly one row, else LookupError
    def all(self): ...               # remaining rows
    def to_arrow(self): ...          # backend's Arrow if native, else built (needs pyarrow)
    def close(self): ...             # also a context manager
    raw: object                      # backend-specific data
```

A result follows the store cursor: it is read once, and reading it again
raises `ResultConsumedError`.

### Connection strings

| DSN | Store | Driver | Default port |
|---|---|---|---|
| `postgresql://…` (alias `postgres://`) | PostgreSQL | `psycopg` | 5432 |
| `clickhouse://…` | ClickHouse | `clickhouse-connect` | 8123 |
| `drevo:///path/graph.drevo`, `drevo+mem://` (tests only) | drevo, embedded | `drevo-py` | — |
| `drevo+bolt://host:7687` | drevo, remote | `neo4j` driver | 7687 |
| `iceberg+rest://…`, `iceberg+sql://…`, `iceberg+glue://…` | Iceberg | `pyiceberg` | — |
| `file:///path` | development journal only | — | — |

- PostgreSQL: `schema=<name>` puts a store (its `dat_applied` and
  `search_path`) or the journal in that schema, so stores configured only by
  DSN, such as datorium-MCP's, can share a database. It is removed before the
  DSN reaches the driver; a `schema=` argument as well must name the same
  schema. A `search_path` set through `options=` is not seen by the
  `dat_applied` check: use `schema=`.
- Iceberg: `iceberg+rest://host:8181/<warehouse>` (`secure=true` for HTTPS);
  `iceberg+sql://user:pass@host:5432/db?warehouse=s3://…` keeps the catalog in
  PostgreSQL, `iceberg+sql:///abs/catalog.db?warehouse=file:///…` in SQLite.
  Other query parameters become pyiceberg catalog properties (`s3.endpoint`,
  `credential`, …).
- Parsing never imports a driver. Scheme matching ignores case; user,
  password and path are percent-decoded; a repeated query parameter is an error.
- `drevo:///` and `file:///` take a local absolute path and no host;
  `clickhouse://`, `drevo+bolt://` and `iceberg+rest://` require a host.
- Passwords and query parameters whose name contains `pass`, `secret`, `token`,
  `key` or `credential` are masked in every printed form and in errors; the
  original string is handed only to the driver.

---

## Package structure

```
datorium/
├── core/
│   ├── identity.py      # dat_id, UUIDv7
│   ├── clock.py         # hybrid logical clock
│   ├── dsn.py           # connection strings
│   ├── mapping.py       # @entity, store mapping
│   ├── repository.py
│   ├── unit_of_work.py
│   ├── coordinator.py   # commit protocol, recovery
│   ├── journal.py
│   ├── rows.py          # one-phase payloads: row batches (ClickHouse)
│   ├── staged.py        # one-phase payloads: staged files (Iceberg)
│   └── result.py
├── adapters/
│   ├── postgres.py
│   ├── drevo.py
│   ├── clickhouse.py
│   └── iceberg.py
├── mcp/                 # datorium-MCP: tools, configuration, server
└── migrations/
```

Store drivers are optional extras: `datorium[postgres]`, `datorium[clickhouse]`,
`datorium[drevo]`, `datorium[iceberg]`; `datorium[mcp]` adds the MCP server.
`drevo-py` is not on PyPI yet and is installed from source.

---

## Plan

### v0.1 roadmap (Python, the executable specification)

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
6. Migrations.
7. Integration scenarios: CBT journal, story/book editor, IT task manager, ERP,
   bug tracker.

Out of scope for v0.1: async API, savepoints / nested transactions, cross-store
query planning.

### Docker Compose for development and tests

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

### Rust core

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
   [`conformance/`](conformance/), run against the Python reference.
2. ✔ **Driver check** (2026-10-12, spikes against the compose stores): every
   step the protocol needs works in all four drivers; none stays on Python.

   | Store | Crate | Verdict | Caveats for the port |
   |---|---|---|---|
   | PostgreSQL | `tokio-postgres` 0.7.18 | ready | gids are SQL literals in `PREPARE TRANSACTION` (quoted by hand); `SqlState` gives `42704` / `40001`; connection loss is `is_closed()` without a code |
   | drevo (Bolt) | `neo4rs` 0.9.0-rc.10 | ready with caveats | release candidate; Bolt 4.4; the prepared fence is a `Transient` error; an unknown gid is still recognised by its message only ([ice1x/drevo#590](https://github.com/ice1x/drevo/issues/590)); no default timeout on pool checkout, and result streams hold pooled connections until dropped |
   | ClickHouse | `clickhouse` 0.15.2 | ready with caveats | server error codes are parsed from the `Code: N.` message; journal batches need the formatted insert (`JSONEachRow`), not typed rows |
   | Iceberg | `iceberg` 0.10.1 + REST catalog + `iceberg-storage-opendal` | ready with caveats | 0.x API; snapshot summary properties, staged Parquet writes, fast append, snapshot listing, `FileIO::delete` and uuid columns work; building a `DataFile` from a path is crate-private, so the Rust journal payload carries the `DataFile` JSON (`serialize_data_file_to_json`, column statistics kept); the commit already rejects files referenced by the table; tables written by Rust read back in pyiceberg |

   Embedded drevo will use `drevo-core` directly and is checked with step 4.
3. **`datorium-core` crate** ([`crates/datorium-core`](crates/datorium-core)):
   protocol, journal, recovery, HLC, identity, tested on fake stores like the
   Python core.
   ✔ Identity (UUIDv7), HLC, gids, the journal state machine and DSN parsing,
   passing the same vectors as Python. Writing them found two gaps in the
   reference, now fixed and pinned by new vectors: another spelling of a
   `dat_txn` (upper case, braces, no hyphens) was taken for a datorium gid, and
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
   ✔ [`crates/datorium-postgres`](crates/datorium-postgres) (tokio-postgres):
   the two-phase store, its participant and the PostgreSQL journal, on the
   same tables as the Python adapter. The journal's `branches` text is pinned
   in `conformance/journal.json`, so either implementation recovers the
   other's records. Tested against the compose PostgreSQL, including a
   meta-transaction over two stores, a crash after prepare (presumed abort)
   and a crash after the decision (roll-forward), both recovered by a worker
   with its own connections. Stores must not share a schema (see "One marker
   table per store"); the Python facade refuses it, the Rust crates document it
   until a Rust facade exists.
   ✔ [`crates/datorium-drevo`](crates/datorium-drevo) (neo4rs, Bolt): the
   two-phase graph store, its participant and the drevo journal, on the same
   nodes as the Python adapter; tested against the compose drevo, including
   the prepared fence and a bug tracker over PostgreSQL and drevo with crashes
   after prepare and after the decision. neo4rs retries a failed auto-commit
   statement on transient errors for up to 60 seconds, which would hide the
   fence from datorium's retry policy: writes therefore run in explicit
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
   ✔ [`crates/datorium-clickhouse`](crates/datorium-clickhouse) (`clickhouse`
   crate): the one-phase branch applied after the decision, with the same
   count / delete / reinsert idempotency, deduplication tokens and
   `dat_applied` marker as the Python adapter. Its journal payload is the
   shared row format, now in `datorium_core::rows` and pinned by
   [`conformance/rows.json`](conformance/rows.json): a Python payload is
   applied by the Rust participant and the reverse (checked both ways). Rows
   travel as `JSONEachRow` (columns are known only at run time) with
   `date_time_input_format = best_effort`; since JSON strings are UTF-8, a
   bytes value must be valid UTF-8 there and anything else is refused at
   stage. The ClickHouse crate needs Rust 1.89.
   ✔ [`crates/datorium-iceberg`](crates/datorium-iceberg) (iceberg-rust 0.10,
   REST catalog, S3 through OpenDAL): the one-phase branch committed after the
   decision, with the same tagged snapshots and idempotency checks as the
   Python adapter. A branch buffers Arrow record batches; stage conforms them
   to the table's schema (by name, missing optional columns null, types cast)
   and writes Parquet files. The journal payload is the shared staged-files
   format, now in `datorium_core::staged` and pinned by
   [`conformance/staged_files.json`](conformance/staged_files.json): paths,
   plus the `DataFile` JSON of each file that iceberg-rust needs to commit
   it. A payload with paths only (the Python adapter's) is committed from the
   Parquet footers, and the Python adapter commits a Rust payload by its paths
   (checked both ways). Tested on iceberg-rust's in-memory catalog over a local
   warehouse and on the compose REST catalog, including an order in
   PostgreSQL with its sales in Iceberg, committed together and rolled forward
   by recovery after a crash. Not yet in Rust: partitioned tables (refused at
   stage) and the SQL / Glue catalogs. iceberg-rust 0.10 needs Rust 1.94, now
   the workspace MSRV. Every store now has a Rust adapter.
5. **Bindings**: the `datorium` Python package becomes a PyO3 wrapper over the
   core without changing its public API; a C ABI (cbindgen) for Go. Open
   questions to settle first: Python callbacks (`transactional` functions) into
   an async core and the GIL, and results across the boundary (Arrow).
6. **CI**: `cargo test`, `cargo clippy`, `cargo fmt --check`, and the conformance
   suite against both implementations.

Order: after step 4 of the v0.1 roadmap (or at least after the Iceberg driver
check), so the port carries a finished protocol, including the non-2PC
branches, rather than a moving target.

### Production deployment

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

### Server mode (after v0.1)

An optional `datorium-server` on top of the same core: clients in any language,
one place for coordination, recovery and connection pools, and no single-process
limit of embedded drevo. It adds a network hop and must run as several
instances (the journal's compare-and-set already makes concurrent coordinators
safe). The open design question is the transaction model over the network:

- **sessions** (`begin` / `execute` / `commit`): flexible, but an idle client
  holds locks and the drevo fence until a timeout;
- **one request per Unit of Work**: safe, but no application logic between
  statements.

The library stays the core; the server is built on it, not instead of it.

### datorium-MCP (out of the box)

datorium ships with an MCP server, `datorium-mcp`, so that AI agents and tools
work with the stores through datorium rather than through four drivers:
inspect the configured stores and their mapping, read entities (with `as_of` /
`known_at` once bitemporal reads land), write through Units of Work, and watch
meta-transactions (the journal, pending and stuck transactions, recovery runs).
It is part of the distribution, configured by the same DSNs, and built on the
library like the server: writes go through the commit protocol, never around
it.

✔ First version (Python, `datorium.mcp`, read-only) and opt-in recovery runs.
Tools, all but `run_recovery` marked read-only for clients:

| Tool | What it does |
|---|---|
| `stores` | the configured stores: name, kind, two-phase or not, whether `query` can read it, DSN with secrets masked |
| `query` | a native read (SQL, Cypher, an Iceberg scan) through the store's `read_only` access, at most `limit` rows (capped by `DATORIUM_MCP_MAX_ROWS`) |
| `transaction` | one meta-transaction's journal record: state, commit time, branches, age |
| `pending_transactions` | unfinished meta-transactions older than a cutoff: in flight, or left for recovery |
| `run_recovery` | opt-in, the one tool that writes: one recovery run (orphan prepared gids rolled back, stale undecided meta-transactions aborted, decided ones finished), reported by `dat_txn` per outcome |

`run_recovery` is offered only with `DATORIUM_MCP_RECOVERY=1` and marked to
clients as writing (destructive, idempotent). It is the library's `Recovery`,
so it never takes over a record changed within the last
`DATORIUM_MCP_RECOVERY_STALE_S` seconds (default 30): a live coordinator may
still be deciding it. It needs a journal and DSNs that can write.

The store itself refuses writes in `query`, so a mistaken or injected query
cannot change data:
- PostgreSQL: one prepared statement in a `READ ONLY` transaction whose mode
  is fixed before the statement runs, always rolled back. Without both, the
  tests showed `SET TRANSACTION READ WRITE` or `COMMIT; INSERT …` getting
  through.
- drevo: a transaction that is always rolled back, since drevo has no
  read-only mode.
- ClickHouse: `readonly = 2`, which also refuses lifting it per query.
- Iceberg: a scan.

As defence in depth, the DSNs can still use read-only accounts.

Configuration is the environment, so secrets stay out of command lines:
`DATORIUM_STORE_<NAME>=<dsn>` per store, `DATORIUM_JOURNAL=<dsn>`, and
optionally `DATORIUM_MCP_MAX_ROWS` (default 1000) and the recovery settings
above. An MCP client starts it
over stdio:

```json
{
  "mcpServers": {
    "datorium": {
      "command": "datorium-mcp",
      "env": {
        "DATORIUM_STORE_ERP": "postgresql://reader:…@db:5432/erp",
        "DATORIUM_STORE_LAKE": "iceberg+rest://catalog:8181/warehouse",
        "DATORIUM_JOURNAL": "postgresql://reader:…@db:5432/erp"
      }
    }
  }
}
```

Next: writes through Units of Work (how a tool call maps to one is the open
design question, shared with the server mode's transaction model), and
entity reads once the repository lands.

---

## Development

`compose.yaml` runs the stores the tests need (ports on 127.0.0.1 only, data in
tmpfs, so every `up` starts empty):

```bash
docker compose up -d --wait   # PostgreSQL (2PC enabled) and a disposable drevo
docker compose --profile '*' up -d --wait   # every store, plus toxiproxy
uv sync --group drevo         # optional: embedded drevo-py from the pinned commit (Rust)
uv run pytest
docker compose down
```

The tests read store DSNs from the environment, else from `.env.test`. A DSN from
`.env.test` whose store is not running skips that store's tests with a hint; a
DSN set in the environment is used as is, so a store that is down fails the run
(CI exports `.env.test` for that reason). Point the tests at other stores with
`DATORIUM_TEST_PG_DSN`, `DATORIUM_TEST_DREVO_BOLT`,
`DATORIUM_TEST_CLICKHOUSE_DSN`, `DATORIUM_TEST_ICEBERG_REST_DSN` and
`DATORIUM_TEST_TOXIPROXY`. The drevo server must be
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
`DATORIUM_TEST_*` variable is set):

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```
