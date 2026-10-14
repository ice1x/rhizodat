# Writing a datorium store adapter

A store joins datorium through an adapter: a small package that declares the
store kind and its connection strings, and implements the interfaces the
commit protocol calls. datorium's own adapters (PostgreSQL, ClickHouse, drevo,
Iceberg) are built exactly like a third-party one. This guide covers what an
adapter must do, how to check it, and which databases can or cannot be one.

Contents:
1. [Can the store join at all?](#1-can-the-store-join-at-all)
2. [The declaration](#2-the-declaration)
3. [The interfaces](#3-the-interfaces)
4. [Optional capabilities](#4-optional-capabilities)
5. [Errors](#5-errors)
6. [Entity parts](#6-entity-parts)
7. [Checking an adapter](#7-checking-an-adapter)
8. [Databases: supported, candidates, not viable](#8-databases-supported-candidates-not-viable)

## 1. Can the store join at all?

datorium commits a meta-transaction over several stores with a journal and
the decision recorded in it (README: *The commit protocol*). A store takes part
in one of two ways, and needs **one** of them.

**Two-phase (before the decision).** The store's own transactions can be
*prepared*:
- a prepared transaction survives the client disconnecting and the server
  restarting;
- it holds its locks and can later be committed or rolled back by a global id
  (`dat:<dat_txn>:<store>`) from any connection;
- the store lists the prepared transactions it holds, so recovery can find
  orphans.

PostgreSQL (`PREPARE TRANSACTION`) and drevo (`drevo.tx.prepare`) work this way.

**One-phase (after the decision).** The store has no prepared state, but a
branch's writes can be applied **after** the decision, **idempotently**, by
anyone (the coordinator, or recovery on another host):
- what to apply survives a crash before the decision: it travels in the
  journal payload (ClickHouse rows) or is staged durably and invisibly in the
  store (Iceberg data files no snapshot references);
- applying twice has the effect of applying once (deduplication tokens, a
  count by `dat_txn`, a snapshot tagged with `dat_txn`);
- whether it was applied can be decided reliably afterwards: an *applied
  marker* written atomically with the data, or a check that is just as exact.

ClickHouse and Iceberg work this way. Their writes become visible after the
decision, so readers may briefly see the other stores' share first. The
Repository hides this: it reads the newest version every part already has.

**A store that offers neither cannot be an adapter.** If writes can be neither
prepared nor re-applied idempotently with a reliable "already applied?" check,
a crash between the decision and the write leaves datorium unable to finish
the transaction or to know it is finished. See [section 8](#8-databases-supported-candidates-not-viable).

## 2. The declaration

```python
from datorium import Adapter, Scheme

ADAPTER = Adapter(
    kind="mysql",                                   # [a-z][a-z0-9_]{0,31}
    schemes=(Scheme("mysql", default_port=3306, host="required"),),
    open_store=lambda name, dsn: MySqlStore(dsn, name=name),
    open_journal=None,          # only for a shared ACID store that can hold the journal
    journal_location=None,      # where a journal at a DSN lives (for Fenced stores)
    query_help="SQL (MySQL 8 dialect), parameters as %s",   # shown by datorium-MCP
)
```

Register it with an entry point, and datorium loads it on first use:

```toml
[project.entry-points."datorium.adapters"]
mysql = "datorium_mysql:ADAPTER"
```

Or call `datorium.register_adapter(ADAPTER)` yourself. A `Scheme` says whether
the DSN needs a host or a path (`"required"`, `"optional"`, `"none"`) and the
default port. Parsing a DSN imports no driver. Import yours lazily, so that
installing the adapter does not force the driver.

## 3. The interfaces

Three objects, as in `datorium.core.coordinator` and
`datorium.core.unit_of_work`:

```python
class Store:                      # one configured store; lives as long as Datorium
    name: str
    two_phase: bool
    def branch(self, dat_txn) -> Branch: ...      # one per Unit of Work touching the store
    def participant(self) -> Participant: ...     # the same object every time is fine
    def execute(self, query, params=None) -> Result: ...   # native access, no transaction
    def close(self) -> None: ...

class Branch:                     # the store's share of one meta-transaction
    store: str
    two_phase: bool
    def stage(self, dat_txn, dat_tx_time) -> bytes | None: ...
    def prepare(self, gid) -> None: ...           # two-phase only
    def commit_one_phase(self) -> None: ...       # when this is the only branch
    def rollback(self) -> None: ...               # before the decision
    def is_retryable(self, error) -> bool: ...

class Participant:                # the store after the decision, and for recovery
    def finish(self, branch_record, dat_txn, dat_tx_time) -> None: ...
    def is_applied(self, branch_record, dat_txn) -> bool: ...
    def abort(self, branch_record) -> None: ...
    def prepared_gids(self) -> list[str]: ...
    def is_retryable(self, error) -> bool: ...
```

What each must keep:

| Method | Promise |
|---|---|
| `stage` | Slow, lock-free work, done before anything is prepared. `dat_tx_time` (HLC) is known only here: fill it into rows. Write the applied marker (two-phase: in the open transaction). Return the payload the journal must carry for a one-phase store (rows, staged file paths), else `None`. |
| `prepare` | Two-phase only: make the open transaction durable under `gid`. Called last, after every branch is staged. |
| `commit_one_phase` | The only branch of a meta-transaction: commit natively, no journal. |
| `rollback` | Before the decision: discard everything, an open or a prepared transaction alike. Never raises for "nothing to roll back". |
| `finish` | After the decision: make the branch visible (`COMMIT PREPARED`, apply the payload, commit the staged files). **Idempotent**; an unknown gid means "already resolved". Recovery may call it on another host, from the journal record alone. |
| `is_applied` | Whether the branch is visible, to settle an ambiguous commit. Never guess: without evidence, answer `False` (datorium then finishes again, which must be harmless). |
| `abort` | Roll back a prepared gid, or drop staged data. Idempotent. Only ever called before a commit decision. |
| `prepared_gids` | The prepared transactions the store holds: datorium's (`dat:` prefix) and others, which it leaves alone. A one-phase store returns `[]`. |
| `is_retryable` | `True` only for errors where re-running the whole Unit of Work may succeed: serialization failures, deadlocks, a lost connection before prepare, a prepared fence. Never for a bug, a constraint violation or a full store. |

Two rules that are easy to get wrong:
- **Nothing is visible before the decision.** Not after `stage`, not after
  `prepare`. A one-phase store applies only in `finish` /
  `commit_one_phase`.
- **The journal record is all recovery has.** `finish` and `abort` run from a
  `BranchRecord` (store, gid, two-phase flag, payload) in a process that never
  saw the branch. Anything they need must be in the payload or findable in the
  store.

## 4. Optional capabilities

Checked with `isinstance`. Implement the ones that apply.

| Capability | Where | What for |
|---|---|---|
| `read_only(query, params)` (`ReadOnlyQueries`) | store | Native reads the store itself refuses to turn into writes. datorium-MCP uses them. |
| `applied_location` (`AppliedLocation`) | store | Where the applied markers live. `Datorium` refuses two stores sharing one: they would block or confuse each other. |
| `fence_location` (`Fenced`) | store | While a branch is prepared, nothing else commits there (drevo's fence), so the journal must live elsewhere. |
| `classify(error) -> ErrorKind` (`Classifies`) | branch, participant | Error kinds beyond retryable / not (section 5). |
| `free_space() -> int \| None` (`Capacity`) | store | Bytes left, for the `min_free` check before the decision. `None` when the store cannot tell. |
| `PartStore`, `PartBranch` | store, branch | Entity parts: the store can hold a share of an entity's fields (section 6). |

## 5. Errors

`classify(error)` returns one of:
- `TRANSIENT`: re-run the Unit of Work;
- `STORE_FULL`: no room: disk, quota;
- `CONSTRAINT`: the data breaks a rule of the store: a unique key, a foreign
  key, a check;
- `PERMANENT`: anything else.

`is_retryable` stays and must agree: true exactly for `TRANSIENT`. Without
`classify`, datorium classifies by `is_retryable`, and an error caused by a
device out of space (`ENOSPC`, `EDQUOT` anywhere in the cause chain) is
`STORE_FULL` whoever raised it.

Recognise the driver's own "full" error. For example, PostgreSQL's `53100`
and ClickHouse's code 243 make the meta-transaction fail with `StoreFull`
(naming the store) instead of a generic abort. If the store applies after the
decision, also implement `Capacity` where you can. It is the only way such a
store can refuse a meta-transaction before the decision.

## 6. Entity parts

To hold entity fields (the Repository), implement
`datorium.core.parts.PartStore` on the store and `PartBranch` on its branches:
- `install_part`;
- `read_part(known_at)`;
- `part_history`;
- `expired_parts`;
- `write_part` / `erase_part` / `read_part` in a branch (its own writes first).

A store keeps every version of a part, tagged with `dat_txn` and
`dat_tx_time`. A soft delete is one more version. A hard delete (`erase_part`)
removes every version, and for a one-phase store only the versions older than
its own `dat_tx_time`, so a late erase never removes a later write. A branch
keeps one version per part per Unit of Work.

The README's *How parts are stored* table shows the four layouts.

## 7. Checking an adapter

1. **Dry run:** `datorium adapter check <module:ADAPTER> [--dsn DSN] [--json]`.
   It reports the declaration, the interfaces and every optional capability,
   with a completeness score, and writes nothing.
2. **The adapter contract:** subclass
   `datorium.testing.contract.AdapterContract` with a `Harness`. The harness
   needs three things:
   - `open_store(name)`: a store in a place of its own;
   - `write(branch, key)`;
   - `count(store, key)`;
   - and, optionally, `store_full_error()`.

   The contract checks visibility, idempotent `finish`, `abort`, error
   classification, and a crash at every step of the commit protocol followed
   by one recovery run.
3. **The parts contract** (entity parts):
   `datorium.testing.parts.PartStoreContract` with a `parts_store` fixture.

```python
from datorium.testing.contract import AdapterContract

class TestMySqlContract(AdapterContract):
    @pytest.fixture
    def harness(self):
        h = MySqlHarness(dsn)
        yield h
        h.cleanup()
```

Run them against a real server; a contract that passes on a mock proves little.

## 8. Databases: supported, candidates, not viable

**Supported** (shipped, contract-tested):

| Store | Joins as | How |
|---|---|---|
| PostgreSQL | two-phase | `PREPARE TRANSACTION` / `COMMIT PREPARED`; `dat_applied` table; needs `max_prepared_transactions > 0` |
| drevo | two-phase | `drevo.tx.prepare` / `commitPrepared`, embedded or over Bolt; `DatApplied` nodes; prepared fence |
| ClickHouse | one-phase | rows in the journal payload; deduplicated insert after the decision; `dat_applied` marker |
| Apache Iceberg | one-phase | Parquet files staged before the decision; one snapshot per table tagged `dat.txn` after it |

**Candidates.** This table is *analysis* from the stores' documented features,
not tested. Every row must be verified against a real server before relying on
it.

| Store | Likely fit | Basis | To verify |
|---|---|---|---|
| MySQL 8 / MariaDB ≥ 10.5 | two-phase | `XA START … XA PREPARE … XA COMMIT 'gid'`, `XA RECOVER` lists prepared; XA prepared state survives disconnect and restart in recent versions | durability across restart and binlog, behaviour under replication |
| Oracle Database | two-phase | XA via `DBMS_XA`, pending transactions in `DBA_PENDING_TRANSACTIONS` | licensing; driver support from Python |
| SQL Server | two-phase, maybe | distributed transactions are MSDTC-centred; XA through JDBC | whether prepare / recover is reachable without MSDTC |
| CockroachDB | one-phase | no prepared transactions; but serializable transactions can write data and an applied marker atomically | idempotent apply via the marker in the same transaction |
| YugabyteDB (YSQL) | one-phase | PostgreSQL-compatible, but `PREPARE TRANSACTION` support is doubtful | whether two-phase works at all; else as CockroachDB |
| MongoDB ≥ 4.0 | one-phase | multi-document transactions (no prepare exposed to clients); marker document in the same transaction | write concern `majority` for the marker |
| Neo4j | one-phase | no client-visible prepare; marker node in the same transaction | — |
| SQLite, DuckDB | one-phase | local transactions with a marker table; single writer | file locking under concurrent processes |
| Redis | one-phase, limited | `MULTI`/`EXEC` or a Lua script writing data plus a marker atomically; durability depends on AOF `fsync` | durability settings; no rollback inside `EXEC` |
| Cassandra / ScyllaDB | one-phase, limited | idempotent upserts by key; a logged batch can include a marker, atomic but not isolated | multi-partition batch guarantees; cost |
| Elasticsearch / OpenSearch | one-phase, limited | idempotent indexing by deterministic document id; no atomic marker, so `is_applied` checks the documents themselves | refresh visibility for `is_applied` |
| Kafka | one-phase, limited | idempotent / transactional producer; `is_applied` would need to read back by `dat_txn` | read-back cost; retention |
| Plain object storage (S3, GCS) | one-phase | objects staged under a key, made visible by a final idempotent write (a manifest), as Iceberg does | list-after-write consistency of the provider |

**Not viable**, by the criteria in [section 1](#1-can-the-store-join-at-all):
- Stores with no durable writes. **Memcached**, and Redis with persistence
  off: a committed write can vanish, so "applied" cannot be known.
- Fire-and-forget sinks with no read-back and no deduplication: StatsD/UDP
  metrics, plain syslog. A write can be neither checked nor safely repeated.
- APIs whose writes are neither idempotent nor checkable afterwards. Many
  third-party SaaS endpoints without idempotency keys: a retry after a lost
  response may write twice.

A store in the last two groups can still be fed *from* datorium, for example
by a consumer that reads committed data. It just cannot be a participant in a
meta-transaction.
