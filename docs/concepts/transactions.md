# Meta-transactions

A Unit of Work is one **meta-transaction** (`dat_txn`) with one **branch** per
store it touches. It is flat: one global transaction, branches underneath, no
nesting.

- **One branch** → the store's own transaction, one-phase commit. No journal.
- **Several branches** → the commit protocol below, coordinated through the journal.

Writes never have to cover a whole entity: updating only the ClickHouse part of
an `Order` is a meta-transaction with a single branch.

## Commit protocol (several branches)

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

## Journal

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
  coordinator's own `PREPARED` / `COMMITTED` writes. `Rhizodat` refuses such a
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

## Recovery

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

## Retries

Retries follow one line: **before the decision a meta-transaction may be
aborted and re-run; after the decision it may only be finished.**

**Before the decision (steps 1–4): retry the whole Unit of Work.**

- A transient failure in any branch aborts every branch, and the Unit of Work
  is re-run from the start with a **new** `dat_txn` and a new `dat_tx_time`.
  Retrying a single statement is not enough: reads made earlier in the work may
  be stale.
- So a retryable Unit of Work is a **function**, not a `with` block (see the
  [API sketch](../guide/api.md)). A `with` block runs once and fails on the
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
| rhizodat | journal unreachable before the decision (→ presumed abort) | invariant / reference validation failures |

**Error kinds and a full store.** Adapters classify their errors:
`classify(error) -> ErrorKind` on a branch and a participant, one of
`TRANSIENT` (worth re-running), `STORE_FULL`, `CONSTRAINT`, `PERMANENT`.
`is_retryable` stays, true only for `TRANSIENT`. An adapter without
`classify` is classified by `is_retryable`, and an error caused by a device out
of space (`ENOSPC`, `EDQUOT`) is `STORE_FULL` whoever raised it.

| Store | `STORE_FULL` | `CONSTRAINT` | free space (`Capacity`) |
|---|---|---|---|
| PostgreSQL | `53100` disk full | class `23` (unique, foreign key, check, not null) | not visible from SQL (none) |
| ClickHouse | code 243 `NOT_ENOUGH_SPACE` | code 469 `VIOLATED_CONSTRAINT` | least `free_space` of `system.disks` |
| Iceberg | `ENOSPC` writing a staged file | — | a `file://` warehouse's disk; object storage: none |
| drevo | `ENOSPC` (embedded) | — | an embedded graph file's disk; memory, Bolt: none |

A full store fails the meta-transaction with `StoreFull` (an `Aborted`,
not retryable, naming the store): from a branch before the decision, or from
the work itself inside a `transactional` function. A store written after the
decision (ClickHouse, Iceberg) can only fail before it if asked first:
`Rhizodat(min_free=<bytes>)` makes the coordinator ask every touched store with
`Capacity` before anything is written and refuse with `StoreFull` (and the
reported `free`) when one is below. A store that cannot tell, or whose query
fails, passes. `min_free` is off by default: it costs one query per store and
commit. Once decided, a full post-decision store leaves the transaction
`CommittedPending` until recovery can finish it.

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
applied). `Rhizodat` refuses such a configuration up front; give each store its
own schema, database or graph (for PostgreSQL also by DSN:
`postgresql://…/erp?schema=orders`). Stores report the place as
`applied_location`.

## Guarantees

| | What rhizodat guarantees |
|---|---|
| **A** | All branches commit or none do. PostgreSQL and drevo commit atomically via 2PC; Iceberg and ClickHouse are rolled forward after the decision, so the outcome is atomic but may become visible later. |
| **C** | Invariants and cross-store references are validated in the Unit of Work before prepare; each store enforces its own constraints. |
| **I** | Native isolation inside each store. Across stores, readers that go through rhizodat filter by committed `dat_txn` and `dat_tx_time`, so they do not see half-applied meta-transactions. Direct reads that bypass rhizodat can. |
| **D** | Durability of each store plus the journal; a decided transaction survives crashes of the application, the coordinator host, or a store (recovery finishes it). |

Known limit (once `overwrite`/`delete` are supported; appends never conflict):
an Iceberg `overwrite`/`delete` may conflict with a concurrent writer after the
decision. Conflicts are pre-checked at prepare; a residual
conflict is retried and, if it cannot be resolved, reported as a heuristic
outcome for that branch.
