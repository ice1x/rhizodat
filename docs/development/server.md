# Server mode: the transaction model

Status: **decided 2026-11-26** (model B, preconditions on by default,
HTTP/JSON); being built in the order of the first slice below. It settles the
open question in the [plan](../plan.md#server-mode-after-v01): how a client's
transaction runs over the network.

## What a transaction is today

In both facades, an application writes a transaction as a function of a Unit of
Work, and `dat.transaction(func)` runs it:

1. **Work.** `func(uow)` reads and writes through the branches. PostgreSQL and
   drevo hold an open transaction from the branch's first use: a PostgreSQL
   connection, with the row locks its writes take, and a drevo snapshot.
   ClickHouse rows are buffered in memory; Iceberg data files are written.
2. **Commit.** Stage, prepare, decide, finish (see
   [meta-transactions](../concepts/transactions.md)). The journal's `ACTIVE`
   record is written only at stage, and drevo's prepared fence is raised only
   at prepare, so both belong to the commit, not to the work.
3. **Retry.** A retryable failure (a serialization failure, a drevo conflict or
   prepared fence, a journal conflict) rolls everything back and **calls
   `func` again** with a new Unit of Work, with backoff, until `Retry` gives up.

Point 3 is what the network changes: a server cannot call the client's function
again.

## The two models

### A. Sessions: `begin` / `execute` / `commit`

The server keeps a Unit of Work per session between requests.

- **Holds while the client is idle:** a PostgreSQL connection and its row locks,
  an open drevo transaction, buffered ClickHouse rows and written Iceberg data
  files on the server. It does **not** hold the journal record or drevo's
  prepared fence (the plan said it did; both come at commit, which the server
  runs in one go).
- **Retry:** the client library reruns its function on a retryable error, as
  `dat.transaction` does now. Application logic may sit between statements,
  including reads of the transaction's own writes.
- **Failure modes:** a slow or dead client keeps the locks and the connection
  until a session timeout; the server needs that timeout, a cap on open
  sessions, and cleanup of Iceberg files written by sessions that are never
  committed. A session is pinned to one server instance, so a crashed instance
  loses its open sessions (their work is uncommitted; nothing is in doubt).

### B. One request per Unit of Work

The client sends the whole Unit of Work in one request: entity writes (add,
update, delete per repository) and native statements per store, plus
**preconditions**. The server runs work and commit with no client round trip
and answers with the outcome.

- **Holds while the client is idle:** nothing. Locks live for one request.
- **Retry:** the server retries what it can by itself (a serialization failure
  or a drevo conflict on the request's own statements: rerun the same request).
  When a precondition fails, it answers with the conflict and the client
  library rereads and resends: the read-modify-write loop of `dat.transaction`,
  with the read outside the transaction.
- **Preconditions** make that correct. A write carries the version it was based
  on (the entity's `dat_tx_time` as read), and the server checks it inside the
  transaction before writing; a mismatch is a retryable conflict. Without them,
  two clients updating the same entity from stale reads lose one update. The
  repositories have no such check today (the open transaction is what protects
  a read-modify-write), so this is new work in the core, useful in-process too.
- **Limits:** no logic between statements, no reading the transaction's own
  writes, and a request must fit in memory (large imports stay in-process or
  get their own streaming endpoint).

## Recommendation

**B first, A later only if users need it.**

- B keeps every lock inside one request, so a server instance is stateless
  between requests: any instance can serve any request, the journal's
  compare-and-set already makes concurrent coordinators safe, and a crashed
  instance leaves only what recovery already handles.
- The two retry levels match what goes wrong: contention on a statement is
  retried by the server, a stale read by the client, and both stay bounded by
  `Retry`.
- The rhizodat use cases are mostly entity writes (the scenarios in
  `tests/scenarios/`), which B covers with preconditions; the ones that need
  logic inside the transaction can keep the library in-process, which stays the
  core.
- A can be added on the same core later as an opt-in, with a short session
  timeout (seconds) and per-instance limits.

## First slice

1. Preconditions in the core and both facades (below): `dat_version` on
   entities, `VersionConflict`, the per-store lock (TDD, in-process first;
   Python reference, then Rust and Go).
2. A Unit of Work as data: a serialisable list of operations, in the C ABI's
   tagged-JSON value format (already the journal's row format), so a request is
   one document. Conformance vectors for it.
3. `rhizodat-server` in Rust: one endpoint that runs such a document through the
   coordinator over HTTP/JSON, recovery as a background task, health and
   metrics.
4. A client in Python and Go that builds the document from repositories and
   retries on conflict.

## Preconditions: how a check stays atomic

An entity read carries its **version**, `dat_version`: the `dat_tx_time` (the
HLC value) of the version it was assembled from. `update` of an entity whose
`dat_version` is set checks that the latest committed version is still that
one, else raises `VersionConflict`; `delete(dat_id, expected_version=...)`
does the same for a given version. An entity built by hand (`dat_version` is
`None`) and a `delete` without a version are written unchecked, as today; over
the server, the client library sends the version of what it read. A write stamps the
returned entity with the new `dat_version`, so updating the same object again
works.

Reading the latest version and comparing is not enough: a new version is a new
row (a new node), so two writers that both read version 1 would both pass the
check and both write. Each transactional store therefore serialises the
writers of one entity first, then reads:

- **PostgreSQL:** `pg_advisory_xact_lock` on the part table and the `dat_id`,
  then the read. A second writer waits on the lock until the first one's
  `COMMIT PREPARED` (a transaction-level advisory lock is kept across
  `PREPARE TRANSACTION`: checked on PostgreSQL 16), then its read, a new
  statement under `READ COMMITTED`, sees the new version and fails the check.
- **drevo:** the write sets a property on the entity's anchor node
  `(:DatEntity {dat_id})`. Two writers of one entity then write one record, and
  drevo refuses the second at commit (`Transaction.Outdated`, retryable;
  checked on 0.0.90); the retry reads again and fails the check.
- **ClickHouse and Iceberg** have no such lock: the check reads and compares,
  which catches a stale read but not two writers racing. An entity with a part
  in PostgreSQL or drevo is protected by that part, since every part of a
  version commits in one meta-transaction.

`VersionConflict` is not retried inside `dat.transaction`, whose function
would write the same stale entity again: the caller reads again. Over the
server it is a `409` with the current version, and the client library rereads
and resends.

## Decisions (2026-11-26)

1. The model: **B**, one request per Unit of Work; sessions (A) may come later
   as an opt-in.
2. Preconditions: **on by default** (an entity that was read carries its
   `dat_version`, and `update` / `delete` check it), in-process and over the
   server alike.
3. Transport: **HTTP/JSON**.
