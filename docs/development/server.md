# Server mode: the transaction model (proposal)

Status: **proposal, for the owner's decision**. Nothing here is built yet. It
settles the open question in the [plan](../plan.md#server-mode-after-v01):
how a client's transaction runs over the network.

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

## If B is chosen: first slice

1. Preconditions in the core and both facades: `update` / `delete` with an
   expected `dat_tx_time`, a retryable conflict when it differs (TDD, in-process
   first; Python reference, then Rust).
2. A Unit of Work as data: a serialisable list of operations, in the C ABI's
   tagged-JSON value format (already the journal's row format), so a request is
   one document. Conformance vectors for it.
3. `rhizodat-server` in Rust: one endpoint that runs such a document through the
   coordinator, recovery as a background task, health and metrics. Transport
   (HTTP/JSON or gRPC) to be decided with the first client.
4. A client in Python and Go that builds the document from repositories and
   retries on conflict.

## Decisions for the owner

1. The model: **B** (recommended), A, or both from the start.
2. Preconditions on by default for `update` / `delete` over the server, or
   opt-in.
3. Transport: HTTP/JSON (simplest, curl-able) or gRPC (streams, typed clients).
