# Repository

`dat.repository(T)` is CRUD for one entity type; `uow.repository(T)` the same
inside a Unit of Work, whose writes commit with the rest of the work and whose
reads see them.

```python
orders = dat.repository(Order)
orders.add(order)                     # fails if the dat_id exists (EntityExists)
orders.update(order)                  # fails without a live version (EntityNotFound)
orders.save(order)                    # add or update
orders.get(order_id)                  # None when missing or deleted
orders.get(order_id, as_of=t)         # the version known at t
orders.get(order_id, include_deleted=True)
orders.history(order_id)              # every version, oldest first
orders.delete(order_id)               # hard: erased everywhere; soft: hidden
orders.restore(order_id)              # soft delete only, within the window
orders.purge_expired()                # erase soft deletes whose window is over
```

A type with a `Vector` field also has `similar`, the nearest entities at a
moment: see [Embeddings](embeddings.md#vector-fields-on-entities).

- **One write, one Unit of Work.** Every write of `dat.repository(T)` is a Unit
  of Work of its own, with retries, and writes a version of **every** part of
  the entity, so the parts of one version share a `dat_txn`. Writing parts in
  separate transactions may come later, after monitoring and benchmarks.
- **Times.** Each instance carries `dat_created_at`, `dat_updated_at` and
  `dat_deleted_at` (UTC, from the HLC), set on reads and ignored by equality.
  The creation time is stored in every version.
- **Soft delete** (opt-in per type): `@entity(..., soft_delete=True)` uses the
  configured window, `Rhizodat(soft_delete_window=timedelta(days=30))` (the
  default); `soft_delete=timedelta(days=7)` gives the type its own, which wins.
  A soft delete adds a version marked deleted; `restore` adds a live one;
  `purge_expired` erases, physically, what left the window.
- **Consistent reads.** Stores finish a meta-transaction one after another, so
  for a moment one part may show a newer version than another. A read then
  reads again at the oldest of the latest times and returns the newest version
  every part already has.

How parts are stored, per store:

| Store | Layout |
|---|---|
| PostgreSQL | one table per type (`tracker.Issue` → `tracker_issue`), one row per version: `dat_id`, `dat_txn`, `dat_tx_time`, `dat_created`, `dat_deleted` and a typed column per field (`jsonb` for lists, dicts and other types); primary key `(dat_id, dat_txn)`. Created on first use. |
| ClickHouse | one `MergeTree` table per type, `ORDER BY (dat_id, dat_tx_time)`, one row per version, the same service columns plus `dat_txn` / `dat_tx_time`; a column per field, `Nullable` of the native type (`String`, `Int64`, `Float64`, `Bool`, `UUID`, `Date32`, `DateTime64(6, 'UTC')`) or tagged JSON in a `String` (`Decimal`, `bytes`, lists, dicts), so values round-trip exactly. Rows are inserted after the decision like any ClickHouse batch; an erase travels in the payload too and deletes (lightweight `DELETE`) only the versions older than its own `dat_tx_time`, so an erase finished late by recovery never removes a later write. |
| drevo | one node per version, labelled after the type (`tracker_issue`), with the service properties and a property per field: `str`, `int`, `float`, `bool` natively, references and UUIDs as strings, anything else as tagged JSON; `None` is no property. drevo keeps node `title`s unique graph-wide, so a field named `title` is stored as `dat_title`. Every reference is also an edge, typed after the field, from the version to an anchor node `(:DatEntity {dat_id})` (one per referenced entity), so graph queries can follow references: `MATCH (c:story_chapter)-[:appears]->(:DatEntity {dat_id: $who})`. An erase removes the versions and the anchors nothing else points to. Versions are written inside the branch's transaction; `dat_id` is indexed. |
| Iceberg | one table per type in the parts namespace (`parts_namespace`, `dat` by default: `dat.tracker_issue`), one row per version; `dat_id` and `dat_txn` as `string` (Arrow cannot filter Iceberg's `uuid` yet), `dat_created`, `dat_deleted`, `dat_tx_time`, and a column per field: `string`, `long`, `double`, `boolean`, `binary`, `date`, `timestamptz`, references and UUIDs as `string`, anything else as tagged JSON in a `string`. A branch's versions are one more data file staged before the decision. An erase travels in the journal payload (`erase`, in the shared staged-files format) and is committed with the files in one Iceberg transaction per table: a copy-on-write delete of the versions older than the branch's `dat_tx_time`, so a late erase never removes a later write. Older snapshots keep the rewritten files until snapshot expiry and orphan-file removal, the usual Iceberg maintenance. The Rust adapter refuses a payload with `erase` (a failure recovery retries) rather than skip it. |

A store joins by implementing the capabilities `PartStore` (committed reads)
and, on its branches, `PartBranch` (writes and reads inside a Unit of Work;
`dat_tx_time` is filled at stage). `rhizodat.testing.memory.MemoryEntityStore`
is an in-memory reference. A branch keeps one version per entity part: a second
write in the same Unit of Work replaces the first, an erase drops it.

**The parts contract** (`rhizodat.testing.parts`) checks a store's parts on the
real store: subclass `PartStoreContract` and provide a `parts_store` fixture.
It covers exact round trips of every field type, own writes visible only to
their branch, one version per Unit of Work, `known_at` reads and history, soft
deletes as versions, erase (alone, before and after a write in one branch),
rollback, idempotent `finish` (and, for one-phase stores, applying from the
journal payload) and `expired_parts`. The memory store, PostgreSQL,
ClickHouse, drevo (embedded and over Bolt) and Iceberg (SQLite and REST
catalogs) pass it in
`tests/contract/test_parts_contracts.py`.
