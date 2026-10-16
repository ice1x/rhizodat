# Time

rhizodat is **bitemporal**:

- **Transaction time** (always on): when the layer recorded a version.
  `dat_tx_time` is a hybrid logical clock (HLC) timestamp, assigned at prepare.
  Wall clocks of different hosts are never compared directly.
  Stored as one int64, `wall_ms << 16 | logical`, which sorts like the
  timestamp. A received timestamp more than `max_offset_ms` (default 500 ms)
  ahead of the local wall clock is refused rather than adopted.
- **Valid time** (opt-in per entity type): `valid_from` / `valid_to`, when the
  fact holds in the real world.

A write creates a new version; nothing is overwritten in place. A delete is a
**hard delete** by default: every version of the entity is erased, in every
store. A type declared with `soft_delete` keeps it instead (see
[Repository](../guide/repository.md)).

```python
repo.get(order_id)                              # current
repo.get(order_id, as_of=t)                     # valid at t
repo.get(order_id, as_of=t, known_at=t2)        # valid at t, as known at t2
repo.history(order_id)                          # every version, every store
```

Both are implemented. Every time is a time-zone-aware `datetime`, and
`known_at` is inclusive to the millisecond.

- **Without valid time**, `as_of=t` (or `known_at=t`) is the version known at
  `t`. Giving both is refused, since they would name the same moment.
- **With valid time** (`@entity(..., valid_time=True)`), each version holds
  for its period: `valid_from` inclusive, `valid_to` exclusive, `None`
  unbounded. The period is checked (time zones, `valid_from < valid_to`) and
  stored with every part as two ordinary datetime fields, so every store keeps
  it.
- `get(id, as_of=t, known_at=k)` takes the versions every store had committed
  by `k`, then returns the latest one whose period contains `t`. `t` and `k`
  both default to now. A later version for an overlapping period therefore
  wins where it overlaps, and the earlier one stays in force elsewhere.
- If the latest known version is a soft delete, the entity holds nowhere,
  unless `include_deleted` is given.
- `history(id)` lists every version with its period.

How each store keeps history: Iceberg snapshots and time travel; drevo facts
with validity windows; ClickHouse append-only versioned rows; PostgreSQL
versioned rows. A retention / compaction policy bounds the growth.
