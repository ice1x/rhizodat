# Principles and non-goals

## Principles

1. **No store is primary.** An entity may live in a single store or be spread
   across all four. Identity, transactions, time and migrations never assume a
   master database.
2. **Native query languages stay.** SQL, Cypher and Iceberg scans are not
   unified into a DSL; the layer unifies identity, transactions, time and
   result access.
3. **ACID belongs to the layer**, not only to the stores, and is stated
   honestly where a store limits it (see [Guarantees](../concepts/transactions.md#guarantees)).
4. **Portable by design.** No Python-only mechanisms in the core contracts
   (identity, journal, protocol, migration format).

## Non-goals

- A universal query DSL
- Cross-store joins inside the database engines
- Schema unification across stores
