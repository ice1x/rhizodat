# Stores and capabilities

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
through rhizodat need `dat_txn UUID` and `dat_tx_time Int64` columns, ideally
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
for these tables (entity parts, below, also erase); catalogs: `iceberg+rest://`
and `iceberg+sql://` (PostgreSQL or SQLite), Glue later. A branch over several tables commits one snapshot per
table: atomic with the rest of the meta-transaction by roll-forward, not by a
multi-table Iceberg commit.

---

# Architecture

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
