# File imports

Load a CSV, TSV, JSON Lines or Parquet file (local or `s3://`, `gs://`;
compressed text files as they are) into a store, with the table's schema
inferred from the file:

```python
from rhizodat.ingest import import_file

report = import_file(dat, "s3://exports/sales-2026-10.csv", store="events",
                     table="sales", migrations="migrations", order_by=["order_id"])
report.rows, report.migration          # 120, 4
```

```
rhizodat import exports/moods.csv --store pg --table mood --migrations migrations --dry-run
```

- **Every row decides a column's type**, not the first one: the narrowest of
  `boolean`, `int64`, `float64`, `date`, `timestamp`, `timestamptz`, `string`
  that holds for all values (types widen `int64 → float64` and
  `date → timestamp`; any other mix is text). Leading zeros and numbers beyond
  64 bits stay text; `""`, `NULL`, `null` and `\N` are missing values (or
  `null_values=`); text with few distinct values is marked low-cardinality.
  `sample_rows=` trades certainty for speed.
- **A schema change is a migration.** A new table, or columns a wider export
  adds, is written as the next `NNNN_import_<table>.toml` in the migration
  directory, run and recorded like any other (with its time, for
  `schema_versions(as_of=)`). An import into a table that fits needs none.
- **The rows load as one unit** (the store's `Loads.load`): PostgreSQL copies
  in one transaction; ClickHouse fills a staging table and attaches its
  partitions (atomic per partition; `replace` exchanges the tables);
  Iceberg commits one table transaction. A failure halfway, the source's or
  the store's, leaves the table as it was. `replace=True` swaps the contents.
- S3 credentials come from the `AWS_*` environment or the URI
  (`s3://ACCESS:SECRET@bucket/key?endpoint_override=...`); the secret is
  masked wherever the source is shown.
