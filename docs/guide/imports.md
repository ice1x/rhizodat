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

## Graphs from RDF

An RDF file (Turtle, N-Triples, N3, JSON-LD, RDF/XML, OWL) goes into a graph
store such as drevo:

```python
from rhizodat.ingest import import_graph

report = import_graph(dat, "world.ttl", store="graph")
report.nodes, report.edges
```

```
rhizodat import world.ttl --store graph --dry-run
```

- Each resource is a node keyed by its IRI (its title in drevo), with the base
  label `Resource` and every `rdf:type` as a further label (the class's local
  name). A blank node is keyed `_:<id>`.
- A literal value is a property named after its predicate's local name; values
  in several languages are a list. Integers, floats, booleans and text keep
  their type; dates, times and decimals are stored as their exact text.
- A link to another resource is an edge typed after its predicate
  (`ex:lives-in` → `lives_in`); a local name two vocabularies share keeps them
  apart with their prefixes (`a_name`, `b_name`).
- Everything is written in one transaction (the store's `LoadsGraphs`): a
  failure halfway writes nothing. Nodes and edges are merged, so a second
  import of a revised file updates values instead of duplicating.
- A graph has no schema to migrate, so a graph import writes no migration.

## Graphs from several tables

Exported tables (CSV, TSV, JSON Lines, Parquet) become one graph, with the
references between them inferred from their columns:

```python
from rhizodat.ingest import import_tables_graph

report = import_tables_graph(
    dat, {"customers": "customers.csv", "orders": "orders.csv"}, store="graph"
)
[(r.table, r.column, r.target, r.coverage, r.accepted) for r in report.relationships]
```

```
rhizodat relate customers.csv orders.csv order_lines.csv --store graph --dry-run
+ orders.customer_id -> customers.id  100/100
+ order_lines.order_id -> orders.id  300/300
- payments.order_ref -> orders.id  47/50
would load 450 nodes, 400 edges -> graph
```

- A table's key is a column with no missing and no repeated value (`id`,
  else `<table>_id`, else the first such column), or the one given
  (`keys=` / `--key TABLE=COLUMN`).
- A column refers to a table when its name says so: without `_id` or `_ref`
  it is the table's name in the singular (`customer_id` → `customers`); an id
  column naming no table refers to its own table (`manager_id` in
  `employees`). Matching values alone are not enough, since quantities 1..5
  match ids 1..5 completely.
- A reference is accepted when at least `min_coverage` (0.95) of its values
  are keys of the target; every candidate is reported with how many matched,
  so orphans in an export show as a near miss. Relationships can also be
  given by hand.
- Rows become nodes labelled after their table (`order_lines` →
  `OrderLine`), keyed `<table>:<key>`; references become edges named after
  the column (`customer_id` → `customer`). All of it loads in one
  transaction. Tables are read into memory: this is for exports of
  moderate size.
