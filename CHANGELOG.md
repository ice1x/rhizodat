# Changelog

## 0.2.0 — the native core's gaps closed

The native core's gaps listed after the MVP are closed (the limits that remain
are in `docs/adapters/stores.md`), and the cloud-operations scenario replaces
the CBT journal:

- AWS Glue catalogs for Iceberg on both facades:
  `iceberg+glue://<region>?warehouse=s3://bucket/prefix`, the `rhizodat[glue]`
  extra (boto3) for the Python facade, `iceberg-catalog-glue` in the native core.
- Native facade, PostgreSQL rows (`uow.store(name).insert`): a list goes to an
  array or a `json(b)` column, a map to `json(b)` (they were refused).
- Native facade, PostgreSQL `execute`: `numeric` columns come back as `Decimal`
  with their scale, and a `Decimal` or an `int` binds to a `numeric` parameter,
  exactly (a binary codec in `rhizodat-postgres`, no float on the way).
- Native facade, Iceberg: partitioned tables are written, one file per
  partition of the default spec (they were refused at stage). A staged
  `DataFile` keeps its partition values in the spec's order through the journal,
  where a JSON object loses its key order.
- The CBT journal scenario is replaced by **cloud operations** over all four
  stores (`tests/scenarios/test_cloud_ops.py`): resources in PostgreSQL, their
  dependencies in drevo, telemetry in ClickHouse, raw API requests in Iceberg.
  A journal for one person fits in SQLite and was a poor case for the package.

## 0.1.0 — the MVP

The v0.1 roadmap is complete. Since 0.1.0a1:

- **Five end-to-end scenarios** in `tests/scenarios/` (a CBT journal, a bug
  tracker, an ERP with valid time, an IT task manager, a story editor), each run
  on the Python facade and on the native facade and closed with the same data
  worked from both in turn. They are the reference for how an application uses
  the package.
- **The shipped stores and their limits**: one documentation page per store and
  per facade with every known limit (`docs/adapters/stores.md`).
- Packaging: `rhizodat[native]` pins the Rust core to the package version;
  the `Release` workflow builds the wheels for Linux and macOS on x86_64 and
  arm64 from a `v*` tag.

## 0.1.0a1 — proof of concept

The first public pre-release. One API over PostgreSQL, ClickHouse,
[drevo](https://github.com/ice1x/drevo) and Apache Iceberg:

- **Meta-transactions**: a Unit of Work across several stores commits atomically
  (two-phase commit where the store has it, journaled one-phase application
  after the decision where it does not), with retries and crash recovery.
- **Bitemporal repositories**: every version of an entity is kept, with
  transaction time and valid time; reads `as_of` / `known_at`, history.
- **Entities over several stores**: an entity's parts may live in different
  stores and are written in one meta-transaction.
- **Migrations**: one TOML format for every store, the history in the journal.
- **Imports**: files into stores with schema inference (`rhizodat import`).
- **An MCP server** (`rhizodat-mcp`) and an adapter contract kit
  (`rhizodat[testing]`) for adapter authors.
- **The core in Rust**, opt-in: `pip install 'rhizodat[native]'` adds
  `rhizodat_native`, whose stores, journal, coordinator and recovery run the
  same protocol with no Python on the path; the Python package stays the
  executable specification, and both pass one conformance suite.

Known limits are listed per adapter in the documentation. Feedback on this
release: open an issue with the "Feedback on the proof of concept" template.
