# Changelog

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
