# rhizodat 🍄

**A bitemporal polyglot data layer.** rhizodat is a data access layer over
several databases that behaves like **one transactional store**:

| Store | Role |
|---|---|
| PostgreSQL | OLTP, SQL |
| ClickHouse | OLAP, SQL |
| [drevo](https://github.com/ice1x/drevo) | graph (Cypher), full-text search, vectors |
| Apache Iceberg | lakehouse tables |

Application code works with entities through a **Repository** and groups
writes in a **Unit of Work**. One Unit of Work is one **meta-transaction** that
spans every store it touches and gives ACID guarantees at the layer level.
Every record carries transaction time (and optionally valid time), so any
entity can be read **as of** any moment.

*A rhizome is the root network that joins separate plants into one organism:
rhizodat joins separate stores into one transactional, versioned whole.*

- [Getting started](getting-started.md): install, run the stores, the first entity.
- [Examples](examples.md): runnable scripts, tested on every change.
- [Concepts](concepts/principles.md): what the layer guarantees, and where a store limits it.
- [API reference](reference/rhizodat.md), generated from the code.

The Python implementation is the executable specification. The critical core
(commit protocol, journal, recovery, clock, identity) is being implemented once
in Rust and shared through bindings (see the [plan](plan.md)).

!!! note "Status: v0.1 in progress"
    Implemented: identity, HLC, DSNs, entity mapping, results, the journal,
    the commit coordinator and recovery; PostgreSQL, drevo, ClickHouse and
    Iceberg adapters; journals in PostgreSQL and drevo; Units of Work with
    retries; the Repository (CRUD, versions, hard and soft delete) with entity
    parts in every store; bitemporal reads (`as_of`, `known_at`); schema
    migrations across every store; file imports with schema inference;
    rhizodat-MCP.
