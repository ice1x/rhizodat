# Unified Python Data Access Layer

## Unified Python API for OLTP, OLAP, Graph/Vector, and Lakehouse data

A Python abstraction layer that provides a consistent execution and result-handling API over multiple database paradigms:

- PostgreSQL (OLTP)
- ClickHouse (OLAP)
- [drevo](https://github.com/ice1x/drevo) (Graph + Full-text + Vector)
- Apache Iceberg (Lakehouse tables)

The project focuses on unifying **connection lifecycle**, **query execution**, and **result access**, while preserving native query languages and backend-specific capabilities.

---

## Goals
- Single Python API for heterogeneous data stores
- Adapter-based architecture
- Capability-driven design (SQL / Graph / Vector / FTS / Table)
- No query language unification

## Non-goals
- No universal query DSL
- No cross-database joins
- No schema unification
- No distributed (cross-backend) transactions

---

## Supported capabilities

| Backend        | SQL | Graph | Vector | FTS | Table | ACID |
|----------------|-----|-------|--------|-----|-------|------|
| PostgreSQL     | ✔   | ✖     | (pgvector, optional) | ✖ | ✖ | ✔ |
| ClickHouse     | ✔   | ✖     | ✖      | ✖   | ✖     | ✖ (no multi-statement transactions) |
| drevo          | ✖ (Cypher) | ✔ | ✔ | ✔ (BM25) | ✖ | ✔ (MVCC + WAL) |
| Apache Iceberg | ✖   | ✖     | ✖      | ✖   | ✔     | ✔ (atomic snapshot commits) |

**Where Iceberg's ACID comes from.** Iceberg is a table format, not a server:
every write produces a new snapshot, and the commit is an atomic swap of the
table's metadata pointer in the catalog (optimistic concurrency, snapshot
isolation for readers). The adapter uses `pyiceberg`, whose Transaction API
groups several operations (append / overwrite / delete / schema update) into a
single atomic commit. Engines that only read Iceberg (e.g. ClickHouse
`iceberg()`) are out of scope for writes.

---

## Connection strings

| DSN scheme | Adapter | Driver |
|------------|---------|--------|
| `postgresql://user:pass@host/db` | `PostgresAdapter` | `psycopg` |
| `clickhouse://user:pass@host:8123/db` | `ClickHouseAdapter` | `clickhouse-connect` |
| `drevo:///path/to/graph.redb` | `DrevoAdapter` (embedded) | `drevo-py` |
| `drevo+mem://` | `DrevoAdapter` (embedded, in-memory) | `drevo-py` |
| `drevo+bolt://host:7687` | `DrevoAdapter` (remote) | `neo4j` Bolt driver |
| `iceberg+rest://host:8181/warehouse` | `IcebergAdapter` | `pyiceberg` (REST catalog) |
| `iceberg+sql://…` / `iceberg+glue://…` | `IcebergAdapter` | `pyiceberg` (SQL / Glue catalog) |

---

## Example

```python
from unifieddb import connect

db = connect("postgresql://user:pass@localhost/db")

result = db.execute(
    "SELECT id, name FROM users WHERE active = true"
)

for row in result:
    print(row["id"], row["name"])
```

```python
graph = connect("drevo:///data/knowledge.redb")   # or "drevo+bolt://localhost:7687"

neighbours = graph.query(
    "MATCH (n:note {title: $title})-[:links_to]->(m) RETURN m.title AS title",
    title="Iceberg",
)

hits = graph.fts_search("snapshot isolation", k=5)

similar = graph.search(
    index="note_embeddings",
    vector=embedding,
    limit=5,
)
```

```python
lake = connect("iceberg+rest://localhost:8181/warehouse")

events = lake.scan(
    "analytics.events",
    filter="event_date >= '2026-01-01'",
    columns=["user_id", "event_type"],
).to_arrow()

with lake.transaction("analytics.events") as tx:   # one atomic commit
    tx.delete("event_date < '2025-01-01'")
    tx.append(new_events)                          # pyarrow.Table
```

---

## Public API (v0.1)

### connect

```python
def connect(dsn: str, **options) -> Adapter
```

- Detects backend by DSN scheme
- Returns an adapter with declared capabilities

---

## Adapter interfaces

### Base Adapter

```python
class Adapter:
    capabilities: set[str]

    def connect(self) -> None: ...
    def close(self) -> None: ...
    def execute(self, query: str, **params) -> Result: ...
```

---

### SQLAdapter

```python
class SQLAdapter(Adapter):
    def execute(
        self,
        query: str,
        params: dict | None = None
    ) -> Result
```

---

### GraphAdapter

```python
class GraphAdapter(Adapter):
    def query(
        self,
        cypher: str,
        **params
    ) -> GraphResult
```

---

### VectorAdapter

```python
class VectorAdapter(Adapter):
    def upsert(
        self,
        index: str,
        points: list[VectorPoint]
    ) -> None

    def search(
        self,
        index: str,
        vector: list[float],
        limit: int = 10,
        filter: dict | None = None
    ) -> VectorResult
```

---

### FTSAdapter

```python
class FTSAdapter(Adapter):
    def fts_search(
        self,
        query: str,
        k: int = 10,
        kind: str | None = None
    ) -> Result
```

---

### TableAdapter

```python
class TableAdapter(Adapter):
    def scan(
        self,
        table: str,
        filter: str | None = None,
        columns: list[str] | None = None,
        snapshot_id: int | None = None,   # time travel
        limit: int | None = None
    ) -> Result

    def append(self, table: str, data: "pyarrow.Table") -> None
    def overwrite(self, table: str, data: "pyarrow.Table", filter: str | None = None) -> None
    def delete(self, table: str, filter: str) -> None
    def transaction(self, table: str) -> ContextManager[TableTransaction]
    def snapshots(self, table: str) -> list[Snapshot]
```

---

### Composite adapters

One backend can declare several capabilities:

```python
class DrevoAdapter(GraphAdapter, VectorAdapter, FTSAdapter):
    capabilities = {"graph", "vector", "fts"}
```

---

## Result model

```python
class Result:
    def __iter__(self): ...
    def first(self): ...
    def all(self): ...
    def to_arrow(self): ...   # optional; ClickHouse / Iceberg
```

Backend-specific data is available via:
```python
result.raw
```

---

## Package structure

```
unifieddb/
├── core/
│   ├── adapter.py
│   ├── result.py
│   └── capabilities.py
├── adapters/
│   ├── postgres.py
│   ├── clickhouse.py
│   ├── drevo.py
│   └── iceberg.py
└── connect.py
```

Backend drivers are optional extras:

```bash
pip install "unifieddb[postgres]"     # psycopg
pip install "unifieddb[clickhouse]"   # clickhouse-connect
pip install "unifieddb[drevo]"        # drevo-py, neo4j
pip install "unifieddb[iceberg]"      # pyiceberg[pyarrow]
```

---

## Scope v0.1

✔ connect / close  
✔ execute / query / search / fts_search / scan  
✔ PostgreSQL + ClickHouse + drevo (embedded + Bolt) + Iceberg (scan / append / transaction)  
✔ transactions within a single backend (PostgreSQL, drevo, Iceberg)  

✖ cross-backend transactions  
✖ async  
✖ schema API  
