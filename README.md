# Unified Python Data Access Layer

## Unified Python API for OLTP, OLAP, Graph, and Vector databases

A Python abstraction layer that provides a consistent execution and result-handling API over multiple database paradigms:

- PostgreSQL (OLTP)
- ClickHouse (OLAP)
- Nebula Graph (Graph)
- Qdrant (Vector)

The project focuses on unifying **connection lifecycle**, **query execution**, and **result access**, while preserving native query languages and backend-specific capabilities.

---

## Goals
- Single Python API for heterogeneous data stores
- Adapter-based architecture
- Capability-driven design (SQL / Graph / Vector)
- No query language unification

## Non-goals
- No universal query DSL
- No cross-database joins
- No schema unification

---

## Supported capabilities

| Backend       | SQL | Graph | Vector |
|--------------|-----|-------|--------|
| PostgreSQL   | ✔   | ✖     | (pgvector, optional) |
| ClickHouse   | ✔   | ✖     | ✖ |
| Nebula Graph | ✖   | ✔     | ✖ |
| Qdrant       | ✖   | ✖     | ✔ |

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
vector_db = connect("qdrant://localhost:6333")

hits = vector_db.search(
    collection="documents",
    vector=embedding,
    limit=5
)
```

---

## Public API (v0.1)

### connect

```python
def connect(dsn: str, **options) -> Adapter
```

- Detects backend by DSN
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
        ngql: str,
        **params
    ) -> GraphResult
```

---

### VectorAdapter

```python
class VectorAdapter(Adapter):
    def upsert(
        self,
        collection: str,
        points: list[VectorPoint]
    ) -> None

    def search(
        self,
        collection: str,
        vector: list[float],
        limit: int = 10,
        filter: dict | None = None
    ) -> VectorResult
```

---

## Result model

```python
class Result:
    def __iter__(self): ...
    def first(self): ...
    def all(self): ...
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
│   ├── nebula.py
│   └── qdrant.py
└── connect.py
```

---

## Scope v0.1

✔ connect / close  
✔ execute / query / search  
✔ PostgreSQL + ClickHouse + Qdrant  

✖ transactions  
✖ async  
✖ schema API  
