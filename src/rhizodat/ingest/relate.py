"""Relationships inferred from tables, and the tables as a graph.

Several exported tables (CSV, TSV, JSON Lines, Parquet) become one graph: each
row a node labelled after its table (``order_lines`` → ``OrderLine``), each
reference between rows an edge.

- A table's **key** is a column with no missing and no repeated value: ``id``,
  else ``<table>_id``, else the first such column (or the one given).
- A column **refers** to a table when its name says so: its stem (the name
  without ``_id`` / ``_ref``) is that table's name in the singular
  (``customer_id`` → ``customers``). A column ending in ``_id`` or ``_ref`` that
  names no table refers to its own table (``manager_id`` in ``employees``).
  Values alone are not enough: quantities 1..5 match ids 1..5 completely.
- A reference is **accepted** when at least ``min_coverage`` of its values are
  found among the target's keys; every candidate is reported with how many
  matched, so a near miss (orphans in an export) is visible.

Tables are read into memory: this is for exports of moderate size.
"""

import datetime as dt
import decimal
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rhizodat.ingest.graph import Edge, GraphData, Node
from rhizodat.ingest.sources import infer_schema, read_batches

__all__ = [
    "Relationship",
    "Table",
    "infer_relationships",
    "measure",
    "read_tables",
    "tables_to_graph",
]

_REF_SUFFIX = re.compile(r"(_id|_ref|id)$", re.IGNORECASE)
_ID_SUFFIX = re.compile(r"_?id$", re.IGNORECASE)
_UNSET: Any = object()


@dataclass(frozen=True, slots=True)
class Table:
    name: str
    key: str | None
    columns: tuple[str, ...]
    rows: list[dict[str, Any]] = field(repr=False)


@dataclass(frozen=True, slots=True)
class Relationship:
    table: str
    column: str
    target: str
    key: str
    matched: int
    total: int
    """Values of the column (missing ones not counted)."""
    accepted: bool

    @property
    def coverage(self) -> float:
        return self.matched / self.total if self.total else 0.0


def singular(name: str) -> str:
    """``customers`` → ``customer``, ``categories`` → ``category`` (plain English plurals)."""
    lower = name.lower()
    if lower.endswith("ies") and len(lower) > 3:
        return lower[:-3] + "y"
    if lower.endswith("s") and not lower.endswith("ss"):
        return lower[:-1]
    return lower


def _label(table: str) -> str:
    """``order_lines`` → ``OrderLine``."""
    words = [w for w in re.split(r"[^A-Za-z0-9]+", singular(table)) if w]
    label = "".join(w[:1].upper() + w[1:] for w in words) or "Row"
    return f"T{label}" if label[0].isdigit() else label


def _edge_type(column: str) -> str:
    """``customer_id`` → ``customer``; a name without an id suffix stays as it is."""
    stem = _ID_SUFFIX.sub("", column).strip("_")
    return re.sub(r"[^A-Za-z0-9_]+", "_", stem or column)


def _match_value(value: Any) -> str | None:  # noqa: ANN401
    """A value as compared with keys: ints and text by their text; others never match."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | str):
        return str(value)
    return None


def _graph_value(value: Any) -> Any:  # noqa: ANN401
    match value:
        case bool() | int() | float() | str():
            return value
        case dt.datetime() | dt.date() | dt.time():
            return value.isoformat()
        case decimal.Decimal():
            return str(value)
    return str(value)


def _unique_complete(rows: Sequence[Mapping[str, Any]], column: str) -> bool:
    seen: set[Any] = set()
    for row in rows:
        value = row.get(column)
        if value is None or value in seen:
            return False
        seen.add(value)
    return bool(rows)


def read_tables(
    sources: Mapping[str, str | Path], *, keys: Mapping[str, str | None] | None = None
) -> dict[str, Table]:
    """Each source read with its inferred schema; ``keys`` names a table's key column
    (``None``: the table has none) instead of finding it."""
    given = dict(keys or {})
    tables: dict[str, Table] = {}
    for name, source in sources.items():
        schema = infer_schema(source)
        rows = [row for batch in read_batches(source, schema) for row in batch.to_pylist()]
        columns = tuple(schema.names)
        key = given.get(name, _UNSET)
        if key is _UNSET:
            preferred = ["id", f"{singular(name)}_id"]
            ordered = [c for c in preferred if c in columns] + [
                c for c in columns if c not in preferred
            ]
            key = next((c for c in ordered if _unique_complete(rows, c)), None)
        elif key is not None:
            if key not in columns:
                raise ValueError(f"table {name}: no column {key} (columns: {', '.join(columns)})")
            if not _unique_complete(rows, key):
                raise ValueError(f"table {name}: key {key} repeats or has missing values")
        tables[name] = Table(name, key, columns, rows)
    return tables


def infer_relationships(
    tables: Mapping[str, Table], *, min_coverage: float = 0.95
) -> list[Relationship]:
    """Every column that refers to a table by its name, with how many of its values
    are that table's keys; accepted at ``min_coverage`` or above."""
    keys = {
        name: {k for k in (_match_value(row[t.key]) for row in t.rows) if k is not None}
        for name, t in tables.items()
        if t.key is not None
    }
    found = []
    for table in tables.values():
        for column in table.columns:
            if column == table.key:
                continue
            stem = _REF_SUFFIX.sub("", column).strip("_").lower()
            named = [t for t in keys if stem and singular(t) == stem]
            if not named and stem != column.lower() and table.name in keys:
                named = [table.name]  # an id-like column naming no table: its own table
            values = [_match_value(row.get(column)) for row in table.rows]
            present = [v for v in values if v is not None]
            for target in named:
                matched = sum(1 for v in present if v in keys[target])
                if not present:
                    continue
                key = tables[target].key
                assert key is not None
                accepted = matched / len(present) >= min_coverage
                found.append(
                    Relationship(table.name, column, target, key, matched, len(present), accepted)
                )
    return found


def measure(tables: Mapping[str, Table], table: str, column: str, target: str) -> Relationship:
    """How many values of ``table.column`` are keys of ``target``: a relationship someone
    proposed (a person, a model), checked against the data. Accepted when any matched."""
    for name in (table, target):
        if name not in tables:
            raise ValueError(f"no table {name} (tables: {', '.join(tables)})")
    source, to = tables[table], tables[target]
    if column not in source.columns:
        raise ValueError(f"table {table}: no column {column}")
    if to.key is None:
        raise ValueError(f"table {target} has no key to refer to")
    keys = {_match_value(row[to.key]) for row in to.rows}
    present = [v for v in (_match_value(row.get(column)) for row in source.rows) if v is not None]
    matched = sum(1 for v in present if v in keys)
    return Relationship(table, column, target, to.key, matched, len(present), matched > 0)


def tables_to_graph(
    tables: Mapping[str, Table], relationships: Sequence[Relationship]
) -> GraphData:
    """Rows as nodes (``<table>:<key>``, or ``<table>:<row number>`` without a key) and
    each accepted relationship's matching values as edges."""
    nodes: dict[str, Node] = {}
    for table in tables.values():
        label = _label(table.name)
        for n, row in enumerate(table.rows, 1):
            ident = row[table.key] if table.key is not None else n
            node_key = f"{table.name}:{ident}"
            props = {c: _graph_value(v) for c, v in row.items() if v is not None}
            nodes[node_key] = Node(node_key, (label,), props)
    edges: set[Edge] = set()
    for rel in relationships:
        if not rel.accepted:
            continue
        for name in (rel.table, rel.target):
            if name not in tables:
                raise ValueError(f"relationship {rel.table}.{rel.column}: no table {name}")
        source = tables[rel.table]
        targets = {
            _match_value(row[rel.key]): f"{rel.target}:{row[rel.key]}"
            for row in tables[rel.target].rows
        }
        kind = _edge_type(rel.column)
        for n, row in enumerate(source.rows, 1):
            target = targets.get(_match_value(row.get(rel.column)))
            if target is None:
                continue
            ident = row[source.key] if source.key is not None else n
            edges.add(Edge(f"{source.name}:{ident}", kind, target))
    return GraphData(nodes, sorted(edges))
