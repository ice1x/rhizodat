"""The schema of an imported file: columns with a portable type.

A column's type is one of a few logical types every store can hold
(``boolean``, ``int64``, ``float64``, ``date``, ``timestamp``,
``timestamptz``, ``string``); each store adapter maps them to its own. Types
widen along two chains, ``int64 → float64`` and ``date → timestamp``. Any
other mix (a flag and a number, a time with and one without a zone) is
``string``: text keeps every value as it was.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = ["Column", "LogicalType", "TableSchema", "arrow_schema", "arrow_type", "join"]

LogicalType = Literal["boolean", "int64", "float64", "date", "timestamp", "timestamptz", "string"]

_CHAINS: tuple[tuple[LogicalType, ...], ...] = (
    ("boolean",),
    ("int64", "float64"),
    ("date", "timestamp"),
    ("timestamptz",),
)


def join(a: LogicalType, b: LogicalType) -> LogicalType:
    """The narrowest type holding values of both ``a`` and ``b``."""
    if a == b:
        return a
    for chain in _CHAINS:
        if a in chain and b in chain:
            return chain[max(chain.index(a), chain.index(b))]
    return "string"


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    type: LogicalType
    nullable: bool = True
    low_cardinality: bool = False
    """Few distinct values for many rows: a dictionary encoding pays (ClickHouse
    ``LowCardinality``); a hint, stores may ignore it."""


@dataclass(frozen=True, slots=True)
class TableSchema:
    columns: tuple[Column, ...]
    rows: int
    """Rows read to infer the schema (the whole file unless ``sample_rows`` was set)."""
    format: str
    """``csv``, ``tsv``, ``jsonl`` or ``parquet``."""
    null_values: tuple[str, ...] = ()
    """The text read as a missing value (CSV and TSV)."""

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(f"no column {name!r}; columns: {self.names}")


def arrow_type(t: LogicalType) -> "pa.DataType":
    import pyarrow as pa

    types: dict[str, Any] = {
        "boolean": pa.bool_(),
        "int64": pa.int64(),
        "float64": pa.float64(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamptz": pa.timestamp("us", tz="UTC"),
        "string": pa.string(),
    }
    return types[t]


def arrow_schema(columns: Sequence[Column]) -> "pa.Schema":
    import pyarrow as pa

    return pa.schema([pa.field(c.name, arrow_type(c.type), nullable=c.nullable) for c in columns])
