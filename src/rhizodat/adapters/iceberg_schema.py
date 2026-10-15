"""Declarative Iceberg schema operations: the body of an ``iceberg`` migration step.

A step is a list of operations, each a table with an ``op`` and its arguments::

    { op = "create_namespace", namespace = "tracker" }
    { op = "create_table", table = "tracker.tasks", columns = [
        { name = "id", type = "string", required = true },
        { name = "title", type = "string" },
    ] }
    { op = "add_column", table = "tracker.tasks", name = "points", type = "int", doc = "points" }
    { op = "rename_column", table = "tracker.tasks", from = "title", to = "summary" }
    { op = "drop_column", table = "tracker.tasks", name = "points" }
    { op = "add_partition", table = "tracker.tasks", column = "opened", transform = "day" }

Types: ``boolean``, ``int``, ``long``, ``float``, ``double``, ``date``, ``time``,
``timestamp``, ``timestamptz``, ``string``, ``uuid``, ``binary``,
``decimal(P, S)``. Transforms: ``identity``, ``year``, ``month``, ``day``,
``hour``, ``bucket[N]``, ``truncate[W]``. Every operation is a no-op when its
result is already there (an existing table, column or partition field, a
column already renamed or dropped), so a step can run again; one that cannot
be reconciled (a column of another type) is an error.
"""

import re
from collections.abc import Mapping
from typing import Any

__all__ = ["run_operations"]

_DECIMAL = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

# op -> (required arguments, optional arguments)
_OPS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "create_namespace": (frozenset({"namespace"}), frozenset()),
    "create_table": (frozenset({"table", "columns"}), frozenset()),
    "add_column": (frozenset({"table", "name", "type"}), frozenset({"doc"})),
    "rename_column": (frozenset({"table", "from", "to"}), frozenset()),
    "drop_column": (frozenset({"table", "name"}), frozenset()),
    "add_partition": (frozenset({"table", "column"}), frozenset({"transform", "name"})),
}


def iceberg_type(name: object) -> Any:  # noqa: ANN401 - a pyiceberg type
    from pyiceberg import types as t

    simple = {
        "boolean": t.BooleanType,
        "int": t.IntegerType,
        "long": t.LongType,
        "float": t.FloatType,
        "double": t.DoubleType,
        "date": t.DateType,
        "time": t.TimeType,
        "timestamp": t.TimestampType,
        "timestamptz": t.TimestamptzType,
        "string": t.StringType,
        "uuid": t.UUIDType,
        "binary": t.BinaryType,
    }
    if isinstance(name, str):
        if name in simple:
            return simple[name]()
        found = _DECIMAL.match(name)
        if found:
            return t.DecimalType(int(found.group(1)), int(found.group(2)))
    raise ValueError(f"unknown iceberg type {name!r}: {', '.join(simple)} or decimal(P, S)")


def _args(op: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    kind = op.get("op")
    if kind not in _OPS:
        raise ValueError(f"unknown iceberg operation {kind!r}: {', '.join(_OPS)}")
    required, optional = _OPS[kind]
    args = {k: v for k, v in op.items() if k != "op"}
    missing = sorted(required - set(args))
    if missing:
        raise ValueError(f"{kind} needs {', '.join(missing)}")
    extra = sorted(set(args) - required - optional)
    if extra:
        raise ValueError(f"{kind} takes no {', '.join(extra)}")
    return kind, args


def run_operations(catalog: Any, body: object) -> None:  # noqa: ANN401 - a pyiceberg catalog
    """Run the operations of one ``iceberg`` step against ``catalog``, in order."""
    if not isinstance(body, list) or not all(isinstance(op, dict) for op in body):
        raise ValueError(f"an iceberg step is a list of operations (tables), got {body!r}")
    ops = [_args(op) for op in body]  # all checked before the first runs
    for kind, args in ops:
        _RUN[kind](catalog, **args)


def _create_namespace(catalog: Any, namespace: str) -> None:  # noqa: ANN401
    catalog.create_namespace_if_not_exists(namespace)


def _create_table(catalog: Any, table: str, columns: list[Any]) -> None:  # noqa: ANN401
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    fields = []
    for i, column in enumerate(columns, 1):
        if not isinstance(column, dict) or not {"name", "type"} <= set(column):
            raise ValueError(f"create_table {table}: column {i} needs a name and a type")
        extra = sorted(set(column) - {"name", "type", "required", "doc"})
        if extra:
            raise ValueError(f"create_table {table}: column {i} takes no {', '.join(extra)}")
        fields.append(
            NestedField(
                field_id=i,
                name=column["name"],
                field_type=iceberg_type(column["type"]),
                required=bool(column.get("required", False)),
                doc=column.get("doc"),
            )
        )
    catalog.create_table_if_not_exists(table, Schema(*fields))


def _field(schema: Any, name: str) -> Any:  # noqa: ANN401
    try:
        return schema.find_field(name)
    except ValueError:
        return None


def _add_column(
    catalog: Any,  # noqa: ANN401
    table: str,
    name: str,
    type: str,
    doc: str | None = None,
) -> None:
    wanted = iceberg_type(type)
    t = catalog.load_table(table)
    existing = _field(t.schema(), name)
    if existing is not None:
        if str(existing.field_type) != str(wanted):
            raise ValueError(
                f"add_column {table}: column {name} is {existing.field_type}, not {type}"
            )
        return
    with t.update_schema() as update:
        update.add_column(name, wanted, doc=doc)


def _rename_column(catalog: Any, table: str, **names: str) -> None:  # noqa: ANN401
    old, new = names["from"], names["to"]
    t = catalog.load_table(table)
    schema = t.schema()
    if _field(schema, old) is None:
        if _field(schema, new) is not None:
            return  # renamed before
        raise ValueError(f"rename_column {table}: neither {old} nor {new} exists")
    with t.update_schema() as update:
        update.rename_column(old, new)


def _drop_column(catalog: Any, table: str, name: str) -> None:  # noqa: ANN401
    t = catalog.load_table(table)
    if _field(t.schema(), name) is None:
        return
    with t.update_schema() as update:
        update.delete_column(name)


def _add_partition(
    catalog: Any,  # noqa: ANN401
    table: str,
    column: str,
    transform: str = "identity",
    name: str | None = None,
) -> None:
    from pyiceberg.transforms import parse_transform

    try:
        wanted = parse_transform(transform)
    except ValueError:
        raise ValueError(f"add_partition {table}: unknown transform {transform!r}") from None
    t = catalog.load_table(table)
    source = _field(t.schema(), column)
    if source is None:
        raise ValueError(f"add_partition {table}: no column {column}")
    for f in t.spec().fields:
        if f.source_id == source.field_id and str(f.transform) == str(wanted):
            return
    with t.update_spec() as update:
        update.add_field(column, wanted, name)


_RUN: dict[str, Any] = {
    "create_namespace": _create_namespace,
    "create_table": _create_table,
    "add_column": _add_column,
    "rename_column": _rename_column,
    "drop_column": _drop_column,
    "add_partition": _add_partition,
}
