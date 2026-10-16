"""Reading import sources: CSV, TSV, JSON Lines and Parquet, local or in object storage.

A source is a path or a URI pyarrow can open (``s3://bucket/key``, ``gs://``,
``file://``). S3 credentials come from the usual ``AWS_*`` environment variables
or the URI itself (``s3://ACCESS:SECRET@bucket/key?endpoint_override=host:port&
scheme=http&region=...``); the secret is masked wherever the source is shown.
Compressed CSV, TSV and JSON Lines (``.gz``, ``.bz2``, ``.zst``, ...) are read
as they are.

Inference reads every row (or the first ``sample_rows``), with each value
seen as text: a type holds for all of them, not only for the first block.
The second pass reads the file again with those types.
"""

import csv
import datetime as dt
import io
import json
import re
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from rhizodat.ingest.schema import Column, LogicalType, TableSchema, arrow_schema, join

__all__ = ["DEFAULT_NULLS", "infer_schema", "read_batches", "source_format"]

DEFAULT_NULLS: tuple[str, ...] = ("", "NULL", "null", "\\N")
BATCH_ROWS = 10_000
_LOW_CARD_MAX = 10_000

_FORMATS = {
    ".csv": "csv",
    ".tsv": "tsv",
    ".tab": "tsv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
}
_COMPRESSED = (".gz", ".bz2", ".zst", ".lz4", ".br")

_BOOL = "^(true|false|True|False|TRUE|FALSE)$"
_INT = "^[+-]?(0|[1-9][0-9]*)$"
_FLOAT = r"^[+-]?(0|[1-9][0-9]*)(\.[0-9]*)?([eE][+-]?[0-9]+)?$|^[+-]?\.[0-9]+([eE][+-]?[0-9]+)?$"
_DATE = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
_TIME = r"[ T][0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]{1,9})?)?"
_TS = f"^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}({_TIME})?$"
_TSTZ = f"^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}{_TIME}(Z|[+-][0-9]{{2}}(:?[0-9]{{2}})?)$"
_CHECKS: tuple[tuple[LogicalType, str], ...] = (
    ("boolean", _BOOL),
    ("int64", _INT),
    ("float64", _FLOAT),
    ("date", _DATE),
    ("timestamp", _TS),
    ("timestamptz", _TSTZ),
)
_PY_CHECKS = tuple((t, re.compile(p)) for t, p in _CHECKS)


def source_format(source: str | Path) -> str:
    """The format named by the file's suffix (a compression suffix is looked past)."""
    name = str(source).split("?", 1)[0].lower()
    for suffix in _COMPRESSED:
        name = name.removesuffix(suffix)
    for suffix, fmt in _FORMATS.items():
        if name.endswith(suffix):
            return fmt
    known = ", ".join(_FORMATS)
    raise ValueError(
        f"cannot tell the format of {_shown(source)}: name it ({known}), or pass format="
    )


def _shown(source: str | Path) -> str:
    from rhizodat.core.dsn import redact

    text = str(source)
    return redact(text) if "://" in text else text


def _open(source: str | Path) -> tuple[Any, str]:
    import pyarrow.fs as pafs

    text = str(source)
    if "://" in text:
        try:
            fs, path = pafs.FileSystem.from_uri(text)
            return fs, str(path)
        except Exception as exc:
            raise ValueError(f"cannot open {_shown(source)}: {type(exc).__name__}") from None
    return pafs.LocalFileSystem(), str(Path(text).absolute())


def _stream(source: str | Path) -> Any:  # noqa: ANN401 - a pyarrow input stream
    fs, path = _open(source)
    return fs.open_input_stream(path, compression="detect")


# --- inference ----------------------------------------------------------------------------


class _Seen:
    """What one column's values have shown so far."""

    def __init__(self) -> None:
        self.type: LogicalType | None = None
        self.nulls = False
        self.values = 0
        self.distinct: set[Any] | None = set()

    def widen(self, t: LogicalType) -> None:
        self.type = t if self.type is None else join(self.type, t)

    def remember(self, values: Sequence[Any]) -> None:
        if self.distinct is None:
            return
        self.distinct.update(values)
        if len(self.distinct) > _LOW_CARD_MAX:
            self.distinct = None

    def column(self, name: str) -> Column:
        t: LogicalType = self.type or "string"
        low = (
            t == "string"
            and self.distinct is not None
            and self.values > 0
            and len(self.distinct) * 10 <= self.values
        )
        return Column(name, t, nullable=self.nulls or self.type is None, low_cardinality=low)


def _check_names(names: Sequence[str]) -> None:
    if not names:
        raise ValueError("the file is empty: no header row")
    seen: set[str] = set()
    for i, name in enumerate(names, 1):
        if not name.strip():
            raise ValueError(f"column {i} has no name")
        if name in seen:
            raise ValueError(f"column {name} appears twice")
        seen.add(name)


def _header(source: str | Path, delimiter: str) -> list[str]:
    with _stream(source) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        try:
            first = next(csv.reader(text, delimiter=delimiter))
        except StopIteration:
            first = []
    _check_names(first)
    return first


def _csv_reader(
    source: str | Path,
    names: list[str],
    delimiter: str,
    types: dict[str, Any],
    nulls: Sequence[str],
) -> Any:  # noqa: ANN401
    import pyarrow.csv as pacsv

    return pacsv.open_csv(
        _stream(source),
        read_options=pacsv.ReadOptions(column_names=names, skip_rows=1, block_size=1 << 22),
        parse_options=pacsv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
        convert_options=pacsv.ConvertOptions(
            column_types=types,
            null_values=list(nulls),
            strings_can_be_null=bool(nulls),
            quoted_strings_can_be_null=bool(nulls),
            true_values=["true", "True", "TRUE"],
            false_values=["false", "False", "FALSE"],
        ),
    )


def _batch_type(values: Any) -> LogicalType:  # noqa: ANN401 - a pyarrow string array
    import pyarrow as pa
    import pyarrow.compute as pc

    for t, pattern in _CHECKS:
        if pc.all(pc.match_substring_regex(values, pattern)).as_py():
            if t == "int64":
                try:
                    pc.cast(values, pa.int64())
                except pa.ArrowInvalid:
                    return "string"  # beyond 64 bits: an identifier, not a quantity
            return t
    return "string"


def _infer_text(
    source: str | Path, fmt: str, nulls: Sequence[str], sample_rows: int | None
) -> TableSchema:
    import pyarrow as pa
    import pyarrow.compute as pc

    delimiter = "\t" if fmt == "tsv" else ","
    names = _header(source, delimiter)
    seen = {n: _Seen() for n in names}
    rows = 0
    reader = _csv_reader(source, names, delimiter, dict.fromkeys(names, pa.string()), ())
    null_set = pa.array(list(nulls), pa.string())
    for batch in reader:
        if sample_rows is not None and rows >= sample_rows:
            break
        if sample_rows is not None and rows + batch.num_rows > sample_rows:
            batch = batch.slice(0, sample_rows - rows)
        rows += batch.num_rows
        for name, values in zip(names, batch.columns, strict=True):
            col = seen[name]
            present = pc.invert(pc.is_in(values, value_set=null_set))
            kept = pc.filter(values, present)
            if len(kept) < len(values):
                col.nulls = True
            if len(kept) == 0:
                continue
            col.values += len(kept)
            col.widen(_batch_type(kept))
            col.remember(pc.unique(kept).to_pylist())
    return TableSchema(tuple(seen[n].column(n) for n in names), rows, fmt, tuple(nulls))


def _json_type(value: Any) -> LogicalType:  # noqa: ANN401
    match value:
        case bool():
            return "boolean"
        case int():
            return "int64" if -(2**63) <= value < 2**63 else "string"
        case float():
            return "float64"
        case str():
            for t, pattern in _PY_CHECKS:
                if t not in ("boolean", "int64", "float64") and pattern.match(value):
                    return t
            return "string"
    return "string"  # objects and arrays: their JSON text


def _json_lines(source: str | Path) -> Iterator[dict[str, Any]]:
    with _stream(source) as raw:
        for n, line in enumerate(io.TextIOWrapper(raw, encoding="utf-8-sig"), 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {n} is not JSON: {exc.msg}") from None
            if not isinstance(row, dict):
                raise ValueError(f"line {n} is not a JSON object")
            yield row


def _infer_jsonl(source: str | Path, sample_rows: int | None) -> TableSchema:
    seen: dict[str, _Seen] = {}
    present: dict[str, int] = {}
    rows = 0
    for row in _json_lines(source):
        if sample_rows is not None and rows >= sample_rows:
            break
        rows += 1
        for key, value in row.items():
            col = seen.setdefault(key, _Seen())
            present[key] = present.get(key, 0) + 1
            if value is None:
                col.nulls = True
                continue
            col.values += 1
            col.widen(_json_type(value))
            col.remember(
                [json.dumps(value, sort_keys=True) if isinstance(value, list | dict) else value]
            )
    if not seen:
        raise ValueError("the file is empty: no JSON objects")
    for key, col in seen.items():
        if present[key] < rows:
            col.nulls = True
    return TableSchema(tuple(col.column(k) for k, col in seen.items()), rows, "jsonl")


def _arrow_logical(name: str, t: Any) -> tuple[LogicalType, bool]:  # noqa: ANN401
    import pyarrow.types as pt

    if pt.is_dictionary(t) and (pt.is_string(t.value_type) or pt.is_large_string(t.value_type)):
        return "string", True
    if pt.is_boolean(t):
        return "boolean", False
    if pt.is_integer(t):
        return "int64", False
    if pt.is_floating(t):
        return "float64", False
    if pt.is_date(t):
        return "date", False
    if pt.is_timestamp(t):
        return ("timestamptz" if t.tz else "timestamp"), False
    if pt.is_string(t) or pt.is_large_string(t):
        return "string", False
    raise ValueError(
        f"column {name} is {t}, which imports do not take yet"
        " (booleans, integers, floats, dates, timestamps and text)"
    )


def _infer_parquet(source: str | Path) -> TableSchema:
    import pyarrow.parquet as pq

    fs, path = _open(source)
    with fs.open_input_file(path) as f:
        meta = pq.ParquetFile(f)
        schema = meta.schema_arrow
        rows = meta.metadata.num_rows
    _check_names(schema.names)
    columns = []
    for field in schema:
        t, low = _arrow_logical(field.name, field.type)
        columns.append(Column(field.name, t, nullable=field.nullable, low_cardinality=low))
    return TableSchema(tuple(columns), rows, "parquet")


def infer_schema(
    source: str | Path,
    *,
    format: str | None = None,
    null_values: Sequence[str] = DEFAULT_NULLS,
    sample_rows: int | None = None,
) -> TableSchema:
    """The columns of ``source`` with the narrowest type each holds for every row read."""
    fmt = format or source_format(source)
    if fmt in ("csv", "tsv"):
        return _infer_text(source, fmt, tuple(null_values), sample_rows)
    if fmt == "jsonl":
        return _infer_jsonl(source, sample_rows)
    if fmt == "parquet":
        return _infer_parquet(source)
    raise ValueError(f"unknown format {fmt!r}: csv, tsv, jsonl or parquet")


# --- reading ------------------------------------------------------------------------------


def _json_value(value: Any, t: LogicalType) -> Any:  # noqa: ANN401
    if value is None:
        return None
    match t:
        case "string":
            return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
        case "float64":
            return float(value)
        case "date":
            return dt.date.fromisoformat(value)
        case "timestamp":
            return dt.datetime.fromisoformat(value)
        case "timestamptz":
            return dt.datetime.fromisoformat(value).astimezone(dt.UTC)
    return value


def read_batches(source: str | Path, schema: TableSchema) -> Iterator[Any]:
    """``source`` as pyarrow record batches with ``schema``'s types."""
    import pyarrow as pa

    target = arrow_schema(schema.columns)
    if schema.format in ("csv", "tsv"):
        delimiter = "\t" if schema.format == "tsv" else ","
        names = _header(source, delimiter)
        types = {f.name: f.type for f in target}
        for batch in _csv_reader(source, names, delimiter, types, schema.null_values):
            yield batch.select(schema.names)
    elif schema.format == "jsonl":
        rows: list[dict[str, Any]] = []
        kinds = {c.name: c.type for c in schema.columns}
        for row in _json_lines(source):
            rows.append({k: _json_value(row.get(k), t) for k, t in kinds.items()})
            if len(rows) == BATCH_ROWS:
                yield pa.RecordBatch.from_pylist(rows, schema=target)
                rows = []
        if rows:
            yield pa.RecordBatch.from_pylist(rows, schema=target)
    elif schema.format == "parquet":
        import pyarrow.parquet as pq

        fs, path = _open(source)
        with fs.open_input_file(path) as f:
            for batch in pq.ParquetFile(f).iter_batches(
                batch_size=BATCH_ROWS, columns=schema.names
            ):
                yield batch.cast(target)
    else:
        raise ValueError(f"unknown format {schema.format!r}")
