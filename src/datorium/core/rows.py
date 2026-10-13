"""Row batches as journal payloads.

A non-2PC branch (ClickHouse) is applied only after the decision, possibly by
recovery on another host, so its rows travel in the journal. The encoding is
JSON; values JSON cannot carry exactly are tagged ``{"$": tag, "v": value}``:

- ``uuid``, ``decimal`` (as text, so ``12.3400`` stays ``12.3400``), ``bytes``
  (base64), ``date``, ``datetime`` (ISO 8601; a zone becomes its UTC offset);
- ``float`` only when not finite (``inf``, ``-inf``, ``nan``);
- ``map`` for every dict (``[[key, value], ...]``, keys are strings), so a
  plain dict can never be mistaken for a tag.

Lists stay JSON arrays. Anything else is rejected at stage, before prepare.
"""

import base64
import datetime as dt
import decimal
import json
import math
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

__all__ = ["Batch", "decode_batches", "encode_batches"]

_VERSION = 1


@dataclass(frozen=True, slots=True)
class Batch:
    """Rows for one table, in column order."""

    table: str
    columns: tuple[str, ...]
    rows: list[list[Any]]


def encode_batches(batches: Sequence[Batch]) -> bytes:
    doc = {
        "v": _VERSION,
        "batches": [
            {"table": b.table, "columns": list(b.columns), "rows": _encode_rows(b)} for b in batches
        ],
    }
    return json.dumps(doc, separators=(",", ":"), allow_nan=False).encode()


def decode_batches(raw: bytes) -> list[Batch]:
    doc = json.loads(raw)
    if doc.get("v") != _VERSION:
        raise ValueError(f"unsupported row payload version {doc.get('v')!r}")
    return [
        Batch(b["table"], tuple(b["columns"]), [[_decode(v) for v in row] for row in b["rows"]])
        for b in doc["batches"]
    ]


def _encode_rows(batch: Batch) -> list[list[Any]]:
    out = []
    for row in batch.rows:
        encoded = []
        for column, value in zip(batch.columns, row, strict=True):
            try:
                encoded.append(_encode(value))
            except TypeError as exc:
                raise TypeError(f"table {batch.table!r}, column {column!r}: {exc}") from None
        out.append(encoded)
    return out


def _tag(tag: str, value: Any) -> dict[str, Any]:  # noqa: ANN401 - JSON value
    return {"$": tag, "v": value}


def _encode(value: Any) -> Any:  # noqa: ANN401 - one branch per type
    if value is None or isinstance(value, bool | int | str):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return _tag("float", repr(value))
    if isinstance(value, uuid.UUID):
        return _tag("uuid", str(value))
    if isinstance(value, decimal.Decimal):
        return _tag("decimal", str(value))
    if isinstance(value, bytes):
        return _tag("bytes", base64.b64encode(value).decode())
    if isinstance(value, dt.datetime):  # before date: datetime is a date
        return _tag("datetime", value.isoformat())
    if isinstance(value, dt.date):
        return _tag("date", value.isoformat())
    if isinstance(value, list):
        return [_encode(v) for v in value]
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise TypeError("map keys must be strings")
        return _tag("map", [[k, _encode(v)] for k, v in value.items()])
    raise TypeError(f"cannot store a {type(value).__name__} in the journal")


_DECODERS: dict[str, Callable[[Any], Any]] = {
    "float": float,
    "uuid": uuid.UUID,
    "decimal": decimal.Decimal,
    "bytes": base64.b64decode,
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
}


def _decode(value: Any) -> Any:  # noqa: ANN401 - JSON value
    if isinstance(value, list):
        return [_decode(v) for v in value]
    if isinstance(value, dict):
        if set(value) != {"$", "v"}:
            raise ValueError(f"invalid row payload: {sorted(value)} is not a tagged value")
        tag, inner = value["$"], value["v"]
        if tag == "map":
            return {k: _decode(v) for k, v in inner}
        if tag not in _DECODERS:
            raise ValueError(f"invalid row payload: unknown tag {tag!r}")
        return _DECODERS[tag](inner)
    return value
