"""Staged data files as journal payloads.

A one-phase branch over files (Iceberg) writes its data files before prepare
and commits them only after the decision, possibly by recovery on another
host, so the journal carries what was staged::

    {"v": 1, "tables": [{"table": name, "files": [path, ...], "data_files": [object, ...]}]}

``data_files`` is optional: the Rust adapter adds one Iceberg ``DataFile``
JSON object per file (it cannot build one from a path); the Python adapter
derives them from the Parquet footers and ignores the objects. Entries of one
table are merged in order of first appearance. The format is pinned by
``conformance/staged_files.json``.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

__all__ = ["StagedTable", "decode_staged", "encode_staged"]

_VERSION = 1


@dataclass(frozen=True, slots=True)
class StagedTable:
    """The files staged for one table, with their ``DataFile`` objects if known."""

    table: str
    files: tuple[str, ...]
    data_files: tuple[dict[str, Any], ...] | None = None


def encode_staged(tables: Sequence[StagedTable]) -> bytes:
    entries: list[dict[str, Any]] = []
    for t in tables:
        entry: dict[str, Any] = {"table": t.table, "files": list(t.files)}
        if t.data_files is not None:
            entry["data_files"] = list(t.data_files)
        entries.append(entry)
    return json.dumps({"v": _VERSION, "tables": entries}, separators=(",", ":")).encode()


def _invalid(message: str) -> ValueError:
    return ValueError(f"invalid staged files payload: {message}")


def _entry(raw: Any) -> StagedTable:  # noqa: ANN401 - decoded JSON
    if not isinstance(raw, dict) or not isinstance(raw.get("table"), str):
        raise _invalid("an entry has no table name")
    files = raw.get("files")
    if not isinstance(files, list) or not all(isinstance(f, str) for f in files):
        raise _invalid(f"table {raw['table']!r}: files is not a list of paths")
    data_files = raw.get("data_files")
    if data_files is not None:
        if not isinstance(data_files, list) or not all(isinstance(d, dict) for d in data_files):
            raise _invalid(f"table {raw['table']!r}: data_files is not a list of objects")
        if len(data_files) != len(files):
            counts = f"{len(data_files)} data_files for {len(files)} files"
            raise _invalid(f"table {raw['table']!r}: {counts}")
        data_files = tuple(data_files)
    return StagedTable(raw["table"], tuple(files), data_files)


def decode_staged(raw: bytes | None) -> list[StagedTable]:
    if not raw:
        return []
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise _invalid(str(exc)) from exc
    if not isinstance(doc, dict):
        raise _invalid("not an object")
    if doc.get("v") != _VERSION:
        raise ValueError(f"unsupported staged files payload version {doc.get('v')!r}")
    if not isinstance(doc.get("tables"), list):
        raise _invalid("no tables")
    merged: dict[str, StagedTable] = {}
    for entry in map(_entry, doc["tables"]):
        seen = merged.get(entry.table)
        if seen is None:
            merged[entry.table] = entry
            continue
        both = seen.data_files is not None and entry.data_files is not None
        merged[entry.table] = StagedTable(
            entry.table,
            seen.files + entry.files,
            (seen.data_files or ()) + (entry.data_files or ()) if both else None,
        )
    return list(merged.values())
