"""Apache Iceberg adapter (``pyiceberg``), a one-phase branch committed after the decision.

Iceberg commits optimistically: a commit swaps the table's metadata pointer to
a new snapshot. A branch therefore splits the work:

- ``append(table, rows)`` buffers rows (mappings or a ``pyarrow.Table``);
- ``stage`` (before prepare, lock-free) converts each table's rows to its
  Arrow schema, adds ``dat_txn`` / ``dat_tx_time``, and writes one Parquet data
  file per table under the table's ``data/`` location. The files are invisible
  (no snapshot references them); the journal payload lists their paths (the
  shared format of ``rhizodat.core.staged``), so whoever finishes the branch
  (the coordinator, or recovery on another host, in either implementation) can
  commit them;
- after the decision the participant adds each table's file in one snapshot
  (``add_files``) whose summary carries ``dat.txn`` and ``dat.tx_time``.

Finishing is idempotent: a table is skipped when a snapshot is tagged with this
``dat_txn`` or its current snapshot already references the files (the tag may
have been expired). Snapshots of one branch are one per table; Iceberg has no
multi-table commit here, but the decision is durable and every table is rolled
forward. Staged files of an aborted branch are deleted; files left by a crash
before the journal record are unreferenced and removed by orphan-file
maintenance. Files are never deleted while a snapshot may reference them.

Tables written through rhizodat need ``dat_txn`` (``uuid`` or ``string``) and
``dat_tx_time`` (``long``) columns. Only appends are supported for them.

Entity parts (``PartStore``) live in tables of their own namespace
(``parts_namespace``, ``dat`` by default): one row per version, with
``dat_id`` / ``dat_txn`` as text (Arrow cannot filter Iceberg's ``uuid`` yet),
``dat_created``, ``dat_deleted``, native columns for str, int, float, bool,
bytes, date and datetime fields, ids as text, and tagged JSON for the rest. A
branch's versions are one more staged data file. A hard delete travels in the
journal payload (``erase``) and is committed with the files, in one Iceberg
transaction per table: it rewrites the data files that hold the entity's
versions older than the branch, so a late erase (recovery) never removes a later
write. Older snapshots still reference the old files until snapshot expiry and
orphan-file removal, the usual Iceberg maintenance, take them away for good.

pyiceberg is imported on first use, so the driver stays an optional extra.
"""

import json
import uuid
import warnings
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from rhizodat.core.adapter import Adapter, Scheme
from rhizodat.core.dsn import parse_dsn
from rhizodat.core.journal import BranchRecord
from rhizodat.core.parts import Part, Version
from rhizodat.core.result import Result
from rhizodat.core.rows import decode_value, encode_value
from rhizodat.core.staged import StagedTable, decode_staged, encode_staged

if TYPE_CHECKING:
    import pyarrow as pa

__all__ = [
    "ADAPTER",
    "IcebergBranch",
    "IcebergParticipant",
    "IcebergStore",
    "catalog_properties",
]

_RESERVED = ("dat_txn", "dat_tx_time")
_TXN_PROPERTY = "dat.txn"
_TIME_PROPERTY = "dat.tx_time"


def _pyiceberg() -> Any:  # noqa: ANN401 - the driver module, imported lazily
    try:
        import pyiceberg.catalog
    except ImportError as exc:
        raise ImportError(
            "the Iceberg adapter needs pyiceberg: pip install 'rhizodat[iceberg]'"
        ) from exc
    return pyiceberg


def catalog_properties(dsn: str) -> dict[str, str]:
    """pyiceberg catalog properties for an ``iceberg+rest`` / ``iceberg+sql`` DSN.

    Query parameters other than ``secure`` pass through as catalog properties
    (``warehouse``, ``s3.endpoint``, ``credential``, ...).
    """
    parsed = parse_dsn(dsn)
    if parsed.store != "iceberg":
        raise ValueError(f"not an iceberg connection string: {parsed}")
    extra = {k: v for k, v in parsed.params.items() if k != "secure"}
    if parsed.transport == "rest":
        secure = parsed.params.get("secure", "").lower() in ("1", "true", "yes")
        port = f":{parsed.port}" if parsed.port else ""
        props = {"type": "rest", "uri": f"{'https' if secure else 'http'}://{parsed.host}{port}"}
        if parsed.path and parsed.path.strip("/"):
            props["warehouse"] = parsed.path.strip("/")
        return {**props, **extra}
    if parsed.transport == "sql":
        if parsed.host is None:
            if not parsed.path:
                raise ValueError("iceberg+sql:// needs a host (PostgreSQL) or a SQLite file path")
            return {"type": "sql", "uri": f"sqlite:///{parsed.path}", **extra}
        auth = ""
        if parsed.user:
            password = f":{quote(parsed.password, safe='')}" if parsed.password else ""
            auth = f"{quote(parsed.user, safe='')}{password}@"
        port = f":{parsed.port}" if parsed.port else ""
        uri = f"postgresql+psycopg://{auth}{parsed.host}{port}/{parsed.database or ''}"
        return {"type": "sql", "uri": uri, **extra}
    raise NotImplementedError(f"the iceberg+{parsed.transport} catalog is not supported yet")


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, ConnectionError | TimeoutError):
        return True
    exceptions = _pyiceberg().exceptions
    return isinstance(
        error,
        exceptions.CommitFailedException  # a concurrent commit moved the metadata pointer
        | exceptions.CommitStateUnknownException  # resolved by is_applied
        | exceptions.ServiceUnavailableError
        | exceptions.ServerError
        | exceptions.TooManyRequestsError,
    )


def _encode_payload(files: list[tuple[str, str]], erase: Mapping[str, list[str]]) -> bytes:
    staged = [StagedTable(t, (f,), erase=tuple(erase.get(t, ()))) for t, f in files]
    with_files = {t for t, _ in files}
    staged += [
        StagedTable(t, (), erase=tuple(ids)) for t, ids in erase.items() if t not in with_files
    ]
    return encode_staged(staged)


# --- entity parts -----------------------------------------------------------------------


def _field_kind(annotation: object) -> str | None:
    """The Iceberg type of a field (``id``: a uuid as text), or ``None``: tagged JSON."""
    import datetime as dt
    import types
    import typing

    from rhizodat.core.mapping import Ref

    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        return _field_kind(args[0]) if len(args) == 1 else None
    if annotation is Ref or origin is Ref or annotation is uuid.UUID:
        return "id"
    natives: dict[object, str] = {
        str: "string",
        bool: "boolean",
        int: "long",
        float: "double",
        bytes: "binary",
        dt.datetime: "timestamptz",
        dt.date: "date",
    }
    return natives.get(annotation)


class _PartTable:
    """How one part is stored: its table, schema and value conversions."""

    def __init__(self, part: Part, namespace: str) -> None:
        self.part = part
        self.table = f"{namespace}.{part.table}"
        self.kinds = {f: _field_kind(part.types.get(f)) for f in part.fields}

    def schema(self) -> Any:  # noqa: ANN401 - a pyiceberg Schema
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            BinaryType,
            BooleanType,
            DateType,
            DoubleType,
            LongType,
            NestedField,
            StringType,
            TimestamptzType,
        )

        types: dict[str | None, Any] = {
            "boolean": BooleanType(),
            "long": LongType(),
            "double": DoubleType(),
            "binary": BinaryType(),
            "timestamptz": TimestamptzType(),
            "date": DateType(),
        }
        columns = [
            ("dat_id", StringType(), True),
            ("dat_created", LongType(), True),
            ("dat_deleted", BooleanType(), True),
            *((f, types.get(self.kinds[f], StringType()), False) for f in self.part.fields),
            ("dat_txn", StringType(), True),
            ("dat_tx_time", LongType(), True),
        ]
        return Schema(
            *(
                NestedField(i, name, kind, required=required)
                for i, (name, kind, required) in enumerate(columns, start=1)
            )
        )

    def row(self, dat_id: uuid.UUID, version: Version, dat_tx_time: int) -> dict[str, Any]:
        created = version.created if version.created is not None else dat_tx_time
        row: dict[str, Any] = {
            "dat_id": str(dat_id),
            "dat_created": created,
            "dat_deleted": version.deleted,
        }
        for f in self.part.fields:
            value = version.values.get(f)
            if value is None:
                row[f] = None
            elif self.kinds[f] == "id":
                row[f] = str(value)
            elif self.kinds[f] is None:
                row[f] = json.dumps(encode_value(value), separators=(",", ":"))
            else:
                row[f] = value
        return row

    def version(self, row: Mapping[str, Any]) -> Version:
        return Version(
            dat_txn=uuid.UUID(row["dat_txn"]),
            dat_tx_time=row["dat_tx_time"],
            created=row["dat_created"],
            deleted=bool(row["dat_deleted"]),
            values={f: self._value(f, row[f]) for f in self.part.fields},
        )

    def _value(self, field: str, raw: Any) -> Any:  # noqa: ANN401 - any stored value
        if raw is None:
            return None
        if self.kinds[field] == "id":
            return uuid.UUID(raw)
        if self.kinds[field] is None:
            return decode_value(json.loads(raw))
        return raw


def _local_free_space(location: str) -> int | None:
    import shutil
    from pathlib import Path
    from urllib.parse import unquote, urlsplit

    parts = urlsplit(location)
    if parts.scheme != "file":
        return None
    path = Path(unquote(parts.path))
    while not path.exists() and path != path.parent:  # not created yet: its disk
        path = path.parent
    return shutil.disk_usage(path).free


class IcebergStore:
    """One Iceberg catalog as a rhizodat store; ``catalog`` is the pyiceberg catalog."""

    two_phase = False

    def __init__(self, dsn: str, *, name: str = "iceberg", parts_namespace: str = "dat") -> None:
        props = catalog_properties(dsn)
        self.name = name
        self.parts_namespace = parts_namespace
        self._dsn = parse_dsn(dsn)
        self.catalog = _pyiceberg().catalog.load_catalog(name, **props)
        self._participant = IcebergParticipant(self)
        self._parts: dict[Part, _PartTable] = {}

    def __repr__(self) -> str:
        return f"IcebergStore({self.name!r}, {self._dsn.redacted()!r})"

    def install(self) -> None:
        """Nothing to create: snapshot summaries are the applied markers."""

    def execute(self, table: str, params: Mapping[str, Any] | None = None) -> Result:
        """Scan ``table``; ``params`` go to ``Table.scan`` (``row_filter``, ``selected_fields``,
        ``snapshot_id``, ``limit``)."""
        scan = self.catalog.load_table(table).scan(**dict(params or {}))
        arrow = scan.to_arrow()
        return Result(
            iter(arrow.to_pylist()),
            columns=tuple(arrow.column_names),
            raw=scan,
            arrow=lambda: arrow,
        )

    migration_kinds = frozenset({"iceberg"})

    def run_migration_step(self, kind: str, body: Any) -> None:  # noqa: ANN401 - TOML data
        """An ``iceberg`` step: declarative schema operations (``iceberg_schema``)."""
        from rhizodat.adapters.iceberg_schema import run_operations

        run_operations(self.catalog, body)

    def read_only(self, table: str, params: Mapping[str, Any] | None = None) -> Result:
        """The same scan as ``execute``: a scan cannot write."""
        return self.execute(table, params)

    def branch(self, dat_txn: uuid.UUID) -> "IcebergBranch":
        return IcebergBranch(self, dat_txn)

    def participant(self) -> "IcebergParticipant":
        return self._participant

    def free_space(self) -> int | None:
        """Free bytes of a local (``file://``) warehouse's disk; ``None`` for object storage."""
        return _local_free_space(str(self.catalog.properties.get("warehouse", "")))

    def close(self) -> None:
        engine = getattr(self.catalog, "engine", None)  # SqlCatalog keeps a SQLAlchemy engine
        if engine is not None:
            engine.dispose()

    # --- PartStore -------------------------------------------------------------------------

    def install_part(self, part: Part) -> None:
        table = _PartTable(part, self.parts_namespace)
        self.catalog.create_namespace_if_not_exists(self.parts_namespace)
        self.catalog.create_table_if_not_exists(table.table, table.schema())
        self._parts[part] = table

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        bound = "" if known_at is None else f" AND dat_tx_time <= {int(known_at)}"
        versions = self._versions(part, f"dat_id == '{uuid.UUID(str(dat_id))}'{bound}")
        return versions[-1] if versions else None

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        return self._versions(part, f"dat_id == '{uuid.UUID(str(dat_id))}'")

    def _versions(self, part: Part, row_filter: str) -> list[Version]:
        """Matching versions, oldest first."""
        table = self.part_table(part)
        rows = self.execute(table.table, {"row_filter": row_filter}).all()
        return [table.version(r) for r in sorted(rows, key=lambda r: r["dat_tx_time"])]

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        table = self.part_table(part)
        fields = ("dat_id", "dat_deleted", "dat_tx_time")
        latest: dict[str, tuple[int, bool]] = {}
        for r in self.execute(table.table, {"selected_fields": fields}).all():
            seen = latest.get(r["dat_id"])
            if seen is None or r["dat_tx_time"] > seen[0]:
                latest[r["dat_id"]] = (r["dat_tx_time"], bool(r["dat_deleted"]))
        expired = sorted(
            i for i, (ts, deleted) in latest.items() if deleted and ts < deleted_before
        )
        return [uuid.UUID(i) for i in expired[:limit]]

    def part_table(self, part: Part) -> _PartTable:
        table = self._parts.get(part)
        if table is None:
            self.install_part(part)
            table = self._parts[part]
        return table

    # --- used by the branch and the participant ------------------------------------------

    def versions_besides(self, table: str, ids: list[str], dat_txn: uuid.UUID) -> bool:
        """Whether ``table`` holds a version of ``ids`` written by another transaction."""
        scan = self.catalog.load_table(table).scan(
            row_filter=f"{_ids_in(ids)} AND dat_txn != '{dat_txn}'",
            selected_fields=("dat_id",),
            limit=1,
        )
        return bool(scan.to_arrow().num_rows)

    def delete_files(self, table: str, files: list[str]) -> None:
        io = self.catalog.load_table(table).io
        for path in files:
            try:
                io.delete(path)
            except FileNotFoundError:
                continue

    def referenced(self, table: str, files: list[str]) -> bool:
        """Whether the current snapshot of ``table`` references every one of ``files``."""
        t = self.catalog.load_table(table)
        if t.current_snapshot() is None:
            return False
        # Not t.inspect.data_files(): it fails on uuid columns (pyiceberg 0.12, column bounds).
        live = {task.file.file_path for task in t.scan().plan_files()}
        return all(f in live for f in files)

    def tagged(self, table: str, dat_txn: uuid.UUID) -> bool:
        snapshots = self.catalog.load_table(table).snapshots()
        txn = str(dat_txn)
        return any(s.summary is not None and s.summary.get(_TXN_PROPERTY) == txn for s in snapshots)


def _ids_in(ids: list[str]) -> str:
    """A row filter on ``dat_id``; every id is a UUID's text, so nothing needs escaping."""
    return "dat_id IN (" + ", ".join(f"'{uuid.UUID(i)}'" for i in ids) + ")"


def _erase_filter(ids: list[str], dat_tx_time: int) -> str:
    return f"{_ids_in(ids)} AND dat_tx_time < {int(dat_tx_time)}"


class IcebergBranch:
    two_phase = False

    def __init__(self, store: IcebergStore, dat_txn: uuid.UUID) -> None:
        self.store = store.name
        self.dat_txn = dat_txn
        self._ice = store
        self._buffer: dict[str, list[Any]] = {}
        self._versions: dict[Part, dict[uuid.UUID, Version]] = {}
        self._erased: dict[Part, set[uuid.UUID]] = {}
        self._written: list[tuple[str, str]] = []
        self._staged: tuple[bytes, int] | None = None
        self._commit_attempted = False

    def append(self, table: str, data: Iterable[Mapping[str, Any]] | Any) -> None:  # noqa: ANN401
        """Buffer rows (mappings or a ``pyarrow.Table``) to append to ``table``."""
        names = getattr(data, "column_names", None)
        if names is not None:
            columns, part = list(names), data
        else:
            part = [dict(row) for row in data]
            columns = sorted({c for row in part for c in row})
        for reserved in _RESERVED:
            if reserved in columns:
                raise ValueError(f"{reserved} is set by rhizodat; leave it out of the rows")
        self._buffer.setdefault(table, []).append(part)

    def execute(self, table: str, params: Mapping[str, Any] | None = None) -> Result:
        """Scan the table. Buffered rows are not visible before the commit."""
        return self._ice.execute(table, params)

    # --- PartBranch ------------------------------------------------------------------------

    def write_part(
        self,
        part: Part,
        dat_id: uuid.UUID,
        values: Mapping[str, Any],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        unknown = set(values) - set(part.fields)
        if unknown:
            raise ValueError(f"{part.type_name}: {sorted(unknown)} are not fields of this part")
        version = Version(self.dat_txn, None, created, deleted, dict(values))
        self._versions.setdefault(part, {})[dat_id] = version

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        self._versions.get(part, {}).pop(dat_id, None)
        self._erased.setdefault(part, set()).add(dat_id)

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        mine = self._versions.get(part, {}).get(dat_id)
        if mine is not None:
            return mine
        if dat_id in self._erased.get(part, ()):
            return None
        return self._ice.read_part(part, dat_id)

    # --- commit ----------------------------------------------------------------------------

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes:
        import pyarrow.parquet as pq

        erase: dict[str, list[str]] = {}
        for part, ids in self._erased.items():
            if ids:
                erase[self._ice.part_table(part).table] = sorted(str(i) for i in ids)
        tables = dict(self._buffer)
        for part, versions in self._versions.items():
            if versions:
                table = self._ice.part_table(part)
                rows = [table.row(i, v, dat_tx_time) for i, v in versions.items()]
                tables.setdefault(table.table, []).append(rows)
        for table_name, parts in tables.items():
            table = self._ice.catalog.load_table(table_name)
            data = self._to_arrow(
                table_name, table.schema().as_arrow(), parts, dat_txn, dat_tx_time
            )
            path = f"{table.location()}/data/dat-{dat_txn}-{self.store}.parquet"
            with table.io.new_output(path).create(overwrite=True) as out:
                pq.write_table(data, out)
            self._written.append((table_name, path))
        payload = _encode_payload(self._written, erase)
        self._staged = (payload, dat_tx_time)
        return payload

    def prepare(self, gid: str) -> None:
        raise TypeError("an Iceberg branch is not two-phase; it is committed after the decision")

    def commit_one_phase(self) -> None:
        if self._staged is None:
            raise RuntimeError("commit_one_phase before stage")
        payload, dat_tx_time = self._staged
        self._commit_attempted = True
        record = BranchRecord(self.store, f"dat:{self.dat_txn}:{self.store}", False, payload)
        self._ice.participant().finish(record, self.dat_txn, dat_tx_time)

    def rollback(self) -> None:
        self._buffer.clear()
        self._versions.clear()
        self._erased.clear()
        if not self._written:
            return
        if self._commit_attempted:
            payload = self._staged[0] if self._staged else None
            record = BranchRecord(self.store, f"dat:{self.dat_txn}:{self.store}", False, payload)
            try:
                if self._ice.participant().is_applied(record, self.dat_txn):
                    return
            except Exception:  # unknown: keep the files, orphan maintenance removes them
                return
        for table, path in self._written:
            self._ice.delete_files(table, [path])
        self._written.clear()

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)

    def _to_arrow(
        self,
        table: str,
        schema: "pa.Schema",
        parts: list[Any],
        dat_txn: uuid.UUID,
        dat_tx_time: int,
    ) -> "pa.Table":
        import pyarrow as pa

        names = set(schema.names)
        missing = [c for c in _RESERVED if c not in names]
        if missing:
            raise ValueError(f"table {table!r} has no column(s) {', '.join(missing)}")
        txn_type = schema.field("dat_txn").type
        as_uuid = pa.types.is_fixed_size_binary(getattr(txn_type, "storage_type", txn_type))
        txn_value: Any = dat_txn.bytes if as_uuid else str(dat_txn)
        tables = []
        for part in parts:
            try:
                if isinstance(part, list):
                    unknown = sorted({c for row in part for c in row} - names)
                    if unknown:
                        raise ValueError(f"table {table!r} has no column(s) {', '.join(unknown)}")
                    rows = [
                        {**_uuid_bytes(row), "dat_txn": txn_value, "dat_tx_time": dat_tx_time}
                        for row in part
                    ]
                    tables.append(pa.Table.from_pylist(rows, schema=schema))
                else:
                    unknown = sorted(set(part.column_names) - names)
                    if unknown:
                        raise ValueError(f"table {table!r} has no column(s) {', '.join(unknown)}")
                    n = part.num_rows
                    part = part.append_column(
                        "dat_txn", pa.array([txn_value] * n, type=schema.field("dat_txn").type)
                    ).append_column("dat_tx_time", pa.array([dat_tx_time] * n, type=pa.int64()))
                    columns = [
                        part.column(f.name)
                        if f.name in part.column_names
                        else pa.nulls(n, type=f.type)
                        for f in schema
                    ]
                    tables.append(pa.Table.from_arrays(columns, schema=schema))
            except (pa.ArrowException, TypeError, OverflowError) as exc:
                raise ValueError(f"rows do not fit {table!r}: {exc}") from exc
        return pa.concat_tables(tables)


def _uuid_bytes(row: Mapping[str, Any]) -> dict[str, Any]:
    """pyarrow's uuid type takes 16 bytes, not ``uuid.UUID``."""
    return {k: v.bytes if isinstance(v, uuid.UUID) else v for k, v in row.items()}


class IcebergParticipant:
    def __init__(self, store: IcebergStore) -> None:
        self._ice = store

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        """Per table, in one Iceberg transaction: erase, then add the staged files.

        An erase is bounded by ``dat_tx_time``, so repeating it is harmless.
        """
        properties = {_TXN_PROPERTY: str(dat_txn), _TIME_PROPERTY: str(dat_tx_time)}
        for staged in decode_staged(branch.payload):
            files, erase = list(staged.files), list(staged.erase)
            if self._ice.tagged(staged.table, dat_txn):
                continue
            add = bool(files) and not self._ice.referenced(staged.table, files)
            if not add and not erase:
                continue
            with self._ice.catalog.load_table(staged.table).transaction() as tx:
                if erase:
                    with warnings.catch_warnings():  # "did not match any records": fine
                        warnings.simplefilter("ignore", UserWarning)
                        tx.delete(_erase_filter(erase, dat_tx_time), snapshot_properties=properties)
                if add:
                    tx.add_files(files, snapshot_properties=properties)

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        tables = decode_staged(branch.payload)
        if not tables:
            return False  # without the payload there is nothing to look at: never guess
        return all(self._applied(t, dat_txn) for t in tables)

    def _applied(self, staged: StagedTable, dat_txn: uuid.UUID) -> bool:
        if self._ice.tagged(staged.table, dat_txn):
            return True
        files, erase = list(staged.files), list(staged.erase)
        if files and not self._ice.referenced(staged.table, files):
            return False
        # An erase that matched nothing leaves no snapshot. Without the tag it counts as
        # applied only when no other transaction's version of the entities is left: a
        # later write reads as "not applied", and finishing again is harmless.
        return not erase or not self._ice.versions_besides(staged.table, erase, dat_txn)

    def abort(self, branch: BranchRecord) -> None:
        """Delete the staged files (only ever called before a commit decision)."""
        for staged in decode_staged(branch.payload):
            files = list(staged.files)
            if files and not self._ice.referenced(staged.table, files):
                self._ice.delete_files(staged.table, files)

    def prepared_gids(self) -> list[str]:
        return []

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)


ADAPTER = Adapter(
    kind="iceberg",
    schemes=(
        Scheme("iceberg+rest", transport="rest", host="required"),
        Scheme("iceberg+sql", transport="sql"),
        Scheme("iceberg+glue", transport="glue"),
    ),
    open_store=lambda name, dsn: IcebergStore(dsn, name=name),
    query_help=(
        "a table name (namespace.table), with scan parameters (row_filter, selected_fields, limit)"
    ),
)
