"""Apache Iceberg adapter (``pyiceberg``), a one-phase branch committed after the decision.

Iceberg commits optimistically: a commit swaps the table's metadata pointer to
a new snapshot. A branch therefore splits the work:

- ``append(table, rows)`` buffers rows (mappings or a ``pyarrow.Table``);
- ``stage`` (before prepare, lock-free) converts each table's rows to its
  Arrow schema, adds ``dat_txn`` / ``dat_tx_time``, and writes one Parquet data
  file per table under the table's ``data/`` location. The files are invisible
  (no snapshot references them); the journal payload lists their paths (the
  shared format of ``datorium.core.staged``), so whoever finishes the branch
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

Tables written through datorium need ``dat_txn`` (``uuid`` or ``string``) and
``dat_tx_time`` (``long``) columns. Only appends are supported for now.

pyiceberg is imported on first use, so the driver stays an optional extra.
"""

import uuid
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from datorium.core.adapter import Adapter, Scheme
from datorium.core.dsn import parse_dsn
from datorium.core.journal import BranchRecord
from datorium.core.result import Result
from datorium.core.staged import StagedTable, decode_staged, encode_staged

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
            "the Iceberg adapter needs pyiceberg: pip install 'datorium[iceberg]'"
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


def _encode_payload(files: list[tuple[str, str]]) -> bytes:
    return encode_staged([StagedTable(t, (f,)) for t, f in files])


def _decode_payload(raw: bytes | None) -> list[tuple[str, list[str]]]:
    return [(t.table, list(t.files)) for t in decode_staged(raw)]


class IcebergStore:
    """One Iceberg catalog as a datorium store; ``catalog`` is the pyiceberg catalog."""

    two_phase = False

    def __init__(self, dsn: str, *, name: str = "iceberg") -> None:
        props = catalog_properties(dsn)
        self.name = name
        self._dsn = parse_dsn(dsn)
        self.catalog = _pyiceberg().catalog.load_catalog(name, **props)
        self._participant = IcebergParticipant(self)

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

    def read_only(self, table: str, params: Mapping[str, Any] | None = None) -> Result:
        """The same scan as ``execute``: a scan cannot write."""
        return self.execute(table, params)

    def branch(self, dat_txn: uuid.UUID) -> "IcebergBranch":
        return IcebergBranch(self, dat_txn)

    def participant(self) -> "IcebergParticipant":
        return self._participant

    def close(self) -> None:
        engine = getattr(self.catalog, "engine", None)  # SqlCatalog keeps a SQLAlchemy engine
        if engine is not None:
            engine.dispose()

    # --- used by the branch and the participant ------------------------------------------

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


class IcebergBranch:
    two_phase = False

    def __init__(self, store: IcebergStore, dat_txn: uuid.UUID) -> None:
        self.store = store.name
        self.dat_txn = dat_txn
        self._ice = store
        self._buffer: dict[str, list[Any]] = {}
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
                raise ValueError(f"{reserved} is set by datorium; leave it out of the rows")
        self._buffer.setdefault(table, []).append(part)

    def execute(self, table: str, params: Mapping[str, Any] | None = None) -> Result:
        """Scan the table. Buffered rows are not visible before the commit."""
        return self._ice.execute(table, params)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes:
        import pyarrow.parquet as pq

        for table_name, parts in self._buffer.items():
            table = self._ice.catalog.load_table(table_name)
            data = self._to_arrow(
                table_name, table.schema().as_arrow(), parts, dat_txn, dat_tx_time
            )
            path = f"{table.location()}/data/dat-{dat_txn}-{self.store}.parquet"
            with table.io.new_output(path).create(overwrite=True) as out:
                pq.write_table(data, out)
            self._written.append((table_name, path))
        payload = _encode_payload(self._written)
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
        properties = {_TXN_PROPERTY: str(dat_txn), _TIME_PROPERTY: str(dat_tx_time)}
        for table, files in _decode_payload(branch.payload):
            if self._ice.tagged(table, dat_txn) or self._ice.referenced(table, files):
                continue
            self._ice.catalog.load_table(table).add_files(files, snapshot_properties=properties)

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        tables = _decode_payload(branch.payload)
        if not tables:
            return False  # without the payload there is nothing to look at: never guess
        return all(
            self._ice.tagged(table, dat_txn) or self._ice.referenced(table, files)
            for table, files in tables
        )

    def abort(self, branch: BranchRecord) -> None:
        """Delete the staged files (only ever called before a commit decision)."""
        for table, files in _decode_payload(branch.payload):
            if not self._ice.referenced(table, files):
                self._ice.delete_files(table, files)

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
