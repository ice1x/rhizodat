"""ClickHouse adapter (``clickhouse-connect``), a one-phase branch applied after the decision.

ClickHouse has no transactions. A branch therefore only buffers rows:

- ``insert(table, rows)`` collects rows per table, nothing reaches the server;
- ``stage`` (before prepare, lock-free) checks every batch against the table
  schema and the size limit, adds ``dat_txn`` / ``dat_tx_time`` to each row and
  returns the batches as the journal payload, so whoever finishes the branch
  (the coordinator, or recovery on another host) has the rows;
- after the decision the participant inserts each batch, then writes the
  applied marker ``(dat_txn, dat_tx_time)`` into ``dat_applied``.

Finishing is idempotent without relying on server-side deduplication (off by
default on non-replicated MergeTree): before inserting a batch the participant
counts the table's rows with this ``dat_txn``. None → insert; all → skip; some
(a crash left part of an insert, e.g. one partition) → delete them and insert
again. Each insert also carries ``insert_deduplication_token`` for replicated
tables. A batch is one INSERT of at most ``max_rows_per_table`` rows, below the
server's ``max_insert_block_size``.

Tables written through rhizodat need ``dat_txn UUID`` and ``dat_tx_time Int64``
columns; a skip index on ``dat_txn`` keeps the idempotency check cheap.

Entity parts (``PartStore``) live in append-only ``MergeTree`` tables ordered by
``(dat_id, dat_tx_time)``, one per entity type: every version is one row. A
branch keeps one version per entity; stage turns them into insert batches. An
erase travels as an erase batch and deletes the entity's versions older than
its own transaction, so a late erase (recovery) never removes a later write.
Fields without an exact native type (``Decimal``, ``bytes``, lists, dicts) are
tagged JSON in a ``String`` column.

clickhouse-connect is imported on first use, so the driver stays an optional extra.
"""

import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from rhizodat.core.adapter import Adapter, Scheme
from rhizodat.core.dsn import parse_dsn
from rhizodat.core.errors import ErrorKind
from rhizodat.core.journal import BranchRecord
from rhizodat.core.parts import Part, Version
from rhizodat.core.result import Result
from rhizodat.core.rows import Batch, decode_batches, decode_value, encode_batches, encode_value

if TYPE_CHECKING:
    from rhizodat.ingest.schema import Column
    from rhizodat.migrations.format import Step

__all__ = [
    "ADAPTER",
    "ClickHouseBranch",
    "ClickHouseParticipant",
    "ClickHouseStore",
    "part_columns",
]

_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
_RESERVED = ("dat_txn", "dat_tx_time")
_ROW_STATEMENTS = ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXISTS", "EXPLAIN")
# TIMEOUT_EXCEEDED, TOO_MANY_SIMULTANEOUS_QUERIES, SOCKET_TIMEOUT, NETWORK_ERROR,
# TABLE_IS_READ_ONLY (replica lost Keeper), TOO_MANY_PARTS, UNKNOWN_STATUS_OF_INSERT,
# KEEPER_EXCEPTION.
_RETRYABLE_CODES = {159, 202, 209, 210, 242, 252, 319, 999}


def _driver() -> Any:  # noqa: ANN401 - the driver module, imported lazily
    try:
        import clickhouse_connect
    except ImportError as exc:
        raise ImportError(
            "the ClickHouse adapter needs clickhouse-connect: pip install 'rhizodat[clickhouse]'"
        ) from exc
    return clickhouse_connect


def _quote(table: str) -> str:
    return ".".join(f"`{part}`" for part in table.split("."))


_SERVICE = ("dat_id", "dat_created", "dat_deleted")

# Imports: portable column types as ClickHouse types.
_IMPORT_TYPES = {
    "boolean": "Bool",
    "int64": "Int64",
    "float64": "Float64",
    "date": "Date32",
    "timestamp": "DateTime64(6)",
    "timestamptz": "DateTime64(6, 'UTC')",
    "string": "String",
}


def _name(name: str) -> str:
    """Any column or table name, quoted."""
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _import_table(table: str) -> str:
    return ".".join(_name(part) for part in table.split(".", 1))


def _import_column(c: "Column", *, nullable: bool | None = None) -> str:
    t = _IMPORT_TYPES[c.type]
    if c.nullable if nullable is None else nullable:
        t = f"Nullable({t})"
    if c.low_cardinality and c.type == "string":
        t = f"LowCardinality({t})"
    return f"{_name(c.name)} {t}"


def _column_type(annotation: object) -> str | None:
    """The native ClickHouse type of a field, or ``None``: tagged JSON in a ``String``."""
    import datetime as dt
    import types
    import typing

    from rhizodat.core.mapping import Ref

    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        return _column_type(args[0]) if len(args) == 1 else None
    if annotation is Ref or origin is Ref:
        return "UUID"
    natives: dict[object, str] = {
        str: "String",
        bool: "Bool",
        int: "Int64",
        float: "Float64",
        dt.datetime: "DateTime64(6, 'UTC')",
        dt.date: "Date32",
        uuid.UUID: "UUID",
    }
    return natives.get(annotation)


def part_columns(part: Part) -> list[tuple[str, str]]:
    """``(field, ClickHouse type)`` of each field of ``part``, in order (``json`` for
    tagged JSON in a ``String``): the columns of its table (shared with the native
    core)."""
    return [(f, _column_type(part.types.get(f)) or "json") for f in part.fields]


class _PartTable:
    """How one part is stored: its table, columns and value conversions."""

    def __init__(self, part: Part) -> None:
        self.part = part
        self.table = part.table
        self.json = frozenset(f for f in part.fields if _column_type(part.types.get(f)) is None)
        self.times = frozenset(
            f for f in part.fields if (_column_type(part.types.get(f)) or "").startswith("DateTime")
        )
        self.columns = (*_SERVICE, *part.fields, *_RESERVED)

    def create(self) -> str:
        fields = "".join(
            f", `{f}` Nullable({_column_type(self.part.types.get(f)) or 'String'})"
            for f in self.part.fields
        )
        return (
            f"CREATE TABLE IF NOT EXISTS `{self.table}` ("
            f" dat_id UUID, dat_created Int64, dat_deleted Bool{fields},"
            " dat_txn UUID, dat_tx_time Int64,"
            " INDEX dat_txn_idx dat_txn TYPE bloom_filter GRANULARITY 4"
            ") ENGINE = MergeTree ORDER BY (dat_id, dat_tx_time)"
        )

    def row(self, dat_id: uuid.UUID, version: Version, dat_tx_time: int) -> list[Any]:
        values = [
            json.dumps(encode_value(version.values.get(f)), separators=(",", ":"))
            if f in self.json and version.values.get(f) is not None
            else version.values.get(f)
            for f in self.part.fields
        ]
        created = version.created if version.created is not None else dat_tx_time
        return [dat_id, created, version.deleted, *values, version.dat_txn, dat_tx_time]

    def select(self) -> str:
        return ", ".join(f"`{c}`" for c in self.columns)

    def version(self, row: Mapping[str, Any]) -> Version:
        values = {f: self._value(f, row[f]) for f in self.part.fields}
        return Version(
            dat_txn=row["dat_txn"],
            dat_tx_time=row["dat_tx_time"],
            created=row["dat_created"],
            deleted=bool(row["dat_deleted"]),
            values=values,
        )

    def _value(self, field: str, raw: Any) -> Any:  # noqa: ANN401 - any stored value
        if raw is None:
            return None
        if field in self.json:
            return decode_value(json.loads(raw))
        if field in self.times and raw.tzinfo is None:  # the driver drops the column's zone
            from datetime import UTC

            return raw.replace(tzinfo=UTC)
        return raw


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, ConnectionError | TimeoutError):
        return True
    from clickhouse_connect.driver.exceptions import OperationalError

    if isinstance(error, OperationalError):  # connection refused / reset, HTTP failures
        return True
    return getattr(error, "code", None) in _RETRYABLE_CODES


_NOT_ENOUGH_SPACE = 243
_VIOLATED_CONSTRAINT = 469


def _classify(error: BaseException) -> ErrorKind:
    code = getattr(error, "code", None)
    if code == _NOT_ENOUGH_SPACE:
        return ErrorKind.STORE_FULL
    if _is_retryable(error):
        return ErrorKind.TRANSIENT
    if code == _VIOLATED_CONSTRAINT:
        return ErrorKind.CONSTRAINT
    return ErrorKind.PERMANENT


class ClickHouseStore:
    """One ClickHouse database as a rhizodat store."""

    two_phase = False

    def __init__(
        self, dsn: str, *, name: str = "clickhouse", max_rows_per_table: int = 100_000
    ) -> None:
        parsed = parse_dsn(dsn)
        if parsed.store != "clickhouse":
            raise ValueError(f"not a clickhouse connection string: {parsed}")
        if max_rows_per_table < 1:
            raise ValueError("max_rows_per_table must be >= 1")
        self.name = name
        self.max_rows_per_table = max_rows_per_table
        self._dsn = parsed
        secure = parsed.params.get("secure", "").lower() in ("1", "true", "yes")
        self._client = _driver().get_client(
            host=parsed.host,
            port=parsed.port,
            username=parsed.user or "default",
            password=parsed.password or "",
            database=parsed.database or "default",
            interface="https" if secure else "http",
            autogenerate_session_id=False,  # sessions serialise queries; none are needed
        )
        self._participant = ClickHouseParticipant(self)
        self._parts: dict[Part, _PartTable] = {}

    def __repr__(self) -> str:
        return f"ClickHouseStore({self.name!r}, {self._dsn.redacted()!r})"

    @property
    def applied_location(self) -> tuple[str, ...]:
        """Where this store's ``dat_applied`` table lives: server and database.

        Two stores with the same location must not meet in one meta-transaction:
        one store's marker would make the other's branch look applied.
        """
        d = self._dsn
        return ("clickhouse", (d.host or "").lower(), str(d.port), d.database or "default")

    def install(self) -> None:
        """Create the service table ``dat_applied`` if it does not exist."""
        self._client.command(
            "CREATE TABLE IF NOT EXISTS dat_applied ("
            " dat_txn UUID, dat_tx_time Int64, applied_at DateTime64(3) DEFAULT now64(3)"
            ") ENGINE = ReplacingMergeTree ORDER BY dat_txn"
        )

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> Result:
        """Native SQL outside any meta-transaction; ``{name:Type}`` placeholders bind ``params``."""
        words = query.lstrip().split(None, 1)
        if words and words[0].upper() in _ROW_STATEMENTS:
            r = self._client.query(query, parameters=dict(params) if params else None)
            columns = tuple(r.column_names)
            return Result(iter(r.named_results()), columns=columns, raw=r)
        self._client.command(query, parameters=dict(params) if params else None)
        return Result(iter(()), columns=())

    migration_kinds = frozenset({"sql"})

    def run_migration_step(self, kind: str, body: Any) -> None:  # noqa: ANN401 - TOML data
        """A ``sql`` step: one statement per request (ClickHouse runs one at a time and
        has no transactional DDL; use ``IF NOT EXISTS`` / ``IF EXISTS``)."""
        from rhizodat.migrations.format import statements

        for sql in statements(kind, body):
            self._client.command(sql)

    def read_only(self, query: str, params: Mapping[str, Any] | None = None) -> Result:
        """A native read the server guarantees cannot write (``readonly = 2``: no writes,
        no DDL; settings may still change for the query)."""
        r = self._client.query(
            query, parameters=dict(params) if params else None, settings={"readonly": 2}
        )
        return Result(iter(r.named_results()), columns=tuple(r.column_names), raw=r)

    def branch(self, dat_txn: uuid.UUID) -> "ClickHouseBranch":
        return ClickHouseBranch(self, dat_txn)

    def participant(self) -> "ClickHouseParticipant":
        return self._participant

    # --- Loads -------------------------------------------------------------------------------

    def _where(self, table: str) -> tuple[str, dict[str, str]]:
        """A ``system.*`` filter for ``table`` (``db.table`` or one in the current database)."""
        db, dot, name = table.rpartition(".")
        if dot:
            return "database = {db:String} AND table = {t:String}", {"db": db, "t": name}
        return "database = currentDatabase() AND table = {t:String}", {"t": name}

    def table_columns(self, table: str) -> list[str] | None:
        where, params = self._where(table)
        r = self._client.query(
            f"SELECT name FROM system.columns WHERE {where} ORDER BY position", parameters=params
        )
        names = [row[0] for row in r.result_rows]
        return names or None

    def create_table_steps(
        self, table: str, columns: Sequence["Column"], *, order_by: Sequence[str] = ()
    ) -> list["Step"]:
        """``CREATE TABLE IF NOT EXISTS`` a MergeTree sorted by ``order_by`` (none: ``tuple()``).
        A sort key column must not be nullable."""
        from rhizodat.migrations.format import Step

        by_name = {c.name: c for c in columns}
        for key in order_by:
            if key not in by_name:
                raise ValueError(f"sort key: no column {key} (columns: {', '.join(by_name)})")
            if by_name[key].nullable:
                raise ValueError(
                    f"sort key column {key} is nullable (the file has missing values):"
                    " ClickHouse refuses a nullable sort key; pick another column"
                )
        key = f"({', '.join(_name(k) for k in order_by)})" if order_by else "tuple()"
        body = (
            f"CREATE TABLE IF NOT EXISTS {_import_table(table)}"
            f" ({', '.join(_import_column(c) for c in columns)})"
            f" ENGINE = MergeTree ORDER BY {key}"
        )
        return [Step(self.name, "sql", body)]

    def add_column_steps(self, table: str, columns: Sequence["Column"]) -> list["Step"]:
        from rhizodat.migrations.format import Step

        body = [
            f"ALTER TABLE {_import_table(table)} ADD COLUMN IF NOT EXISTS"
            f" {_import_column(c, nullable=True)}"
            for c in columns
        ]
        return [Step(self.name, "sql", body)]

    def load(self, table: str, batches: Iterable[Any], *, replace: bool = False) -> int:
        """Load into a staging table of the same structure, then attach its partitions to
        ``table`` (``replace``: exchange the two tables). Attaching a partition is atomic, so
        a table without ``PARTITION BY`` (one partition) gets all rows or none; with
        several partitions each one is. The staging table is always dropped."""
        import pyarrow as pa

        db, dot, name = table.rpartition(".")
        stage_name = f"{name}__rhizodat_import_{uuid.uuid4().hex[:12]}"
        stage = f"{db}.{stage_name}" if dot else stage_name
        self._client.command(f"CREATE TABLE {_import_table(stage)} AS {_import_table(table)}")
        try:
            rows = 0
            for batch in batches:
                if batch.num_rows:
                    self._client.insert_arrow(
                        stage_name, pa.Table.from_batches([batch]), database=db or None
                    )
                    rows += batch.num_rows
            if replace:
                self._client.command(
                    f"EXCHANGE TABLES {_import_table(table)} AND {_import_table(stage)}"
                )
            else:
                where, params = self._where(stage)
                parts = self._client.query(
                    f"SELECT DISTINCT partition_id FROM system.parts WHERE {where} AND active",
                    parameters=params,
                ).result_rows
                for (partition,) in parts:
                    self._client.command(
                        f"ALTER TABLE {_import_table(table)} ATTACH PARTITION ID"
                        f" {{p:String}} FROM {_import_table(stage)}",
                        parameters={"p": partition},
                    )
            return rows
        finally:
            self._client.command(f"DROP TABLE IF EXISTS {_import_table(stage)} SYNC")

    def free_space(self) -> int | None:
        """The least free space of the server's disks (``system.disks``), in bytes."""
        rows = self._rows("SELECT min(free_space) AS free FROM system.disks", {})
        return int(rows[0]["free"]) if rows and rows[0]["free"] is not None else None

    def close(self) -> None:
        self._client.close()

    # --- PartStore -------------------------------------------------------------------------

    def install_part(self, part: Part) -> None:
        table = _PartTable(part)
        self._client.command(table.create())
        self._parts[part] = table

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        table = self.part_table(part)
        bound = "" if known_at is None else " AND dat_tx_time <= {k:Int64}"
        rows = self._rows(
            f"SELECT {table.select()} FROM `{table.table}` WHERE dat_id = {{i:UUID}}{bound}"
            " ORDER BY dat_tx_time DESC LIMIT 1",
            {"i": dat_id, "k": known_at},
        )
        return table.version(rows[0]) if rows else None

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        table = self.part_table(part)
        rows = self._rows(
            f"SELECT {table.select()} FROM `{table.table}` WHERE dat_id = {{i:UUID}}"
            " ORDER BY dat_tx_time",
            {"i": dat_id},
        )
        return [table.version(r) for r in rows]

    def part_versions(
        self, part: Part, *, known_at: int | None = None
    ) -> list[tuple[uuid.UUID, Version]]:
        table = self.part_table(part)
        where = "" if known_at is None else " WHERE dat_tx_time <= {k:Int64}"
        rows = self._rows(
            f"SELECT {table.select()} FROM `{table.table}`{where} ORDER BY dat_tx_time, dat_id",
            {} if known_at is None else {"k": known_at},
        )
        return [(r["dat_id"], table.version(r)) for r in rows]

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        table = self.part_table(part)
        rows = self._rows(
            f"SELECT dat_id FROM `{table.table}` GROUP BY dat_id"
            " HAVING argMax(dat_deleted, dat_tx_time) AND max(dat_tx_time) < {b:Int64}"
            " ORDER BY dat_id LIMIT {n:UInt64}",
            {"b": deleted_before, "n": limit},
        )
        return [r["dat_id"] for r in rows]

    def part_table(self, part: Part) -> _PartTable:
        table = self._parts.get(part)
        if table is None:
            self.install_part(part)
            table = self._parts[part]
        return table

    def _rows(self, query: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        return list(self._client.query(query, parameters=dict(params)).named_results())

    # --- used by the branch and the participant ------------------------------------------

    def columns(self, table: str) -> list[str]:
        r = self._client.query(f"DESCRIBE TABLE {_quote(table)}")
        return [row[0] for row in r.result_rows]

    def apply(self, gid: str, batches: list[Batch], dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        """Erase, insert every batch and then the marker, skipping what is already there."""
        if self.is_applied(dat_txn):
            return
        for batch in batches:
            if batch.erase:
                self._client.command(
                    f"DELETE FROM {_quote(batch.table)}"
                    " WHERE dat_id IN {ids:Array(UUID)} AND dat_tx_time < {ts:Int64}",
                    parameters={"ids": [row[0] for row in batch.rows], "ts": dat_tx_time},
                    settings={"lightweight_deletes_sync": 2},
                )
        for i, batch in enumerate(batches):
            if batch.erase:
                continue
            present = self._count(batch.table, dat_txn)
            if present >= len(batch.rows):
                continue
            if present:
                self._client.command(
                    f"DELETE FROM {_quote(batch.table)} WHERE dat_txn = {{t:UUID}}",
                    parameters={"t": dat_txn},
                    settings={"lightweight_deletes_sync": 2},
                )
            self._client.insert(
                batch.table,
                batch.rows,
                column_names=list(batch.columns),
                settings={"insert_deduplication_token": f"{gid}:{i}", "async_insert": 0},
            )
        if not self.is_applied(dat_txn):
            self._client.insert(
                "dat_applied",
                [[dat_txn, dat_tx_time]],
                column_names=["dat_txn", "dat_tx_time"],
                settings={"insert_deduplication_token": f"{gid}:applied", "async_insert": 0},
            )

    def is_applied(self, dat_txn: uuid.UUID) -> bool:
        return self._count("dat_applied", dat_txn) > 0

    def _count(self, table: str, dat_txn: uuid.UUID) -> int:
        r = self._client.query(
            f"SELECT count() FROM {_quote(table)} WHERE dat_txn = {{t:UUID}}",
            parameters={"t": dat_txn},
        )
        return int(r.result_rows[0][0])


class ClickHouseBranch:
    two_phase = False

    def __init__(self, store: ClickHouseStore, dat_txn: uuid.UUID) -> None:
        self.store = store.name
        self.dat_txn = dat_txn
        self._ch = store
        self._buffer: dict[str, tuple[tuple[str, ...], list[list[Any]]]] = {}
        self._versions: dict[Part, dict[uuid.UUID, Version]] = {}
        self._erased: dict[Part, set[uuid.UUID]] = {}
        self._staged: tuple[list[Batch], int] | None = None

    def insert(self, table: str, rows: Iterable[Mapping[str, Any]]) -> None:
        """Buffer rows for ``table``; they are inserted only if the meta-transaction commits."""
        if not _TABLE.match(table):
            raise ValueError(f"invalid table name {table!r}: [database.]table, letters, digits, _")
        rows = list(rows)
        if not rows:
            return
        columns = tuple(rows[0])
        for reserved in _RESERVED:
            if reserved in columns:
                raise ValueError(f"{reserved} is set by rhizodat; leave it out of the rows")
        known, buffered = self._buffer.get(table, (columns, []))
        for row in rows:
            if tuple(row) != known and set(row) != set(known):
                raise ValueError(f"rows for {table!r} must have the same columns: {known}")
            buffered.append([row[c] for c in known])
        self._buffer[table] = (known, buffered)

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> Result:
        """Read from the store. Buffered rows are not visible: ClickHouse has no transaction."""
        return self._ch.execute(query, params)

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
        return self._ch.read_part(part, dat_id)

    # --- commit ----------------------------------------------------------------------------

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes:
        batches = self._part_batches(dat_tx_time)
        for table, (columns, rows) in self._buffer.items():
            if len(rows) > self._ch.max_rows_per_table:
                raise ValueError(
                    f"{len(rows)} rows for {table!r} exceed max_rows_per_table="
                    f"{self._ch.max_rows_per_table}"
                )
            existing = set(self._ch.columns(table))
            missing = [c for c in (*columns, *_RESERVED) if c not in existing]
            if missing:
                raise ValueError(f"table {table!r} has no column(s) {', '.join(missing)}")
            batches.append(
                Batch(table, (*columns, *_RESERVED), [[*r, dat_txn, dat_tx_time] for r in rows])
            )
        payload = encode_batches(batches)
        self._staged = (batches, dat_tx_time)
        return payload

    def _part_batches(self, dat_tx_time: int) -> list[Batch]:
        batches = []
        for part, erased in self._erased.items():
            table = self._ch.part_table(part)
            batches.append(Batch(table.table, ("dat_id",), [[i] for i in erased], erase=True))
        for part, versions in self._versions.items():
            if not versions:
                continue
            if len(versions) > self._ch.max_rows_per_table:
                raise ValueError(
                    f"{len(versions)} rows for {part.table!r} exceed max_rows_per_table="
                    f"{self._ch.max_rows_per_table}"
                )
            table = self._ch.part_table(part)
            rows = [table.row(i, v, dat_tx_time) for i, v in versions.items()]
            batches.append(Batch(table.table, table.columns, rows))
        return batches

    def prepare(self, gid: str) -> None:
        raise TypeError("a ClickHouse branch is not two-phase; it is applied after the decision")

    def commit_one_phase(self) -> None:
        if self._staged is None:
            raise RuntimeError("commit_one_phase before stage")
        batches, dat_tx_time = self._staged
        self._ch.apply(f"dat:{self.dat_txn}:{self.store}", batches, self.dat_txn, dat_tx_time)

    def rollback(self) -> None:
        self._buffer.clear()
        self._versions.clear()
        self._erased.clear()
        self._staged = None

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)


class ClickHouseParticipant:
    def __init__(self, store: ClickHouseStore) -> None:
        self._ch = store

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        batches = decode_batches(branch.payload) if branch.payload else []
        self._ch.apply(branch.gid, batches, dat_txn, dat_tx_time)

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        return self._ch.is_applied(dat_txn)

    def abort(self, branch: BranchRecord) -> None:
        """Nothing to undo: rows reach ClickHouse only after a commit decision."""

    def prepared_gids(self) -> list[str]:
        return []

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)


ADAPTER = Adapter(
    kind="clickhouse",
    schemes=(Scheme("clickhouse", default_port=8123, host="required"),),
    open_store=lambda name, dsn: ClickHouseStore(dsn, name=name),
    query_help="SQL with {name:Type} placeholders",
)
