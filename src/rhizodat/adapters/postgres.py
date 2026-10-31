"""PostgreSQL adapter (``psycopg``), a two-phase branch.

- ``PostgresStore``: one database as a store; native access via ``execute``.
- ``PostgresBranch``: one Unit of Work's open transaction. ``stage`` checks
  deferred constraints and writes the applied marker; ``prepare`` runs
  ``PREPARE TRANSACTION``.
- ``PostgresParticipant``: ``COMMIT PREPARED`` / ``ROLLBACK PREPARED`` and
  ``pg_prepared_xacts`` for the coordinator and recovery.
- ``PostgresJournal``: the journal table in PostgreSQL, compare-and-set by
  ``UPDATE … WHERE state = expected``.

Entity parts (``rhizodat.core.parts``) live in one table per part, named after
the entity type, with one row per version: ``dat_id``, ``dat_txn``,
``dat_tx_time`` (set at stage), ``dat_created``, ``dat_deleted`` and a typed
column per field (``jsonb`` for lists, dicts and anything without a native type).

Every branch inserts ``(dat_txn, dat_tx_time)`` into ``dat_applied`` inside its
own transaction, so "did this branch commit?" is one primary-key lookup,
whatever tables the application wrote. The server needs
``max_prepared_transactions > 0`` for meta-transactions over several stores.

psycopg is imported on first use, so the driver stays an optional extra.
"""

import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import unquote_plus

from rhizodat.core.adapter import Adapter, Scheme
from rhizodat.core.dsn import DSNError, parse_dsn
from rhizodat.core.errors import ErrorKind
from rhizodat.core.journal import (
    AppliedMigration,
    BranchRecord,
    JournalConflict,
    JournalRecord,
    State,
    check_transition,
    conflict_error,
    decode_branches,
    encode_branches,
    migration_recorded,
)
from rhizodat.core.parts import Part, Version
from rhizodat.core.result import Result

if TYPE_CHECKING:
    import psycopg

    from rhizodat.ingest.schema import Column
    from rhizodat.migrations.format import Step

__all__ = [
    "ADAPTER",
    "PostgresBranch",
    "PostgresJournal",
    "PostgresParticipant",
    "PostgresStore",
    "part_columns",
    "postgres_schema",
]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_RETRYABLE_SQLSTATES = {"40001", "40P01"}  # serialization failure, deadlock
_UNDEFINED_OBJECT = "42704"  # no prepared transaction with that gid

Isolation = Literal["read committed", "repeatable read", "serializable"]


def _psycopg() -> Any:  # noqa: ANN401 - the driver module, imported lazily
    try:
        import psycopg
    except ImportError as exc:
        raise ImportError(
            "the PostgreSQL adapter needs psycopg: pip install 'rhizodat[postgres]'"
        ) from exc
    return psycopg


def postgres_schema(raw: str, schema: str | None = None) -> tuple[str, str | None]:
    """The DSN for the driver and the schema of a PostgreSQL store or journal.

    The schema is the DSN's ``schema`` query parameter, or ``schema``; both given
    must agree. The returned DSN is ``raw`` without that parameter.
    """
    dsn = parse_dsn(raw)
    if dsn.store != "postgres":
        raise DSNError(f"not a PostgreSQL connection string: {dsn}")
    from_dsn = dsn.params.get("schema")
    for value in (from_dsn, schema):
        if value is not None and not _IDENTIFIER.match(value):
            raise DSNError(
                f"invalid schema name {value!r}: a letter or '_', then letters, digits and"
                " underscores, at most 63 characters"
            )
    if from_dsn is not None and schema is not None and from_dsn != schema:
        raise DSNError(f"the DSN names schema {from_dsn!r} but schema={schema!r} was passed")
    if from_dsn is None:
        return raw, schema
    base, _, query = raw.partition("?")
    kept = [p for p in query.split("&") if unquote_plus(p.partition("=")[0]) != "schema"]
    return (f"{base}?{'&'.join(kept)}" if kept else base), from_dsn


def _check_identifier(kind: str, value: str | None) -> None:
    if value is not None and not _IDENTIFIER.match(value):
        raise ValueError(f"invalid {kind} name {value!r}: use letters, digits and underscores")


def _is_retryable(error: BaseException) -> bool:
    if getattr(error, "sqlstate", None) in _RETRYABLE_SQLSTATES:
        return True
    pg = _psycopg()
    # Connection loss / server shutdown; constraint and syntax errors are DatabaseError subclasses
    # with a sqlstate and are not OperationalError.
    return isinstance(error, pg.OperationalError) and getattr(error, "sqlstate", None) is None


_DISK_FULL = "53100"


def _classify(error: BaseException) -> ErrorKind:
    state = getattr(error, "sqlstate", None)
    if state == _DISK_FULL:
        return ErrorKind.STORE_FULL
    if _is_retryable(error):
        return ErrorKind.TRANSIENT
    if isinstance(state, str) and state.startswith("23"):  # integrity constraint violation
        return ErrorKind.CONSTRAINT
    return ErrorKind.PERMANENT


def _literal(value: str) -> str:
    """SQL string literal for statements that take no parameters (gids: dat:<uuid>:<name>)."""
    return "'" + value.replace("'", "''") + "'"


# --- entity parts -----------------------------------------------------------------------

_SERVICE_COLUMNS = ("dat_id", "dat_txn", "dat_tx_time", "dat_created", "dat_deleted")
_DUPLICATE = {"42P07", "23505"}  # duplicate table / type: a concurrent CREATE TABLE won


def _column_type(annotation: object) -> str:
    """The PostgreSQL type of a field; ``jsonb`` when there is no native one."""
    import datetime as dt
    import decimal
    import types
    import typing

    from rhizodat.core.mapping import Ref

    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        return _column_type(args[0]) if len(args) == 1 else "jsonb"
    if annotation is Ref or origin is Ref:
        return "uuid"
    natives: dict[object, str] = {
        str: "text",
        bool: "boolean",
        int: "bigint",
        float: "double precision",
        decimal.Decimal: "numeric",
        dt.datetime: "timestamptz",
        dt.date: "date",
        uuid.UUID: "uuid",
        bytes: "bytea",
    }
    return natives.get(annotation, "jsonb")


def part_columns(part: Part) -> list[tuple[str, str]]:
    """``(field, PostgreSQL type)`` of each field of ``part``, in order: the columns
    of its table after the service columns (shared with the native core)."""
    return [(f, _column_type(part.types.get(f))) for f in part.fields]


# Imports: portable column types as PostgreSQL types.
_IMPORT_TYPES = {
    "boolean": "boolean",
    "int64": "bigint",
    "float64": "double precision",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "timestamptz",
    "string": "text",
}


def _quoted(name: str) -> str:
    """Any name as a quoted identifier."""
    if "\x00" in name:
        raise ValueError(f"cannot name a PostgreSQL column {name!r}: it holds a NUL")
    return '"' + name.replace('"', '""') + '"'


def _import_table(table: str) -> str:
    return ".".join(_quoted(part) for part in table.split(".", 1))


def _ident(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"cannot store {name!r} in PostgreSQL: not a plain identifier")
    return f'"{name}"'


class _PartSql:
    """The statements of one part's table."""

    def __init__(self, part: Part) -> None:
        self.part = part
        self.table = _ident(part.table)
        self.json = frozenset(f for f in part.fields if _column_type(part.types.get(f)) == "jsonb")
        self.columns = ", ".join(_ident(c) for c in (*_SERVICE_COLUMNS, *part.fields))

    def create(self) -> list[str]:
        fields = "".join(
            f", {_ident(f)} {_column_type(self.part.types.get(f))}" for f in self.part.fields
        )
        index = _ident(f"{self.part.table[:50]}_dat_time")
        return [
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            " dat_id uuid NOT NULL, dat_txn uuid NOT NULL, dat_tx_time bigint,"
            f" dat_created bigint, dat_deleted boolean NOT NULL DEFAULT false{fields},"
            " PRIMARY KEY (dat_id, dat_txn))",
            f"CREATE INDEX IF NOT EXISTS {index} ON {self.table} (dat_id, dat_tx_time)",
        ]

    def insert(self) -> str:
        names = (*_SERVICE_COLUMNS, *self.part.fields)
        updates = ", ".join(f"{_ident(c)} = EXCLUDED.{_ident(c)}" for c in names[3:])
        return (
            f"INSERT INTO {self.table} ({self.columns})"
            f" VALUES ({', '.join(['%s'] * len(names))})"
            f" ON CONFLICT (dat_id, dat_txn) DO UPDATE SET {updates}"
        )

    def params(
        self,
        dat_id: uuid.UUID,
        dat_txn: uuid.UUID,
        values: Mapping[str, Any],
        created: int | None,
        deleted: bool,
    ) -> list[Any]:
        unknown = set(values) - set(self.part.fields)
        if unknown:
            raise ValueError(
                f"{self.part.type_name}: {sorted(unknown)} are not fields of this part"
            )
        from psycopg.types.json import Jsonb

        from rhizodat.core.rows import encode_value

        out: list[Any] = [dat_id, dat_txn, None, created, deleted]
        for f in self.part.fields:
            value = values.get(f)
            out.append(Jsonb(encode_value(value)) if f in self.json else value)
        return out

    def select(self, where: str, order: str, limit: int | None = None) -> str:
        tail = f" LIMIT {limit}" if limit is not None else ""
        return f"SELECT {self.columns} FROM {self.table} WHERE {where} ORDER BY {order}{tail}"

    def version(self, row: Mapping[str, Any]) -> Version:
        from rhizodat.core.rows import decode_value

        values = {
            f: decode_value(row[f]) if f in self.json and row[f] is not None else row[f]
            for f in self.part.fields
        }
        return Version(
            dat_txn=row["dat_txn"],
            dat_tx_time=row["dat_tx_time"],
            created=row["dat_created"],
            deleted=row["dat_deleted"],
            values=values,
        )


class _Connector:
    def __init__(self, dsn: str, schema: str | None) -> None:
        self._dsn, self._schema = postgres_schema(dsn, schema)

    @property
    def schema(self) -> str | None:
        return self._schema

    def connect(self, *, autocommit: bool = False) -> "psycopg.Connection[Any]":
        pg = _psycopg()
        from psycopg.rows import dict_row

        conn = pg.connect(self._dsn, autocommit=autocommit, row_factory=dict_row)
        if self._schema is not None:
            conn.execute(f'SET search_path TO "{self._schema}", public')
            if not autocommit:
                conn.commit()
        return conn  # type: ignore[no-any-return]


def _result(cursor: Any) -> Result:  # noqa: ANN401 - psycopg cursor
    if cursor.description is None:
        return Result(iter(()), columns=(), raw=cursor)
    columns = tuple(c.name for c in cursor.description)
    return Result(iter(cursor.fetchall()), columns=columns, raw=cursor)


class PostgresStore:
    """One PostgreSQL database as a rhizodat store."""

    two_phase = True

    def __init__(
        self,
        dsn: str,
        *,
        name: str = "pg",
        schema: str | None = None,
        isolation: Isolation | None = None,
    ) -> None:
        self._dsn = parse_dsn(dsn)
        self._isolation = isolation
        if self._dsn.store != "postgres":
            raise ValueError(f"not a PostgreSQL connection string: {self._dsn}")
        self.name = name
        self._connector = _Connector(dsn, schema)
        self._participant: PostgresParticipant | None = None
        self._lock = threading.Lock()
        self._parts: dict[Part, _PartSql] = {}

    def __repr__(self) -> str:
        return f"PostgresStore({self.name!r}, {self._dsn.redacted()!r})"

    @property
    def applied_location(self) -> tuple[str, ...]:
        """Where this store's ``dat_applied`` table lives: server, database and schema.

        Two stores with the same location must not meet in one meta-transaction:
        both branches would insert the same ``dat_txn`` and the second would wait
        for the first.
        """
        d = self._dsn
        host = (d.host or d.params.get("host") or "").lower()
        port = str(d.port or d.params.get("port") or 5432)
        # No schema: the server's search_path, which writes to public unless a schema
        # named after the user exists.
        return ("postgres", host, port, d.database or "", self._connector.schema or "public")

    def connect(self, *, autocommit: bool = False) -> "psycopg.Connection[Any]":
        return self._connector.connect(autocommit=autocommit)

    def install(self) -> None:
        """Create the service table ``dat_applied`` if it does not exist."""
        with self.connect(autocommit=True) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dat_applied ("
                " dat_txn uuid PRIMARY KEY,"
                " dat_tx_time bigint NOT NULL,"
                " applied_at timestamptz NOT NULL DEFAULT now())"
            )

    def execute(self, sql: str, params: Mapping[str, Any] | Sequence[Any] | None = None) -> Result:
        """Native access in autocommit mode, outside any meta-transaction."""
        with self.connect(autocommit=True) as conn:
            return _result(conn.execute(sql, params))

    migration_kinds = frozenset({"sql"})

    def run_migration_step(self, kind: str, body: Any) -> None:  # noqa: ANN401 - TOML data
        """A ``sql`` step: its statements in one transaction (PostgreSQL DDL is
        transactional, so a step applies whole or not at all)."""
        from rhizodat.migrations.format import statements

        sqls = statements(kind, body)
        with self.connect() as conn, conn.transaction():
            for sql in sqls:
                conn.execute(sql.encode())

    # --- Loads -------------------------------------------------------------------------------

    def table_columns(self, table: str) -> list[str] | None:
        schema, dot, name = table.rpartition(".")
        rows = self.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = coalesce(%s, current_schema()) AND table_name = %s"
            " ORDER BY ordinal_position",
            (schema if dot else None, name),
        ).all()
        return [r["column_name"] for r in rows] or None

    def create_table_steps(
        self, table: str, columns: Sequence["Column"], *, order_by: Sequence[str] = ()
    ) -> list["Step"]:
        """``CREATE TABLE IF NOT EXISTS``; a column with no missing value is ``NOT NULL``.
        ``order_by`` is ignored (PostgreSQL tables have no sort key)."""
        from rhizodat.migrations.format import Step

        cols = ", ".join(
            f"{_quoted(c.name)} {_IMPORT_TYPES[c.type]}{'' if c.nullable else ' NOT NULL'}"
            for c in columns
        )
        return [
            Step(self.name, "sql", f"CREATE TABLE IF NOT EXISTS {_import_table(table)} ({cols})")
        ]

    def add_column_steps(self, table: str, columns: Sequence["Column"]) -> list["Step"]:
        from rhizodat.migrations.format import Step

        body = [
            f"ALTER TABLE {_import_table(table)} ADD COLUMN IF NOT EXISTS"
            f" {_quoted(c.name)} {_IMPORT_TYPES[c.type]}"
            for c in columns
        ]
        return [Step(self.name, "sql", body)]

    def load(self, table: str, batches: Iterable[Any], *, replace: bool = False) -> int:
        """``COPY`` every batch in one transaction (``replace``: ``TRUNCATE`` first, in it)."""
        rows = 0
        with self.connect() as conn, conn.transaction():
            if replace:
                conn.execute(f"TRUNCATE {_import_table(table)}".encode())
            for batch in batches:
                if not batch.num_rows:
                    continue
                names = ", ".join(_quoted(n) for n in batch.schema.names)
                copy_sql = f"COPY {_import_table(table)} ({names}) FROM STDIN"
                values = [col.to_pylist() for col in batch.columns]
                with conn.cursor().copy(copy_sql.encode()) as copy:
                    for row in zip(*values, strict=True):
                        copy.write_row(row)
                rows += batch.num_rows
        return rows

    def read_only(
        self, sql: str, params: Mapping[str, Any] | Sequence[Any] | None = None
    ) -> Result:
        """A native read the server guarantees cannot write: one statement in a
        ``READ ONLY`` transaction that is always rolled back.

        A first query fixes the mode (until then ``SET TRANSACTION READ WRITE``
        could lift it), and the statement is prepared on the server, which
        refuses several statements (``COMMIT; INSERT ...``); psycopg would send
        a string without parameters through the simple protocol, which runs them
        all. Without ``params`` a ``%`` is literal, as in ``execute``.
        """
        if params is None:
            sql, params = sql.replace("%", "%%"), ()
        with self.connect() as conn:
            conn.read_only = True
            try:
                conn.execute("SELECT 1")
                return _result(conn.execute(sql, params, prepare=True))
            finally:
                conn.rollback()

    def branch(self, dat_txn: uuid.UUID, *, isolation: Isolation | None = None) -> "PostgresBranch":
        """A new branch; ``isolation`` defaults to the store's (the server default if unset)."""
        return PostgresBranch(
            self.name,
            self._connector,
            dat_txn,
            isolation=isolation or self._isolation,
            parts=self._part_sql,
        )

    # --- entity parts (PartStore) ------------------------------------------------------

    def install_part(self, part: Part) -> None:
        """Create the part's table and index if missing (and ``dat_applied``)."""
        if part in self._parts:
            return
        sql = _PartSql(part)
        self.install()
        with self.connect(autocommit=True) as conn:
            for statement in sql.create():
                try:
                    conn.execute(statement)
                except Exception as exc:
                    if getattr(exc, "sqlstate", None) not in _DUPLICATE:
                        raise
        with self._lock:
            self._parts[part] = sql

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        sql = self._part_sql(part)
        where = "dat_id = %s AND dat_tx_time IS NOT NULL"
        params: list[Any] = [dat_id]
        if known_at is not None:
            where += " AND dat_tx_time <= %s"
            params.append(known_at)
        rows = self.execute(sql.select(where, "dat_tx_time DESC", 1), params).all()
        return sql.version(rows[0]) if rows else None

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        sql = self._part_sql(part)
        where = "dat_id = %s AND dat_tx_time IS NOT NULL"
        rows = self.execute(sql.select(where, "dat_tx_time"), [dat_id]).all()
        return [sql.version(r) for r in rows]

    def part_versions(
        self, part: Part, *, known_at: int | None = None
    ) -> list[tuple[uuid.UUID, Version]]:
        sql = self._part_sql(part)
        where = "dat_tx_time IS NOT NULL"
        params: list[Any] = []
        if known_at is not None:
            where += " AND dat_tx_time <= %s"
            params.append(known_at)
        rows = self.execute(sql.select(where, "dat_tx_time, dat_id"), params).all()
        return [(r["dat_id"], sql.version(r)) for r in rows]

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        sql = self._part_sql(part)
        rows = self.execute(
            "SELECT dat_id FROM (SELECT DISTINCT ON (dat_id) dat_id, dat_deleted, dat_tx_time"
            f" FROM {sql.table} WHERE dat_tx_time IS NOT NULL"
            " ORDER BY dat_id, dat_tx_time DESC) latest"
            " WHERE dat_deleted AND dat_tx_time < %s ORDER BY dat_id LIMIT %s",
            (deleted_before, max(limit, 0)),
        ).all()
        return [r["dat_id"] for r in rows]

    def _part_sql(self, part: Part) -> _PartSql:
        sql = self._parts.get(part)
        if sql is None:
            self.install_part(part)
            sql = self._parts[part]
        return sql

    def participant(self) -> "PostgresParticipant":
        with self._lock:
            if self._participant is None:
                self._participant = PostgresParticipant(self._connector)
            return self._participant

    def close(self) -> None:
        with self._lock:
            if self._participant is not None:
                self._participant.close()
                self._participant = None


class PostgresBranch:
    two_phase = True

    def __init__(
        self,
        store: str,
        connector: _Connector,
        dat_txn: uuid.UUID,
        *,
        isolation: Isolation | None = None,
        parts: "Callable[[Part], _PartSql] | None" = None,
    ) -> None:
        self.store = store
        self.dat_txn = dat_txn
        self._connector = connector
        self._conn = connector.connect()
        self._gid: str | None = None
        self._parts = parts
        self._written: dict[str, _PartSql] = {}  # tables this branch wrote versions to
        if isolation is not None:
            self._conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation.upper()}")

    def execute(self, sql: str, params: Mapping[str, Any] | Sequence[Any] | None = None) -> Result:
        """Run a statement inside this branch's open transaction."""
        return _result(self._conn.execute(sql, params))

    # --- entity parts (PartBranch) -----------------------------------------------------

    def write_part(
        self,
        part: Part,
        dat_id: uuid.UUID,
        values: Mapping[str, Any],
        *,
        created: int | None,
        deleted: bool = False,
    ) -> None:
        sql = self._sql(part)
        self._conn.execute(sql.insert(), sql.params(dat_id, self.dat_txn, values, created, deleted))
        self._written[sql.table] = sql

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        sql = self._sql(part)
        self._conn.execute(f"DELETE FROM {sql.table} WHERE dat_id = %s", (dat_id,))

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        sql = self._sql(part)
        # This branch's own versions have no dat_tx_time yet: they come first.
        query = sql.select("dat_id = %s", "dat_tx_time DESC NULLS FIRST", 1)
        rows = self._conn.execute(query, (dat_id,)).fetchall()
        return sql.version(rows[0]) if rows else None

    def _sql(self, part: Part) -> _PartSql:
        if self._parts is None:
            raise TypeError("this branch was made without its store: it cannot hold parts")
        return self._parts(part)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        for sql in self._written.values():
            self._conn.execute(
                f"UPDATE {sql.table} SET dat_tx_time = %s,"
                " dat_created = COALESCE(dat_created, %s) WHERE dat_txn = %s",
                (dat_tx_time, dat_tx_time, dat_txn),
            )
        self._conn.execute("SET CONSTRAINTS ALL IMMEDIATE")  # fail here, not at prepare
        self._conn.execute(
            "INSERT INTO dat_applied (dat_txn, dat_tx_time) VALUES (%s, %s)", (dat_txn, dat_tx_time)
        )
        return None

    def prepare(self, gid: str) -> None:
        self._conn.execute(f"PREPARE TRANSACTION {_literal(gid)}")
        self._gid = gid
        # The prepared transaction no longer belongs to this session: free the connection.
        self._conn.close()

    def commit_one_phase(self) -> None:
        try:
            self._conn.commit()
        finally:
            self._conn.close()

    def rollback(self) -> None:
        if self._gid is None:
            try:
                if not self._conn.closed:
                    self._conn.rollback()
            finally:
                self._conn.close()
            return
        with self._connector.connect(autocommit=True) as conn:
            try:
                conn.execute(f"ROLLBACK PREPARED {_literal(self._gid)}")
            except Exception as exc:
                if getattr(exc, "sqlstate", None) != _UNDEFINED_OBJECT:
                    raise

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)


class PostgresParticipant:
    """Resolves prepared gids. One autocommit connection, reopened after a failure."""

    def __init__(self, connector: _Connector) -> None:
        self._connector = connector
        self._lock = threading.Lock()
        self._conn: psycopg.Connection[Any] | None = None

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        try:
            self._run(f"COMMIT PREPARED {_literal(branch.gid)}")
        except Exception as exc:
            if getattr(exc, "sqlstate", None) != _UNDEFINED_OBJECT:
                raise
            if not self.is_applied(branch, dat_txn):
                raise RuntimeError(
                    f"heuristic outcome: {branch.gid} is gone but {dat_txn} was never applied"
                ) from exc

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        return bool(self._run("SELECT 1 FROM dat_applied WHERE dat_txn = %s", (dat_txn,)))

    def abort(self, branch: BranchRecord) -> None:
        try:
            self._run(f"ROLLBACK PREPARED {_literal(branch.gid)}")
        except Exception as exc:
            if getattr(exc, "sqlstate", None) != _UNDEFINED_OBJECT:
                raise

    def prepared_gids(self) -> list[str]:
        rows = self._run(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE database = current_database() ORDER BY prepared"
        )
        return [r["gid"] for r in rows]

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)

    def classify(self, error: BaseException) -> ErrorKind:
        return _classify(error)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def _run(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if self._conn is None or self._conn.closed:
                self._conn = self._connector.connect(autocommit=True)
            try:
                cursor = self._conn.execute(sql, params)
                return list(cursor.fetchall()) if cursor.description else []
            except _psycopg().OperationalError:
                self._conn.close()
                self._conn = None
                raise


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


class PostgresJournal:
    """The journal in PostgreSQL: shared and durable, so recovery can run on any host."""

    _COLUMNS = "dat_txn, state, dat_tx_time, branches, created_ms, updated_ms"

    def __init__(
        self,
        dsn: str,
        *,
        schema: str | None = None,
        table: str = "dat_journal",
        wall_ms: Callable[[], int] = _system_ms,
    ) -> None:
        _check_identifier("table", table)
        self._connector = _Connector(dsn, schema)
        self._table = table
        self._wall_ms = wall_ms
        self._lock = threading.Lock()
        self._conn = self._connector.connect(autocommit=True)
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            " dat_txn uuid PRIMARY KEY,"
            " state text NOT NULL,"
            " dat_tx_time bigint,"
            " branches text NOT NULL,"
            " created_ms bigint NOT NULL,"
            " updated_ms bigint NOT NULL)"
        )
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS {table}_pending ON {table} (state, updated_ms)"
        )
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table}_migrations ("
            " id bigint PRIMARY KEY,"
            " name text NOT NULL,"
            " checksum text NOT NULL,"
            " dat_tx_time bigint NOT NULL)"
        )

    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord:
        now = self._wall_ms()
        rows = self._run(
            f"INSERT INTO {self._table} ({self._COLUMNS}) VALUES (%s, %s, NULL, %s, %s, %s) "
            "ON CONFLICT (dat_txn) DO NOTHING RETURNING dat_txn",
            (dat_txn, State.ACTIVE.value, encode_branches(branches), now, now),
        )
        if not rows:
            raise JournalConflict(f"meta-transaction {dat_txn} is already journaled")
        return JournalRecord(dat_txn, State.ACTIVE, tuple(branches), None, now, now)

    def transition(
        self,
        dat_txn: uuid.UUID,
        expected: State,
        new: State,
        *,
        dat_tx_time: int | None = None,
    ) -> JournalRecord:
        check_transition(expected, new, dat_tx_time)
        rows = self._run(
            f"UPDATE {self._table} SET state = %s, dat_tx_time = COALESCE(%s, dat_tx_time), "
            f"updated_ms = %s WHERE dat_txn = %s AND state = %s RETURNING {self._COLUMNS}",
            (new.value, dat_tx_time, self._wall_ms(), dat_txn, expected.value),
        )
        if rows:
            return self._record(rows[0])
        current = self.get(dat_txn)
        raise conflict_error(dat_txn, expected, current.state if current else None)

    def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
        rows = self._run(
            f"SELECT {self._COLUMNS} FROM {self._table} WHERE dat_txn = %s", (dat_txn,)
        )
        return self._record(rows[0]) if rows else None

    def pending(self, *, updated_before_ms: int, limit: int | None = None) -> list[JournalRecord]:
        rows = self._run(
            f"SELECT {self._COLUMNS} FROM {self._table} "
            "WHERE state IN ('ACTIVE', 'PREPARED', 'COMMITTED') AND updated_ms < %s "
            "ORDER BY updated_ms, created_ms LIMIT %s",
            (updated_before_ms, limit),
        )
        return [self._record(r) for r in rows]

    def forget(self, dat_txn: uuid.UUID) -> None:
        record = self.get(dat_txn)
        if record is None:
            return
        if record.state not in (State.ABORTED, State.DONE):
            raise ValueError(f"cannot forget meta-transaction {dat_txn} in state {record.state}")
        self._run(
            f"DELETE FROM {self._table} WHERE dat_txn = %s AND state IN ('ABORTED', 'DONE')",
            (dat_txn,),
        )

    def applied_migrations(self) -> list[AppliedMigration]:
        rows = self._run(
            f"SELECT id, name, checksum, dat_tx_time FROM {self._table}_migrations ORDER BY id",
            (),
        )
        return [AppliedMigration(r["id"], r["name"], r["checksum"], r["dat_tx_time"]) for r in rows]

    def record_migration(self, applied: AppliedMigration) -> None:
        rows = self._run(
            f"INSERT INTO {self._table}_migrations (id, name, checksum, dat_tx_time)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING RETURNING id",
            (applied.id, applied.name, applied.checksum, applied.dat_tx_time),
        )
        if not rows:
            raise migration_recorded(applied.id)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _run(self, sql: str, params: Sequence[Any]) -> list[dict[str, Any]]:
        with self._lock:
            if self._conn.closed:
                self._conn = self._connector.connect(autocommit=True)
            cursor = self._conn.execute(sql, params)
            return list(cursor.fetchall()) if cursor.description else []

    @staticmethod
    def _record(row: Mapping[str, Any]) -> JournalRecord:
        return JournalRecord(
            dat_txn=row["dat_txn"],
            state=State(row["state"]),
            branches=decode_branches(row["branches"]),
            dat_tx_time=row["dat_tx_time"],
            created_ms=row["created_ms"],
            updated_ms=row["updated_ms"],
        )


ADAPTER = Adapter(
    kind="postgres",
    schemes=(Scheme("postgresql", default_port=5432), Scheme("postgres", default_port=5432)),
    open_store=lambda name, dsn: PostgresStore(dsn, name=name),
    open_journal=lambda dsn, wall_ms: PostgresJournal(dsn, wall_ms=wall_ms),
    query_help="SQL with %s or %(name)s placeholders",
)
