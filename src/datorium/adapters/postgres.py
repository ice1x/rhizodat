"""PostgreSQL adapter (``psycopg``), a two-phase branch.

- ``PostgresStore``: one database as a store; native access via ``execute``.
- ``PostgresBranch``: one Unit of Work's open transaction. ``stage`` checks
  deferred constraints and writes the applied marker; ``prepare`` runs
  ``PREPARE TRANSACTION``.
- ``PostgresParticipant``: ``COMMIT PREPARED`` / ``ROLLBACK PREPARED`` and
  ``pg_prepared_xacts`` for the coordinator and recovery.
- ``PostgresJournal``: the journal table in PostgreSQL, compare-and-set by
  ``UPDATE … WHERE state = expected``.

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
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import unquote_plus

from datorium.core.adapter import Adapter, Scheme
from datorium.core.dsn import DSNError, parse_dsn
from datorium.core.journal import (
    BranchRecord,
    JournalConflict,
    JournalRecord,
    State,
    check_transition,
    conflict_error,
    decode_branches,
    encode_branches,
)
from datorium.core.result import Result

if TYPE_CHECKING:
    import psycopg

__all__ = [
    "ADAPTER",
    "PostgresBranch",
    "PostgresJournal",
    "PostgresParticipant",
    "PostgresStore",
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
            "the PostgreSQL adapter needs psycopg: pip install 'datorium[postgres]'"
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


def _literal(value: str) -> str:
    """SQL string literal for statements that take no parameters (gids: dat:<uuid>:<name>)."""
    return "'" + value.replace("'", "''") + "'"


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
    """One PostgreSQL database as a datorium store."""

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
            self.name, self._connector, dat_txn, isolation=isolation or self._isolation
        )

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
    ) -> None:
        self.store = store
        self.dat_txn = dat_txn
        self._connector = connector
        self._conn = connector.connect()
        self._gid: str | None = None
        if isolation is not None:
            self._conn.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation.upper()}")

    def execute(self, sql: str, params: Mapping[str, Any] | Sequence[Any] | None = None) -> Result:
        """Run a statement inside this branch's open transaction."""
        return _result(self._conn.execute(sql, params))

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
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
