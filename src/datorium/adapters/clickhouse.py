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

Tables written through datorium need ``dat_txn UUID`` and ``dat_tx_time Int64``
columns; a skip index on ``dat_txn`` keeps the idempotency check cheap.

clickhouse-connect is imported on first use, so the driver stays an optional extra.
"""

import re
import uuid
from collections.abc import Iterable, Mapping
from typing import Any

from datorium.core.dsn import parse_dsn
from datorium.core.journal import BranchRecord
from datorium.core.result import Result
from datorium.core.rows import Batch, decode_batches, encode_batches

__all__ = ["ClickHouseBranch", "ClickHouseParticipant", "ClickHouseStore"]

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
            "the ClickHouse adapter needs clickhouse-connect: pip install 'datorium[clickhouse]'"
        ) from exc
    return clickhouse_connect


def _quote(table: str) -> str:
    return ".".join(f"`{part}`" for part in table.split("."))


def _is_retryable(error: BaseException) -> bool:
    if isinstance(error, ConnectionError | TimeoutError):
        return True
    from clickhouse_connect.driver.exceptions import OperationalError

    if isinstance(error, OperationalError):  # connection refused / reset, HTTP failures
        return True
    return getattr(error, "code", None) in _RETRYABLE_CODES


class ClickHouseStore:
    """One ClickHouse database as a datorium store."""

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

    def close(self) -> None:
        self._client.close()

    # --- used by the branch and the participant ------------------------------------------

    def columns(self, table: str) -> list[str]:
        r = self._client.query(f"DESCRIBE TABLE {_quote(table)}")
        return [row[0] for row in r.result_rows]

    def apply(self, gid: str, batches: list[Batch], dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        """Insert every batch and then the marker, skipping what is already there."""
        for i, batch in enumerate(batches):
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
                raise ValueError(f"{reserved} is set by datorium; leave it out of the rows")
        known, buffered = self._buffer.get(table, (columns, []))
        for row in rows:
            if tuple(row) != known and set(row) != set(known):
                raise ValueError(f"rows for {table!r} must have the same columns: {known}")
            buffered.append([row[c] for c in known])
        self._buffer[table] = (known, buffered)

    def execute(self, query: str, params: Mapping[str, Any] | None = None) -> Result:
        """Read from the store. Buffered rows are not visible: ClickHouse has no transaction."""
        return self._ch.execute(query, params)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes:
        batches = []
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

    def prepare(self, gid: str) -> None:
        raise TypeError("a ClickHouse branch is not two-phase; it is applied after the decision")

    def commit_one_phase(self) -> None:
        if self._staged is None:
            raise RuntimeError("commit_one_phase before stage")
        batches, dat_tx_time = self._staged
        self._ch.apply(f"dat:{self.dat_txn}:{self.store}", batches, self.dat_txn, dat_tx_time)

    def rollback(self) -> None:
        self._buffer.clear()
        self._staged = None

    def is_retryable(self, error: BaseException) -> bool:
        return _is_retryable(error)


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
