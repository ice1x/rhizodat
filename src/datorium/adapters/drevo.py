"""drevo adapter, a two-phase branch: embedded (``drevo-py``) or remote over Bolt (``neo4j``).

- ``drevo:///abs/path.drevo`` / ``drevo+mem://`` open the graph in-process with
  drevo-py: ``Transaction.prepare(gid)``, ``Drevo.commit_prepared`` /
  ``rollback_prepared`` / ``list_prepared``.
- ``drevo+bolt://host:7687`` talks to a drevo server with any Neo4j driver;
  two-phase commit goes through the ``drevo.tx.*`` procedures
  (``CALL drevo.tx.prepare($gid)`` inside the explicit transaction, then
  ``commitPrepared`` / ``rollbackPrepared`` / ``listPrepared``).

While a transaction is prepared, drevo's **prepared fence** makes every other
write to that graph fail fast with a retryable error (``TransactionConflict``,
or Bolt ``TransientError``), so branches here prepare last and commit first.

Each branch writes an applied marker ``(:DatApplied {dat_txn, dat_tx_time})``
inside its own transaction at stage, so ``is_applied`` is one lookup.

Drivers are imported on first use; neither is a core dependency.
"""

import contextlib
import re
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from datorium.core.dsn import DSN, parse_dsn
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

__all__ = ["DrevoBranch", "DrevoJournal", "DrevoParticipant", "DrevoStore", "graph_location"]

_MARKER = "CREATE (:DatApplied {dat_txn: $dat_txn, dat_tx_time: $dat_tx_time})"
_IS_APPLIED = "MATCH (m:DatApplied {dat_txn: $dat_txn}) RETURN count(m) AS n"
# Bolt has no distinct code for an unknown gid; the server's message is the only signal.
_BOLT_UNKNOWN_GID = "no transaction is prepared as"

Params = Mapping[str, Any] | None


class _UnknownGid(Exception):
    pass


class _Backend(Protocol):
    def execute(self, query: str, params: Params) -> Result: ...
    def begin(self) -> "_Tx": ...
    def commit_prepared(self, gid: str) -> None: ...  # raises _UnknownGid
    def rollback_prepared(self, gid: str) -> None: ...  # raises _UnknownGid
    def list_prepared(self) -> list[str]: ...
    def is_retryable(self, error: BaseException) -> bool: ...
    def close(self) -> None: ...


class _Tx(Protocol):
    def execute(self, query: str, params: Params) -> Result: ...
    def prepare(self, gid: str) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


def _import(module: str, extra: str) -> Any:  # noqa: ANN401 - optional driver module
    try:
        return __import__(module)
    except ImportError as exc:
        raise ImportError(f"the drevo adapter needs {module}: {extra}") from exc


# --- embedded (drevo-py) ----------------------------------------------------------------


class _EmbeddedTx:
    def __init__(self, tx: Any) -> None:  # noqa: ANN401 - drevo.Transaction
        self._tx = tx

    def execute(self, query: str, params: Params) -> Result:
        r = self._tx.execute(query, dict(params) if params else None)
        return Result(iter(r.rows), columns=tuple(r.columns), raw=r)

    def prepare(self, gid: str) -> None:
        self._tx.prepare(gid)

    def commit(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        if not self._tx.closed:
            self._tx.rollback()


class _Embedded:
    def __init__(self, path: str | None) -> None:
        self._drevo = _import("drevo", "install drevo-py from source (uv sync --group drevo)")
        if not hasattr(self._drevo.Drevo, "list_prepared"):
            raise ImportError("this drevo-py has no two-phase commit; install a newer build")
        self._db = self._drevo.Drevo.open(path) if path else self._drevo.Drevo.open_in_memory()

    def execute(self, query: str, params: Params) -> Result:
        r = self._db.execute(query, dict(params) if params else None)
        return Result(iter(r.rows), columns=tuple(r.columns), raw=r)

    def begin(self) -> _Tx:
        return _EmbeddedTx(self._db.begin())

    def commit_prepared(self, gid: str) -> None:
        try:
            self._db.commit_prepared(gid)
        except self._drevo.UnknownGidError as exc:
            raise _UnknownGid(gid) from exc

    def rollback_prepared(self, gid: str) -> None:
        try:
            self._db.rollback_prepared(gid)
        except self._drevo.UnknownGidError as exc:
            raise _UnknownGid(gid) from exc

    def list_prepared(self) -> list[str]:
        return [p.gid for p in self._db.list_prepared()]

    def is_retryable(self, error: BaseException) -> bool:
        return isinstance(error, self._drevo.TransactionConflict)

    def close(self) -> None:
        self._db.close()


# --- remote (Bolt) ---------------------------------------------------------------------


class _BoltTx:
    def __init__(self, session: Any) -> None:  # noqa: ANN401 - neo4j.Session
        self._session = session
        self._tx = session.begin_transaction()

    def execute(self, query: str, params: Params) -> Result:
        records = self._tx.run(query, dict(params) if params else None)
        rows = records.data()
        return Result(iter(rows), columns=tuple(records.keys()), raw=records)

    def prepare(self, gid: str) -> None:
        self._tx.run("CALL drevo.tx.prepare($gid)", {"gid": gid}).consume()
        self._tx.commit()  # closes the Bolt transaction; the server keeps it prepared
        self._session.close()

    def commit(self) -> None:
        try:
            self._tx.commit()
        finally:
            self._session.close()

    def rollback(self) -> None:
        try:
            if not self._tx.closed():
                self._tx.rollback()
        finally:
            self._session.close()


class _Bolt:
    def __init__(self, uri: str, auth: tuple[str, str] | None) -> None:
        self._neo4j = _import("neo4j", "pip install 'datorium[drevo]'")
        self._driver = self._neo4j.GraphDatabase.driver(uri, auth=auth)

    def execute(self, query: str, params: Params) -> Result:
        with self._driver.session() as session:
            records = session.run(query, dict(params) if params else None)
            rows = records.data()
            return Result(iter(rows), columns=tuple(records.keys()))

    def begin(self) -> _Tx:
        return _BoltTx(self._driver.session())

    def _resolve(self, procedure: str, gid: str) -> None:
        try:
            self.execute(f"CALL drevo.tx.{procedure}($gid)", {"gid": gid})
        except self._neo4j.exceptions.ClientError as exc:
            if _BOLT_UNKNOWN_GID in (exc.message or ""):
                raise _UnknownGid(gid) from exc
            raise

    def commit_prepared(self, gid: str) -> None:
        self._resolve("commitPrepared", gid)

    def rollback_prepared(self, gid: str) -> None:
        self._resolve("rollbackPrepared", gid)

    def list_prepared(self) -> list[str]:
        return [row["gid"] for row in self.execute("CALL drevo.tx.listPrepared()", None)]

    def is_retryable(self, error: BaseException) -> bool:
        exceptions = self._neo4j.exceptions
        return isinstance(
            error,
            exceptions.TransientError | exceptions.ServiceUnavailable | exceptions.SessionExpired,
        )

    def close(self) -> None:
        self._driver.close()


def _open_backend(parsed: DSN) -> _Backend:
    if parsed.store != "drevo":
        raise ValueError(f"not a drevo connection string: {parsed}")
    if parsed.transport == "bolt":
        auth = (parsed.user, parsed.password or "") if parsed.user else None
        return _Bolt(f"bolt://{parsed.host}:{parsed.port}", auth)
    return _Embedded(parsed.path if parsed.transport == "embedded" else None)


def graph_location(dsn: str | DSN) -> tuple[str, ...] | None:
    """Which drevo graph a DSN names (``None`` for ``drevo+mem://``: always its own graph)."""
    parsed = parse_dsn(dsn) if isinstance(dsn, str) else dsn
    if parsed.store != "drevo" or parsed.transport == "memory":
        return None
    if parsed.transport == "bolt":
        return ("bolt", (parsed.host or "").lower(), str(parsed.port))
    return ("embedded", parsed.path or "")


# --- public surface ----------------------------------------------------------------------


class DrevoStore:
    """One drevo graph as a datorium store."""

    two_phase = True

    def __init__(self, dsn: str, *, name: str = "drevo") -> None:
        parsed = parse_dsn(dsn)
        self.name = name
        self._dsn = parsed
        self._backend = _open_backend(parsed)
        self._participant = DrevoParticipant(self._backend)

    @property
    def location(self) -> tuple[str, ...] | None:
        return graph_location(self._dsn)

    @property
    def applied_location(self) -> tuple[str, ...] | None:
        """The graph holding this store's ``DatApplied`` markers (``None``: its own).

        Two stores on one graph must not meet in one meta-transaction: the first
        branch's prepared fence would refuse the second's prepare on every retry.
        """
        where = self.location
        return None if where is None else ("drevo", *where)

    def __repr__(self) -> str:
        return f"DrevoStore({self.name!r}, {self._dsn.redacted()!r})"

    def install(self) -> None:
        """Nothing to create: markers are nodes."""

    def execute(self, query: str, params: Params = None) -> Result:
        """Native Cypher in autocommit mode, outside any meta-transaction."""
        return self._backend.execute(query, params)

    def read_only(self, query: str, params: Params = None) -> Result:
        """A native read that never keeps a write: it runs in a transaction that is
        always rolled back (drevo has no read-only mode)."""
        tx = self._backend.begin()
        try:
            result = tx.execute(query, params)
            rows = list(result)
        finally:
            tx.rollback()
        return Result(iter(rows), columns=result.columns)

    def branch(self, dat_txn: uuid.UUID) -> "DrevoBranch":
        return DrevoBranch(self.name, self._backend, dat_txn)

    def participant(self) -> "DrevoParticipant":
        return self._participant

    def close(self) -> None:
        self._backend.close()


class DrevoBranch:
    two_phase = True

    def __init__(self, store: str, backend: _Backend, dat_txn: uuid.UUID) -> None:
        self.store = store
        self.dat_txn = dat_txn
        self._backend = backend
        self._tx = backend.begin()
        self._gid: str | None = None

    def execute(self, query: str, params: Params = None) -> Result:
        """Run Cypher inside this branch's transaction (it reads its own writes)."""
        return self._tx.execute(query, params)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        self._tx.execute(_MARKER, {"dat_txn": str(dat_txn), "dat_tx_time": dat_tx_time})
        return None

    def prepare(self, gid: str) -> None:
        self._tx.prepare(gid)
        self._gid = gid

    def commit_one_phase(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        if self._gid is None:
            self._tx.rollback()
            return
        with contextlib.suppress(_UnknownGid):  # already resolved
            self._backend.rollback_prepared(self._gid)

    def is_retryable(self, error: BaseException) -> bool:
        return self._backend.is_retryable(error)


class DrevoParticipant:
    def __init__(self, backend: _Backend) -> None:
        self._backend = backend
        self._lock = threading.Lock()

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        try:
            self._backend.commit_prepared(branch.gid)
        except _UnknownGid as exc:
            if not self.is_applied(branch, dat_txn):
                raise RuntimeError(
                    f"heuristic outcome: {branch.gid} is gone but {dat_txn} was never applied"
                ) from exc

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        row = self._backend.execute(_IS_APPLIED, {"dat_txn": str(dat_txn)}).one()
        return int(row["n"]) > 0

    def abort(self, branch: BranchRecord) -> None:
        with contextlib.suppress(_UnknownGid):  # already resolved
            self._backend.rollback_prepared(branch.gid)

    def prepared_gids(self) -> list[str]:
        return self._backend.list_prepared()

    def is_retryable(self, error: BaseException) -> bool:
        return self._backend.is_retryable(error)


# --- journal -------------------------------------------------------------------------------

_LABEL = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_FIELDS = (
    "j.dat_txn AS dat_txn, j.state AS state, j.dat_tx_time AS dat_tx_time,"
    " j.branches AS branches, j.created_ms AS created_ms, j.updated_ms AS updated_ms"
)


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


class DrevoJournal:
    """The journal in a drevo graph: one node per meta-transaction.

    Nodes are ``(:<label> {title, dat_txn, state, dat_tx_time, branches, created_ms,
    updated_ms})``; ``title`` (unique graph-wide in drevo) is ``<label>:<dat_txn>``,
    so a ``dat_txn`` is journaled once. Each compare-and-set is one drevo
    transaction: of two concurrent writers of the same node, drevo rejects the
    second commit, which surfaces as ``JournalConflict``.

    The journal graph must not be the graph of a drevo store in the same
    deployment: a prepared branch raises that graph's fence and would block the
    coordinator's own ``PREPARED`` / ``COMMITTED`` writes.
    """

    def __init__(
        self,
        dsn: str,
        *,
        label: str = "DatJournal",
        wall_ms: Callable[[], int] = _system_ms,
    ) -> None:
        if not _LABEL.match(label):
            raise ValueError(f"invalid journal label {label!r}: a letter, then letters, digits, _")
        self._label = label
        self._wall_ms = wall_ms
        self._backend = _open_backend(parse_dsn(dsn))

    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord:
        now = self._wall_ms()
        if self.get(dat_txn) is not None:
            raise JournalConflict(f"meta-transaction {dat_txn} is already journaled")
        tx = self._backend.begin()
        try:
            tx.execute(
                f"CREATE (j:{self._label} {{title: $title, dat_txn: $t, state: $state,"
                " branches: $branches, created_ms: $now, updated_ms: $now})",
                {
                    "title": f"{self._label}:{dat_txn}",
                    "t": str(dat_txn),
                    "state": State.ACTIVE.value,
                    "branches": encode_branches(branches),
                    "now": now,
                },
            )
            tx.commit()
        except Exception:
            tx.rollback()
            if self.get(dat_txn) is not None:  # lost a race on the unique title
                raise JournalConflict(f"meta-transaction {dat_txn} is already journaled") from None
            raise
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
        time_clause = ", j.dat_tx_time = $ts" if dat_tx_time is not None else ""
        tx = self._backend.begin()
        try:
            rows = tx.execute(
                f"MATCH (j:{self._label} {{dat_txn: $t}}) WHERE j.state = $expected"
                f" SET j.state = $new, j.updated_ms = $now{time_clause} RETURN {_FIELDS}",
                {
                    "t": str(dat_txn),
                    "expected": expected.value,
                    "new": new.value,
                    "now": self._wall_ms(),
                    "ts": dat_tx_time,
                },
            ).all()
            if not rows:
                tx.rollback()
            else:
                tx.commit()
                return self._record(rows[0])
        except Exception as exc:
            tx.rollback()
            if not self._backend.is_retryable(exc):
                raise
            # A concurrent writer committed first: report what it left.
        current = self.get(dat_txn)
        raise conflict_error(dat_txn, expected, current.state if current else None)

    def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
        row = self._backend.execute(
            f"MATCH (j:{self._label} {{dat_txn: $t}}) RETURN {_FIELDS}", {"t": str(dat_txn)}
        ).first()
        return self._record(row) if row else None

    def pending(self, *, updated_before_ms: int, limit: int | None = None) -> list[JournalRecord]:
        query = (
            f"MATCH (j:{self._label}) WHERE j.state IN ['ACTIVE', 'PREPARED', 'COMMITTED']"
            f" AND j.updated_ms < $cut RETURN {_FIELDS} ORDER BY updated_ms, created_ms"
        )
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        rows = self._backend.execute(query, {"cut": updated_before_ms}).all()
        return [self._record(r) for r in rows]

    def forget(self, dat_txn: uuid.UUID) -> None:
        record = self.get(dat_txn)
        if record is None:
            return
        if record.state not in (State.ABORTED, State.DONE):
            raise ValueError(f"cannot forget meta-transaction {dat_txn} in state {record.state}")
        self._backend.execute(
            f"MATCH (j:{self._label} {{dat_txn: $t}}) WHERE j.state IN ['ABORTED', 'DONE']"
            " DETACH DELETE j",
            {"t": str(dat_txn)},
        )

    def close(self) -> None:
        self._backend.close()

    @staticmethod
    def _record(row: Mapping[str, Any]) -> JournalRecord:
        return JournalRecord(
            dat_txn=uuid.UUID(row["dat_txn"]),
            state=State(row["state"]),
            branches=decode_branches(row["branches"]),
            dat_tx_time=row["dat_tx_time"],
            created_ms=row["created_ms"],
            updated_ms=row["updated_ms"],
        )
