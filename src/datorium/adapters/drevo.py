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
import threading
import uuid
from collections.abc import Mapping
from typing import Any, Protocol

from datorium.core.dsn import parse_dsn
from datorium.core.journal import BranchRecord
from datorium.core.result import Result

__all__ = ["DrevoBranch", "DrevoParticipant", "DrevoStore"]

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


# --- public surface ----------------------------------------------------------------------


class DrevoStore:
    """One drevo graph as a datorium store."""

    two_phase = True

    def __init__(self, dsn: str, *, name: str = "drevo") -> None:
        parsed = parse_dsn(dsn)
        if parsed.store != "drevo":
            raise ValueError(f"not a drevo connection string: {parsed}")
        self.name = name
        self._dsn = parsed
        self._backend: _Backend
        if parsed.transport == "bolt":
            auth = (parsed.user, parsed.password or "") if parsed.user else None
            self._backend = _Bolt(f"bolt://{parsed.host}:{parsed.port}", auth)
        else:
            self._backend = _Embedded(parsed.path if parsed.transport == "embedded" else None)
        self._participant = DrevoParticipant(self._backend)

    def __repr__(self) -> str:
        return f"DrevoStore({self.name!r}, {self._dsn.redacted()!r})"

    def install(self) -> None:
        """Nothing to create: markers are nodes."""

    def execute(self, query: str, params: Params = None) -> Result:
        """Native Cypher in autocommit mode, outside any meta-transaction."""
        return self._backend.execute(query, params)

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
