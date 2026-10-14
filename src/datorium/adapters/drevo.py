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

Entity parts: one node per version, labelled after the type (``tracker.Issue`` →
``tracker_issue``), with ``dat_id``, ``dat_txn``, ``dat_tx_time``,
``dat_created``, ``dat_deleted`` and a property per field: ``str``, ``int``,
``float``, ``bool`` natively, references and UUIDs as their string, anything
else as tagged JSON. drevo keeps node ``title``s unique graph-wide, so a field
named ``title`` is stored as ``dat_title``. Every referenced ``dat_id`` is also
an edge, typed after the field, from the version to an anchor node
``(:DatEntity {dat_id})``; anchors nothing points to are removed with the
versions that pointed to them. A branch writes versions inside its transaction
and fills ``dat_tx_time`` at stage.

Drivers are imported on first use; neither is a core dependency.
"""

import contextlib
import json
import re
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from datorium.core.adapter import Adapter, Scheme
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
from datorium.core.mapping import Ref
from datorium.core.parts import Part, Version
from datorium.core.result import Result
from datorium.core.rows import decode_value, encode_value

__all__ = [
    "ADAPTER",
    "ANCHOR",
    "DrevoBranch",
    "DrevoJournal",
    "DrevoParticipant",
    "DrevoStore",
    "graph_location",
]

_MARKER = "CREATE (:DatApplied {dat_txn: $dat_txn, dat_tx_time: $dat_tx_time})"
_IS_APPLIED = "MATCH (m:DatApplied {dat_txn: $dat_txn}) RETURN count(m) AS n"
# Bolt has no distinct code for an unknown gid; the server's message is the only signal.
_BOLT_UNKNOWN_GID = "no transaction is prepared as"

Params = Mapping[str, Any] | None

ANCHOR = "DatEntity"
"""The label of the node a reference edge points to: one per referenced ``dat_id``."""


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


# --- entity parts ---------------------------------------------------------------------

_SERVICE = ("dat_id", "dat_txn", "dat_tx_time", "dat_created", "dat_deleted")
_NATIVE = (str, int, float, bool)
_ORPHANS = f"MATCH (a:{ANCHOR}) WHERE a.dat_id IN $ids AND NOT (a)<--() DELETE a"


def _field_kind(annotation: object) -> str:
    """How a field is stored: ``native``, ``uuid`` (its string) or ``json`` (tagged)."""
    import types
    import typing

    origin = typing.get_origin(annotation)
    if origin is types.UnionType or origin is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        return _field_kind(args[0]) if len(args) == 1 else "json"
    if annotation is Ref or origin is Ref or annotation is uuid.UUID:
        return "uuid"
    return "native" if annotation in _NATIVE else "json"


def _targets(value: Any) -> list[str]:  # noqa: ANN401 - any stored value
    """Every ``dat_id`` in a reference field's value, as strings."""
    if isinstance(value, uuid.UUID):
        return [str(value)]
    if isinstance(value, list | tuple):
        return [t for v in value for t in _targets(v)]
    if isinstance(value, dict):
        return [t for v in value.values() for t in _targets(v)]
    return []


class _PartNodes:
    """How one part is stored: its label, properties and value conversions."""

    def __init__(self, part: Part) -> None:
        self.part = part
        self.label = part.table
        self.kinds = {f: _field_kind(part.types.get(f)) for f in part.fields}
        self.names = {f: "dat_title" if f == "title" else f for f in part.fields}

    def properties(
        self,
        dat_id: uuid.UUID,
        dat_txn: uuid.UUID,
        values: Mapping[str, Any],
        created: int | None,
        deleted: bool,
    ) -> dict[str, Any]:
        unknown = set(values) - set(self.part.fields)
        if unknown:
            raise ValueError(f"{sorted(unknown)} are not fields of {self.part.type_name}")
        props: dict[str, Any] = {
            "dat_id": str(dat_id),
            "dat_txn": str(dat_txn),
            "dat_deleted": deleted,
        }
        if created is not None:
            props["dat_created"] = created
        for f in self.part.fields:
            value = values.get(f)
            if value is not None:
                props[self.names[f]] = self._store(f, value)
        return props

    def _store(self, field: str, value: Any) -> Any:  # noqa: ANN401 - any field value
        kind = self.kinds[field]
        if kind == "uuid" and isinstance(value, uuid.UUID):
            return str(value)
        if kind == "native" and isinstance(value, _NATIVE):
            return value
        return json.dumps(encode_value(value), separators=(",", ":"))

    def edges(self, values: Mapping[str, Any]) -> list[tuple[str, list[str]]]:
        """``(field, targets)`` for every reference field with a value."""
        return [
            (f, targets) for f in sorted(self.part.refs) if (targets := _targets(values.get(f)))
        ]

    def version(self, props: Mapping[str, Any]) -> Version:
        values = {f: self._value(f, props.get(self.names[f])) for f in self.part.fields}
        return Version(
            dat_txn=uuid.UUID(props["dat_txn"]),
            dat_tx_time=props.get("dat_tx_time"),
            created=props.get("dat_created"),
            deleted=bool(props.get("dat_deleted")),
            values=values,
        )

    def _value(self, field: str, raw: Any) -> Any:  # noqa: ANN401 - any stored value
        if raw is None:
            return None
        kind = self.kinds[field]
        if kind == "uuid":
            return uuid.UUID(raw)
        if kind == "native":
            return raw
        return decode_value(json.loads(raw))

    def select(self, where: str, order: str, limit: int | None = None) -> str:
        query = f"MATCH (v:{self.label}) WHERE {where} RETURN properties(v) AS p ORDER BY {order}"
        return query + (f" LIMIT {int(limit)}" if limit is not None else "")


class _Parts:
    """The parts a store has seen, created on first use."""

    def __init__(self, backend: "_Backend") -> None:
        self._backend = backend
        self._lock = threading.Lock()
        self._nodes: dict[Part, _PartNodes] = {}

    def install(self, part: Part) -> _PartNodes:
        with self._lock:
            nodes = self._nodes.get(part)
        if nodes is not None:
            return nodes
        nodes = _PartNodes(part)
        for label in (nodes.label, ANCHOR):
            self._backend.execute(
                f"CREATE INDEX dat_{label.lower()} IF NOT EXISTS FOR (v:{label}) ON (v.dat_id)",
                None,
            )
        with self._lock:
            self._nodes[part] = nodes
        return nodes


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
        self._parts = _Parts(self._backend)

    @property
    def location(self) -> tuple[str, ...] | None:
        return graph_location(self._dsn)

    @property
    def fence_location(self) -> tuple[str, ...] | None:
        """The graph a prepared branch of this store fences: the journal must live elsewhere."""
        return self.location

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
        return DrevoBranch(self.name, self._backend, dat_txn, parts=self._parts)

    # --- entity parts (PartStore) ------------------------------------------------------
    # Committed reads run in autocommit mode: they keep working under a prepared fence.

    def install_part(self, part: Part) -> None:
        """Create the ``dat_id`` indexes of the part's versions and of anchors, if missing."""
        self._parts.install(part)

    def read_part(
        self, part: Part, dat_id: uuid.UUID, *, known_at: int | None = None
    ) -> Version | None:
        nodes = self._parts.install(part)
        where = "v.dat_id = $id AND v.dat_tx_time IS NOT NULL"
        if known_at is not None:
            where += " AND v.dat_tx_time <= $k"
        query = nodes.select(where, "v.dat_tx_time DESC", 1)
        row = self._backend.execute(query, {"id": str(dat_id), "k": known_at}).first()
        return nodes.version(row["p"]) if row else None

    def part_history(self, part: Part, dat_id: uuid.UUID) -> list[Version]:
        nodes = self._parts.install(part)
        query = nodes.select("v.dat_id = $id AND v.dat_tx_time IS NOT NULL", "v.dat_tx_time")
        return [nodes.version(r["p"]) for r in self._backend.execute(query, {"id": str(dat_id)})]

    def expired_parts(self, part: Part, *, deleted_before: int, limit: int) -> list[uuid.UUID]:
        if limit <= 0:
            return []
        label = self._parts.install(part).label
        rows = self._backend.execute(
            f"MATCH (v:{label}) WHERE v.dat_tx_time IS NOT NULL"
            " WITH v.dat_id AS d, max(v.dat_tx_time) AS t"
            f" MATCH (w:{label}) WHERE w.dat_id = d AND w.dat_tx_time = t"
            f" AND w.dat_deleted AND t < $b RETURN d ORDER BY d LIMIT {int(limit)}",
            {"b": deleted_before},
        )
        return [uuid.UUID(r["d"]) for r in rows]

    def participant(self) -> "DrevoParticipant":
        return self._participant

    def close(self) -> None:
        self._backend.close()


class DrevoBranch:
    two_phase = True

    def __init__(
        self,
        store: str,
        backend: _Backend,
        dat_txn: uuid.UUID,
        *,
        parts: _Parts | None = None,
    ) -> None:
        self.store = store
        self.dat_txn = dat_txn
        self._backend = backend
        self._tx = backend.begin()
        self._gid: str | None = None
        self._parts = parts
        self._written: set[str] = set()  # labels this branch wrote versions to

    def execute(self, query: str, params: Params = None) -> Result:
        """Run Cypher inside this branch's transaction (it reads its own writes)."""
        return self._tx.execute(query, params)

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
        nodes = self._nodes(part)
        props = nodes.properties(dat_id, self.dat_txn, values, created, deleted)
        key = {"id": str(dat_id), "txn": str(self.dat_txn)}
        self._remove(nodes, f"MATCH (v:{nodes.label} {{dat_id: $id, dat_txn: $txn}})", key)
        self._tx.execute(f"CREATE (v:{nodes.label}) SET v += $p", {"p": props})
        for field, targets in nodes.edges(values):
            self._tx.execute(
                f"MATCH (v:{nodes.label} {{dat_id: $id, dat_txn: $txn}})"
                f" UNWIND $targets AS t MERGE (a:{ANCHOR} {{dat_id: t}})"
                f" CREATE (v)-[:{field}]->(a)",
                {**key, "targets": targets},
            )
        self._written.add(nodes.label)

    def erase_part(self, part: Part, dat_id: uuid.UUID) -> None:
        nodes = self._nodes(part)
        self._remove(nodes, f"MATCH (v:{nodes.label} {{dat_id: $id}})", {"id": str(dat_id)})

    def read_part(self, part: Part, dat_id: uuid.UUID) -> Version | None:
        nodes = self._nodes(part)
        rows = self._tx.execute(
            nodes.select("v.dat_id = $id", "v.dat_tx_time"), {"id": str(dat_id)}
        )
        versions = [nodes.version(r["p"]) for r in rows]
        # This branch's own version has no dat_tx_time yet: it comes first.
        mine = [v for v in versions if v.dat_tx_time is None]
        if mine:
            return mine[0]
        return max(versions, key=lambda v: v.dat_tx_time or 0, default=None)

    def _remove(self, nodes: _PartNodes, match: str, params: Mapping[str, Any]) -> None:
        """Delete the matched versions, then the anchors only they pointed to."""
        rows = self._tx.execute(f"{match}-->(a:{ANCHOR}) RETURN DISTINCT a.dat_id AS a", params)
        anchors = [r["a"] for r in rows]
        self._tx.execute(f"{match} DETACH DELETE v", params)
        if anchors:
            self._tx.execute(_ORPHANS, {"ids": anchors})

    def _nodes(self, part: Part) -> _PartNodes:
        if self._parts is None:
            raise TypeError("this branch was made without its store: it cannot hold parts")
        return self._parts.install(part)

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        for label in sorted(self._written):
            self._tx.execute(
                f"MATCH (v:{label} {{dat_txn: $txn}}) SET v.dat_tx_time = $ts,"
                " v.dat_created = coalesce(v.dat_created, $ts)",
                {"txn": str(dat_txn), "ts": dat_tx_time},
            )
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


ADAPTER = Adapter(
    kind="drevo",
    schemes=(
        Scheme("drevo", transport="embedded", host="none", path="required"),
        Scheme("drevo+mem", transport="memory", host="none", path="none"),
        Scheme("drevo+bolt", transport="bolt", default_port=7687, host="required"),
    ),
    open_store=lambda name, dsn: DrevoStore(dsn, name=name),
    open_journal=lambda dsn, wall_ms: DrevoJournal(dsn, wall_ms=wall_ms),
    journal_location=graph_location,
    query_help="Cypher with $name parameters",
)
