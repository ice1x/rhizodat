"""Journal of multi-branch meta-transactions.

The journal is the coordination record of a meta-transaction, not a data
master: it holds states and branch lists, never entity data (a ClickHouse
branch may keep its staged payload here). Every state change is a
compare-and-set from an expected state, so a coordinator deciding ``COMMITTED``
and a recovery worker deciding ``ABORTED`` can never both win.

States and the only legal transitions::

    ACTIVE ──► PREPARED ──► COMMITTED ──► DONE
       │           │
       └───────────┴──► ABORTED

``COMMITTED`` is the atomic commit point. ``ABORTED`` and ``DONE`` are final
and may be forgotten (compaction). Language-neutral table layout, used by every
SQL journal::

    dat_txn      UUID      primary key
    state        TEXT      one of the states above
    dat_tx_time  BIGINT    HLC, set when PREPARED
    branches     TEXT      JSON list of {store, gid, two_phase, payload (base64)}
    created_ms   BIGINT    wall clock of the coordinator, for scans only
    updated_ms   BIGINT
"""

import base64
import enum
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol

from datorium.core.dsn import parse_dsn

__all__ = [
    "BranchRecord",
    "Journal",
    "JournalConflict",
    "JournalRecord",
    "MemoryJournal",
    "SqliteJournal",
    "State",
    "check_transition",
    "conflict_error",
    "decode_branches",
    "encode_branches",
    "open_journal",
]


class State(enum.StrEnum):
    ACTIVE = "ACTIVE"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    DONE = "DONE"


_LEGAL: dict[State, frozenset[State]] = {
    State.ACTIVE: frozenset({State.PREPARED, State.ABORTED}),
    State.PREPARED: frozenset({State.COMMITTED, State.ABORTED}),
    State.COMMITTED: frozenset({State.DONE}),
    State.ABORTED: frozenset(),
    State.DONE: frozenset(),
}
_UNFINISHED = (State.ACTIVE, State.PREPARED, State.COMMITTED)
_FINAL = (State.ABORTED, State.DONE)


class JournalConflict(RuntimeError):
    """The stored state is not the expected one (another actor got there first)."""


@dataclass(frozen=True, slots=True)
class BranchRecord:
    store: str
    gid: str
    two_phase: bool
    payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class JournalRecord:
    dat_txn: uuid.UUID
    state: State
    branches: tuple[BranchRecord, ...]
    dat_tx_time: int | None
    created_ms: int
    updated_ms: int


class Journal(Protocol):
    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord: ...

    def transition(
        self,
        dat_txn: uuid.UUID,
        expected: State,
        new: State,
        *,
        dat_tx_time: int | None = None,
    ) -> JournalRecord: ...

    def get(self, dat_txn: uuid.UUID) -> JournalRecord | None: ...

    def pending(self, *, updated_before_ms: int, limit: int | None = None) -> list[JournalRecord]:
        """Unfinished records (ACTIVE, PREPARED, COMMITTED) last changed before the cutoff,
        oldest first."""
        ...

    def forget(self, dat_txn: uuid.UUID) -> None:
        """Remove a final (ABORTED or DONE) record."""
        ...

    def close(self) -> None: ...


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


def check_transition(expected: State, new: State, dat_tx_time: int | None) -> None:
    """Raise ``ValueError`` for an edge that is never legal, whatever is stored."""
    if new not in _LEGAL[expected]:
        raise ValueError(f"illegal journal transition {expected} -> {new}")
    if new is State.PREPARED and dat_tx_time is None:
        raise ValueError("PREPARED needs the dat_tx_time assigned at stage")


def conflict_error(dat_txn: uuid.UUID, expected: State, actual: State | None) -> JournalConflict:
    """The error for a compare-and-set that found ``actual`` instead of ``expected``."""
    if actual is None:
        return JournalConflict(f"meta-transaction {dat_txn} is unknown to the journal")
    return JournalConflict(f"meta-transaction {dat_txn} is {actual}, expected {expected}")


class MemoryJournal:
    """In-process journal for tests and single-process use. Not durable."""

    def __init__(self, *, wall_ms: Callable[[], int] = _system_ms) -> None:
        self._wall_ms = wall_ms
        self._lock = threading.Lock()
        self._records: dict[uuid.UUID, JournalRecord] = {}

    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord:
        with self._lock:
            if dat_txn in self._records:
                raise JournalConflict(f"meta-transaction {dat_txn} is already journaled")
            now = self._wall_ms()
            record = JournalRecord(dat_txn, State.ACTIVE, tuple(branches), None, now, now)
            self._records[dat_txn] = record
            return record

    def transition(
        self,
        dat_txn: uuid.UUID,
        expected: State,
        new: State,
        *,
        dat_tx_time: int | None = None,
    ) -> JournalRecord:
        check_transition(expected, new, dat_tx_time)
        with self._lock:
            current = self._records.get(dat_txn)
            if current is None or current.state is not expected:
                raise conflict_error(dat_txn, expected, current.state if current else None)
            updated = replace(
                current,
                state=new,
                dat_tx_time=dat_tx_time if dat_tx_time is not None else current.dat_tx_time,
                updated_ms=self._wall_ms(),
            )
            self._records[dat_txn] = updated
            return updated

    def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
        with self._lock:
            return self._records.get(dat_txn)

    def pending(self, *, updated_before_ms: int, limit: int | None = None) -> list[JournalRecord]:
        with self._lock:
            found = sorted(
                (
                    r
                    for r in self._records.values()
                    if r.state in _UNFINISHED and r.updated_ms < updated_before_ms
                ),
                key=lambda r: (r.updated_ms, r.created_ms),
            )
        return found[:limit] if limit is not None else found

    def forget(self, dat_txn: uuid.UUID) -> None:
        with self._lock:
            current = self._records.get(dat_txn)
            if current is None:
                return
            if current.state not in _FINAL:
                raise ValueError(
                    f"cannot forget meta-transaction {dat_txn} in state {current.state}"
                )
            del self._records[dat_txn]

    def close(self) -> None:
        pass


_SCHEMA = """
CREATE TABLE IF NOT EXISTS dat_journal (
    dat_txn     TEXT PRIMARY KEY,
    state       TEXT NOT NULL,
    dat_tx_time INTEGER,
    branches    TEXT NOT NULL,
    created_ms  INTEGER NOT NULL,
    updated_ms  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS dat_journal_pending ON dat_journal (state, updated_ms);
"""


def encode_branches(branches: Iterable[BranchRecord]) -> str:
    """The ``branches`` column: JSON list, payloads base64."""
    return json.dumps(
        [
            {
                "store": b.store,
                "gid": b.gid,
                "two_phase": b.two_phase,
                "payload": base64.b64encode(b.payload).decode() if b.payload is not None else None,
            }
            for b in branches
        ]
    )


def decode_branches(raw: str) -> tuple[BranchRecord, ...]:
    return tuple(
        BranchRecord(
            store=b["store"],
            gid=b["gid"],
            two_phase=b["two_phase"],
            payload=base64.b64decode(b["payload"]) if b["payload"] is not None else None,
        )
        for b in json.loads(raw)
    )


class SqliteJournal:
    """Journal in a local SQLite file (``journal="file:///…"``).

    Durable and safe across processes on one host; for development and
    single-host embedded use. Production journals live in PostgreSQL or drevo
    so recovery can run on another host.
    """

    _COLUMNS = "dat_txn, state, dat_tx_time, branches, created_ms, updated_ms"

    def __init__(self, path: str | Path, *, wall_ms: Callable[[], int] = _system_ms) -> None:
        self._wall_ms = wall_ms
        self._lock = threading.Lock()
        self._db = sqlite3.connect(
            str(path), timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(_SCHEMA)

    def begin(self, dat_txn: uuid.UUID, branches: Sequence[BranchRecord]) -> JournalRecord:
        now = self._wall_ms()
        record = JournalRecord(dat_txn, State.ACTIVE, tuple(branches), None, now, now)
        with self._lock:
            try:
                self._db.execute(
                    f"INSERT INTO dat_journal ({self._COLUMNS}) VALUES (?, ?, NULL, ?, ?, ?)",
                    (str(dat_txn), State.ACTIVE.value, encode_branches(branches), now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise JournalConflict(f"meta-transaction {dat_txn} is already journaled") from exc
        return record

    def transition(
        self,
        dat_txn: uuid.UUID,
        expected: State,
        new: State,
        *,
        dat_tx_time: int | None = None,
    ) -> JournalRecord:
        check_transition(expected, new, dat_tx_time)
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._db.execute(
                    "UPDATE dat_journal SET state = ?, "
                    "dat_tx_time = COALESCE(?, dat_tx_time), updated_ms = ? "
                    "WHERE dat_txn = ? AND state = ?",
                    (new.value, dat_tx_time, self._wall_ms(), str(dat_txn), expected.value),
                )
                record = self._select(dat_txn)
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        if cursor.rowcount != 1 or record is None:
            raise conflict_error(dat_txn, expected, record.state if record else None)
        return record

    def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
        with self._lock:
            return self._select(dat_txn)

    def pending(self, *, updated_before_ms: int, limit: int | None = None) -> list[JournalRecord]:
        marks = ", ".join("?" for _ in _UNFINISHED)
        with self._lock:
            rows = self._db.execute(
                f"SELECT {self._COLUMNS} FROM dat_journal "
                f"WHERE state IN ({marks}) AND updated_ms < ? "
                "ORDER BY updated_ms, created_ms LIMIT ?",
                (
                    *(s.value for s in _UNFINISHED),
                    updated_before_ms,
                    -1 if limit is None else limit,
                ),
            ).fetchall()
        return [self._row(r) for r in rows]

    def forget(self, dat_txn: uuid.UUID) -> None:
        with self._lock:
            record = self._select(dat_txn)
            if record is None:
                return
            if record.state not in _FINAL:
                raise ValueError(
                    f"cannot forget meta-transaction {dat_txn} in state {record.state}"
                )
            self._db.execute(
                "DELETE FROM dat_journal WHERE dat_txn = ? AND state IN (?, ?)",
                (str(dat_txn), *(s.value for s in _FINAL)),
            )

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _select(self, dat_txn: uuid.UUID) -> JournalRecord | None:
        row = self._db.execute(
            f"SELECT {self._COLUMNS} FROM dat_journal WHERE dat_txn = ?", (str(dat_txn),)
        ).fetchone()
        return self._row(row) if row else None

    @staticmethod
    def _row(row: tuple[str, str, int | None, str, int, int]) -> JournalRecord:
        dat_txn, state, dat_tx_time, branches, created_ms, updated_ms = row
        return JournalRecord(
            dat_txn=uuid.UUID(dat_txn),
            state=State(state),
            branches=decode_branches(branches),
            dat_tx_time=dat_tx_time,
            created_ms=created_ms,
            updated_ms=updated_ms,
        )


def open_journal(dsn: str | None, *, wall_ms: Callable[[], int] = _system_ms) -> Journal:
    """Open the journal named by ``dsn``. There is no default: a missing journal is an error."""
    if not dsn:
        raise ValueError(
            "meta-transactions with several branches need a journal: "
            "pass journal=<dsn> pointing to PostgreSQL or drevo (file:/// for development)"
        )
    parsed = parse_dsn(dsn)
    if parsed.store == "file":
        assert parsed.path is not None
        return SqliteJournal(parsed.path, wall_ms=wall_ms)
    if parsed.store == "postgres":
        from datorium.adapters.postgres import PostgresJournal

        return PostgresJournal(dsn, wall_ms=wall_ms)
    if parsed.store == "drevo":
        from datorium.adapters.drevo import DrevoJournal

        return DrevoJournal(dsn, wall_ms=wall_ms)
    raise ValueError(
        f"{parsed.store} cannot hold the journal: it must be a shared ACID store "
        "(PostgreSQL or drevo)"
    )
