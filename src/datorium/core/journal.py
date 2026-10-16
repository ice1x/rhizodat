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

The journal store also keeps the schema migration history (``MigrationHistory``),
one row per applied migration, in ``<journal table>_migrations``::

    id           BIGINT    primary key, the migration's number
    name         TEXT
    checksum     TEXT      sha256 of the migration's canonical form, hex
    dat_tx_time  BIGINT    HLC when it was recorded
"""

import base64
import datetime
import enum
import json
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, runtime_checkable

from datorium.core.adapter import Adapter, Scheme
from datorium.core.dsn import parse_dsn

__all__ = [
    "AppliedMigration",
    "BranchRecord",
    "Journal",
    "JournalConflict",
    "JournalRecord",
    "MemoryJournal",
    "MigrationHistory",
    "SqliteJournal",
    "State",
    "check_transition",
    "conflict_error",
    "decode_branches",
    "encode_branches",
    "migration_recorded",
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


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    """One applied schema migration as the journal recorded it."""

    id: int
    name: str
    checksum: str
    dat_tx_time: int
    """The HLC timestamp when it was recorded: ``as_of`` reads compare against it."""

    @property
    def applied_at(self) -> "datetime.datetime":
        from datorium.core.parts import hlc_datetime

        return hlc_datetime(self.dat_tx_time)


@runtime_checkable
class MigrationHistory(Protocol):
    """The schema migrations applied so far, kept in the journal store."""

    def applied_migrations(self) -> list[AppliedMigration]:
        """Every recorded migration, by id."""
        ...

    def record_migration(self, applied: AppliedMigration) -> None:
        """Record ``applied``; ``JournalConflict`` if its id is already recorded."""
        ...


def migration_recorded(migration_id: int) -> JournalConflict:
    return JournalConflict(f"migration {migration_id} is already recorded")


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
        self._migrations: dict[int, AppliedMigration] = {}

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

    def applied_migrations(self) -> list[AppliedMigration]:
        with self._lock:
            return sorted(self._migrations.values(), key=lambda m: m.id)

    def record_migration(self, applied: AppliedMigration) -> None:
        with self._lock:
            if applied.id in self._migrations:
                raise migration_recorded(applied.id)
            self._migrations[applied.id] = applied

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
CREATE TABLE IF NOT EXISTS dat_journal_migrations (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    dat_tx_time INTEGER NOT NULL
);
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

    def applied_migrations(self) -> list[AppliedMigration]:
        with self._lock:
            rows = self._db.execute(
                "SELECT id, name, checksum, dat_tx_time FROM dat_journal_migrations ORDER BY id"
            ).fetchall()
        return [AppliedMigration(*r) for r in rows]

    def record_migration(self, applied: AppliedMigration) -> None:
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO dat_journal_migrations (id, name, checksum, dat_tx_time)"
                    " VALUES (?, ?, ?, ?)",
                    (applied.id, applied.name, applied.checksum, applied.dat_tx_time),
                )
            except sqlite3.IntegrityError as exc:
                raise migration_recorded(applied.id) from exc

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
    from datorium.core.adapter import adapter_for, adapters

    able = ", ".join(a.kind for a in adapters() if a.open_journal is not None)
    if not dsn:
        raise ValueError(
            "meta-transactions with several branches need a journal: "
            f"pass journal=<dsn> for a shared ACID store ({able}; file:/// for development)"
        )
    kind = parse_dsn(dsn).store
    open_ = adapter_for(kind).open_journal
    if open_ is None:
        raise ValueError(f"{kind} cannot hold the journal: it must be a shared ACID store ({able})")
    return open_(dsn, wall_ms)


def _open_file_journal(dsn: str, wall_ms: Callable[[], int]) -> Journal:
    path = parse_dsn(dsn).path
    assert path is not None  # the scheme requires one
    return SqliteJournal(path, wall_ms=wall_ms)


FILE_ADAPTER = Adapter(
    kind="file",
    schemes=(Scheme("file", host="none", path="required"),),
    open_journal=_open_file_journal,
)
