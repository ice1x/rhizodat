"""Recovery on the native core (``rhizodat_native.Recovery``, plan step 5).

The core's recovery worker over a native journal and the participants of native
stores (and of Python objects with the participant methods): undecided stale
records are presumed aborted and rolled back, committed ones are finished and
marked ``DONE``, prepared gids without a decision are rolled back.

The Docker-free tests leave a committed but unfinished meta-transaction in the
in-memory journal with fake participants. The rest crash a real coordinator: a
child process commits a PostgreSQL unit together with a Python branch that kills
the process (``os._exit``) before or after the decision, leaving the PostgreSQL
branch prepared; native recovery in this process then rolls it back or forward.
Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

import subprocess
import sys
import textwrap
import time
import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.core.coordinator import RecoveryReport

native = pytest.importorskip("rhizodat_native")

HOUR_MS = 3_600_000


def later() -> int:
    """A wall clock an hour ahead, so every record is stale."""
    return time.time_ns() // 1_000_000 + HOUR_MS


class FakeParticipant:
    """A Python store's branch and participant; ``finish`` fails while ``broken``."""

    def __init__(self, name: str, *, broken: bool = False) -> None:
        self._name = name
        self.broken = broken
        self.calls: list[str] = []
        self.applied = False

    def store(self) -> str:
        return self._name

    def two_phase(self) -> bool:
        return True

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        return None

    def prepare(self, gid: str) -> None:
        self.calls.append("prepare")

    def commit_one_phase(self) -> None:
        raise AssertionError("two-phase")

    def rollback(self) -> None:
        self.calls.append("rollback")

    def finish(self, payload: bytes | None, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        self.calls.append("finish")
        if self.broken:
            raise RuntimeError("store unreachable")
        self.applied = True

    def is_applied(self, dat_txn: uuid.UUID) -> bool:
        return self.applied

    def abort(self) -> None:
        self.calls.append("abort")

    def prepared_gids(self) -> list[str]:
        return []


# --- Docker-free ------------------------------------------------------------------------


def test_stale_after_must_be_positive() -> None:
    with pytest.raises(ValueError, match="stale_after_ms"):
        native.Recovery(native.MemoryJournal(), [], stale_after_ms=0)


def test_only_native_journals_are_accepted() -> None:
    with pytest.raises(native.InvalidUnitOfWork, match="journal"):
        native.Recovery(object(), [])


def test_an_empty_journal_reports_nothing() -> None:
    report = native.Recovery(native.MemoryJournal(), []).run_once()
    assert report == {"aborted": [], "finished": [], "unfinished": [], "stuck": [], "orphans": []}


def _pending(journal: object) -> tuple[uuid.UUID, FakeParticipant, FakeParticipant]:
    """A meta-transaction committed but left unfinished: ``b`` cannot finish."""
    a, b = FakeParticipant("a"), FakeParticipant("b", broken=True)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [a, b])
    assert outcome["status"] == "pending"
    return txn, a, b


def test_a_committed_meta_transaction_is_finished() -> None:
    journal = native.MemoryJournal()
    txn, a, b = _pending(journal)
    assert journal.get(txn)["state"] == "COMMITTED"
    b.broken = False
    report = native.Recovery(journal, [a, b], wall_ms=later).run_once()
    assert report["finished"] == [txn]
    assert b.applied
    assert journal.get(txn)["state"] == "DONE"


def test_fresh_records_are_left_to_their_coordinator() -> None:
    journal = native.MemoryJournal()
    txn, a, b = _pending(journal)
    b.broken = False
    report = native.Recovery(journal, [a, b]).run_once()
    assert report["finished"] == []
    assert journal.get(txn)["state"] == "COMMITTED"


def test_a_participant_still_failing_leaves_it_unfinished_then_stuck() -> None:
    journal = native.MemoryJournal()
    txn, a, b = _pending(journal)
    recovery = native.Recovery(journal, [a, b], alert_after_ms=1, wall_ms=later)
    report = recovery.run_once()
    # The participant's error does not fail the run: it is reported, and a later
    # run retries.
    assert report["unfinished"] == [txn]
    assert report["stuck"] == [txn]
    assert journal.get(txn)["state"] == "COMMITTED"
    b.broken = False
    assert recovery.run_once()["finished"] == [txn]


def test_a_failing_wall_clock_fails_the_run() -> None:
    def broken_clock() -> int:
        raise OSError("no clock")

    with pytest.raises(OSError, match="no clock"):
        native.Recovery(native.MemoryJournal(), [], wall_ms=broken_clock).run_once()


def test_the_facade_needs_a_journal_for_recovery() -> None:
    stores = {"pg": native.PostgresStore("postgresql://u:p@127.0.0.1:5432/db", "pg")}
    with Rhizodat(stores=stores) as r, pytest.raises(ValueError, match="journal"):
        r.recovery()


# --- a real coordinator crash over PostgreSQL --------------------------------------------


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh PostgreSQL schemas; prepared gids and schemas dropped after."""
    created: list[str] = []

    def make() -> str:
        name = f"dat_native_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{name}"')
        created.append(name)
        return name

    yield make

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute("SET lock_timeout = '10s'")
        for (gid,) in conn.execute(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchall():
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
        for name in created:
            conn.execute(f'DROP SCHEMA "{name}" CASCADE')


_CRASH = textwrap.dedent(
    """
    import os, sys, uuid
    import rhizodat_native as native

    dsn, schema, journal_schema, txn, sku, crash_in = sys.argv[1:]

    class Crash:
        def store(self): return "py"
        def two_phase(self): return True
        def stage(self, dat_txn, dat_tx_time): return None
        def prepare(self, gid):
            if crash_in == "prepare": os._exit(3)
        def commit_one_phase(self): pass
        def rollback(self): pass
        def finish(self, payload, dat_txn, dat_tx_time):
            if crash_in == "finish": os._exit(3)
        def is_applied(self, dat_txn): return False
        def abort(self): pass
        def prepared_gids(self): return []

    pg = native.PostgresStore(dsn, "pg", schema)
    pg.install()
    unit = pg.unit()
    unit.insert("orders", ["sku"], [[sku]])
    journal = native.PostgresJournal(dsn, journal_schema)
    # Prepare runs in this order and finish holds it too: PostgreSQL is prepared
    # before the crash in "prepare", and still prepared at the crash in "finish".
    order = [unit, Crash()] if crash_in == "prepare" else [Crash(), unit]
    native.Coordinator(journal).commit(uuid.UUID(txn), order)
    sys.exit(0)
    """
)


class Done:
    """The Python store of the crashed meta-transaction, already in its final state."""

    def store(self) -> str:
        return "py"

    def finish(self, payload: bytes | None, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        pass

    def is_applied(self, dat_txn: uuid.UUID) -> bool:
        return True

    def abort(self) -> None:
        pass

    def prepared_gids(self) -> list[str]:
        return []


def _crash(pg_dsn: str, schema: str, journal_schema: str, crash_in: str, sku: str) -> uuid.UUID:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE TABLE "{schema}".orders (sku text, dat_txn uuid, dat_tx_time bigint)')
    txn: uuid.UUID = native.new_id()
    child = subprocess.run(
        [sys.executable, "-c", _CRASH, pg_dsn, schema, journal_schema, str(txn), sku, crash_in],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert child.returncode == 3, child.stderr
    return txn


def _prepared(pg_dsn: str, txn: uuid.UUID) -> bool:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT 1 FROM pg_prepared_xacts WHERE gid = %s", (native.gid_for(txn, "pg"),)
        ).fetchone()
    return row is not None


def _orders(pg_dsn: str, schema: str) -> list[tuple[object, ...]]:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        return conn.execute(f'SELECT sku, dat_txn FROM "{schema}".orders').fetchall()


def test_a_crash_before_the_decision_is_rolled_back(
    pg_dsn: str, make_schema: Callable[[], str], faker: Faker
) -> None:
    schema, journal_schema = make_schema(), make_schema()
    txn = _crash(pg_dsn, schema, journal_schema, "prepare", faker.bothify("SKU-####"))
    assert _prepared(pg_dsn, txn), "the crash left PostgreSQL prepared"
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    pg = native.PostgresStore(pg_dsn, "pg", schema)
    report = native.Recovery(journal, [pg, Done()], wall_ms=later).run_once()
    assert report["aborted"] == [txn]
    assert not _prepared(pg_dsn, txn)
    assert _orders(pg_dsn, schema) == []
    assert journal.get(txn)["state"] == "ABORTED"


def test_a_crash_after_the_decision_is_rolled_forward(
    pg_dsn: str, make_schema: Callable[[], str], faker: Faker
) -> None:
    schema, journal_schema = make_schema(), make_schema()
    sku = faker.bothify("SKU-####")
    txn = _crash(pg_dsn, schema, journal_schema, "finish", sku)
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    assert journal.get(txn)["state"] == "COMMITTED"
    assert _prepared(pg_dsn, txn), "the crash left PostgreSQL prepared"
    assert _orders(pg_dsn, schema) == [], "a prepared branch is invisible"
    pg = native.PostgresStore(pg_dsn, "pg", schema)
    report = native.Recovery(journal, [pg, Done()], wall_ms=later).run_once()
    assert report["finished"] == [txn]
    assert not _prepared(pg_dsn, txn)
    assert _orders(pg_dsn, schema) == [(sku, txn)]
    assert journal.get(txn)["state"] == "DONE"
    # Running again changes nothing: recovery is idempotent.
    again = native.Recovery(journal, [pg, Done()], wall_ms=later).run_once()
    assert txn not in again["finished"] + again["aborted"]
    assert _orders(pg_dsn, schema) == [(sku, txn)]


def test_the_facade_rolls_back_an_orphaned_prepared_branch(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    # A gid prepared with no journal record (a coordinator that died between
    # prepare and begin, as far as the journal knows): the sweep rolls it back.
    schema, journal_schema = make_schema(), make_schema()
    txn = native.new_id()
    gid = native.gid_for(txn, "pg")
    with psycopg.connect(pg_dsn) as conn:
        conn.execute(f'CREATE TABLE "{schema}".t (n int)')
        conn.execute(f'INSERT INTO "{schema}".t VALUES (1)')
        conn.execute(f"PREPARE TRANSACTION '{gid}'")
    assert _prepared(pg_dsn, txn)
    stores = {"pg": native.PostgresStore(pg_dsn, "pg", schema)}
    with Rhizodat(stores=stores, journal=native.PostgresJournal(pg_dsn, journal_schema)) as r:
        report = r.recovery(wall_ms=later).run_once()
    assert isinstance(report, RecoveryReport)
    assert gid in report.orphans
    assert not _prepared(pg_dsn, txn)
