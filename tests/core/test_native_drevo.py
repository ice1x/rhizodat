"""The native coordinator over the real Rust drevo adapter (plan step 5).

The real Rust branch, participant and journal of ``crates/rhizodat-drevo`` behind
``rhizodat_native.Coordinator``, over Bolt: a Unit of Work of native stores runs
the whole commit protocol through the core against the compose drevo, with no
Python callbacks on the path. A single store commits one-phase; a drevo store and
a PostgreSQL store commit together as one meta-transaction through a journal,
which is also the first cross-adapter native Unit of Work.

Install the module with ``uv sync --group native`` (needs a Rust toolchain) and
point the tests at a disposable drevo server (2PC raises a prepared fence).
"""

import contextlib
import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest

native = pytest.importorskip("rhizodat_native")


@pytest.fixture(autouse=True)
def _unfence(drevo_bolt_dsn: str) -> Iterator[None]:
    """Best-effort: after each test, roll back any prepared branch left on the
    shared graph, so a failure never fences the graph for the next test."""
    yield
    with contextlib.suppress(Exception):
        from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore
        from rhizodat.core.journal import BranchRecord

        store = PyDrevoStore(drevo_bolt_dsn, name="graph")
        try:
            participant = store.participant()
            for gid in participant.prepared_gids():
                if gid.startswith("dat:"):
                    participant.abort(BranchRecord("graph", gid, True))
        finally:
            store.close()


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh PostgreSQL schemas, dropped afterwards."""
    created: list[str] = []

    def make() -> str:
        name = f"dat_native_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{name}"')
        created.append(name)
        return name

    yield make

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
            " AND state LIKE 'idle in transaction%%'"
        )
        conn.execute("SET lock_timeout = '10s'")
        for (gid,) in conn.execute(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchall():
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
        for name in created:
            conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def _store_name() -> str:
    """A store name unique to the test, so branches never share a gid on Bolt."""
    return f"g{uuid.uuid4().hex[:10]}"


# --- a single native drevo store -------------------------------------------------------


def test_single_drevo_store_commits_natively(drevo_bolt_dsn: str) -> None:
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [store])
    assert outcome["status"] == "committed"
    assert outcome["dat_txn"] == txn
    assert isinstance(outcome["dat_tx_time"], int)


def test_a_replay_of_a_committed_txn_is_idempotent(drevo_bolt_dsn: str) -> None:
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    txn = native.new_id()
    first = native.Coordinator().commit(txn, [store])
    assert first["status"] == "committed"
    # The same dat_txn again: the marker node is already there, so the re-run sees
    # the branch applied and succeeds without writing it twice.
    second = native.Coordinator().commit(txn, [store])
    assert second["status"] == "committed"


# --- a drevo store and a PostgreSQL store, one meta-transaction -------------------------


def test_drevo_and_postgres_commit_through_a_journal(
    drevo_bolt_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    graph = native.DrevoStore(drevo_bolt_dsn, _store_name())
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [graph, pg])
    assert outcome["status"] == "committed"
    # The PostgreSQL branch's marker proves it committed; the journal proves both.
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(
            f'SELECT 1 FROM "{pg_schema}".dat_applied WHERE dat_txn = %s', (txn,)
        ).fetchall()
    assert rows
    record = journal.get(txn)
    assert record["state"] == "DONE"
    assert record["dat_txn"] == txn


def test_a_meta_transaction_needs_a_journal(
    drevo_bolt_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    graph = native.DrevoStore(drevo_bolt_dsn, _store_name())
    pg = native.PostgresStore(pg_dsn, "pg", make_schema())
    pg.install()
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator().commit(native.new_id(), [graph, pg])
