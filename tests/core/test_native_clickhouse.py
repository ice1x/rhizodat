"""The native coordinator over the real Rust ClickHouse adapter (plan step 5).

The real Rust one-phase branch and participant of ``crates/rhizodat-clickhouse``
behind ``rhizodat_native.Coordinator``: a Unit of Work of native stores runs the
commit protocol through the core against the compose ClickHouse, with no Python
callbacks on the path. A single store commits one-phase; a ClickHouse store (one
phase, applied after the decision) and a PostgreSQL store commit together as one
meta-transaction through a journal -- ClickHouse has no journal of its own.

Install the module with ``uv sync --group native`` (needs a Rust toolchain) and
point the tests at the compose ClickHouse.
"""

import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest

native = pytest.importorskip("rhizodat_native")


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


# --- a single native ClickHouse store --------------------------------------------------


def test_single_clickhouse_store_commits_natively(clickhouse_dsn: str) -> None:
    store = native.ClickHouseStore(clickhouse_dsn, "clickhouse")
    store.install()
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [store])
    assert outcome["status"] == "committed"
    assert outcome["dat_txn"] == txn
    assert isinstance(outcome["dat_tx_time"], int)


def test_a_replay_of_a_committed_txn_is_idempotent(clickhouse_dsn: str) -> None:
    store = native.ClickHouseStore(clickhouse_dsn, "clickhouse")
    store.install()
    txn = native.new_id()
    first = native.Coordinator().commit(txn, [store])
    assert first["status"] == "committed"
    # The same dat_txn again: the marker is already there, so the re-run sees the
    # branch applied and succeeds without applying it twice.
    second = native.Coordinator().commit(txn, [store])
    assert second["status"] == "committed"


# --- a ClickHouse store and a PostgreSQL store, one meta-transaction --------------------


def test_clickhouse_and_postgres_commit_through_a_journal(
    clickhouse_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    ch = native.ClickHouseStore(clickhouse_dsn, "clickhouse")
    ch.install()
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [ch, pg])
    assert outcome["status"] == "committed"
    # The PostgreSQL branch's marker proves it committed; the journal proves both
    # (ClickHouse is applied after the decision, from the journal).
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(
            f'SELECT 1 FROM "{pg_schema}".dat_applied WHERE dat_txn = %s', (txn,)
        ).fetchall()
    assert rows
    record = journal.get(txn)
    assert record["state"] == "DONE"
    assert record["dat_txn"] == txn


def test_a_meta_transaction_needs_a_journal(
    clickhouse_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    ch = native.ClickHouseStore(clickhouse_dsn, "clickhouse")
    ch.install()
    pg = native.PostgresStore(pg_dsn, "pg", make_schema())
    pg.install()
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator().commit(native.new_id(), [ch, pg])
