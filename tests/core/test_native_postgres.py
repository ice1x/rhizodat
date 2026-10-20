"""The native coordinator over the real Rust PostgreSQL adapter (plan step 5).

The real Rust branch, participant and journal of ``crates/rhizodat-postgres``
behind ``rhizodat_native.Coordinator``: a Unit of Work of native stores runs the
whole commit protocol through the core against the compose PostgreSQL, with no
Python callbacks on the path. One-phase for a single store, two-phase through the
PostgreSQL journal for several, roll back before the decision, the
all-or-nothing property.

A Unit of Work's own rows reach a store through ``store.unit()``: buffer them
with ``unit.insert(table, columns, rows)`` and hand the unit to ``commit`` like a
store, so the rows are written inside the branch's transaction and commit
atomically with the meta-transaction (two-phase). Install the module with
``uv sync --group native`` (needs a Rust toolchain) and point the tests at a
disposable PostgreSQL with ``max_prepared_transactions > 0``.
"""

import uuid
from collections.abc import Callable, Iterator

import psycopg
import pytest
from faker import Faker

native = pytest.importorskip("rhizodat_native")


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh schemas, each one its own store, all dropped afterwards."""
    created: list[str] = []

    def make() -> str:
        name = f"dat_native_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{name}"')
        created.append(name)
        return name

    yield make

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        # A failed test may leave a branch open or a transaction prepared.
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


def _applied(pg_dsn: str, schema: str, dat_txn: uuid.UUID) -> bool:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(
            f'SELECT 1 FROM "{schema}".dat_applied WHERE dat_txn = %s',
            (dat_txn,),
        ).fetchall()
    return bool(rows)


def _prepared(pg_dsn: str) -> int:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchone()
    assert row is not None
    return int(row[0])


# --- a single native store -------------------------------------------------------------


def test_single_store_commits_natively_and_marks_applied(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    store = native.PostgresStore(pg_dsn, "pg", schema)
    store.install()
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [store])
    assert outcome["status"] == "committed"
    assert outcome["dat_txn"] == txn
    assert isinstance(outcome["dat_tx_time"], int)
    assert _applied(pg_dsn, schema, txn)


def test_a_replay_of_a_committed_txn_is_idempotent(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    store = native.PostgresStore(pg_dsn, "pg", schema)
    store.install()
    txn = native.new_id()
    first = native.Coordinator().commit(txn, [store])
    assert first["status"] == "committed"
    # The same dat_txn again: the marker is already there, so the re-run sees the
    # branch applied and succeeds without inserting it twice.
    second = native.Coordinator().commit(txn, [store])
    assert second["status"] == "committed"
    assert _applied(pg_dsn, schema, txn)


# --- several native stores through the PostgreSQL journal -------------------------------


def test_two_store_meta_transaction_commits_through_the_journal(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema_a, schema_b, journal_schema = make_schema(), make_schema(), make_schema()
    store_a = native.PostgresStore(pg_dsn, "pg_a", schema_a)
    store_b = native.PostgresStore(pg_dsn, "pg_b", schema_b)
    store_a.install()
    store_b.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [store_a, store_b])
    assert outcome["status"] == "committed"
    assert _applied(pg_dsn, schema_a, txn)
    assert _applied(pg_dsn, schema_b, txn)
    record = journal.get(txn)
    assert record["state"] == "DONE"
    assert record["dat_txn"] == txn


def test_a_staging_failure_before_the_decision_commits_nothing(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema_a, schema_b, journal_schema = make_schema(), make_schema(), make_schema()
    store_a = native.PostgresStore(pg_dsn, "pg_a", schema_a)
    store_b = native.PostgresStore(pg_dsn, "pg_b", schema_b)
    store_a.install()
    # store_b has no dat_applied table, so its stage INSERT fails before the decision.
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    with pytest.raises(native.AbortedError) as excinfo:
        native.Coordinator(journal).commit(txn, [store_a, store_b])
    assert excinfo.value.dat_txn == txn
    # All-or-nothing: nobody applied, nothing left prepared, the journal not DONE.
    assert not _applied(pg_dsn, schema_a, txn)
    assert _prepared(pg_dsn) == 0
    record = journal.get(txn)
    assert record is None or record["state"] == "ABORTED"


def test_a_meta_transaction_needs_a_journal(pg_dsn: str, make_schema: Callable[[], str]) -> None:
    store_a = native.PostgresStore(pg_dsn, "pg_a", make_schema())
    store_b = native.PostgresStore(pg_dsn, "pg_b", make_schema())
    store_a.install()
    store_b.install()
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator().commit(native.new_id(), [store_a, store_b])


# --- the Unit of Work's rows across the boundary ----------------------------------------


def _create(pg_dsn: str, schema: str, ddl: str) -> None:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'SET search_path TO "{schema}"')
        conn.execute(ddl)


def _rows(pg_dsn: str, schema: str, sql: str) -> list[tuple[object, ...]]:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'SET search_path TO "{schema}"')
        return list(conn.execute(sql).fetchall())


def test_a_unit_writes_its_rows_stamped_with_the_meta_transaction(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    fake = Faker()
    Faker.seed(7)
    schema = make_schema()
    _create(
        pg_dsn,
        schema,
        "CREATE TABLE people (name text, age int, dat_txn uuid, dat_tx_time bigint)",
    )
    store = native.PostgresStore(pg_dsn, "pg", schema)
    store.install()
    unit = store.unit()
    people = [[fake.name(), fake.random_int(18, 90)] for _ in range(5)]
    unit.insert("people", ["name", "age"], people)
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [unit])
    assert outcome["status"] == "committed"
    rows = _rows(pg_dsn, schema, "SELECT name, age FROM people ORDER BY name")
    assert rows == sorted((name, age) for name, age in people)
    # Every row carries the meta-transaction's stamp.
    stamped = _rows(pg_dsn, schema, "SELECT DISTINCT dat_txn, dat_tx_time FROM people")
    assert stamped == [(txn, outcome["dat_tx_time"])]


def test_a_unit_carries_scalars_bytes_and_temporal_values(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    _create(
        pg_dsn,
        schema,
        "CREATE TABLE items ("
        " price numeric(10,2), ok bool, raw bytea, when_ timestamptz,"
        " note text, dat_txn uuid, dat_tx_time bigint)",
    )
    store = native.PostgresStore(pg_dsn, "pg", schema)
    store.install()
    unit = store.unit()
    unit.insert(
        "items",
        ["price", "ok", "raw", "when_", "note"],
        [
            ["12.34", True, b"\xff\x00o", "2026-10-20T09:30:00+00:00", "hi"],
            [None, None, None, None, None],
        ],
    )
    native.Coordinator().commit(native.new_id(), [unit])
    rows = _rows(
        pg_dsn,
        schema,
        "SELECT price::text, ok, raw, note FROM items ORDER BY note NULLS LAST",
    )
    assert rows[0] == ("12.34", True, b"\xff\x00o", "hi")
    assert rows[1] == (None, None, None, None)


def test_a_unit_and_a_second_store_write_together_through_a_journal(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema_a, schema_b, journal_schema = make_schema(), make_schema(), make_schema()
    _create(
        pg_dsn,
        schema_a,
        "CREATE TABLE orders (sku text, qty int, dat_txn uuid, dat_tx_time bigint)",
    )
    store_a = native.PostgresStore(pg_dsn, "pg_a", schema_a)
    store_a.install()
    store_b = native.PostgresStore(pg_dsn, "pg_b", schema_b)
    store_b.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    unit = store_a.unit()
    unit.insert("orders", ["sku", "qty"], [["A-1", 3], ["B-2", 7]])
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [unit, store_b])
    assert outcome["status"] == "committed"
    assert _rows(pg_dsn, schema_a, "SELECT sku, qty FROM orders ORDER BY sku") == [
        ("A-1", 3),
        ("B-2", 7),
    ]
    assert _applied(pg_dsn, schema_b, txn)
    assert journal.get(txn)["state"] == "DONE"


def test_a_unit_may_be_committed_again_after_a_replay_without_duplicating_rows(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    _create(pg_dsn, schema, "CREATE TABLE log (n int, dat_txn uuid, dat_tx_time bigint)")
    store = native.PostgresStore(pg_dsn, "pg", schema)
    store.install()
    unit = store.unit()
    unit.insert("log", ["n"], [[1], [2]])
    txn = native.new_id()
    first = native.Coordinator().commit(txn, [unit])
    assert first["status"] == "committed"
    # The unit keeps its rows; replaying the same dat_txn is idempotent (the marker
    # conflicts at stage, so nothing is written twice) and leaves the rows intact.
    second = native.Coordinator().commit(txn, [unit])
    assert second["status"] == "committed"
    assert _rows(pg_dsn, schema, "SELECT n FROM log ORDER BY n") == [(1,), (2,)]


def test_a_reserved_column_in_the_rows_is_rejected(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    store = native.PostgresStore(pg_dsn, "pg", make_schema())
    store.install()
    unit = store.unit()
    with pytest.raises(native.InvalidUnitOfWork):
        unit.insert("t", ["dat_txn"], [[native.new_id()]])
