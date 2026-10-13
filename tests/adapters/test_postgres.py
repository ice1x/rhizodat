"""PostgreSQL branch, participant and journal against a real server."""

import contextlib
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from datorium.adapters.postgres import PostgresBranch, PostgresJournal, PostgresStore
from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, JournalConflict, State, open_journal


@pytest.fixture
def store(pg_dsn: str, pg_schema: str) -> Iterator[PostgresStore]:
    s = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    s.install()
    with s.connect(autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE orders (id uuid PRIMARY KEY, status text NOT NULL,"
            " amount numeric CHECK (amount >= 0))"
        )
    opened: list[PostgresBranch] = []
    original = s.branch

    def tracked(dat_txn: uuid.UUID, **kw: Any) -> PostgresBranch:
        branch = original(dat_txn, **kw)
        opened.append(branch)
        return branch

    s.branch = tracked  # type: ignore[method-assign]
    yield s
    try:
        for branch in opened:  # tests that fail half-way still release their transactions
            with contextlib.suppress(Exception):
                branch.rollback()
    finally:
        s.close()


def count(store: PostgresStore, sql: str = "SELECT count(*) AS n FROM orders") -> int:
    row = store.execute(sql).one()
    return int(row["n"])


def gids(store: PostgresStore) -> list[str]:
    return store.participant().prepared_gids()


# --- native access -------------------------------------------------------------


def test_execute_returns_a_result_with_columns(store: PostgresStore) -> None:
    result = store.execute("SELECT 1 AS a, 'x' AS b")
    assert result.columns == ("a", "b")
    assert result.all() == [{"a": 1, "b": "x"}]


def test_read_only_reads(store: PostgresStore) -> None:
    result = store.read_only("SELECT %s::int AS a", (7,))
    assert result.columns == ("a",)
    assert result.all() == [{"a": 7}]


def test_read_only_refuses_writes(store: PostgresStore) -> None:
    store.execute("CREATE TABLE notes (body text)")
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        store.read_only("INSERT INTO notes VALUES ('x')")
    for escape in (
        "SET TRANSACTION READ WRITE",
        "COMMIT; INSERT INTO notes VALUES ('x')",
        "SET TRANSACTION READ WRITE; INSERT INTO notes VALUES ('x')",
    ):
        with pytest.raises(psycopg.Error):
            store.read_only(escape)
    assert count(store, "SELECT count(*) AS n FROM notes") == 0


def test_read_only_keeps_a_literal_percent(store: PostgresStore) -> None:
    assert store.read_only("SELECT '100%' AS p").one() == {"p": "100%"}


def test_install_is_idempotent(store: PostgresStore) -> None:
    store.install()
    assert count(store, "SELECT count(*) AS n FROM dat_applied") == 0


def test_dsn_password_is_not_in_repr(pg_dsn: str, pg_schema: str) -> None:
    s = PostgresStore(pg_dsn, schema=pg_schema)
    assert "datorium@" not in repr(s) or ":***@" in repr(s)
    s.close()


def test_schema_name_is_validated(pg_dsn: str) -> None:
    with pytest.raises(ValueError, match="schema"):
        PostgresStore(pg_dsn, schema='x"; DROP TABLE y; --')
    with pytest.raises(ValueError, match="schema"):
        PostgresStore(with_schema(pg_dsn, "a-b"))


def with_schema(dsn: str, schema: str) -> str:
    return f"{dsn}{'&' if '?' in dsn else '?'}schema={schema}"


def tables_in(pg_dsn: str, schema: str) -> set[str]:
    with psycopg.connect(pg_dsn) as conn:
        rows = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,)
        ).fetchall()
    return {r[0] for r in rows}


def test_the_dsn_names_the_schema(pg_dsn: str, pg_schema: str) -> None:
    s = PostgresStore(with_schema(pg_dsn, pg_schema), name="orders")
    s.install()
    s.execute("CREATE TABLE orders (id int)")
    assert tables_in(pg_dsn, pg_schema) == {"dat_applied", "orders"}
    assert s.applied_location[-1] == pg_schema
    assert s.execute("SELECT current_schema() AS s").one()["s"] == pg_schema
    s.close()


def test_the_dsn_schema_and_the_argument_must_agree(pg_dsn: str, pg_schema: str) -> None:
    PostgresStore(with_schema(pg_dsn, pg_schema), schema=pg_schema).close()
    with pytest.raises(ValueError, match="schema"):
        PostgresStore(with_schema(pg_dsn, pg_schema), schema="other")
    with pytest.raises(ValueError, match="schema"):
        PostgresJournal(with_schema(pg_dsn, pg_schema), schema="other")


def test_the_journal_dsn_names_the_schema(pg_dsn: str, pg_schema: str) -> None:
    j = open_journal(with_schema(pg_dsn, pg_schema))
    txn = new_id()
    j.begin(txn, [])
    assert j.get(txn) is not None
    assert "dat_journal" in tables_in(pg_dsn, pg_schema)
    j.close()


# --- one-phase -----------------------------------------------------------------------


def test_one_phase_commit_writes_rows_and_the_applied_marker(store: PostgresStore) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', 10)", {"id": uuid.uuid4()})
    branch.stage(txn, 42)
    branch.commit_one_phase()

    assert count(store) == 1
    record = BranchRecord("pg", gid_for(txn, "pg"), True)
    assert store.participant().is_applied(record, txn) is True
    assert store.participant().is_applied(record, new_id()) is False


def test_rollback_discards_the_open_transaction(store: PostgresStore) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', 10)", {"id": uuid.uuid4()})
    branch.rollback()
    assert count(store) == 0


def test_branch_reads_its_own_writes_only(store: PostgresStore) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', 10)", {"id": uuid.uuid4()})
    assert branch.execute("SELECT count(*) AS n FROM orders").one()["n"] == 1
    assert count(store) == 0
    branch.rollback()


# --- two-phase ------------------------------------------------------------------------


def test_prepare_then_finish_commits(store: PostgresStore) -> None:
    txn = new_id()
    gid = gid_for(txn, "pg")
    branch = store.branch(txn)
    branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', 10)", {"id": uuid.uuid4()})
    branch.stage(txn, 1)
    branch.prepare(gid)
    assert gids(store) == [gid]
    assert count(store) == 0  # invisible until COMMIT PREPARED

    record = BranchRecord("pg", gid, True)
    participant = store.participant()
    participant.finish(record, txn, 1)
    assert count(store) == 1
    assert gids(store) == []
    participant.finish(record, txn, 1)  # idempotent: unknown gid after commit is fine
    assert participant.is_applied(record, txn)


def test_prepared_branch_rollback_releases_it(store: PostgresStore) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', 10)", {"id": uuid.uuid4()})
    branch.stage(txn, 1)
    branch.prepare(gid_for(txn, "pg"))
    branch.rollback()
    assert gids(store) == []
    assert count(store) == 0


def test_participant_abort_is_idempotent(store: PostgresStore) -> None:
    txn = new_id()
    gid = gid_for(txn, "pg")
    branch = store.branch(txn)
    branch.stage(txn, 1)
    branch.prepare(gid)
    record = BranchRecord("pg", gid, True)
    store.participant().abort(record)
    store.participant().abort(record)
    assert gids(store) == []


def test_finish_of_a_gid_that_was_rolled_back_is_an_error(store: PostgresStore) -> None:
    """Unknown gid and no applied marker: someone rolled it back. Never report done."""
    txn = new_id()
    gid = gid_for(txn, "pg")
    branch = store.branch(txn)
    branch.stage(txn, 1)
    branch.prepare(gid)
    record = BranchRecord("pg", gid, True)
    store.participant().abort(record)
    with pytest.raises(RuntimeError, match="heuristic"):
        store.participant().finish(record, txn, 1)


def test_prepared_gids_lists_only_this_database(store: PostgresStore, pg_dsn: str) -> None:
    """Prepared transactions are server-wide; another database's gids are not ours."""
    other_db = f"dat_other_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{other_db}"')
    other_dsn = pg_dsn.rsplit("/", 1)[0] + f"/{other_db}"
    foreign_gid = gid_for(new_id(), "pg")
    try:
        with psycopg.connect(other_dsn) as other:
            other.execute("SELECT 1")
            other.execute(f"PREPARE TRANSACTION '{foreign_gid}'")
        txn = new_id()
        branch = store.branch(txn)
        branch.stage(txn, 1)
        branch.prepare(gid_for(txn, "pg"))
        listed = gids(store)
        assert gid_for(txn, "pg") in listed
        assert foreign_gid not in listed
        branch.rollback()
    finally:
        with (
            psycopg.connect(other_dsn, autocommit=True) as other,
            contextlib.suppress(psycopg.Error),
        ):
            other.execute(f"ROLLBACK PREPARED '{foreign_gid}'")
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE "{other_db}"')


def test_deferred_constraints_fail_at_stage_not_at_prepare(store: PostgresStore) -> None:
    with store.connect(autocommit=True) as conn:
        conn.execute("CREATE TABLE parent (id int PRIMARY KEY)")
        conn.execute("CREATE TABLE child (pid int REFERENCES parent DEFERRABLE INITIALLY DEFERRED)")
    txn = new_id()
    branch = store.branch(txn)
    branch.execute("INSERT INTO child VALUES (1)")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        branch.stage(txn, 1)
    branch.rollback()
    assert gids(store) == []


# --- retryable errors ---------------------------------------------------------------


def test_serialization_failure_and_deadlock_are_retryable(store: PostgresStore) -> None:
    branch = store.branch(new_id())
    assert branch.is_retryable(psycopg.errors.SerializationFailure())
    assert branch.is_retryable(psycopg.errors.DeadlockDetected())
    assert branch.is_retryable(psycopg.OperationalError("connection lost"))
    assert not branch.is_retryable(psycopg.errors.CheckViolation())
    assert not branch.is_retryable(psycopg.errors.SyntaxError())
    branch.rollback()


def test_a_real_check_violation_is_not_retryable(store: PostgresStore) -> None:
    branch = store.branch(new_id())
    with pytest.raises(psycopg.errors.CheckViolation) as err:
        branch.execute("INSERT INTO orders VALUES (%(id)s, 'new', -1)", {"id": uuid.uuid4()})
    assert not branch.is_retryable(err.value)
    branch.rollback()


def test_a_real_serialization_failure_is_retryable(store: PostgresStore) -> None:
    order = uuid.uuid4()
    with store.connect(autocommit=True) as conn:
        conn.execute("INSERT INTO orders VALUES (%s, 'new', 10)", (order,))
    a = store.branch(new_id(), isolation="serializable")
    b = store.branch(new_id(), isolation="serializable")
    a.execute("SELECT * FROM orders WHERE id = %(id)s", {"id": order})
    b.execute("SELECT * FROM orders WHERE id = %(id)s", {"id": order})
    a.execute("UPDATE orders SET status = 'paid' WHERE id = %(id)s", {"id": order})
    a.commit_one_phase()
    with pytest.raises(psycopg.errors.SerializationFailure) as err:
        b.execute("UPDATE orders SET status = 'void' WHERE id = %(id)s", {"id": order})
    assert b.is_retryable(err.value)
    b.rollback()


# --- journal ----------------------------------------------------------------------------


@pytest.fixture
def journal(pg_dsn: str, pg_schema: str) -> Iterator[PostgresJournal]:
    j = PostgresJournal(pg_dsn, schema=pg_schema)
    yield j
    j.close()


def test_journal_happy_path_and_cas(journal: PostgresJournal) -> None:
    txn = new_id()
    branches = [
        BranchRecord("pg", gid_for(txn, "pg"), True),
        BranchRecord("ch", gid_for(txn, "ch"), False, b"x"),
    ]
    journal.begin(txn, branches)
    with pytest.raises(JournalConflict, match="already"):
        journal.begin(txn, branches)
    journal.transition(txn, State.ACTIVE, State.PREPARED, dat_tx_time=5)
    with pytest.raises(JournalConflict, match="PREPARED"):
        journal.transition(txn, State.ACTIVE, State.ABORTED)
    record = journal.transition(txn, State.PREPARED, State.COMMITTED)
    assert (record.state, record.dat_tx_time, record.branches) == (
        State.COMMITTED,
        5,
        tuple(branches),
    )
    assert [r.dat_txn for r in journal.pending(updated_before_ms=record.updated_ms + 1)] == [txn]
    journal.transition(txn, State.COMMITTED, State.DONE)
    journal.forget(txn)
    assert journal.get(txn) is None
