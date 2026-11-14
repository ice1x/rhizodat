"""The PostgreSQL adapter's checks on what it is given, and the journal's
reconnection: what the other PostgreSQL tests do not reach."""

import uuid
from collections.abc import Iterator

import pyarrow as pa
import pytest

from rhizodat.adapters.postgres import (
    PostgresBranch,
    PostgresJournal,
    PostgresStore,
    postgres_schema,
)
from rhizodat.core.dsn import DSNError
from rhizodat.core.identity import new_id
from rhizodat.core.journal import BranchRecord, State
from rhizodat.core.parts import Part


@pytest.fixture
def store(pg_dsn: str, pg_schema: str) -> Iterator[PostgresStore]:
    s = PostgresStore(pg_dsn, name="inventory", schema=pg_schema)
    s.install()
    yield s
    s.close()


def test_a_dsn_of_another_store_is_refused() -> None:
    with pytest.raises(DSNError, match="not a PostgreSQL"):
        postgres_schema("clickhouse://localhost/db")
    with pytest.raises(ValueError, match="not a PostgreSQL"):
        PostgresStore("clickhouse://localhost/db")


def test_a_journal_table_name_must_be_an_identifier(pg_dsn: str, pg_schema: str) -> None:
    with pytest.raises(ValueError, match="invalid table name"):
        PostgresJournal(pg_dsn, schema=pg_schema, table="dat-journal; DROP")


def test_a_part_needs_plain_field_names(store: PostgresStore) -> None:
    part = Part("checks.Odd", "inventory", ("bad name",))
    with pytest.raises(ValueError, match="not a plain identifier"):
        store.install_part(part)


def test_a_part_version_with_an_unknown_field_is_refused(store: PostgresStore) -> None:
    part = Part("checks.Host", "inventory", ("name",))
    store.install_part(part)
    branch = store.branch(new_id())
    try:
        with pytest.raises(ValueError, match=r"\['cores'\] are not fields"):
            branch.write_part(part, uuid.uuid4(), {"name": "web1", "cores": 4}, created=None)
    finally:
        branch.rollback()


def test_a_branch_made_without_its_store_holds_no_parts(store: PostgresStore) -> None:
    branch = PostgresBranch("inventory", store._connector, new_id())
    try:
        with pytest.raises(TypeError, match="without its store"):
            branch.write_part(
                Part("checks.Host", "inventory", ("name",)), uuid.uuid4(), {}, created=None
            )
    finally:
        branch.rollback()


def test_a_load_skips_empty_batches_and_refuses_a_nul_in_a_name(store: PostgresStore) -> None:
    store.execute('CREATE TABLE host ("name" text)')
    empty = pa.RecordBatch.from_pylist([], schema=pa.schema([("name", pa.string())]))
    full = pa.RecordBatch.from_pylist([{"name": "web1"}, {"name": "db1"}])
    assert store.load("host", [empty, full]) == 2
    nul = pa.RecordBatch.from_pylist([{"na\x00me": "x"}])
    with pytest.raises(ValueError, match="holds a NUL"):
        store.load("host", [nul])


def test_the_journal_reconnects_after_its_connection_closes(pg_dsn: str, pg_schema: str) -> None:
    journal = PostgresJournal(pg_dsn, schema=pg_schema)
    txn = new_id()
    journal.begin(txn, [BranchRecord("inventory", "gid", True)])
    journal._conn.close()
    record = journal.get(txn)
    assert record is not None
    assert record.state is State.ACTIVE
    journal.close()


def test_forgetting_an_unknown_transaction_does_nothing(pg_dsn: str, pg_schema: str) -> None:
    journal = PostgresJournal(pg_dsn, schema=pg_schema)
    journal.forget(new_id())
    journal.close()
