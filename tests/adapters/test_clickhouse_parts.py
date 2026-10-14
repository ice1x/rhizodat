"""ClickHouse as a home for entity parts: append-only typed tables. The behaviour is
the parts contract (tests/contract/test_parts_contracts.py)."""

from collections.abc import Iterator

import pytest
from faker import Faker

from datorium.adapters.clickhouse import ClickHouseStore
from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord
from datorium.testing.parts import CONTRACT_PART, commit, part_values


@pytest.fixture
def store(clickhouse_dsn: str) -> Iterator[ClickHouseStore]:
    s = ClickHouseStore(clickhouse_dsn, name="parts")
    s.install()
    s.install_part(CONTRACT_PART)
    yield s
    s.close()


def test_a_part_table_has_typed_columns(store: ClickHouseStore) -> None:
    rows = store.execute(f"DESCRIBE TABLE {CONTRACT_PART.table}").all()
    assert {r["name"]: r["type"] for r in rows} == {
        "dat_id": "UUID",
        "dat_txn": "UUID",
        "dat_tx_time": "Int64",
        "dat_created": "Int64",
        "dat_deleted": "Bool",
        "title": "Nullable(String)",
        "points": "Nullable(Int64)",
        "estimate": "Nullable(String)",  # tagged JSON: a Decimal keeps its exact scale
        "due": "Nullable(Date32)",
        "done": "Nullable(Bool)",
        "opened": "Nullable(DateTime64(6, 'UTC'))",
        "owner": "Nullable(UUID)",
        "tags": "Nullable(String)",
        "blob": "Nullable(String)",
        "ratio": "Nullable(Float64)",
        "watchers": "Nullable(String)",
        "meta": "Nullable(String)",
        "note": "Nullable(String)",
    }


def test_versions_are_ordered_by_entity_and_time(store: ClickHouseStore) -> None:
    row = store.execute(
        "SELECT engine, sorting_key FROM system.tables"
        " WHERE database = currentDatabase() AND name = {t:String}",
        {"t": CONTRACT_PART.table},
    ).one()
    assert (row["engine"], row["sorting_key"]) == ("MergeTree", "dat_id, dat_tx_time")


def test_install_is_idempotent_across_store_objects(
    store: ClickHouseStore, clickhouse_dsn: str
) -> None:
    other = ClickHouseStore(clickhouse_dsn, name="parts")
    other.install_part(CONTRACT_PART)
    other.close()


def test_a_late_erase_keeps_versions_written_after_it(store: ClickHouseStore, faker: Faker) -> None:
    """An erase finished late (by recovery) removes only versions older than itself."""
    dat_id, later = new_id(), part_values(faker)
    commit(
        store, lambda b: b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None), 1
    )
    txn = new_id()
    erase = store.branch(txn)
    erase.erase_part(CONTRACT_PART, dat_id)
    record = BranchRecord("parts", gid_for(txn, "parts"), False, erase.stage(txn, 2 << 16))
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, later, created=None), 3 << 16)

    store.participant().finish(record, txn, 2 << 16)
    assert [dict(v.values) for v in store.part_history(CONTRACT_PART, dat_id)] == [later]


def test_an_applied_branch_is_not_applied_again(store: ClickHouseStore, faker: Faker) -> None:
    dat_id, txn = new_id(), new_id()
    branch = store.branch(txn)
    branch.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)
    record = BranchRecord("parts", gid_for(txn, "parts"), False, branch.stage(txn, 5 << 16))
    store.participant().finish(record, txn, 5 << 16)
    store.execute(
        f"DELETE FROM {CONTRACT_PART.table} WHERE dat_id = {{i:UUID}}",
        {"i": dat_id},
    )

    store.participant().finish(record, txn, 5 << 16)  # the marker says it is done
    assert store.part_history(CONTRACT_PART, dat_id) == []
