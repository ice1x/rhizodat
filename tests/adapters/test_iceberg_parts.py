"""Iceberg as a home for entity parts: one row per version in a table of the parts
namespace, written as a data file at stage and committed after the decision. A hard
delete rewrites the files that held the entity. The behaviour is the parts contract
(tests/contract/test_parts_contracts.py)."""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from datorium.adapters.iceberg import IcebergStore
from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord
from datorium.core.staged import decode_staged
from datorium.testing.parts import CONTRACT_PART, commit, part_values
from iceberg_parts import iceberg_parts_dsn, iceberg_parts_store


@pytest.fixture(params=["sql", "rest"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[IcebergStore]:
    with iceberg_parts_store(iceberg_parts_dsn(request, tmp_path), name="parts") as s:
        s.install_part(CONTRACT_PART)
        yield s


def table_name(store: IcebergStore) -> str:
    return f"{store.parts_namespace}.{CONTRACT_PART.table}"


def rows(store: IcebergStore) -> list[dict[str, Any]]:
    return [dict(r) for r in store.execute(table_name(store)).all()]


def tags(store: IcebergStore) -> list[str | None]:
    table = store.catalog.load_table(table_name(store))
    return [s.summary.get("dat.txn") if s.summary else None for s in table.snapshots()]


def live_files(store: IcebergStore) -> set[str]:
    table = store.catalog.load_table(table_name(store))
    return {task.file.file_path for task in table.scan().plan_files()}


def test_the_part_is_a_table_of_the_parts_namespace(store: IcebergStore) -> None:
    schema = store.catalog.load_table(table_name(store)).schema()
    types = {f.name: str(f.field_type) for f in schema.fields}
    assert types == {
        "dat_id": "string",
        "dat_created": "long",
        "dat_deleted": "boolean",
        "title": "string",
        "points": "long",
        "estimate": "string",  # tagged JSON: a Decimal of unknown precision
        "due": "date",
        "done": "boolean",
        "opened": "timestamptz",
        "owner": "string",  # an id, as text: Arrow cannot filter Iceberg's uuid yet
        "tags": "string",
        "blob": "binary",
        "ratio": "double",
        "watchers": "string",
        "meta": "string",
        "note": "string",
        "dat_txn": "string",
        "dat_tx_time": "long",
    }


def test_a_version_is_a_row_with_native_and_tagged_values(
    store: IcebergStore, faker: Faker
) -> None:
    dat_id, v = new_id(), part_values(faker)
    txn = commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 3 << 16)

    [row] = rows(store)
    assert (row["dat_id"], row["dat_txn"], row["dat_tx_time"], row["dat_created"]) == (
        str(dat_id),
        str(txn),
        3 << 16,
        3 << 16,
    )
    assert row["dat_deleted"] is False
    assert (row["title"], row["points"], row["blob"]) == (v["title"], v["points"], v["blob"])
    assert row["owner"] == str(v["owner"])
    assert json.loads(row["estimate"]) == {"$": "decimal", "v": "3.50"}
    assert json.loads(row["tags"]) == v["tags"]
    assert row["note"] is None


def test_each_committed_branch_is_tagged(store: IcebergStore, faker: Faker) -> None:
    dat_id = new_id()
    first = commit(
        store,
        lambda b: b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None),
        1 << 16,
    )

    def replace(b: Any) -> None:
        b.erase_part(CONTRACT_PART, dat_id)
        b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)

    second = commit(store, replace, 2 << 16)
    # The erase and the new file commit together: every snapshot carries its branch.
    assert set(tags(store)) == {str(first), str(second)}


def test_a_staged_erase_travels_in_the_journal_payload(store: IcebergStore, faker: Faker) -> None:
    gone, txn = new_id(), new_id()
    branch = store.branch(txn)
    branch.erase_part(CONTRACT_PART, gone)
    branch.write_part(CONTRACT_PART, new_id(), part_values(faker), created=None)
    [staged] = decode_staged(branch.stage(txn, 2 << 16))
    assert staged.table == table_name(store)
    assert staged.erase == (str(gone),)
    assert len(staged.files) == 1
    branch.rollback()


def test_an_erase_rewrites_the_files_that_held_the_entity(
    store: IcebergStore, faker: Faker
) -> None:
    gone, kept = new_id(), new_id()

    def both(b: Any) -> None:
        b.write_part(CONTRACT_PART, gone, part_values(faker), created=None)
        b.write_part(CONTRACT_PART, kept, part_values(faker), created=None)

    commit(store, both, 1 << 16)
    before = live_files(store)
    commit(store, lambda b: b.erase_part(CONTRACT_PART, gone), 2 << 16)

    assert not live_files(store) & before
    assert [r["dat_id"] for r in rows(store)] == [str(kept)]


def test_a_late_erase_never_removes_a_later_write(store: IcebergStore, faker: Faker) -> None:
    dat_id, txn = new_id(), new_id()
    branch = store.branch(txn)
    branch.erase_part(CONTRACT_PART, dat_id)
    payload = branch.stage(txn, 3 << 16)
    later = part_values(faker)
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, later, created=None), 5 << 16)

    record = BranchRecord(store.name, gid_for(txn, store.name), False, payload)
    store.participant().finish(record, txn, 3 << 16)  # recovery, after the later write

    got = store.read_part(CONTRACT_PART, dat_id)
    assert got is not None
    assert dict(got.values) == later


def test_an_erase_of_nothing_is_applied_without_a_snapshot(store: IcebergStore) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.erase_part(CONTRACT_PART, new_id())
    payload = branch.stage(txn, 2 << 16)
    branch.commit_one_phase()
    record = BranchRecord(store.name, gid_for(txn, store.name), False, payload)
    assert tags(store) == []
    assert store.participant().is_applied(record, txn)


def test_a_rolled_back_branch_deletes_its_staged_file(store: IcebergStore, faker: Faker) -> None:
    txn = new_id()
    branch = store.branch(txn)
    branch.write_part(CONTRACT_PART, new_id(), part_values(faker), created=None)
    [staged] = decode_staged(branch.stage(txn, 2 << 16))
    io = store.catalog.load_table(table_name(store)).io
    assert io.new_input(staged.files[0]).exists()
    branch.rollback()
    assert not io.new_input(staged.files[0]).exists()


def test_parts_share_the_store_with_appended_tables(store: IcebergStore, faker: Faker) -> None:
    """A branch writes parts and plain appended rows: one payload, both committed."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    namespace = store.parts_namespace
    store.catalog.create_table(
        f"{namespace}.audit",
        Schema(
            NestedField(1, "what", StringType()),
            NestedField(2, "dat_txn", StringType()),
            NestedField(3, "dat_tx_time", LongType()),
        ),
    )
    dat_id = new_id()

    def write(b: Any) -> None:
        b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)
        b.append(f"{namespace}.audit", [{"what": faker.sentence()}])

    commit(store, write, 4 << 16)
    assert store.read_part(CONTRACT_PART, dat_id) is not None
    assert len(store.execute(f"{namespace}.audit").all()) == 1


def test_uuid_ids_are_returned_as_uuids(store: IcebergStore, faker: Faker) -> None:
    dat_id, v = new_id(), part_values(faker)
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 1 << 16)
    got = store.read_part(CONTRACT_PART, dat_id)
    assert got is not None
    assert isinstance(got.dat_txn, uuid.UUID)
    assert got.values["owner"] == v["owner"]
    assert got.values["watchers"] == v["watchers"]
    assert store.expired_parts(CONTRACT_PART, deleted_before=9 << 16, limit=5) == []
