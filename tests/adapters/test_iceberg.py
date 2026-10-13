"""Iceberg branch and participant on a SQLite catalog with a local warehouse.

Iceberg commits optimistically: a branch writes Parquet data files at stage
(invisible: no snapshot references them), the journal carries their paths, and
after the decision one snapshot per table adds them, tagged with ``dat.txn``.
"""

import datetime as dt
import json
import uuid
from collections.abc import Iterator, Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from faker import Faker
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
    UUIDType,
)

from datorium.adapters.iceberg import IcebergBranch, IcebergStore, catalog_properties
from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord

TS = 1_760_000_000_000 << 16

SALES = Schema(
    NestedField(1, "order_id", UUIDType(), required=True),
    NestedField(2, "sku", StringType(), required=True),
    NestedField(3, "qty", LongType()),
    NestedField(4, "price", DecimalType(12, 2)),
    NestedField(5, "at", TimestamptzType()),
    NestedField(6, "dat_txn", UUIDType()),
    NestedField(7, "dat_tx_time", LongType()),
)


@pytest.fixture(params=["sql", "rest"])
def dsn(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    """A SQLite catalog with a local warehouse, and a REST catalog over S3 (compose)."""
    if request.param == "rest":
        return str(request.getfixturevalue("iceberg_rest_dsn"))
    return f"iceberg+sql://{tmp_path / 'catalog.db'}?warehouse=file://{tmp_path / 'warehouse'}"


def drop_namespace(s: IcebergStore) -> None:
    """The REST catalog is shared by every test: leave it empty."""
    if ("erp",) not in s.catalog.list_namespaces():
        return
    for identifier in s.catalog.list_tables("erp"):
        s.catalog.drop_table(identifier)
    s.catalog.drop_namespace("erp")


@pytest.fixture
def store(dsn: str) -> Iterator[IcebergStore]:
    s = IcebergStore(dsn, name="lake")
    drop_namespace(s)
    s.catalog.create_namespace("erp")
    s.catalog.create_table("erp.sales", SALES)
    yield s
    try:
        drop_namespace(s)
    finally:
        s.close()


def sales(faker: Faker, n: int) -> list[dict[str, Any]]:
    return [
        {
            "order_id": uuid.uuid4(),
            "sku": faker.bothify("SKU-####"),
            "qty": faker.pyint(1, 9),
            "price": Decimal(faker.numerify("##.##")),
            "at": faker.date_time(tzinfo=dt.UTC),
        }
        for _ in range(n)
    ]


def rows(store: IcebergStore, table: str = "erp.sales") -> list[Mapping[str, Any]]:
    return store.execute(table).all()


def snapshots(store: IcebergStore, table: str = "erp.sales") -> int:
    return len(store.catalog.load_table(table).snapshots())


def record(branch: IcebergBranch, payload: bytes | None) -> BranchRecord:
    return BranchRecord("lake", gid_for(branch.dat_txn, "lake"), False, payload)


def staged_files(payload: bytes) -> list[tuple[str, str]]:
    return [(t["table"], f) for t in json.loads(payload)["tables"] for f in t["files"]]


def existing(store: IcebergStore, payload: bytes) -> int:
    """How many of the payload's data files exist, through the table's FileIO (local or S3)."""
    return sum(
        store.catalog.load_table(table).io.new_input(path).exists()
        for table, path in staged_files(payload)
    )


# --- connection strings -----------------------------------------------------------


def test_sqlite_catalog_from_a_path() -> None:
    props = catalog_properties("iceberg+sql:///srv/catalog.db?warehouse=s3://bucket/wh")
    assert props == {
        "type": "sql",
        "uri": "sqlite:////srv/catalog.db",
        "warehouse": "s3://bucket/wh",
    }


def test_postgres_catalog_from_a_host(faker: Faker) -> None:
    password = faker.password(special_chars=False)
    props = catalog_properties(
        f"iceberg+sql://cat:{password}@db.local:5433/lake?warehouse=file:///w"
    )
    assert props["uri"] == f"postgresql+psycopg://cat:{password}@db.local:5433/lake"


def test_rest_catalog_passes_other_parameters_through() -> None:
    props = catalog_properties(
        "iceberg+rest://catalog.local:8181/prod?secure=true&s3.endpoint=http://minio:9000"
    )
    assert props == {
        "type": "rest",
        "uri": "https://catalog.local:8181",
        "warehouse": "prod",
        "s3.endpoint": "http://minio:9000",
    }


def test_glue_is_not_supported_yet() -> None:
    with pytest.raises(NotImplementedError, match="glue"):
        catalog_properties("iceberg+glue://eu-west-1/lake")


def test_not_an_iceberg_dsn_is_rejected() -> None:
    with pytest.raises(ValueError, match="iceberg"):
        IcebergStore("postgresql://localhost/x")


def test_secrets_are_not_in_repr(tmp_path: Path) -> None:
    s = IcebergStore(f"iceberg+sql://{tmp_path / 'c.db'}?warehouse=file://{tmp_path}&token=abc")
    assert "abc" not in repr(s)
    s.close()


# --- native access ----------------------------------------------------------------


def test_execute_scans_a_table_with_a_filter(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 3))
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    result = store.execute("erp.sales", {"row_filter": "qty >= 1", "selected_fields": ("sku",)})
    assert result.columns == ("sku",)
    assert len(result.all()) == 3


def test_read_only_is_a_scan(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    assert len(store.read_only("erp.sales", {"selected_fields": ("sku",)}).all()) == 2


# --- branch -------------------------------------------------------------------------


def test_staged_files_are_invisible(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 3))
    payload = branch.stage(branch.dat_txn, TS)
    assert existing(store, payload) == 1
    assert rows(store) == []
    assert snapshots(store) == 0


def test_one_phase_commit_adds_one_tagged_snapshot(store: IcebergStore, faker: Faker) -> None:
    data = sales(faker, 3)
    branch = store.branch(new_id())
    branch.append("erp.sales", data)
    payload = branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    [snapshot] = store.catalog.load_table("erp.sales").snapshots()
    assert snapshot.summary is not None
    assert snapshot.summary["dat.txn"] == str(branch.dat_txn)
    assert snapshot.summary["dat.tx_time"] == str(TS)
    got = sorted(rows(store), key=lambda r: r["sku"])
    assert [r["sku"] for r in got] == sorted(r["sku"] for r in data)
    assert {r["dat_txn"] for r in got} == {branch.dat_txn}
    assert {r["dat_tx_time"] for r in got} == {TS}
    assert store.participant().is_applied(record(branch, payload), branch.dat_txn)
    # Without the payload the participant cannot know which tables to look at.
    assert store.participant().is_applied(record(branch, None), branch.dat_txn) is False


def test_arrow_tables_are_accepted(store: IcebergStore, faker: Faker) -> None:
    data = sales(faker, 2)
    arrow = pa.Table.from_pylist(
        [{**r, "order_id": r["order_id"].bytes} for r in data],
        schema=pa.schema(
            [
                pa.field("order_id", pa.uuid(), nullable=False),
                pa.field("sku", pa.string(), nullable=False),
                pa.field("qty", pa.int64()),
                pa.field("price", pa.decimal128(12, 2)),
                pa.field("at", pa.timestamp("us", tz="UTC")),
            ]
        ),
    )
    branch = store.branch(new_id())
    branch.append("erp.sales", arrow)
    branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    assert len(rows(store)) == 2


def test_appends_to_one_table_become_one_file(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    branch.append("erp.sales", sales(faker, 1))
    payload = branch.stage(branch.dat_txn, TS)
    assert len(staged_files(payload)) == 1
    assert existing(store, payload) == 1


def test_reserved_columns_are_set_by_datorium(store: IcebergStore, faker: Faker) -> None:
    data = [{**r, "dat_txn": uuid.uuid4()} for r in sales(faker, 1)]
    with pytest.raises(ValueError, match="dat_txn"):
        store.branch(new_id()).append("erp.sales", data)


def test_stage_rejects_rows_that_do_not_fit_the_schema(store: IcebergStore, faker: Faker) -> None:
    data = [{**r, "qty": "many"} for r in sales(faker, 1)]
    branch = store.branch(new_id())
    branch.append("erp.sales", data)
    with pytest.raises(ValueError, match=r"erp\.sales"):
        branch.stage(branch.dat_txn, TS)


def test_stage_rejects_an_unknown_column(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", [{**r, "colour": "red"} for r in sales(faker, 1)])
    with pytest.raises(ValueError, match="colour"):
        branch.stage(branch.dat_txn, TS)


def test_stage_rejects_a_table_without_transaction_columns(
    store: IcebergStore, faker: Faker
) -> None:
    store.catalog.create_table("erp.plain", Schema(NestedField(1, "sku", StringType())))
    branch = store.branch(new_id())
    branch.append("erp.plain", [{"sku": "x"}])
    with pytest.raises(ValueError, match="dat_txn"):
        branch.stage(branch.dat_txn, TS)


def test_stage_fails_for_a_missing_table(store: IcebergStore, faker: Faker) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    branch = store.branch(new_id())
    branch.append("erp.nope", sales(faker, 1))
    with pytest.raises(NoSuchTableError) as caught:
        branch.stage(branch.dat_txn, TS)
    assert not branch.is_retryable(caught.value)


def test_rollback_deletes_staged_files(store: IcebergStore, faker: Faker) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    payload = branch.stage(branch.dat_txn, TS)
    branch.rollback()
    assert existing(store, payload) == 0
    branch.rollback()  # idempotent


def test_rollback_after_an_applied_commit_keeps_the_files(
    store: IcebergStore, faker: Faker
) -> None:
    """A lost commit ack must never turn into deleting files a snapshot references."""
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    payload = branch.stage(branch.dat_txn, TS)
    branch.commit_one_phase()
    branch.rollback()
    assert existing(store, payload) == 1
    assert len(rows(store)) == 2


def test_rollback_keeps_files_when_the_outcome_is_unknown(
    store: IcebergStore, faker: Faker
) -> None:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    payload = branch.stage(branch.dat_txn, TS)
    participant = store.participant()

    def lost(*a: object, **k: object) -> None:
        raise ConnectionError("catalog unreachable")

    participant.finish = lost  # type: ignore[method-assign]
    participant.is_applied = lambda *a, **k: (_ for _ in ()).throw(ConnectionError())  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        branch.commit_one_phase()
    branch.rollback()
    assert existing(store, payload) == 1  # left for orphan-file maintenance, never deleted blindly


def test_branch_is_not_two_phase(store: IcebergStore) -> None:
    branch = store.branch(new_id())
    assert branch.two_phase is False
    with pytest.raises(TypeError, match="two-phase"):
        branch.prepare(gid_for(branch.dat_txn, "lake"))


# --- participant ----------------------------------------------------------------------


def staged(store: IcebergStore, faker: Faker, n: int = 3) -> tuple[IcebergBranch, bytes]:
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, n))
    payload = branch.stage(branch.dat_txn, TS)
    return branch, payload


def test_finish_commits_from_the_journal_payload(
    store: IcebergStore, dsn: str, faker: Faker
) -> None:
    branch, payload = staged(store, faker)
    elsewhere = IcebergStore(dsn, name="lake")  # e.g. recovery on another host
    participant = elsewhere.participant()
    assert not participant.is_applied(record(branch, payload), branch.dat_txn)
    participant.finish(record(branch, payload), branch.dat_txn, TS)
    assert len(rows(store)) == 3
    assert participant.is_applied(record(branch, payload), branch.dat_txn)
    elsewhere.close()


def test_finish_is_idempotent(store: IcebergStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    for _ in range(3):
        store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert len(rows(store)) == 3
    assert snapshots(store) == 1


def test_finish_after_the_tag_snapshot_expired_is_still_idempotent(
    store: IcebergStore, faker: Faker
) -> None:
    """Files already referenced mean applied, even when the tagged snapshot is gone."""
    branch, payload = staged(store, faker)
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    later, later_payload = staged(store, faker, n=1)
    store.participant().finish(record(later, later_payload), later.dat_txn, TS)
    table = store.catalog.load_table("erp.sales")
    first = table.snapshots()[0].snapshot_id
    table.maintenance.expire_snapshots().by_id(first).commit()
    assert snapshots(store) == 1
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert len(rows(store)) == 4
    assert store.participant().is_applied(record(branch, payload), branch.dat_txn)


def test_finish_after_compaction_is_still_idempotent(store: IcebergStore, faker: Faker) -> None:
    """Rewritten files no longer list ours; the snapshot tag still says it is applied."""
    branch, payload = staged(store, faker)
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    table = store.catalog.load_table("erp.sales")
    table.overwrite(table.scan().to_arrow())  # compaction-like rewrite into new files
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert len(rows(store)) == 3


def test_abort_never_deletes_files_a_snapshot_references(store: IcebergStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    store.participant().abort(record(branch, payload))
    assert existing(store, payload) == 1
    assert len(rows(store)) == 3


def test_finish_skips_tables_already_committed(store: IcebergStore, faker: Faker) -> None:
    store.catalog.create_table("erp.audit", SALES)
    branch = store.branch(new_id())
    branch.append("erp.sales", sales(faker, 2))
    branch.append("erp.audit", sales(faker, 1))
    payload = branch.stage(branch.dat_txn, TS)
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    store.participant().finish(record(branch, payload), branch.dat_txn, TS)
    assert (snapshots(store, "erp.sales"), snapshots(store, "erp.audit")) == (1, 1)


def test_abort_deletes_staged_files(store: IcebergStore, faker: Faker) -> None:
    branch, payload = staged(store, faker)
    participant = store.participant()
    participant.abort(record(branch, payload))
    participant.abort(record(branch, payload))  # idempotent
    assert existing(store, payload) == 0
    assert participant.prepared_gids() == []


def test_transient_errors_are_retryable(store: IcebergStore) -> None:
    from pyiceberg.exceptions import (
        CommitFailedException,
        CommitStateUnknownException,
        NoSuchTableError,
        ServiceUnavailableError,
    )

    participant = store.participant()
    assert participant.is_retryable(CommitFailedException("concurrent commit"))
    assert participant.is_retryable(CommitStateUnknownException("timeout"))
    assert participant.is_retryable(ServiceUnavailableError("503"))
    assert participant.is_retryable(ConnectionError())
    assert not participant.is_retryable(NoSuchTableError("erp.x"))
    assert not participant.is_retryable(ValueError("schema"))
