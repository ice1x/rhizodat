"""PostgreSQL as a home for entity parts: typed versioned tables, branch reads, erase."""

# @entity adds dat_id at runtime; mypy cannot see it.
# mypy: disable-error-code="attr-defined"

import uuid
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from faker import Faker

from datorium.adapters.postgres import PostgresStore
from datorium.core.identity import new_id
from datorium.core.mapping import Ref, entity
from datorium.core.parts import Part, PartBranch, PartStore


@entity(
    stores={
        "pg": [
            "title",
            "points",
            "estimate",
            "due",
            "done",
            "opened",
            "owner",
            "tags",
            "blob",
            "ratio",
            "watchers",
            "meta",
        ]
    },
    name="tracker.Issue",
)
class Issue:
    title: str
    points: int
    estimate: Decimal
    due: date | None
    done: bool
    opened: datetime
    owner: Ref["Issue"] | None
    tags: list[str]
    blob: bytes
    ratio: float
    watchers: list[Ref["Issue"]]
    meta: dict[str, object]


PART = Part.of(Issue, "pg")


@pytest.fixture
def store(pg_dsn: str, pg_schema: str) -> Iterator[PostgresStore]:
    s = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    s.install()
    s.install_part(PART)
    yield s
    s.close()


def values(faker: Faker) -> dict[str, object]:
    return {
        "title": faker.sentence(),
        "points": faker.pyint(),
        "estimate": Decimal("3.50"),
        "due": faker.date_object(),
        "done": faker.pybool(),
        "opened": faker.date_time(tzinfo=UTC),
        "owner": uuid.uuid4(),
        "tags": faker.words(3),
        "blob": faker.binary(16),
        "ratio": 0.25,
        "watchers": [uuid.uuid4(), uuid.uuid4()],
        "meta": {"source": faker.word(), "n": 2},
    }


def commit(store: PostgresStore, write: object, ts: int) -> uuid.UUID:
    txn = new_id()
    branch = store.branch(txn)
    write(branch)  # type: ignore[operator]
    branch.stage(txn, ts)
    branch.commit_one_phase()
    return txn


def test_the_store_and_its_branches_hold_parts(store: PostgresStore) -> None:
    assert isinstance(store, PartStore)
    branch = store.branch(new_id())
    assert isinstance(branch, PartBranch)
    branch.rollback()


def test_a_part_table_has_typed_columns(store: PostgresStore, pg_schema: str) -> None:
    assert PART.table == "tracker_issue"
    rows = store.execute(
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s",
        (pg_schema, PART.table),
    ).all()
    types = {r["column_name"]: r["data_type"] for r in rows}
    assert types == {
        "dat_id": "uuid",
        "dat_txn": "uuid",
        "dat_tx_time": "bigint",
        "dat_created": "bigint",
        "dat_deleted": "boolean",
        "title": "text",
        "points": "bigint",
        "estimate": "numeric",
        "due": "date",
        "done": "boolean",
        "opened": "timestamp with time zone",
        "owner": "uuid",
        "tags": "jsonb",
        "blob": "bytea",
        "ratio": "double precision",
        "watchers": "jsonb",
        "meta": "jsonb",
    }


def test_install_is_idempotent(store: PostgresStore, pg_dsn: str, pg_schema: str) -> None:
    store.install_part(PART)
    other = PostgresStore(pg_dsn, name="pg", schema=pg_schema)  # its own install cache
    other.install_part(PART)
    other.close()


def test_values_round_trip_exactly(store: PostgresStore, faker: Faker) -> None:
    dat_id, v = new_id(), values(faker)
    txn = commit(store, lambda b: b.write_part(PART, dat_id, v, created=None), 7 << 16)

    got = store.read_part(PART, dat_id)
    assert got is not None
    assert dict(got.values) == v
    assert (got.dat_txn, got.dat_tx_time, got.created, got.deleted) == (
        txn,
        7 << 16,
        7 << 16,
        False,
    )


def test_a_branch_reads_its_own_writes_others_do_not(store: PostgresStore, faker: Faker) -> None:
    dat_id = new_id()
    branch = store.branch(new_id())
    branch.write_part(PART, dat_id, values(faker), created=None)

    mine = branch.read_part(PART, dat_id)
    assert mine is not None
    assert mine.dat_tx_time is None
    assert store.read_part(PART, dat_id) is None
    other = store.branch(new_id())
    assert other.read_part(PART, dat_id) is None
    other.rollback()
    branch.rollback()


def test_writing_twice_in_one_branch_keeps_the_last(store: PostgresStore, faker: Faker) -> None:
    dat_id, first, second = new_id(), values(faker), values(faker)

    def write(b: object) -> None:
        b.write_part(PART, dat_id, first, created=None)
        b.write_part(PART, dat_id, second, created=None)

    commit(store, write, 9 << 16)
    assert [dict(v.values) for v in store.part_history(PART, dat_id)] == [second]


def test_versions_known_at_and_history(store: PostgresStore, faker: Faker) -> None:
    dat_id, v1, v2 = new_id(), values(faker), values(faker)
    commit(store, lambda b: b.write_part(PART, dat_id, v1, created=None), 10 << 16)
    commit(store, lambda b: b.write_part(PART, dat_id, v2, created=10 << 16), 20 << 16)

    assert store.read_part(PART, dat_id, known_at=(10 << 16) - 1) is None
    assert dict(store.read_part(PART, dat_id, known_at=15 << 16).values) == v1  # type: ignore[union-attr]
    latest = store.read_part(PART, dat_id)
    assert latest is not None
    assert dict(latest.values) == v2
    assert latest.created == 10 << 16
    assert [v.dat_tx_time for v in store.part_history(PART, dat_id)] == [10 << 16, 20 << 16]


def test_erase_removes_every_version(store: PostgresStore, faker: Faker) -> None:
    dat_id = new_id()
    commit(store, lambda b: b.write_part(PART, dat_id, values(faker), created=None), 1 << 16)
    commit(store, lambda b: b.write_part(PART, dat_id, values(faker), created=1 << 16), 2 << 16)
    commit(store, lambda b: b.erase_part(PART, dat_id), 3 << 16)
    assert store.part_history(PART, dat_id) == []


def test_expired_parts_are_latest_soft_deletes_older_than_the_bound(
    store: PostgresStore, faker: Faker
) -> None:
    old, recent, revived = new_id(), new_id(), new_id()

    def version(dat_id: uuid.UUID, ts: int, *, deleted: bool = False) -> None:
        def write(b: object) -> None:
            b.write_part(PART, dat_id, values(faker), created=1, deleted=deleted)

        commit(store, write, ts)

    for dat_id in (old, recent, revived):
        version(dat_id, 1 << 16)
    version(old, 5 << 16, deleted=True)
    version(revived, 5 << 16, deleted=True)
    version(revived, 6 << 16)
    version(recent, 9 << 16, deleted=True)

    assert store.expired_parts(PART, deleted_before=8 << 16, limit=10) == [old]
    assert store.expired_parts(PART, deleted_before=8 << 16, limit=0) == []


def test_a_two_phase_branch_publishes_parts_at_commit_prepared(
    store: PostgresStore, faker: Faker
) -> None:
    from datorium.core.coordinator import gid_for
    from datorium.core.journal import BranchRecord

    dat_id, txn = new_id(), new_id()
    branch = store.branch(txn)
    branch.write_part(PART, dat_id, values(faker), created=None)
    branch.stage(txn, 4 << 16)
    gid = gid_for(txn, "pg")
    branch.prepare(gid)
    assert store.read_part(PART, dat_id) is None

    store.participant().finish(BranchRecord("pg", gid, True, None), txn, 4 << 16)
    got = store.read_part(PART, dat_id)
    assert got is not None
    assert got.dat_tx_time == 4 << 16
