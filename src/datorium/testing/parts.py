"""The contract a store must keep to hold entity parts, run against the real store.

A store that implements ``PartStore`` (and whose branches implement
``PartBranch``) can hold a share of an entity's fields. Subclass
``PartStoreContract`` and provide a ``parts_store`` fixture: a fresh store of
your kind, installed, cleaned up afterwards::

    from datorium.testing.parts import PartStoreContract

    class TestMySqlParts(PartStoreContract):
        @pytest.fixture
        def parts_store(self) -> Iterator[MySqlStore]:
            store = MySqlStore(...)
            yield store
            store.close()

Every write is committed the way the coordinator would: stage, then prepare
and finish for a two-phase branch, or apply after the decision for a one-phase
one (from the journal payload too, the way recovery does).
"""

# @entity adds dat_id at runtime; mypy cannot see it. The store under test is any
# store kind, so it is typed Any.
# mypy: disable-error-code="attr-defined"
# ruff: noqa: ANN401

import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
from faker import Faker

from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord
from datorium.core.mapping import Ref, entity
from datorium.core.parts import Part, PartBranch, PartStore

__all__ = ["CONTRACT_PART", "PartStoreContract", "commit", "part_values"]

STORE = "parts"


@entity(
    stores={
        STORE: [
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
            "note",
        ]
    },
    name="contract.Issue",
)
class ContractIssue:
    title: str
    points: int
    estimate: Decimal
    due: date | None
    done: bool
    opened: datetime
    owner: Ref["ContractIssue"] | None
    tags: list[str]
    blob: bytes
    ratio: float
    watchers: list[Ref["ContractIssue"]]
    meta: dict[str, object]
    note: str | None


CONTRACT_PART = Part.of(ContractIssue, STORE)


def part_values(faker: Faker) -> dict[str, Any]:
    """One version's values, of every type an entity field may have."""
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
        "note": None,
    }


def commit(
    store: Any,
    write: Callable[[Any], None],
    ts: int,
    *,
    via_journal: bool = False,
) -> uuid.UUID:
    """Run ``write(branch)`` and commit it at ``ts`` as the coordinator would.

    ``via_journal``: a one-phase branch is applied by the participant from its
    payload, as recovery does after the process that staged it died.
    """
    txn = new_id()
    branch = store.branch(txn)
    write(branch)
    payload = branch.stage(txn, ts)
    gid = gid_for(txn, store.name)
    if branch.two_phase:
        branch.prepare(gid)
        store.participant().finish(BranchRecord(store.name, gid, True, payload), txn, ts)
    elif via_journal:
        store.participant().finish(BranchRecord(store.name, gid, False, payload), txn, ts)
    else:
        branch.commit_one_phase()
    return txn


class PartStoreContract:
    """The parts contract as pytest tests: subclass it and provide ``parts_store``."""

    @pytest.fixture
    def parts_store(self) -> Iterator[Any]:
        raise NotImplementedError("provide a `parts_store` fixture for your store")

    @pytest.fixture
    def store(self, parts_store: Any) -> Any:
        parts_store.install_part(CONTRACT_PART)
        return parts_store

    def test_the_store_and_its_branches_hold_parts(self, store: Any) -> None:
        assert isinstance(store, PartStore)
        branch = store.branch(new_id())
        assert isinstance(branch, PartBranch)
        branch.rollback()

    def test_install_is_idempotent(self, store: Any) -> None:
        store.install_part(CONTRACT_PART)

    def test_values_round_trip_exactly(self, store: Any, faker: Faker) -> None:
        dat_id, v = new_id(), part_values(faker)
        txn = commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 7 << 16)

        got = store.read_part(CONTRACT_PART, dat_id)
        assert got is not None
        assert dict(got.values) == v
        assert (got.dat_txn, got.dat_tx_time, got.created, got.deleted) == (
            txn,
            7 << 16,
            7 << 16,
            False,
        )

    def test_unknown_fields_are_refused(self, store: Any, faker: Faker) -> None:
        branch = store.branch(new_id())
        with pytest.raises(ValueError, match="not fields"):
            branch.write_part(CONTRACT_PART, new_id(), {"nope": faker.word()}, created=None)
        branch.rollback()

    def test_a_missing_entity_reads_as_none(self, store: Any) -> None:
        assert store.read_part(CONTRACT_PART, new_id()) is None
        assert store.part_history(CONTRACT_PART, new_id()) == []

    def test_a_branch_reads_its_own_writes_others_do_not(self, store: Any, faker: Faker) -> None:
        dat_id = new_id()
        branch = store.branch(new_id())
        branch.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)

        mine = branch.read_part(CONTRACT_PART, dat_id)
        assert mine is not None
        assert mine.dat_tx_time is None
        assert store.read_part(CONTRACT_PART, dat_id) is None
        other = store.branch(new_id())
        assert other.read_part(CONTRACT_PART, dat_id) is None
        other.rollback()
        branch.rollback()
        assert store.read_part(CONTRACT_PART, dat_id) is None

    def test_a_branch_reads_committed_versions(self, store: Any, faker: Faker) -> None:
        dat_id, v = new_id(), part_values(faker)
        commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 3 << 16)
        branch = store.branch(new_id())
        seen = branch.read_part(CONTRACT_PART, dat_id)
        assert seen is not None
        assert dict(seen.values) == v
        branch.erase_part(CONTRACT_PART, dat_id)
        assert branch.read_part(CONTRACT_PART, dat_id) is None
        branch.rollback()

    def test_writing_twice_in_one_branch_keeps_the_last(self, store: Any, faker: Faker) -> None:
        dat_id, first, second = new_id(), part_values(faker), part_values(faker)

        def write(b: Any) -> None:
            b.write_part(CONTRACT_PART, dat_id, first, created=None)
            b.write_part(CONTRACT_PART, dat_id, second, created=None)

        commit(store, write, 9 << 16)
        assert [dict(v.values) for v in store.part_history(CONTRACT_PART, dat_id)] == [second]

    def test_versions_known_at_and_history(self, store: Any, faker: Faker) -> None:
        dat_id, v1, v2 = new_id(), part_values(faker), part_values(faker)
        commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v1, created=None), 10 << 16)
        commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v2, created=10 << 16), 20 << 16)

        assert store.read_part(CONTRACT_PART, dat_id, known_at=(10 << 16) - 1) is None
        at_15 = store.read_part(CONTRACT_PART, dat_id, known_at=15 << 16)
        assert at_15 is not None
        assert dict(at_15.values) == v1
        latest = store.read_part(CONTRACT_PART, dat_id)
        assert latest is not None
        assert dict(latest.values) == v2
        assert latest.created == 10 << 16
        assert [v.dat_tx_time for v in store.part_history(CONTRACT_PART, dat_id)] == [
            10 << 16,
            20 << 16,
        ]

    def test_a_soft_delete_is_a_version(self, store: Any, faker: Faker) -> None:
        dat_id, v = new_id(), part_values(faker)
        commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 1 << 16)
        commit(
            store,
            lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=1 << 16, deleted=True),
            2 << 16,
        )
        assert [v.deleted for v in store.part_history(CONTRACT_PART, dat_id)] == [False, True]

    def test_erase_removes_every_version(self, store: Any, faker: Faker) -> None:
        dat_id, kept = new_id(), new_id()
        for i, target in enumerate((dat_id, kept, dat_id)):

            def write(b: Any, t: uuid.UUID = target) -> None:
                b.write_part(CONTRACT_PART, t, part_values(faker), created=1 << 16)

            commit(store, write, (i + 1) << 16)
        commit(store, lambda b: b.erase_part(CONTRACT_PART, dat_id), 5 << 16)
        assert store.part_history(CONTRACT_PART, dat_id) == []
        assert store.read_part(CONTRACT_PART, dat_id) is None
        assert len(store.part_history(CONTRACT_PART, kept)) == 1

    def test_a_write_after_an_erase_in_one_branch_survives(self, store: Any, faker: Faker) -> None:
        dat_id, v = new_id(), part_values(faker)
        commit(
            store,
            lambda b: b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None),
            1 << 16,
        )

        def write(b: Any) -> None:
            b.erase_part(CONTRACT_PART, dat_id)
            b.write_part(CONTRACT_PART, dat_id, v, created=None)

        commit(store, write, 2 << 16)
        assert [dict(x.values) for x in store.part_history(CONTRACT_PART, dat_id)] == [v]

    def test_an_erase_after_a_write_in_one_branch_wins(self, store: Any, faker: Faker) -> None:
        dat_id = new_id()

        def write(b: Any) -> None:
            b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)
            b.erase_part(CONTRACT_PART, dat_id)

        commit(store, write, 2 << 16)
        assert store.part_history(CONTRACT_PART, dat_id) == []

    def test_a_rolled_back_branch_leaves_nothing(self, store: Any, faker: Faker) -> None:
        dat_id = new_id()
        commit(
            store,
            lambda b: b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None),
            1 << 16,
        )
        branch = store.branch(new_id())
        branch.write_part(CONTRACT_PART, dat_id, part_values(faker), created=1 << 16)
        branch.erase_part(CONTRACT_PART, dat_id)
        branch.rollback()
        assert len(store.part_history(CONTRACT_PART, dat_id)) == 1

    def test_finishing_twice_applies_once(self, store: Any, faker: Faker) -> None:
        dat_id, txn, ts = new_id(), new_id(), 4 << 16
        branch = store.branch(txn)
        branch.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None)
        payload = branch.stage(txn, ts)
        record = BranchRecord(store.name, gid_for(txn, store.name), branch.two_phase, payload)
        if branch.two_phase:
            branch.prepare(record.gid)
        assert store.read_part(CONTRACT_PART, dat_id) is None  # nothing before the decision
        store.participant().finish(record, txn, ts)
        store.participant().finish(record, txn, ts)

        history = store.part_history(CONTRACT_PART, dat_id)
        assert [v.dat_tx_time for v in history] == [ts]
        assert store.participant().is_applied(record, txn)

    def test_a_one_phase_branch_is_applied_from_its_payload(self, store: Any, faker: Faker) -> None:
        dat_id, v = new_id(), part_values(faker)
        commit(
            store,
            lambda b: b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=None),
            1 << 16,
        )

        def write(b: Any) -> None:
            b.erase_part(CONTRACT_PART, dat_id)
            b.write_part(CONTRACT_PART, dat_id, v, created=None)

        commit(store, write, 2 << 16, via_journal=True)
        assert [dict(x.values) for x in store.part_history(CONTRACT_PART, dat_id)] == [v]

    def test_expired_parts_are_latest_soft_deletes_older_than_the_bound(
        self, store: Any, faker: Faker
    ) -> None:
        old, recent, revived = new_id(), new_id(), new_id()

        def version(dat_id: uuid.UUID, ts: int, *, deleted: bool = False) -> None:
            def write(b: Any) -> None:
                b.write_part(CONTRACT_PART, dat_id, part_values(faker), created=1, deleted=deleted)

            commit(store, write, ts)

        for dat_id in (old, recent, revived):
            version(dat_id, 1 << 16)
        version(old, 5 << 16, deleted=True)
        version(revived, 5 << 16, deleted=True)
        version(revived, 6 << 16)
        version(recent, 9 << 16, deleted=True)

        assert store.expired_parts(CONTRACT_PART, deleted_before=8 << 16, limit=10) == [old]
        assert store.expired_parts(CONTRACT_PART, deleted_before=8 << 16, limit=0) == []
