"""Version preconditions: an entity that was read carries ``dat_version``, and an
``update`` (or a ``delete`` with ``expected_version``) of a version that is no
longer the latest raises ``VersionConflict`` instead of losing a write."""

# @entity adds dat_id and the service fields at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined, call-arg, arg-type, assignment"

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from rhizodat import Rhizodat, VersionConflict
from rhizodat.core.journal import MemoryJournal
from rhizodat.core.mapping import entity
from rhizodat.core.parts import LocksParts, Part
from rhizodat.core.repository import EntityNotFound
from rhizodat.testing.memory import MemoryEntityStore


@entity(stores={"pg": ["name", "email"]})
class Customer:
    name: str
    email: str


@entity(stores={"pg": ["title"]}, soft_delete=True)
class Note:
    title: str


@pytest.fixture
def stores() -> dict[str, MemoryEntityStore]:
    return {"pg": MemoryEntityStore("pg")}


@pytest.fixture
def dat(stores: dict[str, MemoryEntityStore]) -> Iterator[Rhizodat]:
    d = Rhizodat(stores=dict(stores), journal=MemoryJournal())
    yield d
    d.close()


def customer(faker: Faker) -> Customer:
    return Customer(name=faker.name(), email=faker.email())


def test_a_read_entity_carries_the_version_it_was_read_at(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    added = customers.add(customer(faker))
    got = customers.get(added.value.dat_id)
    assert got is not None
    assert got.dat_version == added.dat_tx_time
    assert added.value.dat_version == added.dat_tx_time
    history = customers.history(got.dat_id)
    assert [v.dat_version for v in history] == [added.dat_tx_time]


def test_an_entity_built_by_hand_has_no_version(faker: Faker) -> None:
    assert customer(faker).dat_version is None


def test_the_version_is_not_part_of_equality(dat: Rhizodat, faker: Faker) -> None:
    c = customer(faker)
    dat.repository(Customer).add(c)
    assert dat.repository(Customer).get(c.dat_id) == c


def test_an_update_of_a_stale_read_is_refused(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    first, second = customers.get(c.dat_id), customers.get(c.dat_id)
    assert first is not None
    assert second is not None

    first.email = faker.email()
    customers.update(first)
    second.name = faker.name()
    with pytest.raises(VersionConflict) as refused:
        customers.update(second)

    latest = customers.get(c.dat_id)
    assert latest is not None
    assert latest.email == first.email
    assert latest.name == c.name
    assert refused.value.dat_id == c.dat_id
    assert refused.value.expected == second.dat_version
    assert refused.value.actual == latest.dat_version


def test_updating_the_returned_entity_again_passes(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    customers.add(customer(faker))
    current = customers.get(customers.add(customer(faker)).value.dat_id)
    assert current is not None
    for _ in range(3):
        current.email = faker.email()
        current = customers.update(current).value
    got = customers.get(current.dat_id)
    assert got is not None
    assert got.email == current.email
    assert len(customers.history(current.dat_id)) == 4


def test_an_entity_without_a_version_is_written_unchecked(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    stale = customers.get(c.dat_id)
    assert stale is not None
    customers.update(Customer(dat_id=c.dat_id, name=faker.name(), email=faker.email()))
    stale.dat_version = None
    customers.update(stale)  # last writer wins, as before preconditions
    assert len(customers.history(c.dat_id)) == 3


def test_save_checks_the_version_of_an_existing_entity(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    stale = customers.get(c.dat_id)
    assert stale is not None
    customers.update(Customer(dat_id=c.dat_id, name=faker.name(), email=faker.email()))
    with pytest.raises(VersionConflict):
        customers.save(stale)


def test_a_versioned_update_of_a_deleted_entity_is_a_conflict(dat: Rhizodat, faker: Faker) -> None:
    notes = dat.repository(Note)
    n = notes.add(Note(title=faker.sentence())).value
    notes.delete(n.dat_id)
    with pytest.raises(VersionConflict):
        notes.update(n)
    hand_made = Note(dat_id=n.dat_id, title=faker.sentence())
    with pytest.raises(EntityNotFound):
        notes.update(hand_made)


def test_a_delete_with_an_expected_version_checks_it(dat: Rhizodat, faker: Faker) -> None:
    notes = dat.repository(Note)
    n = notes.add(Note(title=faker.sentence())).value
    moved = notes.update(Note(dat_id=n.dat_id, title=faker.sentence())).value
    with pytest.raises(VersionConflict):
        notes.delete(n.dat_id, expected_version=n.dat_version)
    assert notes.get(n.dat_id) is not None
    notes.delete(n.dat_id, expected_version=moved.dat_version)
    assert notes.get(n.dat_id) is None


def test_a_conflict_is_not_retried_inside_a_transaction(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    stale = customers.get(c.dat_id)
    assert stale is not None
    customers.update(Customer(dat_id=c.dat_id, name=faker.name(), email=faker.email()))
    calls = []

    @dat.transactional
    def write(uow: Any) -> None:
        calls.append(1)
        uow.repository(Customer).update(stale)

    with pytest.raises(VersionConflict):
        write()
    assert calls == [1]


def test_a_unit_of_work_checks_an_entity_once(dat: Rhizodat, faker: Faker) -> None:
    """Two writes of one read entity in one Unit of Work: the second follows the
    first (its own write), not the committed version."""
    customers = dat.repository(Customer)
    c = customers.add(customer(faker)).value

    @dat.transactional
    def write(uow: Any) -> None:
        repo = uow.repository(Customer)
        c.email = faker.email()
        repo.update(c)
        c.name = faker.name()
        repo.update(c)

    write()
    got = customers.get(c.dat_id)
    assert got is not None
    assert (got.name, got.email) == (c.name, c.email)


class Locking(MemoryEntityStore):
    """A memory store whose branches record the locks taken, in order with reads."""

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.events: list[tuple[str, uuid.UUID]] = []

    def branch(self, dat_txn: uuid.UUID) -> Any:
        inner = super().branch(dat_txn)
        events = self.events

        class Branch(type(inner)):  # type: ignore[misc]
            def lock_part(self, part: Part, dat_id: uuid.UUID) -> None:
                events.append(("lock", dat_id))

            def read_part(self, part: Part, dat_id: uuid.UUID) -> Any:
                events.append(("read", dat_id))
                return super().read_part(part, dat_id)

        inner.__class__ = Branch
        return inner


def test_a_write_locks_the_entity_before_reading_its_version(faker: Faker) -> None:
    store = Locking("pg")
    dat = Rhizodat(stores={"pg": store}, journal=MemoryJournal())
    try:
        customers = dat.repository(Customer)
        c = customers.add(customer(faker)).value
        assert isinstance(store.branch(uuid.uuid4()), LocksParts)
        store.events.clear()
        c.email = faker.email()
        customers.update(c)
        assert store.events[0] == ("lock", c.dat_id)
        assert ("read", c.dat_id) in store.events[1:]
        # An unchecked write of an existing entity locks it too, so it cannot slip
        # in between another writer's check and commit; adding a new one does not.
        store.events.clear()
        customers.update(Customer(dat_id=c.dat_id, name=faker.name(), email=faker.email()))
        assert ("lock", c.dat_id) in store.events
        store.events.clear()
        fresh = customers.add(customer(faker)).value
        assert ("lock", fresh.dat_id) not in store.events
        store.events.clear()
        customers.delete(fresh.dat_id)
        assert ("lock", fresh.dat_id) in store.events
    finally:
        dat.close()
