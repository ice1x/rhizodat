"""Repository: entity CRUD across mapped stores, versions, hard and soft delete."""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from faker import Faker

from rhizodat import Rhizodat, UnitOfWork
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.coordinator import Committed
from rhizodat.core.journal import MemoryJournal
from rhizodat.core.mapping import Ref, entity
from rhizodat.core.parts import Part, hlc_bound, hlc_datetime
from rhizodat.core.repository import EntityExists, EntityNotFound, NotRestorable
from rhizodat.testing.memory import MemoryEntityStore

DAY_MS = 86_400_000


@entity(stores={"pg": ["name", "email"]})
class Customer:
    name: str
    email: str


@entity(stores={"pg": ["status", "amount", "customer"], "graph": ["related"]})
class Order:
    status: str
    amount: Decimal
    customer: Ref["Customer"]
    related: list[Ref["Order"]]


@entity(stores={"pg": ["title"]}, soft_delete=True)
class Note:
    title: str


@entity(stores={"pg": ["title"]}, soft_delete=timedelta(days=2))
class Draft:
    title: str


class Wall:
    """A settable wall clock for the HLC, in milliseconds."""

    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms

    def advance(self, ms: int) -> None:
        self.ms += ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def stores() -> dict[str, MemoryEntityStore]:
    return {"pg": MemoryEntityStore("pg"), "graph": MemoryEntityStore("graph")}


@pytest.fixture
def dat(stores: dict[str, MemoryEntityStore], wall: Wall) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores=dict(stores),
        journal=MemoryJournal(),
        clock=HybridLogicalClock(wall_ms=wall),
        soft_delete_window=timedelta(days=7),
    )
    yield d
    d.close()


def customer(faker: Faker) -> Customer:
    return Customer(name=faker.name(), email=faker.email())


def order(faker: Faker, **overrides: object) -> Order:
    fields: dict[str, object] = {
        "status": faker.random_element(["new", "paid", "shipped"]),
        "amount": Decimal(faker.pydecimal(left_digits=4, right_digits=2, positive=True)),
        "customer": Ref[Customer](uuid.uuid4()),
        "related": [Ref[Order](uuid.uuid4()) for _ in range(2)],
    }
    fields.update(overrides)
    return Order(**fields)  # type: ignore[arg-type]


# --- time -------------------------------------------------------------------------------


def test_a_datetime_bounds_every_hlc_timestamp_of_its_millisecond() -> None:
    at = datetime(2026, 10, 14, 12, 0, 0, 123_000, tzinfo=UTC)
    ms = int(at.timestamp() * 1000)
    assert hlc_bound(at) == (ms << 16) | 0xFFFF
    assert hlc_datetime(ms << 16) == at


def test_a_naive_datetime_is_refused() -> None:
    with pytest.raises(ValueError, match="time zone"):
        hlc_bound(datetime(2026, 10, 14))


# --- create and read --------------------------------------------------------------------


def test_add_then_get_returns_the_entity_with_its_times(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    outcome = customers.add(c)
    assert isinstance(outcome, Committed)

    got = customers.get(c.dat_id)
    assert got == c
    assert got is not None
    assert got.dat_created_at == got.dat_updated_at == wall.now()
    assert got.dat_deleted_at is None
    assert outcome.value == got


def test_get_of_an_unknown_id_is_none(dat: Rhizodat) -> None:
    assert dat.repository(Customer).get(uuid.uuid4()) is None


def test_adding_an_existing_entity_fails(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    with pytest.raises(EntityExists):
        customers.add(c)


def test_parts_go_to_their_stores_in_one_meta_transaction(
    dat: Rhizodat, stores: dict[str, MemoryEntityStore], faker: Faker
) -> None:
    o = order(faker)
    outcome = dat.repository(Order).add(o)

    pg = stores["pg"].read_part(Part.of(Order, "pg"), o.dat_id)
    graph = stores["graph"].read_part(Part.of(Order, "graph"), o.dat_id)
    assert pg is not None
    assert graph is not None
    assert set(pg.values) == {"status", "amount", "customer"}
    assert set(graph.values) == {"related"}
    assert pg.dat_txn == graph.dat_txn == outcome.dat_txn
    # References are stored as plain ids and come back as Ref.
    assert pg.values["customer"] == o.customer.dat_id
    assert dat.repository(Order).get(o.dat_id) == o


# --- versions ---------------------------------------------------------------------------


def test_update_writes_a_new_version_and_keeps_the_creation_time(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    created = wall.now()
    wall.advance(1_000)

    c.email = faker.email()
    customers.update(c)

    got = customers.get(c.dat_id)
    assert got is not None
    assert got.email == c.email
    assert got.dat_created_at == created
    assert got.dat_updated_at == wall.now()
    assert [v.email for v in customers.history(c.dat_id)][-1] == c.email
    assert len(customers.history(c.dat_id)) == 2


def test_update_of_a_missing_entity_fails(dat: Rhizodat, faker: Faker) -> None:
    with pytest.raises(EntityNotFound):
        dat.repository(Customer).update(customer(faker))


def test_save_creates_or_updates(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.save(c)
    c.name = faker.name()
    customers.save(c)
    assert customers.get(c.dat_id) == c
    assert len(customers.history(c.dat_id)) == 2


def test_as_of_reads_the_version_known_then(dat: Rhizodat, faker: Faker, wall: Wall) -> None:
    customers = dat.repository(Customer)
    before = wall.now()
    wall.advance(1_000)
    c = customer(faker)
    first_email = c.email
    customers.add(c)
    first = wall.now()
    wall.advance(1_000)
    c.email = faker.email()
    customers.update(c)

    assert customers.get(c.dat_id, as_of=before) is None
    old = customers.get(c.dat_id, as_of=first)
    assert old is not None
    assert old.email == first_email
    assert customers.get(c.dat_id, as_of=wall.now()) == c


def test_history_is_oldest_first_with_the_times_of_each_version(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    times = [wall.now()]
    for _ in range(2):
        wall.advance(500)
        c.name = faker.name()
        customers.update(c)
        times.append(wall.now())

    history = customers.history(c.dat_id)
    assert [v.dat_updated_at for v in history] == times
    assert {v.dat_created_at for v in history} == {times[0]}
    assert history[-1] == c


# --- hard delete ------------------------------------------------------------------------


def test_delete_erases_every_version_in_every_store(
    dat: Rhizodat, stores: dict[str, MemoryEntityStore], faker: Faker
) -> None:
    orders = dat.repository(Order)
    o = order(faker)
    orders.add(o)
    o.status = "paid"
    orders.update(o)

    orders.delete(o.dat_id)

    assert orders.get(o.dat_id) is None
    assert orders.get(o.dat_id, include_deleted=True) is None
    assert orders.history(o.dat_id) == []
    for name in ("pg", "graph"):
        assert stores[name].part_history(Part.of(Order, name), o.dat_id) == []
    orders.add(o)  # the id is free again
    assert orders.get(o.dat_id) == o


def test_delete_of_a_missing_entity_fails(dat: Rhizodat) -> None:
    with pytest.raises(EntityNotFound):
        dat.repository(Customer).delete(uuid.uuid4())


def test_a_hard_deleted_entity_cannot_be_restored(dat: Rhizodat, faker: Faker) -> None:
    customers = dat.repository(Customer)
    c = customer(faker)
    customers.add(c)
    customers.delete(c.dat_id)
    with pytest.raises(NotRestorable):
        customers.restore(c.dat_id)


# --- soft delete ------------------------------------------------------------------------


def test_soft_delete_hides_the_entity_but_keeps_it(dat: Rhizodat, faker: Faker, wall: Wall) -> None:
    notes = dat.repository(Note)
    n = Note(title=faker.sentence())
    notes.add(n)
    wall.advance(1_000)

    notes.delete(n.dat_id)

    assert notes.get(n.dat_id) is None
    hidden = notes.get(n.dat_id, include_deleted=True)
    assert hidden is not None
    assert hidden.title == n.title
    assert hidden.dat_deleted_at == wall.now()
    assert len(notes.history(n.dat_id)) == 2
    with pytest.raises(EntityNotFound):
        notes.update(n)
    with pytest.raises(EntityNotFound):
        notes.delete(n.dat_id)


def test_restore_within_the_window_brings_the_entity_back(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    notes = dat.repository(Note)
    n = Note(title=faker.sentence())
    notes.add(n)
    notes.delete(n.dat_id)
    wall.advance(6 * DAY_MS)

    restored = notes.restore(n.dat_id).value

    assert notes.get(n.dat_id) == n
    assert restored == n
    assert restored.dat_deleted_at is None


def test_restore_after_the_window_fails(dat: Rhizodat, faker: Faker, wall: Wall) -> None:
    notes = dat.repository(Note)
    n = Note(title=faker.sentence())
    notes.add(n)
    notes.delete(n.dat_id)
    wall.advance(7 * DAY_MS + 1)
    with pytest.raises(NotRestorable, match="window"):
        notes.restore(n.dat_id)


def test_restore_of_a_live_entity_fails(dat: Rhizodat, faker: Faker) -> None:
    notes = dat.repository(Note)
    n = Note(title=faker.sentence())
    notes.add(n)
    with pytest.raises(NotRestorable):
        notes.restore(n.dat_id)


def test_purge_erases_only_what_left_the_window(
    dat: Rhizodat, stores: dict[str, MemoryEntityStore], faker: Faker, wall: Wall
) -> None:
    notes = dat.repository(Note)
    old, recent, live = (Note(title=faker.sentence()) for _ in range(3))
    for n in (old, recent, live):
        notes.add(n)
    notes.delete(old.dat_id)
    wall.advance(5 * DAY_MS)
    notes.delete(recent.dat_id)
    wall.advance(2 * DAY_MS + 1)

    assert notes.purge_expired() == 1

    assert stores["pg"].part_history(Part.of(Note, "pg"), old.dat_id) == []
    assert notes.get(recent.dat_id, include_deleted=True) is not None
    assert notes.get(live.dat_id) == live
    assert notes.purge_expired() == 0


def test_an_explicit_window_wins_over_the_configured_default(
    dat: Rhizodat, faker: Faker, wall: Wall
) -> None:
    drafts = dat.repository(Draft)  # 2 days, the configured default is 7
    d = Draft(title=faker.sentence())
    drafts.add(d)
    drafts.delete(d.dat_id)
    wall.advance(2 * DAY_MS + 1)
    with pytest.raises(NotRestorable):
        drafts.restore(d.dat_id)
    assert drafts.purge_expired() == 1


def test_the_default_window_is_thirty_days(stores: dict[str, MemoryEntityStore]) -> None:
    with Rhizodat(stores=dict(stores), journal=MemoryJournal()) as d:
        assert d.soft_delete_window == timedelta(days=30)


@pytest.mark.parametrize("window", [timedelta(0), timedelta(seconds=-1)])
def test_the_configured_window_must_be_positive(
    stores: dict[str, MemoryEntityStore], window: timedelta
) -> None:
    with pytest.raises(ValueError, match="soft_delete_window"):
        Rhizodat(stores=dict(stores), soft_delete_window=window)


def test_purge_is_a_no_op_without_soft_delete(dat: Rhizodat) -> None:
    assert dat.repository(Customer).purge_expired() == 0


# --- inside a Unit of Work --------------------------------------------------------------


def test_repository_writes_join_a_bigger_unit_of_work(dat: Rhizodat, faker: Faker) -> None:
    c = customer(faker)

    @dat.transactional
    def place(uow: UnitOfWork, buyer: Customer) -> Order:
        uow.repository(Customer).add(buyer)
        o = order(faker, customer=Ref.to(buyer))
        uow.repository(Order).add(o)
        assert uow.repository(Customer).get(buyer.dat_id) == buyer  # its own write
        return o

    outcome = place(c)
    o = outcome.value
    assert dat.repository(Customer).get(c.dat_id) == c
    assert dat.repository(Order).get(o.dat_id) == o
    assert outcome.dat_txn is not None


def test_a_failed_unit_of_work_leaves_no_entity(dat: Rhizodat, faker: Faker) -> None:
    c = customer(faker)

    def fail() -> None:
        with dat.unit_of_work() as uow:
            uow.repository(Customer).add(c)
            raise RuntimeError("domain failure")

    with pytest.raises(RuntimeError):
        fail()
    assert dat.repository(Customer).get(c.dat_id) is None


# --- reads across stores ----------------------------------------------------------------


def test_a_read_never_mixes_versions_of_different_transactions(
    dat: Rhizodat, stores: dict[str, MemoryEntityStore], faker: Faker, wall: Wall
) -> None:
    orders = dat.repository(Order)
    o = order(faker)
    orders.add(o)
    first = orders.get(o.dat_id)
    wall.advance(1_000)
    # The pg part of a newer version is visible, its graph part not yet (still finishing).
    stores["graph"].hold_finish = True
    o.status = "shipped"
    o.related = []
    orders.update(o)

    got = orders.get(o.dat_id)
    assert got == first
    stores["graph"].release()
    assert orders.get(o.dat_id) == o


def test_a_store_without_entity_parts_is_refused(faker: Faker) -> None:
    from rhizodat.testing.contract import MemoryStore

    with Rhizodat(stores={"pg": MemoryStore("pg")}) as d, pytest.raises(TypeError, match="pg"):
        d.repository(Customer)
