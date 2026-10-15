"""Valid time: when a fact holds in the real world, next to when rhizodat learnt it.

An ERP keeps negotiated prices: each version of a price agreement holds for a
period (``valid_from`` inclusive, ``valid_to`` exclusive, ``None`` unbounded).
A later version for an overlapping period wins over an earlier one; one for
another period leaves the earlier one in force there. ``as_of`` is the moment
the fact must hold, ``known_at`` the moment of knowledge.
"""

# @entity adds dat_id, the validity fields and the service times at runtime.
# mypy: disable-error-code="attr-defined, call-arg"

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.journal import MemoryJournal
from rhizodat.core.mapping import entity
from rhizodat.core.parts import Part
from rhizodat.testing.memory import MemoryEntityStore


@entity(
    stores={"pg": ["customer", "price"], "lake": ["currency"]},
    name="erp.PriceAgreement",
    valid_time=True,
    soft_delete=True,
)
class PriceAgreement:
    customer: str
    price: Decimal
    currency: str


@entity(stores={"pg": ["name"]}, name="erp.Customer")
class Customer:
    name: str


def day(n: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=n)


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 15, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def tick(self) -> datetime:
        """Move on a minute; the moment before the move is in the past for good."""
        before = datetime.fromtimestamp(self.ms / 1000, UTC)
        self.ms += 60_000
        return before


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def dat(wall: Wall) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores={"pg": MemoryEntityStore("pg"), "lake": MemoryEntityStore("lake")},
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d
    d.close()


def agreement(
    faker: Faker, price: str, valid_from: datetime | None, valid_to: datetime | None
) -> PriceAgreement:
    return PriceAgreement(
        customer=faker.company(),
        price=Decimal(price),
        currency="EUR",
        valid_from=valid_from,
        valid_to=valid_to,
    )


def test_the_validity_period_is_stored_with_every_part(dat: Rhizodat, faker: Faker) -> None:
    prices = dat.repository(PriceAgreement)
    a = agreement(faker, "10.00", day(0), day(30))
    prices.add(a)

    stored = prices.get(a.dat_id, as_of=day(5))
    assert stored == a
    assert stored is not None
    assert (stored.valid_from, stored.valid_to) == (day(0), day(30))
    for store in ("pg", "lake"):
        part = Part.of(PriceAgreement, store)
        assert {"valid_from", "valid_to"} <= set(part.fields)


def test_as_of_picks_the_version_that_holds_then(dat: Rhizodat, faker: Faker) -> None:
    prices = dat.repository(PriceAgreement)
    a = agreement(faker, "10.00", day(0), day(30))
    prices.add(a)
    # The next period is agreed later: a new version for days 30..60.
    a.price, a.valid_from, a.valid_to = Decimal("12.00"), day(30), day(60)
    prices.update(a)

    assert prices.get(a.dat_id, as_of=day(-1)) is None  # before any agreement
    first = prices.get(a.dat_id, as_of=day(0))
    assert first is not None
    assert first.price == Decimal("10.00")  # valid_from is inclusive
    later = prices.get(a.dat_id, as_of=day(30))
    assert later is not None
    assert later.price == Decimal("12.00")  # valid_to is exclusive
    assert prices.get(a.dat_id, as_of=day(60)) is None


def test_a_correction_wins_for_its_period_only(dat: Rhizodat, faker: Faker, wall: Wall) -> None:
    prices = dat.repository(PriceAgreement)
    a = agreement(faker, "10.00", day(0), None)  # open-ended
    prices.add(a)
    before_correction = wall.tick()

    # A rebate for days 10..20 is entered later.
    a.price, a.valid_from, a.valid_to = Decimal("8.00"), day(10), day(20)
    prices.update(a)

    def price(at: datetime, known_at: datetime | None = None) -> Decimal | None:
        found = prices.get(a.dat_id, as_of=at, known_at=known_at)
        return None if found is None else found.price

    assert [price(day(n)) for n in (5, 10, 19, 20, 400)] == [
        Decimal("10.00"),
        Decimal("8.00"),
        Decimal("8.00"),
        Decimal("10.00"),
        Decimal("10.00"),
    ]
    # As known before the correction, day 15 was at the full price.
    assert price(day(15), known_at=before_correction) == Decimal("10.00")


def test_without_as_of_the_fact_valid_now_is_read(dat: Rhizodat, faker: Faker) -> None:
    prices = dat.repository(PriceAgreement)
    now = datetime.now(UTC)
    current = agreement(faker, "9.00", now - timedelta(days=1), now + timedelta(days=1))
    expired = agreement(faker, "7.00", now - timedelta(days=9), now - timedelta(days=2))
    prices.add(current)
    prices.add(expired)
    assert prices.get(current.dat_id) == current
    assert prices.get(expired.dat_id) is None
    assert prices.get(expired.dat_id, as_of=now - timedelta(days=5)) == expired


def test_history_keeps_every_period(dat: Rhizodat, faker: Faker) -> None:
    prices = dat.repository(PriceAgreement)
    a = agreement(faker, "10.00", day(0), day(30))
    prices.add(a)
    a.price, a.valid_from, a.valid_to = Decimal("12.00"), day(30), day(60)
    prices.update(a)
    assert [(v.price, v.valid_from, v.valid_to) for v in prices.history(a.dat_id)] == [
        (Decimal("10.00"), day(0), day(30)),
        (Decimal("12.00"), day(30), day(60)),
    ]


def test_a_soft_deleted_agreement_holds_nowhere(dat: Rhizodat, faker: Faker) -> None:
    prices = dat.repository(PriceAgreement)
    a = agreement(faker, "10.00", day(0), None)
    prices.add(a)
    prices.delete(a.dat_id)
    assert prices.get(a.dat_id, as_of=day(5)) is None
    deleted = prices.get(a.dat_id, as_of=day(5), include_deleted=True)
    assert deleted is not None
    assert deleted.dat_deleted_at is not None


@pytest.mark.parametrize(
    ("valid_from", "valid_to", "message"),
    [
        (day(5), day(5), "valid_from must be before valid_to"),
        (day(6), day(5), "valid_from must be before valid_to"),
        (datetime(2026, 1, 1), None, "time zone"),
        (None, datetime(2026, 1, 1), "time zone"),
    ],
)
def test_a_period_must_make_sense(
    dat: Rhizodat,
    faker: Faker,
    valid_from: datetime | None,
    valid_to: datetime | None,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        dat.repository(PriceAgreement).add(agreement(faker, "1.00", valid_from, valid_to))


def test_without_valid_time_known_at_is_as_of(dat: Rhizodat, faker: Faker, wall: Wall) -> None:
    customers = dat.repository(Customer)
    c = Customer(name=faker.company())
    customers.add(c)
    first = wall.tick()
    c.name = faker.company()
    customers.update(c)

    old = customers.get(c.dat_id, known_at=first)
    assert old is not None
    assert old.name != c.name
    with pytest.raises(ValueError, match="no valid time"):
        customers.get(c.dat_id, as_of=first, known_at=first)
