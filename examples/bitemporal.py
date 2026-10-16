"""Valid time and transaction time: what held when, as known when.

A price agreement holds for a period; a rebate entered later wins for its own
days only, and the price as known before the rebate is still readable.
"""

import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from _stores import JOURNAL, PG

from rhizodat import Rhizodat, entity


@entity(stores={"pg": ["customer", "price"]}, name="erp.Price", valid_time=True)
class Price:
    customer: str
    price: Decimal


def day(n: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=n)


with Rhizodat(stores={"pg": PG}, journal=JOURNAL) as dat:
    prices = dat.repository(Price)
    p = Price(customer="acme", price=Decimal("10.00"), valid_from=day(0), valid_to=None)
    prices.add(p)
    before_rebate = datetime.now(UTC)
    time.sleep(0.01)

    p.price, p.valid_from, p.valid_to = Decimal("8.00"), day(10), day(20)  # a rebate
    prices.update(p)

    def price(at: datetime, known_at: datetime | None = None) -> Decimal:
        found = prices.get(p.dat_id, as_of=at, known_at=known_at)
        assert found is not None
        return found.price

    print(price(day(5)), price(day(15)), price(day(25)))  # 10.00 8.00 10.00
    print(price(day(15), known_at=before_rebate))  # 10.00: as known before the rebate
