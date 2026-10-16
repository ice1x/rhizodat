"""Quickstart: one entity, two stores, every version kept.

An order's money lives in PostgreSQL and its links in the drevo graph. Each
write is one atomic meta-transaction over both; reads see any past moment.
"""

import time
from datetime import UTC, datetime
from decimal import Decimal

from _stores import GRAPH, JOURNAL, PG

from rhizodat import Rhizodat, entity


@entity(stores={"pg": ["status", "amount"], "graph": ["customer"]}, name="shop.Order")
class Order:
    status: str
    amount: Decimal
    customer: str


with Rhizodat(stores={"pg": PG, "graph": GRAPH}, journal=JOURNAL) as dat:
    orders = dat.repository(Order)

    order = Order(status="new", amount=Decimal("10.00"), customer="ada")
    orders.add(order)  # one meta-transaction across PostgreSQL and drevo
    before = datetime.now(UTC)
    time.sleep(0.01)  # the update must come after `before`

    order.status = "paid"
    orders.update(order)

    print(orders.get(order.dat_id).status)  # paid
    print(orders.get(order.dat_id, as_of=before).status)  # new
    print([v.status for v in orders.history(order.dat_id)])  # ['new', 'paid']
