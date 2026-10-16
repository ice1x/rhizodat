# API sketch (v0.1)

```python
from rhizodat import Rhizodat

dat = Rhizodat(
    stores={
        "pg":         "postgresql://user:pass@localhost/app",
        "drevo":      "drevo:///data/graph.drevo",
        "clickhouse": "clickhouse://localhost:8123/analytics",
        "iceberg":    "iceberg+rest://localhost:8181/warehouse",
    },
    journal="postgresql://user:pass@localhost/rhizodat",
)

orders = dat.repository(Order)

@dat.transactional(retry=Retry(attempts=5, deadline_s=10))
def place_order(uow, customer: Ref["Customer"]) -> Order:
    order = Order(status="new", amount=Decimal("10.00"), customer=customer)
    uow.repository(Order).add(order)
    return order                          # commit on return; re-run on a transient error

outcome = place_order(customer=ref)       # Committed | CommittedPending; raises Aborted

order = orders.get(outcome.value.dat_id, as_of=yesterday)
```

A plain `with dat.unit_of_work() as uow:` block is also available; it runs once
and does not retry.

Inside a Unit of Work, native statements run in that store's branch of the
meta-transaction; the branch is opened the first time the store is touched:

```python
@dat.transactional
def claim_task(uow, worker: str) -> int:
    tasks = uow.store("pg")
    row = tasks.execute("SELECT id FROM task WHERE claimed_by IS NULL LIMIT 1").one()
    tasks.execute("UPDATE task SET claimed_by = %s WHERE id = %s", (worker, row["id"]))
    uow.store("drevo").execute("MATCH (w:Worker {name: $w}) SET w.busy = true", {"w": worker})
    return row["id"]                      # outcome.value
```

A transient failure in the work itself (e.g. a serialization failure) or before
the decision re-runs the whole function with a new `dat_txn`; a domain error
from the function propagates unchanged.

Native access stays available through adapters:

```python
dat.store("pg").execute("SELECT count(*) FROM orders WHERE status = %(s)s", {"s": "new"})
dat.store("drevo").execute("MATCH (o:Order)-[:related]->(x) RETURN x LIMIT 10")
dat.store("iceberg").execute("analytics.orders", {"row_filter": "amount > 100"}).to_arrow()
```
