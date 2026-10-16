# rhizodat 🍄

**A bitemporal polyglot data layer:** PostgreSQL, ClickHouse,
[drevo](https://github.com/ice1x/drevo) and Apache Iceberg behave like one
transactional store. An entity may span several stores; every write is one
atomic meta-transaction across them, and every version is kept, so any entity
can be read as of any moment.

```bash
pip install 'rhizodat[postgres,clickhouse,drevo,iceberg]'
```

```python
from rhizodat import Rhizodat, entity


@entity(stores={"pg": ["status", "amount"], "graph": ["customer"]}, name="shop.Order")
class Order:
    status: str
    amount: Decimal
    customer: str


with Rhizodat(stores={"pg": PG, "graph": GRAPH}, journal=JOURNAL) as dat:
    orders = dat.repository(Order)
    orders.add(order)  # one meta-transaction across PostgreSQL and drevo
    print(orders.get(order.dat_id, as_of=before).status)  # new
```

The whole script: [examples/quickstart.py](examples/quickstart.py). More in
[examples/](examples/) and the [documentation](docs/index.md): concepts,
guides (Repository, migrations, file imports, MCP), adapters and the API
reference (`uv run --group docs mkdocs serve`).

Apache-2.0. Status: v0.1 in progress.
