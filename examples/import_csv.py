"""Import a CSV into ClickHouse with its schema inferred from every row.

The new table becomes a migration file next to the others (here a temporary
directory), runs, and the rows load as one unit. The same from a shell:
``rhizodat import sales.csv --store events --table sales --migrations migrations``.
"""

import random
import tempfile
from pathlib import Path

from _stores import CLICKHOUSE

from rhizodat import Rhizodat
from rhizodat.ingest import import_file

tmp = tempfile.mkdtemp()
# A journal of its own: the migration history belongs to this example's directory.
with Rhizodat(stores={"events": CLICKHOUSE}, journal=f"file://{tmp}/journal.db") as dat:
    sales = Path(tmp) / "sales.csv"
    rows = ["order_id,amount,placed_at,channel"]
    for i in range(1, 101):
        channel = random.choice(["web", "shop", "phone"])
        amount = random.randint(100, 99999) / 100
        rows.append(f"{i},{amount},2026-10-{i % 28 + 1:02} 12:00:00,{channel}")
    sales.write_text("\n".join(rows) + "\n")

    migrations = Path(tmp) / "migrations"
    migrations.mkdir()
    report = import_file(
        dat,
        sales,
        store="events",
        table="example_sales",
        migrations=migrations,
        order_by=["order_id"],
        replace=True,
    )
    print(f"{report.rows} rows, migration {report.migration}")
    for c in report.plan.schema.columns:
        print(f"  {c.name}: {c.type}{' (low cardinality)' if c.low_cardinality else ''}")
