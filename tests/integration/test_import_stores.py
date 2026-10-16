"""An ERP loads its exports into every store, with schemas inferred from the files.

The sales export lands in object storage as CSV and goes to ClickHouse, sorted
by order id; the customer list comes as JSON Lines and goes to PostgreSQL; the
monthly snapshot is Parquet and goes to the Iceberg lakehouse. Each new table
is a migration written next to the application's own and recorded with its
time; a later sales export with a discount column adds it through the next
migration; a corrected customer list replaces the old one.
"""

import datetime as dt
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

import pytest
from faker import Faker

from iceberg_parts import drop_parts
from rhizodat import Rhizodat
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.ingest import import_file
from rhizodat.migrations import load_migrations


@pytest.fixture
def dat(
    pg_dsn: str, pg_schema: str, clickhouse_dsn: str, iceberg_rest_dsn: str
) -> Iterator[Rhizodat]:
    from rhizodat.adapters.clickhouse import ClickHouseStore
    from rhizodat.adapters.iceberg import IcebergStore

    lake = IcebergStore(
        iceberg_rest_dsn, name="lake", parts_namespace=f"erp_{uuid.uuid4().hex[:10]}"
    )
    d = Rhizodat(
        stores={
            "books": PostgresStore(pg_dsn, schema=pg_schema),
            "events": ClickHouseStore(clickhouse_dsn, name="events"),
            "lake": lake,
        },
        journal=PostgresJournal(pg_dsn, schema=pg_schema),
    )
    yield d
    try:
        drop_parts(lake)
    finally:
        d.close()


def s3_uri(iceberg_rest_dsn: str, key: str) -> str:
    """The compose object storage, as a pyarrow S3 URI with its credentials."""
    q = {k: v[0] for k, v in parse_qs(urlsplit(iceberg_rest_dsn).query).items()}
    endpoint = urlsplit(q["s3.endpoint"])
    return (
        f"s3://{quote(q['s3.access-key-id'], safe='')}:{quote(q['s3.secret-access-key'], safe='')}"
        f"@warehouse/{key}?endpoint_override={endpoint.netloc}&scheme={endpoint.scheme}"
        f"&region={q['s3.region']}"
    )


def upload(uri: str, data: bytes) -> None:
    import pyarrow.fs as pafs

    fs, path = pafs.FileSystem.from_uri(uri)
    with fs.open_output_stream(path) as out:
        out.write(data)


def sales_csv(faker: Faker, n: int, *, discount: bool = False) -> bytes:
    lines = ["order_id,customer,amount,placed_at,channel" + (",discount" if discount else "")]
    for i in range(n):
        line = (
            f"{i + 1},{faker.uuid4()},"
            f"{faker.pyfloat(left_digits=4, right_digits=2, positive=True)},"
            f"{faker.date_time_between('-1y').strftime('%Y-%m-%d %H:%M:%S')},"
            f"{faker.random_element(['web', 'shop', 'phone'])}"
        )
        lines.append(
            line + (f",{faker.random_element(['0.05', '0.1', 'NULL'])}" if discount else "")
        )
    return ("\n".join(lines) + "\n").encode()


def customers(path: Path, faker: Faker, n: int) -> Path:
    with path.open("w") as f:
        for _ in range(n):
            row = {
                "id": faker.uuid4(),
                "name": faker.company(),
                "vip": faker.pybool(),
                "since": faker.date_between("-5y").isoformat(),
                "address": {"city": faker.city()},
            }
            f.write(json.dumps(row) + "\n")
    return path


def snapshot(path: Path, faker: Faker, n: int) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(
        pa.table(
            {
                "sku": [faker.bothify("SKU-####") for _ in range(n)],
                "stock": pa.array([faker.random_int(0, 900) for _ in range(n)], pa.int32()),
                "counted_at": pa.array(
                    [faker.date_time_between("-30d", tzinfo=dt.UTC) for _ in range(n)],
                    pa.timestamp("us", "UTC"),
                ),
            }
        ),
        path,
    )
    return path


def count(dat: Rhizodat, store: str, query: str) -> int:
    row = dat.store(store).execute(query).first()
    assert row is not None
    return int(next(iter(row.values())))


def test_exports_land_in_every_store_with_their_schema_as_migrations(
    dat: Rhizodat, tmp_path: Path, iceberg_rest_dsn: str, faker: Faker
) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    prefix = f"imports/{uuid.uuid4().hex[:10]}"

    sales = s3_uri(iceberg_rest_dsn, f"{prefix}/sales-2026-09.csv")
    upload(sales, sales_csv(faker, 300))
    r1 = import_file(
        dat, sales, store="events", table="sales", migrations=migrations, order_by=["order_id"]
    )
    assert (r1.rows, r1.migration) == (300, 1)
    secret = parse_qs(urlsplit(iceberg_rest_dsn).query)["s3.secret-access-key"][0]
    assert secret not in r1.plan.source  # shown masked, never the secret itself
    assert "***" in r1.plan.source
    described = {
        r["name"]: r["type"] for r in dat.store("events").execute("DESCRIBE TABLE sales").all()
    }
    assert described == {
        "order_id": "Int64",
        "customer": "String",
        "amount": "Float64",
        "placed_at": "DateTime64(6)",
        "channel": "LowCardinality(String)",
    }

    r2 = import_file(
        dat,
        customers(tmp_path / "customers.jsonl", faker, 40),
        store="books",
        table="customer",
        migrations=migrations,
    )
    assert (r2.rows, r2.migration) == (40, 2)
    assert count(dat, "books", "SELECT count(*) FROM customer WHERE address::text LIKE '{%'") == 40

    lake: Any = dat.store("lake")
    stock_table = f"{lake.parts_namespace}.stock"
    r3 = import_file(
        dat,
        snapshot(tmp_path / "stock.parquet", faker, 25),
        store="lake",
        table=stock_table,
        migrations=migrations,
    )
    assert (r3.rows, r3.migration) == (25, 3)
    assert lake.catalog.load_table(stock_table).scan().to_arrow().num_rows == 25

    # October's export has a discount column: the next migration adds it.
    october = s3_uri(iceberg_rest_dsn, f"{prefix}/sales-2026-10.csv")
    upload(october, sales_csv(faker, 120, discount=True))
    r4 = import_file(dat, october, store="events", table="sales", migrations=migrations)
    assert (r4.rows, r4.migration) == (120, 4)
    assert count(dat, "events", "SELECT count() FROM sales") == 420
    assert count(dat, "events", "SELECT countIf(discount IS NULL) FROM sales") >= 300

    # A corrected customer list replaces the old one; no schema change, no migration.
    r5 = import_file(
        dat,
        customers(tmp_path / "fixed.jsonl", faker, 35),
        store="books",
        table="customer",
        replace=True,
    )
    assert (r5.rows, r5.migration) == (35, None)
    assert count(dat, "books", "SELECT count(*) FROM customer") == 35

    assert [m.name for m in load_migrations(migrations)] == [
        "import_sales",
        "import_customer",
        "import_stock",
        "import_sales_columns",
    ]
    assert [v.id for v in dat.schema_versions()] == [1, 2, 3, 4]


def test_a_broken_export_loads_nothing(dat: Rhizodat, tmp_path: Path, faker: Faker) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    good = tmp_path / "sales.csv"
    good.write_bytes(sales_csv(faker, 50))
    import_file(dat, good, store="events", table="sales", migrations=migrations)

    # Row 40 of the next file has text where the schema says a number: sampling the
    # first rows missed it, so the load fails, and none of its rows are kept.
    broken = tmp_path / "broken.csv"
    lines = sales_csv(faker, 60).decode().splitlines()
    lines[40] = lines[40].replace(lines[40].split(",")[2], "n/a", 1)
    broken.write_text("\n".join(lines) + "\n")
    with pytest.raises(Exception, match=r"n/a|convert|parse"):
        import_file(dat, broken, store="events", table="sales", sample_rows=10)
    assert count(dat, "events", "SELECT count() FROM sales") == 50
    tables = [r["name"] for r in dat.store("events").execute("SHOW TABLES").all()]
    assert tables == ["sales"]
