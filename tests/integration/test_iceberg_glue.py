"""An Iceberg lake on an AWS Glue catalog, next to PostgreSQL.

The Glue catalog is moto's local Glue endpoint (its server mode, in-process),
the data files go to the compose S3. The store opens from an
``iceberg+glue://<region>?warehouse=s3://...`` DSN like any other: entity parts
and application tables are written in meta-transactions with PostgreSQL, read
back, read ``as_of`` an earlier moment, and migrated with ``iceberg`` steps,
on the Python facade and on the native one (the same DSN), and each facade
reads what the other wrote.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,call-arg"

import contextlib
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlsplit

import pyarrow as pa
import pytest
from faker import Faker

from rhizodat import Rhizodat, UnitOfWork, entity
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.native_uow import NativeUnitOfWork
from rhizodat.migrations import Migration, Step

pytest.importorskip("boto3", reason="the glue extra: pip install 'rhizodat[glue]'")
moto_server = pytest.importorskip("moto.server", reason="moto[server] is a dev dependency")


@entity(stores={"books": ["title"], "lake": ["pages", "tags"]}, name="glue.Book")
class Book:
    title: str
    pages: int
    tags: list[str]


@pytest.fixture(scope="module")
def glue_endpoint() -> Iterator[str]:
    server = moto_server.ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def glue_dsn(glue_endpoint: str, iceberg_rest_dsn: str) -> str:
    """A Glue DSN over moto, with the compose S3 of the REST DSN for the files."""
    s3 = {k: v for k, v in parse_qsl(urlsplit(iceberg_rest_dsn).query) if k.startswith("s3.")}
    params = {
        "warehouse": f"s3://warehouse/glue-{uuid.uuid4().hex[:8]}",
        "glue.endpoint": glue_endpoint,
        "glue.access-key-id": "testing",
        "glue.secret-access-key": "testing",
        **s3,
    }
    return f"iceberg+glue://us-east-1?{urlencode(params)}"


@pytest.fixture
def namespace(glue_dsn: str) -> Iterator[str]:
    name = f"dat_glue_{uuid.uuid4().hex[:10]}"
    yield name
    cleaner = IcebergStore(glue_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for ident in cleaner.catalog.list_tables(name):
                cleaner.catalog.purge_table(ident)
            cleaner.catalog.drop_namespace(name)
    finally:
        cleaner.close()


def python_facade(pg_dsn: str, pg_schema: str, glue_dsn: str, namespace: str) -> Rhizodat:
    return Rhizodat(
        stores={
            "books": PostgresStore(pg_dsn, name="books", schema=pg_schema),
            "lake": IcebergStore(glue_dsn, name="lake", parts_namespace=namespace),
        },
        journal=PostgresJournal(pg_dsn, schema=pg_schema),
    )


def native_facade(pg_dsn: str, pg_schema: str, glue_dsn: str, namespace: str) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "books": native.PostgresStore(pg_dsn, "books", pg_schema),
            "lake": native.IcebergStore(glue_dsn, "lake", parts_namespace=namespace),
        },
        journal=native.PostgresJournal(pg_dsn, pg_schema),
    )


FACADES: dict[str, Callable[..., Rhizodat]] = {"python": python_facade, "native": native_facade}


@pytest.fixture(params=list(FACADES))
def dat(
    request: pytest.FixtureRequest, pg_dsn: str, pg_schema: str, glue_dsn: str, namespace: str
) -> Iterator[Rhizodat]:
    d = FACADES[request.param](pg_dsn, pg_schema, glue_dsn, namespace)
    yield d
    d.close()


def test_the_glue_catalog_answers(glue_dsn: str) -> None:
    store = IcebergStore(glue_dsn, name="lake")
    try:
        assert type(store.catalog).__name__ == "GlueCatalog"
        assert isinstance(store.catalog.list_namespaces(), list)
    finally:
        store.close()


def test_entities_are_written_and_read_back_through_glue(dat: Rhizodat, faker: Faker) -> None:
    books = dat.repository(Book)
    book = Book(title=faker.sentence(), pages=120, tags=["draft"])
    books.add(book)
    written = datetime.now(UTC)
    time.sleep(0.01)
    book.pages, book.tags = 180, ["draft", "edited"]
    books.update(book)

    assert books.get(book.dat_id) == book
    then = books.get(book.dat_id, as_of=written)
    assert then is not None
    assert (then.pages, then.tags) == (120, ["draft"])
    assert [b.pages for b in books.history(book.dat_id)] == [120, 180]


def test_an_application_table_is_migrated_and_appended_in_a_meta_transaction(
    dat: Rhizodat, namespace: str, faker: Faker
) -> None:
    table = f"{namespace}.sales"
    report = dat.migrate(
        [
            Migration.of(
                1,
                "sales",
                [
                    Step(
                        "lake",
                        "iceberg",
                        [
                            {"op": "create_namespace", "namespace": namespace},
                            {
                                "op": "create_table",
                                "table": table,
                                "columns": [
                                    {"name": "sku", "type": "string"},
                                    {"name": "qty", "type": "long"},
                                    {"name": "dat_txn", "type": "string"},
                                    {"name": "dat_tx_time", "type": "long"},
                                ],
                            },
                        ],
                    ),
                    Step(
                        "books",
                        "sql",
                        "CREATE TABLE orders (sku text, qty bigint,"
                        " dat_txn uuid, dat_tx_time bigint)",
                    ),
                ],
            )
        ]
    )
    assert report.applied == (1,)
    skus = [faker.bothify("SKU-####") for _ in range(3)]

    @dat.transactional
    def order(uow: UnitOfWork) -> None:
        books = uow.store("books")
        if isinstance(uow, NativeUnitOfWork):
            books.insert("orders", ["sku", "qty"], [[sku, 2] for sku in skus])
        else:
            for sku in skus:
                books.execute("INSERT INTO orders (sku, qty) VALUES (%s, %s)", (sku, 2))
        uow.store("lake").append(table, pa.Table.from_pylist([{"sku": s, "qty": 2} for s in skus]))

    order()
    rows = dat.store("lake").execute(table).all()
    assert sorted(r["sku"] for r in rows) == sorted(skus)
    assert len({r["dat_txn"] for r in rows}) == 1
    assert dat.store("books").execute("SELECT count(*) AS n FROM orders").one()["n"] == 3


def test_both_facades_share_the_glue_lake(
    pg_dsn: str, pg_schema: str, glue_dsn: str, namespace: str, faker: Faker
) -> None:
    """Written on the Python facade, updated natively, read back on Python."""
    args = (pg_dsn, pg_schema, glue_dsn, namespace)
    book = Book(title=faker.sentence(), pages=90, tags=["a"])
    with python_facade(*args) as python_side:
        python_side.repository(Book).add(book)
    with native_facade(*args) as native_side:
        read = native_side.repository(Book).get(book.dat_id)
        assert read == book
        assert read is not None
        read.pages = 95
        native_side.repository(Book).update(read)
    with python_facade(*args) as python_side:
        assert [b.pages for b in python_side.repository(Book).history(book.dat_id)] == [90, 95]
