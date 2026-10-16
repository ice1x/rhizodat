"""A third-party adapter over a real store: a multi-tenant ERP plugs in its own
``tenant://<tenant>`` scheme, each tenant a PostgreSQL schema, and gets rhizodat's
meta-transactions and recovery without any change to rhizodat."""

from collections.abc import Iterator

import psycopg
import pytest
from faker import Faker

from rhizodat import Adapter, Rhizodat, Scheme, register_adapter
from rhizodat.adapters.postgres import PostgresStore
from rhizodat.core import adapter as registry
from rhizodat.core.dsn import parse_dsn
from rhizodat.core.journal import State


@pytest.fixture
def tenants(pg_dsn: str, pg_schema: str) -> Iterator[list[str]]:
    saved = registry._snapshot()
    names = [f"{pg_schema}_acme", f"{pg_schema}_globex"]
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for name in names:
            conn.execute(f'CREATE SCHEMA "{name}"')

    def open_tenant(name: str, dsn: str) -> PostgresStore:
        store = PostgresStore(pg_dsn, name=name, schema=parse_dsn(dsn).host)
        store.install()
        store.execute("CREATE TABLE IF NOT EXISTS invoices (dat_id uuid, customer text)")
        return store

    register_adapter(
        Adapter(
            kind="tenant",
            schemes=(Scheme("tenant", host="required"),),
            open_store=open_tenant,
            query_help="SQL over one tenant's schema",
        )
    )
    yield names
    registry._restore(saved)
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        for name in names:
            conn.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')


def test_an_invoice_across_two_tenants_commits_through_the_plugin(
    tenants: list[str], pg_dsn: str, pg_schema: str, faker: Faker
) -> None:
    acme, globex = tenants
    dat = Rhizodat(
        stores={"acme": f"tenant://{acme}", "globex": f"tenant://{globex}"},
        journal=f"{pg_dsn}?schema={pg_schema}",
    )
    customer = faker.company()
    with dat.unit_of_work() as uow:
        for name in ("acme", "globex"):
            uow.store(name).execute("INSERT INTO invoices VALUES (%s, %s)", (uow.dat_txn, customer))
    assert uow.outcome is not None
    assert dat.journal is not None
    record = dat.journal.get(uow.dat_txn)
    assert record is not None
    assert record.state is State.DONE
    assert {b.two_phase for b in record.branches} == {True}  # PostgreSQL 2PC under the plugin
    for name in ("acme", "globex"):
        rows = dat.store(name).execute("SELECT customer FROM invoices").all()
        assert [r["customer"] for r in rows] == [customer]
    dat.close()


def test_one_tenant_twice_is_refused_like_any_store(tenants: list[str]) -> None:
    acme, _ = tenants
    with pytest.raises(ValueError, match="share one dat_applied"):
        Rhizodat(stores={"a": f"tenant://{acme}", "b": f"tenant://{acme}"})
