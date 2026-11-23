"""rhizodat's PostgreSQL adapter keeps the adapter contract (real server, 2PC)."""

from collections.abc import Iterator

import pytest

from own_prepared import OwnBranches
from rhizodat.adapters.postgres import PostgresBranch, PostgresStore
from rhizodat.testing.contract import AdapterContract


class PostgresHarness:
    def __init__(self, dsn: str, schema: str) -> None:
        self.dsn, self.schema = dsn, schema
        self.stores: list[PostgresStore] = []
        self.own = OwnBranches()

    def open_store(self, name: str) -> PostgresStore:
        store = self.own.track(PostgresStore(self.dsn, name=name, schema=self.schema))
        store.install()
        store.execute("CREATE TABLE IF NOT EXISTS contract (key text NOT NULL)")
        self.stores.append(store)
        return store

    def write(self, branch: PostgresBranch, key: str) -> None:
        branch.execute("INSERT INTO contract VALUES (%s)", (key,))

    def count(self, store: PostgresStore, key: str) -> int:
        row = store.execute("SELECT count(*) AS n FROM contract WHERE key = %s", (key,)).one()
        return int(row["n"])

    def store_full_error(self) -> BaseException:
        import psycopg

        return psycopg.errors.DiskFull("could not extend file: No space left on device")

    def cleanup(self) -> None:
        for store in self.stores:  # a failed test must not leave locks; only its own
            self.own.abort_prepared(store.name, store.participant())
            store.close()


class TestPostgresContract(AdapterContract):
    @pytest.fixture
    def harness(self, pg_dsn: str, pg_schema: str) -> Iterator[PostgresHarness]:
        h = PostgresHarness(pg_dsn, pg_schema)
        yield h
        h.cleanup()
