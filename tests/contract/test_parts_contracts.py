"""Every store that holds entity parts keeps the parts contract."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from datorium.adapters.clickhouse import ClickHouseStore
from datorium.adapters.drevo import DrevoStore
from datorium.adapters.postgres import PostgresStore
from datorium.testing.memory import MemoryEntityStore
from datorium.testing.parts import STORE, PartStoreContract
from drevo_parts import clear_parts, drevo_parts_dsn


class TestMemoryParts(PartStoreContract):
    @pytest.fixture
    def parts_store(self) -> MemoryEntityStore:
        return MemoryEntityStore(STORE)


class TestPostgresParts(PartStoreContract):
    @pytest.fixture
    def parts_store(self, pg_dsn: str, pg_schema: str) -> Iterator[PostgresStore]:
        store = PostgresStore(pg_dsn, name=STORE, schema=pg_schema)
        store.install()
        yield store
        store.close()


class TestClickHouseParts(PartStoreContract):
    @pytest.fixture
    def parts_store(self, clickhouse_dsn: str) -> Iterator[ClickHouseStore]:
        store = ClickHouseStore(clickhouse_dsn, name=STORE)
        store.install()
        yield store
        store.close()


class TestDrevoParts(PartStoreContract):
    @pytest.fixture(params=["memory", "file", "bolt"])
    def parts_store(self, request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DrevoStore]:
        store = DrevoStore(drevo_parts_dsn(request, tmp_path), name=STORE)
        clear_parts(store)
        yield store
        clear_parts(store)
        store.close()
