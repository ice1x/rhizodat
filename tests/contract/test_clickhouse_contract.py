"""rhizodat's ClickHouse adapter keeps the adapter contract (one phase: the rows are
applied from the journal payload after the decision, idempotently)."""

from collections.abc import Iterator

import pytest

from rhizodat.adapters.clickhouse import ClickHouseBranch, ClickHouseStore
from rhizodat.testing.contract import AdapterContract

TABLE = (
    "CREATE TABLE IF NOT EXISTS contract (key String, dat_txn UUID, dat_tx_time Int64)"
    " ENGINE = MergeTree ORDER BY key"
)


class ClickHouseHarness:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.stores: list[ClickHouseStore] = []

    def open_store(self, name: str) -> ClickHouseStore:
        store = ClickHouseStore(self.dsn, name=name)
        store.install()
        store.execute(TABLE)
        self.stores.append(store)
        return store

    def write(self, branch: ClickHouseBranch, key: str) -> None:
        branch.insert("contract", [{"key": key}])

    def store_full_error(self) -> BaseException:
        from clickhouse_connect.driver.exceptions import DatabaseError

        return DatabaseError(
            "Code: 243. DB::Exception: Cannot reserve 1.00 MiB, not enough space."
            " (NOT_ENOUGH_SPACE)",
            code=243,
            name="NOT_ENOUGH_SPACE",
        )

    def count(self, store: ClickHouseStore, key: str) -> int:
        row = store.execute("SELECT count() AS n FROM contract WHERE key = {k:String}", {"k": key})
        return int(row.one()["n"])

    def cleanup(self) -> None:
        for store in self.stores:
            store.close()


class TestClickHouseContract(AdapterContract):
    @pytest.fixture
    def harness(self, clickhouse_dsn: str) -> Iterator[ClickHouseHarness]:
        h = ClickHouseHarness(clickhouse_dsn)
        yield h
        h.cleanup()
