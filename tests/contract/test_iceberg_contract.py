"""rhizodat's Iceberg adapter keeps the adapter contract (one phase: staged Parquet
files committed as a snapshot after the decision): SQLite catalog and REST over S3."""

import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, UUIDType

from rhizodat.adapters.iceberg import IcebergBranch, IcebergStore
from rhizodat.testing.contract import AdapterContract

SCHEMA = Schema(
    NestedField(1, "key", StringType(), required=True),
    NestedField(2, "dat_txn", UUIDType()),
    NestedField(3, "dat_tx_time", LongType()),
)


class IcebergHarness:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.namespace = f"contract_{uuid.uuid4().hex[:10]}"  # the REST catalog is shared
        self.stores: list[IcebergStore] = []

    @property
    def table(self) -> str:
        return f"{self.namespace}.items"

    def open_store(self, name: str) -> IcebergStore:
        store = IcebergStore(self.dsn, name=name)
        if (self.namespace,) not in store.catalog.list_namespaces():
            store.catalog.create_namespace(self.namespace)
            store.catalog.create_table(self.table, SCHEMA)
        self.stores.append(store)
        return store

    def write(self, branch: IcebergBranch, key: str) -> None:
        branch.append(self.table, [{"key": key}])

    def count(self, store: IcebergStore, key: str) -> int:
        return sum(1 for row in store.execute(self.table).all() if row["key"] == key)

    def cleanup(self) -> None:
        if self.stores:
            catalog = self.stores[0].catalog
            if (self.namespace,) in catalog.list_namespaces():
                for identifier in catalog.list_tables(self.namespace):
                    catalog.drop_table(identifier)
                catalog.drop_namespace(self.namespace)
        for store in self.stores:
            store.close()


class TestIcebergContract(AdapterContract):
    @pytest.fixture(params=["sql", "rest"])
    def harness(self, request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[IcebergHarness]:
        if request.param == "rest":
            dsn = str(request.getfixturevalue("iceberg_rest_dsn"))
        else:
            dsn = f"iceberg+sql://{tmp_path / 'catalog.db'}?warehouse=file://{tmp_path / 'wh'}"
        h = IcebergHarness(dsn)
        yield h
        h.cleanup()
