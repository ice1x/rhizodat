"""Iceberg stores for the parts tests: a SQLite catalog with a local warehouse, or the
compose REST catalog over S3. Each store gets its own parts namespace, dropped after."""

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from rhizodat.adapters.iceberg import IcebergStore


def iceberg_parts_dsn(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "rest":
        return str(request.getfixturevalue("iceberg_rest_dsn"))
    return f"iceberg+sql://{tmp_path / 'catalog.db'}?warehouse=file://{tmp_path / 'warehouse'}"


def drop_parts(store: IcebergStore) -> None:
    """The REST catalog outlives the test: leave no table behind."""
    namespace = (store.parts_namespace,)
    if namespace not in store.catalog.list_namespaces():
        return
    for identifier in store.catalog.list_tables(namespace):
        store.catalog.drop_table(identifier)
    store.catalog.drop_namespace(namespace)


@contextmanager
def iceberg_parts_store(dsn: str, *, name: str) -> Iterator[IcebergStore]:
    store = IcebergStore(dsn, name=name, parts_namespace=f"dat_{uuid.uuid4().hex[:12]}")
    try:
        yield store
    finally:
        try:
            drop_parts(store)
        finally:
            store.close()
