"""rhizodat's drevo adapter keeps the adapter contract: embedded (memory, file) and
remote over Bolt, with two-phase commit."""

import contextlib
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from conftest import drevo_embedded_available
from own_prepared import OwnBranches
from rhizodat.adapters.drevo import DrevoBranch, DrevoStore
from rhizodat.testing.contract import AdapterContract


class DrevoHarness:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.stores: list[DrevoStore] = []
        self.branches: list[DrevoBranch] = []

    def open_store(self, name: str) -> DrevoStore:
        store = DrevoStore(self.dsn, name=name)
        original = store.branch

        def tracked(dat_txn: uuid.UUID, **kw: Any) -> DrevoBranch:
            branch = original(dat_txn, **kw)
            self.branches.append(branch)
            return branch

        store.branch = tracked  # type: ignore[method-assign]
        self.stores.append(store)
        return store

    def write(self, branch: DrevoBranch, key: str) -> None:
        branch.execute("CREATE (:Contract {key: $k})", {"k": key})

    def count(self, store: DrevoStore, key: str) -> int:
        result = store.execute("MATCH (n:Contract {key: $k}) RETURN count(n) AS n", {"k": key})
        return int(result.one()["n"])

    def cleanup(self) -> None:
        """A failed test must not leave the graph fenced for the next one."""
        for branch in self.branches:
            with contextlib.suppress(Exception):  # committed or resolved ones refuse: fine
                branch.rollback()
        own = OwnBranches()  # only this test's gids: the Bolt server is shared
        for branch in self.branches:
            own.note(branch.dat_txn)
        for store in self.stores:
            own.abort_prepared(store.name, store.participant())
            store.close()


class TestDrevoContract(AdapterContract):
    @pytest.fixture(params=["memory", "file", "bolt"])
    def harness(self, request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DrevoHarness]:
        if request.param == "bolt":
            dsn = str(request.getfixturevalue("drevo_bolt_dsn"))
        elif not drevo_embedded_available():
            pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
        elif request.param == "memory":
            dsn = "drevo+mem://"
        else:
            dsn = f"drevo://{tmp_path / 'graph.drevo'}"
        h = DrevoHarness(dsn)
        yield h
        h.cleanup()
