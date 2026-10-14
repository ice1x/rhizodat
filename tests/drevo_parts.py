"""drevo stores for the parts tests: embedded (memory, file) or a Bolt server."""

from pathlib import Path

import pytest

from conftest import drevo_embedded_available
from datorium.adapters.drevo import ANCHOR, DrevoStore
from datorium.testing.parts import CONTRACT_PART

LABEL = CONTRACT_PART.table


def clear_parts(store: DrevoStore) -> None:
    """A Bolt server outlives the test: leave no version or anchor behind."""
    store.execute(f"MATCH (v:{LABEL}) DETACH DELETE v")
    store.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")


def drevo_parts_dsn(request: pytest.FixtureRequest, tmp_path: Path) -> str:
    if request.param == "bolt":
        return str(request.getfixturevalue("drevo_bolt_dsn"))
    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
    return "drevo+mem://" if request.param == "memory" else f"drevo://{tmp_path / 'g.drevo'}"
