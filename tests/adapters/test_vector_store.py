"""Embeddings in drevo (``Vectors``): vectors written beside each node's text, the
model and a hash of the text with them, and the nearest nodes by cosine.

A story editor embeds its chapter summaries, then asks which chapters are
closest to a sentence. Unchanged summaries are not embedded twice; a new model
or an edited summary is.
"""

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from drevo_parts import drevo_parts_dsn
from rhizodat.core.adapter import Vectors


@pytest.fixture(params=["memory", "file", "bolt"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    from rhizodat.adapters.drevo import DrevoStore

    s = DrevoStore(drevo_parts_dsn(request, tmp_path), name="graph")
    yield s
    s.close()


@pytest.fixture
def label(store: Any) -> Iterator[str]:
    name = f"Chapter{uuid.uuid4().hex[:8]}"
    yield name
    store.execute(f"MATCH (n:{name}) DETACH DELETE n")


def seed(store: Any, label: str, texts: dict[str, str | None]) -> None:
    for key, text in texts.items():
        store.execute(
            f"MERGE (n:{label} {{title: $k}}) SET n.summary = $t",
            {"k": f"{label}:{key}", "t": text},
        )


def test_drevo_holds_vectors(store: Any) -> None:
    assert isinstance(store, Vectors)


def test_candidates_write_and_nearest(store: Any, label: str) -> None:
    seed(store, label, {"1": "engines", "2": "gardens", "3": None})
    candidates = store.vector_candidates(label, "summary", "embedding")
    assert sorted((c.key.split(":")[1], c.text, c.hash, c.model) for c in candidates) == [
        ("1", "engines", None, None),
        ("2", "gardens", None, None),
    ]
    store.write_vectors(
        label,
        "embedding",
        [
            {"key": f"{label}:1", "vector": [1.0, 0.0], "hash": "h1", "model": "m"},
            {"key": f"{label}:2", "vector": [0.0, 1.0], "hash": "h2", "model": "m"},
        ],
    )
    after = {
        c.key.split(":")[1]: (c.hash, c.model)
        for c in store.vector_candidates(label, "summary", "embedding")
    }
    assert after == {"1": ("h1", "m"), "2": ("h2", "m")}
    [first, second] = store.nearest(label, "embedding", [0.9, 0.1], k=2)
    assert first.key == f"{label}:1"
    assert first.score > second.score
    assert first.properties == {
        "title": f"{label}:1",
        "summary": "engines",
    }  # no vector bookkeeping


def test_a_failed_write_writes_nothing(
    store: Any, label: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed(store, label, {str(i): f"text {i}" for i in range(5)})
    begin = store._backend.begin

    def failing() -> Any:
        tx = begin()
        execute = tx.execute
        calls = {"n": 0}

        def run(query: str, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("connection reset")
            return execute(query, params)

        tx.execute = run
        return tx

    monkeypatch.setattr(store._backend, "begin", failing)
    rows = [
        {"key": f"{label}:{i}", "vector": [1.0, float(i)], "hash": "h", "model": "m"}
        for i in range(5)
    ]
    with pytest.raises(RuntimeError, match="reset"):
        store.write_vectors(label, "embedding", rows, batch=2)
    monkeypatch.undo()
    assert all(c.hash is None for c in store.vector_candidates(label, "summary", "embedding"))


def test_names_must_be_identifiers(store: Any) -> None:
    with pytest.raises(ValueError, match="bad label"):
        store.nearest("bad label", "embedding", [1.0], k=1)
    with pytest.raises(ValueError, match="k must be"):
        store.nearest("Chapter", "embedding", [1.0], k=0)
