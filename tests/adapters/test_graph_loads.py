"""Loading a graph into drevo (``LoadsGraphs``): every node and edge in one transaction.

The story editor's world goes into the graph: each resource a node with the
base label ``Resource`` plus its classes, each link an edge. A second load of
the same file changes nothing but updated values; a load that fails halfway
leaves the graph as it was.
"""

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from drevo_parts import drevo_parts_dsn
from rhizodat.core.adapter import LoadsGraphs
from rhizodat.ingest.graph import Edge, GraphData, Node


@pytest.fixture(params=["memory", "file", "bolt"])
def graph_store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    from rhizodat.adapters.drevo import DrevoStore

    store = DrevoStore(drevo_parts_dsn(request, tmp_path), name="graph")
    yield store
    store.close()


@pytest.fixture
def base() -> str:
    """An IRI namespace of this test only: the Bolt server outlives it."""
    return f"https://example.org/{uuid.uuid4().hex[:10]}#"


def world(base: str, faker: Faker, *, age: int = 36) -> GraphData:
    ada, charles, london, chapter = (f"{base}{n}" for n in ("ada", "charles", "london", "ch1"))
    nodes = {
        ada: Node(
            ada,
            ("Character", "Person"),
            {"iri": ada, "name": "Ada", "age": age, "label": ["Ada", "Ада"]},
        ),
        charles: Node(charles, ("Person",), {"iri": charles, "name": faker.first_name()}),
        london: Node(london, ("Place",), {"iri": london, "name": "London"}),
        chapter: Node(chapter, ("Match",), {"iri": chapter}),  # a Cypher keyword as a class
        f"_:{base}b1": Node(f"_:{base}b1", (), {"text": "draft"}),
    }
    edges = [
        Edge(ada, "knows", charles),
        Edge(ada, "lives_in", london),
        Edge(chapter, "mentions", ada),
        Edge(chapter, "mentions", london),
        Edge(chapter, "note", f"_:{base}b1"),
    ]
    return GraphData(nodes, edges)


def rows(store: Any, query: str, base: str) -> list[dict[str, Any]]:
    return [dict(r) for r in store.execute(query, {"base": base}).all()]


def cleanup(store: Any, base: str) -> None:
    store.execute(
        "MATCH (n:Resource) WHERE n.title STARTS WITH $base OR n.title STARTS WITH $blank"
        " DETACH DELETE n",
        {"base": base, "blank": f"_:{base}"},
    )


def test_drevo_loads_graphs(graph_store: Any) -> None:
    assert isinstance(graph_store, LoadsGraphs)


def test_nodes_and_edges_land_with_their_labels(graph_store: Any, base: str, faker: Faker) -> None:
    try:
        assert graph_store.load_graph(world(base, faker)) == (5, 5)
        [ada] = rows(
            graph_store,
            "MATCH (n:Person {title: $base + 'ada'})"
            " RETURN labels(n) AS l, n.age AS age, n.label AS label",
            base,
        )
        assert sorted(ada["l"]) == ["Character", "Person", "Resource"]
        assert (ada["age"], ada["label"]) == (36, ["Ada", "Ада"])
        links = rows(
            graph_store,
            "MATCH (a:Resource)-[r]->(b:Resource) WHERE a.title STARTS WITH $base"
            " RETURN type(r) AS t, b.title AS o",
            base,
        )
        assert sorted(r["t"] for r in links) == [
            "knows",
            "lives_in",
            "mentions",
            "mentions",
            "note",
        ]
    finally:
        cleanup(graph_store, base)


def test_a_second_load_updates_in_place(graph_store: Any, base: str, faker: Faker) -> None:
    try:
        graph_store.load_graph(world(base, faker))
        graph_store.load_graph(world(base, faker, age=37))
        count = rows(
            graph_store,
            "MATCH (n:Resource) WHERE n.title STARTS WITH $base RETURN count(n) AS c",
            base,
        )
        assert count == [{"c": 4}]
        [ada] = rows(
            graph_store, "MATCH (n:Person {title: $base + 'ada'}) RETURN n.age AS age", base
        )
        assert ada["age"] == 37
        edges = rows(
            graph_store,
            "MATCH (a:Resource)-[r]->() WHERE a.title STARTS WITH $base RETURN count(r) AS c",
            base,
        )
        assert edges == [{"c": 5}]
    finally:
        cleanup(graph_store, base)


def test_a_load_that_fails_halfway_writes_nothing(
    graph_store: Any, base: str, faker: Faker, monkeypatch: pytest.MonkeyPatch
) -> None:
    begin = graph_store._backend.begin

    def failing_begin() -> Any:
        tx = begin()
        execute = tx.execute
        calls = {"n": 0}

        def execute_then_fail(query: str, params: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 3:
                raise RuntimeError("the connection dropped")
            return execute(query, params)

        tx.execute = execute_then_fail
        return tx

    monkeypatch.setattr(graph_store._backend, "begin", failing_begin)
    try:
        with pytest.raises(RuntimeError, match="dropped"):
            graph_store.load_graph(world(base, faker), batch=2)
        monkeypatch.undo()
        count = rows(
            graph_store,
            "MATCH (n:Resource) WHERE n.title STARTS WITH $base RETURN count(n) AS c",
            base,
        )
        assert count == [{"c": 0}]
    finally:
        monkeypatch.undo()
        cleanup(graph_store, base)


def test_names_that_are_not_identifiers_are_refused(graph_store: Any, base: str) -> None:
    bad = GraphData({f"{base}x": Node(f"{base}x", ("Bad Label",), {})}, [])
    with pytest.raises(ValueError, match="Bad Label"):
        graph_store.load_graph(bad)
