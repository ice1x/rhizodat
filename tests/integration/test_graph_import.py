"""A story editor loads its world ontology into the graph, then asks it questions.

Characters, places and chapters come as Turtle; after the import the editor
finds who a chapter mentions and where they live with plain Cypher. A revised
file updates the graph in place; the CLI does the same from a shell.
"""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from rhizodat import Rhizodat
from rhizodat.check import main
from rhizodat.ingest import import_graph


def story(base: str, *, ada_age: int = 36) -> str:
    return f"""
    @prefix ex: <{base}> .
    @prefix schema: <https://schema.org/> .
    ex:ada a schema:Person, ex:Character ; schema:name "Ada" ; ex:age {ada_age} ;
        ex:lives-in ex:london .
    ex:charles a schema:Person ; schema:name "Charles" ; ex:lives-in ex:london .
    ex:london a schema:Place ; schema:name "London" .
    ex:chapter1 a ex:Chapter ; schema:name "Engines" ; ex:mentions ex:ada, ex:charles .
    """


@pytest.fixture
def base() -> str:
    return f"https://example.org/{uuid.uuid4().hex[:10]}/story#"


@pytest.fixture
def dat(drevo_bolt_dsn: str, base: str) -> Iterator[Rhizodat]:
    d = Rhizodat(stores={"graph": drevo_bolt_dsn})
    yield d
    try:
        d.store("graph").execute(
            "MATCH (n:Resource) WHERE n.title STARTS WITH $b DETACH DELETE n", {"b": base}
        )
    finally:
        d.close()


def ask(dat: Rhizodat, query: str, base: str) -> list[dict[str, Any]]:
    return [dict(r) for r in dat.store("graph").execute(query, {"b": base}).all()]


def test_the_world_lands_and_answers_questions(dat: Rhizodat, base: str, tmp_path: Path) -> None:
    source = tmp_path / "world.ttl"
    source.write_text(story(base))
    report = import_graph(dat, source, store="graph")
    assert (report.nodes, report.edges) == (4, 4)

    mentioned = ask(
        dat,
        "MATCH (c:Chapter)-[:mentions]->(p:Person)-[:lives_in]->(l:Place)"
        " WHERE c.title STARTS WITH $b RETURN p.name AS who, l.name AS place",
        base,
    )
    assert sorted((r["who"], r["place"]) for r in mentioned) == [
        ("Ada", "London"),
        ("Charles", "London"),
    ]

    # A revision of the file: Ada's age changes, nothing is duplicated.
    source.write_text(story(base, ada_age=37))
    import_graph(dat, source, store="graph")
    people = ask(
        dat, "MATCH (p:Person) WHERE p.title STARTS WITH $b RETURN p.name AS n, p.age AS a", base
    )
    assert sorted((r["n"], r["a"]) for r in people) == [("Ada", 37), ("Charles", None)]


def test_a_store_without_graphs_is_named(base: str, pg_dsn: str, tmp_path: Path) -> None:
    source = tmp_path / "world.ttl"
    source.write_text(story(base))
    with (
        Rhizodat(stores={"pg": pg_dsn}) as d,
        pytest.raises(ValueError, match="cannot load graphs"),
    ):
        import_graph(d, source, store="pg")


def test_the_cli_plans_and_imports(
    drevo_bolt_dsn: str,
    base: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    dat: Rhizodat,
) -> None:
    monkeypatch.setenv("RHIZODAT_STORE_GRAPH", drevo_bolt_dsn)
    source = tmp_path / "world.ttl"
    source.write_text(story(base))

    assert main(["import", str(source), "--store", "graph", "--dry-run", "--json"]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan == {
        "nodes": 4,
        "edges": 4,
        "labels": ["Chapter", "Character", "Person", "Place"],
        "edge_types": ["lives_in", "mentions"],
    }
    assert ask(dat, "MATCH (n:Resource) WHERE n.title STARTS WITH $b RETURN n", base) == []

    assert main(["import", str(source), "--store", "graph"]) == 0
    assert "4 nodes, 4 edges -> graph" in capsys.readouterr().out
    assert main(["import", str(source), "--store", "graph", "--table", "t"]) == 2
    assert "--table does not apply" in capsys.readouterr().err
    assert main(["import", str(tmp_path / "x.csv"), "--store", "graph"]) == 2
    assert "--table is needed" in capsys.readouterr().err
