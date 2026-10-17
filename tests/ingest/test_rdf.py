"""Reading RDF for a graph import: resources become nodes, links become edges.

A story editor keeps its world as an ontology in Turtle: characters, places
and chapters, typed with schema.org and its own classes. Each resource is one
node keyed by its IRI; every ``rdf:type`` is a label; literal values are
properties (a language-tagged name in two languages is a list); a link to
another resource is an edge typed after its predicate.
"""

import datetime as dt
import json
from pathlib import Path

import pytest

from rhizodat.ingest.graph import graph_format, read_rdf

STORY = """
@prefix ex: <https://example.org/story#> .
@prefix schema: <https://schema.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:ada a schema:Person, ex:Character ;
    schema:name "Ada" ;
    ex:age 36 ;
    ex:born "1815-12-10"^^xsd:date ;
    ex:knows ex:charles ;
    ex:lives-in ex:london ;
    rdfs:label "Ada"@en, "Ада"@ru .

ex:charles a schema:Person ; schema:name "Charles" .
ex:london a schema:Place ; schema:name "London" .
ex:chapter1 a ex:Chapter ;
    ex:mentions ex:ada, ex:london ;
    ex:note [ ex:text "draft" ] .
"""

EX = "https://example.org/story#"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / name
    path.write_text(text)
    return path


def test_resources_are_nodes_with_labels_and_properties(tmp_path: Path) -> None:
    graph = read_rdf(write(tmp_path, "story.ttl", STORY))
    ada = graph.nodes[f"{EX}ada"]
    assert ada.labels == ("Character", "Person")
    assert ada.properties == {
        "iri": f"{EX}ada",
        "name": "Ada",
        "age": 36,
        "born": "1815-12-10",
        "label": ["Ada", "Ада"],
    }
    assert graph.nodes[f"{EX}london"].labels == ("Place",)


def test_links_are_edges_typed_after_the_predicate(tmp_path: Path) -> None:
    graph = read_rdf(write(tmp_path, "story.ttl", STORY))
    edges = {
        (e.source.rsplit("#", 1)[-1], e.type, e.target.rsplit("#", 1)[-1]) for e in graph.edges
    }
    assert {
        ("ada", "knows", "charles"),
        ("ada", "lives_in", "london"),  # not an identifier as it was
        ("chapter1", "mentions", "ada"),
        ("chapter1", "mentions", "london"),
    } <= edges
    [note] = [e for e in graph.edges if e.type == "note"]
    blank = graph.nodes[note.target]
    assert note.target.startswith("_:")
    assert blank.labels == ()
    assert blank.properties["text"] == "draft"


def test_a_local_name_from_two_vocabularies_keeps_them_apart(tmp_path: Path) -> None:
    text = """
    @prefix a: <https://a.example/> .
    @prefix b: <https://b.example/> .
    a:x a:name "from a" ; b:name "from b" ; a:size 1 .
    """
    [node] = read_rdf(write(tmp_path, "t.ttl", text)).nodes.values()
    assert node.properties == {
        "iri": "https://a.example/x",
        "a_name": "from a",
        "b_name": "from b",
        "size": 1,
    }


def test_literal_values_keep_their_type(tmp_path: Path) -> None:
    text = """
    @prefix ex: <https://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:x ex:n 3 ; ex:f 1.5e0 ; ex:ok true ; ex:price "9.99"^^xsd:decimal ;
         ex:at "2026-01-01T10:00:00Z"^^xsd:dateTime ; ex:raw "x"^^ex:custom .
    """
    p = read_rdf(write(tmp_path, "t.ttl", text)).nodes["https://example.org/x"].properties
    assert (p["n"], p["f"], p["ok"]) == (3, 1.5, True)
    assert p["price"] == "9.99"  # exact, as text
    assert dt.datetime.fromisoformat(p["at"]) == dt.datetime(2026, 1, 1, 10, tzinfo=dt.UTC)
    assert p["raw"] == "x"


@pytest.mark.parametrize(
    ("name", "fmt"),
    [
        ("a.ttl", "turtle"),
        ("a.nt", "nt"),
        ("a.n3", "n3"),
        ("a.jsonld", "json-ld"),
        ("a.rdf", "xml"),
        ("a.owl", "xml"),
        ("a.ttl.gz", "turtle"),
        ("a.csv", None),
    ],
)
def test_graph_formats_by_suffix(name: str, fmt: str | None) -> None:
    assert graph_format(name) == fmt


def test_json_ld_and_n_triples_read_the_same_graph(tmp_path: Path) -> None:
    jsonld = [
        {
            "@id": f"{EX}ada",
            "@type": ["https://schema.org/Person"],
            "https://schema.org/name": [{"@value": "Ada"}],
            f"{EX}knows": [{"@id": f"{EX}charles"}],
        }
    ]
    nt = (
        f"<{EX}ada> <{RDF_TYPE}> <https://schema.org/Person> .\n"
        f'<{EX}ada> <https://schema.org/name> "Ada" .\n'
        f"<{EX}ada> <{EX}knows> <{EX}charles> .\n"
    )
    a = read_rdf(write(tmp_path, "a.jsonld", json.dumps(jsonld)))
    b = read_rdf(write(tmp_path, "a.nt", nt))
    assert a.nodes[f"{EX}ada"] == b.nodes[f"{EX}ada"]
    assert a.edges == b.edges
    assert f"{EX}charles" in a.nodes  # a linked resource is a node even without triples


def test_a_broken_file_says_where(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"bad\.ttl"):
        read_rdf(write(tmp_path, "bad.ttl", "ex:a ex:b ."))
