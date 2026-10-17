"""Reading RDF (Turtle, N-Triples, N3, JSON-LD, RDF/XML, OWL) as nodes and edges.

Each resource is one node keyed by its IRI (a blank node by ``_:<id>``, unique
within one file). Every ``rdf:type`` of a resource is a label (the class's
local name). A literal value is a property named after its predicate's local
name; several values of one predicate (a name in two languages) are a list. A
link to another resource is an edge typed after its predicate, and the target
is a node even when the file says nothing else about it.

Names are made identifiers (``lives-in`` → ``lives_in``). A local name that two
vocabularies of the file share (``a:name``, ``b:name``) keeps them apart with
their prefixes (``a_name``, ``b_name``). Values keep their type where the graph
can hold it (integers, floats, booleans, text); dates, times and decimals are
stored as their exact ISO or decimal text.
"""

import datetime as dt
import decimal
import re
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = ["Edge", "GraphData", "Node", "graph_format", "read_rdf"]

_FORMATS = {
    ".ttl": "turtle",
    ".turtle": "turtle",
    ".nt": "nt",
    ".n3": "n3",
    ".jsonld": "json-ld",
    ".json-ld": "json-ld",
    ".rdf": "xml",
    ".owl": "xml",
    ".xml": "xml",
}
_COMPRESSED = (".gz", ".bz2", ".zst", ".lz4", ".br")
_NOT_IDENT = re.compile(r"[^A-Za-z0-9_]+")


def graph_format(source: str | Path) -> str | None:
    """The RDF format named by the file's suffix, or ``None`` if it is not RDF."""
    name = str(source).split("?", 1)[0].lower()
    for suffix in _COMPRESSED:
        name = name.removesuffix(suffix)
    for suffix, fmt in _FORMATS.items():
        if name.endswith(suffix):
            return fmt
    return None


@dataclass(frozen=True, slots=True)
class Node:
    key: str
    """The IRI, or ``_:<id>`` for a blank node."""
    labels: tuple[str, ...] = ()
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True, order=True)
class Edge:
    source: str
    type: str
    target: str


@dataclass(frozen=True, slots=True)
class GraphData:
    nodes: dict[str, Node]
    edges: list[Edge]


def _ident(text: str) -> str:
    name = _NOT_IDENT.sub("_", text).strip("_")
    if not name:
        return "value"
    return f"_{name}" if name[0].isdigit() else name


def _split(iri: str) -> tuple[str, str]:
    """``(namespace, local name)`` of an IRI: after the last ``#``, ``/`` or ``:``."""
    for sep in ("#", "/", ":"):
        head, found, tail = iri.rpartition(sep)
        if found and tail:
            return head + sep, tail
    return "", iri


def _value(literal: Any) -> Any:  # noqa: ANN401 - an rdflib Literal
    value = literal.toPython()
    if value is literal:  # an unknown datatype: rdflib gives the literal back
        return str(literal)
    match value:
        case bool() | int() | float() | str():
            return value
        case decimal.Decimal():
            return str(value)
        case dt.datetime() | dt.date() | dt.time():
            return value.isoformat()
    return str(literal)  # an unknown datatype: its lexical form


def read_rdf(source: str | Path, *, format: str | None = None) -> GraphData:
    """The resources and links of an RDF file (local or in object storage)."""
    import rdflib
    from rdflib.namespace import RDF

    from rhizodat.ingest.sources import _shown, _stream

    fmt = format or graph_format(source)
    if fmt is None:
        raise ValueError(
            f"cannot tell the RDF format of {_shown(source)}: name it"
            f" ({', '.join(_FORMATS)}), or pass format="
        )
    with _stream(source) as raw:
        data = raw.read()
    graph = rdflib.Graph()
    try:
        with warnings.catch_warnings():
            # rdflib's JSON-LD parser uses its own deprecated ConjunctiveGraph.
            warnings.simplefilter("ignore", DeprecationWarning)
            graph.parse(data=data, format=fmt)
    except Exception as exc:
        raise ValueError(f"{_shown(source)} is not valid {fmt}: {exc}") from None

    def key(term: Any) -> str:  # noqa: ANN401
        return f"_:{term}" if isinstance(term, rdflib.BNode) else str(term)

    prefixes = {str(ns): p for p, ns in graph.namespace_manager.namespaces()}
    locals_by_ns: dict[str, set[str]] = defaultdict(set)
    for predicate in set(graph.predicates()):
        ns, local = _split(str(predicate))
        locals_by_ns[local].add(ns)

    def name(predicate: Any) -> str:  # noqa: ANN401
        ns, local = _split(str(predicate))
        if len(locals_by_ns[local]) > 1:
            return _ident(f"{prefixes.get(ns) or 'ns'}_{local}")
        return _ident(local)

    labels: dict[str, set[str]] = defaultdict(set)
    props: dict[str, dict[str, list[Any]]] = defaultdict(lambda: defaultdict(list))
    edges: set[Edge] = set()
    keys: dict[str, None] = {}

    def node(term: Any) -> str:  # noqa: ANN401
        k = key(term)
        keys.setdefault(k, None)
        return k

    for s, p, o in graph:
        subject = node(s)
        if p == RDF.type and not isinstance(o, rdflib.Literal):
            labels[subject].add(_ident(_split(str(o))[1]))
        elif isinstance(o, rdflib.Literal):
            props[subject][name(p)].append(_value(o))
        else:
            edges.add(Edge(subject, name(p), node(o)))

    nodes = {}
    for k in keys:
        values = {n: (v[0] if len(v) == 1 else sorted(v, key=str)) for n, v in props[k].items()}
        properties = ({} if k.startswith("_:") else {"iri": k}) | values
        nodes[k] = Node(k, tuple(sorted(labels[k])), properties)
    return GraphData(nodes, sorted(edges))
