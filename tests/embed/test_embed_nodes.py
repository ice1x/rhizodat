"""Embedding a graph's nodes and finding the nearest ones, through a Rhizodat.

A CBT journal keeps its entries as graph nodes; it embeds their text once,
skips entries whose text and model are unchanged, re-embeds an edited entry
or everything after a model change, and finds entries close to a new thought.
"""

from collections.abc import Iterator

import pytest

from embedders_fixture import Fixed
from rhizodat import Rhizodat
from rhizodat.embed import HashEmbedder, embed_nodes, similar
from rhizodat.testing.memory import MemoryEntityStore


@pytest.fixture
def dat() -> Iterator[Rhizodat]:
    d = Rhizodat(stores={"journal": "drevo+mem://"})
    yield d
    d.close()


def entry(dat: Rhizodat, key: str, text: str) -> None:
    dat.store("journal").execute(
        "MERGE (n:Entry {title: $k}) SET n.text = $t", {"k": f"entry:{key}", "t": text}
    )


def test_entries_are_embedded_once_and_again_when_they_change(dat: Rhizodat) -> None:
    entry(dat, "1", "anxious before the exam")
    entry(dat, "2", "a calm walk in the park")
    e = HashEmbedder(128)

    first = embed_nodes(dat, store="journal", label="Entry", text="text", embedder=e)
    assert (first.embedded, first.unchanged) == (2, 0)
    again = embed_nodes(dat, store="journal", label="Entry", text="text", embedder=e)
    assert (again.embedded, again.unchanged) == (0, 2)

    entry(dat, "2", "a calm walk by the river")
    edited = embed_nodes(dat, store="journal", label="Entry", text="text", embedder=e)
    assert (edited.embedded, edited.unchanged) == (1, 1)

    other = embed_nodes(dat, store="journal", label="Entry", text="text", embedder=HashEmbedder(64))
    assert (other.embedded, other.model) == (2, "hash-64")  # a new model re-embeds all


def test_the_nearest_entries_to_a_new_thought(dat: Rhizodat) -> None:
    for key, text in {
        "1": "anxious before the exam",
        "2": "a calm walk in the park",
        "3": "exam results made me anxious again",
    }.items():
        entry(dat, key, text)
    e = HashEmbedder(256)
    embed_nodes(dat, store="journal", label="Entry", text="text", embedder=e)
    matches = similar(
        dat, store="journal", label="Entry", query="anxious about the exam", embedder=e, k=2
    )
    assert {m.key for m in matches} == {"entry:1", "entry:3"}
    assert matches[0].score >= matches[1].score
    by_vector = similar(dat, store="journal", label="Entry", query=e.embed(["a calm walk"])[0], k=1)
    assert by_vector[0].key == "entry:2"


def test_batches_and_refusals(dat: Rhizodat) -> None:
    for i in range(5):
        entry(dat, str(i), f"entry number {i}")
    fixed = Fixed()
    report = embed_nodes(dat, store="journal", label="Entry", text="text", embedder=fixed, batch=2)
    assert (report.embedded, fixed.calls) == (5, 3)

    with pytest.raises(ValueError, match="needs an embedder"):
        similar(dat, store="journal", label="Entry", query="text")
    with pytest.raises(ValueError, match="2 dimensions"):
        embed_nodes(dat, store="journal", label="Entry", text="text", embedder=_Wrong())
    plain = Rhizodat(stores={"mem": MemoryEntityStore("mem")})
    try:
        with pytest.raises(ValueError, match="cannot hold vectors"):
            embed_nodes(plain, store="mem", label="Entry", text="text", embedder=fixed)
    finally:
        plain.close()


class _Wrong:
    model = "wrong"
    dimensions = 2

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [[1.0] for _ in texts]


# --- the CLI ------------------------------------------------------------------------------


def test_embed_and_similar_from_a_shell(
    drevo_bolt_dsn: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import json
    import uuid

    from rhizodat.check import main

    label = f"Note{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("RHIZODAT_STORE_GRAPH", drevo_bolt_dsn)
    with Rhizodat(stores={"graph": drevo_bolt_dsn}) as d:
        for key, text in {
            "1": "storm at sea",
            "2": "quiet library",
            "3": "sea storm at night",
        }.items():
            d.store("graph").execute(
                f"MERGE (n:{label} {{title: $k}}) SET n.body = $t",
                {"k": f"{label}:{key}", "t": text},
            )
        try:
            common = ["--store", "graph", "--label", label, "--model", "hash:128"]
            assert main(["embed", *common, "--text", "body"]) == 0
            assert "3 embedded, 0 unchanged (hash-128)" in capsys.readouterr().out
            assert main(["embed", *common, "--text", "body", "--json"]) == 0
            assert json.loads(capsys.readouterr().out)["unchanged"] == 3

            assert main(["similar", *common, "-k", "2", "--json", "a storm on the sea"]) == 0
            found = json.loads(capsys.readouterr().out)
            assert {f["key"] for f in found} == {f"{label}:1", f"{label}:3"}
            assert main(["similar", *common, "-k", "1", "quiet library"]) == 0
            assert f"{label}:2" in capsys.readouterr().out

            assert (
                main(
                    [
                        "embed",
                        "--store",
                        "graph",
                        "--label",
                        label,
                        "--text",
                        "body",
                        "--model",
                        "nope",
                    ]
                )
                == 2
            )
            assert "module:Class" in capsys.readouterr().err
            assert main(["similar", "--store", "graph", "--label", "bad label", "x"]) == 2
            assert "bad label" in capsys.readouterr().err
        finally:
            d.store("graph").execute(f"MATCH (n:{label}) DETACH DELETE n")
