"""Proposing an ontology with Claude, and applying what a person accepted.

A bug tracker hands its exports (people, bugs, comments) to the plugin. The
model sees each table's columns and a few rows and proposes classes and
relationships with a confidence and a reason. The proposal names the model,
the prompt version and a hash of the sample; every relationship starts
unaccepted. ``apply`` loads only what a person accepted, after checking it
against the data. The client here is a stand-in: no request leaves the test.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from faker import Faker
from rhizodat_ontology import (
    PROMPT_VERSION,
    ProposalError,
    ProposalRefused,
    accepted_relationships,
    propose,
)


def write(path: Path, header: str, rows: list[str]) -> Path:
    path.write_text("\n".join([header, *rows]) + "\n")
    return path


@pytest.fixture
def exports(tmp_path: Path, faker: Faker) -> dict[str, Path]:
    return {
        "people": write(
            tmp_path / "people.csv", "id,name", [f'{i},"{faker.name()}"' for i in range(1, 6)]
        ),
        "bugs": write(
            tmp_path / "bugs.csv",
            "id,title,reporter",
            [f'{i},"{faker.sentence()}",{faker.random_int(1, 5)}' for i in range(1, 21)],
        ),
    }


ANSWER = {
    "classes": [
        {
            "table": "people",
            "label": "Person",
            "description": "someone who reports bugs",
            "key": "id",
            "properties": [{"column": "name", "meaning": "full name"}],
        },
        {
            "table": "bugs",
            "label": "Bug",
            "description": "a reported defect",
            "key": "id",
            "properties": [{"column": "title", "meaning": "one-line summary"}],
        },
    ],
    "relationships": [
        {
            "table": "bugs",
            "column": "reporter",
            "target": "people",
            "name": "reported_by",
            "confidence": 0.9,
            "reason": "reporter values are people ids",
        },
        {
            "table": "bugs",
            "column": "assignee",
            "target": "people",
            "name": "assigned_to",
            "confidence": 0.4,
            "reason": "guessed",
        },
    ],
    "notes": "comments were not provided",
}


class FakeMessages:
    def __init__(self, answer: object, stop_reason: str = "end_turn") -> None:
        self.answer = answer
        self.stop_reason = stop_reason
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Any:  # noqa: ANN401
        self.calls.append(kwargs)
        text = self.answer if isinstance(self.answer, str) else json.dumps(self.answer)
        return SimpleNamespace(
            model=kwargs["model"],
            stop_reason=self.stop_reason,
            stop_details=SimpleNamespace(category="cyber")
            if self.stop_reason == "refusal"
            else None,
            content=[
                SimpleNamespace(type="thinking", thinking=""),
                SimpleNamespace(type="text", text=text),
            ],
        )


def client(answer: object = ANSWER, stop_reason: str = "end_turn") -> Any:  # noqa: ANN401
    messages = FakeMessages(answer, stop_reason)
    return SimpleNamespace(beta=SimpleNamespace(messages=messages), messages_=messages)


def test_a_proposal_carries_classes_relationships_and_provenance(exports: dict[str, Path]) -> None:
    c = client()
    proposal = propose(exports, client=c)

    [call] = c.messages_.calls
    assert call["model"] == "claude-opus-5-5"
    assert call["output_config"]["effort"] == "high"
    assert call["output_config"]["format"]["type"] == "json_schema"
    assert call["fallbacks"] == "default"
    assert "server-side-fallback-2026-07-01" in call["betas"]
    prompt = call["messages"][0]["content"]
    assert "bugs" in prompt  # the tables' columns and rows
    assert "reporter" in prompt

    assert proposal["provenance"]["model"] == "claude-opus-5-5"
    assert proposal["provenance"]["prompt_version"] == PROMPT_VERSION
    assert len(proposal["provenance"]["sample_sha256"]) == 64
    assert [c["label"] for c in proposal["classes"]] == ["Person", "Bug"]
    kept = proposal["relationships"]
    assert [(r["column"], r["accepted"]) for r in kept] == [("reporter", False)]
    # A relationship on a column the data does not have is dropped, and said so.
    assert any("assignee" in n for n in proposal["dropped"])


def test_the_sample_hash_follows_the_data(exports: dict[str, Path]) -> None:
    first = propose(exports, client=client())["provenance"]["sample_sha256"]
    exports["people"].write_text("id,name\n1,Ada\n")
    assert propose(exports, client=client())["provenance"]["sample_sha256"] != first


def test_refusals_and_broken_answers_raise(exports: dict[str, Path]) -> None:
    with pytest.raises(ProposalRefused, match="cyber"):
        propose(exports, client=client(stop_reason="refusal"))
    with pytest.raises(ProposalError, match="max_tokens"):
        propose(exports, client=client(stop_reason="max_tokens"))
    with pytest.raises(ProposalError, match="not JSON"):
        propose(exports, client=client("{nope"))


def test_only_accepted_relationships_are_applied(exports: dict[str, Path]) -> None:
    proposal = propose(exports, client=client())
    assert accepted_relationships(proposal) == []
    proposal["relationships"][0]["accepted"] = True
    [rel] = accepted_relationships(proposal)
    assert (rel["table"], rel["column"], rel["target"]) == ("bugs", "reporter", "people")


# --- applying -----------------------------------------------------------------------------


@pytest.fixture
def graph() -> Any:  # noqa: ANN401
    from conftest import drevo_embedded_available
    from rhizodat import Rhizodat

    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
    d = Rhizodat(stores={"graph": "drevo+mem://"})
    yield d
    d.close()


def test_apply_loads_only_what_was_accepted(exports: dict[str, Path], graph: Any) -> None:  # noqa: ANN401
    from rhizodat_ontology import apply

    proposal = propose(exports, client=client())
    with pytest.raises(ProposalError, match="no relationship is accepted"):
        apply(graph, proposal, store="graph")
    proposal["relationships"][0]["accepted"] = True
    report = apply(graph, proposal, store="graph")
    [rel] = report.relationships
    assert (rel.matched, rel.total) == (20, 20)
    assert (report.nodes, report.edges) == (25, 20)
    with pytest.raises(ProposalError, match="format"):
        apply(graph, {**proposal, "format": 99}, store="graph")


def test_the_cli_proposes_and_applies(
    exports: dict[str, Path],
    tmp_path: Path,
    graph: Any,  # noqa: ANN401
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import rhizodat_ontology
    from rhizodat_ontology.cli import main

    monkeypatch.setattr(rhizodat_ontology, "_client", client)
    out = tmp_path / "proposal.json"
    assert main(["propose", *map(str, exports.values()), "-o", str(out)]) == 0
    assert "2 classes, 1 relationships" in capsys.readouterr().out

    monkeypatch.setenv("RHIZODAT_STORE_GRAPH", "drevo+mem://")
    assert main(["apply", str(out), "--store", "graph"]) == 2
    assert "no relationship is accepted" in capsys.readouterr().err
    reviewed = json.loads(out.read_text())
    reviewed["relationships"][0]["accepted"] = True
    out.write_text(json.dumps(reviewed))
    assert main(["apply", str(out), "--store", "graph"]) == 0
    assert "bugs.reporter -> people.id  20/20" in capsys.readouterr().out

    monkeypatch.setattr(rhizodat_ontology, "_client", lambda: client(stop_reason="refusal"))
    assert main(["propose", *map(str, exports.values()), "-o", str(out)]) == 1
    assert main(["apply", str(tmp_path / "missing.json"), "--store", "graph"]) == 2
