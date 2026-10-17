"""Bug tracker with embeddings on the Repository, in PostgreSQL and in a drevo graph.

Issues keep their title in PostgreSQL; their embedding and the model that made
it live either in PostgreSQL too or in drevo (both are tried). Triage finds
duplicates of a new report with ``similar``; after the tracker switches to an
embedder that also reads the description, every issue is re-embedded, and the
search as of the day before still runs on the old vectors.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import pytest
from faker import Faker

from conftest import drevo_embedded_available
from rhizodat import Rhizodat, Vector, entity
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.parts import Part
from rhizodat.embed import HashEmbedder

DIMENSIONS = 64


@entity(stores={"text": ["title", "description", "embedding", "model"]}, name="bugs.Issue")
class PgIssue:
    title: str
    description: str
    embedding: Annotated[list[float], Vector(DIMENSIONS)] | None
    model: str | None


@entity(
    stores={"text": ["title", "description"], "graph": ["embedding", "model"]},
    name="bugs.GraphIssue",
)
class GraphIssue:
    title: str
    description: str
    embedding: Annotated[list[float], Vector(DIMENSIONS)] | None
    model: str | None


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 17, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@dataclass
class Tracker:
    dat: Rhizodat
    graph: DrevoStore
    wall: Wall
    kind: Any  # PgIssue or GraphIssue

    def clear(self) -> None:
        self.graph.execute(f"MATCH (v:{Part.of(GraphIssue, 'graph').table}) DETACH DELETE v")
        self.graph.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")


@pytest.fixture(params=[("pg", "file"), ("graph", "file"), ("graph", "bolt")])
def tracker(
    request: pytest.FixtureRequest, tmp_path: Path, pg_dsn: str, pg_schema: str
) -> Iterator[Tracker]:
    vectors_in, drevo = request.param
    wall = Wall()
    if drevo == "bolt":
        graph_dsn = str(request.getfixturevalue("drevo_bolt_dsn"))
    elif drevo_embedded_available():
        graph_dsn = f"drevo://{tmp_path / 'g.drevo'}"
    else:
        pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
    graph = DrevoStore(graph_dsn, name="graph")
    dat = Rhizodat(
        stores={"text": PostgresStore(pg_dsn, name="text", schema=pg_schema), "graph": graph},
        journal=PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    t = Tracker(dat, graph, wall, PgIssue if vectors_in == "pg" else GraphIssue)
    t.clear()
    yield t
    t.clear()
    dat.close()


TITLES = [
    "login page crashes after password reset",
    "invoice totals are rounded wrong",
    "dark mode loses contrast on charts",
    "export to csv drops the last row",
]


class ReadsDescriptions:
    """The tracker's second embedder: the title and the description together."""

    model = "hash-64+description"
    dimensions = DIMENSIONS

    def __init__(self) -> None:
        self._inner = HashEmbedder(DIMENSIONS)

    def embed(self, texts: list[str]) -> list[list[float]]:
        return self._inner.embed(texts)


def test_duplicates_are_found_and_reembedding_keeps_the_past(
    tracker: Tracker, faker: Faker
) -> None:
    first, second = HashEmbedder(DIMENSIONS), ReadsDescriptions()
    issues = tracker.dat.repository(tracker.kind)
    filed = []
    for title in TITLES:
        issue = tracker.kind(
            title=title,
            description=faker.paragraph(),
            embedding=first.embed([title])[0],
            model=first.model,
        )
        issues.add(issue)
        filed.append(issue)
    issues.add(tracker.kind(title=faker.sentence(), description="", embedding=None, model=None))

    [dup] = issues.similar("embedding", "the login page crashes", k=1, embedder=first)
    assert dup.entity.title == TITLES[0]
    assert dup.entity.embedding == filed[0].embedding  # floats come back exactly
    assert dup.entity.model == "hash-64"
    before = tracker.wall.now()

    tracker.wall.ms += 86_400_000
    for issue in filed:
        issue.embedding = second.embed([f"{issue.title}\n{issue.description}"])[0]
        issue.model = second.model
        issues.update(issue)

    found = issues.similar("embedding", filed[1].embedding, k=len(TITLES) + 1)
    assert len(found) == len(TITLES)  # the issue without a vector is left out
    assert found[0].entity.dat_id == filed[1].dat_id
    assert found[0].score == pytest.approx(1.0)
    assert {m.entity.model for m in found} == {second.model}

    then = issues.similar("embedding", "the login page crashes", k=1, embedder=first, as_of=before)
    assert (then[0].entity.dat_id, then[0].entity.model) == (filed[0].dat_id, "hash-64")
    assert [v.model for v in issues.history(filed[0].dat_id)] == ["hash-64", second.model]
