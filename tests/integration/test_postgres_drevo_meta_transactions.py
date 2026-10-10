"""Story editor: chapter text in PostgreSQL, the character graph in drevo, one meta-transaction."""

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from faker import Faker

from conftest import drevo_embedded_available
from datorium.adapters.drevo import DrevoStore
from datorium.adapters.postgres import PostgresJournal, PostgresStore
from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import Committed, Coordinator, Participant, Recovery
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, State
from datorium.core.retry import Retry
from fakes import Crash


def raised(call: Callable[[], object]) -> BaseException:
    """The exception ``call`` raises; the type depends on the transport, so no pytest.raises."""
    try:
        call()
    except Exception as exc:
        return exc
    pytest.fail("expected an error")


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


class CrashingParticipant:
    """Delegates to a real participant but dies at a chosen call."""

    def __init__(self, inner: Participant, die_on: str) -> None:
        self.inner = inner
        self.die_on = die_on

    def __getattr__(self, name: str) -> object:
        attr = getattr(self.inner, name)
        if name == self.die_on:

            def die(*a: object, **k: object) -> object:
                raise Crash()

            return die
        return attr


@dataclass
class Editor:
    pg: PostgresStore
    graph: DrevoStore
    journal: PostgresJournal
    wall: Wall
    label: str  # unique per test so a shared drevo server stays clean

    def participants(self) -> dict[str, Participant]:
        return {"pg": self.pg.participant(), "graph": self.graph.participant()}

    def coordinator(self, participants: dict[str, Participant] | None = None) -> Coordinator:
        return Coordinator(
            journal=self.journal,
            participants=participants or self.participants(),
            clock=HybridLogicalClock(),
            finish=Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
            sleep=lambda s: None,
        )

    def write_chapter(
        self, txn: uuid.UUID, title: str, text: str, characters: list[str]
    ) -> list[object]:
        chapter = self.pg.branch(txn)
        chapter.execute(
            "INSERT INTO chapters (dat_id, title, body) VALUES (%s, %s, %s)", (txn, title, text)
        )
        graph = self.graph.branch(txn)
        graph.execute(
            f"CREATE (c:{self.label} {{dat_id: $id, heading: $title}}) "
            f"WITH c UNWIND $names AS name "
            f"CREATE (c)-[:APPEARS]->(:{self.label}Character {{name: name}})",
            {"id": str(txn), "title": title, "names": characters},
        )
        return [graph, chapter]

    def chapters(self) -> int:
        return int(self.pg.execute("SELECT count(*) AS n FROM chapters").one()["n"])

    def characters(self) -> int:
        q = f"MATCH (:{self.label})-[:APPEARS]->(c) RETURN count(c) AS n"
        return int(self.graph.execute(q).one()["n"])

    def prepared(self) -> list[str]:
        gids = self.pg.participant().prepared_gids() + self.graph.participant().prepared_gids()
        return [g for g in gids if g.startswith("dat:")]


@pytest.fixture(params=["embedded", "bolt"])
def editor(
    request: pytest.FixtureRequest, pg_dsn: str, pg_schema: str, tmp_path: Path
) -> Iterator[Editor]:
    if request.param == "bolt":
        dsn = request.getfixturevalue("drevo_bolt_dsn")
    elif drevo_embedded_available():
        dsn = f"drevo://{tmp_path / 'story.redb'}"
    else:
        pytest.skip("drevo-py with 2PC is not installed")
    pg = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    pg.install()
    pg.execute("CREATE TABLE chapters (dat_id uuid PRIMARY KEY, title text NOT NULL, body text)")
    graph = DrevoStore(dsn, name="graph")
    wall = Wall()
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    ed = Editor(pg, graph, journal, wall, f"Ch{uuid.uuid4().hex[:8]}")
    yield ed
    try:
        for gid in graph.participant().prepared_gids():
            if gid.startswith("dat:"):
                graph.participant().abort(BranchRecord("graph", gid, True))
    finally:
        journal.close()
        graph.close()
        pg.close()


def test_chapter_and_its_character_graph_commit_together(editor: Editor, faker: Faker) -> None:
    txn = new_id()
    branches = editor.write_chapter(
        txn, faker.sentence(), faker.text(), [faker.first_name() for _ in range(3)]
    )
    outcome = editor.coordinator().commit(txn, branches)  # type: ignore[arg-type]

    assert isinstance(outcome, Committed)
    assert (editor.chapters(), editor.characters()) == (1, 3)
    assert editor.prepared() == []
    record = editor.journal.get(txn)
    assert record is not None
    assert record.state is State.DONE


def test_crash_before_the_decision_releases_the_graph_fence(editor: Editor, faker: Faker) -> None:
    txn = new_id()
    branches = editor.write_chapter(txn, faker.sentence(), faker.text(), ["Ada"])

    class CrashOnDecision:
        def __init__(self, inner: PostgresJournal) -> None:
            self.inner = inner

        def __getattr__(self, name: str) -> object:
            return getattr(self.inner, name)

        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, **kw: int | None
        ) -> object:
            if new is State.COMMITTED:
                raise Crash()
            return self.inner.transition(dat_txn, expected, new, **kw)

    coordinator = editor.coordinator()
    coordinator._journal = CrashOnDecision(editor.journal)  # type: ignore[assignment]
    with pytest.raises(Crash):
        coordinator.commit(txn, branches)  # type: ignore[arg-type]
    assert len(editor.prepared()) == 2  # both stores hold it; drevo is fenced for writers

    editor.wall.ms += 60_000
    report = Recovery(
        journal=editor.journal, participants=editor.participants(), wall_ms=editor.wall
    ).run_once()
    assert report.aborted == [txn]
    assert (editor.chapters(), editor.characters()) == (0, 0)
    assert editor.prepared() == []
    editor.graph.execute(f"CREATE (:{editor.label}Note)")  # the fence is gone


def test_crash_while_finishing_is_rolled_forward(editor: Editor, faker: Faker) -> None:
    txn = new_id()
    branches = editor.write_chapter(txn, faker.sentence(), faker.text(), ["Ada", "Grace"])
    participants = editor.participants()
    participants["pg"] = CrashingParticipant(participants["pg"], "finish")  # type: ignore[assignment]

    with pytest.raises(Crash):
        editor.coordinator(participants).commit(txn, branches)  # type: ignore[arg-type]
    # drevo is a lock holder too: whichever finished first, the decision is durable.

    editor.wall.ms += 60_000
    report = Recovery(
        journal=editor.journal, participants=editor.participants(), wall_ms=editor.wall
    ).run_once()
    assert report.finished == [txn]
    assert (editor.chapters(), editor.characters()) == (1, 2)
    assert editor.prepared() == []


def test_a_concurrent_graph_writer_sees_a_retryable_fence(editor: Editor, faker: Faker) -> None:
    txn = new_id()
    graph_branch, chapter_branch = editor.write_chapter(
        txn, faker.sentence(), faker.text(), ["Ada"]
    )
    graph_branch.stage(txn, 1)  # type: ignore[attr-defined]
    graph_branch.prepare(f"dat:{txn}:graph")  # type: ignore[attr-defined]

    other = editor.graph.branch(new_id())
    other.execute(f"CREATE (:{editor.label}Other)")
    fenced = raised(other.commit_one_phase)
    assert other.is_retryable(fenced)

    graph_branch.rollback()  # type: ignore[attr-defined]
    chapter_branch.rollback()  # type: ignore[attr-defined]
    assert editor.prepared() == []
