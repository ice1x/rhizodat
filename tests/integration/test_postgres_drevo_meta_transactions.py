"""Story editor: chapter text in PostgreSQL, the character graph in drevo, one meta-transaction."""

import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from faker import Faker

from conftest import drevo_embedded_available
from fakes import Crash
from own_prepared import OwnBranches
from rhizodat.adapters.drevo import DrevoStore
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.coordinator import Committed, Coordinator, Participant, Recovery, gid_for
from rhizodat.core.identity import new_id
from rhizodat.core.journal import State
from rhizodat.core.retry import Retry


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
    own: OwnBranches  # the meta-transactions this test opened branches for

    def participants(self) -> dict[str, Participant]:
        return {self.pg.name: self.pg.participant(), self.graph.name: self.graph.participant()}

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
        """This test's prepared gids (the servers also hold other processes' gids)."""
        return self.own.prepared(self.pg.name, self.pg.participant()) + self.own.prepared(
            self.graph.name, self.graph.participant()
        )

    def release(self) -> None:
        """Teardown: roll back the graph branches this test left prepared, and only those."""
        self.own.abort_prepared(self.graph.name, self.graph.participant())


@pytest.fixture(params=["embedded", "bolt"])
def editor(
    request: pytest.FixtureRequest, pg_dsn: str, pg_schema: str, tmp_path: Path
) -> Iterator[Editor]:
    if request.param == "bolt":
        dsn = request.getfixturevalue("drevo_bolt_dsn")
    elif drevo_embedded_available():
        dsn = f"drevo://{tmp_path / 'story.drevo'}"
    else:
        pytest.skip("drevo-py with 2PC is not installed")
    # Store names unique to the test: on the shared servers, recovery's orphan sweep
    # then only ever sees this test's gids, never another process's.
    suffix = uuid.uuid4().hex[:8]
    own = OwnBranches()
    pg = own.track(PostgresStore(pg_dsn, name=f"pg_{suffix}", schema=pg_schema))
    pg.install()
    pg.execute("CREATE TABLE chapters (dat_id uuid PRIMARY KEY, title text NOT NULL, body text)")
    graph = own.track(DrevoStore(dsn, name=f"graph_{suffix}"))
    wall = Wall()
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    ed = Editor(pg, graph, journal, wall, f"Ch{suffix}", own)
    yield ed
    try:
        ed.release()
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
    participants[editor.pg.name] = CrashingParticipant(participants[editor.pg.name], "finish")  # type: ignore[assignment]

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
    """A writer touching what a prepared chapter touched (the book's chapter count)
    is refused, retryably, until the meta-transaction ends."""
    book = f"MATCH (b:{editor.label}Book)"
    editor.graph.execute(f"CREATE (:{editor.label}Book {{chapters: 0}})")
    txn = new_id()
    graph_branch, chapter_branch = editor.write_chapter(
        txn, faker.sentence(), faker.text(), ["Ada"]
    )
    graph_branch.execute(f"{book} SET b.chapters = b.chapters + 1")  # type: ignore[attr-defined]
    graph_branch.stage(txn, 1)  # type: ignore[attr-defined]
    graph_branch.prepare(gid_for(txn, editor.graph.name))  # type: ignore[attr-defined]

    other = editor.graph.branch(new_id())
    other.execute(f"{book} SET b.chapters = b.chapters + 1")
    fenced = raised(other.commit_one_phase)
    assert other.is_retryable(fenced)

    graph_branch.rollback()  # type: ignore[attr-defined]
    chapter_branch.rollback()  # type: ignore[attr-defined]
    assert editor.prepared() == []


def test_teardown_leaves_another_processes_prepared_branch_alone(
    editor: Editor, faker: Faker
) -> None:
    """The Bolt drevo server is shared with other test processes (``cargo test``, another
    pytest): the teardown rolls back this test's prepared branches, never theirs."""
    theirs = new_id()
    foreign = DrevoStore.branch(editor.graph, theirs)  # not through the editor: not ours
    foreign.stage(theirs, 1)
    foreign_gid = gid_for(theirs, editor.graph.name)
    foreign.prepare(foreign_gid)
    try:
        editor.release()
        assert foreign_gid in editor.graph.participant().prepared_gids()
    finally:
        foreign.rollback()

    # The same teardown does roll back a branch the test opened and left prepared.
    mine = new_id()
    graph_branch, chapter_branch = editor.write_chapter(
        mine, faker.sentence(), faker.text(), ["Ada"]
    )
    chapter_branch.rollback()  # type: ignore[attr-defined]
    graph_branch.stage(mine, 1)  # type: ignore[attr-defined]
    graph_branch.prepare(gid_for(mine, editor.graph.name))  # type: ignore[attr-defined]
    assert editor.prepared() == [gid_for(mine, editor.graph.name)]
    editor.release()
    assert editor.prepared() == []
