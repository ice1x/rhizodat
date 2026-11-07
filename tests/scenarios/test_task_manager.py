"""Scenario: an IT task manager over PostgreSQL and the drevo graph, on either facade.

A task keeps its workflow fields (title, status, assignee, sprint) in PostgreSQL
and its dependencies in the drevo graph, where "blocked by" is an edge to the
blocking task. The scenario runs end to end the way an application would:
migrate the schema (a SQL step and a Cypher step), file tasks with dependencies,
plan a sprint in one transaction (an interrupted plan changes nothing), ask the
graph what a task blocks and confirm against the latest versions, finish a task
and release its dependents, follow a task's status through its history and as of
an earlier moment, archive a task and bring it back, and work the same board from
both facades in turn.

The same scenario runs on the Python facade and on the native facade (the core
in Rust under the same API), so both must agree.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,call-arg"

import contextlib
import time
import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from faker import Faker

from rhizodat import Ref, Rhizodat, UnitOfWork, entity
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(
    stores={"work": ["title", "status", "assignee", "sprint"], "graph": ["blocked_by"]},
    name="it.Task",
    soft_delete=timedelta(days=7),
)
class Task:
    title: str
    status: str  # todo | doing | done
    assignee: str | None
    sprint: int | None
    blocked_by: list[Ref["Task"]]


TASK_GRAPH = Part.of(Task, "graph").table
SEED_LABEL = "ItBoardSeed"

SCHEMA = [
    Migration.of(
        1,
        "team",
        [
            Step(
                "work", "sql", "CREATE TABLE it_team (member text PRIMARY KEY, role text NOT NULL)"
            ),
            Step("work", "sql", "INSERT INTO it_team VALUES ('ada', 'lead'), ('bob', 'dev')"),
            Step("graph", "cypher", f"CREATE (:{SEED_LABEL} {{board: 'kanban'}})"),
        ],
        description="the team and the board node",
    )
]


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture(autouse=True)
def _clean_graph(drevo_bolt_dsn: str) -> Iterator[None]:
    """The graph is shared: remove this run's versions, seed and bare anchors."""
    yield
    cleaner = DrevoStore(drevo_bolt_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            cleaner.execute(f"MATCH (v:{TASK_GRAPH}) DETACH DELETE v")
            cleaner.execute(f"MATCH (s:{SEED_LABEL}) DELETE s")
            cleaner.execute(f"MATCH (a:{ANCHOR}) WHERE NOT (a)<--() DELETE a")
    finally:
        cleaner.close()


def python_facade(pg_dsn: str, pg_schema: str, journal_schema: str, bolt: str) -> Rhizodat:
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    return Rhizodat(
        stores={
            "work": PostgresStore(pg_dsn, name="work", schema=pg_schema),
            "graph": DrevoStore(bolt, name="graph"),
        },
        journal=PostgresJournal(pg_dsn, schema=journal_schema),
    )


def native_facade(pg_dsn: str, pg_schema: str, journal_schema: str, bolt: str) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "work": native.PostgresStore(pg_dsn, "work", pg_schema),
            "graph": native.DrevoStore(bolt, "graph"),
        },
        journal=native.PostgresJournal(pg_dsn, journal_schema),
    )


FACADES: dict[str, Callable[..., Rhizodat]] = {"python": python_facade, "native": native_facade}


@pytest.fixture(params=list(FACADES))
def dat(
    request: pytest.FixtureRequest,
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    drevo_bolt_dsn: str,
) -> Iterator[Rhizodat]:
    d = FACADES[request.param](pg_dsn, pg_schema, journal_schema, drevo_bolt_dsn)
    d.migrate(SCHEMA)
    yield d
    d.close()


def file_tasks(dat: Rhizodat, faker: Faker) -> dict[str, Task]:
    """schema -> api -> ui, plus docs blocked by both api and ui, plus a free one."""
    tasks = dat.repository(Task)

    def task(blocked_by: list[Task]) -> Task:
        t = Task(
            title=faker.bs(),
            status="todo",
            assignee=None,
            sprint=None,
            blocked_by=[Ref.to(b) for b in blocked_by],
        )
        tasks.add(t)
        return t

    board: dict[str, Task] = {}
    board["schema"] = task([])
    board["api"] = task([board["schema"]])
    board["ui"] = task([board["api"]])
    board["docs"] = task([board["api"], board["ui"]])
    board["free"] = task([])
    return board


def plan_sprint(dat: Rhizodat, tasks: list[Task], *, fail_after: int | None = None) -> None:
    """One transaction: the tasks go into sprint 1, assigned round-robin."""

    @dat.transactional
    def plan(uow: UnitOfWork) -> None:
        repo = uow.repository(Task)
        for i, t in enumerate(tasks):
            if fail_after is not None and i == fail_after:
                raise RuntimeError("planning interrupted")
            t.sprint, t.assignee = 1, ("ada", "bob")[i % 2]
            repo.update(t)

    plan()


def blocked_by(dat: Rhizodat, blocker: Task) -> set[uuid.UUID]:
    """The graph names every task that was ever blocked by ``blocker`` (edges go from
    each version to the blocker's anchor); the latest version of each confirms."""
    rows = (
        dat.store("graph")
        .execute(
            f"MATCH (v:{TASK_GRAPH})-[:blocked_by]->(a:{ANCHOR} {{dat_id: $b}})"
            " RETURN DISTINCT v.dat_id AS d",
            {"b": str(blocker.dat_id)},
        )
        .all()
    )
    tasks = dat.repository(Task)
    confirmed = set()
    for row in rows:
        latest = tasks.get(uuid.UUID(str(row["d"])))
        if latest and any(r.dat_id == blocker.dat_id for r in latest.blocked_by):
            confirmed.add(latest.dat_id)
    return confirmed


def test_the_schema_is_migrated_once_including_the_graph_step(dat: Rhizodat) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(SCHEMA)
    assert (report.applied, report.already) == ((), (1,))
    seeds = dat.store("graph").execute(f"MATCH (s:{SEED_LABEL}) RETURN s.board AS b").all()
    assert [r["b"] for r in seeds] == ["kanban"]


def test_tasks_and_their_dependencies_are_read_back(dat: Rhizodat, faker: Faker) -> None:
    board = file_tasks(dat, faker)
    tasks = dat.repository(Task)
    docs = tasks.get(board["docs"].dat_id)
    assert docs == board["docs"]
    assert docs is not None
    assert [r.dat_id for r in docs.blocked_by] == [board["api"].dat_id, board["ui"].dat_id]
    free = tasks.get(board["free"].dat_id)
    assert free is not None
    assert free.blocked_by == []


def test_a_sprint_is_planned_in_one_transaction_or_not_at_all(dat: Rhizodat, faker: Faker) -> None:
    board = file_tasks(dat, faker)
    tasks = dat.repository(Task)
    chain = [board["schema"], board["api"], board["ui"]]

    with pytest.raises(RuntimeError, match="interrupted"):
        plan_sprint(dat, chain, fail_after=2)
    for t in chain:
        current = tasks.get(t.dat_id)
        assert current is not None
        assert (current.sprint, current.assignee) == (None, None)

    plan_sprint(dat, chain)
    planned = [tasks.get(t.dat_id) for t in chain]
    assert [(p.sprint, p.assignee) for p in planned if p] == [(1, "ada"), (1, "bob"), (1, "ada")]


def test_finishing_a_task_releases_what_it_blocked(dat: Rhizodat, faker: Faker) -> None:
    board = file_tasks(dat, faker)
    tasks = dat.repository(Task)
    api = board["api"]
    assert blocked_by(dat, api) == {board["ui"].dat_id, board["docs"].dat_id}

    api.status = "done"
    tasks.update(api)

    @dat.transactional
    def release(uow: UnitOfWork) -> None:
        repo = uow.repository(Task)
        for t in (board["ui"], board["docs"]):
            t.blocked_by = [r for r in t.blocked_by if r.dat_id != api.dat_id]
            repo.update(t)

    release()
    assert blocked_by(dat, api) == set()
    docs = tasks.get(board["docs"].dat_id)
    assert docs is not None
    assert [r.dat_id for r in docs.blocked_by] == [board["ui"].dat_id]
    assert blocked_by(dat, board["ui"]) == {board["docs"].dat_id}


def test_a_task_moves_through_the_board_and_an_archived_one_comes_back(
    dat: Rhizodat, faker: Faker
) -> None:
    board = file_tasks(dat, faker)
    tasks = dat.repository(Task)
    free = board["free"]
    filed_at = datetime.now(UTC)
    time.sleep(0.01)

    free.status, free.assignee = "doing", "bob"
    tasks.update(free)
    free.status = "done"
    tasks.update(free)
    assert [t.status for t in tasks.history(free.dat_id)] == ["todo", "doing", "done"]
    as_filed = tasks.get(free.dat_id, as_of=filed_at)
    assert as_filed is not None
    assert (as_filed.status, as_filed.assignee) == ("todo", None)

    tasks.delete(free.dat_id)
    assert tasks.get(free.dat_id) is None
    tasks.restore(free.dat_id)
    back = tasks.get(free.dat_id)
    assert back is not None
    assert (back.status, back.dat_deleted_at) == ("done", None)


def test_both_facades_work_the_same_board(
    pg_dsn: str, pg_schema: str, journal_schema: str, drevo_bolt_dsn: str, faker: Faker
) -> None:
    """Tasks filed on the Python facade are planned natively; the graph agrees on both."""
    args = (pg_dsn, pg_schema, journal_schema, drevo_bolt_dsn)
    with python_facade(*args) as python_side:
        python_side.migrate(SCHEMA)
        board = file_tasks(python_side, faker)
    with native_facade(*args) as native_side:
        assert [m.id for m in native_side.schema_versions()] == [1]
        plan_sprint(native_side, [board["schema"], board["api"]])
        assert blocked_by(native_side, board["api"]) == {board["ui"].dat_id, board["docs"].dat_id}
    with python_facade(*args) as python_side:
        api = python_side.repository(Task).get(board["api"].dat_id)
        assert api is not None
        assert (api.sprint, api.assignee) == (1, "bob")
        assert blocked_by(python_side, board["api"]) == {board["ui"].dat_id, board["docs"].dat_id}
        assert [t.sprint for t in python_side.repository(Task).history(api.dat_id)] == [None, 1]
