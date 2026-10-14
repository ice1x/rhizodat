"""Bug tracker and IT task manager on the Repository over real PostgreSQL stores.

Issues keep their core fields in one PostgreSQL store and their triage data in
another (two schemas, a PostgreSQL journal), so every write is a meta-transaction
over two stores. Comments are soft-deleted; tasks are erased on delete.
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined"

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from faker import Faker

from datorium import Datorium, Ref, UnitOfWork, entity
from datorium.adapters.postgres import PostgresJournal, PostgresStore
from datorium.core.clock import HybridLogicalClock
from datorium.core.repository import EntityNotFound, NotRestorable

DAY_MS = 86_400_000


@entity(
    stores={"tracker": ["title", "status", "reporter"], "triage": ["severity", "labels"]},
    name="tracker.Issue",
)
class Issue:
    title: str
    status: str
    reporter: str
    severity: int
    labels: list[str]


@entity(stores={"tracker": ["issue", "author", "body"]}, name="tracker.Comment", soft_delete=True)
class Comment:
    issue: Ref["Issue"]
    author: str
    body: str


@entity(stores={"tracker": ["summary", "assignee", "done"]}, name="it.Task")
class Task:
    summary: str
    assignee: str | None
    done: bool


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 14, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@pytest.fixture
def triage_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def dat(pg_dsn: str, pg_schema: str, triage_schema: str, wall: Wall) -> Iterator[Datorium]:
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    d = Datorium(
        stores={
            "tracker": PostgresStore(pg_dsn, name="tracker", schema=pg_schema),
            "triage": PostgresStore(pg_dsn, name="triage", schema=triage_schema),
        },
        journal=journal,
        clock=HybridLogicalClock(wall_ms=wall),
        soft_delete_window=timedelta(days=14),
    )
    yield d
    d.close()


def new_issue(faker: Faker) -> Issue:
    return Issue(
        title=faker.sentence(nb_words=6),
        status="open",
        reporter=faker.user_name(),
        severity=faker.random_int(1, 4),
        labels=faker.words(2),
    )


def test_an_issue_lives_across_two_stores_and_keeps_its_history(
    dat: Datorium, faker: Faker, wall: Wall
) -> None:
    issues = dat.repository(Issue)
    issue = new_issue(faker)
    reported_severity = issue.severity
    issues.add(issue)
    reported = wall.now()

    wall.ms += 60_000
    issue.status, issue.severity, issue.labels = "triaged", 1, [*issue.labels, "regression"]
    issues.update(issue)
    wall.ms += 60_000
    issue.status = "closed"
    issues.update(issue)

    current = issues.get(issue.dat_id)
    assert current == issue
    assert current is not None
    assert current.dat_created_at == reported
    assert current.dat_updated_at == wall.now()
    at_report = issues.get(issue.dat_id, as_of=reported)
    assert at_report is not None
    assert (at_report.status, at_report.severity) == ("open", reported_severity)
    assert [i.status for i in issues.history(issue.dat_id)] == ["open", "triaged", "closed"]
    # Each store holds its own part of every version, under the same dat_txn.
    tracker = (
        dat.store("tracker")
        .execute(
            "SELECT dat_txn FROM tracker_issue WHERE dat_id = %s ORDER BY dat_tx_time",
            (issue.dat_id,),
        )
        .all()
    )
    triage = (
        dat.store("triage")
        .execute(
            "SELECT dat_txn FROM tracker_issue WHERE dat_id = %s ORDER BY dat_tx_time",
            (issue.dat_id,),
        )
        .all()
    )
    assert [r["dat_txn"] for r in tracker] == [r["dat_txn"] for r in triage]
    assert len(tracker) == 3


def test_a_deleted_task_is_gone_from_every_version(dat: Datorium, faker: Faker) -> None:
    tasks = dat.repository(Task)
    task = Task(summary=faker.bs(), assignee=None, done=False)
    tasks.add(task)
    task.assignee = faker.user_name()
    tasks.update(task)

    tasks.delete(task.dat_id)

    assert tasks.get(task.dat_id) is None
    assert tasks.history(task.dat_id) == []
    rows = (
        dat.store("tracker")
        .execute("SELECT count(*) AS n FROM it_task WHERE dat_id = %s", (task.dat_id,))
        .one()
    )
    assert rows["n"] == 0
    with pytest.raises(NotRestorable):
        tasks.restore(task.dat_id)


def test_a_deleted_comment_can_be_restored_then_is_purged(
    dat: Datorium, faker: Faker, wall: Wall
) -> None:
    issue = new_issue(faker)
    dat.repository(Issue).add(issue)
    comments = dat.repository(Comment)
    spam, kept = (
        Comment(issue=Ref.to(issue), author=faker.user_name(), body=faker.paragraph())
        for _ in range(2)
    )
    comments.add(spam)
    comments.add(kept)

    comments.delete(spam.dat_id)
    comments.delete(kept.dat_id)
    wall.ms += DAY_MS
    restored = comments.restore(kept.dat_id).value
    assert restored == kept
    assert comments.get(kept.dat_id) == kept
    assert comments.get(spam.dat_id) is None
    hidden = comments.get(spam.dat_id, include_deleted=True)
    assert hidden is not None
    assert hidden.issue == Ref.to(issue)

    wall.ms += 13 * DAY_MS  # spam was deleted 14 days ago
    assert comments.purge_expired() == 0
    wall.ms += 1
    assert comments.purge_expired() == 1
    assert comments.get(spam.dat_id, include_deleted=True) is None
    assert comments.get(kept.dat_id) == kept


def test_closing_an_issue_and_its_task_is_one_unit_of_work(dat: Datorium, faker: Faker) -> None:
    issue = new_issue(faker)
    task = Task(summary=f"fix: {issue.title}", assignee=faker.user_name(), done=False)
    dat.repository(Issue).add(issue)
    dat.repository(Task).add(task)

    @dat.transactional
    def close(uow: UnitOfWork, issue_id: uuid.UUID, task_id: uuid.UUID) -> None:
        issues, tasks = uow.repository(Issue), uow.repository(Task)
        current = issues.get(issue_id)
        current.status = "closed"
        issues.update(current)
        done = tasks.get(task_id)
        done.done = True
        tasks.update(done)
        if current.severity == 0:
            raise AssertionError("unreachable")

    close(issue.dat_id, task.dat_id)
    closed, done = dat.repository(Issue).get(issue.dat_id), dat.repository(Task).get(task.dat_id)
    assert closed is not None
    assert done is not None
    assert (closed.status, done.done) == ("closed", True)

    @dat.transactional
    def reopen_and_fail(uow: UnitOfWork, issue_id: uuid.UUID) -> None:
        issues = uow.repository(Issue)
        current = issues.get(issue_id)
        current.status = "open"
        issues.update(current)
        raise EntityNotFound("the linked task vanished")

    with pytest.raises(EntityNotFound):
        reopen_and_fail(issue.dat_id)
    after = dat.repository(Issue).get(issue.dat_id)
    assert after is not None
    assert after.status == "closed"
