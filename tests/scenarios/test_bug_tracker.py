"""Scenario: a bug tracker over PostgreSQL and an Iceberg lake, on either facade.

A bug is one entity in two stores: its workflow fields (title, status, assignee,
priority, reporter) in PostgreSQL, its lifecycle figures (reopens, time to close,
labels) in Iceberg, where the analytics run. The scenario runs end to end the way
an application would: migrate the schema, file bugs, triage them in one
transaction (all or nothing), count the open bugs per assignee natively in
PostgreSQL over the latest version of each bug, close and reopen a bug and read
it as of the moment before, scan the reopened bugs natively in the lake with a
row filter, withdraw a spam report, and read on one facade what the other wrote.

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
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.core.parts import Part
from rhizodat.migrations import Migration, Step


@entity(stores={"pg": ["name", "email"]}, name="tracker.Reporter")
class Reporter:
    name: str
    email: str


@entity(
    stores={
        "pg": ["title", "status", "assignee", "priority", "reporter"],
        "lake": ["reopens", "time_to_close_s", "labels"],
    },
    name="tracker.Bug",
    soft_delete=timedelta(days=30),
)
class Bug:
    title: str
    status: str  # open | closed
    assignee: str | None
    priority: int  # 1 (urgent) .. 4
    reporter: Ref["Reporter"]
    reopens: int
    time_to_close_s: int | None
    labels: list[str]


BUGS_PG = Part.of(Bug, "pg").table
BUGS_LAKE = Part.of(Bug, "lake").table

SCHEMA = [
    Migration.of(
        1,
        "components",
        [
            Step(
                "pg",
                "sql",
                "CREATE TABLE tracker_component (name text PRIMARY KEY, owner text NOT NULL)",
            ),
            Step(
                "pg",
                "sql",
                "INSERT INTO tracker_component VALUES ('api', 'ada'), ('ui', 'bob')",
            ),
        ],
        description="the components bugs are filed against",
    )
]

DEVS = ("ada", "bob")


@pytest.fixture
def journal_schema(pg_dsn: str) -> Iterator[str]:
    name = f"dat_journal_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


@pytest.fixture
def parts_namespace(iceberg_rest_dsn: str) -> Iterator[str]:
    """The lake outlives the test: a parts namespace of its own, purged afterwards."""
    name = f"dat_bugs_{uuid.uuid4().hex[:10]}"
    yield name
    cleaner = IcebergStore(iceberg_rest_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):
            for ident in cleaner.catalog.list_tables(name):
                cleaner.catalog.purge_table(ident)
            cleaner.catalog.drop_namespace(name)
    finally:
        cleaner.close()


def python_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, lake_dsn: str, namespace: str
) -> Rhizodat:
    from rhizodat.adapters.postgres import PostgresJournal, PostgresStore

    return Rhizodat(
        stores={
            "pg": PostgresStore(pg_dsn, name="pg", schema=pg_schema),
            "lake": IcebergStore(lake_dsn, name="lake", parts_namespace=namespace),
        },
        journal=PostgresJournal(pg_dsn, schema=journal_schema),
    )


def native_facade(
    pg_dsn: str, pg_schema: str, journal_schema: str, lake_dsn: str, namespace: str
) -> Rhizodat:
    native = pytest.importorskip("rhizodat_native")
    return Rhizodat(
        stores={
            "pg": native.PostgresStore(pg_dsn, "pg", pg_schema),
            "lake": native.IcebergStore(lake_dsn, "lake", parts_namespace=namespace),
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
    iceberg_rest_dsn: str,
    parts_namespace: str,
) -> Iterator[Rhizodat]:
    d = FACADES[request.param](pg_dsn, pg_schema, journal_schema, iceberg_rest_dsn, parts_namespace)
    d.migrate(SCHEMA)
    yield d
    d.close()


def file_bugs(dat: Rhizodat, faker: Faker, count: int = 6) -> list[Bug]:
    reporter = Reporter(name=faker.name(), email=faker.email())
    dat.repository(Reporter).add(reporter)
    bugs = dat.repository(Bug)
    filed = []
    for i in range(count):
        bug = Bug(
            title=faker.sentence(),
            status="open",
            assignee=None,
            priority=1 + i % 4,
            reporter=Ref.to(reporter),
            reopens=0,
            time_to_close_s=None,
            labels=["ui" if i % 2 else "api"],
        )
        bugs.add(bug)
        filed.append(bug)
    return filed


def triage(dat: Rhizodat, filed: list[Bug], *, fail_after: int | None = None) -> None:
    """One transaction: the open bugs go to the devs round-robin; a failure undoes all."""

    @dat.transactional
    def assign(uow: UnitOfWork) -> None:
        repo = uow.repository(Bug)
        for i, bug in enumerate(filed):
            if fail_after is not None and i == fail_after:
                raise RuntimeError("triage interrupted")
            bug.assignee = DEVS[i % 2]
            repo.update(bug)

    assign()


def open_per_assignee(dat: Rhizodat) -> list[tuple[str, int]]:
    """Native SQL over the latest version of each bug (PostgreSQL keeps every version)."""
    rows = (
        dat.store("pg")
        .execute(
            "SELECT assignee, count(*) AS n FROM ("
            f"  SELECT DISTINCT ON (dat_id) assignee, status, dat_deleted FROM {BUGS_PG}"
            "  ORDER BY dat_id, dat_tx_time DESC) latest"
            " WHERE status = 'open' AND NOT dat_deleted AND assignee IS NOT NULL"
            " GROUP BY assignee ORDER BY assignee"
        )
        .all()
    )
    return [(str(r["assignee"]), int(r["n"])) for r in rows]


def test_the_schema_is_migrated_once_and_the_history_says_so(dat: Rhizodat) -> None:
    assert [m.id for m in dat.schema_versions()] == [1]
    report = dat.migrate(SCHEMA)
    assert (report.applied, report.already) == ((), (1,))


def test_bugs_are_filed_across_both_stores_and_read_back(dat: Rhizodat, faker: Faker) -> None:
    filed = file_bugs(dat, faker)
    bugs = dat.repository(Bug)
    for bug in filed:
        read = bugs.get(bug.dat_id)
        assert read == bug
        assert read is not None
        assert dat.repository(Reporter).get(read.reporter.dat_id) is not None


def test_triage_assigns_every_open_bug_in_one_transaction_or_none(
    dat: Rhizodat, faker: Faker
) -> None:
    filed = file_bugs(dat, faker)
    with pytest.raises(RuntimeError, match="interrupted"):
        triage(dat, filed, fail_after=4)
    assert open_per_assignee(dat) == []  # the four assignments before the failure never landed

    triage(dat, filed)
    assert open_per_assignee(dat) == [("ada", 3), ("bob", 3)]


def test_a_closed_bug_is_reopened_and_the_lake_knows_the_reopened_ones(
    dat: Rhizodat, parts_namespace: str, faker: Faker
) -> None:
    filed = file_bugs(dat, faker)
    triage(dat, filed)
    bugs = dat.repository(Bug)
    bug = filed[2]
    filed_at = datetime.now(UTC)
    time.sleep(0.01)

    bug.status, bug.time_to_close_s = "closed", 3600
    bugs.update(bug)
    closed_at = datetime.now(UTC)
    time.sleep(0.01)
    bug.status, bug.reopens, bug.labels = "open", 1, [*bug.labels, "regression"]
    bugs.update(bug)

    now = bugs.get(bug.dat_id)
    assert now is not None
    assert (now.status, now.reopens, now.labels) == ("open", 1, ["api", "regression"])
    when_closed = bugs.get(bug.dat_id, as_of=closed_at)
    assert when_closed is not None
    assert (when_closed.status, when_closed.time_to_close_s) == ("closed", 3600)
    when_filed = bugs.get(bug.dat_id, as_of=filed_at)
    assert when_filed is not None
    assert (when_filed.status, when_filed.assignee) == ("open", "ada")
    assert [v.status for v in bugs.history(bug.dat_id)] == ["open", "open", "closed", "open"]
    assert open_per_assignee(dat) == [("ada", 3), ("bob", 3)]

    reopened = dat.store("lake").execute(
        f"{parts_namespace}.{BUGS_LAKE}",
        {"row_filter": "reopens > 0", "selected_fields": ["dat_id", "reopens"]},
    )
    assert {str(r["dat_id"]) for r in reopened.all()} == {str(bug.dat_id)}


def test_a_spam_report_is_withdrawn_and_leaves_the_counts(dat: Rhizodat, faker: Faker) -> None:
    filed = file_bugs(dat, faker)
    triage(dat, filed)
    bugs = dat.repository(Bug)
    spam = filed[1]  # bob's

    bugs.delete(spam.dat_id)

    assert bugs.get(spam.dat_id) is None
    assert open_per_assignee(dat) == [("ada", 3), ("bob", 2)]
    withdrawn = bugs.get(spam.dat_id, include_deleted=True)
    assert withdrawn is not None
    assert withdrawn.dat_deleted_at is not None


def test_both_facades_work_the_same_tracker(
    pg_dsn: str,
    pg_schema: str,
    journal_schema: str,
    iceberg_rest_dsn: str,
    parts_namespace: str,
    faker: Faker,
) -> None:
    """Bugs filed and triaged on the native facade are closed on the Python one, and back."""
    args = (pg_dsn, pg_schema, journal_schema, iceberg_rest_dsn, parts_namespace)
    with native_facade(*args) as native_side:
        native_side.migrate(SCHEMA)
        filed = file_bugs(native_side, faker, count=4)
        triage(native_side, filed)
    with python_facade(*args) as python_side:
        assert [m.id for m in python_side.schema_versions()] == [1]
        assert open_per_assignee(python_side) == [("ada", 2), ("bob", 2)]
        bug = python_side.repository(Bug).get(filed[0].dat_id)
        assert bug == filed[0]
        assert bug is not None
        bug.status, bug.time_to_close_s = "closed", 60
        python_side.repository(Bug).update(bug)
    with native_facade(*args) as native_side:
        assert open_per_assignee(native_side) == [("ada", 1), ("bob", 2)]
        assert [v.status for v in native_side.repository(Bug).history(bug.dat_id)] == [
            "open",
            "open",
            "closed",
        ]
