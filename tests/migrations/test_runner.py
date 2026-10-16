"""Running migrations: steps in order, each store in its own language, the history in
the journal with its transaction time.

A CBT journal app ships schema changes with each release. The runner applies
what the journal has not seen, refuses a history that no longer matches the
files, and stops at a failing step so the next run picks up from there (steps
are idempotent: a migration across stores is not atomic).
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.core.adapter import Migrates
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.journal import (
    AppliedMigration,
    JournalConflict,
    MemoryJournal,
    MigrationHistory,
)
from rhizodat.migrations import Migration, MigrationError, MigrationFailed, Step
from rhizodat.testing.memory import MemoryEntityStore


class SchemaStore(MemoryEntityStore):
    """A store whose migration language is ``ddl``: each statement is recorded."""

    migration_kinds = frozenset({"ddl"})

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.ran: list[str] = []
        self.fail_on: str | None = None

    def run_migration_step(self, kind: str, body: Any) -> None:
        assert kind == "ddl"
        for statement in [body] if isinstance(body, str) else body:
            if statement == self.fail_on:
                raise RuntimeError(f"cannot run {statement}")
            self.ran.append(statement)


class Wall:
    def __init__(self) -> None:
        self.ms = int(datetime(2026, 10, 15, 9, 0, tzinfo=UTC).timestamp() * 1000)

    def __call__(self) -> int:
        return self.ms

    def at(self) -> datetime:
        return datetime.fromtimestamp(self.ms / 1000, UTC)


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def stores() -> dict[str, SchemaStore]:
    return {"pg": SchemaStore("pg"), "graph": SchemaStore("graph")}


@pytest.fixture
def dat(stores: dict[str, SchemaStore], wall: Wall) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores=dict(stores),
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    yield d
    d.close()


def release(faker: Faker, n: int, store: str = "pg") -> Migration:
    return Migration.of(n, f"release_{n}", [Step(store, "ddl", f"{faker.word()}_{n}")])


def test_the_test_store_and_journal_have_the_capabilities(stores: dict[str, SchemaStore]) -> None:
    assert isinstance(stores["pg"], Migrates)
    assert isinstance(MemoryJournal(), MigrationHistory)
    assert not isinstance(MemoryEntityStore(), Migrates)


def test_pending_migrations_run_in_order_and_are_recorded(
    dat: Rhizodat, stores: dict[str, SchemaStore], faker: Faker
) -> None:
    first = Migration.of(1, "entries", [Step("pg", "ddl", ["a", "b"]), Step("graph", "ddl", "c")])
    second = release(faker, 2, "graph")
    report = dat.migrate([second, first])

    assert report.applied == (1, 2)
    assert report.already == ()
    assert stores["pg"].ran == ["a", "b"]
    assert stores["graph"].ran == ["c", second.steps[0].body]
    history = dat.schema_versions()
    assert [(h.id, h.name, h.checksum) for h in history] == [
        (1, "entries", first.checksum),
        (2, "release_2", second.checksum),
    ]


def test_a_second_run_applies_only_what_is_new(
    dat: Rhizodat, stores: dict[str, SchemaStore], faker: Faker
) -> None:
    one, two = release(faker, 1), release(faker, 2)
    dat.migrate([one])
    report = dat.migrate([one, two])
    assert (report.applied, report.already) == ((2,), (1,))
    assert stores["pg"].ran == [one.steps[0].body, two.steps[0].body]
    assert dat.migrate([one, two]).applied == ()


def test_a_dry_run_says_what_would_run_and_runs_nothing(
    dat: Rhizodat, stores: dict[str, SchemaStore], faker: Faker
) -> None:
    one = release(faker, 1)
    dat.migrate([one])
    report = dat.migrate([one, release(faker, 2)], dry_run=True)
    assert (report.applied, report.pending, report.already) == ((), (2,), (1,))
    assert len(stores["pg"].ran) == 1
    assert [h.id for h in dat.schema_versions()] == [1]


def test_to_stops_at_a_version(dat: Rhizodat, faker: Faker) -> None:
    report = dat.migrate([release(faker, n) for n in (1, 2, 3)], to=2)
    assert (report.applied, report.pending) == ((1, 2), (3,))


def test_a_failing_step_stops_the_run_and_the_next_run_resumes(
    dat: Rhizodat, stores: dict[str, SchemaStore]
) -> None:
    ms = [
        Migration.of(1, "ok", [Step("pg", "ddl", "x")]),
        Migration.of(2, "broken", [Step("graph", "ddl", "y"), Step("pg", "ddl", ["z", "w"])]),
        Migration.of(3, "later", [Step("pg", "ddl", "v")]),
    ]
    stores["pg"].fail_on = "w"
    with pytest.raises(MigrationFailed, match=r"migration 2 \(broken\), step 2 on store 'pg'") as e:
        dat.migrate(ms)
    assert (e.value.migration, e.value.step, e.value.store) == (2, 2, "pg")
    assert isinstance(e.value.__cause__, RuntimeError)
    assert [h.id for h in dat.schema_versions()] == [1]

    stores["pg"].fail_on = None
    assert dat.migrate(ms).applied == (2, 3)
    # The whole of migration 2 ran again: its steps are idempotent.
    assert stores["graph"].ran == ["y", "y"]
    assert stores["pg"].ran == ["x", "z", "z", "w", "v"]


def test_schema_versions_as_of_a_moment(dat: Rhizodat, wall: Wall, faker: Faker) -> None:
    one = release(faker, 1)
    dat.migrate([one])
    before_second = wall.at()
    wall.ms += 60_000
    dat.migrate([one, release(faker, 2)])

    assert [h.id for h in dat.schema_versions(as_of=before_second)] == [1]
    assert [h.id for h in dat.schema_versions(as_of=wall.at())] == [1, 2]
    assert dat.schema_versions(as_of=before_second - timedelta(days=1)) == []
    [first, _] = dat.schema_versions()
    assert first.applied_at == before_second
    with pytest.raises(ValueError, match="time zone"):
        dat.schema_versions(as_of=datetime(2026, 1, 1))


# --- what is refused before anything runs -------------------------------------------------


def test_an_edited_applied_migration_is_refused(dat: Rhizodat, faker: Faker) -> None:
    dat.migrate([Migration.of(1, "a", [Step("pg", "ddl", "x")])])
    with pytest.raises(MigrationError, match=r"migration 1 .* changed since it was applied"):
        dat.migrate([Migration.of(1, "a", [Step("pg", "ddl", "x2")]), release(faker, 2)])
    assert [h.id for h in dat.schema_versions()] == [1]


def test_an_applied_migration_missing_from_the_source_is_refused(
    dat: Rhizodat, faker: Faker
) -> None:
    dat.migrate([release(faker, 1), release(faker, 2)])
    with pytest.raises(MigrationError, match=r"migration 1 .* is missing"):
        dat.migrate([release(faker, 2)])


def test_a_new_migration_older_than_an_applied_one_is_refused(
    dat: Rhizodat, stores: dict[str, SchemaStore], faker: Faker
) -> None:
    three = release(faker, 3)
    dat.migrate([three])
    with pytest.raises(MigrationError, match=r"migration 2 .* older than applied 3"):
        dat.migrate([release(faker, 2), three])
    assert len(stores["pg"].ran) == 1


@pytest.mark.parametrize(
    ("step", "message"),
    [
        (Step("nowhere", "ddl", "x"), "no store named 'nowhere'"),
        (Step("pg", "sql", "x"), "store 'pg' runs ddl steps, not sql"),
    ],
)
def test_steps_a_store_cannot_run_are_refused_up_front(
    dat: Rhizodat, stores: dict[str, SchemaStore], faker: Faker, step: Step, message: str
) -> None:
    with pytest.raises(MigrationError, match=message):
        dat.migrate([release(faker, 1), Migration.of(2, "bad", [step])])
    assert stores["pg"].ran == []


def test_a_store_without_migrations_is_named(wall: Wall) -> None:
    dat = Rhizodat(stores={"mem": MemoryEntityStore("mem")}, journal=MemoryJournal())
    try:
        with pytest.raises(MigrationError, match=r"store 'mem' .* runs no migration steps"):
            dat.migrate([Migration.of(1, "a", [Step("mem", "ddl", "x")])])
    finally:
        dat.close()


def test_migrations_need_a_journal_with_a_history(stores: dict[str, SchemaStore]) -> None:
    dat = Rhizodat(stores={"pg": stores["pg"]})
    try:
        with pytest.raises(ValueError, match="journal"):
            dat.migrate([Migration.of(1, "a", [Step("pg", "ddl", "x")])])
    finally:
        dat.close()


def test_a_migration_is_recorded_once(dat: Rhizodat, faker: Faker) -> None:
    one = release(faker, 1)
    assert isinstance(dat.journal, MigrationHistory)
    dat.migrate([one], dry_run=True)
    dat.journal.record_migration(AppliedMigration(1, one.name, one.checksum, 1))
    with pytest.raises(JournalConflict, match="already recorded"):
        dat.journal.record_migration(AppliedMigration(1, one.name, one.checksum, 2))
    # The runner that loses the race says so instead of recording twice.
    assert dat.migrate([one]).applied == ()


def test_a_runner_that_loses_the_race_to_record_reports_nothing_applied(
    stores: dict[str, SchemaStore], wall: Wall, faker: Faker
) -> None:
    class Raced(MemoryJournal):
        """Another runner records every migration just after this one read the history."""

        def record_migration(self, applied: AppliedMigration) -> None:
            super().record_migration(applied)  # the other runner
            super().record_migration(applied)  # this one: JournalConflict

    dat = Rhizodat(stores={"pg": stores["pg"]}, journal=Raced(wall_ms=wall))
    try:
        one = release(faker, 1)
        report = dat.migrate([one])
        assert (report.applied, report.pending) == ((), ())
        assert [h.id for h in dat.schema_versions()] == [1]
    finally:
        dat.close()
