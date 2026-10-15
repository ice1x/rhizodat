"""Running migrations against a ``Datorium``: the history lives in the journal store.

Before any step runs, the source is checked against the history: an applied
migration must still be there unchanged (same checksum), and a new one must be
newer than every applied one. Every pending step must name a configured store
that runs its kind. Then each pending migration runs its steps in order and is
recorded with the HLC time of the record, so ``schema_versions(as_of=...)`` says
which schema applied at a moment.

A migration over several stores is not atomic: a failing step stops the run
with ``MigrationFailed`` and leaves the migration unrecorded, so the next run
runs all of it again. That is why steps must be idempotent. Run one runner at a
time; a second one that records a migration first is reported, not repeated.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from datorium.core.adapter import Migrates
from datorium.core.journal import AppliedMigration, JournalConflict, MigrationHistory
from datorium.migrations.format import Migration, by_id, load_migrations

if TYPE_CHECKING:
    from datorium.api import Datorium

__all__ = ["MigrationError", "MigrationFailed", "MigrationReport", "migrate", "schema_versions"]


class MigrationError(RuntimeError):
    """The migrations cannot run: the history does not match the source, or a step
    names a store that cannot run it. Nothing ran."""


class MigrationFailed(MigrationError):
    """A step failed (its error is the ``__cause__``); earlier migrations stay applied."""

    def __init__(self, migration: Migration, step: int, store: str, reason: str) -> None:
        super().__init__(
            f"migration {migration.id} ({migration.name}), step {step} on store {store!r}"
            f" failed: {reason}; it is not recorded, and the next run runs it again"
        )
        self.migration = migration.id
        self.step = step
        self.store = store


@dataclass(frozen=True, slots=True)
class MigrationReport:
    applied: tuple[int, ...]
    """Run and recorded by this call."""
    pending: tuple[int, ...]
    """Still to run: a dry run, or beyond ``to``."""
    already: tuple[int, ...]
    """Applied before this call."""


def _history(dat: "Datorium") -> MigrationHistory:
    journal = dat.journal
    if journal is None:
        raise ValueError(
            "migrations need the journal, which keeps their history: pass journal=<dsn>"
        )
    if not isinstance(journal, MigrationHistory):
        raise ValueError(f"the journal ({type(journal).__name__}) keeps no migration history")
    return journal


def _source(source: str | Path | Sequence[Migration]) -> list[Migration]:
    if isinstance(source, str | Path):
        return load_migrations(source)
    return by_id(source)


def _label(m: Migration) -> str:
    return f"migration {m.id} ({m.name})"


def _check_history(migrations: list[Migration], applied: list[AppliedMigration]) -> None:
    given = {m.id: m for m in migrations}
    for a in applied:
        m = given.get(a.id)
        if m is None:
            raise MigrationError(
                f"migration {a.id} ({a.name}) is applied, but is missing from the migrations given"
            )
        if m.checksum != a.checksum:
            raise MigrationError(
                f"{_label(m)} changed since it was applied (checksum {a.checksum[:12]}…,"
                f" now {m.checksum[:12]}…): add a new migration instead of editing it"
            )
    newest = max((a.id for a in applied), default=0)
    done = {a.id for a in applied}
    for m in migrations:
        if m.id not in done and m.id < newest:
            raise MigrationError(
                f"{_label(m)} is new but older than applied {newest}: give it a higher number"
            )


def _check_steps(dat: "Datorium", migrations: list[Migration]) -> None:
    for m in migrations:
        for i, step in enumerate(m.steps, 1):
            where = f"{_label(m)}, step {i}"
            try:
                store = dat.store(step.store)
            except KeyError as exc:
                raise MigrationError(f"{where}: {exc.args[0]}") from None
            if not isinstance(store, Migrates):
                raise MigrationError(
                    f"{where}: store {step.store!r} ({type(store).__name__}) runs no"
                    " migration steps"
                )
            if step.kind not in store.migration_kinds:
                kinds = ", ".join(sorted(store.migration_kinds))
                raise MigrationError(
                    f"{where}: store {step.store!r} runs {kinds} steps, not {step.kind}"
                )


def migrate(
    dat: "Datorium",
    source: str | Path | Sequence[Migration],
    *,
    to: int | None = None,
    dry_run: bool = False,
) -> MigrationReport:
    history = _history(dat)
    migrations = _source(source)
    applied = history.applied_migrations()
    _check_history(migrations, applied)
    done = {a.id for a in applied}
    pending = [m for m in migrations if m.id not in done]
    runnable = [m for m in pending if to is None or m.id <= to]
    _check_steps(dat, runnable)
    already = tuple(a.id for a in applied)
    if dry_run:
        return MigrationReport((), tuple(m.id for m in pending), already)

    ran: list[int] = []
    for m in runnable:
        for i, step in enumerate(m.steps, 1):
            store = dat.store(step.store)
            assert isinstance(store, Migrates)  # checked above
            try:
                store.run_migration_step(step.kind, step.body)
            except Exception as exc:
                raise MigrationFailed(m, i, step.store, str(exc)) from exc
        try:
            history.record_migration(
                AppliedMigration(m.id, m.name, m.checksum, dat.clock.now().encode())
            )
        except JournalConflict:
            # Another runner recorded it first; its steps are idempotent, so nothing is lost.
            continue
        ran.append(m.id)
    left = tuple(m.id for m in pending if m not in runnable)
    return MigrationReport(tuple(ran), left, already)


def schema_versions(dat: "Datorium", *, as_of: datetime | None = None) -> list[AppliedMigration]:
    from datorium.core.parts import hlc_bound

    applied = _history(dat).applied_migrations()
    if as_of is None:
        return applied
    bound = hlc_bound(as_of)
    return [a for a in applied if a.dat_tx_time <= bound]
