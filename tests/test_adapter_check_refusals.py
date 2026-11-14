"""``rhizodat adapter check`` names what an incomplete or wrong adapter gets
wrong, one check at a time, and reports the optional capabilities it finds."""

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from rhizodat.check import Report, check_adapter
from rhizodat.core import adapter as registry
from rhizodat.core.adapter import Adapter, Scheme
from rhizodat.core.errors import ErrorKind
from rhizodat.core.journal import MemoryJournal
from rhizodat.testing.contract import MemoryStore


@pytest.fixture(autouse=True)
def isolated_registry() -> Iterator[None]:
    saved = registry._snapshot()
    yield
    registry._restore(saved)


def open_memory(name: str, dsn: str) -> Any:
    return MemoryStore(name)


def kind_of(faker: Faker) -> str:
    return f"mem_{faker.pystr(min_chars=6, max_chars=6).lower()}"


def run(faker: Faker, store: type) -> dict[str, Any]:
    kind = kind_of(faker)
    adapter = Adapter(
        kind=kind,
        schemes=(Scheme(kind.replace("_", "-"), host="none"),),
        open_store=lambda name, dsn: store(name),
    )
    report = check_adapter(adapter, dsn=f"{kind.replace('_', '-')}://")
    return {c.name: c for c in report.checks}


def test_a_journal_only_adapter_has_no_store_to_check(faker: Faker) -> None:
    kind = kind_of(faker)
    scheme = kind.replace("_", "-")
    adapter = Adapter(
        kind=kind,
        schemes=(Scheme(scheme, host="none"),),
        open_journal=lambda dsn, wall_ms: MemoryJournal(),
    )
    report = check_adapter(adapter, dsn=f"{scheme}://")
    checks = {c.name: c for c in report.checks}
    assert checks["store opens"].passed is None
    assert "journal-only" in checks["store opens"].detail
    assert checks["store interface"].passed is None
    assert report.ok


def test_something_that_is_not_an_adapter_fails_the_declaration() -> None:
    report = check_adapter(object())  # type: ignore[arg-type]
    check = {c.name: c for c in report.checks}["declaration: kind and DSN schemes are valid"]
    assert not check.passed
    assert "not a rhizodat Adapter" in check.detail


def test_a_scheme_registered_to_another_adapter_is_refused(faker: Faker) -> None:
    kind = kind_of(faker)
    scheme = Scheme(kind.replace("_", "-"), host="none")
    registry.register_adapter(Adapter(kind=kind, schemes=(scheme,), open_store=open_memory))
    other = Adapter(kind=kind_of(faker), schemes=(scheme,), open_store=open_memory)
    report = check_adapter(other, dsn=f"{scheme.name}://")
    check = {c.name: c for c in report.checks}["DSN parses with this adapter"]
    assert not check.passed
    assert "registered to another" in check.detail


class Nameless(MemoryStore):
    def __init__(self, name: str = "memory") -> None:
        super().__init__(name)
        self.name = 42  # type: ignore[assignment]
        self.two_phase = "yes"  # type: ignore[assignment]


def test_a_store_without_a_name_or_a_phase_flag_fails_the_interface(faker: Faker) -> None:
    check = run(faker, Nameless)["store interface"]
    assert not check.passed
    assert "store.name (str)" in check.detail
    assert "store.two_phase (bool)" in check.detail


class HalfBranch(MemoryStore):
    def branch(self, dat_txn: uuid.UUID) -> Any:
        class Branch:
            def rollback(self) -> None:
                pass

        return Branch()


def test_a_branch_without_its_methods_is_named(faker: Faker) -> None:
    check = run(faker, HalfBranch)["store.branch opens and rolls back"]
    assert not check.passed
    assert "branch is missing" in check.detail
    assert "stage()" in check.detail


class OddParticipant(MemoryStore):
    def participant(self) -> Any:
        inner = super().participant()
        inner.prepared_gids = lambda: ("not", "a", "list")  # type: ignore[method-assign,assignment,return-value]
        return inner


class HalfParticipant(MemoryStore):
    def participant(self) -> Any:
        class Participant:
            def prepared_gids(self) -> list[str]:
                return []

        return Participant()


def test_a_participant_is_checked_for_its_methods_and_its_gids(faker: Faker) -> None:
    odd = run(faker, OddParticipant)["participant.prepared_gids"]
    assert not odd.passed
    assert "must return a list of str" in odd.detail
    half = run(faker, HalfParticipant)["participant.prepared_gids"]
    assert not half.passed
    assert "participant is missing" in half.detail


class FullIsTransient(MemoryStore):
    def branch(self, dat_txn: uuid.UUID) -> Any:
        inner = super().branch(dat_txn)
        inner.classify = lambda error: ErrorKind.TRANSIENT  # type: ignore[attr-defined]
        return inner


def test_a_full_disk_must_be_store_full(faker: Faker) -> None:
    check = run(faker, FullIsTransient)["errors: device out of space is STORE_FULL"]
    assert not check.passed
    assert "ENOSPC" in check.detail


class CannotTell(MemoryStore):
    def free_space(self) -> int | None:
        return None


class Negative(MemoryStore):
    def free_space(self) -> int | None:
        return -1


def test_free_space_is_bytes_or_none(faker: Faker) -> None:
    unknown = run(faker, CannotTell)["Capacity (free space)"]
    assert unknown.passed
    assert "cannot tell" in unknown.detail
    negative = run(faker, Negative)["Capacity (free space)"]
    assert not negative.passed
    assert "int >= 0" in negative.detail


class NoKinds(MemoryStore):
    migration_kinds: frozenset[str] = frozenset()

    def run_migration_step(self, kind: str, body: object) -> None:
        pass


class SqlSteps(NoKinds):
    migration_kinds = frozenset({"sql"})


def test_migration_kinds_must_name_step_kinds(faker: Faker) -> None:
    empty = run(faker, NoKinds)["Migrates (schema migration steps)"]
    assert not empty.passed
    assert "must name step kinds" in empty.detail
    sql = run(faker, SqlSteps)["Migrates (schema migration steps)"]
    assert sql.passed
    assert sql.detail == "runs sql steps"


def test_a_report_without_a_dsn_says_so(faker: Faker) -> None:
    kind = kind_of(faker)
    report: Report = check_adapter(
        Adapter(
            kind=kind,
            schemes=(Scheme(kind.replace("_", "-"), host="none"),),
            open_store=open_memory,
        )
    )
    assert report.dsn is None
    assert "(" not in report.to_text().splitlines()[0]
