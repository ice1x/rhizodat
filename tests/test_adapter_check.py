"""``rhizodat adapter check``: a dry run of an adapter that reports what it implements."""

import json
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from rhizodat.check import Report, check_adapter, load_adapter, main
from rhizodat.core import adapter as registry
from rhizodat.core.adapter import Adapter, Scheme
from rhizodat.testing.contract import MemoryStore


@pytest.fixture(autouse=True)
def isolated_registry() -> Iterator[None]:
    saved = registry._snapshot()
    yield
    registry._restore(saved)


def memory_adapter(faker: Faker, store: type = MemoryStore) -> Adapter:
    kind = f"mem_{faker.pystr(min_chars=6, max_chars=6).lower()}"
    return Adapter(
        kind=kind,
        schemes=(Scheme(kind.replace("_", "-"), host="none"),),
        open_store=lambda name, dsn: store(name),
    )


def by_name(report: Report) -> dict[str, Any]:
    return {c.name: c for c in report.checks}


def test_a_complete_two_phase_store_passes_every_required_check(faker: Faker) -> None:
    adapter = memory_adapter(faker)
    report = check_adapter(adapter, dsn=f"{adapter.schemes[0].name}://")

    assert report.ok
    failed = [c.name for c in report.checks if c.level == "required" and not c.passed]
    assert failed == []
    checks = by_name(report)
    assert checks["store.branch opens and rolls back"].passed
    assert checks["participant.prepared_gids"].passed
    assert checks["errors: device out of space is STORE_FULL"].passed
    # The reference memory store has no optional capability.
    assert not checks["PartStore (entity parts)"].passed
    assert not checks["Capacity (free space)"].passed


def test_the_summary_counts_and_scores(faker: Faker) -> None:
    adapter = memory_adapter(faker)
    report = check_adapter(adapter, dsn=f"{adapter.schemes[0].name}://")
    summary = report.summary()
    required = [c for c in report.checks if c.level == "required"]
    optional = [c for c in report.checks if c.level == "optional"]
    assert summary["required"] == {"passed": len(required), "total": len(required)}
    assert summary["optional"]["total"] == len(optional)
    assert summary["optional"]["passed"] == sum(1 for c in optional if c.passed)
    assert summary["not_run"] == 0
    run = len(required) + len(optional)
    expected = round(100 * (len(required) + summary["optional"]["passed"]) / run)
    assert summary["completeness"] == expected


def test_without_a_dsn_only_the_declaration_is_checked(faker: Faker) -> None:
    report = check_adapter(memory_adapter(faker))
    checks = by_name(report)
    assert checks["declaration: kind and DSN schemes are valid"].passed
    assert checks["store opens"].passed is None  # not run
    assert report.summary()["not_run"] > 0
    assert report.ok  # nothing that ran failed


class NoParticipant(MemoryStore):
    participant = None  # type: ignore[assignment]


class RetriesEverything(MemoryStore):
    def branch(self, dat_txn: uuid.UUID) -> Any:
        inner = super().branch(dat_txn)
        inner.is_retryable = lambda error: True  # type: ignore[method-assign]
        return inner


def test_a_missing_participant_fails_and_says_what_is_missing(faker: Faker) -> None:
    adapter = memory_adapter(faker, NoParticipant)
    report = check_adapter(adapter, dsn=f"{adapter.schemes[0].name}://")
    assert not report.ok
    check = by_name(report)["store interface"]
    assert not check.passed
    assert "participant" in check.detail


def test_calling_every_error_retryable_fails(faker: Faker) -> None:
    adapter = memory_adapter(faker, RetriesEverything)
    report = check_adapter(adapter, dsn=f"{adapter.schemes[0].name}://")
    assert not report.ok
    assert not by_name(report)["errors: plain errors are not retryable"].passed


def test_an_invalid_declaration_fails_without_a_dsn() -> None:
    adapter = Adapter(kind="Bad Kind", schemes=())
    report = check_adapter(adapter)
    assert not report.ok
    check = by_name(report)["declaration: kind and DSN schemes are valid"]
    assert not check.passed
    assert "kind" in check.detail
    assert not by_name(report)["declaration: opens a store or a journal"].passed


def test_a_store_that_fails_to_open_is_reported(faker: Faker) -> None:
    def broken(name: str, dsn: str) -> MemoryStore:
        raise ConnectionError("connection refused")

    kind = f"mem_{faker.pystr(min_chars=6, max_chars=6).lower()}"
    adapter = Adapter(kind=kind, schemes=(Scheme(kind.replace("_", "-")),), open_store=broken)
    report = check_adapter(adapter, dsn=f"{kind.replace('_', '-')}://host")
    check = by_name(report)["store opens"]
    assert not check.passed
    assert "connection refused" in check.detail
    assert by_name(report)["store interface"].passed is None


def test_a_dsn_of_another_store_kind_is_refused(faker: Faker) -> None:
    report = check_adapter(memory_adapter(faker), dsn="postgresql://localhost/db")
    check = by_name(report)["DSN parses with this adapter"]
    assert not check.passed
    assert "postgresql" in check.detail


def test_adapters_load_by_kind_or_module_path() -> None:
    from rhizodat.adapters.postgres import ADAPTER

    assert load_adapter("postgres") is ADAPTER
    assert load_adapter("rhizodat.adapters.postgres:ADAPTER") is ADAPTER
    with pytest.raises(ValueError, match="not a rhizodat Adapter"):
        load_adapter("rhizodat.adapters.postgres:Isolation")
    with pytest.raises(ValueError, match="no adapter"):
        load_adapter("nosuchkind")


def test_postgres_reports_parts_and_classification(pg_dsn: str) -> None:
    report = check_adapter(load_adapter("postgres"), dsn=pg_dsn)
    checks = by_name(report)
    assert report.ok, [c for c in report.checks if c.level == "required" and not c.passed]
    for name in (
        "PartStore (entity parts)",
        "PartBranch (entity parts in a Unit of Work)",
        "Classifies on the branch",
        "Classifies on the participant",
        "ReadOnlyQueries",
        "AppliedLocation",
        "Migrates (schema migration steps)",
        "Loads (file imports)",
    ):
        assert checks[name].passed, name
    assert checks["Migrates (schema migration steps)"].detail == "runs sql steps"
    assert not checks["Capacity (free space)"].passed


def test_clickhouse_reports_its_free_space(clickhouse_dsn: str) -> None:
    report = check_adapter(load_adapter("clickhouse"), dsn=clickhouse_dsn)
    check = by_name(report)["Capacity (free space)"]
    assert check.passed
    assert "bytes" in check.detail


def test_the_cli_prints_json_and_exits_by_the_result(
    pg_dsn: str, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        ["adapter", "check", "rhizodat.adapters.postgres:ADAPTER", "--dsn", pg_dsn, "--json"]
    )
    out = json.loads(capsys.readouterr().out)
    assert code == 0
    assert out["ok"] is True
    assert out["adapter"] == "postgres"
    assert {"name", "level", "passed", "detail"} <= set(out["checks"][0])
    assert out["summary"]["required"]["passed"] == out["summary"]["required"]["total"]


def test_the_cli_redacts_the_password(capsys: pytest.CaptureFixture[str]) -> None:
    main(["adapter", "check", "postgres", "--dsn", "postgresql://u:hunter2@127.0.0.1:9/db"])
    out = capsys.readouterr().out
    assert "hunter2" not in out
    assert "store opens" in out


def test_secret_query_parameters_are_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    main(
        ["adapter", "check", "iceberg", "--dsn", "iceberg+rest://h:1/w?s3.secret-access-key=xyzzy"]
    )
    assert "xyzzy" not in capsys.readouterr().out


def test_the_cli_text_report_and_a_failing_exit_code(
    faker: Faker, capsys: pytest.CaptureFixture[str]
) -> None:
    adapter = memory_adapter(faker, NoParticipant)
    registry.register_adapter(adapter)
    code = main(["adapter", "check", adapter.kind, "--dsn", f"{adapter.schemes[0].name}://"])
    out = capsys.readouterr().out
    assert code == 1
    assert "required" in out
    assert "completeness" in out
    assert "AdapterContract" in out  # where to check behaviour next


def test_an_unknown_adapter_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["adapter", "check", "no.such.module:ADAPTER"]) == 2
    assert "no.such.module" in capsys.readouterr().err
