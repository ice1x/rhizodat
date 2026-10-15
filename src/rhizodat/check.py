"""``rhizodat adapter check``: a dry run of a store adapter, with a completeness report.

    rhizodat adapter check <module:ADAPTER | kind> [--dsn DSN] [--json]

Without a DSN only the declaration is checked (kind, DSN schemes, what it
opens). With one, the store is opened and its interface checked: the methods
rhizodat calls (``Store``, ``Branch``, ``Participant``), a branch opened and
rolled back, prepared gids listed, error classification, and every optional
capability (read-only queries, applied location, fence, ``Classifies``,
``Capacity``, entity parts, migration steps). Nothing is written. Behaviour (atomicity,
idempotence, recovery after a crash) is the adapter contract's job:
``rhizodat.testing.contract.AdapterContract`` and, for entity parts,
``rhizodat.testing.parts.PartStoreContract``.

Exit status: 0 when every required check that ran passed, 1 otherwise, 2 when
the adapter cannot be loaded.
"""

import argparse
import errno
import importlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from rhizodat.core.adapter import Adapter

__all__ = ["Check", "Report", "check_adapter", "load_adapter", "main"]

Level = Literal["required", "optional"]

_STORE_METHODS = ("branch", "participant", "execute", "close")
_BRANCH_METHODS = ("stage", "prepare", "commit_one_phase", "rollback", "is_retryable")
_PARTICIPANT_METHODS = ("finish", "is_applied", "abort", "prepared_gids", "is_retryable")
_CHECK_NAME = "dat_check"


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    level: Level
    passed: bool | None
    """``None``: not run (no DSN, or what it needs failed first)."""
    detail: str = ""


@dataclass
class Report:
    adapter: str
    dsn: str | None
    """Redacted."""
    checks: list[Check] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.passed is not False for c in self.checks if c.level == "required")

    def summary(self) -> dict[str, Any]:
        def count(level: Level) -> dict[str, int]:
            ran = [c for c in self.checks if c.level == level and c.passed is not None]
            return {"passed": sum(1 for c in ran if c.passed), "total": len(ran)}

        required, optional = count("required"), count("optional")
        run = required["total"] + optional["total"]
        passed = required["passed"] + optional["passed"]
        return {
            "required": required,
            "optional": optional,
            "not_run": sum(1 for c in self.checks if c.passed is None),
            "completeness": round(100 * passed / run) if run else 0,
        }

    def to_json(self) -> dict[str, Any]:
        return {
            "adapter": self.adapter,
            "dsn": self.dsn,
            "ok": self.ok,
            "checks": [asdict(c) for c in self.checks],
            "summary": self.summary(),
        }

    def to_text(self) -> str:
        marks = {True: "✔", False: "✖", None: "·"}
        where = f" ({self.dsn})" if self.dsn else ""
        lines = [f"rhizodat adapter check: {self.adapter}{where}"]
        for c in self.checks:
            detail = f" — {c.detail}" if c.detail else ""
            lines.append(f"  {c.level:<8}  {marks[c.passed]} {c.name}{detail}")
        s = self.summary()
        lines.append(
            f"required {s['required']['passed']}/{s['required']['total']},"
            f" optional {s['optional']['passed']}/{s['optional']['total']},"
            f" not run {s['not_run']}, completeness {s['completeness']}%"
        )
        lines.append(
            "behaviour is not checked here: run rhizodat.testing.contract.AdapterContract"
            " (and rhizodat.testing.parts.PartStoreContract for entity parts)"
        )
        return "\n".join(lines)


def load_adapter(target: str) -> Adapter:
    """``module:ATTR`` (e.g. ``rhizodat_mysql:ADAPTER``), or a registered kind (``postgres``)."""
    if ":" in target:
        module, _, attr = target.partition(":")
        try:
            found = getattr(importlib.import_module(module), attr)
        except (ImportError, AttributeError) as exc:
            raise ValueError(f"cannot load {target!r}: {exc}") from exc
        if not isinstance(found, Adapter):
            raise ValueError(f"{target!r} is not a rhizodat Adapter: {type(found).__name__}")
        return found
    from rhizodat.core.adapter import adapter_for

    try:
        return adapter_for(target)
    except KeyError as exc:
        raise ValueError(str(exc.args[0])) from exc


class _Run:
    def __init__(self, report: Report) -> None:
        self.report = report

    def add(self, name: str, level: Level, passed: bool | None, detail: str = "") -> bool:
        self.report.checks.append(Check(name, level, passed, detail))
        return passed is True

    def probe(self, name: str, level: Level, test: Callable[[], str | None]) -> bool:
        """Run ``test``: it returns a detail (pass) or raises ``_Fail`` / anything (fail)."""
        try:
            detail = test()
        except _Fail as fail:
            return self.add(name, level, False, str(fail))
        except Exception as exc:
            return self.add(name, level, False, f"{type(exc).__name__}: {exc}")
        return self.add(name, level, True, detail or "")

    def skip(self, names: Sequence[tuple[str, Level]], why: str) -> None:
        for name, level in names:
            self.add(name, level, None, why)


class _Fail(Exception):
    pass


def _missing(obj: object, names: Sequence[str]) -> list[str]:
    return [n for n in names if not callable(getattr(obj, n, None))]


# Checks that need an open store, in report order (shown as not run without one).
_STORE_CHECKS: tuple[tuple[str, Level], ...] = (
    ("store interface", "required"),
    ("store.branch opens and rolls back", "required"),
    ("participant.prepared_gids", "required"),
    ("errors: plain errors are not retryable", "required"),
    ("errors: device out of space is STORE_FULL", "required"),
    ("Classifies on the branch", "optional"),
    ("Classifies on the participant", "optional"),
    ("ReadOnlyQueries", "optional"),
    ("AppliedLocation", "optional"),
    ("Fenced", "optional"),
    ("Capacity (free space)", "optional"),
    ("PartStore (entity parts)", "optional"),
    ("PartBranch (entity parts in a Unit of Work)", "optional"),
    ("Migrates (schema migration steps)", "optional"),
    ("store closes", "required"),
)


def check_adapter(adapter: Adapter, *, dsn: str | None = None) -> Report:
    """Check ``adapter`` (and, with ``dsn``, the store it opens) without writing anything."""
    report = Report(adapter=str(getattr(adapter, "kind", "?")), dsn=_redacted(dsn))
    run = _Run(report)
    _declaration(run, adapter)
    if dsn is None:
        run.skip(
            (("DSN parses with this adapter", "required"), ("store opens", "required")),
            "no --dsn",
        )
        run.skip(_STORE_CHECKS, "no --dsn")
        return report
    if not run.probe("DSN parses with this adapter", "required", lambda: _parses(adapter, dsn)):
        run.skip((("store opens", "required"),), "the DSN does not parse")
        run.skip(_STORE_CHECKS, "the DSN does not parse")
        return report
    open_store = adapter.open_store
    if open_store is None:
        run.add("store opens", "required", None, "journal-only adapter: no store")
        run.skip(_STORE_CHECKS, "journal-only adapter")
        return report
    try:
        store = open_store(_CHECK_NAME, dsn)
    except Exception as exc:
        run.add("store opens", "required", False, f"{type(exc).__name__}: {exc}")
        run.skip(_STORE_CHECKS, "the store did not open")
        return report
    run.add("store opens", "required", True, type(store).__name__)
    try:
        _store_checks(run, store)
    finally:
        run.probe("store closes", "required", lambda: store.close())
    return report


def _declaration(run: _Run, adapter: Adapter) -> None:
    from rhizodat.core.adapter import _Registry

    def valid() -> str:
        if not isinstance(adapter, Adapter):
            raise _Fail(f"not a rhizodat Adapter: {type(adapter).__name__}")
        _Registry().add(adapter)  # the registry's own rules, on an empty registry
        return ", ".join(f"{s.name}://" for s in adapter.schemes)

    run.probe("declaration: kind and DSN schemes are valid", "required", valid)
    opens = [n for n in ("open_store", "open_journal") if getattr(adapter, n, None) is not None]
    run.add(
        "declaration: opens a store or a journal",
        "required",
        bool(opens),
        ", ".join(opens) or "neither open_store nor open_journal",
    )
    for name in ("open_journal", "journal_location", "query_help"):
        present = getattr(adapter, name, None) is not None
        run.add(f"declaration: {name}", "optional", present)


def _parses(adapter: Adapter, dsn: str) -> str:
    from rhizodat.core.adapter import register_adapter, scheme
    from rhizodat.core.dsn import parse_dsn

    name = dsn.partition("://")[0].lower()
    if name not in {s.name.lower() for s in adapter.schemes}:
        theirs = ", ".join(f"{s.name}://" for s in adapter.schemes)
        raise _Fail(f"{name}:// is not a scheme of this adapter ({theirs})")
    found = scheme(name)
    if found is None:
        register_adapter(adapter)  # loaded by module path, not installed: for this run
    elif found[0] is not adapter:
        raise _Fail(f"{name}:// is registered to another {found[0].kind!r} adapter")
    parsed = parse_dsn(dsn)
    return f"transport {parsed.transport}" if parsed.transport else ""


def _store_checks(run: _Run, store: Any) -> None:  # noqa: ANN401 - any adapter's store
    from rhizodat.core.adapter import AppliedLocation, Fenced, Migrates, ReadOnlyQueries
    from rhizodat.core.errors import Capacity, Classifies, ErrorKind, classify
    from rhizodat.core.identity import new_id
    from rhizodat.core.parts import PartBranch, PartStore

    def interface() -> str:
        problems = [f"store.{n}()" for n in _missing(store, _STORE_METHODS)]
        if not isinstance(getattr(store, "name", None), str):
            problems.append("store.name (str)")
        if not isinstance(getattr(store, "two_phase", None), bool):
            problems.append("store.two_phase (bool)")
        if problems:
            raise _Fail("missing " + ", ".join(problems))
        return "two-phase" if store.two_phase else "one-phase (applied after the decision)"

    if not run.probe("store interface", "required", interface):
        run.skip(_STORE_CHECKS[1:-1], "the store interface is incomplete")
        return

    branch: Any = None

    def open_branch() -> str:
        nonlocal branch
        branch = store.branch(new_id())
        missing = _missing(branch, _BRANCH_METHODS)
        if missing:
            raise _Fail("branch is missing " + ", ".join(f"{n}()" for n in missing))
        branch.rollback()
        return type(branch).__name__

    participant = store.participant()

    def prepared() -> str:
        missing = _missing(participant, _PARTICIPANT_METHODS)
        if missing:
            raise _Fail("participant is missing " + ", ".join(f"{n}()" for n in missing))
        gids = participant.prepared_gids()
        if not isinstance(gids, list) or not all(isinstance(g, str) for g in gids):
            raise _Fail(f"prepared_gids() must return a list of str, got {gids!r}")
        return f"{len(gids)} prepared"

    has_branch = run.probe("store.branch opens and rolls back", "required", open_branch)
    has_participant = run.probe("participant.prepared_gids", "required", prepared)
    sources = [s for s, ok in ((branch, has_branch), (participant, has_participant)) if ok]

    def not_retryable() -> None:
        error = ValueError("a bug, not a transient failure")
        wrong = [type(s).__name__ for s in sources if s.is_retryable(error)]
        if wrong:
            raise _Fail(f"{', '.join(wrong)} call(s) ValueError retryable")

    def no_space() -> None:
        error = OSError(errno.ENOSPC, "No space left on device")
        wrong = [
            type(s).__name__ for s in sources if classify(s, error) is not ErrorKind.STORE_FULL
        ]
        if wrong:
            raise _Fail(f"{', '.join(wrong)} do(es) not classify ENOSPC as STORE_FULL")

    if sources:
        run.probe("errors: plain errors are not retryable", "required", not_retryable)
        run.probe("errors: device out of space is STORE_FULL", "required", no_space)
    else:
        run.skip(_STORE_CHECKS[3:5], "no branch or participant")
    run.add("Classifies on the branch", "optional", has_branch and isinstance(branch, Classifies))
    run.add(
        "Classifies on the participant",
        "optional",
        has_participant and isinstance(participant, Classifies),
    )
    run.add("ReadOnlyQueries", "optional", isinstance(store, ReadOnlyQueries))
    run.add("AppliedLocation", "optional", isinstance(store, AppliedLocation))
    run.add("Fenced", "optional", isinstance(store, Fenced))
    if isinstance(store, Capacity):

        def free() -> str:
            value = store.free_space()
            if value is None:
                return "implemented; this store cannot tell (None)"
            if not isinstance(value, int) or value < 0:
                raise _Fail(f"free_space() must return bytes (int >= 0) or None, got {value!r}")
            return f"{value} bytes free"

        run.probe("Capacity (free space)", "optional", free)
    else:
        run.add("Capacity (free space)", "optional", False)
    run.add("PartStore (entity parts)", "optional", isinstance(store, PartStore))
    run.add(
        "PartBranch (entity parts in a Unit of Work)",
        "optional",
        has_branch and isinstance(branch, PartBranch),
    )
    if isinstance(store, Migrates):

        def kinds() -> str:
            found = store.migration_kinds
            if not found or not all(isinstance(k, str) for k in found):
                raise _Fail(f"migration_kinds must name step kinds, got {found!r}")
            return "runs " + ", ".join(sorted(found)) + " steps"

        run.probe("Migrates (schema migration steps)", "optional", kinds)
    else:
        run.add("Migrates (schema migration steps)", "optional", False)


def _redacted(dsn: str | None) -> str | None:
    from rhizodat.core.dsn import redact

    return None if dsn is None else redact(dsn)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rhizodat", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    adapter_cmd = commands.add_parser("adapter", help="work with store adapters")
    adapter_sub = adapter_cmd.add_subparsers(dest="action", required=True)
    check = adapter_sub.add_parser("check", help="dry-run an adapter and report what it has")
    check.add_argument("adapter", help="module:ATTR (e.g. rhizodat_mysql:ADAPTER) or a kind")
    check.add_argument("--dsn", help="open a store at this DSN and check it (nothing is written)")
    check.add_argument("--json", action="store_true", help="print the report as JSON")
    from rhizodat.migrations import cli as migrate_cli

    migrate_cli.add_parser(commands)
    args = parser.parse_args(argv)
    if args.command == "migrate":
        return migrate_cli.run(args)

    try:
        adapter = load_adapter(args.adapter)
    except ValueError as exc:
        print(f"rhizodat adapter check: {exc}", file=sys.stderr)
        return 2
    report = check_adapter(adapter, dsn=args.dsn)
    if args.json:
        print(json.dumps(report.to_json(), indent=2))
    else:
        print(report.to_text())
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
