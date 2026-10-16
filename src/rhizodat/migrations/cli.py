"""``rhizodat migrate DIR``: run a migration directory from an operator shell.

Stores and the journal come from the environment, so DSNs and their secrets
stay out of command lines and shell history:

- ``RHIZODAT_STORE_<NAME>=<dsn>``: one per store; the name is lower-cased and
  is what a step's ``store`` names;
- ``RHIZODAT_JOURNAL=<dsn>``: the journal, which keeps the migration history.

Exit status: 0 done, 1 a step failed (the next run resumes), 2 nothing ran
(bad files, a history that does not match them, a missing store or journal).
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from typing import Any

__all__ = ["add_parser", "run"]

_STORE_PREFIX = "RHIZODAT_STORE_"


def add_parser(commands: Any) -> None:  # noqa: ANN401 - argparse's subparsers action
    cmd = commands.add_parser(
        "migrate",
        help="run schema migrations (stores and journal from RHIZODAT_STORE_<NAME>,"
        " RHIZODAT_JOURNAL)",
    )
    cmd.add_argument("directory", help="the directory of NNNN_name.toml migrations")
    cmd.add_argument("--to", type=int, help="stop after this migration id")
    cmd.add_argument("--dry-run", action="store_true", help="check and report, run nothing")
    cmd.add_argument("--json", action="store_true", help="print the report as JSON")


def _stores(environ: Mapping[str, str]) -> dict[str, str]:
    return {
        key.removeprefix(_STORE_PREFIX).lower(): dsn
        for key, dsn in sorted(environ.items())
        if key.startswith(_STORE_PREFIX) and len(key) > len(_STORE_PREFIX)
    }


def _fail(message: str, status: int) -> int:
    print(f"rhizodat migrate: {message}", file=sys.stderr)
    return status


def run(args: argparse.Namespace, environ: Mapping[str, str] | None = None) -> int:
    from rhizodat.api import Rhizodat
    from rhizodat.migrations import MigrationError, MigrationFailed, load_migrations

    env = os.environ if environ is None else environ
    try:
        migrations = load_migrations(args.directory)
    except ValueError as exc:
        return _fail(str(exc), 2)
    stores = _stores(env)
    if not stores:
        return _fail(f"no stores configured: set {_STORE_PREFIX}<NAME>=<dsn> for each store", 2)
    journal = env.get("RHIZODAT_JOURNAL") or None
    if journal is None:
        return _fail("the migration history lives in the journal: set RHIZODAT_JOURNAL=<dsn>", 2)
    try:
        dat = Rhizodat(stores=stores, journal=journal)
    except Exception as exc:
        return _fail(f"cannot open the stores: {type(exc).__name__}: {exc}", 2)
    with dat:
        try:
            report = dat.migrate(migrations, to=args.to, dry_run=args.dry_run)
        except MigrationFailed as exc:
            return _fail(str(exc), 1)
        except (MigrationError, ValueError) as exc:
            return _fail(str(exc), 2)
        versions = dat.schema_versions()
    if args.json:
        out: dict[str, Any] = {
            "applied": list(report.applied),
            "pending": list(report.pending),
            "already": list(report.already),
            "schema_versions": [
                {
                    "id": v.id,
                    "name": v.name,
                    "checksum": v.checksum,
                    "applied_at": v.applied_at.isoformat(),
                }
                for v in versions
            ],
        }
        print(json.dumps(out, indent=2))
    else:
        for label, ids in (
            ("already", report.already),
            ("applied", report.applied),
            ("pending", report.pending),
        ):
            print(f"{label}: {', '.join(map(str, ids)) or '-'}")
    return 0
