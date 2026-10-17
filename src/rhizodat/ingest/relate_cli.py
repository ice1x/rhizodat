"""``rhizodat relate FILE... --store NAME``: several tables as one graph.

Each file is a table named after the file (``orders.csv`` → ``orders``). The
references between them are inferred (see ``rhizodat.ingest.relate``) and
shown with how much of each matched; ``--dry-run`` stops there. Stores come
from ``RHIZODAT_STORE_<NAME>``. Exit status: 0 done, 1 the load failed (the
graph is as it was), 2 nothing ran.
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

__all__ = ["add_parser", "run"]


def add_parser(commands: Any) -> None:  # noqa: ANN401 - argparse's subparsers action
    cmd = commands.add_parser(
        "relate",
        help="load several tables into a graph store, with the references between them",
    )
    cmd.add_argument("sources", nargs="+", help="table files; each is named after its file")
    cmd.add_argument("--store", required=True, help="the graph store (RHIZODAT_STORE_<NAME>)")
    cmd.add_argument(
        "--key",
        action="append",
        default=[],
        metavar="TABLE=COLUMN",
        help="a table's key column (TABLE= for none); found when not given",
    )
    cmd.add_argument("--min-coverage", type=float, default=0.95, help="default 0.95")
    cmd.add_argument("--dry-run", action="store_true", help="show the references, load nothing")
    cmd.add_argument("--json", action="store_true", help="print the report as JSON")


def _fail(message: str, status: int) -> int:
    print(f"rhizodat relate: {message}", file=sys.stderr)
    return status


def _table_name(source: str) -> str:
    name = Path(source.split("?", 1)[0]).name
    for _ in range(2):  # orders.csv.gz → orders
        name = name.rsplit(".", 1)[0] if "." in name else name
    return name


def run(args: argparse.Namespace, environ: Mapping[str, str] | None = None) -> int:
    from rhizodat.api import Rhizodat
    from rhizodat.ingest import import_tables_graph
    from rhizodat.migrations.cli import _stores

    env = os.environ if environ is None else environ
    stores = _stores(env)
    if not stores:
        return _fail("no stores configured: set RHIZODAT_STORE_<NAME>=<dsn> for each store", 2)
    sources = {_table_name(s): s for s in args.sources}
    if len(sources) < len(args.sources):
        return _fail("two files would make tables of the same name", 2)
    keys: dict[str, str | None] = {}
    for item in args.key:
        table, sep, column = item.partition("=")
        if not sep or table not in sources:
            return _fail(f"--key {item!r}: expected TABLE=COLUMN for one of {sorted(sources)}", 2)
        keys[table] = column or None
    try:
        dat = Rhizodat(stores=stores)
    except Exception as exc:
        return _fail(f"cannot open the stores: {type(exc).__name__}: {exc}", 2)
    with dat:
        try:
            report = import_tables_graph(
                dat,
                sources,
                store=args.store,
                keys=keys,
                min_coverage=args.min_coverage,
                dry_run=args.dry_run,
            )
        except (ValueError, KeyError, OSError) as exc:
            text = str(exc.args[0]) if isinstance(exc, KeyError) and exc.args else str(exc)
            return _fail(text, 2)
        except Exception as exc:
            return _fail(f"load failed, the graph is as it was: {type(exc).__name__}: {exc}", 1)
    rels = [
        {
            "from": f"{r.table}.{r.column}",
            "to": f"{r.target}.{r.key}",
            "matched": r.matched,
            "total": r.total,
            "accepted": r.accepted,
        }
        for r in report.relationships
    ]
    if args.json:
        out = {"nodes": report.nodes, "edges": report.edges, "relationships": rels}
        print(json.dumps(out, indent=2))
        return 0
    for r in rels:
        mark = "+" if r["accepted"] else "-"
        print(f"{mark} {r['from']} -> {r['to']}  {r['matched']}/{r['total']}")
    verb = "would load" if args.dry_run else "loaded"
    print(f"{verb} {report.nodes} nodes, {report.edges} edges -> {report.store}")
    return 0
