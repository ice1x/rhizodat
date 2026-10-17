"""``rhizodat import SOURCE --store NAME --table T``: import a file from an operator shell.

Stores and the journal come from the environment (``RHIZODAT_STORE_<NAME>``,
``RHIZODAT_JOURNAL``), as for ``rhizodat migrate``. ``--dry-run`` prints the
inferred columns and the migration the import would write, and touches
nothing. Exit status: 0 done, 1 the load failed (the table is as it was), 2
nothing ran (a bad file, a missing directory or store, a refused migration).
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlsplit

__all__ = ["add_parser", "run"]


def add_parser(commands: Any) -> None:  # noqa: ANN401 - argparse's subparsers action
    cmd = commands.add_parser(
        "import",
        help="import a CSV, TSV, JSON Lines or Parquet file into a store, inferring its schema",
    )
    cmd.add_argument("source", help="a path or URI (s3://, gs://, file://)")
    cmd.add_argument("--store", required=True, help="the store's name (RHIZODAT_STORE_<NAME>)")
    cmd.add_argument(
        "--table", help="the table (Iceberg: namespace.table); not for an RDF file into a graph"
    )
    cmd.add_argument(
        "--migrations", help="the migration directory; needed for a new table or columns"
    )
    cmd.add_argument("--replace", action="store_true", help="replace the table's contents")
    cmd.add_argument(
        "--order-by", default="", help="sort key columns, comma-separated (ClickHouse)"
    )
    cmd.add_argument(
        "--format",
        choices=["csv", "tsv", "jsonl", "parquet", "turtle", "nt", "n3", "json-ld", "xml"],
        help="the file's format when its name does not tell (the last five are RDF)",
    )
    cmd.add_argument(
        "--null",
        action="append",
        dest="nulls",
        help='text read as a missing value (repeatable; default "", NULL, null, \\N)',
    )
    cmd.add_argument("--sample-rows", type=int, help="infer from the first N rows only")
    cmd.add_argument("--dry-run", action="store_true", help="show the plan, change nothing")
    cmd.add_argument("--json", action="store_true", help="print the report as JSON")


def _scrub(message: str, source: str) -> str:
    """``message`` without the source's secret, whatever echoed it."""
    try:
        secret = urlsplit(source).password
    except ValueError:
        return message
    if secret:
        for form in {secret, unquote(secret)}:
            message = message.replace(form, "***")
    return message


def _fail(message: str, status: int, source: str) -> int:
    print(f"rhizodat import: {_scrub(message, source)}", file=sys.stderr)
    return status


def _columns(plan: Any) -> list[dict[str, Any]]:  # noqa: ANN401 - ImportPlan
    return [
        {
            "name": c.name,
            "type": c.type,
            "nullable": c.nullable,
            "low_cardinality": c.low_cardinality,
        }
        for c in plan.schema.columns
    ]


_RDF = ("turtle", "nt", "n3", "json-ld", "xml")


def _run_graph(args: argparse.Namespace, stores: dict[str, str], journal: str | None) -> int:
    """An RDF file into a graph store: no table, no migration, one transaction."""
    from rhizodat.api import Rhizodat
    from rhizodat.ingest import import_graph

    source = args.source
    if args.table:
        return _fail("an RDF file goes into a graph: --table does not apply", 2, source)
    try:
        dat = Rhizodat(stores=stores, journal=journal)
    except Exception as exc:
        return _fail(f"cannot open the stores: {type(exc).__name__}: {exc}", 2, source)
    with dat:
        if args.dry_run:
            from rhizodat.ingest.graph import read_rdf

            try:
                graph = read_rdf(source, format=args.format)
            except (ValueError, OSError) as exc:
                return _fail(str(exc), 2, source)
            labels = sorted({label for n in graph.nodes.values() for label in n.labels})
            types = sorted({e.type for e in graph.edges})
            out = {
                "nodes": len(graph.nodes),
                "edges": len(graph.edges),
                "labels": labels,
                "edge_types": types,
            }
            if args.json:
                print(json.dumps(out, indent=2))
            else:
                print(f"{len(graph.nodes)} nodes, {len(graph.edges)} edges -> {args.store}")
                print(f"  labels: {', '.join(labels) or '-'}")
                print(f"  edge types: {', '.join(types) or '-'}")
            return 0
        try:
            report = import_graph(dat, source, store=args.store, format=args.format)
        except (ValueError, KeyError, OSError) as exc:
            text = str(exc.args[0]) if isinstance(exc, KeyError) and exc.args else str(exc)
            return _fail(text, 2, source)
        except Exception as exc:
            return _fail(
                f"load failed, the graph is as it was: {type(exc).__name__}: {exc}", 1, source
            )
    if args.json:
        print(
            json.dumps(
                {
                    "source": report.source,
                    "store": report.store,
                    "nodes": report.nodes,
                    "edges": report.edges,
                },
                indent=2,
            )
        )
    else:
        print(f"{report.nodes} nodes, {report.edges} edges -> {report.store}")
    return 0


def run(args: argparse.Namespace, environ: Mapping[str, str] | None = None) -> int:
    from pathlib import Path

    from rhizodat.api import Rhizodat
    from rhizodat.ingest import DEFAULT_NULLS, import_file, plan_import
    from rhizodat.migrations import Migration, MigrationError, to_toml
    from rhizodat.migrations.cli import _stores

    env = os.environ if environ is None else environ
    source = args.source
    stores = _stores(env)
    if not stores:
        return _fail(
            "no stores configured: set RHIZODAT_STORE_<NAME>=<dsn> for each store", 2, source
        )
    from rhizodat.ingest.graph import graph_format

    if args.format in _RDF or (args.format is None and graph_format(source)):
        return _run_graph(args, stores, env.get("RHIZODAT_JOURNAL") or None)
    if not args.table:
        return _fail("--table is needed to import a table (only RDF files go without)", 2, source)
    options: dict[str, Any] = {
        "store": args.store,
        "table": args.table,
        "format": args.format,
        "null_values": tuple(args.nulls) if args.nulls else DEFAULT_NULLS,
        "sample_rows": args.sample_rows,
        "order_by": [c.strip() for c in args.order_by.split(",") if c.strip()],
    }
    try:
        dat = Rhizodat(stores=stores, journal=env.get("RHIZODAT_JOURNAL") or None)
    except Exception as exc:
        return _fail(f"cannot open the stores: {type(exc).__name__}: {exc}", 2, source)
    with dat:
        try:
            plan = plan_import(dat, source, **options)
        except (ValueError, KeyError, OSError) as exc:
            text = str(exc.args[0]) if isinstance(exc, KeyError) and exc.args else str(exc)
            return _fail(text, 2, source)
        if args.dry_run:
            out: dict[str, Any] = {
                "source": plan.source,
                "store": plan.store,
                "table": plan.table,
                "columns": _columns(plan),
            }
            migration = None
            if plan.steps:
                from rhizodat.ingest.importer import _next_id, _slug

                n = _next_id(dat, Path(args.migrations)) if args.migrations else 1
                suffix = "" if plan.new_table else "_columns"
                migration = Migration.of(n, f"import_{_slug(plan.table)}{suffix}", plan.steps)
                out["migration"] = {
                    "file": f"{migration.id:04}_{migration.name}.toml",
                    "text": to_toml(migration),
                }
            if args.json:
                print(json.dumps(out, indent=2))
            else:
                width = max(len(c.name) for c in plan.schema.columns)
                print(f"{plan.source} -> {plan.store}.{plan.table} ({plan.schema.rows} rows read)")
                for c in plan.schema.columns:
                    flags = ("nullable " if c.nullable else "") + (
                        "low-cardinality" if c.low_cardinality else ""
                    )
                    print(f"  {c.name.ljust(width)}  {c.type}  {flags}".rstrip())
                if migration is not None:
                    print(f"\nwould write {out['migration']['file']}:\n")
                    print(out["migration"]["text"])
                else:
                    print("\nthe table fits: no migration")
            return 0
        try:
            report = import_file(
                dat, source, migrations=args.migrations, replace=args.replace, **options
            )
        except (MigrationError, ValueError) as exc:
            return _fail(str(exc), 2, source)
        except Exception as exc:
            return _fail(
                f"load failed, the table is as it was: {type(exc).__name__}: {exc}", 1, source
            )
    if args.json:
        print(
            json.dumps(
                {
                    "source": report.plan.source,
                    "store": report.plan.store,
                    "table": report.plan.table,
                    "rows": report.rows,
                    "migration": report.migration,
                    "columns": _columns(report.plan),
                },
                indent=2,
            )
        )
    else:
        migrated = f", migration {report.migration}" if report.migration else ""
        print(f"{report.rows} rows -> {report.plan.store}.{report.plan.table}{migrated}")
    return 0
