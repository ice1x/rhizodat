"""``rhizodat-ontology propose`` and ``rhizodat-ontology apply``.

    rhizodat-ontology propose customers.csv orders.csv -o proposal.json
    rhizodat-ontology apply proposal.json --store graph

Each file is a table named after the file. ``apply`` reads stores from
``RHIZODAT_STORE_<NAME>``. Exit status: 0 done, 1 the model or the store
failed, 2 nothing ran.
"""

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path


def _fail(message: str, status: int) -> int:
    print(f"rhizodat-ontology: {message}", file=sys.stderr)
    return status


def main(argv: Sequence[str] | None = None) -> int:
    from rhizodat_ontology import MODEL, ProposalError, apply, propose

    parser = argparse.ArgumentParser(prog="rhizodat-ontology", description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("propose", help="ask the model for an ontology of these tables")
    p.add_argument("sources", nargs="+", help="table files; each is named after its file")
    p.add_argument("-o", "--output", required=True, help="where to write the proposal (JSON)")
    p.add_argument("--model", default=MODEL)
    p.add_argument("--sample-rows", type=int, default=20)
    a = commands.add_parser("apply", help="load the tables with the accepted relationships")
    a.add_argument("proposal", help="a reviewed proposal file")
    a.add_argument("--store", required=True, help="the graph store (RHIZODAT_STORE_<NAME>)")
    args = parser.parse_args(argv)

    if args.command == "propose":
        sources = {Path(s).name.split(".", 1)[0]: s for s in args.sources}
        try:
            proposal = propose(sources, model=args.model, sample_rows=args.sample_rows)
        except ProposalError as exc:
            return _fail(str(exc), 1)
        except ValueError as exc:
            return _fail(str(exc), 2)
        Path(args.output).write_text(json.dumps(proposal, indent=2, ensure_ascii=False) + "\n")
        n = len(proposal["relationships"])
        print(f"{len(proposal['classes'])} classes, {n} relationships -> {args.output}")
        print('review it, then set "accepted": true on the relationships to keep')
        return 0

    from rhizodat import Rhizodat
    from rhizodat.migrations.cli import _stores

    try:
        proposal = json.loads(Path(args.proposal).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        return _fail(f"cannot read {args.proposal}: {exc}", 2)
    stores = _stores(os.environ)
    if not stores:
        return _fail("no stores configured: set RHIZODAT_STORE_<NAME>=<dsn>", 2)
    with Rhizodat(stores=stores) as dat:
        try:
            report = apply(dat, proposal, store=args.store)
        except (ProposalError, ValueError, KeyError) as exc:
            return _fail(str(exc.args[0]) if exc.args else str(exc), 2)
        except Exception as exc:
            return _fail(f"load failed, the graph is as it was: {type(exc).__name__}: {exc}", 1)
    for r in report.relationships:
        print(f"{r.table}.{r.column} -> {r.target}.{r.key}  {r.matched}/{r.total}")
    print(f"loaded {report.nodes} nodes, {report.edges} edges -> {report.store}")
    return 0
