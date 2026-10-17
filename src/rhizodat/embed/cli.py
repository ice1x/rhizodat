"""``rhizodat embed`` and ``rhizodat similar``: embeddings from an operator shell.

    rhizodat embed --store graph --label Chapter --text summary --model mypkg:Embedder
    rhizodat similar --store graph --label Chapter --model mypkg:Embedder "a stormy night"

Stores come from ``RHIZODAT_STORE_<NAME>``; ``--model`` is ``hash[:<dimensions>]``
or ``module:Class``. Exit status: 0 done, 1 the store failed, 2 nothing ran.
"""

import argparse
import json
import os
import sys
from collections.abc import Mapping
from typing import Any

__all__ = ["add_parsers", "run_embed", "run_similar"]


def add_parsers(commands: Any) -> None:  # noqa: ANN401 - argparse's subparsers action
    embed = commands.add_parser("embed", help="embed the text of a label's nodes")
    similar = commands.add_parser("similar", help="the nodes nearest to a text")
    for cmd in (embed, similar):
        cmd.add_argument("--store", required=True, help="the store (RHIZODAT_STORE_<NAME>)")
        cmd.add_argument("--label", required=True, help="the nodes' label")
        cmd.add_argument("--model", default="hash", help="hash[:<dimensions>] or module:Class")
        cmd.add_argument("--property", default="embedding", help="where the vector is kept")
        cmd.add_argument("--json", action="store_true", help="print the result as JSON")
    embed.add_argument("--text", required=True, help="the property holding the text")
    embed.add_argument("--batch", type=int, default=64, help="texts per embedder call")
    similar.add_argument("query", help="the text to find neighbours of")
    similar.add_argument("-k", type=int, default=10, help="how many (default 10)")


def _fail(command: str, message: str, status: int) -> int:
    print(f"rhizodat {command}: {message}", file=sys.stderr)
    return status


def _open(command: str, args: argparse.Namespace, environ: Mapping[str, str] | None) -> Any:  # noqa: ANN401
    from rhizodat.api import Rhizodat
    from rhizodat.embed import load_embedder
    from rhizodat.migrations.cli import _stores

    stores = _stores(os.environ if environ is None else environ)
    if not stores:
        return _fail(command, "no stores configured: set RHIZODAT_STORE_<NAME>=<dsn>", 2)
    try:
        embedder = load_embedder(args.model)
    except ValueError as exc:
        return _fail(command, str(exc), 2)
    try:
        return Rhizodat(stores=stores), embedder
    except Exception as exc:
        return _fail(command, f"cannot open the stores: {type(exc).__name__}: {exc}", 2)


def run_embed(args: argparse.Namespace, environ: Mapping[str, str] | None = None) -> int:
    from rhizodat.embed import embed_nodes

    opened = _open("embed", args, environ)
    if isinstance(opened, int):
        return opened
    dat, embedder = opened
    with dat:
        try:
            report = embed_nodes(
                dat,
                store=args.store,
                label=args.label,
                text=args.text,
                embedder=embedder,
                property=args.property,
                batch=args.batch,
            )
        except (ValueError, KeyError) as exc:
            return _fail("embed", str(exc.args[0]) if exc.args else str(exc), 2)
        except Exception as exc:
            return _fail("embed", f"failed, nothing written: {type(exc).__name__}: {exc}", 1)
    if args.json:
        out = {"embedded": report.embedded, "unchanged": report.unchanged, "model": report.model}
        print(json.dumps(out))
    else:
        print(f"{report.embedded} embedded, {report.unchanged} unchanged ({report.model})")
    return 0


def run_similar(args: argparse.Namespace, environ: Mapping[str, str] | None = None) -> int:
    from rhizodat.embed import similar

    opened = _open("similar", args, environ)
    if isinstance(opened, int):
        return opened
    dat, embedder = opened
    with dat:
        try:
            found = similar(
                dat,
                store=args.store,
                label=args.label,
                query=args.query,
                embedder=embedder,
                property=args.property,
                k=args.k,
            )
        except (ValueError, KeyError) as exc:
            return _fail("similar", str(exc.args[0]) if exc.args else str(exc), 2)
        except Exception as exc:
            return _fail("similar", f"{type(exc).__name__}: {exc}", 1)
    if args.json:
        print(
            json.dumps(
                [{"key": n.key, "score": n.score, "properties": n.properties} for n in found]
            )
        )
    else:
        for n in found:
            print(f"{n.score:.3f}  {n.key}")
    return 0
