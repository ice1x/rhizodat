"""An ERP turns its exported tables into a graph and walks it.

Customers, orders and order lines go into drevo with the references inferred
from their columns; the planner then follows lines to orders to customers in
Cypher. The CLI shows the inferred references before loading anything.
"""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.check import main
from rhizodat.ingest import import_tables_graph


@pytest.fixture
def prefix() -> str:
    """Table names of this test only: the Bolt server outlives it."""
    return f"t{uuid.uuid4().hex[:8]}_"


@pytest.fixture
def exports(tmp_path: Path, faker: Faker, prefix: str) -> dict[str, Path]:
    def write(name: str, header: str, rows: list[str]) -> Path:
        path = tmp_path / f"{prefix}{name}.csv"
        path.write_text("\n".join([header, *rows]) + "\n")
        return path

    return {
        f"{prefix}customers": write(
            "customers", "id,name", [f'{i},"{faker.company()}"' for i in range(1, 11)]
        ),
        f"{prefix}orders": write(
            "orders",
            f"id,{prefix}customer_id,total",
            [f"{i},{faker.random_int(1, 10)},{faker.random_int(10, 999)}" for i in range(1, 31)],
        ),
    }


@pytest.fixture
def dat(drevo_bolt_dsn: str, prefix: str) -> Iterator[Rhizodat]:
    d = Rhizodat(stores={"graph": drevo_bolt_dsn})
    yield d
    try:
        d.store("graph").execute(
            "MATCH (n:Resource) WHERE n.title STARTS WITH $p DETACH DELETE n", {"p": prefix}
        )
    finally:
        d.close()


def test_tables_become_a_graph_the_planner_can_walk(
    dat: Rhizodat, exports: dict[str, Path], prefix: str
) -> None:
    report = import_tables_graph(dat, exports, store="graph")
    [rel] = [r for r in report.relationships if r.accepted]
    assert (rel.table, rel.column, rel.target) == (
        f"{prefix}orders",
        f"{prefix}customer_id",
        f"{prefix}customers",
    )
    assert (report.nodes, report.edges) == (40, 30)
    walked = (
        dat.store("graph")
        .execute(
            f"MATCH (o:Resource)-[:{prefix}customer]->(c:Resource) WHERE o.title STARTS WITH $p"
            " RETURN count(o) AS n",
            {"p": prefix},
        )
        .one()
    )
    assert walked["n"] == 30


def test_the_cli_shows_the_references_first(
    dat: Rhizodat,
    exports: dict[str, Path],
    prefix: str,
    drevo_bolt_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("RHIZODAT_STORE_GRAPH", drevo_bolt_dsn)
    files = [str(p) for p in exports.values()]
    assert main(["relate", *files, "--store", "graph", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert f"+ {prefix}orders.{prefix}customer_id -> {prefix}customers.id  30/30" in out
    assert "would load 40 nodes, 30 edges -> graph" in out

    assert main(["relate", *files, "--store", "graph", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["edges"] == 30

    assert main(["relate", *files, "--store", "graph", "--key", "nope=id"]) == 2
    assert "expected TABLE=COLUMN" in capsys.readouterr().err
    # 30 orders of 10 customers: the customer column must repeat.
    key = f"{prefix}orders={prefix}customer_id"
    assert main(["relate", *files, "--store", "graph", "--key", key]) == 2
    assert "repeats" in capsys.readouterr().err
