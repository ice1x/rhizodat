"""Relationships inferred from tables: a column whose values are another table's keys.

An ERP exports customers, orders, order lines and employees as CSV. The orders
point at customers, the lines at orders, employees at their managers; the
quantities merely look like ids. A relationship is accepted when the column's
name says it refers (``customer_id``, ``order``) and almost all of its values
are found among the target's keys; every candidate is reported with how much
of it matched.
"""

from pathlib import Path

import pytest
from faker import Faker

from rhizodat.ingest.relate import (
    Relationship,
    infer_relationships,
    read_tables,
    tables_to_graph,
)


def write(path: Path, header: str, rows: list[str]) -> Path:
    path.write_text("\n".join([header, *rows]) + "\n")
    return path


@pytest.fixture
def erp(tmp_path: Path, faker: Faker) -> dict[str, Path]:
    customers = [f'{i},"{faker.company()}"' for i in range(1, 41)]
    orders = [f"{i},{faker.random_int(1, 40)},{faker.random_int(1, 5)}" for i in range(1, 101)]
    lines = [f"{faker.random_int(1, 100)},SKU-{i},{faker.random_int(1, 5)}" for i in range(300)]
    staff = ["1,Grace,"] + [
        f"{i},{faker.first_name()},{faker.random_int(1, i - 1)}" for i in range(2, 21)
    ]
    # 3 of 50 payments name an order that is not in the export: 94 % < 95 %.
    payments = [f"P{i},{i if i < 48 else 900 + i}" for i in range(1, 51)]
    return {
        "customers": write(tmp_path / "customers.csv", "id,name", customers),
        "orders": write(tmp_path / "orders.csv", "id,customer_id,qty", orders),
        "order_lines": write(tmp_path / "order_lines.csv", "order_id,sku,qty", lines),
        "employees": write(tmp_path / "employees.csv", "id,name,manager_id", staff),
        "payments": write(tmp_path / "payments.csv", "payment_ref,order_ref", payments),
    }


def test_keys_are_unique_complete_columns(erp: dict[str, Path]) -> None:
    tables = read_tables(erp)
    assert {name: t.key for name, t in tables.items()} == {
        "customers": "id",
        "orders": "id",
        "order_lines": "sku",  # no id column: the first unique, complete one
        "employees": "id",
        "payments": "payment_ref",
    }


def test_references_are_found_and_look_alikes_are_not(erp: dict[str, Path]) -> None:
    found = infer_relationships(read_tables(erp))
    accepted = {(r.table, r.column, r.target) for r in found if r.accepted}
    assert accepted == {
        ("orders", "customer_id", "customers"),
        ("order_lines", "order_id", "orders"),
        ("employees", "manager_id", "employees"),  # a table may point at itself
    }
    # Quantities 1..5 match ids 1..5 completely, but nothing in their name refers.
    assert not any(r.column == "qty" for r in found)
    [orphans] = [r for r in found if r.column == "order_ref"]
    assert not orphans.accepted
    assert (orphans.matched, orphans.total) == (47, 50)
    assert orphans.coverage == pytest.approx(0.94)


def test_the_threshold_and_explicit_keys_are_choices(erp: dict[str, Path]) -> None:
    lenient = infer_relationships(read_tables(erp), min_coverage=0.9)
    assert any(r.column == "order_ref" and r.accepted for r in lenient)

    tables = read_tables(erp, keys={"order_lines": None})  # no key: nothing points at it
    assert tables["order_lines"].key is None
    with pytest.raises(ValueError, match="column nope"):
        read_tables(erp, keys={"orders": "nope"})
    with pytest.raises(ValueError, match="repeats"):
        read_tables(erp, keys={"order_lines": "qty"})


def test_tables_become_nodes_and_relationships_edges(erp: dict[str, Path]) -> None:
    tables = read_tables(erp)
    relationships = [r for r in infer_relationships(tables) if r.accepted]
    graph = tables_to_graph(tables, relationships)

    order = graph.nodes["orders:7"]
    assert order.labels == ("Order",)
    assert set(order.properties) == {"id", "qty", "customer_id"}
    line_labels = {graph.nodes[k].labels for k in graph.nodes if k.startswith("order_lines:")}
    assert line_labels == {("OrderLine",)}

    edges = {(e.type, e.source.split(":")[0], e.target.split(":")[0]) for e in graph.edges}
    assert edges == {
        ("customer", "orders", "customers"),
        ("order", "order_lines", "orders"),
        ("manager", "employees", "employees"),
    }
    assert sum(1 for e in graph.edges if e.type == "order") == 300
    assert sum(1 for e in graph.edges if e.type == "manager") == 19  # the head has none


def test_a_relationship_can_be_given_by_hand(erp: dict[str, Path]) -> None:
    tables = read_tables(erp)
    by_hand = Relationship("payments", "order_ref", "orders", "id", 47, 50, accepted=True)
    graph = tables_to_graph(tables, [by_hand])
    assert sum(1 for e in graph.edges if e.type == "order_ref") == 47  # orphans have no edge
    with pytest.raises(ValueError, match="no table nope"):
        tables_to_graph(tables, [Relationship("nope", "x", "orders", "id", 0, 0, True)])


def test_names_values_and_tables_without_keys(tmp_path: Path) -> None:
    import datetime as dt
    import decimal

    from rhizodat.ingest.relate import Table, _graph_value, _label, singular

    assert (singular("categories"), singular("class"), singular("orders")) == (
        "category",
        "class",
        "order",
    )
    assert (_label("order_lines"), _label("2024_sales"), _label("--")) == (
        "OrderLine",
        "T2024Sale",
        "Row",
    )
    assert _graph_value(dt.date(2026, 1, 2)) == "2026-01-02"
    assert _graph_value(decimal.Decimal("1.50")) == "1.50"
    assert _graph_value(b"x") == "b'x'"

    log = Table("log", None, ("category_id", "note"), [{"category_id": None, "note": "a"}] * 2)
    cats = Table("categories", "id", ("id",), [{"id": 1}])
    tables = {"log": log, "categories": cats}
    # Only missing values: no candidate at all.
    assert infer_relationships(tables) == []
    graph = tables_to_graph(tables, [])
    assert set(graph.nodes) == {"log:1", "log:2", "categories:1"}  # row numbers without a key


def test_a_proposed_relationship_is_measured_against_the_data(erp: dict[str, Path]) -> None:
    from rhizodat.ingest.relate import measure

    tables = read_tables(erp)
    rel = measure(tables, "payments", "order_ref", "orders")
    assert (rel.matched, rel.total, rel.accepted) == (47, 50, True)
    assert not measure(tables, "customers", "name", "orders").accepted  # nothing matches
    for args, message in [
        (("nope", "x", "orders"), "no table nope"),
        (("orders", "nope", "customers"), "no column nope"),
    ]:
        with pytest.raises(ValueError, match=message):
            measure(tables, *args)
    no_key = read_tables(erp, keys={"customers": None})
    with pytest.raises(ValueError, match="no key"):
        measure(no_key, "orders", "customer_id", "customers")
