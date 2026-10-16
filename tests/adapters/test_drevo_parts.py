"""drevo as a home for entity parts: one node per version, references as edges. The
behaviour is the parts contract (tests/contract/test_parts_contracts.py)."""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from faker import Faker

from drevo_parts import clear_parts, drevo_parts_dsn
from rhizodat.adapters.drevo import ANCHOR, DrevoStore
from rhizodat.core.coordinator import gid_for
from rhizodat.core.identity import new_id
from rhizodat.core.journal import BranchRecord
from rhizodat.testing.parts import CONTRACT_PART, commit, part_values

LABEL = CONTRACT_PART.table


@pytest.fixture(params=["memory", "file", "bolt"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DrevoStore]:
    s = DrevoStore(drevo_parts_dsn(request, tmp_path), name="parts")
    clear_parts(s)
    s.install_part(CONTRACT_PART)
    yield s
    clear_parts(s)
    s.close()


def node(store: DrevoStore, dat_id: uuid.UUID) -> dict[str, Any]:
    rows = store.execute(
        f"MATCH (v:{LABEL} {{dat_id: $id}}) RETURN properties(v) AS p", {"id": str(dat_id)}
    ).all()
    assert len(rows) == 1
    return dict(rows[0]["p"])


def edges(store: DrevoStore, dat_id: uuid.UUID) -> list[tuple[str, str]]:
    rows = store.execute(
        f"MATCH (v:{LABEL} {{dat_id: $id}})-[r]->(a:{ANCHOR}) RETURN type(r) AS r, a.dat_id AS a",
        {"id": str(dat_id)},
    ).all()
    return sorted((r["r"], r["a"]) for r in rows)


def anchors(store: DrevoStore) -> set[str]:
    return {r["a"] for r in store.execute(f"MATCH (a:{ANCHOR}) RETURN a.dat_id AS a").all()}


def test_a_version_is_a_node_with_native_and_tagged_properties(
    store: DrevoStore, faker: Faker
) -> None:
    dat_id, v = new_id(), part_values(faker)
    txn = commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 3 << 16)

    p = node(store, dat_id)
    assert (p["dat_id"], p["dat_txn"], p["dat_tx_time"], p["dat_created"]) == (
        str(dat_id),
        str(txn),
        3 << 16,
        3 << 16,
    )
    assert p["dat_deleted"] is False
    # drevo keeps node titles unique graph-wide: the field lives under another name.
    assert "title" not in p
    assert p["dat_title"] == v["title"]
    assert (p["points"], p["done"], p["ratio"]) == (v["points"], v["done"], v["ratio"])
    assert p["owner"] == str(v["owner"])
    assert json.loads(p["estimate"]) == {"$": "decimal", "v": "3.50"}
    assert json.loads(p["tags"]) == v["tags"]
    assert "note" not in p  # None is no property


def test_versions_may_share_a_title(store: DrevoStore, faker: Faker) -> None:
    dat_id, v = new_id(), part_values(faker)
    other = {**part_values(faker), "title": v["title"]}
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 1 << 16)
    commit(store, lambda b: b.write_part(CONTRACT_PART, new_id(), other, created=None), 2 << 16)
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=1 << 16), 3 << 16)
    assert len(store.part_history(CONTRACT_PART, dat_id)) == 2


def test_references_are_edges_to_anchor_nodes(store: DrevoStore, faker: Faker) -> None:
    dat_id, v = new_id(), part_values(faker)
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 1 << 16)

    assert edges(store, dat_id) == sorted(
        [("owner", str(v["owner"]))] + [("watchers", str(w)) for w in v["watchers"]]
    )
    # A graph query can follow them: who watches what.
    watcher = str(v["watchers"][0])
    rows = store.execute(
        f"MATCH (v:{LABEL})-[:watchers]->(a:{ANCHOR} {{dat_id: $w}}) RETURN v.dat_id AS d",
        {"w": watcher},
    ).all()
    assert [r["d"] for r in rows] == [str(dat_id)]


def test_an_entity_referenced_twice_has_one_anchor(store: DrevoStore, faker: Faker) -> None:
    owner = uuid.uuid4()
    for ts in (1, 2):
        values = {**part_values(faker), "owner": owner}

        def write(b: Any, v: dict[str, Any] = values) -> None:
            b.write_part(CONTRACT_PART, new_id(), v, created=None)

        commit(store, write, ts)
    rows = store.execute(
        f"MATCH (a:{ANCHOR} {{dat_id: $o}}) RETURN count(a) AS n", {"o": str(owner)}
    ).one()
    assert rows["n"] == 1


def test_erase_drops_anchors_nothing_else_points_to(store: DrevoStore, faker: Faker) -> None:
    erased, kept, shared = new_id(), new_id(), uuid.uuid4()
    gone = part_values(faker)
    gone["owner"] = shared
    commit(store, lambda b: b.write_part(CONTRACT_PART, erased, gone, created=None), 1 << 16)
    still = {**part_values(faker), "owner": shared}
    commit(store, lambda b: b.write_part(CONTRACT_PART, kept, still, created=None), 2 << 16)

    commit(store, lambda b: b.erase_part(CONTRACT_PART, erased), 3 << 16)

    left = anchors(store)
    assert str(shared) in left
    assert not {str(w) for w in gone["watchers"]} & left
    assert edges(store, kept) == sorted(
        [("owner", str(shared))] + [("watchers", str(w)) for w in still["watchers"]]
    )


def test_a_replaced_write_leaves_no_edges(store: DrevoStore, faker: Faker) -> None:
    dat_id, first, second = new_id(), part_values(faker), part_values(faker)

    def write(b: Any) -> None:
        b.write_part(CONTRACT_PART, dat_id, first, created=None)
        b.write_part(CONTRACT_PART, dat_id, second, created=None)

    commit(store, write, 1 << 16)
    assert edges(store, dat_id) == sorted(
        [("owner", str(second["owner"]))] + [("watchers", str(w)) for w in second["watchers"]]
    )
    assert str(first["owner"]) not in anchors(store)


def test_committed_reads_work_while_another_branch_is_prepared(
    store: DrevoStore, faker: Faker
) -> None:
    dat_id, v = new_id(), part_values(faker)
    commit(store, lambda b: b.write_part(CONTRACT_PART, dat_id, v, created=None), 1 << 16)
    txn = new_id()
    branch = store.branch(txn)
    branch.write_part(CONTRACT_PART, new_id(), part_values(faker), created=None)
    branch.stage(txn, 2 << 16)
    branch.prepare(gid_for(txn, store.name))
    try:
        got = store.read_part(CONTRACT_PART, dat_id)
        assert got is not None
        assert dict(got.values) == v
        assert len(store.part_history(CONTRACT_PART, dat_id)) == 1
        assert store.expired_parts(CONTRACT_PART, deleted_before=9 << 16, limit=5) == []
    finally:
        store.participant().abort(BranchRecord(store.name, gid_for(txn, store.name), True))
