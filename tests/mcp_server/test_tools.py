"""datorium-MCP tools without the MCP SDK: stores, read-only queries, the journal.

The tools are plain functions over a ``Datorium``; the server only exposes them.
Their results are JSON values, so every store value is converted.
"""

import base64
import datetime as dt
import decimal
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from datorium import Datorium
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, MemoryJournal, State
from datorium.core.result import Result
from datorium.mcp.config import ConfigError, config_from_env
from datorium.mcp.tools import Tools, jsonable
from fakes import FakeStoreHandle, Faults, World


class ReadableStore(FakeStoreHandle):
    """A fake store whose read-only reads return fixed rows and record the query."""

    def __init__(self, world: World, name: str, rows: list[dict[str, Any]]) -> None:
        super().__init__(world.store(name, two_phase=True))
        self.rows = rows
        self.reads: list[tuple[str, object]] = []

    def read_only(self, query: str, params: object = None) -> Result:
        self.reads.append((query, params))
        columns = tuple(self.rows[0]) if self.rows else ()
        return Result(iter(self.rows), columns=columns)


class Wall:
    def __init__(self) -> None:
        self.ms = 1_000_000

    def __call__(self) -> int:
        return self.ms


@pytest.fixture
def wall() -> Wall:
    return Wall()


@pytest.fixture
def journal(wall: Wall) -> MemoryJournal:
    return MemoryJournal(wall_ms=wall)


@pytest.fixture
def world() -> World:
    return World(faults=Faults())


def tools_for(
    stores: dict[str, Any], journal: MemoryJournal | None, wall: Wall, **kwargs: Any
) -> Tools:
    return Tools(Datorium(stores=stores, journal=journal), wall_ms=wall, **kwargs)


@pytest.fixture
def orders(world: World, faker: Faker) -> ReadableStore:
    rows = [{"id": i, "customer": faker.name()} for i in range(5)]
    return ReadableStore(world, "orders", rows)


@pytest.fixture
def tools(orders: ReadableStore, journal: MemoryJournal, wall: Wall) -> Iterator[Tools]:
    t = tools_for({"orders": orders}, journal, wall)
    yield t
    t.close()


# --- stores ----------------------------------------------------------------------------


def test_stores_lists_names_kinds_and_redacted_dsns(world: World, wall: Wall) -> None:
    store = ReadableStore(world, "erp", [])
    t = Tools(
        Datorium(stores={"erp": store}),
        dsns={"erp": "postgresql://app:s3cret@db.internal:5432/erp"},
        wall_ms=wall,
    )
    assert t.stores() == [
        {
            "name": "erp",
            "kind": "postgres",
            "two_phase": True,
            "read_only_queries": True,
            "dsn": "postgresql://app:***@db.internal:5432/erp",
        }
    ]


def test_stores_without_a_dsn_show_the_adapter(tools: Tools) -> None:
    [entry] = tools.stores()
    assert entry["kind"] == "ReadableStore"
    assert entry["dsn"] is None


# --- query -----------------------------------------------------------------------------


def test_query_reads_through_the_stores_read_only_access(
    tools: Tools, orders: ReadableStore
) -> None:
    out = tools.query("orders", "SELECT * FROM orders WHERE id > %s", [1], limit=10)
    assert orders.reads == [("SELECT * FROM orders WHERE id > %s", [1])]
    assert out["columns"] == ["id", "customer"]
    assert [r["id"] for r in out["rows"]] == [0, 1, 2, 3, 4]
    assert out["truncated"] is False


def test_query_stops_at_the_limit(tools: Tools) -> None:
    out = tools.query("orders", "q", limit=2)
    assert len(out["rows"]) == 2
    assert out["truncated"] is True


def test_query_limit_is_capped(orders: ReadableStore, journal: MemoryJournal, wall: Wall) -> None:
    t = tools_for({"orders": orders}, journal, wall, max_rows=3)
    out = t.query("orders", "q", limit=1000)
    assert (len(out["rows"]), out["truncated"]) == (3, True)
    with pytest.raises(ValueError, match="limit"):
        t.query("orders", "q", limit=0)


def test_query_refuses_a_store_without_read_only_access(
    world: World, journal: MemoryJournal, wall: Wall
) -> None:
    plain = FakeStoreHandle(world.store("feed", two_phase=False))
    t = tools_for({"feed": plain}, journal, wall)
    with pytest.raises(ValueError, match="read-only"):
        t.query("feed", "SELECT 1")
    assert t.stores()[0]["read_only_queries"] is False


def test_query_names_the_configured_stores_for_an_unknown_one(tools: Tools) -> None:
    with pytest.raises(KeyError, match="orders"):
        tools.query("nope", "SELECT 1")


def test_values_become_json(faker: Faker) -> None:
    some = uuid.uuid4()
    raw = faker.binary(length=8)
    at = faker.date_time(tzinfo=dt.UTC)
    row = {
        "id": some,
        "price": decimal.Decimal("12.3400"),
        "at": at,
        "day": at.date(),
        "time": at.timetz(),
        "took": dt.timedelta(seconds=90),
        "blob": raw,
        "tags": ("a", "b"),
        "set": frozenset({1}),
        "nested": {"x": [1.5, None, True]},
        "inf": float("inf"),
        "nan": float("nan"),
    }
    assert jsonable(row) == {
        "id": str(some),
        "price": "12.3400",
        "at": at.isoformat(),
        "day": at.date().isoformat(),
        "time": at.timetz().isoformat(),
        "took": 90.0,
        "blob": {"base64": base64.b64encode(raw).decode()},
        "tags": ["a", "b"],
        "set": [1],
        "nested": {"x": [1.5, None, True]},
        "inf": "inf",
        "nan": "nan",
    }


def test_unknown_objects_become_their_text() -> None:
    class Node:
        def __str__(self) -> str:
            return "(:Task {title: 'x'})"

    assert jsonable(Node()) == "(:Task {title: 'x'})"
    assert jsonable({1: "a"}) == {"1": "a"}


# --- journal ---------------------------------------------------------------------------


def branches(stores: list[str], txn: uuid.UUID) -> list[BranchRecord]:
    return [
        BranchRecord(s, f"dat:{txn}:{s}", two_phase=(s == "orders"), payload=b"abc") for s in stores
    ]


def test_transaction_shows_one_record(tools: Tools, journal: MemoryJournal, wall: Wall) -> None:
    txn = new_id()
    journal.begin(txn, branches(["orders", "feed"], txn))
    wall.ms += 500
    journal.transition(txn, State.ACTIVE, State.PREPARED, dat_tx_time=42)
    wall.ms += 250
    assert tools.transaction(str(txn)) == {
        "dat_txn": str(txn),
        "state": "PREPARED",
        "dat_tx_time": 42,
        "created_ms": 1_000_000,
        "updated_ms": 1_000_500,
        "age_ms": 250,
        "branches": [
            {"store": "orders", "gid": f"dat:{txn}:orders", "two_phase": True, "payload_bytes": 3},
            {"store": "feed", "gid": f"dat:{txn}:feed", "two_phase": False, "payload_bytes": 3},
        ],
    }


def test_transaction_unknown_or_invalid(tools: Tools) -> None:
    assert tools.transaction(str(new_id())) is None
    with pytest.raises(ValueError, match="dat_txn"):
        tools.transaction("not-a-uuid")


def test_pending_lists_unfinished_records_older_than_the_cutoff(
    tools: Tools, journal: MemoryJournal, wall: Wall
) -> None:
    old, decided, done, fresh = new_id(), new_id(), new_id(), new_id()
    for txn in (old, decided, done):
        journal.begin(txn, branches(["orders"], txn))
    journal.transition(decided, State.ACTIVE, State.PREPARED, dat_tx_time=7)
    journal.transition(decided, State.PREPARED, State.COMMITTED)
    journal.transition(done, State.ACTIVE, State.ABORTED)
    wall.ms += 60_000
    journal.begin(fresh, branches(["orders"], fresh))
    wall.ms += 1_000

    out = tools.pending_transactions(older_than_s=30)
    assert [(t["dat_txn"], t["state"]) for t in out["transactions"]] == [
        (str(old), "ACTIVE"),
        (str(decided), "COMMITTED"),
    ]
    assert out["transactions"][0]["age_ms"] == 61_000
    assert len(tools.pending_transactions()["transactions"]) == 3
    assert len(tools.pending_transactions(limit=1)["transactions"]) == 1


def test_journal_tools_need_a_journal(orders: ReadableStore, wall: Wall) -> None:
    t = tools_for({"orders": orders}, None, wall)
    with pytest.raises(ValueError, match="journal"):
        t.pending_transactions()
    with pytest.raises(ValueError, match="journal"):
        t.transaction(str(new_id()))


# --- configuration ---------------------------------------------------------------------


def test_config_from_the_environment(faker: Faker) -> None:
    pg = f"postgresql://{faker.user_name()}@db/erp"
    env = {
        "DATORIUM_STORE_ERP": pg,
        "DATORIUM_STORE_LAKE": "iceberg+rest://cat:8181/w",
        "DATORIUM_JOURNAL": "file:///tmp/j.db",
        "DATORIUM_MCP_MAX_ROWS": "50",
        "HOME": "/home/x",
    }
    config = config_from_env(env)
    assert config.stores == {"erp": pg, "lake": "iceberg+rest://cat:8181/w"}
    assert config.journal == "file:///tmp/j.db"
    assert config.max_rows == 50


def test_config_needs_a_store_and_valid_numbers() -> None:
    with pytest.raises(ConfigError, match="DATORIUM_STORE_"):
        config_from_env({"DATORIUM_JOURNAL": "file:///j.db"})
    with pytest.raises(ConfigError, match="DATORIUM_MCP_MAX_ROWS"):
        config_from_env({"DATORIUM_STORE_A": "clickhouse://h/db", "DATORIUM_MCP_MAX_ROWS": "x"})
    with pytest.raises(ConfigError, match="DATORIUM_MCP_MAX_ROWS"):
        config_from_env({"DATORIUM_STORE_A": "clickhouse://h/db", "DATORIUM_MCP_MAX_ROWS": "0"})


def test_config_store_names_are_lower_case() -> None:
    config = config_from_env({"DATORIUM_STORE_Big_Lake": "clickhouse://h/db"})
    assert config.stores == {"big_lake": "clickhouse://h/db"}
    assert config.journal is None


# --- recovery --------------------------------------------------------------------------


def test_recovery_aborts_stale_undecided_and_finishes_decided_records(
    world: World, journal: MemoryJournal, wall: Wall
) -> None:
    store = world.store("orders", two_phase=True)
    t = tools_for({"orders": FakeStoreHandle(store)}, journal, wall, stale_after_ms=30_000)
    undecided, decided = new_id(), new_id()
    for txn in (undecided, decided):
        gid = f"dat:{txn}:orders"
        store.prepared[gid] = (txn, ["row"])
        journal.begin(txn, [BranchRecord("orders", gid, two_phase=True)])
    journal.transition(decided, State.ACTIVE, State.PREPARED, dat_tx_time=7)
    journal.transition(decided, State.PREPARED, State.COMMITTED)

    assert t.run_recovery()["aborted"] == []  # not stale yet: a coordinator may be alive
    wall.ms += 30_001
    out = t.run_recovery()

    assert out == {
        "aborted": [str(undecided)],
        "finished": [str(decided)],
        "unfinished": [],
        "stuck": [],
        "orphans": [],
    }
    assert store.prepared == {}
    assert store.committed == {decided: ["row"]}
    assert t.transaction(str(decided))["state"] == "DONE"  # type: ignore[index]


def test_recovery_rolls_back_orphan_prepared_gids(
    world: World, journal: MemoryJournal, wall: Wall
) -> None:
    store = world.store("orders", two_phase=True)
    t = tools_for({"orders": FakeStoreHandle(store)}, journal, wall)
    orphan = f"dat:{new_id()}:orders"
    store.prepared[orphan] = (new_id(), ["row"])
    assert t.run_recovery()["orphans"] == [orphan]
    assert store.prepared == {}


def test_recovery_limits_and_needs_a_journal(
    world: World, journal: MemoryJournal, wall: Wall
) -> None:
    handle = FakeStoreHandle(world.store("orders", two_phase=True))
    for _ in range(3):
        journal.begin(new_id(), [])
    wall.ms += 60_000
    t = tools_for({"orders": handle}, journal, wall)
    assert len(t.run_recovery(limit=2)["aborted"]) == 2
    with pytest.raises(ValueError, match="limit"):
        t.run_recovery(limit=0)
    with pytest.raises(ValueError, match="journal"):
        tools_for({"orders": handle}, None, wall).run_recovery()
    with pytest.raises(ValueError, match="stale_after_ms"):
        tools_for({"orders": handle}, journal, wall, stale_after_ms=0)


def test_config_enables_recovery_explicitly() -> None:
    base = {"DATORIUM_STORE_A": "clickhouse://h/db"}
    assert config_from_env(base).recovery is False
    assert config_from_env(base).stale_after_s == 30
    for on in ("1", "true", "YES"):
        assert config_from_env({**base, "DATORIUM_MCP_RECOVERY": on}).recovery is True
    for off in ("0", "false", "no", ""):
        assert config_from_env({**base, "DATORIUM_MCP_RECOVERY": off}).recovery is False
    with pytest.raises(ConfigError, match="DATORIUM_MCP_RECOVERY"):
        config_from_env({**base, "DATORIUM_MCP_RECOVERY": "maybe"})
    config = config_from_env({**base, "DATORIUM_MCP_RECOVERY_STALE_S": "2.5"})
    assert config.stale_after_s == 2.5
    for bad in ("0", "-1", "x", "nan"):
        with pytest.raises(ConfigError, match="DATORIUM_MCP_RECOVERY_STALE_S"):
            config_from_env({**base, "DATORIUM_MCP_RECOVERY_STALE_S": bad})
