"""rhizodat-MCP tools without the MCP SDK: stores and read-only queries.

The tools are plain functions over a ``Rhizodat``; the server only exposes them.
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

from fakes import FakeStoreHandle, Faults, World
from rhizodat import Rhizodat
from rhizodat.core.journal import MemoryJournal
from rhizodat.core.result import Result
from rhizodat.mcp.config import ConfigError, config_from_env
from rhizodat.mcp.tools import Tools, jsonable


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


@pytest.fixture
def journal() -> MemoryJournal:
    return MemoryJournal()


@pytest.fixture
def world() -> World:
    return World(faults=Faults())


def tools_for(stores: dict[str, Any], journal: MemoryJournal | None, **kwargs: Any) -> Tools:
    return Tools(Rhizodat(stores=stores, journal=journal), **kwargs)


@pytest.fixture
def orders(world: World, faker: Faker) -> ReadableStore:
    rows = [{"id": i, "customer": faker.name()} for i in range(5)]
    return ReadableStore(world, "orders", rows)


@pytest.fixture
def tools(orders: ReadableStore, journal: MemoryJournal) -> Iterator[Tools]:
    t = tools_for({"orders": orders}, journal)
    yield t
    t.close()


# --- stores ----------------------------------------------------------------------------


def test_stores_lists_names_kinds_and_redacted_dsns(world: World) -> None:
    store = ReadableStore(world, "erp", [])
    t = Tools(
        Rhizodat(stores={"erp": store}),
        dsns={"erp": "postgresql://app:s3cret@db.internal:5432/erp"},
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


def test_query_limit_is_capped(orders: ReadableStore, journal: MemoryJournal) -> None:
    t = tools_for({"orders": orders}, journal, max_rows=3)
    out = t.query("orders", "q", limit=1000)
    assert (len(out["rows"]), out["truncated"]) == (3, True)
    with pytest.raises(ValueError, match="limit"):
        t.query("orders", "q", limit=0)


def test_query_refuses_a_store_without_read_only_access(
    world: World, journal: MemoryJournal
) -> None:
    plain = FakeStoreHandle(world.store("feed", two_phase=False))
    t = tools_for({"feed": plain}, journal)
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


# --- configuration ---------------------------------------------------------------------


def test_config_from_the_environment(faker: Faker) -> None:
    pg = f"postgresql://{faker.user_name()}@db/erp"
    env = {
        "RHIZODAT_STORE_ERP": pg,
        "RHIZODAT_STORE_LAKE": "iceberg+rest://cat:8181/w",
        "RHIZODAT_JOURNAL": "file:///tmp/j.db",
        "RHIZODAT_MCP_MAX_ROWS": "50",
        "HOME": "/home/x",
    }
    config = config_from_env(env)
    assert config.stores == {"erp": pg, "lake": "iceberg+rest://cat:8181/w"}
    assert config.journal == "file:///tmp/j.db"
    assert config.max_rows == 50


def test_config_needs_a_store_and_valid_numbers() -> None:
    with pytest.raises(ConfigError, match="RHIZODAT_STORE_"):
        config_from_env({"RHIZODAT_JOURNAL": "file:///j.db"})
    with pytest.raises(ConfigError, match="RHIZODAT_MCP_MAX_ROWS"):
        config_from_env({"RHIZODAT_STORE_A": "clickhouse://h/db", "RHIZODAT_MCP_MAX_ROWS": "x"})
    with pytest.raises(ConfigError, match="RHIZODAT_MCP_MAX_ROWS"):
        config_from_env({"RHIZODAT_STORE_A": "clickhouse://h/db", "RHIZODAT_MCP_MAX_ROWS": "0"})


def test_config_store_names_are_lower_case() -> None:
    config = config_from_env({"RHIZODAT_STORE_Big_Lake": "clickhouse://h/db"})
    assert config.stores == {"big_lake": "clickhouse://h/db"}
    assert config.journal is None


@pytest.mark.parametrize("removed", ["RHIZODAT_MCP_RECOVERY", "RHIZODAT_MCP_RECOVERY_STALE_S"])
def test_config_refuses_the_removed_recovery_settings(removed: str) -> None:
    """Transactions and recovery are rhizodat's own business, not the agent's."""
    with pytest.raises(ConfigError, match=f"{removed}.*removed"):
        config_from_env({"RHIZODAT_STORE_A": "clickhouse://h/db", removed: "1"})


def test_tools_offer_no_journal_or_recovery_access(tools: Tools) -> None:
    for gone in ("transaction", "pending_transactions", "run_recovery"):
        assert not hasattr(tools, gone), gone
