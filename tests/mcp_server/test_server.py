"""datorium-MCP as an MCP server: in process through the SDK's client, and as the
``datorium-mcp`` command over stdio against the compose PostgreSQL."""

import importlib.metadata
import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import anyio
import pytest
from faker import Faker
from mcp import Client
from mcp.client.stdio import StdioServerParameters

from datorium import Datorium
from datorium.adapters.postgres import PostgresStore
from datorium.core.coordinator import gid_for
from datorium.core.identity import new_id
from datorium.core.journal import BranchRecord, MemoryJournal, State, open_journal
from datorium.core.result import Result
from datorium.mcp.server import build_server
from datorium.mcp.tools import Tools
from fakes import FakeStoreHandle, World

TOOLS = {"stores", "query", "transaction", "pending_transactions"}


class Readable(FakeStoreHandle):
    def read_only(self, query: str, params: object = None) -> Result:
        if query == "boom":
            raise RuntimeError("syntax error at or near boom")
        return Result(iter([{"q": query}]), columns=("q",))


@pytest.fixture
def tools() -> Iterator[Tools]:
    store = Readable(World().store("orders", two_phase=True))
    t = Tools(Datorium(stores={"orders": store}, journal=MemoryJournal()))
    yield t
    t.close()


def call(tools: Tools, name: str, arguments: dict[str, Any] | None = None) -> Any:
    async def go() -> Any:
        async with Client(build_server(tools)) as client:
            return await client.call_tool(name, arguments or {})

    return anyio.run(go)


def test_every_tool_is_listed_as_read_only(tools: Tools) -> None:
    async def go() -> Any:
        async with Client(build_server(tools)) as client:
            return await client.list_tools()

    listed = anyio.run(go).tools
    assert {t.name for t in listed} == TOOLS
    for t in listed:
        assert t.annotations is not None
        assert t.annotations.read_only_hint is True, t.name
        assert t.description, t.name


def test_recovery_is_offered_only_when_enabled(tools: Tools) -> None:
    async def go() -> Any:
        async with Client(build_server(tools, recovery=True)) as client:
            listed = await client.list_tools()
            ran = await client.call_tool("run_recovery", {})
            return listed, ran

    listed, ran = anyio.run(go)
    [tool] = [t for t in listed.tools if t.name == "run_recovery"]
    assert {t.name for t in listed.tools} == TOOLS | {"run_recovery"}
    assert tool.annotations is not None
    assert tool.annotations.read_only_hint is False
    assert tool.annotations.destructive_hint is True
    assert tool.annotations.idempotent_hint is True
    assert not ran.is_error
    assert ran.structured_content["aborted"] == []


def test_tools_return_structured_results(tools: Tools) -> None:
    result = call(tools, "query", {"store": "orders", "query": "SELECT 1"})
    assert not result.is_error
    assert result.structured_content["rows"] == [{"q": "SELECT 1"}]
    stores = call(tools, "stores")
    assert [s["name"] for s in stores.structured_content["result"]] == ["orders"]
    missing = call(tools, "transaction", {"dat_txn": "0199e0a4-6f39-7cc1-8a3a-5a2b6d8f1c00"})
    assert missing.structured_content == {"result": None}


def test_store_errors_reach_the_client_as_tool_errors(tools: Tools) -> None:
    result = call(tools, "query", {"store": "orders", "query": "boom"})
    assert result.is_error
    assert "syntax error at or near boom" in result.content[0].text
    result = call(tools, "query", {"store": "nope", "query": "x"})
    assert result.is_error
    assert "orders" in result.content[0].text


def test_the_command_is_installed() -> None:
    scripts = importlib.metadata.entry_points(group="console_scripts")
    assert any(s.name == "datorium-mcp" and s.value == "datorium.mcp.server:main" for s in scripts)


def test_the_command_refuses_to_start_without_stores() -> None:
    import subprocess

    out = subprocess.run(
        [sys.executable, "-m", "datorium.mcp"],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin"},
        check=False,
    )
    assert out.returncode == 2
    assert "DATORIUM_STORE_" in out.stderr


# --- over stdio, against PostgreSQL --------------------------------------------------


def test_stdio_server_reads_postgres_and_refuses_writes(
    pg_dsn: str, pg_schema: str, tmp_path: Path, faker: Faker
) -> None:
    store = PostgresStore(pg_dsn, schema=pg_schema)
    customer = faker.name()
    store.execute("CREATE TABLE customers (name text)")
    store.execute("INSERT INTO customers VALUES (%s)", (customer,))
    store.close()
    dsn = f"{pg_dsn}?schema={pg_schema}"
    env = {
        "PATH": "/usr/bin:/bin",
        "DATORIUM_STORE_CRM": dsn,
        "DATORIUM_JOURNAL": f"file://{tmp_path / 'journal.db'}",
    }
    server = StdioServerParameters(command=sys.executable, args=["-m", "datorium.mcp"], env=env)

    async def go() -> tuple[Any, Any, Any, Any]:
        async with Client(server) as client:
            stores = await client.call_tool("stores", {})
            read = await client.call_tool(
                "query", {"store": "crm", "query": "SELECT name FROM customers"}
            )
            write = await client.call_tool(
                "query", {"store": "crm", "query": "DELETE FROM customers"}
            )
            pending = await client.call_tool("pending_transactions", {})
            return stores, read, write, pending

    stores, read, write, pending = anyio.run(go)
    [entry] = stores.structured_content["result"]
    assert entry["kind"] == "postgres"
    assert "datorium@" not in json.dumps(entry)  # the password is masked
    assert read.structured_content["rows"] == [{"name": customer}]
    assert write.is_error
    assert "read-only transaction" in write.content[0].text
    assert pending.structured_content["transactions"] == []
    check = PostgresStore(pg_dsn, schema=pg_schema)
    assert check.execute("SELECT count(*) AS n FROM customers").one()["n"] == 1
    check.close()


def test_stdio_recovery_cleans_up_after_a_crashed_erp_coordinator(
    pg_dsn: str, pg_schema: str, tmp_path: Path, faker: Faker
) -> None:
    """A coordinator died mid-commit: one order undecided, one decided but unfinished.
    An operator's agent runs recovery through datorium-MCP: the first is rolled back,
    the second committed, and the journal says so."""
    name = f"crm_{faker.pystr(min_chars=8, max_chars=8).lower()}"
    store = PostgresStore(pg_dsn, name=name, schema=pg_schema)
    store.install()
    store.execute("CREATE TABLE orders (dat_id uuid PRIMARY KEY, customer text)")
    journal_dsn = f"file://{tmp_path / 'journal.db'}"
    journal = open_journal(journal_dsn)
    lost, decided = new_id(), new_id()
    for txn in (lost, decided):
        branch = store.branch(txn)
        branch.execute("INSERT INTO orders VALUES (%s, %s)", (txn, faker.name()))
        branch.stage(txn, 1)
        journal.begin(txn, [BranchRecord(name, gid_for(txn, name), two_phase=True)])
        branch.prepare(gid_for(txn, name))
    journal.transition(decided, State.ACTIVE, State.PREPARED, dat_tx_time=1)
    journal.transition(decided, State.PREPARED, State.COMMITTED)
    journal.close()
    time.sleep(0.05)
    env = {
        "PATH": "/usr/bin:/bin",
        f"DATORIUM_STORE_{name.upper()}": f"{pg_dsn}?schema={pg_schema}",
        "DATORIUM_JOURNAL": journal_dsn,
        "DATORIUM_MCP_RECOVERY": "1",
        "DATORIUM_MCP_RECOVERY_STALE_S": "0.01",
    }
    server = StdioServerParameters(command=sys.executable, args=["-m", "datorium.mcp"], env=env)

    async def go() -> tuple[Any, Any, Any]:
        async with Client(server) as client:
            pending = await client.call_tool("pending_transactions", {})
            ran = await client.call_tool("run_recovery", {})
            after = await client.call_tool("transaction", {"dat_txn": str(decided)})
            return pending, ran, after

    pending, ran, after = anyio.run(go)
    assert len(pending.structured_content["transactions"]) == 2
    assert not ran.is_error, ran.content
    assert ran.structured_content["aborted"] == [str(lost)]
    assert ran.structured_content["finished"] == [str(decided)]
    assert after.structured_content["result"]["state"] == "DONE"
    rows = store.execute("SELECT dat_id FROM orders").all()
    assert [r["dat_id"] for r in rows] == [decided]
    assert not [g for g in store.participant().prepared_gids() if g.endswith(f":{name}")]
    store.close()
