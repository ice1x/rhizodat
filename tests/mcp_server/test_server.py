"""datorium-MCP as an MCP server: in process through the SDK's client, and as the
``datorium-mcp`` command over stdio against the compose PostgreSQL."""

import importlib.metadata
import json
import sys
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
from datorium.core.journal import MemoryJournal
from datorium.core.result import Result
from datorium.mcp.server import build_server
from datorium.mcp.tools import Tools
from fakes import FakeStoreHandle, World

TOOLS = {"stores", "query"}


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


def test_the_server_never_exposes_transactions_or_recovery(tools: Tools) -> None:
    async def go() -> Any:
        async with Client(build_server(tools)) as client:
            return await client.list_tools()

    names = {t.name for t in anyio.run(go).tools}
    assert not names & {"transaction", "pending_transactions", "run_recovery"}
    assert "transaction" not in (build_server(tools).instructions or "").lower()


def test_the_query_tool_describes_the_configured_kinds() -> None:
    """Each adapter tells clients its native query language; only configured kinds show."""
    store = Readable(World().store("erp", two_phase=True))
    t = Tools(Datorium(stores={"erp": store}), dsns={"erp": "postgresql://db/erp"})

    async def go() -> Any:
        async with Client(build_server(t)) as client:
            return await client.list_tools()

    [query] = [x for x in anyio.run(go).tools if x.name == "query"]
    assert query.description is not None
    assert "postgres: SQL with %s" in query.description
    assert "Cypher" not in query.description
    t.close()


def test_tools_return_structured_results(tools: Tools) -> None:
    result = call(tools, "query", {"store": "orders", "query": "SELECT 1"})
    assert not result.is_error
    assert result.structured_content["rows"] == [{"q": "SELECT 1"}]
    stores = call(tools, "stores")
    assert [s["name"] for s in stores.structured_content["result"]] == ["orders"]


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

    async def go() -> tuple[Any, Any, Any]:
        async with Client(server) as client:
            stores = await client.call_tool("stores", {})
            read = await client.call_tool(
                "query", {"store": "crm", "query": "SELECT name FROM customers"}
            )
            write = await client.call_tool(
                "query", {"store": "crm", "query": "DELETE FROM customers"}
            )
            return stores, read, write

    stores, read, write = anyio.run(go)
    [entry] = stores.structured_content["result"]
    assert entry["kind"] == "postgres"
    assert "datorium@" not in json.dumps(entry)  # the password is masked
    assert read.structured_content["rows"] == [{"name": customer}]
    assert write.is_error
    assert "read-only transaction" in write.content[0].text
    check = PostgresStore(pg_dsn, schema=pg_schema)
    assert check.execute("SELECT count(*) AS n FROM customers").one()["n"] == 1
    check.close()
