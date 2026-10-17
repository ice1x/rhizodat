"""rhizodat-MCP end to end in-process: an agent triages a bug through every entity tool,
and the ``rhizodat-mcp`` command refuses a bad environment before it serves.

The stdio test starts the server as a subprocess; this one calls the same tools
through an in-process client, and ``main()`` with its serving stubbed out.
"""

import base64
from typing import Any

import anyio
import pytest
from faker import Faker
from mcp.client.client import Client

from rhizodat import Rhizodat
from rhizodat.core.journal import MemoryJournal
from rhizodat.mcp import server as server_module
from rhizodat.mcp.server import build_server, main
from rhizodat.mcp.tools import Tools
from rhizodat.testing.memory import MemoryEntityStore


def tracker_tools() -> Tools:
    from mcp_tracker import Bug, Person

    dat = Rhizodat(
        stores={"pg": MemoryEntityStore("pg"), "graph": MemoryEntityStore("graph")},
        journal=MemoryJournal(),
    )
    return Tools(dat, entities=[Person, Bug], writes=True)


def bug(faker: Faker) -> dict[str, Any]:
    return {
        "title": faker.sentence(),
        "severity": 2,
        "estimate": "1.5",
        "reported": "2026-10-17T09:00:00+00:00",
        "due": None,
        "reporter": None,
        "watchers": [],
        "tags": ["ui"],
        "attachment": {"base64": base64.b64encode(b"log").decode()},
        "meta": {},
    }


def test_an_agent_triages_a_bug_through_every_tool(faker: Faker) -> None:
    tools = tracker_tools()

    async def go() -> dict[str, Any]:
        async with Client(build_server(tools)) as client:
            out: dict[str, Any] = {}
            out["types"] = await client.call_tool("entity_types", {})
            created = await client.call_tool(
                "create_entity", {"type": "tracker.Bug", "values": bug(faker)}
            )
            dat_id = created.structured_content["entity"]["dat_id"]
            out["updated"] = await client.call_tool(
                "update_entity",
                {"type": "tracker.Bug", "dat_id": dat_id, "values": {"severity": 1}},
            )
            out["history"] = await client.call_tool(
                "entity_history", {"type": "tracker.Bug", "dat_id": dat_id}
            )
            out["deleted"] = await client.call_tool(
                "delete_entity", {"type": "tracker.Bug", "dat_id": dat_id}
            )
            out["restored"] = await client.call_tool(
                "restore_entity", {"type": "tracker.Bug", "dat_id": dat_id}
            )
            out["unknown"] = await client.call_tool(
                "get_entity", {"type": "tracker.Nope", "dat_id": dat_id}
            )
            return out

    try:
        out = anyio.run(go)
    finally:
        tools.close()
    names = {t["name"] for t in out["types"].structured_content["result"]}
    assert names == {"tracker.Person", "tracker.Bug"}
    assert out["updated"].structured_content["entity"]["severity"] == 1
    assert [v["severity"] for v in out["history"].structured_content["result"]] == [2, 1]
    assert out["deleted"].structured_content["deleted"] == "soft"
    assert out["restored"].structured_content["entity"]["dat_deleted_at"] is None
    assert out["unknown"].is_error
    assert "tracker.Bug" in out["unknown"].content[0].text  # what is offered instead


# --- the command --------------------------------------------------------------------------


def test_main_refuses_a_bad_environment(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for key in [k for k in __import__("os").environ if k.startswith("RHIZODAT_")]:
        monkeypatch.delenv(key)
    with pytest.raises(SystemExit) as stopped:
        main()
    assert stopped.value.code == 2
    assert "no stores configured" in capsys.readouterr().err

    monkeypatch.setenv("RHIZODAT_STORE_PG", "postgresql://db/app")
    monkeypatch.setenv("RHIZODAT_MCP_ENTITIES", "no_such_module:Thing")
    with pytest.raises(SystemExit) as stopped:
        main()
    assert stopped.value.code == 2
    assert "RHIZODAT_MCP_ENTITIES" in capsys.readouterr().err


def test_main_serves_and_closes_the_stores(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    served: list[Tools] = []
    closed: list[bool] = []

    class Server:
        def __init__(self, tools: Tools) -> None:
            served.append(tools)

        def run(self) -> None:
            pass

    real_close = Tools.close

    def close(self: Tools) -> None:
        closed.append(True)
        real_close(self)

    monkeypatch.setattr(server_module, "build_server", Server)
    monkeypatch.setattr(Tools, "close", close)
    monkeypatch.setenv("RHIZODAT_STORE_MEM", f"file://{tmp_path / 'journal.db'}")
    for key in ("RHIZODAT_MCP_ENTITIES", "RHIZODAT_MCP_WRITES", "RHIZODAT_JOURNAL"):
        monkeypatch.delenv(key, raising=False)

    def open_in_memory(
        cls: type[Tools], stores: Any, /, *, journal: Any = None, **kw: Any
    ) -> Tools:
        return cls(Rhizodat(stores={"m": MemoryEntityStore("m")}), **kw)

    monkeypatch.setattr(Tools, "open", classmethod(open_in_memory))
    main()
    assert len(served) == 1
    assert closed == [True]
