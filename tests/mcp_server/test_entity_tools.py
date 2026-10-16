"""datorium-MCP entity tools: CRUD over the Repository, one Unit of Work per call.

An agent manages a bug tracker: bugs split between two stores, people in one.
The tools take and return JSON; values are converted to and from each field's
type. Write tools exist only when writes are enabled.
"""

import base64
import datetime as dt
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from datorium import Datorium
from datorium.core.clock import HybridLogicalClock
from datorium.core.journal import MemoryJournal
from datorium.mcp.config import ConfigError, config_from_env
from datorium.mcp.entities import load_entity_types
from datorium.mcp.tools import Tools
from datorium.testing.memory import MemoryEntityStore
from mcp_tracker import Bug, Person


class Ticking:
    """A wall clock one second further on at every reading: versions never share a
    millisecond, so ``as_of`` a version's time is exactly that version."""

    def __init__(self) -> None:
        self.ms = int(dt.datetime(2026, 10, 15, 9, 0, tzinfo=dt.UTC).timestamp() * 1000)

    def __call__(self) -> int:
        self.ms += 1000
        return self.ms


def open_tools(*, writes: bool = True) -> Tools:
    wall = Ticking()
    dat = Datorium(
        stores={"pg": MemoryEntityStore("pg"), "graph": MemoryEntityStore("graph")},
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    return Tools(dat, entities=[Person, Bug], writes=writes)


@pytest.fixture
def tools() -> Iterator[Tools]:
    t = open_tools()
    yield t
    t.close()


def bug_json(faker: Faker, reporter: str | None = None) -> dict[str, Any]:
    return {
        "title": faker.sentence(),
        "severity": faker.random_int(1, 4),
        "estimate": "2.50",
        "reported": faker.date_time(tzinfo=dt.UTC).isoformat(),
        "due": faker.date_object().isoformat(),
        "reporter": reporter,
        "watchers": [],
        "tags": faker.words(2),
        "attachment": {"base64": base64.b64encode(faker.binary(8)).decode()},
        "meta": {"source": "triage"},
    }


def person(tools: Tools, faker: Faker) -> str:
    created = tools.create_entity("tracker.Person", {"name": faker.name(), "email": faker.email()})
    return str(created["entity"]["dat_id"])


# --- types ------------------------------------------------------------------------------


def test_entity_types_describe_fields_stores_and_delete_mode(tools: Tools) -> None:
    types = {t["name"]: t for t in tools.entity_types()}
    assert set(types) == {"tracker.Person", "tracker.Bug"}
    bug = types["tracker.Bug"]
    assert bug["stores"] == {
        "pg": ["title", "severity", "estimate", "reported", "due"],
        "graph": ["reporter", "watchers", "tags", "attachment", "meta"],
    }
    assert bug["fields"]["reporter"] == "reference to tracker.Person, or null"
    assert bug["fields"]["estimate"] == "decimal (as a string)"
    assert bug["fields"]["attachment"] == 'bytes ({"base64": ...}), or null'
    assert bug["delete"] == "soft"
    assert types["tracker.Person"]["delete"] == "hard"


# --- the CRUD cycle ---------------------------------------------------------------------


def test_an_agent_files_reads_and_triages_a_bug(tools: Tools, faker: Faker) -> None:
    reporter = person(tools, faker)
    values = bug_json(faker, reporter)

    created = tools.create_entity("tracker.Bug", values)
    bug = created["entity"]
    dat_id = bug["dat_id"]
    assert created["pending"] is False
    assert {k: bug[k] for k in values} == values
    assert bug["dat_created_at"] == bug["dat_updated_at"]

    watcher = person(tools, faker)
    updated = tools.update_entity(
        "tracker.Bug", dat_id, {"severity": 1, "watchers": [watcher], "due": None}
    )["entity"]
    assert (updated["severity"], updated["watchers"], updated["due"]) == (1, [watcher], None)
    assert updated["title"] == values["title"]  # fields not given are kept

    assert tools.get_entity("tracker.Bug", dat_id) == updated
    history = tools.entity_history("tracker.Bug", dat_id)
    assert [v["severity"] for v in history] == [values["severity"], 1]


def test_reads_as_of_a_moment(tools: Tools, faker: Faker) -> None:
    dat_id = tools.create_entity("tracker.Bug", bug_json(faker))["entity"]["dat_id"]
    first = tools.get_entity("tracker.Bug", dat_id)
    assert first is not None
    tools.update_entity("tracker.Bug", dat_id, {"severity": 4})

    then = tools.get_entity("tracker.Bug", dat_id, as_of=first["dat_updated_at"])
    assert then == first
    assert tools.get_entity("tracker.Bug", dat_id, as_of="2000-01-01T00:00:00+00:00") is None
    with pytest.raises(ValueError, match="time zone"):
        tools.get_entity("tracker.Bug", dat_id, as_of="2030-01-01T00:00:00")


def test_a_soft_deleted_bug_can_be_restored(tools: Tools, faker: Faker) -> None:
    dat_id = tools.create_entity("tracker.Bug", bug_json(faker))["entity"]["dat_id"]

    assert tools.delete_entity("tracker.Bug", dat_id) == {
        "dat_id": dat_id,
        "deleted": "soft",
        "pending": False,
    }
    assert tools.get_entity("tracker.Bug", dat_id) is None
    hidden = tools.get_entity("tracker.Bug", dat_id, include_deleted=True)
    assert hidden is not None
    assert hidden["dat_deleted_at"] is not None

    restored = tools.restore_entity("tracker.Bug", dat_id)["entity"]
    assert restored["dat_deleted_at"] is None
    assert tools.get_entity("tracker.Bug", dat_id) == restored


def test_a_person_is_erased_for_good(tools: Tools, faker: Faker) -> None:
    dat_id = person(tools, faker)
    assert tools.delete_entity("tracker.Person", dat_id)["deleted"] == "hard"
    assert tools.get_entity("tracker.Person", dat_id, include_deleted=True) is None
    assert tools.entity_history("tracker.Person", dat_id) == []


# --- what is refused --------------------------------------------------------------------


def test_unknown_types_and_fields_are_refused_with_what_exists(tools: Tools, faker: Faker) -> None:
    with pytest.raises(KeyError, match=r"tracker\.Bug"):
        tools.get_entity("tracker.Nope", str(uuid.uuid4()))
    with pytest.raises(ValueError, match="no field"):
        tools.create_entity("tracker.Person", {"name": faker.name(), "nick": "x"})
    with pytest.raises(ValueError, match="email"):
        tools.create_entity("tracker.Person", {"name": faker.name()})  # missing field
    with pytest.raises(ValueError, match="service"):
        tools.create_entity("tracker.Person", {"name": "a", "email": "b", "dat_id": "x"})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("severity", "high", "severity: expected an integer"),
        ("severity", True, "severity: expected an integer"),
        ("estimate", "lots", "estimate: expected a decimal"),
        ("reported", "yesterday", "reported: expected an ISO 8601"),
        ("reporter", "not-a-uuid", "reporter: expected a dat_id"),
        ("watchers", "one", "watchers: expected a list"),
        ("attachment", "raw", "attachment: expected"),
    ],
)
def test_values_that_do_not_fit_the_field_are_refused(
    tools: Tools, faker: Faker, field: str, value: object, message: str
) -> None:
    values = {**bug_json(faker), field: value}
    with pytest.raises(ValueError, match=message):
        tools.create_entity("tracker.Bug", values)


def test_updating_or_restoring_a_missing_entity_says_so(tools: Tools) -> None:
    missing = str(uuid.uuid4())
    with pytest.raises(LookupError, match=missing):
        tools.update_entity("tracker.Bug", missing, {"severity": 1})
    with pytest.raises(LookupError):
        tools.restore_entity("tracker.Bug", missing)


def test_without_writes_enabled_only_reads_work(faker: Faker) -> None:
    t = open_tools(writes=False)
    try:
        assert t.writes is False
        with pytest.raises(PermissionError, match="DATORIUM_MCP_WRITES"):
            t.create_entity("tracker.Person", {"name": faker.name(), "email": faker.email()})
        assert t.get_entity("tracker.Person", str(uuid.uuid4())) is None
    finally:
        t.close()


# --- configuration ----------------------------------------------------------------------


def test_entity_types_load_from_module_paths() -> None:
    assert load_entity_types("mcp_tracker:Person, mcp_tracker:Bug") == [Person, Bug]
    with pytest.raises(ValueError, match="not an @entity"):
        load_entity_types("mcp_tracker:entity")
    with pytest.raises(ValueError, match="no_such_module"):
        load_entity_types("no_such_module:Thing")


def test_the_environment_names_entities_and_enables_writes() -> None:
    env = {
        "DATORIUM_STORE_PG": "postgresql://db/app",
        "DATORIUM_MCP_ENTITIES": "mcp_tracker:Person,mcp_tracker:Bug",
        "DATORIUM_MCP_WRITES": "1",
    }
    config = config_from_env(env)
    assert config.entities == ("mcp_tracker:Person", "mcp_tracker:Bug")
    assert config.writes is True
    assert config_from_env({"DATORIUM_STORE_PG": "postgresql://db/app"}).writes is False
    with pytest.raises(ConfigError, match="DATORIUM_MCP_WRITES"):
        config_from_env({**env, "DATORIUM_MCP_WRITES": "maybe"})


# --- valid time -------------------------------------------------------------------------


def test_an_agent_reads_and_writes_validity_periods(faker: Faker) -> None:
    from mcp_tracker import Sla

    wall = Ticking()
    dat = Datorium(
        stores={"pg": MemoryEntityStore("pg"), "graph": MemoryEntityStore("graph")},
        journal=MemoryJournal(wall_ms=wall),
        clock=HybridLogicalClock(wall_ms=wall),
    )
    t = Tools(dat, entities=[Sla], writes=True)
    try:
        [described] = t.entity_types()
        assert described["valid_time"] is True
        assert (
            described["fields"]["valid_from"] == "date-time (ISO 8601, with a time zone), or null"
        )
        start, end = "2026-01-01T00:00:00+00:00", "2026-07-01T00:00:00+00:00"
        created = t.create_entity(
            "tracker.Sla",
            {"hours": 24, "valid_from": start, "valid_to": end},
        )["entity"]
        assert (created["valid_from"], created["valid_to"]) == (start, end)
        dat_id = created["dat_id"]
        assert t.get_entity("tracker.Sla", dat_id, as_of="2026-03-01T00:00:00+00:00") is not None
        assert t.get_entity("tracker.Sla", dat_id, as_of="2026-08-01T00:00:00+00:00") is None
        with pytest.raises(ValueError, match="valid_from must be before valid_to"):
            t.update_entity("tracker.Sla", dat_id, {"valid_from": end, "valid_to": start})
    finally:
        t.close()
