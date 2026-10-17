"""rhizodat-MCP reads each field's JSON by its annotation, and names the field when it
does not fit.

An IT task manager's task has every kind of field an agent may send: flags,
numbers, ids, references, free-form metadata. Each wrong value is refused with
the field and what it expected; ``Any`` fields take the value as it is.
"""

import uuid
from typing import Any

import pytest

from rhizodat import Ref, entity
from rhizodat.core.mapping import mapping_of
from rhizodat.mcp.entities import describe, from_json, load_entity_types


@entity(stores={"pg": ["tags"]}, name="it.Team")
class Team:
    tags: list[str]


@entity(
    stores={"pg": ["done", "effort", "ticket", "owner", "team", "extra", "anything", "title"]},
    name="it.Task",
)
class Task:
    done: bool
    effort: float
    ticket: uuid.UUID
    owner: Ref  # type: ignore[type-arg]  # a reference without a declared target
    team: Ref[Team]
    extra: dict[str, Any]
    anything: Any
    title: str


MAPPING = mapping_of(Task)
TYPES = MAPPING.field_types()


def read(field: str, value: object) -> Any:
    return from_json(MAPPING, field, TYPES[field], value)


def test_values_that_fit_are_converted() -> None:
    ticket = uuid.uuid4()
    assert read("done", True) is True
    assert read("effort", 3) == 3.0
    assert isinstance(read("effort", 3), float)
    assert read("ticket", str(ticket)) == ticket
    assert read("owner", str(ticket)).dat_id == ticket
    assert read("extra", {"a": 1}) == {"a": 1}
    assert read("anything", [1, "x"]) == [1, "x"]
    assert read("anything", None) is None


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("done", "yes", "done: expected a boolean"),
        ("effort", True, "effort: expected a number"),
        ("effort", "3", "effort: expected a number"),
        ("ticket", "nope", "ticket: expected a uuid"),
        ("extra", [1], "extra: expected an object"),
        ("title", 5, "title: expected a string"),
        ("title", None, "title: null is not allowed"),
    ],
)
def test_values_that_do_not_fit_name_the_field(field: str, value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        read(field, value)


def test_descriptions_name_references_and_free_values() -> None:
    assert describe(MAPPING, "owner", TYPES["owner"]) == "reference to an entity"
    assert describe(MAPPING, "team", TYPES["team"]) == "reference to it.Team"
    assert describe(MAPPING, "anything", TYPES["anything"]) == "any JSON value"


def test_entity_lists_skip_blanks_and_need_module_and_class() -> None:
    assert load_entity_types(" , mcp_tracker:Person ,") == load_entity_types("mcp_tracker:Person")
    with pytest.raises(ValueError, match="module:Class"):
        load_entity_types("mcp_tracker.Person")


def test_bytes_and_decimals_refuse_what_they_cannot_read() -> None:
    from mcp_tracker import Bug

    bug = mapping_of(Bug)
    types = bug.field_types()
    with pytest.raises(ValueError, match="attachment: expected base64 text"):
        from_json(bug, "attachment", types["attachment"], {"base64": "not base64!"})
    with pytest.raises(ValueError, match="estimate: expected a decimal"):
        from_json(bug, "estimate", types["estimate"], 1.5)
