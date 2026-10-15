"""A bug tracker's entities, importable by module path (``mcp_tracker:Bug``) for the
rhizodat-MCP entity tests: a bug split between two stores, its reporter in one."""

import datetime as dt
from decimal import Decimal

from rhizodat import Ref, entity


@entity(stores={"pg": ["name", "email"]}, name="tracker.Person")
class Person:
    name: str
    email: str


@entity(
    stores={
        "pg": ["title", "severity", "estimate", "reported", "due"],
        "graph": ["reporter", "watchers", "tags", "attachment", "meta"],
    },
    name="tracker.Bug",
    soft_delete=True,
)
class Bug:
    title: str
    severity: int
    estimate: Decimal
    reported: dt.datetime
    due: dt.date | None
    reporter: Ref[Person] | None
    watchers: list[Ref[Person]]
    tags: list[str]
    attachment: bytes | None
    meta: dict[str, object]


@entity(stores={"pg": ["hours"]}, name="tracker.Sla", valid_time=True)
class Sla:
    """A service level that holds for a period: answer within ``hours``."""

    hours: int
