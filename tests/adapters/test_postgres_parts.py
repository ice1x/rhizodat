"""PostgreSQL as a home for entity parts: typed tables. The behaviour is the parts
contract (tests/contract/test_parts_contracts.py)."""

# @entity adds dat_id at runtime; mypy cannot see it.
# mypy: disable-error-code="attr-defined"

from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal

import pytest

from rhizodat.adapters.postgres import PostgresStore
from rhizodat.core.mapping import Ref, entity
from rhizodat.core.parts import Part


@entity(
    stores={
        "pg": [
            "title",
            "points",
            "estimate",
            "due",
            "done",
            "opened",
            "owner",
            "tags",
            "blob",
            "ratio",
            "watchers",
            "meta",
        ]
    },
    name="tracker.Issue",
)
class Issue:
    title: str
    points: int
    estimate: Decimal
    due: date | None
    done: bool
    opened: datetime
    owner: Ref["Issue"] | None
    tags: list[str]
    blob: bytes
    ratio: float
    watchers: list[Ref["Issue"]]
    meta: dict[str, object]


PART = Part.of(Issue, "pg")


@pytest.fixture
def store(pg_dsn: str, pg_schema: str) -> Iterator[PostgresStore]:
    s = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    s.install()
    s.install_part(PART)
    yield s
    s.close()


def test_a_part_table_has_typed_columns(store: PostgresStore, pg_schema: str) -> None:
    assert PART.table == "tracker_issue"
    rows = store.execute(
        "SELECT column_name, data_type FROM information_schema.columns"
        " WHERE table_schema = %s AND table_name = %s",
        (pg_schema, PART.table),
    ).all()
    types = {r["column_name"]: r["data_type"] for r in rows}
    assert types == {
        "dat_id": "uuid",
        "dat_txn": "uuid",
        "dat_tx_time": "bigint",
        "dat_created": "bigint",
        "dat_deleted": "boolean",
        "title": "text",
        "points": "bigint",
        "estimate": "numeric",
        "due": "date",
        "done": "boolean",
        "opened": "timestamp with time zone",
        "owner": "uuid",
        "tags": "jsonb",
        "blob": "bytea",
        "ratio": "double precision",
        "watchers": "jsonb",
        "meta": "jsonb",
    }


def test_install_is_idempotent(store: PostgresStore, pg_dsn: str, pg_schema: str) -> None:
    store.install_part(PART)
    other = PostgresStore(pg_dsn, name="pg", schema=pg_schema)  # its own install cache
    other.install_part(PART)
    other.close()
