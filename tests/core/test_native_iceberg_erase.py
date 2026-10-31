"""The Rust Iceberg participant erases what the Python facade wrote (copy-on-write).

The Python facade keeps an entity type in Iceberg (the reference's part table in
its own namespace). A meta-transaction committed in the PostgreSQL journal carries
an Iceberg branch whose payload erases two of the entities; native recovery
(``rhizodat_native.Recovery``) finishes it with the Rust participant, which
rewrites the data files without those versions and commits one ``overwrite``
snapshot through the REST catalog protocol. pyiceberg then reads the result: the
proof that a snapshot built by hand is the one the reference expects.

Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

# @entity adds dat_id and the service times at runtime; mypy cannot see them.
# mypy: disable-error-code="attr-defined,union-attr"

import contextlib
import time
import uuid
from collections.abc import Iterator

import pytest
from faker import Faker

from rhizodat import Rhizodat, entity
from rhizodat.adapters.iceberg import IcebergStore
from rhizodat.adapters.postgres import PostgresJournal
from rhizodat.core.coordinator import gid_for
from rhizodat.core.journal import BranchRecord, State
from rhizodat.core.parts import Part
from rhizodat.core.staged import StagedTable, encode_staged

native = pytest.importorskip("rhizodat_native")

HOUR_MS = 3_600_000


@entity(stores={"lake": ["summary", "points"]}, name="it.Task")
class Task:
    summary: str
    points: int


@pytest.fixture
def parts_namespace(iceberg_rest_dsn: str) -> Iterator[str]:
    """A namespace of its own for the part tables, dropped afterwards."""
    name = f"dat_erase_{uuid.uuid4().hex[:10]}"
    yield name
    store = IcebergStore(iceberg_rest_dsn, name="cleaner")
    try:
        with contextlib.suppress(Exception):  # best effort: a test namespace
            for ident in store.catalog.list_tables(name):
                store.catalog.purge_table(ident)
            store.catalog.drop_namespace(name)
    finally:
        store.close()


@pytest.fixture
def dat(
    pg_dsn: str, pg_schema: str, iceberg_rest_dsn: str, parts_namespace: str
) -> Iterator[Rhizodat]:
    d = Rhizodat(
        stores={
            "lake": IcebergStore(iceberg_rest_dsn, name="lake", parts_namespace=parts_namespace)
        },
        journal=PostgresJournal(pg_dsn, schema=pg_schema),
    )
    yield d
    d.close()


def later() -> int:
    """A wall clock an hour ahead, so every record is stale."""
    return time.time_ns() // 1_000_000 + HOUR_MS


def test_a_rust_participant_erases_what_the_python_facade_wrote(
    dat: Rhizodat,
    pg_dsn: str,
    pg_schema: str,
    iceberg_rest_dsn: str,
    parts_namespace: str,
    faker: Faker,
) -> None:
    tasks = dat.repository(Task)
    gone, rewritten, kept = (Task(summary=faker.sentence(), points=i) for i in range(3))
    for task in (gone, rewritten, kept):
        tasks.add(task)
    rewritten.points = 10
    tasks.update(rewritten)
    assert len(tasks.history(rewritten.dat_id)) == 2

    # A committed meta-transaction whose Iceberg branch erases `gone` and `rewritten`.
    table = f"{parts_namespace}.{Part.of(Task, 'lake').table}"
    erase = (str(gone.dat_id), str(rewritten.dat_id))
    payload = encode_staged([StagedTable(table, (), erase=erase)])
    txn: uuid.UUID = native.new_id()
    journal = dat.journal
    assert journal is not None
    journal.begin(txn, [BranchRecord("lake", gid_for(txn, "lake"), False, payload)])
    at = dat.clock.now().encode()
    journal.transition(txn, State.ACTIVE, State.PREPARED, dat_tx_time=at)
    journal.transition(txn, State.PREPARED, State.COMMITTED, dat_tx_time=at)

    recovery = native.Recovery(
        native.PostgresJournal(pg_dsn, pg_schema),
        [native.IcebergStore(iceberg_rest_dsn, "lake")],
        wall_ms=later,
    )
    assert recovery.run_once()["finished"] == [txn]

    # pyiceberg reads the snapshot the Rust participant built.
    assert tasks.history(gone.dat_id) == []
    assert tasks.history(rewritten.dat_id) == []
    assert tasks.get(kept.dat_id).points == kept.points
    lake = IcebergStore(iceberg_rest_dsn, name="reader")
    try:
        ice = lake.catalog.load_table(table)
        assert ice.scan().to_arrow().num_rows == 1
        summary = ice.current_snapshot().summary
        assert summary.operation.value == "overwrite"
        assert summary["dat.txn"] == str(txn)
    finally:
        lake.close()

    # Finishing again changes nothing: recovery marked it done.
    assert txn not in recovery.run_once()["finished"]
