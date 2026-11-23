"""Teardown rolls back only the prepared branches its own test opened.

The drevo and PostgreSQL test servers are shared with other test processes
(another pytest run, ``cargo test``); their prepared ``dat:`` gids must survive.
"""

import uuid

import psycopg
import pytest
from faker import Faker

from fakes import FakeStore, FakeStoreHandle, Faults
from own_prepared import OwnBranches, release_schemas
from rhizodat.adapters.postgres import PostgresStore
from rhizodat.core.coordinator import gid_for
from rhizodat.core.identity import new_id


def prepare(store: FakeStore, dat_txn: uuid.UUID, gid: str) -> None:
    store.prepared[gid] = (dat_txn, ["row"])


@pytest.fixture
def fake(faker: Faker) -> FakeStore:
    return FakeStore(name=faker.word(), two_phase=True, faults=Faults())


def test_only_gids_of_tracked_branches_are_aborted(fake: FakeStore) -> None:
    own = OwnBranches()
    handle = own.track(FakeStoreHandle(fake))
    mine = new_id()
    handle.branch(mine)
    theirs, other_store = new_id(), new_id()
    prepare(fake, mine, gid_for(mine, fake.name))
    prepare(fake, theirs, gid_for(theirs, fake.name))  # another process, same store name
    prepare(fake, other_store, gid_for(mine, "elsewhere"))  # our txn, another store
    prepare(fake, theirs, "foreign-xid")

    participant = handle.participant()
    assert own.prepared(handle.name, participant) == [gid_for(mine, fake.name)]
    own.abort_prepared(handle.name, participant)

    assert sorted(fake.prepared) == sorted(
        [gid_for(theirs, fake.name), gid_for(mine, "elsewhere"), "foreign-xid"]
    )


def test_a_failing_abort_does_not_stop_the_teardown(fake: FakeStore) -> None:
    own = OwnBranches()
    handle = own.track(FakeStoreHandle(fake))
    first, second = new_id(), new_id()
    for txn in (first, second):
        handle.branch(txn)
        prepare(fake, txn, gid_for(txn, fake.name))
    fake.faults.add(f"{fake.name}.abort", RuntimeError("abort lost"))

    own.abort_prepared(handle.name, handle.participant())

    assert len(fake.prepared) == 1  # the first abort failed, the second still ran


def test_noted_txns_count_as_own(fake: FakeStore) -> None:
    own = OwnBranches()
    txn = new_id()
    own.note(txn)
    prepare(fake, txn, gid_for(txn, fake.name))
    handle = FakeStoreHandle(fake)
    own.abort_prepared(handle.name, handle.participant())
    assert fake.prepared == {}


def test_release_schemas_rolls_back_only_branches_in_those_schemas(
    pg_dsn: str, pg_schema: str
) -> None:
    foreign_schema = f"dat_foreign_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{foreign_schema}"')
    mine = PostgresStore(pg_dsn, name="pg", schema=pg_schema)
    theirs = PostgresStore(pg_dsn, name="pg", schema=foreign_schema)
    try:
        gids = {}
        for schema, store in ((pg_schema, mine), (foreign_schema, theirs)):
            store.install()
            txn = new_id()
            branch = store.branch(txn)
            branch.stage(txn, 1)
            gids[schema] = gid_for(txn, "pg")
            branch.prepare(gids[schema])

        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            release_schemas(conn, [pg_schema])

        listed = mine.participant().prepared_gids()
        assert gids[pg_schema] not in listed
        assert gids[foreign_schema] in listed
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            release_schemas(conn, [foreign_schema])
            conn.execute(f'DROP SCHEMA "{foreign_schema}" CASCADE')
        mine.close()
        theirs.close()
