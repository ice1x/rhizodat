"""The native coordinator over the real Rust drevo adapter (plan step 5).

The real Rust branch, participant and journal of ``crates/rhizodat-drevo`` behind
``rhizodat_native.Coordinator``, over Bolt: a Unit of Work of native stores runs
the whole commit protocol through the core against the compose drevo, with no
Python callbacks on the path. A single store commits one-phase; a drevo store and
a PostgreSQL store commit together as one meta-transaction through a journal,
which is also the first cross-adapter native Unit of Work.

A Unit of Work's own graph writes reach a store through ``store.unit()``: buffer
parameterized Cypher with ``unit.run(cypher, params)`` and hand the unit to
``commit`` like a store, so the statements run inside the branch's transaction
and commit atomically with the meta-transaction. Each statement gets ``dat_txn``
and ``dat_tx_time`` parameters injected, so it may stamp its nodes with
``$dat_txn``.

Install the module with ``uv sync --group native`` (needs a Rust toolchain) and
point the tests at a disposable drevo server (2PC raises a prepared fence).
"""

import contextlib
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from faker import Faker

native = pytest.importorskip("rhizodat_native")


@pytest.fixture(autouse=True)
def _unfence(drevo_bolt_dsn: str) -> Iterator[None]:
    """Best-effort: after each test, roll back any prepared branch left on the
    shared graph, so a failure never fences the graph for the next test."""
    yield
    with contextlib.suppress(Exception):
        from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore
        from rhizodat.core.journal import BranchRecord

        store = PyDrevoStore(drevo_bolt_dsn, name="graph")
        try:
            participant = store.participant()
            for gid in participant.prepared_gids():
                if gid.startswith("dat:"):
                    participant.abort(BranchRecord("graph", gid, True))
        finally:
            store.close()


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh PostgreSQL schemas, dropped afterwards."""
    created: list[str] = []

    def make() -> str:
        name = f"dat_native_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{name}"')
        created.append(name)
        return name

    yield make

    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
            " AND state LIKE 'idle in transaction%%'"
        )
        conn.execute("SET lock_timeout = '10s'")
        for (gid,) in conn.execute(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchall():
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
        for name in created:
            conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def _store_name() -> str:
    """A store name unique to the test, so branches never share a gid on Bolt."""
    return f"g{uuid.uuid4().hex[:10]}"


@pytest.fixture
def py_graph(drevo_bolt_dsn: str) -> Iterator[Any]:
    """A Python drevo store for read-back, cleaning up labels it was asked about."""
    from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore

    store = PyDrevoStore(drevo_bolt_dsn, name="reader")
    labels: list[str] = []

    class Reader:
        def label(self) -> str:
            lbl = f"T{uuid.uuid4().hex[:10]}"
            labels.append(lbl)
            return lbl

        def count(self, cypher: str, params: dict[str, Any] | None = None) -> int:
            return int(store.execute(cypher, params).one()["n"])

    try:
        yield Reader()
    finally:
        for lbl in labels:
            with contextlib.suppress(Exception):
                store.execute(f"MATCH (n:{lbl}) DETACH DELETE n")
        store.close()


# --- a single native drevo store -------------------------------------------------------


def test_single_drevo_store_commits_natively(drevo_bolt_dsn: str) -> None:
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [store])
    assert outcome["status"] == "committed"
    assert outcome["dat_txn"] == txn
    assert isinstance(outcome["dat_tx_time"], int)


def test_a_replay_of_a_committed_txn_is_idempotent(drevo_bolt_dsn: str) -> None:
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    txn = native.new_id()
    first = native.Coordinator().commit(txn, [store])
    assert first["status"] == "committed"
    # The same dat_txn again: the marker node is already there, so the re-run sees
    # the branch applied and succeeds without writing it twice.
    second = native.Coordinator().commit(txn, [store])
    assert second["status"] == "committed"


# --- a drevo store and a PostgreSQL store, one meta-transaction -------------------------


def test_drevo_and_postgres_commit_through_a_journal(
    drevo_bolt_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    graph = native.DrevoStore(drevo_bolt_dsn, _store_name())
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [graph, pg])
    assert outcome["status"] == "committed"
    # The PostgreSQL branch's marker proves it committed; the journal proves both.
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(
            f'SELECT 1 FROM "{pg_schema}".dat_applied WHERE dat_txn = %s', (txn,)
        ).fetchall()
    assert rows
    record = journal.get(txn)
    assert record["state"] == "DONE"
    assert record["dat_txn"] == txn


def test_a_meta_transaction_needs_a_journal(
    drevo_bolt_dsn: str, pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    graph = native.DrevoStore(drevo_bolt_dsn, _store_name())
    pg = native.PostgresStore(pg_dsn, "pg", make_schema())
    pg.install()
    with pytest.raises(native.InvalidUnitOfWork):
        native.Coordinator().commit(native.new_id(), [graph, pg])


# --- the Unit of Work's graph writes across the boundary --------------------------------


def test_a_unit_writes_its_cypher_stamped_with_the_meta_transaction(
    drevo_bolt_dsn: str, py_graph: Any
) -> None:
    fake = Faker()
    Faker.seed(11)
    label = py_graph.label()
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    unit = store.unit()
    create = f"CREATE (:{label} {{title: $title, dat_txn: $dat_txn}})"
    titles = [fake.sentence(nb_words=3) for _ in range(3)]
    for title in titles:
        unit.run(create, {"title": title})
    txn = native.new_id()
    outcome = native.Coordinator().commit(txn, [unit])
    assert outcome["status"] == "committed"
    assert py_graph.count(f"MATCH (n:{label}) RETURN count(n) AS n") == 3
    # Every node carries the meta-transaction's stamp (injected $dat_txn).
    stamped = py_graph.count(
        f"MATCH (n:{label} {{dat_txn: $t}}) RETURN count(n) AS n",
        {"t": str(txn)},
    )
    assert stamped == 3


def test_a_unit_and_postgres_write_together_through_a_journal(
    drevo_bolt_dsn: str, pg_dsn: str, make_schema: Callable[[], str], py_graph: Any
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    label = py_graph.label()
    graph = native.DrevoStore(drevo_bolt_dsn, _store_name())
    unit = graph.unit()
    unit.run(f"CREATE (:{label} {{name: $name, dat_txn: $dat_txn}})", {"name": "alice"})
    pg = native.PostgresStore(pg_dsn, "pg", pg_schema)
    pg.install()
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    txn = native.new_id()
    outcome = native.Coordinator(journal).commit(txn, [unit, pg])
    assert outcome["status"] == "committed"
    assert py_graph.count(f"MATCH (n:{label}) RETURN count(n) AS n") == 1
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        rows = conn.execute(
            f'SELECT 1 FROM "{pg_schema}".dat_applied WHERE dat_txn = %s', (txn,)
        ).fetchall()
    assert rows
    assert journal.get(txn)["state"] == "DONE"


def test_a_unit_is_reusable_across_meta_transactions(drevo_bolt_dsn: str, py_graph: Any) -> None:
    label = py_graph.label()
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    unit = store.unit()
    unit.run(f"CREATE (:{label} {{n: $n, dat_txn: $dat_txn}})", {"n": 1})
    # The unit keeps its writes across commits (its buffer is cloned on open), so
    # committing it under two different meta-transactions writes a node for each,
    # stamped with that meta-transaction.
    txn_a, txn_b = native.new_id(), native.new_id()
    assert native.Coordinator().commit(txn_a, [unit])["status"] == "committed"
    assert native.Coordinator().commit(txn_b, [unit])["status"] == "committed"
    assert py_graph.count(f"MATCH (n:{label}) RETURN count(n) AS n") == 2
    for txn in (txn_a, txn_b):
        stamped = py_graph.count(
            f"MATCH (n:{label} {{dat_txn: $t}}) RETURN count(n) AS n", {"t": str(txn)}
        )
        assert stamped == 1


def test_a_reserved_parameter_in_a_statement_is_rejected(drevo_bolt_dsn: str) -> None:
    store = native.DrevoStore(drevo_bolt_dsn, _store_name())
    unit = store.unit()
    with pytest.raises(native.InvalidUnitOfWork):
        unit.run("CREATE (:X {t: $dat_txn})", {"dat_txn": "x"})
