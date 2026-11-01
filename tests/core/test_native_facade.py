"""The ``Rhizodat`` facade running its Units of Work on the native core.

Opt-in: when ``Rhizodat`` is built with native store objects
(``rhizodat_native.PostgresStore`` and the like) and a native journal (or none),
``transactional`` and ``unit_of_work`` commit through
``rhizodat_native.Coordinator`` — the whole protocol runs in Rust with no Python
callbacks on the path, exactly as the direct ``rhizodat_native.Coordinator``
tests exercise it. A Unit of Work buffers its data on each store's native unit:
``uow.store(name).insert(table, columns, rows)`` for a tabular store,
``uow.store(name).run(cypher, params)`` for drevo. Repositories and parts are
covered by ``test_native_parts*.py``, ``execute`` on ClickHouse and Iceberg by
``test_native_execute.py``; migrations are not on this path yet.

The Docker-free tests only build the facade (its native detection and the
not-yet-supported surface); the rest need the compose PostgreSQL and drevo.
Install the module with ``uv sync --group native`` (needs a Rust toolchain).
"""

import uuid
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.core.coordinator import Aborted, Committed, CommittedPending, InDoubt
from rhizodat.core.mapping import MappingError
from rhizodat.core.native_uow import NativeUnitOfWork
from rhizodat.core.unit_of_work import UnitOfWorkClosed

native = pytest.importorskip("rhizodat_native")

_LITERAL_DSN = "postgresql://u:p@127.0.0.1:5432/db"


@pytest.fixture
def make_schema(pg_dsn: str) -> Iterator[Callable[[], str]]:
    """A factory for fresh schemas, each its own store, all dropped afterwards."""
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
    """A drevo store name unique to the test, so branches never share a gid."""
    return f"g{uuid.uuid4().hex[:10]}"


def _create(pg_dsn: str, schema: str, ddl: str) -> None:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'SET search_path TO "{schema}"')
        conn.execute(ddl)


def _rows(pg_dsn: str, schema: str, sql: str) -> list[tuple[object, ...]]:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'SET search_path TO "{schema}"')
        return list(conn.execute(sql).fetchall())


def _applied(pg_dsn: str, schema: str, dat_txn: uuid.UUID) -> bool:
    return bool(_rows(pg_dsn, schema, f"SELECT 1 FROM dat_applied WHERE dat_txn = '{dat_txn}'"))


# --- the NativeUnitOfWork wrapper, in isolation (no store) ----------------------------


class _FakeUnit:
    """A native unit stand-in: records what is buffered on it."""

    def __init__(self) -> None:
        self.rows: list[object] = []

    def insert(self, *args: object) -> None:
        self.rows.append(args)


class _FakeCoordinator:
    """A native coordinator stand-in: returns a canned result or raises."""

    def __init__(self, *, result: object = None, error: BaseException | None = None) -> None:
        self._result = result
        self._error = error
        self.calls: list[object] = []

    def commit(self, dat_txn: uuid.UUID, units: list[object]) -> object:
        self.calls.append((dat_txn, list(units)))
        if self._error is not None:
            raise self._error
        return self._result


def _unit_of_work(coordinator: _FakeCoordinator) -> NativeUnitOfWork:
    units = {"s": _FakeUnit()}
    return NativeUnitOfWork(native.new_id(), lambda name: _Store(units[name]), coordinator, native)


class _Store:
    def __init__(self, unit: _FakeUnit) -> None:
        self._unit = unit

    def unit(self) -> _FakeUnit:
        return self._unit


def test_an_empty_unit_of_work_commits_without_touching_the_coordinator() -> None:
    coordinator = _FakeCoordinator()
    uow = _unit_of_work(coordinator)
    outcome = uow.commit()
    assert isinstance(outcome, Committed)
    assert outcome.dat_tx_time is None
    assert coordinator.calls == []


def test_the_store_unit_is_created_once_and_reused() -> None:
    uow = _unit_of_work(_FakeCoordinator())
    assert uow.store("s") is uow.store("s")


def test_a_native_unit_of_work_made_outside_the_facade_has_no_repositories() -> None:
    uow = _unit_of_work(_FakeCoordinator())
    with pytest.raises(RuntimeError, match="no repositories"):
        uow.repository(int)


def test_rollback_drops_buffered_data_and_closes_the_unit_of_work() -> None:
    uow = _unit_of_work(_FakeCoordinator())
    uow.store("s").insert("people", ["n"], [[1]])
    assert uow.stores_touched == ("s",)
    uow.rollback()
    uow.rollback()  # idempotent: a second rollback does nothing
    with pytest.raises(UnitOfWorkClosed):
        uow.commit()


def test_a_pending_result_maps_to_committed_pending() -> None:
    result = {"status": "pending", "dat_txn": native.new_id(), "dat_tx_time": 42}
    uow = _unit_of_work(_FakeCoordinator(result=result))
    uow.store("s").insert("t", ["n"], [[1]])
    outcome = uow.commit()
    assert isinstance(outcome, CommittedPending)
    assert outcome.dat_tx_time == 42


def test_a_retryable_native_abort_becomes_a_retryable_aborted() -> None:
    error: Any = native.AbortedError("boom")
    error.retryable = True
    uow = _unit_of_work(_FakeCoordinator(error=error))
    uow.store("s").insert("t", ["n"], [[1]])
    with pytest.raises(Aborted) as caught:
        uow.commit()
    assert caught.value.retryable is True
    assert uow.is_retryable(error) is True
    assert uow.full_store(error) is None
    assert caught.value.__cause__ is error


def test_a_native_indoubt_becomes_indoubt() -> None:
    uow = _unit_of_work(_FakeCoordinator(error=native.InDoubtError("unknown")))
    uow.store("s").insert("t", ["n"], [[1]])
    with pytest.raises(InDoubt):
        uow.commit()


def test_a_context_manager_rolls_back_on_an_exception() -> None:
    coordinator = _FakeCoordinator()
    uow = _unit_of_work(coordinator)

    def body() -> None:
        with uow:
            uow.store("s").insert("t", ["n"], [[1]])
            raise ValueError("domain")

    with pytest.raises(ValueError, match="domain"):
        body()
    assert coordinator.calls == []  # never committed


# --- the native facade, without a store -----------------------------------------------


def test_a_native_store_needs_a_native_journal_or_none() -> None:
    store = native.PostgresStore(_LITERAL_DSN, "pg", "public")
    with pytest.raises(ValueError, match="native journal"):
        Rhizodat(stores={"pg": store}, journal="postgresql:///somewhere")


def test_native_and_python_stores_cannot_mix() -> None:
    store = native.PostgresStore(_LITERAL_DSN, "pg", "public")
    with pytest.raises(ValueError, match="native"):
        Rhizodat(stores={"pg": store, "other": "postgresql:///x"})


def test_the_native_facade_offers_repositories_of_entity_types() -> None:
    store = native.PostgresStore(_LITERAL_DSN, "pg", "public")
    with Rhizodat(stores={"pg": store}) as r:
        assert r.store_names == ("pg",)
        assert r.store("pg") is store
        with pytest.raises(MappingError, match="not an @entity"):
            r.repository(int)


# --- a single native store through the facade -----------------------------------------


def test_transactional_writes_rows_through_the_native_core(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    fake = Faker()
    Faker.seed(13)
    schema = make_schema()
    _create(
        pg_dsn,
        schema,
        "CREATE TABLE people (name text, age int, dat_txn uuid, dat_tx_time bigint)",
    )
    people = [[fake.name(), fake.random_int(18, 90)] for _ in range(4)]
    with Rhizodat(stores={"pg": native.PostgresStore(pg_dsn, "pg", schema)}) as r:

        @r.transactional
        def write(uow: object) -> int:
            uow.store("pg").insert("people", ["name", "age"], people)  # type: ignore[attr-defined]
            return len(people)

        outcome = write()

    assert isinstance(outcome, Committed)
    assert outcome.value == len(people)
    assert outcome.dat_txn is not None
    rows = _rows(pg_dsn, schema, "SELECT name, age FROM people ORDER BY name")
    assert rows == sorted((name, age) for name, age in people)
    stamped = _rows(pg_dsn, schema, "SELECT DISTINCT dat_txn, dat_tx_time FROM people")
    assert stamped == [(outcome.dat_txn, outcome.dat_tx_time)]


def test_unit_of_work_as_a_context_manager_commits(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    _create(pg_dsn, schema, "CREATE TABLE log (n int, dat_txn uuid, dat_tx_time bigint)")
    with Rhizodat(stores={"pg": native.PostgresStore(pg_dsn, "pg", schema)}) as r:
        with r.unit_of_work() as uow:
            uow.store("pg").insert("log", ["n"], [[1], [2]])
        assert isinstance(uow.outcome, Committed | CommittedPending)
        txn = uow.outcome.dat_txn
    assert _rows(pg_dsn, schema, "SELECT n FROM log ORDER BY n") == [(1,), (2,)]
    assert _applied(pg_dsn, schema, txn)


def test_a_failing_commit_raises_aborted_and_writes_nothing(
    pg_dsn: str, make_schema: Callable[[], str]
) -> None:
    schema = make_schema()
    # No table "missing": the branch fails at stage, so the commit aborts and
    # nothing (not even the applied marker) is written.
    with Rhizodat(stores={"pg": native.PostgresStore(pg_dsn, "pg", schema)}) as r:

        @r.transactional
        def write(uow: object) -> None:
            uow.store("pg").insert("missing", ["n"], [[1]])  # type: ignore[attr-defined]

        with pytest.raises(Aborted) as caught:
            write()

    assert caught.value.dat_txn is not None
    assert _rows(pg_dsn, schema, "SELECT count(*) FROM dat_applied") == [(0,)]


# --- a single native ClickHouse store (one-phase, no journal) -------------------------


def _ch_client(clickhouse_dsn: str) -> object:
    import clickhouse_connect

    from rhizodat.core.dsn import parse_dsn

    server = parse_dsn(clickhouse_dsn)
    return clickhouse_connect.get_client(
        host=server.host,
        port=server.port,
        username=server.user or "default",
        password=server.password or "",
        database=(server.path or "/").lstrip("/") or "default",
        autogenerate_session_id=False,
    )


def test_a_clickhouse_store_commits_through_the_facade(clickhouse_dsn: str) -> None:
    client = _ch_client(clickhouse_dsn)
    client.command(  # type: ignore[attr-defined]
        "CREATE TABLE events (id Int64, name String, dat_txn UUID, dat_tx_time Int64)"
        " ENGINE = MergeTree ORDER BY id"
    )
    with Rhizodat(stores={"ch": native.ClickHouseStore(clickhouse_dsn, "ch")}) as r:

        @r.transactional
        def write(uow: object) -> None:
            uow.store("ch").insert("events", ["id", "name"], [[1, "alice"], [2, "bob"]])  # type: ignore[attr-defined]

        outcome = write()

    assert isinstance(outcome, Committed)
    rows = client.query("SELECT id, name FROM events ORDER BY id").result_rows  # type: ignore[attr-defined]
    assert rows == [(1, "alice"), (2, "bob")]


# --- a meta-transaction over two native stores through a journal ----------------------


def test_two_native_stores_commit_through_a_journal(
    pg_dsn: str, drevo_bolt_dsn: str, make_schema: Callable[[], str]
) -> None:
    pg_schema, journal_schema = make_schema(), make_schema()
    label = f"T{uuid.uuid4().hex[:10]}"
    bolt = drevo_bolt_dsn
    _create(pg_dsn, pg_schema, "CREATE TABLE orders (sku text, dat_txn uuid, dat_tx_time bigint)")
    stores = {
        "pg": native.PostgresStore(pg_dsn, "pg", pg_schema),
        "graph": native.DrevoStore(bolt, _store_name()),
    }
    journal = native.PostgresJournal(pg_dsn, journal_schema)
    try:
        with Rhizodat(stores=stores, journal=journal) as r:

            @r.transactional
            def write(uow: object) -> None:
                uow.store("pg").insert("orders", ["sku"], [["A-1"], ["B-2"]])  # type: ignore[attr-defined]
                uow.store("graph").run(  # type: ignore[attr-defined]
                    f"CREATE (:{label} {{dat_txn: $dat_txn}})"
                )

            outcome = write()

        assert isinstance(outcome, Committed)
        assert _rows(pg_dsn, pg_schema, "SELECT sku FROM orders ORDER BY sku") == [
            ("A-1",),
            ("B-2",),
        ]
        assert journal.get(outcome.dat_txn)["state"] == "DONE"
        count = _graph_count(bolt, label, outcome.dat_txn)
        assert count == 1
    finally:
        _cleanup_graph(bolt, label)


def test_postgres_and_iceberg_commit_through_the_facade(
    pg_dsn: str, iceberg_rest_dsn: str, make_schema: Callable[[], str], faker: Faker
) -> None:
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType, UUIDType

    from rhizodat.adapters.iceberg import IcebergStore as PyIcebergStore

    pg_schema, journal_schema = make_schema(), make_schema()
    _create(pg_dsn, pg_schema, "CREATE TABLE orders (sku text, dat_txn uuid, dat_tx_time bigint)")
    lake = PyIcebergStore(iceberg_rest_dsn, name="reader")
    namespace = f"dat_native_{uuid.uuid4().hex[:12]}"
    table = f"{namespace}.sales"
    lake.catalog.create_namespace(namespace)
    lake.catalog.create_table(
        table,
        Schema(
            NestedField(1, "sku", StringType(), required=True),
            NestedField(2, "qty", LongType()),
            NestedField(3, "dat_txn", UUIDType()),
            NestedField(4, "dat_tx_time", LongType()),
        ),
    )
    skus = [faker.bothify("SKU-####") for _ in range(3)]
    stores = {
        "pg": native.PostgresStore(pg_dsn, "pg", pg_schema),
        "lake": native.IcebergStore(iceberg_rest_dsn, "lake"),
    }
    try:
        with Rhizodat(stores=stores, journal=native.PostgresJournal(pg_dsn, journal_schema)) as r:

            @r.transactional
            def sell(uow: object) -> None:
                uow.store("pg").insert("orders", ["sku"], [[s] for s in skus])  # type: ignore[attr-defined]
                uow.store("lake").append(  # type: ignore[attr-defined]
                    table, pa.table({"sku": skus, "qty": [1, 2, 3]})
                )

            outcome = sell()

        assert isinstance(outcome, Committed)
        assert sorted(_rows(pg_dsn, pg_schema, "SELECT sku FROM orders")) == sorted(
            (s,) for s in skus
        )
        sales = lake.execute(table).all()
        assert sorted(r["sku"] for r in sales) == sorted(skus)
        assert {r["dat_txn"] for r in sales} == {outcome.dat_txn}
    finally:
        lake.catalog.drop_table(table)
        lake.catalog.drop_namespace(namespace)
        lake.close()


# drevo read-back helpers (kept local so the Docker-free tests do not import them)


def _graph_count(bolt: str, label: str, dat_txn: uuid.UUID) -> int:
    from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore

    store = PyDrevoStore(bolt, name="reader")
    try:
        result = store.execute(
            f"MATCH (n:{label} {{dat_txn: $t}}) RETURN count(n) AS n", {"t": str(dat_txn)}
        )
        return int(result.one()["n"])
    finally:
        store.close()


def _cleanup_graph(bolt: str, label: str) -> None:
    import contextlib

    from rhizodat.adapters.drevo import DrevoStore as PyDrevoStore

    store = PyDrevoStore(bolt, name="reader")
    try:
        with contextlib.suppress(Exception):
            store.execute(f"MATCH (n:{label}) DETACH DELETE n")
    finally:
        store.close()
