"""ERP orders over two PostgreSQL stores, a fake analytics store and a PostgreSQL journal."""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import psycopg
import pytest
from faker import Faker

from fakes import Crash, FakeBranch, Faults, World
from rhizodat.adapters.postgres import PostgresJournal, PostgresStore
from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.coordinator import Aborted, Committed, Coordinator, Recovery, gid_for
from rhizodat.core.identity import new_id
from rhizodat.core.journal import State
from rhizodat.core.retry import Retry


class Wall:
    def __init__(self) -> None:
        self.ms = 1_760_000_000_000

    def __call__(self) -> int:
        return self.ms


@dataclass
class Erp:
    orders: PostgresStore
    stock: PostgresStore
    world: World  # fake analytics
    journal: PostgresJournal
    wall: Wall

    def participants(self) -> dict[str, object]:
        return {
            "orders": self.orders.participant(),
            "stock": self.stock.participant(),
            **self.world.participants(),
        }

    def coordinator(self) -> Coordinator:
        return Coordinator(
            journal=self.journal,
            participants=self.participants(),  # type: ignore[arg-type]
            clock=HybridLogicalClock(),
            finish=Retry(attempts=2, deadline_s=1.0, base_delay_s=0.0, max_delay_s=0.0),
            sleep=lambda s: None,
        )

    def recovery(self) -> Recovery:
        participants = self.participants()
        return Recovery(journal=self.journal, participants=participants, wall_ms=self.wall)  # type: ignore[arg-type]

    def place_order(self, txn: uuid.UUID, sku: str, qty: int, customer: str) -> list[object]:
        """The work of one Unit of Work: insert the order, reserve stock, emit a fact.
        On a failure the branches opened so far are rolled back, as the Unit of Work does."""
        opened: list[object] = []
        try:
            order = self.orders.branch(txn)
            opened.append(order)
            order.execute(
                "INSERT INTO orders (dat_id, customer, sku, qty) VALUES (%s, %s, %s, %s)",
                (txn, customer, sku, qty),
            )
            stock = self.stock.branch(txn)
            opened.append(stock)
            stock.execute("UPDATE stock SET on_hand = on_hand - %s WHERE sku = %s", (qty, sku))
        except Exception:
            for branch in opened:
                branch.rollback()  # type: ignore[attr-defined]
            raise
        fact = FakeBranch(self.world.stores["analytics"], [f"sold {qty} {sku}"])
        return [order, stock, fact]

    def order_count(self) -> int:
        return int(self.orders.execute("SELECT count(*) AS n FROM orders").one()["n"])

    def on_hand(self, sku: str) -> int:
        return int(
            self.stock.execute("SELECT on_hand FROM stock WHERE sku = %s", (sku,)).one()["on_hand"]
        )

    def prepared(self) -> list[str]:
        return self.orders.participant().prepared_gids()  # same server: one list for both stores


@pytest.fixture
def erp(pg_dsn: str, pg_schema: str) -> Iterator[Erp]:
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{pg_schema}_stock"')
    orders = PostgresStore(pg_dsn, name="orders", schema=pg_schema)
    stock = PostgresStore(pg_dsn, name="stock", schema=f"{pg_schema}_stock")
    for store in (orders, stock):
        store.install()
    with orders.connect(autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE orders (dat_id uuid PRIMARY KEY DEFERRABLE INITIALLY DEFERRED,"
            " customer text, sku text, qty int)"
        )
    with stock.connect(autocommit=True) as conn:
        conn.execute(
            "CREATE TABLE stock (sku text PRIMARY KEY, on_hand int NOT NULL CHECK (on_hand >= 0))"
        )
        conn.execute("INSERT INTO stock VALUES ('SKU-1', 10)")
    world = World(faults=Faults())
    world.store("analytics", two_phase=False)
    wall = Wall()
    journal = PostgresJournal(pg_dsn, schema=pg_schema, wall_ms=wall)
    yield Erp(orders, stock, world, journal, wall)
    journal.close()
    orders.close()
    stock.close()
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'DROP SCHEMA "{pg_schema}_stock" CASCADE')


def test_order_commits_atomically_across_stores(erp: Erp, faker: Faker) -> None:
    txn = new_id()
    branches = erp.place_order(txn, "SKU-1", 3, faker.company())
    outcome = erp.coordinator().commit(txn, branches)  # type: ignore[arg-type]

    assert isinstance(outcome, Committed)
    assert (erp.order_count(), erp.on_hand("SKU-1")) == (1, 7)
    assert erp.world.applied(txn) == {"analytics": True}
    assert erp.prepared() == []
    record = erp.journal.get(txn)
    assert record is not None
    assert record.state is State.DONE


def test_overselling_aborts_every_store(erp: Erp, faker: Faker) -> None:
    txn = new_id()
    with pytest.raises(psycopg.errors.CheckViolation):
        erp.place_order(txn, "SKU-1", 11, faker.company())
    # The application gives up on the Unit of Work; nothing reached any store.
    assert (erp.order_count(), erp.on_hand("SKU-1")) == (0, 10)


def test_constraint_failing_at_stage_aborts_without_retry(erp: Erp, faker: Faker) -> None:
    txn = new_id()
    erp.orders.execute(
        "INSERT INTO orders (dat_id, customer, sku, qty) VALUES (%s, 'earlier', 'SKU-1', 1)", (txn,)
    )
    branches = erp.place_order(txn, "SKU-1", 1, faker.company())  # deferred: not checked yet

    with pytest.raises(Aborted) as err:
        erp.coordinator().commit(txn, branches)  # type: ignore[arg-type]
    assert isinstance(err.value.__cause__, psycopg.errors.UniqueViolation)
    assert err.value.retryable is False
    assert (erp.order_count(), erp.on_hand("SKU-1")) == (1, 10)
    assert erp.prepared() == []


class CrashOnDecision:
    """The coordinator host dies right before writing COMMITTED."""

    def __init__(self, inner: PostgresJournal) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)

    def transition(
        self, dat_txn: uuid.UUID, expected: State, new: State, **kw: int | None
    ) -> object:
        if new is State.COMMITTED:
            raise Crash()
        return self.inner.transition(dat_txn, expected, new, **kw)


@pytest.mark.parametrize("point", ["before the decision", "analytics.finish"])
def test_coordinator_crash_is_resolved_by_recovery(erp: Erp, faker: Faker, point: str) -> None:
    txn = new_id()
    branches = erp.place_order(txn, "SKU-1", 2, faker.company())
    coordinator = erp.coordinator()
    if point == "before the decision":
        coordinator._journal = CrashOnDecision(erp.journal)  # type: ignore[assignment]
    else:
        erp.world.faults.add(point, Crash())

    with pytest.raises(Crash):
        coordinator.commit(txn, branches)  # type: ignore[arg-type]
    assert erp.prepared() if point == "before the decision" else True

    erp.wall.ms += 60_000
    report = erp.recovery().run_once()
    if point == "before the decision":
        assert report.aborted == [txn]
        assert (erp.order_count(), erp.on_hand("SKU-1")) == (0, 10)
        assert erp.world.applied(txn) == {"analytics": False}
    else:
        assert report.finished == [txn]
        assert (erp.order_count(), erp.on_hand("SKU-1")) == (1, 8)
        assert erp.world.applied(txn) == {"analytics": True}
    assert erp.prepared() == []


def test_prepared_order_blocks_a_competing_writer_until_resolved(erp: Erp, faker: Faker) -> None:
    """A prepared PostgreSQL branch keeps its row locks: the window must be short."""
    txn = new_id()
    order, stock, fact = erp.place_order(txn, "SKU-1", 1, faker.company())
    for branch in (fact, order, stock):
        branch.stage(txn, 1)  # type: ignore[attr-defined]
    stock.prepare(gid_for(txn, "stock"))  # type: ignore[attr-defined]

    with erp.stock.connect() as competitor:
        competitor.execute("SET lock_timeout = '200ms'")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            competitor.execute("UPDATE stock SET on_hand = on_hand - 1 WHERE sku = 'SKU-1'")
    stock.rollback()  # type: ignore[attr-defined]
    order.rollback()  # type: ignore[attr-defined]
    assert erp.prepared() == []
