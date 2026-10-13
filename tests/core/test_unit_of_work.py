"""Unit of Work, the Datorium facade and whole-Unit-of-Work retries."""

import uuid

import pytest
from faker import Faker

from datorium import Datorium
from datorium.core.coordinator import Aborted, Committed, CommittedPending, InDoubt
from datorium.core.identity import is_uuid7
from datorium.core.journal import JournalRecord, MemoryJournal, State
from datorium.core.retry import Retry
from datorium.core.unit_of_work import UnitOfWork, UnitOfWorkClosed
from fakes import Crash, FakeStoreHandle, Permanent, Transient, World


class Clock:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


@pytest.fixture
def world() -> World:
    w = World()
    w.store("pg", two_phase=True)
    w.store("graph", two_phase=True)
    w.store("clickhouse", two_phase=False)
    return w


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def dat(world: World, clock: Clock) -> Datorium:
    return Datorium(
        stores={name: FakeStoreHandle(s) for name, s in world.stores.items()},
        journal=MemoryJournal(),
        retry=Retry(attempts=4, deadline_s=10.0, base_delay_s=0.1, max_delay_s=1.0),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        rand=lambda: 0.5,
    )


# --- Unit of Work ------------------------------------------------------------------


def test_branches_open_lazily_one_per_store(dat: Datorium, world: World) -> None:
    with dat.unit_of_work() as uow:
        assert is_uuid7(uow.dat_txn)
        assert not uow.stores_touched
        uow.store("pg").execute("order")
        uow.store("pg").execute("line")
        assert uow.store("pg") is uow.store("pg")
        assert uow.stores_touched == ("pg",)
    assert world.stores["pg"].rows_of(uow.dat_txn) == ["order", "line"]
    assert uow.outcome == Committed(uow.dat_txn, uow.outcome.dat_tx_time)  # type: ignore[union-attr]


def test_single_store_uow_skips_the_journal(dat: Datorium, world: World) -> None:
    with dat.unit_of_work() as uow:
        uow.store("clickhouse").execute("event")
    assert dat.journal is not None
    assert dat.journal.get(uow.dat_txn) is None
    assert world.stores["clickhouse"].rows_of(uow.dat_txn) == ["event"]


def test_multi_store_uow_commits_through_the_journal(dat: Datorium, world: World) -> None:
    with dat.unit_of_work() as uow:
        for name in ("pg", "graph", "clickhouse"):
            uow.store(name).execute(f"{name}-row")
    assert world.applied(uow.dat_txn) == dict.fromkeys(world.stores, True)
    assert dat.journal is not None
    record = dat.journal.get(uow.dat_txn)
    assert record is not None
    assert record.state is State.DONE


def test_exception_in_the_block_rolls_back_and_propagates(dat: Datorium, world: World) -> None:
    uow = dat.unit_of_work()

    def block() -> None:
        with uow:
            uow.store("pg").execute("row")
            uow.store("graph").execute("row")
            raise KeyError("domain error")

    with pytest.raises(KeyError):
        block()
    assert world.applied(uow.dat_txn) == dict.fromkeys(world.stores, False)
    assert uow.outcome is None
    # Branches are released, not left open (an open PostgreSQL transaction, a drevo fence).
    assert world.faults.seen.count("pg.rollback") == 1
    assert world.faults.seen.count("graph.rollback") == 1


def test_a_crash_inside_the_block_leaves_branches_for_recovery(dat: Datorium, world: World) -> None:
    uow = dat.unit_of_work()

    def block() -> None:
        with uow:
            uow.store("pg").execute("row")
            raise Crash()

    with pytest.raises(Crash):
        block()
    assert "pg.rollback" not in world.faults.seen


def test_explicit_commit_and_rollback(dat: Datorium, world: World) -> None:
    uow = dat.unit_of_work()
    uow.store("pg").execute("row")
    outcome = uow.commit()
    assert isinstance(outcome, Committed)
    with pytest.raises(UnitOfWorkClosed):
        uow.store("pg")
    with pytest.raises(UnitOfWorkClosed):
        uow.commit()

    other = dat.unit_of_work()
    other.store("pg").execute("row")
    other.rollback()
    other.rollback()  # idempotent: the branches are rolled back once
    assert world.applied(other.dat_txn)["pg"] is False
    assert world.faults.seen.count("pg.rollback") == 1


def test_unknown_store_name_is_an_error(dat: Datorium) -> None:
    uow = dat.unit_of_work()
    with pytest.raises(KeyError, match="'nope'"):
        uow.store("nope")
    uow.rollback()


def test_a_with_block_runs_once_and_does_not_retry(dat: Datorium, world: World) -> None:
    world.faults.add("pg.prepare", Transient("fence"))
    runs = 0

    def block() -> None:
        nonlocal runs
        with dat.unit_of_work() as uow:
            runs += 1
            uow.store("pg").execute("row")
            uow.store("graph").execute("row")

    with pytest.raises(Aborted) as err:
        block()
    assert runs == 1
    assert err.value.retryable is True


def test_uow_is_not_reusable_across_threads_of_work(dat: Datorium) -> None:
    uow = dat.unit_of_work()
    assert isinstance(uow, UnitOfWork)
    uow.rollback()
    with pytest.raises(UnitOfWorkClosed):
        uow.store("pg")


# --- transactional: whole Unit of Work retries ------------------------------------------


def test_transactional_returns_the_outcome_with_the_function_value(
    dat: Datorium, world: World
) -> None:
    @dat.transactional
    def place_order(uow: UnitOfWork, sku: str) -> str:
        uow.store("pg").execute(f"order {sku}")
        uow.store("clickhouse").execute(f"sold {sku}")
        return f"ok {sku}"

    outcome = place_order(sku="SKU-1")
    assert isinstance(outcome, Committed)
    assert outcome.value == "ok SKU-1"
    assert world.stores["pg"].rows_of(outcome.dat_txn) == ["order SKU-1"]


def test_transient_failure_before_the_decision_reruns_with_a_new_dat_txn(
    dat: Datorium, world: World, clock: Clock
) -> None:
    world.faults.add("graph.prepare", Transient("prepared fence"), times=2)
    seen: list[uuid.UUID] = []

    @dat.transactional
    def link(uow: UnitOfWork) -> None:
        seen.append(uow.dat_txn)
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    outcome = link()
    assert isinstance(outcome, Committed)
    assert len(seen) == 3
    assert len(set(seen)) == 3  # never re-used: reads of a failed attempt may be stale
    assert outcome.dat_txn == seen[-1]
    assert world.applied(seen[0]) == dict.fromkeys(world.stores, False)
    assert len(clock.sleeps) == 2


def test_backoff_uses_full_jitter_and_grows(dat: Datorium, world: World, clock: Clock) -> None:
    world.faults.add("pg.prepare", Transient("x"), times=3)

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    work()
    # rand() = 0.5 of min(max_delay, base * 2**n): 0.05, 0.1, 0.2
    assert clock.sleeps == pytest.approx([0.05, 0.1, 0.2])


def test_retryable_error_raised_by_the_work_itself_is_retried(dat: Datorium, world: World) -> None:
    world.faults.add("pg.execute", Transient("40001 serialization failure"))
    runs = 0

    @dat.transactional
    def work(uow: UnitOfWork) -> int:
        nonlocal runs
        runs += 1
        uow.store("pg").execute("a")
        return runs

    outcome = work()
    assert outcome.value == 2


def test_non_retryable_failure_is_not_retried(dat: Datorium, world: World) -> None:
    world.faults.add("pg.prepare", Permanent("constraint"))
    runs = 0

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        nonlocal runs
        runs += 1
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(Aborted) as err:
        work()
    assert runs == 1
    assert err.value.retryable is False


def test_domain_error_in_the_function_propagates_unwrapped(dat: Datorium) -> None:
    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        uow.store("pg").execute("a")
        raise ValueError("insufficient stock")

    with pytest.raises(ValueError, match="insufficient stock"):
        work()


def test_attempts_are_bounded(dat: Datorium, world: World) -> None:
    world.faults.add("pg.prepare", Transient("x"), times=100)
    runs = 0

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        nonlocal runs
        runs += 1
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(Aborted) as err:
        work()
    assert runs == 4
    assert err.value.retryable is True


def test_deadline_bounds_retries(world: World, clock: Clock) -> None:
    world.faults.add("pg.prepare", Transient("x"), times=100)
    dat = Datorium(
        stores={n: FakeStoreHandle(s) for n, s in world.stores.items()},
        journal=MemoryJournal(),
        retry=Retry(attempts=100, deadline_s=1.0, base_delay_s=0.4, max_delay_s=0.4),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
        rand=lambda: 1.0,
    )

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(Aborted):
        work()
    assert clock.t <= 1.0


def test_per_function_retry_policy(dat: Datorium, world: World) -> None:
    world.faults.add("pg.prepare", Transient("x"), times=1)

    @dat.transactional(retry=Retry(attempts=1))
    def work(uow: UnitOfWork) -> None:
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(Aborted):
        work()


def test_committed_pending_is_returned_not_retried(dat: Datorium, world: World) -> None:
    world.faults.add("clickhouse.finish", Transient("down"), times=100)
    runs = 0

    @dat.transactional
    def work(uow: UnitOfWork) -> str:
        nonlocal runs
        runs += 1
        uow.store("pg").execute("a")
        uow.store("clickhouse").execute("b")
        return "v"

    outcome = work()
    assert isinstance(outcome, CommittedPending)
    assert outcome.value == "v"
    assert runs == 1


def test_in_doubt_is_never_retried(world: World, clock: Clock) -> None:
    class Dark(MemoryJournal):
        def transition(
            self, dat_txn: uuid.UUID, expected: State, new: State, *, dat_tx_time: int | None = None
        ) -> JournalRecord:
            if new is State.COMMITTED:
                raise TimeoutError("no answer")
            return super().transition(dat_txn, expected, new, dat_tx_time=dat_tx_time)

        def get(self, dat_txn: uuid.UUID) -> JournalRecord | None:
            raise ConnectionError("unreachable")

    dat = Datorium(
        stores={n: FakeStoreHandle(s) for n, s in world.stores.items()},
        journal=Dark(),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )
    runs = 0

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        nonlocal runs
        runs += 1
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(InDoubt):
        work()
    assert runs == 1


def test_crash_propagates_through_transactional(dat: Datorium, world: World) -> None:
    world.faults.add("pg.prepare", Crash())

    @dat.transactional
    def work(uow: UnitOfWork) -> None:
        uow.store("pg").execute("a")
        uow.store("graph").execute("b")

    with pytest.raises(Crash):
        work()


# --- facade ---------------------------------------------------------------------


def test_native_store_access_outside_a_uow(dat: Datorium, world: World) -> None:
    with dat.unit_of_work() as uow:
        uow.store("pg").execute("row")
    assert dat.store("pg").execute("SELECT").all() == [{"row": "row"}]


def test_multi_store_without_a_journal_fails_clearly(world: World) -> None:
    dat = Datorium(stores={n: FakeStoreHandle(s) for n, s in world.stores.items()})
    uow = dat.unit_of_work()
    uow.store("pg").execute("a")
    uow.store("graph").execute("b")
    with pytest.raises(ValueError, match="journal"):
        uow.commit()
    assert world.applied(uow.dat_txn) == dict.fromkeys(world.stores, False)


def test_recovery_uses_every_store(dat: Datorium, world: World) -> None:
    world.faults.add("graph.finish", Crash())
    uow = dat.unit_of_work()
    uow.store("pg").execute("a")
    uow.store("graph").execute("b")
    with pytest.raises(Crash):
        uow.commit()
    report = dat.recovery(stale_after_ms=1, wall_ms=lambda: 10**15).run_once()
    assert report.finished == [uow.dat_txn]
    assert world.applied(uow.dat_txn) == {"pg": True, "graph": True, "clickhouse": False}


def test_recovery_needs_a_journal(world: World) -> None:
    dat = Datorium(stores={n: FakeStoreHandle(s) for n, s in world.stores.items()})
    with pytest.raises(ValueError, match="journal"):
        dat.recovery()


def test_close_closes_every_store(dat: Datorium) -> None:
    handles = [dat.store(n) for n in ("pg", "graph", "clickhouse")]
    dat.close()
    assert all(h.closed for h in handles)  # type: ignore[attr-defined]


def test_stores_from_connection_strings(tmp_path: object) -> None:
    dat = Datorium(stores={"graph": "drevo+mem://"}, journal=None) if _drevo() else None
    if dat is None:
        pytest.skip("drevo-py not installed")
    assert dat.store("graph").name == "graph"
    dat.close()


def test_unsupported_store_kind_is_reported() -> None:
    with pytest.raises(NotImplementedError, match="glue"):
        Datorium(stores={"lake": "iceberg+glue://eu-west-1/warehouse"})


def test_a_drevo_journal_cannot_share_a_graph_with_a_drevo_store() -> None:
    """The store's prepared fence would block the coordinator's own journal writes."""
    with pytest.raises(ValueError, match="separate drevo graph"):
        Datorium(
            stores={"graph": "drevo+bolt://graph.local:7687"}, journal="drevo+bolt://graph.local"
        )


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("postgresql://u:p@db.local/app", "postgresql://other@DB.local:5432/app"),
        ("postgresql://db.local/app?options=x", "postgres://db.local/app"),
    ],
)
def test_two_postgres_stores_cannot_share_dat_applied(first: str, second: str) -> None:
    """Both branches of one meta-transaction would insert the same dat_txn into one
    dat_applied table, and the second would wait for the first forever."""
    with pytest.raises(ValueError, match="share one dat_applied") as caught:
        Datorium(stores={"orders": first, "ledger": second})
    message = str(caught.value)
    assert "'orders'" in message
    assert "'ledger'" in message
    assert ":p@" not in message  # no secret in the error


@pytest.mark.parametrize(
    "second",
    ["postgresql://db.local/other", "postgresql://db.local:6432/app", "postgresql://db2.local/app"],
)
def test_postgres_stores_in_other_databases_or_servers_are_fine(second: str) -> None:
    dat = Datorium(stores={"orders": "postgresql://db.local/app", "ledger": second})
    dat.close()


def test_postgres_stores_in_other_schemas_of_one_database_are_fine(faker: Faker) -> None:
    from datorium.adapters.postgres import PostgresStore

    dsn = f"postgresql://db.local/{faker.word()}"
    orders = PostgresStore(dsn, name="orders", schema="orders")
    ledger = PostgresStore(dsn, name="ledger", schema="ledger")
    dat = Datorium(stores={"orders": orders, "ledger": ledger})
    dat.close()
    shared = PostgresStore(dsn, name="ledger", schema="orders")
    with pytest.raises(ValueError, match="share one dat_applied"):
        Datorium(stores={"orders": orders, "ledger": shared})
    default = PostgresStore(dsn, name="a")
    public = PostgresStore(dsn, name="b", schema="public")
    with pytest.raises(ValueError, match="share one dat_applied"):
        Datorium(stores={"a": default, "b": public})


def test_two_drevo_stores_cannot_share_a_graph() -> None:
    """The first branch's prepared fence would refuse the second's prepare on every retry."""
    with pytest.raises(ValueError, match="share one dat_applied"):
        Datorium(stores={"a": "drevo+bolt://g.local", "b": "drevo+bolt://G.local:7687"})


def test_any_store_reporting_the_same_applied_location_is_refused() -> None:
    """ClickHouse, or a store object passed in, reports where its dat_applied lives."""
    world = World()
    one = FakeStoreHandle(world.store("feed", two_phase=False))
    two = FakeStoreHandle(world.store("events", two_phase=False))
    one.applied_location = two.applied_location = ("clickhouse", "ch", "8123", "analytics")  # type: ignore[attr-defined]
    with pytest.raises(ValueError, match="share one dat_applied"):
        Datorium(stores={"feed": one, "events": two})


def test_store_names_are_validated() -> None:
    with pytest.raises(ValueError, match="store name"):
        Datorium(stores={"bad:name": "drevo+mem://"})


def _drevo() -> bool:
    from conftest import drevo_embedded_available

    return drevo_embedded_available()
