"""drevo branch and participant, embedded (drevo-py) and remote (Bolt), against real engines."""

import contextlib
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from conftest import drevo_embedded_available
from rhizodat.adapters.drevo import DrevoBranch, DrevoStore
from rhizodat.core.coordinator import gid_for
from rhizodat.core.identity import new_id
from rhizodat.core.journal import BranchRecord


def raised(call: Callable[[], object]) -> BaseException:
    """The exception ``call`` raises; the type depends on the transport, so no pytest.raises."""
    try:
        call()
    except Exception as exc:
        return exc
    pytest.fail("expected an error")


@pytest.fixture(params=["memory", "file", "bolt"])
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DrevoStore]:
    if request.param == "bolt":
        dsn = request.getfixturevalue("drevo_bolt_dsn")
    elif not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed: uv sync --group drevo")
    else:
        dsn = "drevo+mem://" if request.param == "memory" else f"drevo://{tmp_path / 'graph.drevo'}"
    s = DrevoStore(dsn, name="graph")
    opened: list[DrevoBranch] = []
    original = s.branch

    def tracked(dat_txn: uuid.UUID, **kw: Any) -> DrevoBranch:
        branch = original(dat_txn, **kw)
        opened.append(branch)
        return branch

    s.branch = tracked  # type: ignore[method-assign]
    yield s
    try:
        for branch in opened:  # a failed test must not leave the graph fenced
            with contextlib.suppress(Exception):
                branch.rollback()
        participant = s.participant()
        for gid in participant.prepared_gids():
            if gid.startswith("dat:"):
                participant.abort(BranchRecord("graph", gid, True))
    finally:
        s.close()


def label() -> str:
    """A label unique to the test, so tests on a shared Bolt server never see each other."""
    return f"T{uuid.uuid4().hex[:10]}"


def count(store: DrevoStore, lbl: str) -> int:
    return int(store.execute(f"MATCH (n:{lbl}) RETURN count(n) AS n").one()["n"])


def test_execute_returns_rows(store: DrevoStore) -> None:
    lbl = label()
    # Not `title`: drevo keeps node titles unique across the whole graph.
    store.execute(f"CREATE (:{lbl} {{heading: $h}})", {"h": "hello"})
    result = store.execute(f"MATCH (n:{lbl}) RETURN n.heading AS heading")
    assert result.all() == [{"heading": "hello"}]


def test_read_only_reads_and_never_keeps_a_write(store: DrevoStore) -> None:
    lbl = label()
    store.execute(f"CREATE (:{lbl} {{heading: 'kept'}})")
    rows = store.read_only(f"MATCH (n:{lbl}) RETURN n.heading AS heading").all()
    assert rows == [{"heading": "kept"}]
    # A write inside a read is rolled back with it.
    store.read_only(f"CREATE (:{lbl} {{heading: 'dropped'}})")
    assert count(store, lbl) == 1


def test_one_phase_commit_with_applied_marker(store: DrevoStore) -> None:
    lbl, txn = label(), new_id()
    branch = store.branch(txn)
    branch.execute(f"CREATE (:{lbl} {{x: 1}})")
    branch.stage(txn, 7)
    branch.commit_one_phase()
    assert count(store, lbl) == 1
    record = BranchRecord("graph", gid_for(txn, "graph"), True)
    assert store.participant().is_applied(record, txn)
    assert not store.participant().is_applied(record, new_id())


def test_rollback_discards_writes(store: DrevoStore) -> None:
    lbl, txn = label(), new_id()
    branch = store.branch(txn)
    branch.execute(f"CREATE (:{lbl})")
    branch.rollback()
    assert count(store, lbl) == 0


def test_prepare_fences_other_writers_until_finish(store: DrevoStore) -> None:
    lbl, txn = label(), new_id()
    gid = gid_for(txn, "graph")
    branch = store.branch(txn)
    branch.execute(f"CREATE (:{lbl})")
    branch.stage(txn, 1)
    branch.prepare(gid)
    participant = store.participant()
    assert gid in participant.prepared_gids()

    other = store.branch(new_id())
    other.execute(f"CREATE (:{lbl}Other)")
    fenced = raised(other.commit_one_phase)
    assert other.is_retryable(fenced)  # the prepared fence is transient

    record = BranchRecord("graph", gid, True)
    participant.finish(record, txn, 1)
    assert count(store, lbl) == 1
    assert gid not in participant.prepared_gids()
    participant.finish(record, txn, 1)  # unknown gid + marker = already done


def test_prepared_branch_rollback_lifts_the_fence(store: DrevoStore) -> None:
    lbl, txn = label(), new_id()
    branch = store.branch(txn)
    branch.execute(f"CREATE (:{lbl})")
    branch.stage(txn, 1)
    branch.prepare(gid_for(txn, "graph"))
    branch.rollback()
    assert count(store, lbl) == 0
    store.execute(f"CREATE (:{lbl}After)")  # writes work again
    assert count(store, f"{lbl}After") == 1


def test_abort_is_idempotent(store: DrevoStore) -> None:
    txn = new_id()
    gid = gid_for(txn, "graph")
    branch = store.branch(txn)
    branch.stage(txn, 1)
    branch.prepare(gid)
    record = BranchRecord("graph", gid, True)
    store.participant().abort(record)
    store.participant().abort(record)
    assert gid not in store.participant().prepared_gids()


def test_finish_of_a_rolled_back_gid_is_a_heuristic_outcome(store: DrevoStore) -> None:
    txn = new_id()
    gid = gid_for(txn, "graph")
    branch = store.branch(txn)
    branch.stage(txn, 1)
    branch.prepare(gid)
    record = BranchRecord("graph", gid, True)
    store.participant().abort(record)
    with pytest.raises(RuntimeError, match="heuristic"):
        store.participant().finish(record, txn, 1)


def test_non_transient_errors_are_not_retryable(store: DrevoStore) -> None:
    branch = store.branch(new_id())
    error = raised(lambda: branch.execute("THIS IS NOT CYPHER"))
    assert not branch.is_retryable(error)


def test_embedded_file_store_keeps_prepared_state_across_reopen(tmp_path: Path) -> None:
    if not drevo_embedded_available():
        pytest.skip("drevo-py with 2PC is not installed")
    dsn = f"drevo://{tmp_path / 'graph.drevo'}"
    first = DrevoStore(dsn, name="graph")
    lbl, txn = label(), new_id()
    gid = gid_for(txn, "graph")
    branch = first.branch(txn)
    branch.execute(f"CREATE (:{lbl})")
    branch.stage(txn, 1)
    branch.prepare(gid)
    first.close()  # the process "crashes" with a prepared transaction

    second = DrevoStore(dsn, name="graph")
    assert gid in second.participant().prepared_gids()
    second.participant().finish(BranchRecord("graph", gid, True), txn, 1)
    assert count(second, lbl) == 1
    second.close()


def test_dsn_must_be_drevo() -> None:
    with pytest.raises(ValueError, match="drevo"):
        DrevoStore("postgresql://localhost/x")


# --- capacity --------------------------------------------------------------------------


def test_an_embedded_graph_file_reports_its_disk(store: DrevoStore, tmp_path: Path) -> None:
    import shutil

    free = store.free_space()
    if store.location is not None and store.location[0] == "embedded":
        assert free is not None
        assert abs(free - shutil.disk_usage(tmp_path).free) < 1 << 30
    else:
        assert free is None  # in memory, or a server whose disk is not ours to see
