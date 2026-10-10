"""A file journal shared by separate processes: crash, recovery scan, late decision."""

import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import pytest
from faker import Faker

from datorium.core.journal import BranchRecord, JournalConflict, SqliteJournal, State


def run_process(code: str, *args: str) -> str:
    """Run ``code`` in a fresh interpreter, the way a separate app instance would."""
    out = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code), *args],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return out.stdout.strip()


COORDINATOR_THAT_CRASHES_AFTER_PREPARE = """
    import os, sys, uuid
    from datorium.core.journal import BranchRecord, SqliteJournal, State
    path, txn = sys.argv[1], uuid.UUID(sys.argv[2])
    j = SqliteJournal(path, wall_ms=lambda: 1_000)
    branches = [BranchRecord(s, f"dat:{txn}:{s}", True) for s in ("pg", "graph")]
    j.begin(txn, branches)
    j.transition(txn, State.ACTIVE, State.PREPARED, dat_tx_time=123)
    print("prepared", flush=True)
    os._exit(1)  # crash before deciding: no close, no COMMITTED
"""

RECOVERY_WORKER = """
    import sys
    from datorium.core.journal import SqliteJournal, State
    j = SqliteJournal(sys.argv[1], wall_ms=lambda: 60_000)
    for record in j.pending(updated_before_ms=30_000):
        if record.state in (State.ACTIVE, State.PREPARED):
            j.transition(record.dat_txn, record.state, State.ABORTED)  # presumed abort
            print(f"aborted {record.dat_txn} {','.join(b.gid for b in record.branches)}")
    j.close()
"""


def test_it_task_manager_coordinator_crash_is_presumed_aborted(tmp_path: Path) -> None:
    path = str(tmp_path / "journal.db")
    txn = uuid.uuid4()

    with pytest.raises(subprocess.CalledProcessError) as crash:
        run_process(COORDINATOR_THAT_CRASHES_AFTER_PREPARE, path, str(txn))
    assert crash.value.stdout.strip() == "prepared"

    report = run_process(RECOVERY_WORKER, path)
    assert report == f"aborted {txn} dat:{txn}:pg,dat:{txn}:graph"

    # The original coordinator comes back and tries to decide: it must lose.
    late = SqliteJournal(path)
    with pytest.raises(JournalConflict, match="ABORTED"):
        late.transition(txn, State.PREPARED, State.COMMITTED)
    stored = late.get(txn)
    late.close()
    assert stored is not None
    assert (stored.state, stored.dat_tx_time) == (State.ABORTED, 123)


def test_erp_decided_transactions_are_left_for_roll_forward(tmp_path: Path, faker: Faker) -> None:
    path = tmp_path / "journal.db"
    ticks = iter(range(1_000, 2_000))
    journal = SqliteJournal(path, wall_ms=lambda: next(ticks))
    decided = []
    for _ in range(5):
        txn = uuid.uuid4()
        invoice = faker.bothify("INV-####")
        journal.begin(
            txn, [BranchRecord("ledger", f"dat:{txn}:ledger", True, payload=invoice.encode())]
        )
        journal.transition(txn, State.ACTIVE, State.PREPARED, dat_tx_time=1)
        journal.transition(txn, State.PREPARED, State.COMMITTED)
        decided.append((txn, invoice))
    journal.close()

    report = run_process(RECOVERY_WORKER, str(path))
    assert report == ""  # recovery never aborts after the decision

    reopened = SqliteJournal(path, wall_ms=lambda: 60_000)
    pending = reopened.pending(updated_before_ms=30_000)
    reopened.close()
    assert [(r.dat_txn, r.state) for r in pending] == [(t, State.COMMITTED) for t, _ in decided]
    assert [r.branches[0].payload for r in pending] == [i.encode() for _, i in decided]
