"""Commit protocol of meta-transactions.

A Unit of Work hands the coordinator one ``Branch`` per store it touched.

- No branch: nothing to do.
- One branch: the store's own one-phase commit, no journal.
- Several branches: stage → journal ``ACTIVE`` → prepare the 2PC branches →
  ``PREPARED`` → decide ``COMMITTED`` → finish every branch → ``DONE``.

Before the decision any failure rolls every branch back and raises ``Aborted``
(re-running the whole Unit of Work is then safe if ``retryable``). After the
decision nothing is rolled back: each branch is finished through its store's
``Participant`` with retries up to a short deadline, and anything left is
handed to recovery; ``commit`` then returns ``CommittedPending`` instead of
raising, so callers never write the transaction twice. When the decision write
itself cannot be confirmed either way, ``InDoubt`` is raised and recovery
decides from the journal.

Only ``Exception`` is caught. A ``BaseException`` (process shutdown, a test's
simulated crash) leaves stores and journal exactly as they are, which is what
recovery is built for.
"""

import contextlib
import random
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from datorium.core.clock import HybridLogicalClock
from datorium.core.journal import BranchRecord, Journal, JournalConflict, JournalRecord, State
from datorium.core.retry import Retry

__all__ = [
    "Aborted",
    "Branch",
    "Committed",
    "CommittedPending",
    "Coordinator",
    "InDoubt",
    "Participant",
    "Recovery",
    "RecoveryReport",
    "gid_for",
    "parse_gid",
]

_GID_PREFIX = "dat:"


class Branch(Protocol):
    """One store's part of one meta-transaction (lives as long as the Unit of Work)."""

    store: str
    two_phase: bool

    def stage(self, dat_txn: uuid.UUID, dat_tx_time: int) -> bytes | None:
        """Slow, lock-free work. Returns a payload to keep in the journal, if any."""
        ...

    def prepare(self, gid: str) -> None:
        """Lock-holding prepare (2PC branches only)."""
        ...

    def commit_one_phase(self) -> None:
        """Native commit when this is the only branch."""
        ...

    def rollback(self) -> None:
        """Undo before the decision: roll back an open or prepared transaction."""
        ...

    def is_retryable(self, error: BaseException) -> bool: ...


class Participant(Protocol):
    """A store as seen after the decision and by recovery (outlives Units of Work)."""

    def finish(self, branch: BranchRecord, dat_txn: uuid.UUID, dat_tx_time: int) -> None:
        """Make the branch visible: ``COMMIT PREPARED``, snapshot commit, deduplicated insert.
        Idempotent; an unknown gid means already resolved."""
        ...

    def is_applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        """Whether the branch is already visible, to resolve an ambiguous commit."""
        ...

    def abort(self, branch: BranchRecord) -> None:
        """Roll back a prepared gid / drop staged data. Idempotent."""
        ...

    def prepared_gids(self) -> list[str]:
        """Prepared transactions currently held by the store."""
        ...

    def is_retryable(self, error: BaseException) -> bool: ...


@dataclass(frozen=True, slots=True)
class Committed:
    dat_txn: uuid.UUID
    dat_tx_time: int | None
    value: object = None
    """What a ``transactional`` function returned."""


@dataclass(frozen=True, slots=True)
class CommittedPending:
    """Committed (the decision is durable), but some branches are still being finished."""

    dat_txn: uuid.UUID
    dat_tx_time: int | None
    value: object = None


class Aborted(Exception):
    """Nothing was committed. ``retryable``: re-running the Unit of Work may succeed."""

    def __init__(self, dat_txn: uuid.UUID, *, retryable: bool, reason: str) -> None:
        super().__init__(f"meta-transaction {dat_txn} aborted: {reason}")
        self.dat_txn = dat_txn
        self.retryable = retryable


class InDoubt(Exception):
    """The decision write could not be confirmed; recovery resolves it from the journal.
    Do not re-run the Unit of Work: it may have committed."""

    def __init__(self, dat_txn: uuid.UUID) -> None:
        super().__init__(f"outcome of meta-transaction {dat_txn} is unknown; left to recovery")
        self.dat_txn = dat_txn


def gid_for(dat_txn: uuid.UUID, store: str) -> str:
    """Global id of one branch: ``dat:<dat_txn>:<store>``."""
    return f"{_GID_PREFIX}{dat_txn}:{store}"


def parse_gid(gid: str) -> tuple[uuid.UUID, str] | None:
    """``(dat_txn, store)`` for a datorium gid, ``None`` for anything else.

    Only the spelling ``gid_for`` writes is ours: another spelling of the same
    UUID (upper case, braces, no hyphens) is a foreign gid.
    """
    if not gid.startswith(_GID_PREFIX):
        return None
    txn, sep, store = gid[len(_GID_PREFIX) :].partition(":")
    if not sep or not store:
        return None
    try:
        parsed = uuid.UUID(txn)
    except ValueError:
        return None
    return (parsed, store) if str(parsed) == txn else None


class Coordinator:
    def __init__(
        self,
        *,
        journal: Journal | None,
        participants: Mapping[str, Participant],
        clock: HybridLogicalClock,
        finish: Retry | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        rand: Callable[[], float] = random.random,
    ) -> None:
        self._journal = journal
        self._participants = participants
        self._clock = clock
        self._finish = finish or Retry(
            attempts=5, deadline_s=2.0, base_delay_s=0.02, max_delay_s=0.5
        )
        self._sleep = sleep
        self._monotonic = monotonic
        self._rand = rand

    def commit(
        self, dat_txn: uuid.UUID, branches: Sequence[Branch]
    ) -> Committed | CommittedPending:
        if not branches:
            return Committed(dat_txn, None)
        try:
            self._validate(branches)
        except ValueError:
            self.rollback(dat_txn, branches)
            raise
        if len(branches) == 1:
            return self._commit_one_phase(dat_txn, branches[0])
        return self._commit_two_phase(dat_txn, branches)

    def rollback(self, dat_txn: uuid.UUID, branches: Sequence[Branch]) -> None:
        """Undo every branch before the decision; one failing branch does not stop the rest."""
        for branch in branches:
            # Best effort; recovery cleans up prepared leftovers.
            with contextlib.suppress(Exception):
                branch.rollback()

    # --- one branch ---------------------------------------------------------------

    def _commit_one_phase(self, dat_txn: uuid.UUID, branch: Branch) -> Committed:
        ts = self._clock.now().encode()
        payload = None
        try:
            payload = branch.stage(dat_txn, ts)
            branch.commit_one_phase()
        except Exception as exc:
            gid = gid_for(dat_txn, branch.store)
            record = BranchRecord(branch.store, gid, branch.two_phase, payload)
            if self._applied(record, dat_txn):
                return Committed(dat_txn, ts)
            self.rollback(dat_txn, [branch])
            raise Aborted(dat_txn, retryable=branch.is_retryable(exc), reason=str(exc)) from exc
        return Committed(dat_txn, ts)

    # --- several branches ---------------------------------------------------------

    def _commit_two_phase(
        self, dat_txn: uuid.UUID, branches: Sequence[Branch]
    ) -> Committed | CommittedPending:
        journal = self._journal
        if journal is None:
            self.rollback(dat_txn, branches)
            raise ValueError(
                "a meta-transaction over several stores needs a journal: "
                "pass journal=<dsn> (PostgreSQL or drevo; file:/// for development)"
            )
        ordered = sorted(branches, key=lambda b: b.two_phase)  # lock-free work first
        ts = self._clock.now().encode()
        journaled = False
        try:
            staged = {}
            for branch in ordered:
                payload = branch.stage(dat_txn, ts)
                staged[branch.store] = BranchRecord(
                    branch.store, gid_for(dat_txn, branch.store), branch.two_phase, payload
                )
            journaled = True  # from here a record may exist, even if begin() reports an error
            journal.begin(dat_txn, [staged[b.store] for b in branches])
            for branch in ordered:
                if branch.two_phase:
                    branch.prepare(gid_for(dat_txn, branch.store))
            journal.transition(dat_txn, State.ACTIVE, State.PREPARED, dat_tx_time=ts)
        except Exception as exc:
            self._abort(dat_txn, branches, journaled)
            failing = next((b for b in branches if b.is_retryable(exc)), None)
            retryable = failing is not None or isinstance(exc, ConnectionError | TimeoutError)
            raise Aborted(dat_txn, retryable=retryable, reason=str(exc)) from exc

        record = self._decide(journal, dat_txn, branches)
        return self._finish_all(journal, record)

    def _decide(
        self, journal: Journal, dat_txn: uuid.UUID, branches: Sequence[Branch]
    ) -> JournalRecord:
        deadline = self._monotonic() + self._finish.deadline_s
        attempt = 0
        while True:
            try:
                return journal.transition(dat_txn, State.PREPARED, State.COMMITTED)
            except JournalConflict as exc:
                # Recovery (or anyone) moved the record; only ABORTED is possible from PREPARED.
                self.rollback(dat_txn, branches)
                raise Aborted(dat_txn, retryable=True, reason=str(exc)) from exc
            except Exception as exc:
                try:
                    current = journal.get(dat_txn)
                except Exception as read_exc:
                    raise InDoubt(dat_txn) from read_exc
                if current is not None and current.state is State.COMMITTED:
                    return current
                if current is None or current.state is not State.PREPARED:
                    self.rollback(dat_txn, branches)
                    raise Aborted(dat_txn, retryable=True, reason=str(exc)) from exc
                attempt += 1
                if attempt >= self._finish.attempts or self._monotonic() >= deadline:
                    return self._give_up_deciding(journal, dat_txn, branches, exc)
                self._pause(attempt - 1, deadline)

    def _give_up_deciding(
        self, journal: Journal, dat_txn: uuid.UUID, branches: Sequence[Branch], cause: Exception
    ) -> JournalRecord:
        """Still PREPARED after every attempt: settle it with one more compare-and-set."""
        try:
            journal.transition(dat_txn, State.PREPARED, State.ABORTED)
        except JournalConflict:
            try:
                current = journal.get(dat_txn)
            except Exception as read_exc:
                raise InDoubt(dat_txn) from read_exc
            if current is not None and current.state is State.COMMITTED:
                return current
        except Exception as exc:
            raise InDoubt(dat_txn) from exc
        self.rollback(dat_txn, branches)
        raise Aborted(dat_txn, retryable=True, reason=str(cause)) from cause

    def _abort(self, dat_txn: uuid.UUID, branches: Sequence[Branch], journaled: bool) -> None:
        if journaled and self._journal is not None:
            for expected in (State.ACTIVE, State.PREPARED):
                try:
                    self._journal.transition(dat_txn, expected, State.ABORTED)
                    break
                except Exception:  # no decision was written: presumed abort
                    continue
        self.rollback(dat_txn, branches)

    def _finish_all(self, journal: Journal, record: JournalRecord) -> Committed | CommittedPending:
        assert record.dat_tx_time is not None
        deadline = self._monotonic() + self._finish.deadline_s
        lock_holders_first = sorted(record.branches, key=lambda b: not b.two_phase)
        done = [self._finish_one(b, record, deadline) for b in lock_holders_first]
        if not all(done):
            return CommittedPending(record.dat_txn, record.dat_tx_time)
        try:
            journal.transition(record.dat_txn, State.COMMITTED, State.DONE)
        except Exception:  # recovery re-finishes idempotently and marks DONE
            return CommittedPending(record.dat_txn, record.dat_tx_time)
        return Committed(record.dat_txn, record.dat_tx_time)

    def _finish_one(self, branch: BranchRecord, record: JournalRecord, deadline: float) -> bool:
        assert record.dat_tx_time is not None
        participant = self._participants[branch.store]
        for attempt in range(self._finish.attempts):
            try:
                participant.finish(branch, record.dat_txn, record.dat_tx_time)
                return True
            except Exception as exc:
                if self._applied(branch, record.dat_txn):
                    return True
                retryable = participant.is_retryable(exc)
                if not retryable or attempt + 1 >= self._finish.attempts:
                    return False
                if self._monotonic() >= deadline:
                    return False
                self._pause(attempt, deadline)
        return False

    # --- helpers --------------------------------------------------------------------

    def _applied(self, branch: BranchRecord, dat_txn: uuid.UUID) -> bool:
        try:
            return self._participants[branch.store].is_applied(branch, dat_txn)
        except Exception:  # unknown counts as not applied; never guess "done"
            return False

    def _pause(self, attempt: int, deadline: float) -> None:
        remaining = deadline - self._monotonic()
        if remaining > 0:
            self._sleep(min(self._finish.delay(attempt, self._rand), remaining))

    def _validate(self, branches: Sequence[Branch]) -> None:
        seen: set[str] = set()
        for branch in branches:
            if branch.store in seen:
                raise ValueError(f"store {branch.store!r} has more than one branch")
            if branch.store not in self._participants:
                raise ValueError(f"no participant is registered for store {branch.store!r}")
            seen.add(branch.store)


def _system_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(slots=True)
class RecoveryReport:
    aborted: list[uuid.UUID] = field(default_factory=list)
    finished: list[uuid.UUID] = field(default_factory=list)
    unfinished: list[uuid.UUID] = field(default_factory=list)
    stuck: list[uuid.UUID] = field(default_factory=list)
    """Decided but unfinished for longer than ``alert_after_ms``: raise an alert."""
    orphans: list[str] = field(default_factory=list)
    """Prepared gids rolled back because the journal has no decision for them."""


class Recovery:
    """Finishes what crashed coordinators left behind. Safe to run on any host, repeatedly.

    Each run first sweeps prepared gids held by the stores (rolling back those
    whose journal record is missing or ``ABORTED``), then handles journal records
    unchanged for ``stale_after_ms``: no decision → ``ABORTED`` (compare-and-set
    first, so a live coordinator cannot decide afterwards) and roll back;
    ``COMMITTED`` → finish every branch idempotently and mark ``DONE``.
    """

    def __init__(
        self,
        *,
        journal: Journal,
        participants: Mapping[str, Participant],
        stale_after_ms: int = 30_000,
        alert_after_ms: int = 600_000,
        wall_ms: Callable[[], int] = _system_ms,
    ) -> None:
        if stale_after_ms <= 0:
            raise ValueError("stale_after_ms must be > 0: recovery must not race live coordinators")
        self._journal = journal
        self._participants = participants
        self._stale_after_ms = stale_after_ms
        self._alert_after_ms = alert_after_ms
        self._wall_ms = wall_ms

    def run_once(self, *, limit: int = 100) -> RecoveryReport:
        report = RecoveryReport()
        self._sweep_orphans(report)
        now = self._wall_ms()
        for record in self._journal.pending(
            updated_before_ms=now - self._stale_after_ms, limit=limit
        ):
            if record.state is State.COMMITTED:
                self._roll_forward(record, now, report)
            else:
                self._presume_abort(record, report)
        return report

    def _presume_abort(self, record: JournalRecord, report: RecoveryReport) -> None:
        try:
            self._journal.transition(record.dat_txn, record.state, State.ABORTED)
        except JournalConflict:
            return  # it moved on since the scan; the next run sees the new state
        report.aborted.append(record.dat_txn)
        for branch in record.branches:
            try:
                self._participants[branch.store].abort(branch)
            except Exception:  # left prepared: the orphan sweep of a later run retries
                continue

    def _roll_forward(self, record: JournalRecord, now: int, report: RecoveryReport) -> None:
        assert record.dat_tx_time is not None
        done = True
        for branch in sorted(record.branches, key=lambda b: not b.two_phase):
            participant = self._participants[branch.store]
            try:
                participant.finish(branch, record.dat_txn, record.dat_tx_time)
            except Exception:
                try:
                    applied = participant.is_applied(branch, record.dat_txn)
                except Exception:
                    applied = False
                done = done and applied
        if done:
            with contextlib.suppress(JournalConflict):  # another worker finished it
                self._journal.transition(record.dat_txn, State.COMMITTED, State.DONE)
            report.finished.append(record.dat_txn)
            return
        report.unfinished.append(record.dat_txn)
        if now - record.created_ms > self._alert_after_ms:
            report.stuck.append(record.dat_txn)

    def _sweep_orphans(self, report: RecoveryReport) -> None:
        for store, participant in self._participants.items():
            try:
                gids = participant.prepared_gids()
            except Exception:
                continue
            for gid in gids:
                parsed = parse_gid(gid)
                if parsed is None or parsed[1] != store:
                    continue  # not ours
                record = self._journal.get(parsed[0])
                if record is not None and record.state is not State.ABORTED:
                    continue  # undecided (handled when stale) or committed (rolled forward)
                try:
                    participant.abort(BranchRecord(store, gid, two_phase=True))
                except Exception:
                    continue
                report.orphans.append(gid)
