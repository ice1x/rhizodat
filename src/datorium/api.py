"""The ``Datorium`` facade: stores, journal, clock, Units of Work and retries."""

import dataclasses
import functools
import random
import re
import time
from collections.abc import Callable, Mapping
from typing import Any, Concatenate, overload

from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import Aborted, Committed, CommittedPending, Coordinator, Recovery
from datorium.core.identity import new_id
from datorium.core.journal import Journal, open_journal
from datorium.core.retry import Retry
from datorium.core.unit_of_work import Store, UnitOfWork

__all__ = ["Datorium"]

_STORE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

Outcome = Committed | CommittedPending


def open_store(name: str, dsn: str) -> Store:
    """Open the adapter for ``dsn``; drivers are imported only here."""
    from datorium.core.dsn import parse_dsn

    kind = parse_dsn(dsn).store
    if kind == "postgres":
        from datorium.adapters.postgres import PostgresStore

        return PostgresStore(dsn, name=name)
    if kind == "drevo":
        from datorium.adapters.drevo import DrevoStore

        return DrevoStore(dsn, name=name)
    if kind == "clickhouse":
        from datorium.adapters.clickhouse import ClickHouseStore

        return ClickHouseStore(dsn, name=name)
    from datorium.adapters.iceberg import IcebergStore

    return IcebergStore(dsn, name=name)


def _check_journal_graph(journal: str, stores: Mapping[str, "str | Store"]) -> None:
    """A drevo journal in the graph of a drevo store would be blocked by its prepared fence."""
    from datorium.core.dsn import parse_dsn

    if parse_dsn(journal).store != "drevo":
        return
    from datorium.adapters.drevo import graph_location

    where = graph_location(journal)
    if where is None:
        return
    for name, store in stores.items():
        other = (
            graph_location(store) if isinstance(store, str) else getattr(store, "location", None)
        )
        if other == where:
            raise ValueError(
                f"the journal must live in a separate drevo graph: store {name!r} uses the same"
                " one, and its prepared fence would block the coordinator's journal writes"
            )


def _check_applied_locations(stores: Mapping[str, "Store"]) -> None:
    """Two stores sharing one ``dat_applied`` would block or confuse each other."""
    seen: dict[tuple[str, ...], str] = {}
    for name, store in stores.items():
        where = getattr(store, "applied_location", None)
        if where is None:
            continue
        if where in seen:
            raise ValueError(
                f"stores {seen[where]!r} and {name!r} share one dat_applied ({':'.join(where)}):"
                " their branches of one meta-transaction would wait for each other or look"
                " applied when they are not; give each store its own schema, database or graph"
            )
        seen[where] = name


class Datorium:
    def __init__(
        self,
        *,
        stores: Mapping[str, str | Store],
        journal: str | Journal | None = None,
        clock: HybridLogicalClock | None = None,
        retry: Retry | None = None,
        finish: Retry | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        rand: Callable[[], float] = random.random,
    ) -> None:
        for name in stores:
            if not _STORE_NAME.match(name):
                raise ValueError(f"invalid store name {name!r}: letters, digits, '_', '.', '-'")
        if isinstance(journal, str):
            _check_journal_graph(journal, stores)
        self._stores: dict[str, Store] = {
            name: open_store(name, s) if isinstance(s, str) else s for name, s in stores.items()
        }
        try:
            _check_applied_locations(self._stores)
        except ValueError:
            for opened in self._stores.values():
                opened.close()
            raise
        self.journal: Journal | None = (
            open_journal(journal) if isinstance(journal, str) else journal
        )
        self.clock = clock or HybridLogicalClock()
        self.retry = retry or Retry()
        self._sleep = sleep
        self._monotonic = monotonic
        self._rand = rand
        self._coordinator = Coordinator(
            journal=self.journal,
            participants={name: s.participant() for name, s in self._stores.items()},
            clock=self.clock,
            finish=finish,
            sleep=sleep,
            monotonic=monotonic,
            rand=rand,
        )

    @property
    def store_names(self) -> tuple[str, ...]:
        """The configured stores, in configuration order."""
        return tuple(self._stores)

    def store(self, name: str) -> Store:
        """Native access to a store, outside any Unit of Work."""
        try:
            return self._stores[name]
        except KeyError:
            raise KeyError(f"no store named {name!r}; configured: {sorted(self._stores)}") from None

    def unit_of_work(self) -> UnitOfWork:
        """A Unit of Work that runs once; use ``transactional`` for retries."""
        return UnitOfWork(new_id(), self.store, self._coordinator)

    @overload
    def transactional[**P, T](
        self, fn: Callable[Concatenate[UnitOfWork, P], T], *, retry: Retry | None = None
    ) -> Callable[P, Outcome]: ...

    @overload
    def transactional[**P, T](
        self, fn: None = None, *, retry: Retry | None = None
    ) -> Callable[[Callable[Concatenate[UnitOfWork, P], T]], Callable[P, Outcome]]: ...

    def transactional(self, fn: Any = None, *, retry: Retry | None = None) -> Any:
        """Run ``fn(uow, *args, **kwargs)`` as one Unit of Work, re-running all of it on a
        transient failure before the decision (new ``dat_txn`` each time).

        Returns ``Committed`` or ``CommittedPending`` with ``value`` set to what ``fn``
        returned. Raises ``Aborted`` when retries are exhausted or the failure is
        permanent, ``InDoubt`` when the outcome is unknown, and domain errors from ``fn``
        unchanged.
        """
        policy = retry or self.retry

        def decorate(func: Callable[..., Any]) -> Callable[..., Outcome]:
            @functools.wraps(func)
            def run(*args: Any, **kwargs: Any) -> Outcome:  # noqa: ANN401
                return self._run(func, policy, args, kwargs)

            return run

        return decorate(fn) if fn is not None else decorate

    def recovery(self, **kwargs: Any) -> Recovery:  # noqa: ANN401 - Recovery settings
        if self.journal is None:
            raise ValueError("recovery needs the journal: pass journal=<dsn>")
        return Recovery(
            journal=self.journal,
            participants={name: s.participant() for name, s in self._stores.items()},
            **kwargs,
        )

    def close(self) -> None:
        for s in self._stores.values():
            s.close()
        if self.journal is not None:
            self.journal.close()

    def _run(
        self,
        func: Callable[..., Any],
        policy: Retry,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Outcome:
        deadline = self._monotonic() + policy.deadline_s
        for attempt in range(policy.attempts):
            uow = self.unit_of_work()
            try:
                value = func(uow, *args, **kwargs)
            except Exception as exc:
                uow.rollback()
                if not uow.is_retryable(exc):
                    raise
                failure = Aborted(uow.dat_txn, retryable=True, reason=str(exc))
                failure.__cause__ = exc
            else:
                try:
                    return dataclasses.replace(uow.commit(), value=value)
                except Aborted as aborted:
                    if not aborted.retryable:
                        raise
                    failure = aborted
            delay = policy.delay(attempt, self._rand)
            if attempt + 1 >= policy.attempts or self._monotonic() + delay > deadline:
                raise failure
            self._sleep(delay)
        raise AssertionError("unreachable: Retry.attempts >= 1")
