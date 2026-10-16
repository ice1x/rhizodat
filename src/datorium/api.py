"""The ``Datorium`` facade: stores, journal, clock, Units of Work and retries."""

import dataclasses
import functools
import random
import re
import time
from collections.abc import Callable, Mapping
from datetime import timedelta
from types import TracebackType
from typing import TYPE_CHECKING, Any, Concatenate, Self, overload

from datorium.core.clock import HybridLogicalClock
from datorium.core.coordinator import (
    Aborted,
    Committed,
    CommittedPending,
    Coordinator,
    Recovery,
    StoreFull,
)
from datorium.core.errors import Capacity
from datorium.core.identity import new_id
from datorium.core.journal import Journal, open_journal
from datorium.core.retry import Retry
from datorium.core.unit_of_work import Store, UnitOfWork

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from pathlib import Path

    from datorium.core.journal import AppliedMigration
    from datorium.core.repository import EntityRepository, Repository, _Kind
    from datorium.migrations import Migration, MigrationReport

__all__ = ["Datorium"]

_STORE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

Outcome = Committed | CommittedPending


def open_store(name: str, dsn: str) -> Store:
    """Open the store ``dsn`` names with its registered adapter."""
    from datorium.core.adapter import adapter_for
    from datorium.core.dsn import parse_dsn

    kind = parse_dsn(dsn).store
    open_ = adapter_for(kind).open_store
    if open_ is None:
        raise ValueError(f"{kind} cannot be a store: it only holds the journal")
    return open_(name, dsn)


def _check_journal_fence(journal: str, stores: Mapping[str, Store]) -> None:
    """A journal inside a store's prepared fence would block the coordinator's own writes."""
    from datorium.core.adapter import Fenced, adapter_for
    from datorium.core.dsn import parse_dsn

    locate = adapter_for(parse_dsn(journal).store).journal_location
    where = locate(journal) if locate is not None else None
    if where is None:
        return
    for name, store in stores.items():
        if isinstance(store, Fenced) and store.fence_location == where:
            raise ValueError(
                f"the journal must live outside store {name!r}: while one of its branches is"
                " prepared, its fence blocks every other commit there, including the"
                " coordinator's journal writes (for drevo: use a separate drevo graph)"
            )


def _check_applied_locations(stores: Mapping[str, "Store"]) -> None:
    """Two stores sharing one ``dat_applied`` would block or confuse each other."""
    from datorium.core.adapter import AppliedLocation

    seen: dict[tuple[str, ...], str] = {}
    for name, store in stores.items():
        where = store.applied_location if isinstance(store, AppliedLocation) else None
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
        soft_delete_window: timedelta = timedelta(days=30),
        min_free: int | None = None,
    ) -> None:
        if min_free is not None and min_free < 0:
            raise ValueError(f"min_free must not be negative, got {min_free}")
        if soft_delete_window <= timedelta(0):
            raise ValueError(f"soft_delete_window must be positive, got {soft_delete_window}")
        self.soft_delete_window = soft_delete_window
        """How long a soft-deleted entity stays restorable, unless its type sets its own."""
        self._kinds: dict[type, _Kind] = {}
        for name in stores:
            if not _STORE_NAME.match(name):
                raise ValueError(f"invalid store name {name!r}: letters, digits, '_', '.', '-'")
        self._stores: dict[str, Store] = {}
        try:
            for name, s in stores.items():
                self._stores[name] = open_store(name, s) if isinstance(s, str) else s
            _check_applied_locations(self._stores)
            if isinstance(journal, str):
                _check_journal_fence(journal, self._stores)
        except Exception:
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
            capacity={n: s for n, s in self._stores.items() if isinstance(s, Capacity)},
            min_free=min_free,
        )
        self.min_free = min_free
        """Bytes a store reporting its free space (``Capacity``) must keep: a meta-transaction
        touching one below it fails with ``StoreFull`` before anything is written.
        ``None``: not checked (a full store still fails when the store reports it)."""

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
        return UnitOfWork(
            new_id(), self.store, self._coordinator, repositories=self._uow_repository
        )

    def repository[T](self, entity_type: type[T]) -> "EntityRepository[T]":
        """CRUD for one entity type: committed reads, one Unit of Work per write."""
        from datorium.core.repository import EntityRepository

        return EntityRepository(self, entity_type, self._kind(entity_type))

    def _kind(self, entity_type: type) -> "_Kind":
        """The type's parts, installed once in every store that holds one."""
        from datorium.core.parts import PartStore
        from datorium.core.repository import _Kind

        kind = self._kinds.get(entity_type)
        if kind is None:
            kind = _Kind(entity_type, self.soft_delete_window)
            for name, part in kind.parts.items():
                store = self.store(name)
                if not isinstance(store, PartStore):
                    raise TypeError(
                        f"store {name!r} cannot hold entity parts of"
                        f" {kind.mapping.type_name} (its adapter has no PartStore)"
                    )
                store.install_part(part)
            self._kinds[entity_type] = kind
        return kind

    def _uow_repository(self, uow: UnitOfWork, entity_type: type) -> "Repository[Any]":
        from datorium.core.repository import Repository

        return Repository(uow, self._kind(entity_type), lambda: self.clock.now().encode())

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

    def migrate(
        self,
        source: "str | Path | Sequence[Migration]",
        *,
        to: int | None = None,
        dry_run: bool = False,
    ) -> "MigrationReport":
        """Run the migrations in ``source`` (a directory of ``NNNN_name.toml`` files, or
        ``Migration`` objects) that the journal has not recorded, up to ``to``.

        Raises ``MigrationError`` before anything runs if the history does not match
        the source or a step cannot run on its store, ``MigrationFailed`` when a step
        fails (earlier migrations stay applied; the next run repeats the failed one).
        ``dry_run`` only checks and reports what would run.
        """
        from datorium.migrations.runner import migrate

        return migrate(self, source, to=to, dry_run=dry_run)

    def schema_versions(self, *, as_of: "datetime | None" = None) -> "list[AppliedMigration]":
        """The migrations applied (as known at ``as_of``, if given), by id."""
        from datorium.migrations.runner import schema_versions

        return schema_versions(self, as_of=as_of)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

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
                full = uow.full_store(exc)
                if full is not None:
                    raise StoreFull(uow.dat_txn, store=full, reason=str(exc)) from exc
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
