"""The ``Rhizodat`` facade: stores, journal, clock, Units of Work and retries."""

import dataclasses
import functools
import random
import re
import time
from collections.abc import Callable, Mapping
from datetime import timedelta
from types import ModuleType, TracebackType
from typing import TYPE_CHECKING, Any, Concatenate, Self, cast, overload

from rhizodat.core.clock import HybridLogicalClock
from rhizodat.core.coordinator import (
    Aborted,
    Committed,
    CommittedPending,
    Coordinator,
    Recovery,
    StoreFull,
)
from rhizodat.core.errors import Capacity
from rhizodat.core.identity import new_id
from rhizodat.core.journal import Journal, open_journal
from rhizodat.core.native_uow import (
    NativeRecovery,
    NativeUnitOfWork,
    journal_classes,
    native_module,
    store_classes,
)
from rhizodat.core.retry import Retry
from rhizodat.core.unit_of_work import Store, UnitOfWork

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime
    from pathlib import Path

    from rhizodat.core.journal import AppliedMigration
    from rhizodat.core.native_parts import NativePartStore
    from rhizodat.core.parts import PartStore
    from rhizodat.core.repository import EntityRepository, Repository, _Kind
    from rhizodat.migrations import Migration, MigrationReport

__all__ = ["Rhizodat"]

_STORE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

Outcome = Committed | CommittedPending


def open_store(name: str, dsn: str) -> Store:
    """Open the store ``dsn`` names with its registered adapter."""
    from rhizodat.core.adapter import adapter_for
    from rhizodat.core.dsn import parse_dsn

    kind = parse_dsn(dsn).store
    open_ = adapter_for(kind).open_store
    if open_ is None:
        raise ValueError(f"{kind} cannot be a store: it only holds the journal")
    return open_(name, dsn)


def _check_journal_fence(journal: str, stores: Mapping[str, Store]) -> None:
    """A journal inside a store's prepared fence would block the coordinator's own writes."""
    from rhizodat.core.adapter import Fenced, adapter_for
    from rhizodat.core.dsn import parse_dsn

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
    from rhizodat.core.adapter import AppliedLocation

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


def _native_config(
    stores: Mapping[str, Any],
    journal: Any,  # noqa: ANN401 - a native journal, a Journal, a DSN string or None
) -> "tuple[ModuleType, dict[str, Any], Any] | None":
    """The native setup for ``stores`` and ``journal``, or ``None`` for the Python path.

    A native facade is opt-in: every store is a ``rhizodat_native`` store object
    and the journal is a native journal (or ``None``). A store object that the
    package recognises as native forces the native path, so a partly native
    configuration is an error rather than a silent fallback.
    """
    native = native_module()
    if native is None:
        return None
    store_types = store_classes(native)
    native_stores = {n: s for n, s in stores.items() if isinstance(s, store_types)}
    if not native_stores:
        return None
    if len(native_stores) != len(stores):
        other = sorted(set(stores) - set(native_stores))
        raise ValueError(
            f"native stores cannot mix with non-native ones (non-native: {other}); give every"
            " store a rhizodat_native store object, or none"
        )
    if journal is not None and not isinstance(journal, journal_classes(native)):
        raise ValueError(
            "native stores need a native journal or none: pass a rhizodat_native"
            " MemoryJournal, PostgresJournal or DrevoJournal (not a DSN string)"
        )
    return native, native_stores, journal


class Rhizodat:
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
        self._installed: set[str] = set()
        for name in stores:
            if not _STORE_NAME.match(name):
                raise ValueError(f"invalid store name {name!r}: letters, digits, '_', '.', '-'")
        self.retry = retry or Retry()
        self._sleep = sleep
        self._monotonic = monotonic
        self._rand = rand
        native = _native_config(stores, journal)
        if native is not None:
            self._init_native(*native)
            return
        self._native = False
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

    def _init_native(
        self,
        native: ModuleType,
        stores: "dict[str, Any]",
        journal: Any,  # noqa: ANN401 - a native journal or None
    ) -> None:
        """Set up the opt-in native path: Units of Work commit through
        ``rhizodat_native.Coordinator`` over the given native stores and journal."""
        self._native = True
        self._native_mod = native
        self._stores = stores
        self.journal = journal
        # The native coordinator keeps its own clock; this one is unused on the
        # native path but kept so ``self.clock`` stays a real clock.
        self.clock = HybridLogicalClock()
        self.min_free = None
        self._coordinator = native.Coordinator(journal)
        self._native_parts: dict[str, NativePartStore] = {}

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

    def _native_uow_store(self, name: str) -> Any:  # noqa: ANN401 - a native store object
        """A native store about to be used, with its service tables installed the
        first time (idempotent; only on the write path, like the Python facade)."""
        store = self.store(name)
        if name not in self._installed:
            install = getattr(store, "install", None)
            if callable(install):
                install()
            self._installed.add(name)
        return store

    def unit_of_work(self) -> UnitOfWork:
        """A Unit of Work that runs once; use ``transactional`` for retries.

        On the native path this is a :class:`~rhizodat.core.native_uow.NativeUnitOfWork`,
        duck-typed to the same surface (``store``, ``commit``, ``rollback`` …) but
        buffering data on each store's native unit rather than a Python branch.
        """
        if self._native:
            native = NativeUnitOfWork(
                new_id(),
                self._native_uow_store,
                self._coordinator,
                self._native_mod,
                repositories=self._uow_repository,
            )
            return cast("UnitOfWork", native)
        return UnitOfWork(
            new_id(), self._uow_store, self._coordinator, repositories=self._uow_repository
        )

    def _uow_store(self, name: str) -> Store:
        """A store about to get a branch, with its service tables (``dat_applied``) created
        the first time: idempotent, and only on write paths, so a read-only user (such as
        rhizodat-MCP's) never needs DDL rights."""
        store = self.store(name)
        if name not in self._installed:
            install = getattr(store, "install", None)
            if callable(install):
                install()
            self._installed.add(name)
        return store

    def _require_python(self, feature: str) -> None:
        if self._native:
            raise NotImplementedError(
                f"{feature} is not on the native core path yet; use the Python facade"
                " (DSN strings or Python adapter stores) for it"
            )

    def repository[T](self, entity_type: type[T]) -> "EntityRepository[T]":
        """CRUD for one entity type: committed reads, one Unit of Work per write."""
        from rhizodat.core.repository import EntityRepository

        return EntityRepository(self, entity_type, self._kind(entity_type))

    def _kind(self, entity_type: type) -> "_Kind":
        """The type's parts, installed once in every store that holds one."""
        from rhizodat.core.parts import PartStore
        from rhizodat.core.repository import _Kind

        kind = self._kinds.get(entity_type)
        if kind is None:
            kind = _Kind(entity_type, self.soft_delete_window)
            for name, part in kind.parts.items():
                store = self._part_store(name)
                if not isinstance(store, PartStore):
                    raise TypeError(
                        f"store {name!r} cannot hold entity parts of"
                        f" {kind.mapping.type_name} (its adapter has no PartStore)"
                    )
                store.install_part(part)
            self._kinds[entity_type] = kind
        return kind

    def _part_store(self, name: str) -> "PartStore":
        """Store ``name``'s committed parts: the store itself on the Python path, its
        :class:`~rhizodat.core.native_parts.NativePartStore` on the native one."""
        if not self._native:
            return cast("PartStore", self.store(name))
        parts = self._native_parts.get(name)
        if parts is None:
            from rhizodat.core.native_parts import NativePartStore

            parts = self._native_parts[name] = NativePartStore(self._native_mod, self.store(name))
        return parts

    def _uow_repository(self, uow: UnitOfWork, entity_type: type) -> "Repository[Any]":
        from rhizodat.core.repository import Repository

        now = lambda: self.clock.now().encode()  # noqa: E731 - a one-line clock
        if not self._native:
            return Repository(uow, self._kind(entity_type), now)
        from rhizodat.core.native_parts import NativePartBranch

        def branch(name: str) -> NativePartBranch:
            parts = cast("NativePartStore", self._part_store(name))
            return NativePartBranch(uow.store(name), parts, uow.dat_txn)

        return Repository(uow, self._kind(entity_type), now, branches=branch)

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
        if self._native:
            # Same settings and report as the Python Recovery, run by the core.
            return cast(
                "Recovery",
                NativeRecovery(
                    self._native_mod, self.journal, list(self._stores.values()), **kwargs
                ),
            )
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
        self._require_python("migrations")
        from rhizodat.migrations.runner import migrate

        return migrate(self, source, to=to, dry_run=dry_run)

    def schema_versions(self, *, as_of: "datetime | None" = None) -> "list[AppliedMigration]":
        """The migrations applied (as known at ``as_of``, if given), by id."""
        self._require_python("migrations")
        from rhizodat.migrations.runner import schema_versions

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
            close = getattr(s, "close", None)
            if callable(close):
                close()
        if self.journal is not None:
            close = getattr(self.journal, "close", None)
            if callable(close):
                close()

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
