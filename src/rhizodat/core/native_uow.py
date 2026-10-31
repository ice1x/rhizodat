"""Running a Unit of Work on the native core (``rhizodat_native``), opt-in.

When ``Rhizodat`` is built with native store objects (``PostgresStore``,
``DrevoStore``, ``ClickHouseStore``, ``IcebergStore`` from ``rhizodat_native``)
and a native journal (or none), its Units of Work commit through
``rhizodat_native.Coordinator``
instead of the Python coordinator: the whole protocol runs in Rust with no
Python callbacks on the path. A Unit of Work buffers its data on each store's
native unit — ``uow.store(name).insert(table, columns, rows)`` for a tabular
store, ``uow.store(name).run(cypher, params)`` for drevo,
``uow.store(name).append(table, arrow_data)`` for Iceberg — and the commit writes
it inside the meta-transaction.

Recovery runs on the core too: :class:`NativeRecovery` drives
``rhizodat_native.Recovery`` over the native journal and stores, and reports like
the Python :class:`~rhizodat.core.coordinator.Recovery`.

Repositories run on this path too, with their parts in native PostgreSQL stores
(:mod:`rhizodat.core.native_parts`). ``execute``, migrations and parts in the
other stores are not on it yet; use the Python facade for those.
"""

import functools
import importlib
import uuid
from collections.abc import Callable
from types import ModuleType, TracebackType
from typing import Any, Self

from rhizodat.core.coordinator import (
    Aborted,
    Committed,
    CommittedPending,
    InDoubt,
    RecoveryReport,
)
from rhizodat.core.unit_of_work import UnitOfWorkClosed

__all__ = [
    "NativeRecovery",
    "NativeUnitOfWork",
    "journal_classes",
    "native_module",
    "store_classes",
]

Outcome = Committed | CommittedPending


@functools.cache
def native_module() -> ModuleType | None:
    """``rhizodat_native`` for the facade's native path, imported on demand.

    Unlike :func:`rhizodat.core.native.load`, this ignores ``RHIZODAT_NATIVE=0``:
    that flag governs whether the package uses the module by default (``new_id``
    and the like), not an explicit choice to build a native-backed facade.
    """
    try:
        return importlib.import_module("rhizodat_native")
    except ImportError:
        return None


def store_classes(native: ModuleType) -> tuple[type, ...]:
    """The native store types a facade may be configured with."""
    return (native.PostgresStore, native.DrevoStore, native.ClickHouseStore, native.IcebergStore)


def journal_classes(native: ModuleType) -> tuple[type, ...]:
    """The native journal types a native facade accepts."""
    return (native.MemoryJournal, native.PostgresJournal, native.DrevoJournal)


class NativeUnitOfWork:
    """One meta-transaction that commits through ``rhizodat_native.Coordinator``.

    ``store(name)`` returns that store's native unit, on which the caller buffers
    the Unit of Work's own data; ``commit`` hands every touched unit to the
    coordinator. The interface the facade's ``transactional`` retry loop needs
    (``commit``, ``rollback``, ``is_retryable``, ``full_store``) mirrors the
    Python :class:`~rhizodat.core.unit_of_work.UnitOfWork`, so the loop drives
    either kind unchanged.
    """

    def __init__(
        self,
        dat_txn: uuid.UUID,
        stores: Callable[[str], Any],
        coordinator: Any,  # noqa: ANN401 - rhizodat_native.Coordinator (an extension type)
        native: ModuleType,
        *,
        repositories: "Callable[[Any, type], Any] | None" = None,
    ) -> None:
        self.dat_txn = dat_txn
        self._repositories = repositories
        self._repos: dict[type, Any] = {}
        self.outcome: Outcome | None = None
        self._stores = stores
        self._coordinator = coordinator
        self._native = native
        self._units: dict[str, Any] = {}
        self._closed = False

    @property
    def stores_touched(self) -> tuple[str, ...]:
        return tuple(self._units)

    def store(self, name: str) -> Any:  # noqa: ANN401 - the store's native unit type
        """This Unit of Work's native unit in store ``name``: buffer its data on it
        (``insert`` for a tabular store, ``run`` for drevo, ``append`` for Iceberg)."""
        self._check_open()
        unit = self._units.get(name)
        if unit is None:
            unit = self._units[name] = self._stores(name).unit()
        return unit

    def repository(self, entity_type: type) -> Any:  # noqa: ANN401 - Repository[entity_type]
        """CRUD for ``entity_type`` inside this Unit of Work: its parts are buffered
        on the stores' native units (PostgreSQL only on this path so far)."""
        self._check_open()
        if self._repositories is None:
            raise RuntimeError("this Unit of Work has no repositories (made outside Rhizodat)")
        repo = self._repos.get(entity_type)
        if repo is None:
            repo = self._repos[entity_type] = self._repositories(self, entity_type)
        return repo

    def commit(self) -> Outcome:
        self._check_open()
        self._closed = True
        if not self._units:
            self.outcome = Committed(self.dat_txn, None)
            return self.outcome
        try:
            result = self._coordinator.commit(self.dat_txn, list(self._units.values()))
        except self._native.AbortedError as exc:
            raise Aborted(
                self.dat_txn, retryable=bool(getattr(exc, "retryable", False)), reason=str(exc)
            ) from exc
        except self._native.InDoubtError as exc:
            raise InDoubt(self.dat_txn) from exc
        self.outcome = _outcome(self.dat_txn, result)
        return self.outcome

    def rollback(self) -> None:
        # Nothing is staged before commit: the units only hold buffered data, so
        # dropping them is the whole rollback.
        if self._closed:
            return
        self._closed = True
        self._units.clear()

    def is_retryable(self, error: BaseException) -> bool:
        """Whether ``error`` is a transient native abort (what the retry loop re-runs on)."""
        return isinstance(error, self._native.AbortedError) and bool(
            getattr(error, "retryable", False)
        )

    def full_store(self, error: BaseException) -> str | None:
        # The native path does not report a distinct full-store outcome yet.
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.commit()
        elif isinstance(exc, Exception):
            self.rollback()
        # A BaseException (shutdown, crash) leaves everything as is for recovery.

    def _check_open(self) -> None:
        if self._closed:
            raise UnitOfWorkClosed(f"unit of work {self.dat_txn} is already finished")


def _outcome(dat_txn: uuid.UUID, result: Any) -> Outcome:  # noqa: ANN401 - coordinator result dict
    """Map the coordinator's ``{"status", "dat_txn", "dat_tx_time"}`` to an ``Outcome``."""
    dat_tx_time = result["dat_tx_time"]
    if result["status"] == "committed":
        return Committed(dat_txn, dat_tx_time)
    return CommittedPending(dat_txn, dat_tx_time)


class NativeRecovery:
    """Recovery of a native facade: ``rhizodat_native.Recovery`` over its journal
    and stores, reporting a :class:`~rhizodat.core.coordinator.RecoveryReport`."""

    def __init__(
        self,
        native: ModuleType,
        journal: Any,  # noqa: ANN401 - a native journal (an extension type)
        stores: list[Any],
        *,
        stale_after_ms: int = 30_000,
        alert_after_ms: int = 600_000,
        wall_ms: Callable[[], int] | None = None,
    ) -> None:
        self._recovery = native.Recovery(
            journal,
            stores,
            stale_after_ms=stale_after_ms,
            alert_after_ms=alert_after_ms,
            wall_ms=wall_ms,
        )

    def run_once(self, *, limit: int = 100) -> RecoveryReport:
        report = self._recovery.run_once(limit=limit)
        return RecoveryReport(
            aborted=list(report["aborted"]),
            finished=list(report["finished"]),
            unfinished=list(report["unfinished"]),
            stuck=list(report["stuck"]),
            orphans=list(report["orphans"]),
        )
