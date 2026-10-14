"""Unit of Work: one flat meta-transaction with one branch per store.

A branch is opened the first time the Unit of Work touches a store and lives
until commit or rollback. ``commit`` hands every branch to the coordinator; a
Unit of Work used as a ``with`` block commits on a clean exit, rolls back on an
exception and runs exactly once (retries need ``Datorium.transactional``).
"""

import uuid
from collections.abc import Callable
from types import TracebackType
from typing import Any, Protocol, Self

from datorium.core.coordinator import Branch, Committed, CommittedPending, Coordinator, Participant
from datorium.core.result import Result

__all__ = ["Store", "UnitOfWork", "UnitOfWorkClosed"]


class Store(Protocol):
    """A store as the facade sees it; adapters implement it."""

    name: str
    two_phase: bool

    def branch(self, dat_txn: uuid.UUID) -> Branch: ...
    def participant(self) -> Participant: ...
    def execute(self, query: str, params: Any = None) -> Result:  # noqa: ANN401 - driver params
        ...
    def close(self) -> None: ...


class UnitOfWorkClosed(RuntimeError):
    """The Unit of Work was already committed or rolled back."""


class UnitOfWork:
    def __init__(
        self,
        dat_txn: uuid.UUID,
        stores: Callable[[str], Store],
        coordinator: Coordinator,
        *,
        repositories: "Callable[[UnitOfWork, type], Any] | None" = None,
    ) -> None:
        self.dat_txn = dat_txn
        self._repositories = repositories
        self._repos: dict[type, Any] = {}
        self.outcome: Committed | CommittedPending | None = None
        self._stores = stores
        self._coordinator = coordinator
        self._branches: dict[str, Any] = {}
        self._closed = False

    @property
    def stores_touched(self) -> tuple[str, ...]:
        return tuple(self._branches)

    def store(self, name: str) -> Any:  # noqa: ANN401 - the adapter's branch type
        """This Unit of Work's branch in store ``name``: run native statements on it."""
        self._check_open()
        branch = self._branches.get(name)
        if branch is None:
            branch = self._stores(name).branch(self.dat_txn)
            self._branches[name] = branch
        return branch

    def repository(self, entity_type: type) -> Any:  # noqa: ANN401 - Repository[entity_type]
        """CRUD for ``entity_type`` inside this Unit of Work (``datorium.core.repository``)."""
        self._check_open()
        if self._repositories is None:
            raise RuntimeError("this Unit of Work has no repositories (made outside Datorium)")
        repo = self._repos.get(entity_type)
        if repo is None:
            repo = self._repos[entity_type] = self._repositories(self, entity_type)
        return repo

    def commit(self) -> Committed | CommittedPending:
        self._check_open()
        self._closed = True
        self.outcome = self._coordinator.commit(self.dat_txn, list(self._branches.values()))
        return self.outcome

    def rollback(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._coordinator.rollback(self.dat_txn, list(self._branches.values()))

    def is_retryable(self, error: BaseException) -> bool:
        """Whether any touched store calls ``error`` transient."""
        return any(b.is_retryable(error) for b in self._branches.values())

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
