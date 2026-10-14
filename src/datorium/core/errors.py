"""What a store's error means: worth a retry, a full store, a constraint, or a bug.

Adapters classify their own errors: a branch or participant may implement
``classify(error) -> ErrorKind`` (``Classifies``). One that only has
``is_retryable`` is classified by it (``TRANSIENT`` or ``PERMANENT``), and any
error caused by a device out of space (``ENOSPC``, ``EDQUOT``) is
``STORE_FULL`` whoever raised it.

A full store fails the meta-transaction with ``StoreFull``
(``datorium.core.coordinator``), an ``Aborted`` that names the store. Before
the decision that is all it takes: nothing is committed. A store written after
the decision (ClickHouse, Iceberg) can only fail before it if it is asked
first: a store with ``Capacity`` reports its free space, and with ``min_free``
set the coordinator refuses a meta-transaction touching a store below it,
before anything is written.
"""

import errno
from enum import Enum
from typing import Protocol, runtime_checkable

__all__ = ["Capacity", "Classifies", "ErrorKind", "classify", "no_space"]

_NO_SPACE = {errno.ENOSPC, errno.EDQUOT}


class ErrorKind(Enum):
    TRANSIENT = "transient"
    """Re-running the Unit of Work may succeed: a conflict, a timeout, a lost connection."""
    STORE_FULL = "store_full"
    """The store has no room left: disk, quota or memory for the write."""
    CONSTRAINT = "constraint"
    """The data breaks a rule of the store: unique key, foreign key, check."""
    PERMANENT = "permanent"
    """Anything else: re-running cannot help."""

    @property
    def retryable(self) -> bool:
        return self is ErrorKind.TRANSIENT


@runtime_checkable
class Classifies(Protocol):
    def classify(self, error: BaseException) -> ErrorKind: ...


class _Retryable(Protocol):
    def is_retryable(self, error: BaseException) -> bool: ...


@runtime_checkable
class Capacity(Protocol):
    """A store that can tell how much room it has left."""

    def free_space(self) -> int | None:
        """Bytes the store can still write, ``None`` when it cannot tell."""
        ...


def no_space(error: BaseException) -> bool:
    """Whether ``error`` or anything that caused it is a device out of space."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, OSError) and current.errno in _NO_SPACE:
            return True
        current = current.__cause__ or current.__context__
    return False


def classify(source: _Retryable, error: BaseException) -> ErrorKind:
    """``source.classify(error)`` (a branch or participant), else by ``is_retryable``."""
    if isinstance(source, Classifies):
        kind = source.classify(error)
        if kind is not ErrorKind.PERMANENT or not no_space(error):
            return kind
    if no_space(error):
        return ErrorKind.STORE_FULL
    return ErrorKind.TRANSIENT if source.is_retryable(error) else ErrorKind.PERMANENT
