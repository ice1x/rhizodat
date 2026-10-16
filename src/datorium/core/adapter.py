"""Store adapters as plug-ins: what a store must declare to join datorium.

The core knows no store. An ``Adapter`` names its kind, the DSN schemes it
accepts and how to open a store (and, if the store is a shared ACID one, the
journal). datorium's own adapters register exactly like a third-party package,
which declares one entry point::

    [project.entry-points."datorium.adapters"]
    mysql = "datorium_mysql:ADAPTER"

or calls ``register_adapter`` itself. Adapters are loaded on first use; a plug-in
that fails to load is skipped and reported when a DSN needs a missing scheme.

What a store can do beyond ``Store`` is a capability protocol here
(``ReadOnlyQueries``, ``AppliedLocation``, ``Fenced``, ``Migrates``), checked
with ``isinstance``.
"""

import importlib
import importlib.metadata
import re
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datorium.core.journal import Journal
    from datorium.core.result import Result
    from datorium.core.unit_of_work import Store

__all__ = [
    "ENTRY_POINT_GROUP",
    "Adapter",
    "AppliedLocation",
    "Fenced",
    "Migrates",
    "ReadOnlyQueries",
    "Scheme",
    "adapter_for",
    "adapters",
    "register_adapter",
    "scheme",
]

ENTRY_POINT_GROUP = "datorium.adapters"

# datorium's own adapters, loaded the same way as entry points.
_BUILTIN = (
    ("file", "datorium.core.journal:FILE_ADAPTER"),
    ("postgres", "datorium.adapters.postgres:ADAPTER"),
    ("clickhouse", "datorium.adapters.clickhouse:ADAPTER"),
    ("drevo", "datorium.adapters.drevo:ADAPTER"),
    ("iceberg", "datorium.adapters.iceberg:ADAPTER"),
)
_KIND = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_SCHEME = re.compile(r"^[a-z][a-z0-9+.-]{0,31}$")

Presence = Literal["required", "optional", "none"]


@dataclass(frozen=True, slots=True)
class Scheme:
    """One DSN scheme and its rules: ``host`` and ``path`` required, optional or
    forbidden (``"none"``), the port used when a host has none."""

    name: str
    transport: str | None = None
    default_port: int | None = None
    host: Presence = "optional"
    path: Presence = "optional"


@dataclass(frozen=True, slots=True)
class Adapter:
    """A store kind as datorium sees it.

    - ``open_store(name, dsn)``: the ``Store``; ``None`` for a journal-only kind;
    - ``open_journal(dsn, wall_ms)``: the journal, only for a shared ACID store;
    - ``journal_location(dsn)``: where a journal at ``dsn`` lives, compared with
      the ``fence_location`` of ``Fenced`` stores;
    - ``query_help``: one line on the store's native query language, for clients
      such as datorium-MCP.
    """

    kind: str
    schemes: tuple[Scheme, ...]
    open_store: "Callable[[str, str], Store] | None" = None
    open_journal: "Callable[[str, Callable[[], int]], Journal] | None" = None
    journal_location: Callable[[str], tuple[str, ...] | None] | None = None
    query_help: str | None = None


# --- capabilities -----------------------------------------------------------------------


@runtime_checkable
class ReadOnlyQueries(Protocol):
    """Native reads the store itself refuses to turn into writes."""

    def read_only(self, query: str, params: Any = None) -> "Result":  # noqa: ANN401
        ...


@runtime_checkable
class AppliedLocation(Protocol):
    """Where the store's ``dat_applied`` markers live. Two stores in one place
    would block or confuse each other, so ``Datorium`` refuses them."""

    @property
    def applied_location(self) -> tuple[str, ...] | None: ...


@runtime_checkable
class Fenced(Protocol):
    """While a branch of this store is prepared, nothing else commits in
    ``fence_location`` (drevo's prepared fence), so the journal must live elsewhere."""

    @property
    def fence_location(self) -> tuple[str, ...] | None: ...


@runtime_checkable
class Migrates(Protocol):
    """Runs schema migration steps of the kinds it names (``sql``, ``cypher``, ...).

    A step must be safe to run again (``IF NOT EXISTS``, a check before a change):
    a migration over several stores is not atomic, and after a failure the whole
    migration runs again.
    """

    @property
    def migration_kinds(self) -> frozenset[str]: ...

    def run_migration_step(self, kind: str, body: Any) -> None:  # noqa: ANN401 - TOML data
        ...


# --- the registry -----------------------------------------------------------------------


def _entry_points(*, group: str) -> Sequence[importlib.metadata.EntryPoint]:
    return list(importlib.metadata.entry_points(group=group))


class _Registry:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.loaded = False
        self.by_kind: dict[str, Adapter] = {}
        self.by_scheme: dict[str, tuple[Adapter, Scheme]] = {}
        self.errors: dict[str, str] = {}

    def add(self, adapter: Adapter) -> None:
        if not isinstance(adapter, Adapter):
            raise TypeError(f"not a datorium Adapter: {type(adapter).__name__}")
        if not _KIND.match(adapter.kind):
            raise ValueError(
                f"invalid adapter kind {adapter.kind!r}: lower-case letters, digits, _"
            )
        if adapter.kind in self.by_kind:
            raise ValueError(f"an adapter for {adapter.kind!r} is already registered")
        if not adapter.schemes:
            raise ValueError(f"adapter {adapter.kind!r} declares no DSN scheme")
        names = [s.name.lower() for s in adapter.schemes]
        for s, name in zip(adapter.schemes, names, strict=True):
            if not _SCHEME.match(s.name.lower()):
                raise ValueError(f"invalid DSN scheme {s.name!r} for adapter {adapter.kind!r}")
            if names.count(name) > 1:
                raise ValueError(f"adapter {adapter.kind!r} declares {name}:// twice")
            if name in self.by_scheme:
                other = self.by_scheme[name][0].kind
                raise ValueError(f"{name}:// already belongs to the {other!r} adapter")
        self.by_kind[adapter.kind] = adapter
        for s, name in zip(adapter.schemes, names, strict=True):
            self.by_scheme[name] = (adapter, s)

    def load(self) -> None:
        with self.lock:
            if self.loaded:
                return
            self.loaded = True
            for name, target in _BUILTIN:
                self._load_one(name, target, builtin=True)
            for ep in _entry_points(group=ENTRY_POINT_GROUP):
                self._load_one(ep.name, ep.value, builtin=False)

    def _load_one(self, name: str, target: str, *, builtin: bool) -> None:
        module, _, attr = target.partition(":")
        try:
            self.add(getattr(importlib.import_module(module), attr))
        except Exception as exc:
            if builtin:
                raise
            self.errors[name] = f"{type(exc).__name__}: {exc}"


_registry = _Registry()


def register_adapter(adapter: Adapter) -> None:
    """Register a store kind in this process (entry points do it on installation)."""
    _registry.load()
    with _registry.lock:
        _registry.add(adapter)


def adapters() -> tuple[Adapter, ...]:
    """Every registered adapter, datorium's own first."""
    _registry.load()
    return tuple(_registry.by_kind.values())


def adapter_for(kind: str) -> Adapter:
    _registry.load()
    try:
        return _registry.by_kind[kind]
    except KeyError:
        raise KeyError(
            f"no adapter for store kind {kind!r}; registered: {sorted(_registry.by_kind)}"
        ) from None


def scheme(name: str) -> tuple[Adapter, Scheme] | None:
    """The adapter and rules of a DSN scheme (lower-case), if one is registered."""
    _registry.load()
    return _registry.by_scheme.get(name)


def schemes() -> tuple[str, ...]:
    _registry.load()
    return tuple(_registry.by_scheme)


def load_errors() -> dict[str, str]:
    """Plug-ins (entry point name → error) that failed to load."""
    _registry.load()
    return dict(_registry.errors)


# --- for tests --------------------------------------------------------------------------


_Saved = tuple[bool, dict[str, Adapter], dict[str, tuple[Adapter, Scheme]], dict[str, str]]


def _snapshot() -> _Saved:
    r = _registry
    return r.loaded, dict(r.by_kind), dict(r.by_scheme), dict(r.errors)


def _restore(saved: _Saved) -> None:
    r = _registry
    r.loaded, r.by_kind, r.by_scheme, r.errors = (
        saved[0],
        dict(saved[1]),
        dict(saved[2]),
        dict(saved[3]),
    )


def _reset() -> None:
    """Forget every adapter: the next use loads them again."""
    r = _registry
    r.loaded, r.by_kind, r.by_scheme, r.errors = False, {}, {}, {}
