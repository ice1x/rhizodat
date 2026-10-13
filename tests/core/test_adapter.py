"""The adapter registry: stores are plugged in, not built in.

An adapter declares its DSN schemes and how to open a store (and, if it can hold
one, the journal). datorium's own adapters register the same way a third-party
package does through the ``datorium.adapters`` entry point group.
"""

import importlib.metadata
import sys
import types
from collections.abc import Iterator
from typing import Any

import pytest
from faker import Faker

from datorium import Adapter, Datorium, Scheme, register_adapter
from datorium.core import adapter as registry
from datorium.core.adapter import (
    ENTRY_POINT_GROUP,
    AppliedLocation,
    Fenced,
    ReadOnlyQueries,
    adapter_for,
    adapters,
)
from datorium.core.dsn import DSNError, parse_dsn
from datorium.core.journal import MemoryJournal, State, open_journal
from fakes import FakeStoreHandle, World

BUILTIN = {"file", "postgres", "clickhouse", "drevo", "iceberg"}


@pytest.fixture(autouse=True)
def isolated_registry() -> Iterator[None]:
    saved = registry._snapshot()
    yield
    registry._restore(saved)


class KvWorld:
    """A made-up key-value store: what a third-party adapter would wrap."""

    def __init__(self) -> None:
        self.world = World()
        self.opened: list[tuple[str, str]] = []
        self.journals: dict[str, MemoryJournal] = {}

    def open_store(self, name: str, dsn: str) -> FakeStoreHandle:
        self.opened.append((name, dsn))
        return FakeStoreHandle(self.world.store(name, two_phase=True))

    def open_journal(self, dsn: str, wall_ms: Any) -> MemoryJournal:
        return self.journals.setdefault(dsn, MemoryJournal(wall_ms=wall_ms))

    def adapter(self, **overrides: Any) -> Adapter:
        fields: dict[str, Any] = {
            "kind": "kv",
            "schemes": (Scheme("kv", default_port=7000, host="required"),),
            "open_store": self.open_store,
            "open_journal": self.open_journal,
            "query_help": "kv: GET <key>",
        }
        return Adapter(**{**fields, **overrides})


@pytest.fixture
def kv() -> KvWorld:
    return KvWorld()


# --- built-in adapters ------------------------------------------------------------------


def test_datoriums_own_stores_are_registered_adapters() -> None:
    assert {a.kind for a in adapters()} >= BUILTIN
    assert adapter_for("postgres").open_journal is not None
    assert adapter_for("drevo").open_journal is not None
    assert adapter_for("clickhouse").open_journal is None  # no ACID: cannot hold the journal
    assert adapter_for("iceberg").open_journal is None
    assert adapter_for("file").open_store is None  # journal only
    for a in adapters():
        if a.open_store is not None:
            assert a.query_help, a.kind


def test_an_unknown_kind_names_the_registered_ones() -> None:
    with pytest.raises(KeyError, match="postgres"):
        adapter_for("mysql")


# --- a third-party adapter --------------------------------------------------------------


def test_a_registered_adapter_parses_its_dsns(kv: KvWorld, faker: Faker) -> None:
    register_adapter(kv.adapter())
    host = faker.hostname()
    dsn = parse_dsn(f"kv://{host}/space")
    assert (dsn.store, dsn.host, dsn.port, dsn.database) == ("kv", host, 7000, "space")
    with pytest.raises(DSNError, match="host"):
        parse_dsn("kv:///space")


def test_scheme_rules_are_declared_by_the_adapter(kv: KvWorld) -> None:
    register_adapter(
        kv.adapter(
            schemes=(
                Scheme("kv", host="required"),
                Scheme("kv+file", transport="file", host="none", path="required"),
                Scheme("kv+mem", transport="memory", host="none", path="none"),
            )
        )
    )
    assert parse_dsn("kv+file:///data/x.kv").transport == "file"
    for bad in ("kv+file://h/data", "kv+file://", "kv+mem://h", "kv+mem:///x"):
        with pytest.raises(DSNError):
            parse_dsn(bad)
    assert parse_dsn("kv+mem://").transport == "memory"


def test_a_meta_transaction_over_a_third_party_store_and_journal(kv: KvWorld, faker: Faker) -> None:
    """An IT task manager on a key-value store plugged in by its own package: two
    stores and the journal all come from the adapter, and the commit protocol is
    datorium's."""
    register_adapter(kv.adapter())
    dat = Datorium(
        stores={"tasks": "kv://kv1/tasks", "audit": "kv://kv2/audit"},
        journal="kv://kv3/journal",
    )
    assert kv.opened == [("tasks", "kv://kv1/tasks"), ("audit", "kv://kv2/audit")]
    with dat.unit_of_work() as uow:
        uow.store("tasks").execute(faker.sentence())
        uow.store("audit").execute(faker.sentence())
    assert uow.outcome is not None
    record = kv.journals["kv://kv3/journal"].get(uow.outcome.dat_txn)
    assert record is not None
    assert record.state is State.DONE
    assert set(kv.world.applied(uow.outcome.dat_txn).values()) == {True}
    dat.close()


def test_an_adapter_without_a_journal_cannot_hold_one(kv: KvWorld) -> None:
    register_adapter(kv.adapter(open_journal=None))
    with pytest.raises(ValueError, match="kv cannot hold the journal") as caught:
        open_journal("kv://h/j")
    message = str(caught.value)
    for kind in ("postgres", "drevo", "file"):
        assert kind in message  # the kinds that can


def test_a_journal_only_adapter_cannot_be_a_store() -> None:
    with pytest.raises(ValueError, match="file cannot be a store"):
        Datorium(stores={"x": "file:///tmp/x.db"})


@pytest.mark.parametrize(
    ("bad", "error"),
    [
        ({"kind": "postgres"}, "already registered"),
        ({"schemes": (Scheme("postgresql"),)}, "already belongs to the 'postgres'"),
        ({"schemes": (Scheme("kv"), Scheme("KV"))}, "twice"),
        ({"schemes": ()}, "no DSN scheme"),
        ({"kind": "Key Value"}, "invalid adapter kind"),
        ({"schemes": (Scheme("kv:x"),)}, "invalid DSN scheme"),
    ],
)
def test_registration_refuses_ambiguous_or_invalid_adapters(
    kv: KvWorld, bad: dict[str, Any], error: str
) -> None:
    with pytest.raises(ValueError, match=error):
        register_adapter(kv.adapter(**bad))
    assert "kv" not in {a.kind for a in adapters()}


# --- entry points -----------------------------------------------------------------------


def fake_entry_points(monkeypatch: pytest.MonkeyPatch, *points: tuple[str, str]) -> None:
    eps = [importlib.metadata.EntryPoint(n, v, ENTRY_POINT_GROUP) for n, v in points]

    def select(*, group: str) -> list[importlib.metadata.EntryPoint]:
        return eps if group == ENTRY_POINT_GROUP else []

    monkeypatch.setattr(registry, "_entry_points", select)
    registry._reset()


def test_installed_packages_register_through_entry_points(
    kv: KvWorld, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = types.ModuleType("datorium_kv_plugin")
    module.ADAPTER = kv.adapter()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datorium_kv_plugin", module)
    fake_entry_points(monkeypatch, ("kv", "datorium_kv_plugin:ADAPTER"))
    assert {a.kind for a in adapters()} == BUILTIN | {"kv"}
    assert parse_dsn("kv://h/x").store == "kv"


def test_a_broken_plugin_does_not_break_the_others_and_is_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = types.ModuleType("datorium_bad_plugin")
    module.ADAPTER = "not an adapter"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "datorium_bad_plugin", module)
    fake_entry_points(
        monkeypatch,
        ("missing", "datorium_no_such_module:ADAPTER"),
        ("bad", "datorium_bad_plugin:ADAPTER"),
    )
    assert {a.kind for a in adapters()} == BUILTIN
    with pytest.raises(DSNError) as caught:
        parse_dsn("mysql://h/db")
    message = str(caught.value)
    assert "'missing'" in message
    assert "'bad'" in message


# --- capabilities -----------------------------------------------------------------------


def test_capabilities_are_protocols(world_store: FakeStoreHandle) -> None:
    from datorium.adapters.postgres import PostgresStore

    pg = PostgresStore("postgresql://db.local/app", schema="orders")
    assert isinstance(pg, ReadOnlyQueries)
    assert isinstance(pg, AppliedLocation)
    assert not isinstance(pg, Fenced)
    assert not isinstance(world_store, ReadOnlyQueries)
    assert not isinstance(world_store, AppliedLocation)
    pg.close()


@pytest.fixture
def world_store() -> FakeStoreHandle:
    return FakeStoreHandle(World().store("plain", two_phase=True))


class FencedHandle(FakeStoreHandle):
    def __init__(self, where: tuple[str, ...]) -> None:
        super().__init__(World().store("fenced", two_phase=True))
        self.fence_location = where


def test_a_journal_inside_a_stores_fence_is_refused(kv: KvWorld) -> None:
    """While a branch of a fenced store is prepared, nothing else commits there:
    a journal in the same place would block the coordinator's own decision."""
    register_adapter(kv.adapter(journal_location=lambda dsn: ("kv", parse_dsn(dsn).host or "")))
    fenced = FencedHandle(("kv", "kv1"))
    with pytest.raises(ValueError, match="fence") as caught:
        Datorium(stores={"tasks": fenced}, journal="kv://kv1/journal")
    assert "'tasks'" in str(caught.value)
    assert fenced.closed
    Datorium(stores={"tasks": FencedHandle(("kv", "kv1"))}, journal="kv://kv2/journal").close()


def test_parsing_a_dsn_needs_no_driver() -> None:
    import subprocess

    code = (
        "import sys\n"
        "from datorium.core.dsn import parse_dsn\n"
        "for d in ['postgresql://h/d', 'clickhouse://h/d', 'drevo+bolt://h', 'iceberg+rest://h/w']:\n"
        "    parse_dsn(d)\n"
        "print(sorted(m for m in ['psycopg', 'clickhouse_connect', 'drevo', 'neo4j', 'pyiceberg']"
        " if m in sys.modules))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[]"
