"""The Rust core is used when ``rhizodat_native`` is installed, and only then.

``RHIZODAT_NATIVE=0`` keeps the Python implementation even when it is.
"""

import sys
import uuid

import pytest

from rhizodat.core import native
from rhizodat.core.identity import is_uuid7, new_id


def test_without_the_module_the_python_core_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "rhizodat_native", None)  # import fails
    monkeypatch.delenv(native.ENV, raising=False)
    assert native.load() is None


def test_the_module_can_be_turned_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(native.ENV, "0")
    assert native.load() is None


def test_an_installed_module_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    module = pytest.importorskip("rhizodat_native")
    monkeypatch.delenv(native.ENV, raising=False)
    assert native.load() is module


def test_new_id_makes_increasing_uuid7s_either_way() -> None:
    made = [new_id() for _ in range(100)]
    assert all(isinstance(i, uuid.UUID) and is_uuid7(i) for i in made)
    assert made == sorted(made)
    assert len(set(made)) == 100
