"""Package skeleton: layout, metadata and optional store drivers."""

import importlib
import importlib.metadata
import importlib.resources
import subprocess
import sys

import pytest

import rhizodat

CORE_MODULES = [
    "identity",
    "clock",
    "mapping",
    "repository",
    "unit_of_work",
    "coordinator",
    "journal",
    "result",
]
ADAPTER_MODULES = ["postgres", "drevo", "clickhouse", "iceberg"]
STORE_EXTRAS = {"postgres", "clickhouse", "drevo", "iceberg"}
DRIVER_MODULES = ["psycopg", "clickhouse_connect", "drevo", "neo4j", "pyiceberg"]


def test_version_matches_distribution_metadata() -> None:
    assert rhizodat.__version__ == importlib.metadata.version("rhizodat")


@pytest.mark.parametrize("name", CORE_MODULES)
def test_core_module_is_importable(name: str) -> None:
    importlib.import_module(f"rhizodat.core.{name}")


@pytest.mark.parametrize("name", ADAPTER_MODULES)
def test_adapter_module_is_importable(name: str) -> None:
    importlib.import_module(f"rhizodat.adapters.{name}")


def test_migrations_package_is_importable() -> None:
    importlib.import_module("rhizodat.migrations")


def test_package_ships_type_information() -> None:
    assert importlib.resources.files("rhizodat").joinpath("py.typed").is_file()


def test_store_drivers_are_optional_extras() -> None:
    extras = set(importlib.metadata.metadata("rhizodat").get_all("Provides-Extra") or [])
    assert extras >= STORE_EXTRAS


def test_importing_rhizodat_loads_no_store_driver() -> None:
    modules = ["rhizodat", *(f"rhizodat.adapters.{m}" for m in ADAPTER_MODULES)]
    code = (
        "import importlib, sys\n"
        f"for m in {modules!r}: importlib.import_module(m)\n"
        f"print(sorted(d for d in {DRIVER_MODULES!r} if d in sys.modules))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "[]"


def test_mcp_is_an_optional_extra_and_the_tools_do_not_need_it() -> None:
    extras = set(importlib.metadata.metadata("rhizodat").get_all("Provides-Extra") or [])
    assert "mcp" in extras
    code = (
        "import sys\n"
        "import rhizodat.mcp, rhizodat.mcp.config, rhizodat.mcp.tools, rhizodat.mcp.server\n"
        f"print(sorted(d for d in ['mcp', *{DRIVER_MODULES!r}] if d in sys.modules))\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "[]"


def test_the_adapter_contract_kit_ships_as_an_extra() -> None:
    extras = set(importlib.metadata.metadata("rhizodat").get_all("Provides-Extra") or [])
    assert "testing" in extras
    contract = importlib.import_module("rhizodat.testing.contract")
    assert hasattr(contract, "AdapterContract")
    code = "import sys, rhizodat\nprint('pytest' in sys.modules)\n"
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "False"  # only the kit needs pytest
