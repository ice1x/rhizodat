"""The built wheel installs into a clean environment without any store driver."""

import json
import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]

pytestmark = pytest.mark.skipif(shutil.which("uv") is None, reason="uv is required")


def run(*args: str | Path) -> str:
    return subprocess.run([str(a) for a in args], capture_output=True, text=True, check=True).stdout


@pytest.fixture(scope="module")
def wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("dist")
    run("uv", "build", "--wheel", "--out-dir", out, ROOT)
    (path,) = out.glob("rhizodat-*.whl")
    return path


def test_wheel_contains_the_package_tree(wheel: Path) -> None:
    names = set(zipfile.ZipFile(wheel).namelist())
    assert "rhizodat/py.typed" in names
    assert "rhizodat/core/coordinator.py" in names
    assert "rhizodat/adapters/iceberg.py" in names
    assert not any(n.startswith("tests/") for n in names)


def test_wheel_installs_and_imports_in_a_clean_venv(wheel: Path, tmp_path: Path) -> None:
    venv = tmp_path / "venv"
    run("uv", "venv", "--python", sys.executable, venv)
    python = venv / "bin" / "python"
    run("uv", "pip", "install", "--python", python, wheel)

    installed = json.loads(run("uv", "pip", "list", "--python", python, "--format", "json"))
    assert [p["name"] for p in installed] == ["rhizodat"]

    code = "import rhizodat, rhizodat.core, rhizodat.adapters; print(rhizodat.__version__)"
    assert run(python, "-c", code).strip() == VERSION
