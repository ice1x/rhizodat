"""The operator commands refuse cleanly: no traceback, a message and an exit code.

Exit 2 is a refusal before anything is written (no stores, a store that does
not open, arguments that do not fit); exit 1 is a store failing during the
work, which leaves the data as it was.
"""

import subprocess
import sys
from pathlib import Path

import pytest
from faker import Faker

from rhizodat.check import main

UNREACHABLE_GRAPH = "drevo+bolt://127.0.0.1:1"


@pytest.fixture
def bare_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No store and no journal configured, whatever the shell running the tests has."""
    import os

    for key in list(os.environ):
        if key.startswith("RHIZODAT_STORE_") or key == "RHIZODAT_JOURNAL":
            monkeypatch.delenv(key)
    return monkeypatch


@pytest.fixture
def table_file(tmp_path: Path, faker: Faker) -> Path:
    path = tmp_path / "hosts.csv"
    rows = ["host_id,name"] + [f"{i},{faker.hostname()}" for i in range(3)]
    path.write_text("\n".join(rows) + "\n")
    return path


@pytest.fixture
def rdf_file(tmp_path: Path) -> Path:
    path = tmp_path / "fleet.ttl"
    path.write_text(
        "@prefix ex: <https://example.org/fleet#> .\n"
        'ex:web1 a ex:Host ; ex:name "web1" ; ex:runs-on ex:rack1 .\n'
    )
    return path


def commands(table_file: Path, rdf_file: Path) -> dict[str, list[str]]:
    return {
        "import table": ["import", str(table_file), "--store", "s", "--table", "hosts"],
        "import graph": ["import", str(rdf_file), "--store", "s"],
        "relate": ["relate", str(table_file), "--store", "s"],
        "embed": ["embed", "--store", "s", "--label", "Host", "--text", "name"],
        "similar": ["similar", "--store", "s", "--label", "Host", "web"],
    }


@pytest.mark.parametrize("command", ["import table", "import graph", "relate", "embed", "similar"])
def test_no_store_configured_is_a_refusal(
    command: str,
    bare_env: pytest.MonkeyPatch,
    table_file: Path,
    rdf_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(commands(table_file, rdf_file)[command]) == 2
    assert "no stores configured" in capsys.readouterr().err


@pytest.mark.parametrize("command", ["import table", "import graph", "relate", "embed", "similar"])
def test_a_store_that_does_not_open_is_a_refusal(
    command: str,
    bare_env: pytest.MonkeyPatch,
    table_file: Path,
    rdf_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bare_env.setenv("RHIZODAT_STORE_S", "nope://somewhere")
    assert main(commands(table_file, rdf_file)[command]) == 2
    err = capsys.readouterr().err
    assert "cannot open the stores: DSNError" in err
    assert "Traceback" not in err


@pytest.mark.parametrize("command", ["import graph", "relate", "embed", "similar"])
def test_a_store_failing_during_the_work_exits_1(
    command: str,
    bare_env: pytest.MonkeyPatch,
    table_file: Path,
    rdf_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    bare_env.setenv("RHIZODAT_STORE_S", UNREACHABLE_GRAPH)
    assert main(commands(table_file, rdf_file)[command]) == 1
    assert "ServiceUnavailable" in capsys.readouterr().err


def test_an_unknown_embedder_is_a_refusal(
    bare_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    bare_env.setenv("RHIZODAT_STORE_S", UNREACHABLE_GRAPH)
    args = ["similar", "--store", "s", "--label", "Host", "--model", "nope:Missing", "web"]
    assert main(args) == 2
    assert "cannot load 'nope:Missing'" in capsys.readouterr().err


def test_relate_refuses_two_files_of_one_table_name(
    bare_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bare_env.setenv("RHIZODAT_STORE_S", UNREACHABLE_GRAPH)
    for d in ("a", "b"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "hosts.csv").write_text("host_id\n1\n")
    args = ["relate", str(tmp_path / "a/hosts.csv"), str(tmp_path / "b/hosts.csv"), "--store", "s"]
    assert main(args) == 2
    assert "same name" in capsys.readouterr().err


def test_a_broken_rdf_file_is_a_refusal_in_a_dry_run(
    bare_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bare_env.setenv("RHIZODAT_STORE_S", UNREACHABLE_GRAPH)
    broken = tmp_path / "broken.ttl"
    broken.write_text("@prefix ex: <https://example.org/> .\nex:a ex:b .\n")
    assert main(["import", str(broken), "--store", "s", "--dry-run"]) == 2
    assert "broken.ttl" in capsys.readouterr().err


def test_python_m_rhizodat_mcp_runs_the_server(bare_env: pytest.MonkeyPatch) -> None:
    """``python -m rhizodat.mcp`` is the ``rhizodat-mcp`` command: it reads the
    same configuration and says what is missing."""
    import os

    env = {k: v for k, v in os.environ.items() if not k.startswith("RHIZODAT_STORE_")}
    done = subprocess.run(
        [sys.executable, "-m", "rhizodat.mcp"],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "no stores configured" in done.stderr
