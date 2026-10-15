"""``datorium migrate``: run a migration directory from an operator shell.

Stores and the journal come from the environment (``DATORIUM_STORE_<NAME>``,
``DATORIUM_JOURNAL``), so DSNs and their secrets stay out of the command line.
A story editor's deploy script runs it before starting the new release.
"""

import json
from pathlib import Path

import pytest
from faker import Faker

from datorium.check import main

CHAPTERS = '[[steps]]\nstore = "pg"\nsql = "CREATE TABLE IF NOT EXISTS chapter (id int)"\n'
SCENES = '[[steps]]\nstore = "pg"\nsql = "CREATE TABLE IF NOT EXISTS scene (id int)"\n'


@pytest.fixture
def env(
    monkeypatch: pytest.MonkeyPatch, pg_dsn: str, pg_schema: str, tmp_path: Path
) -> dict[str, str]:
    sep = "&" if "?" in pg_dsn else "?"
    values = {
        "DATORIUM_STORE_PG": f"{pg_dsn}{sep}schema={pg_schema}",
        "DATORIUM_JOURNAL": f"file://{tmp_path / 'journal.db'}",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


def folder(tmp_path: Path, *files: tuple[str, str]) -> Path:
    root = tmp_path / "migrations"
    root.mkdir(exist_ok=True)
    for name, text in files:
        (root / name).write_text(text)
    return root


def test_migrate_runs_reports_and_is_repeatable(
    env: dict[str, str], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = folder(tmp_path, ("0001_chapters.toml", CHAPTERS), ("0002_scenes.toml", SCENES))
    assert main(["migrate", str(root), "--dry-run"]) == 0
    assert "pending: 1, 2" in capsys.readouterr().out

    assert main(["migrate", str(root), "--to", "1"]) == 0
    out = capsys.readouterr().out
    assert "applied: 1" in out
    assert "pending: 2" in out

    assert main(["migrate", str(root), "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["applied"] == [2]
    assert report["already"] == [1]
    assert [v["id"] for v in report["schema_versions"]] == [1, 2]
    assert report["schema_versions"][0]["name"] == "chapters"


def test_migrate_fails_with_the_reason_and_status_2_or_1(
    env: dict[str, str],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    faker: Faker,
) -> None:
    root = folder(tmp_path, ("0001_chapters.toml", CHAPTERS.replace("CREATE", faker.word())))
    assert main(["migrate", str(root)]) == 1
    err = capsys.readouterr().err
    assert "migration 1 (chapters), step 1 on store 'pg' failed" in err

    assert main(["migrate", str(tmp_path / "nowhere")]) == 2
    assert "no such directory" in capsys.readouterr().err

    (root / "0001_chapters.toml").write_text('[[steps]]\nstore = "missing"\nsql = "SELECT 1"\n')
    assert main(["migrate", str(root)]) == 2  # refused before anything ran
    assert "no store named 'missing'" in capsys.readouterr().err

    monkeypatch.delenv("DATORIUM_STORE_PG")
    assert main(["migrate", str(root)]) == 2
    assert "DATORIUM_STORE_" in capsys.readouterr().err


def test_secrets_never_reach_the_output(
    env: dict[str, str],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    faker: Faker,
) -> None:
    secret = faker.pystr(min_chars=24, max_chars=24)
    monkeypatch.setenv("DATORIUM_STORE_PG", f"postgresql://writer:{secret}@127.0.0.1:1/app")
    root = folder(tmp_path, ("0001_chapters.toml", CHAPTERS))
    assert main(["migrate", str(root)]) == 1  # the store is unreachable
    captured = capsys.readouterr()
    assert "step 1 on store 'pg' failed" in captured.err
    assert secret not in captured.out + captured.err
