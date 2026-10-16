"""``rhizodat import``: load a file from an operator shell.

A CBT journal app moves its old mood log (CSV) into PostgreSQL. The dry run
shows the inferred columns and the migration the import would write; the real
run writes it into the migration directory, runs it and loads the rows.
"""

import json
import re
from pathlib import Path

import pytest
from faker import Faker

from rhizodat.check import main


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, pg_schema: str, tmp_path: Path) -> None:
    sep = "&" if "?" in pg_dsn else "?"
    monkeypatch.setenv("RHIZODAT_STORE_PG", f"{pg_dsn}{sep}schema={pg_schema}")
    monkeypatch.setenv("RHIZODAT_JOURNAL", f"file://{tmp_path / 'journal.db'}")


def mood_log(path: Path, faker: Faker, n: int) -> Path:
    rows = ["entry_id,felt_at,mood,note"]
    for i in range(n):
        rows.append(
            f"{i},{faker.date_time_between('-1y').strftime('%Y-%m-%d %H:%M:%S')},"
            f'{faker.random_int(1, 10)},"{faker.sentence()}"'
        )
    path.write_text("\n".join(rows) + "\n")
    return path


def test_dry_run_then_import(
    env: None, tmp_path: Path, capsys: pytest.CaptureFixture[str], faker: Faker
) -> None:
    source = mood_log(tmp_path / "moods.csv", faker, 25)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    args = [
        "import",
        str(source),
        "--store",
        "pg",
        "--table",
        "mood",
        "--migrations",
        str(migrations),
    ]

    assert main([*args, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert re.search(r"entry_id +int64", out)
    assert re.search(r"felt_at +timestamp", out)
    assert "0001_import_mood.toml" in out
    assert 'CREATE TABLE IF NOT EXISTS "mood"' in out
    assert list(migrations.iterdir()) == []

    assert main([*args, "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert (report["rows"], report["migration"]) == (25, 1)
    assert [c["name"] for c in report["columns"]] == ["entry_id", "felt_at", "mood", "note"]
    assert (migrations / "0001_import_mood.toml").exists()


def test_refusals_exit_2_and_load_failures_exit_1(
    env: None, tmp_path: Path, capsys: pytest.CaptureFixture[str], faker: Faker
) -> None:
    source = mood_log(tmp_path / "moods.csv", faker, 5)
    assert main(["import", str(source), "--store", "pg", "--table", "mood"]) == 2
    assert "--migrations DIR" in capsys.readouterr().err
    assert main(["import", str(tmp_path / "x.xlsx"), "--store", "pg", "--table", "t"]) == 2
    assert "format" in capsys.readouterr().err

    # A column that does not fit the existing table: the store refuses the rows.
    migrations = tmp_path / "m"
    migrations.mkdir()
    assert (
        main(
            [
                "import",
                str(source),
                "--store",
                "pg",
                "--table",
                "mood",
                "--migrations",
                str(migrations),
            ]
        )
        == 0
    )
    capsys.readouterr()
    wrong = tmp_path / "wrong.csv"
    wrong.write_text("entry_id,felt_at,mood,note\nx,2026-01-01 10:00:00,3,hi\n")
    assert main(["import", str(wrong), "--store", "pg", "--table", "mood"]) == 1
    assert "load failed" in capsys.readouterr().err


def test_a_secret_in_the_source_is_never_printed(
    env: None, capsys: pytest.CaptureFixture[str], faker: Faker
) -> None:
    secret = faker.pystr(min_chars=24, max_chars=24)
    source = f"s3://reader:{secret}@bucket/moods.csv?endpoint_override=127.0.0.1:1&scheme=http"
    status = main(["import", source, "--store", "pg", "--table", "mood", "--dry-run"])
    captured = capsys.readouterr()
    assert status != 0
    assert secret not in captured.out + captured.err
