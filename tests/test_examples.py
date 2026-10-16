"""The examples in examples/ run as they are, against the test stores.

They are what the documentation shows, so they must not rot: each runs as a
script with the stores pointed at this run's own schema and database.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def env(pg_dsn: str, pg_schema: str, clickhouse_dsn: str, drevo_bolt_dsn: str) -> dict[str, str]:
    sep = "&" if "?" in pg_dsn else "?"
    pg = f"{pg_dsn}{sep}schema={pg_schema}"
    return {
        **os.environ,
        "RHIZODAT_PG": pg,
        "RHIZODAT_JOURNAL": pg,
        "RHIZODAT_CLICKHOUSE": clickhouse_dsn,
        "RHIZODAT_GRAPH": drevo_bolt_dsn,
    }


def run(script: str, env: dict[str, str]) -> list[str]:
    done = subprocess.run(
        [sys.executable, str(EXAMPLES / script)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return done.stdout.splitlines()


def test_quickstart(env: dict[str, str]) -> None:
    assert run("quickstart.py", env) == ["paid", "new", "['new', 'paid']"]


def test_transactions(env: dict[str, str]) -> None:
    [line] = run("transactions.py", env)
    assert line.startswith("claimed task ")


def test_bitemporal(env: dict[str, str]) -> None:
    assert run("bitemporal.py", env) == ["10.00 8.00 10.00", "10.00"]


def test_migrate_twice(env: dict[str, str]) -> None:
    first = run("migrate.py", env)
    assert first[0] == "applied (1, 2), already ()"
    assert [line.split()[:2] for line in first[1:]] == [["1", "tasks"], ["2", "points"]]
    assert run("migrate.py", env)[0] == "applied (), already (1, 2)"


def test_import_csv(env: dict[str, str]) -> None:
    out = run("import_csv.py", env)
    assert out[0] == "100 rows, migration 1"
    assert "  channel: string (low cardinality)" in out


def test_the_readme_shows_the_quickstart_as_it_is() -> None:
    """Every line of the README's Python block is a line of examples/quickstart.py."""
    readme = (EXAMPLES.parent / "README.md").read_text()
    block = readme.split("```python\n", 1)[1].split("```", 1)[0]
    script = {line.strip() for line in (EXAMPLES / "quickstart.py").read_text().splitlines()}
    missing = [line for line in block.splitlines() if line.strip() and line.strip() not in script]
    assert missing == []
