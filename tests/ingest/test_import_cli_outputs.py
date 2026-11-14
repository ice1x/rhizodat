"""``rhizodat import`` outputs the other import tests do not reach: a dry run
in text, a table the file already fits, an RDF dry run and import with their
reports, and a store name that is not configured."""

import json
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from faker import Faker

from rhizodat import Rhizodat
from rhizodat.check import main
from rhizodat.ingest.cli import _scrub


@pytest.fixture
def pg_env(monkeypatch: pytest.MonkeyPatch, pg_dsn: str, pg_schema: str, tmp_path: Path) -> str:
    sep = "&" if "?" in pg_dsn else "?"
    dsn = f"{pg_dsn}{sep}schema={pg_schema}"
    monkeypatch.setenv("RHIZODAT_STORE_PG", dsn)
    monkeypatch.setenv("RHIZODAT_JOURNAL", f"file://{tmp_path / 'journal.db'}")
    return dsn


def hosts(path: Path, faker: Faker, n: int) -> Path:
    rows = ["host_id,name,cores"] + [f"{i},{faker.hostname()},{2 ** (i % 4)}" for i in range(n)]
    path.write_text("\n".join(rows) + "\n")
    return path


def test_a_dry_run_in_text_shows_the_columns_and_then_that_the_table_fits(
    pg_env: str, tmp_path: Path, capsys: pytest.CaptureFixture[str], faker: Faker
) -> None:
    source = hosts(tmp_path / "hosts.csv", faker, 8)
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    args = [
        "import",
        str(source),
        "--store",
        "pg",
        "--table",
        "host",
        "--migrations",
        str(migrations),
    ]

    assert main([*args, "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "-> pg.host (8 rows read)" in out
    assert "would write 0001_import_host.toml" in out

    assert main(args) == 0  # creates the table
    capsys.readouterr()
    assert main([*args, "--dry-run"]) == 0
    assert "the table fits: no migration" in capsys.readouterr().out


@pytest.fixture
def base() -> str:
    return f"https://example.org/{uuid.uuid4().hex[:10]}/fleet#"


@pytest.fixture
def graph_env(monkeypatch: pytest.MonkeyPatch, drevo_bolt_dsn: str, base: str) -> Iterator[None]:
    monkeypatch.setenv("RHIZODAT_STORE_GRAPH", drevo_bolt_dsn)
    yield
    with Rhizodat(stores={"graph": drevo_bolt_dsn}) as d:
        d.store("graph").execute(
            "MATCH (n:Resource) WHERE n.title STARTS WITH $b DETACH DELETE n", {"b": base}
        )


def fleet(path: Path, base: str) -> Path:
    path.write_text(
        f"@prefix ex: <{base}> .\n"
        'ex:web1 a ex:Host ; ex:name "web1" ; ex:runs-on ex:rack1 .\n'
        'ex:rack1 a ex:Rack ; ex:name "rack1" .\n'
    )
    return path


def test_an_rdf_dry_run_in_text_and_an_import_in_json(
    graph_env: None, base: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = fleet(tmp_path / "fleet.ttl", base)
    assert main(["import", str(source), "--store", "graph", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "2 nodes, 1 edges -> graph" in out
    assert "labels: Host, Rack" in out
    assert "edge types: runs_on" in out

    assert main(["import", str(source), "--store", "graph", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert (report["store"], report["nodes"], report["edges"]) == ("graph", 2, 1)


def test_an_rdf_import_into_a_store_that_is_not_configured_is_refused(
    graph_env: None, base: str, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = fleet(tmp_path / "fleet.ttl", base)
    assert main(["import", str(source), "--store", "nowhere"]) == 2
    assert "nowhere" in capsys.readouterr().err


def test_a_source_that_is_not_a_url_is_left_as_it_is() -> None:
    # urlsplit refuses an unclosed IPv6 host: there is no secret to scrub.
    assert _scrub("failed: http://[::1", "http://[::1") == "failed: http://[::1"
    assert _scrub("at s3://u:pa55@b/x", "s3://u:pa55@b/x") == "at s3://u:***@b/x"
