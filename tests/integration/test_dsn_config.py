"""A realistic multi-store configuration parsed from environment-style settings."""

import logging
from urllib.parse import quote

import pytest
from faker import Faker

from datorium.core.dsn import DSNError, parse_dsn


@pytest.fixture
def erp_settings(faker: Faker) -> dict[str, str]:
    """ERP deployment: orders in PostgreSQL, graph in drevo, analytics, lakehouse."""
    pw = quote(faker.password(length=20, special_chars=True), safe="")
    return {
        "pg": f"postgresql://{faker.user_name()}:{pw}@{faker.hostname()}:6432/erp?sslmode=verify-full",
        "drevo": f"drevo+bolt://graph:{pw}@{faker.hostname()}",
        "clickhouse": f"clickhouse://reporter:{pw}@{faker.hostname()}/analytics?secure=true",
        "iceberg": f"iceberg+rest://{faker.hostname()}:8181/warehouse?token={faker.sha256()}",
        "journal": f"postgresql://coordinator:{pw}@{faker.hostname()}/datorium",
    }


def test_erp_configuration_parses_into_four_stores_and_a_journal(
    erp_settings: dict[str, str],
) -> None:
    parsed = {name: parse_dsn(raw) for name, raw in erp_settings.items()}

    assert {n: d.store for n, d in parsed.items()} == {
        "pg": "postgres",
        "drevo": "drevo",
        "clickhouse": "clickhouse",
        "iceberg": "iceberg",
        "journal": "postgres",
    }
    assert parsed["drevo"].port == 7687
    assert parsed["clickhouse"].params == {"secure": "true"}
    for name, dsn in parsed.items():
        assert dsn.unredacted() == erp_settings[name]


def test_logging_the_configuration_leaks_no_secret(
    erp_settings: dict[str, str], caplog: pytest.LogCaptureFixture
) -> None:
    parsed = {name: parse_dsn(raw) for name, raw in erp_settings.items()}
    secrets = {d.password for d in parsed.values() if d.password}
    secrets.add(parsed["iceberg"].params["token"])

    with caplog.at_level(logging.INFO):
        log = logging.getLogger("datorium.test")
        log.info("stores: %s", parsed)
        for name, dsn in parsed.items():
            log.info("%s -> %s", name, dsn)

    assert caplog.text
    for secret in secrets:
        assert secret not in caplog.text
        assert quote(secret, safe="") not in caplog.text


def test_a_typo_in_the_scheme_does_not_echo_the_credentials(faker: Faker) -> None:
    password = faker.password(length=16, special_chars=False)
    with pytest.raises(DSNError) as err:
        parse_dsn(f"postgressql://app:{password}@db/erp")
    assert password not in str(err.value)


def test_bug_tracker_on_a_laptop_uses_embedded_stores(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    root = tmp_path_factory.mktemp("tracker")
    graph = parse_dsn(f"drevo://{root / 'graph.drevo'}")
    journal = parse_dsn(f"file://{root / 'journal.db'}")
    tests_graph = parse_dsn("drevo+mem://")

    assert (graph.transport, graph.path) == ("embedded", str(root / "graph.drevo"))
    assert (journal.store, journal.path) == ("file", str(root / "journal.db"))
    assert tests_graph.transport == "memory"
