"""Shared fixtures. Store-backed tests run when their store is configured or running.

A store's DSN comes from the environment, else from ``.env.test`` (the stores in
``compose.yaml``); see ``stores.py``.
"""

import os
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

from no_skips import pytest_runtest_makereport  # noqa: F401 - DATORIUM_TEST_NO_SKIPS
from stores import ENV_FILE, StoreUnavailable, load_env_file, resolve_dsn

if TYPE_CHECKING:
    from toxiproxy import Toxiproxy

PG_ENV = "DATORIUM_TEST_PG_DSN"
DREVO_BOLT_ENV = "DATORIUM_TEST_DREVO_BOLT"
CLICKHOUSE_ENV = "DATORIUM_TEST_CLICKHOUSE_DSN"
ICEBERG_REST_ENV = "DATORIUM_TEST_ICEBERG_REST_DSN"
TOXIPROXY_ENV = "DATORIUM_TEST_TOXIPROXY"


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    """DSN of a disposable PostgreSQL with ``max_prepared_transactions > 0``."""
    return _store_dsn(PG_ENV)


@pytest.fixture
def pg_schema(pg_dsn: str) -> Iterator[str]:
    """A fresh schema per test, dropped afterwards, so tests never share tables."""
    import psycopg

    name = f"dat_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        conn.execute(f'CREATE SCHEMA "{name}"')
    yield name
    with psycopg.connect(pg_dsn, autocommit=True) as conn:
        # A failed test may leave a branch open; never let it block the cleanup.
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
            " AND state LIKE 'idle in transaction%%'"
        )
        conn.execute("SET lock_timeout = '10s'")
        for (gid,) in conn.execute(
            "SELECT gid FROM pg_prepared_xacts"
            " WHERE gid LIKE 'dat:%%' AND database = current_database()"
        ).fetchall():
            conn.execute(f"ROLLBACK PREPARED '{gid}'")
        conn.execute(f'DROP SCHEMA "{name}" CASCADE')


def drevo_embedded_available() -> bool:
    """drevo-py with two-phase commit (``uv sync --group drevo``)."""
    try:
        import drevo
    except ImportError:
        return False
    return hasattr(drevo.Drevo, "list_prepared")


@pytest.fixture(scope="session")
def drevo_bolt_dsn() -> str:
    """``drevo+bolt://`` DSN of a disposable drevo server (never a shared one: 2PC fences it)."""
    return _store_dsn(DREVO_BOLT_ENV)


@pytest.fixture(scope="session")
def clickhouse_server_dsn() -> str:
    """DSN of a disposable ClickHouse; tests get their own database (``clickhouse_dsn``)."""
    return _store_dsn(CLICKHOUSE_ENV)


@pytest.fixture
def clickhouse_dsn(clickhouse_server_dsn: str) -> Iterator[str]:
    """``clickhouse_server_dsn`` pointing at a fresh database, dropped afterwards."""
    from urllib.parse import urlsplit, urlunsplit

    import clickhouse_connect

    from datorium.core.dsn import parse_dsn

    server = parse_dsn(clickhouse_server_dsn)
    name = f"dat_test_{uuid.uuid4().hex[:12]}"
    client = clickhouse_connect.get_client(
        host=server.host,
        port=server.port,
        username=server.user or "default",
        password=server.password or "",
        autogenerate_session_id=False,
    )
    client.command(f"CREATE DATABASE `{name}`")
    yield urlunsplit(urlsplit(clickhouse_server_dsn)._replace(path=f"/{name}"))
    try:
        client.command(f"DROP DATABASE IF EXISTS `{name}` SYNC")
    finally:
        client.close()


@pytest.fixture(scope="session")
def iceberg_rest_dsn() -> str:
    """``iceberg+rest://`` DSN of a disposable REST catalog over S3 (compose profile iceberg)."""
    return _store_dsn(ICEBERG_REST_ENV)


@pytest.fixture(scope="session")
def toxiproxy() -> "Toxiproxy":
    """The toxiproxy API (compose profile faults) for real network failures."""
    from toxiproxy import Toxiproxy, http_reachable

    try:
        url = resolve_dsn(TOXIPROXY_ENV, os.environ, load_env_file(ENV_FILE), http_reachable)
    except StoreUnavailable as exc:
        pytest.skip(str(exc))
    return Toxiproxy(url)


def _store_dsn(var: str) -> str:
    try:
        return resolve_dsn(var, os.environ, load_env_file(ENV_FILE))
    except StoreUnavailable as exc:
        pytest.skip(str(exc))
