"""Shared fixtures. Store-backed tests run only when a test database is configured."""

import os
import uuid
from collections.abc import Iterator

import pytest

PG_ENV = "DATORIUM_TEST_PG_DSN"


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    """DSN of a disposable PostgreSQL with ``max_prepared_transactions > 0``."""
    dsn = os.environ.get(PG_ENV)
    if not dsn:
        pytest.skip(f"set {PG_ENV} to run PostgreSQL tests")
    return dsn


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
