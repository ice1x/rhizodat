"""Where the tests find their stores: the environment first, then ``.env.test`` (compose)."""

import socket
from collections.abc import Iterator
from pathlib import Path

import pytest
from faker import Faker

from stores import StoreUnavailable, load_env_file, reachable, resolve_dsn

PG = "DATORIUM_TEST_PG_DSN"


@pytest.fixture
def listening() -> Iterator[int]:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        yield server.getsockname()[1]


@pytest.fixture
def closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_env_file_reads_assignments_and_skips_comments(tmp_path: Path, faker: Faker) -> None:
    dsn = f"postgresql://{faker.user_name()}:{faker.password()}@127.0.0.1:55440/{faker.word()}"
    path = tmp_path / ".env.test"
    path.write_text(f"# test stores\n\n{PG}={dsn}\n  DATORIUM_X = a=b  \n")
    assert load_env_file(path) == {PG: dsn, "DATORIUM_X": "a=b"}


def test_env_file_missing_is_empty(tmp_path: Path) -> None:
    assert load_env_file(tmp_path / "absent") == {}


def test_env_file_rejects_a_line_without_assignment(tmp_path: Path) -> None:
    path = tmp_path / ".env.test"
    path.write_text("DATORIUM_X\n")
    with pytest.raises(ValueError, match="line 1"):
        load_env_file(path)


def test_reachable_true_for_a_listening_port(listening: int) -> None:
    assert reachable(f"postgresql://u@127.0.0.1:{listening}/db")
    assert reachable(f"drevo+bolt://127.0.0.1:{listening}")


def test_reachable_false_for_a_closed_port(closed_port: int) -> None:
    assert not reachable(f"postgresql://u@127.0.0.1:{closed_port}/db")


def test_reachable_true_without_a_host() -> None:
    """Unix sockets and embedded stores: nothing to probe, let the driver decide."""
    assert reachable("postgresql:///app?host=/var/run/postgresql")
    assert reachable("drevo+mem://")


def test_explicit_environment_wins_and_is_never_probed() -> None:
    def probe(dsn: str) -> bool:
        raise AssertionError("an explicit DSN must not be probed")

    dsn = resolve_dsn(PG, {PG: "postgresql://a@h/x"}, {PG: "postgresql://b@h/y"}, probe)
    assert dsn == "postgresql://a@h/x"


def test_compose_default_is_used_when_reachable() -> None:
    assert resolve_dsn(PG, {}, {PG: "postgresql://b@h/y"}, lambda d: True) == "postgresql://b@h/y"


def test_compose_default_that_is_down_tells_how_to_start_it() -> None:
    with pytest.raises(StoreUnavailable, match="docker compose up -d --wait"):
        resolve_dsn(PG, {}, {PG: "postgresql://b@h/y"}, lambda d: False)


def test_empty_environment_value_counts_as_unset() -> None:
    with pytest.raises(StoreUnavailable, match=PG):
        resolve_dsn(PG, {PG: ""}, {}, lambda d: True)
