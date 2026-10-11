"""The toxiproxy test helper against the real proxy: a disabled proxy refuses connections."""

import socket

import pytest
from faker import Faker

from toxiproxy import Toxiproxy, through


def connects(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1) as s:
            s.settimeout(1)
            s.sendall(b"\x00\x00\x00\x08\x04\xd2\x16\x2f")  # PostgreSQL SSLRequest
            return s.recv(1) in (b"S", b"N")
    except OSError:
        return False


def test_through_replaces_host_and_port_and_keeps_credentials(faker: Faker) -> None:
    password = faker.password(special_chars=False)
    dsn = f"postgresql://app:{password}@db.local:5432/orders?sslmode=disable"
    assert (
        through(dsn, 58401) == f"postgresql://app:{password}@127.0.0.1:58401/orders?sslmode=disable"
    )
    assert through("drevo+bolt://graph:7687", 58402) == "drevo+bolt://127.0.0.1:58402"


def test_disabled_proxy_refuses_and_enabled_proxy_forwards(toxiproxy: Toxiproxy) -> None:
    with toxiproxy.proxy("dat_probe", "postgres:5432", 58404) as proxy:
        assert connects(58404)
        proxy.disable()
        assert not proxy.enabled
        assert not connects(58404)
        proxy.enable()
        assert connects(58404)
    assert "dat_probe" not in toxiproxy.call("GET", "/proxies")


def test_unpublished_port_is_rejected(toxiproxy: Toxiproxy) -> None:
    with pytest.raises(ValueError, match="published"), toxiproxy.proxy("x", "postgres:5432", 1):
        pass
