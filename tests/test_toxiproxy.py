"""The toxiproxy test helper against the real proxy: a disabled proxy refuses connections."""

import http.server
import json
import re
import socket
import threading
from pathlib import Path

import pytest
from faker import Faker

from toxiproxy import LISTEN_PORTS, Toxiproxy, ToxiproxyError, through


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


def test_the_proxy_ports_are_outside_the_containers_ephemeral_range() -> None:
    """A client socket of toxiproxy's own healthcheck stays in TIME_WAIT for a minute on
    an ephemeral port; when that port is a proxy's, the proxy cannot listen (HTTP 500,
    "bind: address already in use"). compose.yaml keeps the range below the proxies."""
    compose = (Path(__file__).parent.parent / "compose.yaml").read_text()
    service = compose.split("\n  toxiproxy:\n", 1)[1]  # to the end: its keys come first
    [(low_text, high_text)] = re.findall(r'net\.ipv4\.ip_local_port_range: "(\d+) (\d+)"', service)
    low, high = int(low_text), int(high_text)
    assert low <= high
    assert all(not low <= port <= high for port in LISTEN_PORTS)


def test_a_refused_call_says_what_toxiproxy_answered(faker: Faker) -> None:
    message = f"listen tcp 0.0.0.0:58403: bind: {faker.sentence()}"

    class Refuse(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = json.dumps({"error": message, "status": 500}).encode()
            self.send_response(500)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: object) -> None:
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Refuse) as server:
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            api = Toxiproxy(f"http://127.0.0.1:{server.server_address[1]}")
            with pytest.raises(ToxiproxyError, match="POST /proxies: HTTP 500") as caught:
                api.call("POST", "/proxies", {"name": "x"})
            assert message in str(caught.value)
        finally:
            server.shutdown()
