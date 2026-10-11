"""A minimal client for the toxiproxy HTTP API (compose profile "faults"); stdlib only.

A proxy listens on one of the ports compose publishes (58401-58404) and forwards
to a store inside the compose network. Disabling it drops open connections and
refuses new ones: a real network failure, at a step the test chooses.
"""

import json
import socket
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit, urlunsplit

__all__ = ["LISTEN_PORTS", "Proxy", "Toxiproxy", "http_reachable", "through"]

LISTEN_PORTS = range(58401, 58405)


def http_reachable(url: str, timeout_s: float = 0.5) -> bool:
    parts = urlsplit(url)
    try:
        socket.create_connection((parts.hostname, parts.port or 80), timeout=timeout_s).close()
    except OSError:
        return False
    return True


def through(dsn: str, port: int) -> str:
    """``dsn`` with its host and port replaced by the proxy's listen address."""
    parts = urlsplit(dsn)
    auth = parts.netloc.rpartition("@")[0]
    netloc = f"{auth}@127.0.0.1:{port}" if auth else f"127.0.0.1:{port}"
    return urlunsplit(parts._replace(netloc=netloc))


class Proxy:
    def __init__(self, api: "Toxiproxy", name: str, port: int) -> None:
        self._api = api
        self.name = name
        self.port = port

    def disable(self) -> None:
        """Drop every open connection and refuse new ones."""
        self._api.call("POST", f"/proxies/{self.name}", {"enabled": False})

    def enable(self) -> None:
        self._api.call("POST", f"/proxies/{self.name}", {"enabled": True})

    @property
    def enabled(self) -> bool:
        return bool(self._api.call("GET", f"/proxies/{self.name}")["enabled"])


class Toxiproxy:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")

    def call(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.url + path, data=data, method=method, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read()
        return json.loads(raw) if raw else None

    @contextmanager
    def proxy(self, name: str, upstream: str, port: int) -> Iterator[Proxy]:
        """A proxy for the duration of a test; removed afterwards even if the test fails."""
        if port not in LISTEN_PORTS:
            raise ValueError(f"port {port} is not published by compose: use {LISTEN_PORTS}")
        existing = self.call("GET", "/proxies")
        if name in existing:
            self.call("DELETE", f"/proxies/{name}")
        self.call(
            "POST",
            "/proxies",
            {"name": name, "listen": f"0.0.0.0:{port}", "upstream": upstream, "enabled": True},
        )
        try:
            yield Proxy(self, name, port)
        finally:
            self.call("DELETE", f"/proxies/{name}")
