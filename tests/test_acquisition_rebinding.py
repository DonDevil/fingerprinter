"""P14 (TC-061): DNS-rebinding closure in the acquirer's own session.

A local server stands in for the internal target. Resolvers are scripted, so no real
DNS happens: the first lookup (the per-hop guard) answers with a public address, the
second (the connection) with loopback. The pinned connection must refuse it.
"""
from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import List

import pytest

from acquisition import MediaAcquirer, UnsafeDestinationError
from acquisition import pinned_adapter
from acquisition.ssrf_guard import resolve_addresses

PUBLIC = "93.184.216.34"


class _Handler(BaseHTTPRequestHandler):
    hits: List[str] = []

    def do_GET(self):  # noqa: N802
        type(self).hits.append(self.headers.get("Host", ""))
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


@pytest.fixture()
def internal_server():
    _Handler.hits = []
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    thread.join(5)


def _scripted(*answers: str):
    calls = []

    def resolve(hostname, port):
        ip = answers[min(len(calls), len(answers) - 1)]
        calls.append(hostname)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))]

    resolve.calls = calls  # type: ignore[attr-defined]
    return resolve


def test_answer_that_flips_between_check_and_connect_is_refused(internal_server):
    port = internal_server.server_address[1]
    resolver = _scripted(PUBLIC, "127.0.0.1")
    acquirer = MediaAcquirer(resolver=resolver, validate=False)
    with pytest.raises(UnsafeDestinationError):
        acquirer.acquire("http://rebind.example:%d/v.mp4" % port)
    assert len(resolver.calls) == 2, "guard lookup and connect-time lookup both happened"
    assert _Handler.hits == [], "the internal server must never see a request"


def test_connection_is_made_to_the_validated_address_not_the_name(monkeypatch):
    seen = []

    def fake_create_connection(address, *args, **kwargs):
        seen.append(address)
        raise OSError("stop here")

    monkeypatch.setattr(pinned_adapter.connection, "create_connection", fake_create_connection)
    acquirer = MediaAcquirer(resolver=_scripted(PUBLIC), validate=False)
    with pytest.raises(Exception):
        acquirer.acquire("http://pin.example:8080/v.mp4")
    assert seen == [(PUBLIC, 8080)]


def test_every_answer_must_be_safe_at_connect_time(monkeypatch):
    def mixed(hostname, port):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", (PUBLIC, port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", port)),
        ]

    monkeypatch.setattr(
        pinned_adapter.connection,
        "create_connection",
        lambda *a, **k: pytest.fail("must not connect when any record is internal"),
    )
    with pytest.raises(UnsafeDestinationError):
        MediaAcquirer(resolver=mixed, validate=False).acquire("http://mixed.example/v.mp4")


@pytest.mark.parametrize("host", ["2130706433", "0x7f.1", "0177.0.0.1", "127.1"])
def test_legacy_ip_spellings_are_resolved_without_the_resolver(host):
    def forbidden(hostname, port):
        raise AssertionError("literal must not reach the resolver")

    assert [str(a) for a in resolve_addresses(host, 80, forbidden)] == ["127.0.0.1"]


def test_opt_out_session_is_not_pinned():
    acquirer = MediaAcquirer(allow_private_networks=True)
    adapter = acquirer._session.get_adapter("http://example.com/")
    assert not isinstance(adapter, pinned_adapter.PinnedHTTPAdapter)
