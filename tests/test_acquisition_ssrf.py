"""Phase 13A: SSRF / outbound-destination hardening regression tests.

Runs entirely against literal IP addresses (no real DNS lookups — a
`getaddrinfo` call on a numeric address never touches the network) and a
fully in-process fake `requests.Session` for the redirect-chain cases (no
sockets at all) — no external network access anywhere in this file, per
the "controlled local test infrastructure only" requirement.
"""
from __future__ import annotations

import socket
from typing import Dict, List, Sequence, Tuple

import ipaddress
import pytest

from acquisition import (
    MediaAcquirer,
    NetworkError,
    PermanentAcquisitionError,
    UnsafeDestinationError,
)
from acquisition.ssrf_guard import is_unsafe_address


class _NeverCalledSession:
    """Fails the test if the acquirer ever tries to open a connection.

    Used for the direct-address rejection cases below to prove the SSRF
    check runs, and raises, *before* any request is attempted.
    """

    def get(self, *args, **kwargs):  # pragma: no cover - should never run
        raise AssertionError("acquirer attempted a network request past the SSRF guard")


def _acquirer_for_direct_check(**overrides) -> MediaAcquirer:
    defaults = dict(session=_NeverCalledSession())
    defaults.update(overrides)
    return MediaAcquirer(**defaults)


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:80/video.mp4",
        "http://127.1.2.3:80/video.mp4",  # any 127/8 address, not just .1
    ],
)
def test_ipv4_loopback_rejected(url):
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire(url)


def test_ipv6_loopback_rejected():
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire("http://[::1]:80/video.mp4")


@pytest.mark.parametrize(
    "url",
    [
        "http://10.1.2.3/video.mp4",
        "http://172.16.5.5/video.mp4",
        "http://192.168.0.9/video.mp4",
    ],
)
def test_rfc1918_private_ipv4_rejected(url):
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire(url)


def test_ipv6_private_local_address_rejected():
    # fd00::/8 — RFC 4193 unique local address space, the IPv6 analogue of
    # RFC1918.
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire("http://[fd12:3456:789a::1]/video.mp4")


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",  # cloud metadata endpoint
        "http://[fe80::1]/video.mp4",
    ],
)
def test_link_local_address_rejected(url):
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire(url)


@pytest.mark.parametrize("url", ["http://0.0.0.0/video.mp4", "http://[::]/video.mp4"])
def test_unspecified_address_rejected(url):
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire(url)


@pytest.mark.parametrize(
    "url",
    [
        "http://224.0.0.1/video.mp4",  # IPv4 multicast
        "http://240.0.0.1/video.mp4",  # IPv4 reserved (Class E)
        "http://[ff02::1]/video.mp4",  # IPv6 multicast
    ],
)
def test_multicast_and_reserved_addresses_rejected(url):
    with pytest.raises(UnsafeDestinationError):
        _acquirer_for_direct_check().acquire(url)


def test_carrier_grade_nat_range_rejected():
    # 100.64.0.0/10 (RFC 6598): not flagged private/reserved by Python's
    # stdlib ipaddress module, so acquisition/ssrf_guard.py adds it
    # explicitly. Exercised directly against is_unsafe_address rather than
    # through acquire() since it's the one range this module treats
    # specially rather than delegating to `ipaddress`.
    assert is_unsafe_address(ipaddress.ip_address("100.64.1.1")) is True


def test_normal_loopback_destination_is_allowed_when_explicitly_opted_in(media_server):
    # Sanity check on the opt-out itself: allow_private_networks=True must
    # still let a legitimate (test) fetch through — proven end-to-end
    # against tests/media_test_server.py, matching every other acquisition
    # test's fixture.
    acquirer = MediaAcquirer(allow_private_networks=True)
    artifact = acquirer.acquire(media_server.url("/ok"))
    try:
        assert artifact.local_path.exists()
    finally:
        artifact.cleanup()


# --- Fake-transport redirect-chain tests -----------------------------------
#
# These decouple "what the SSRF check believes a hostname resolves to"
# (via an injected `resolver`) from the real transport, using an in-process
# fake `requests.Session` — no sockets, no real DNS, per the "use
# dependency injection or a test seam" instruction. This is the only way to
# exercise "external hostname resolves to a public address, then redirects
# to an internal one" without a second real, internet-routable host.


class _FakeResponse:
    def __init__(self, status_code: int, headers: Dict[str, str], body: bytes = b""):
        self.status_code = status_code
        self.headers = headers
        self._body = body
        self.url = headers.get("_request_url", "")
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def iter_content(self, chunk_size: int = 65536):
        yield self._body


class _ScriptedSession:
    """Replays one canned response per call, in order. Extra calls fail the test."""

    def __init__(self, responses: Sequence[_FakeResponse]):
        self._responses = list(responses)
        self.call_count = 0

    def get(self, url, **kwargs):
        if self.call_count >= len(self._responses):
            raise AssertionError(
                f"session.get called more times than expected (call #{self.call_count + 1} for {url}); "
                "the SSRF guard should have rejected the request before this hop"
            )
        response = self._responses[self.call_count]
        self.call_count += 1
        return response


def _fake_resolver(mapping: Dict[str, str]):
    """Returns a Resolver that answers with a fixed IP per hostname, in
    socket.getaddrinfo's exact shape — independent of what the fake
    transport above actually does, which is the point: it lets us assert
    what the *SSRF check* believes without needing that address to be
    real/reachable.
    """

    def resolver(hostname: str, port: int) -> List[Tuple]:
        if hostname not in mapping:
            raise socket.gaierror(f"unmapped test hostname: {hostname!r}")
        ip = mapping[hostname]
        family = socket.AF_INET6 if ipaddress.ip_address(ip).version == 6 else socket.AF_INET
        sockaddr = (ip, port, 0, 0) if family == socket.AF_INET6 else (ip, port)
        return [(family, socket.SOCK_STREAM, 6, "", sockaddr)]

    return resolver


def test_normal_public_destination_remains_allowed():
    resolver = _fake_resolver({"cdn.example": "93.184.216.34"})
    session = _ScriptedSession(
        [_FakeResponse(200, {"Content-Type": "video/mp4"}, body=b"fake-video-bytes")]
    )
    acquirer = MediaAcquirer(session=session, resolver=resolver, validate=False)
    artifact = acquirer.acquire("http://cdn.example/video.mp4")
    try:
        assert artifact.byte_size == len(b"fake-video-bytes")
        assert session.call_count == 1
    finally:
        artifact.cleanup()


def test_external_url_redirecting_to_loopback_is_rejected():
    resolver = _fake_resolver(
        {
            "external.example": "93.184.216.34",
            "internal.example": "127.0.0.1",
        }
    )
    session = _ScriptedSession(
        [_FakeResponse(302, {"Location": "http://internal.example/secret"})]
    )
    acquirer = MediaAcquirer(session=session, resolver=resolver, validate=False)
    with pytest.raises(UnsafeDestinationError):
        acquirer.acquire("http://external.example/redirect-me")
    assert session.call_count == 1  # the internal hop was never attempted


def test_external_url_redirecting_to_private_address_is_rejected():
    resolver = _fake_resolver(
        {
            "external.example": "93.184.216.34",
            "internal-corp.example": "10.20.30.40",
        }
    )
    session = _ScriptedSession(
        [_FakeResponse(302, {"Location": "http://internal-corp.example/secret"})]
    )
    acquirer = MediaAcquirer(session=session, resolver=resolver, validate=False)
    with pytest.raises(UnsafeDestinationError):
        acquirer.acquire("http://external.example/redirect-me")
    assert session.call_count == 1


def test_normal_redirect_between_two_public_hosts_still_functions():
    resolver = _fake_resolver(
        {
            "external.example": "93.184.216.34",
            "cdn.example": "185.199.108.153",
        }
    )
    session = _ScriptedSession(
        [
            _FakeResponse(302, {"Location": "http://cdn.example/final.mp4"}),
            _FakeResponse(200, {"Content-Type": "video/mp4"}, body=b"final-bytes"),
        ]
    )
    acquirer = MediaAcquirer(session=session, resolver=resolver, validate=False)
    artifact = acquirer.acquire("http://external.example/redirect-me")
    try:
        assert artifact.final_url == "http://cdn.example/final.mp4"
        assert artifact.byte_size == len(b"final-bytes")
        assert session.call_count == 2
    finally:
        artifact.cleanup()


def test_dns_resolution_failure_maps_to_transient_network_error():
    # An unresolvable hostname is a DNS failure, not an SSRF finding — it
    # must keep the acquirer's existing transient/retryable classification,
    # not the new permanent UnsafeDestinationError.
    resolver = _fake_resolver({})  # empty: every lookup raises gaierror
    acquirer = MediaAcquirer(session=_NeverCalledSession(), resolver=resolver)
    with pytest.raises(NetworkError):
        acquirer.acquire("http://does-not-resolve.example/video.mp4")


def test_existing_redirect_and_content_tests_are_unaffected_by_default_policy():
    # Regression guard: with the SSRF guard active (default
    # allow_private_networks=False) but no request ever reaching a real
    # address, a malformed/hostless URL is still a plain permanent
    # acquisition error, not a crash inside the new check.
    with pytest.raises(PermanentAcquisitionError):
        _acquirer_for_direct_check().acquire("http:///no-host-here")
