"""Resolved-destination validation for outbound media acquisition (SSRF hardening).

The acquirer's other protections (scheme allowlist, bounded redirects,
content-type/ffprobe validation) all operate on *what* is fetched. This
module is the one check that operates on *where* it is fetched from: it
resolves the request's hostname and rejects any resolved address that is
loopback, private, link-local, unspecified, multicast, or otherwise
reserved for internal use — closing the gap where a crawler-supplied
`candidate_url` (or a redirect target reached through it) could point the
acquirer at internal infrastructure.

DNS-rebinding / TOCTOU limitation (deliberately not solved here — see
docs/architecture/phase-13-production-hardening.md, "Phase 13A"): this
module re-resolves DNS itself immediately before each connection attempt,
but `requests`/urllib3 performs its own, independent resolution a moment
later when it actually opens the socket. An attacker who controls DNS for
the candidate's hostname and changes the answer between those two lookups
("DNS rebinding") is not defeated by this check alone. Fully closing that
gap requires pinning the actual connection to the address validated here
(a custom transport adapter) — a materially larger change, deliberately
out of scope for this pass.
"""
from __future__ import annotations

import ipaddress
import socket
from typing import Callable, List, Sequence, Tuple, Union

from acquisition.errors import UnsafeDestinationError

IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

# `socket.getaddrinfo`-shaped: (family, type, proto, canonname, sockaddr)
Resolver = Callable[[str, int], Sequence[Tuple]]

# Ranges the stdlib `ipaddress` module does not classify as private/reserved
# on its own but that are not legitimate public destinations for this
# system's purposes.
#   - 100.64.0.0/10: RFC 6598 shared/carrier-grade-NAT address space —
#     never a real server's public address, only ever internal-to-a-network.
_EXTRA_UNSAFE_NETWORKS = (ipaddress.ip_network("100.64.0.0/10"),)


def default_resolver(hostname: str, port: int) -> Sequence[Tuple]:
    return socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)


def is_unsafe_address(addr: IPAddress) -> bool:
    """True if `addr` must not be connected to in production mode."""
    if isinstance(addr, ipaddress.IPv6Address):
        mapped = addr.ipv4_mapped
        if mapped is not None:
            addr = mapped
    if (
        addr.is_loopback
        or addr.is_private
        or addr.is_link_local
        or addr.is_unspecified
        or addr.is_multicast
        or addr.is_reserved
    ):
        return True
    return any(addr in network for network in _EXTRA_UNSAFE_NETWORKS if addr.version == network.version)


def resolve_addresses(hostname: str, port: int, resolver: Resolver = default_resolver) -> List[IPAddress]:
    """Resolve `hostname` to concrete IP addresses via `resolver`.

    Raises `socket.gaierror` unchanged on resolution failure — the caller
    (acquisition/acquirer.py) maps that onto the same transient-network
    classification a real connection failure already gets.
    """
    infos = resolver(hostname, port)
    addresses: List[IPAddress] = []
    for _family, _type, _proto, _canonname, sockaddr in infos:
        raw_ip = sockaddr[0]
        try:
            addresses.append(ipaddress.ip_address(raw_ip))
        except ValueError:
            continue
    if not addresses:
        raise socket.gaierror(f"no usable addresses resolved for {hostname!r}")
    return addresses


def validate_destination(hostname: str, port: int, resolver: Resolver = default_resolver) -> List[IPAddress]:
    """Resolve `hostname` and return its addresses if every one is safe.

    Raises `UnsafeDestinationError` (permanent — not retried) if any
    resolved address is loopback, private, link-local, unspecified,
    multicast, or otherwise reserved. Conservative by design: a hostname
    with multiple A/AAAA records is rejected if *any* of them is unsafe,
    since nothing in this module controls which record the real HTTP
    connection ends up using.
    """
    addresses = resolve_addresses(hostname, port, resolver)
    unsafe = [a for a in addresses if is_unsafe_address(a)]
    if unsafe:
        raise UnsafeDestinationError(
            f"{hostname!r} resolves to a disallowed internal/reserved address: {unsafe[0]}"
        )
    return addresses
