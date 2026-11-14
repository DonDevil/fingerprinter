"""Resolved-destination validation for outbound media acquisition (SSRF hardening).

The acquirer's other protections (scheme allowlist, bounded redirects,
content-type/ffprobe validation) all operate on *what* is fetched. This
module is the one check that operates on *where* it is fetched from: it
resolves the request's hostname and rejects any resolved address that is
loopback, private, link-local, unspecified, multicast, or otherwise
reserved for internal use — closing the gap where a crawler-supplied
`candidate_url` (or a redirect target reached through it) could point the
acquirer at internal infrastructure.

DNS rebinding: closed for sessions the acquirer creates itself by
``acquisition/pinned_adapter.py``, which resolves, validates and connects in
one step (the address checked is the address used; Host/SNI stay the
hostname). The per-hop check here still runs first so a hostile URL fails
fast with a typed error before any socket is opened. A caller-supplied
``requests.Session`` is not modified and therefore keeps the old
check-then-resolve-again window.
"""
from __future__ import annotations

import ipaddress
import socket
from typing import Callable, List, Optional, Sequence, Tuple, Union

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


def _legacy_literal(hostname: str) -> Optional[IPAddress]:
    """The address `hostname` denotes if it is an IP literal in any spelling inet_aton accepts."""
    host = hostname.strip("[]")
    try:
        return ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        pass
    if not host or not all(c in "0123456789abcdefABCDEFxX." for c in host):
        return None
    try:
        return ipaddress.IPv4Address(socket.inet_aton(host))
    except OSError:
        return None


def resolve_addresses(hostname: str, port: int, resolver: Resolver = default_resolver) -> List[IPAddress]:
    """Resolve `hostname` to concrete IP addresses via `resolver`.

    Raises `socket.gaierror` unchanged on resolution failure — the caller
    (acquisition/acquirer.py) maps that onto the same transient-network
    classification a real connection failure already gets.
    """
    literal = _legacy_literal(hostname)
    if literal is not None:  # exotic spellings (2130706433, 0x7f.1) never reach the resolver
        return [literal]
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
