"""Connection pinning for the acquirer's ``requests`` session (closes DNS rebinding).

``ssrf_guard.validate_destination`` checks *where* a hostname points, but
urllib3 would resolve it a second time when it opens the socket, so an
attacker-controlled DNS server could answer "public" to the check and
"internal" to the connection. Here the connection itself resolves, validates
every answer and connects to a validated address, so the address that was
checked is the address that is used. The URL, the HTTP ``Host`` header and
the TLS SNI/certificate name keep the hostname (only the TCP socket differs).

Mounted by ``MediaAcquirer`` on sessions it creates itself and when the
private-network opt-out is off; a caller-supplied session is left untouched.
"""
from __future__ import annotations

import socket
import sys
from socket import timeout as SocketTimeout
from typing import Any, Optional

from requests.adapters import HTTPAdapter
from urllib3 import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.exceptions import ConnectTimeoutError, NameResolutionError, NewConnectionError
from urllib3.util import connection

from acquisition.ssrf_guard import Resolver, default_resolver, validate_destination


def _pinned(base: type, resolver: Resolver) -> type:
    class Pinned(base):  # type: ignore[valid-type, misc]
        def _new_conn(self) -> socket.socket:
            try:
                addresses = validate_destination(self._dns_host, self.port, resolver=resolver)
            except socket.gaierror as exc:
                raise NameResolutionError(self.host, self, exc) from exc
            last: Optional[BaseException] = None
            for address in addresses:
                try:
                    sock = connection.create_connection(
                        (str(address), self.port),
                        self.timeout,
                        source_address=self.source_address,
                        socket_options=self.socket_options,
                    )
                except SocketTimeout as exc:
                    last = ConnectTimeoutError(
                        self, f"Connection to {self.host} timed out. (connect timeout={self.timeout})"
                    )
                    last.__cause__ = exc
                except OSError as exc:
                    last = NewConnectionError(self, f"Failed to establish a new connection: {exc}")
                    last.__cause__ = exc
                else:
                    sys.audit("http.client.connect", self, self.host, self.port)
                    return sock
            assert last is not None  # validate_destination never returns an empty list
            raise last

    return Pinned


class PinnedHTTPAdapter(HTTPAdapter):
    """``requests`` adapter whose sockets only ever connect to validated addresses."""

    def __init__(self, resolver: Resolver = default_resolver, **kwargs: Any) -> None:
        self._pin_resolver = resolver
        super().__init__(**kwargs)

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        super().init_poolmanager(*args, **kwargs)
        http_pool = type(
            "PinnedHTTPPool",
            (HTTPConnectionPool,),
            {"ConnectionCls": _pinned(HTTPConnection, self._pin_resolver)},
        )
        https_pool = type(
            "PinnedHTTPSPool",
            (HTTPSConnectionPool,),
            {"ConnectionCls": _pinned(HTTPSConnection, self._pin_resolver)},
        )
        self.poolmanager.pool_classes_by_scheme = {"http": http_pool, "https": https_pool}
