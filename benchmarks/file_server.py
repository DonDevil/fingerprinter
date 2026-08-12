"""Minimal in-process static-file HTTP server for pipeline benchmarks.

Deliberately not a reuse of `tests/media_test_server.py` (that server's
route table is purpose-built for Phase 5's error-classification tests —
/notfound, /corrupt, /slow, etc. — and isn't meant to serve arbitrary
benchmark fixtures). This one serves whatever bytes it's given at
construction time under a single fixed path, over loopback, so
`acquisition.acquirer.MediaAcquirer` exercises its real HTTP path (real
socket, real chunked streaming, real checksum) rather than a mocked one —
"acquisition time" in the phase-11 report is loopback HTTP, explicitly
labeled as such (near-zero network variability, not representative of a
real crawler-fleet media host).
"""
from __future__ import annotations

import http.server
import threading


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    body: bytes = b""
    content_type: str = "video/mp4"

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", self.content_type)
        self.send_header("Content-Length", str(len(self.body)))
        self.end_headers()
        self.wfile.write(self.body)


class _QuietServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address) -> None:
        pass


class StaticFileServer:
    """Serves `body` bytes at every path on a loopback port."""

    def __init__(self, body: bytes, content_type: str = "video/mp4") -> None:
        handler_cls = type("_BoundHandler", (_Handler,), {"body": body, "content_type": content_type})
        self._httpd = _QuietServer(("127.0.0.1", 0), handler_cls)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def url(self, path: str = "/candidate.mp4") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)
