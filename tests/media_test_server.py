"""In-process HTTP test server for Phase 5 media-acquisition tests.

No external network access — every acquisition test hits this server on
127.0.0.1. Not a test module itself (no test_ prefix); wired into a fixture
in conftest.py.
"""
from __future__ import annotations

import http.server
import os
import threading
import time

# Smallest valid PNG: 1x1 transparent pixel, 67 bytes. Real, ffprobe-decodable.
MIN_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a4944415478da6360000002000155bc7d8700000000"
    "49454e44ae426082"
)

_CORRUPT_BODY = os.urandom(2048)
_LARGE_BODY = b"\x00" * 4096
_HTML_BODY = b"<html><body>not media</body></html>"
_SLOW_DELAY_S = 1.5


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args) -> None:  # keep test output quiet
        pass

    def do_GET(self) -> None:
        path = self.path

        if path == "/ok":
            self._send(200, MIN_PNG, "image/png")
        elif path.startswith("/redirect/"):
            n = int(path.rsplit("/", 1)[-1])
            target = "/ok" if n <= 0 else f"/redirect/{n - 1}"
            self._redirect(target)
        elif path == "/redirect-loop":
            self._redirect("/redirect-loop")
        elif path == "/notfound":
            self._send(404, b"not found", "text/plain")
        elif path == "/gone":
            self._send(410, b"gone", "text/plain")
        elif path == "/error":
            self._send(500, b"server error", "text/plain")
        elif path == "/toomany":
            self._send(429, b"slow down", "text/plain")
        elif path == "/badtype":
            self._send(200, _HTML_BODY, "text/html")
        elif path == "/corrupt":
            self._send(200, _CORRUPT_BODY, "video/mp4")
        elif path == "/large":
            self._send(200, _LARGE_BODY, "image/png")
        elif path == "/slow":
            self._send_slow()
        else:
            self._send(404, b"not found", "text/plain")

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_slow(self) -> None:
        body = MIN_PNG
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.flush()
        time.sleep(_SLOW_DELAY_S)
        self.wfile.write(body)


class _QuietThreadingHTTPServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address) -> None:
        # Tests deliberately abort connections early (size-limit enforcement);
        # the resulting BrokenPipeError/ConnectionResetError on this side is
        # expected, not a real server fault, so don't dump it to stderr.
        pass


class MediaTestServer:
    def __init__(self) -> None:
        self._httpd = _QuietThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    @property
    def port(self) -> int:
        return self._httpd.server_address[1]

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def shutdown(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=2)
