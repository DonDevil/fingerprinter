"""A loopback HTTP fixture server for acquisition tests (TS-*).

Routes (``/<route>/<name>``): ``f`` plain file; ``r<N>`` redirect chain of N hops
to ``/f/<name>``; ``private`` redirect to 10.0.0.1; ``gzip`` gzip-encoded body
with ``Content-Encoding: gzip``; ``s<code>`` status only; ``short`` declares
more bytes than it sends; ``slow`` trickles; ``ref`` echoes the Referer header in
``X-Seen-Referer`` and records it.
"""

from __future__ import annotations

import gzip
import shutil
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class _Handler(BaseHTTPRequestHandler):
    server_version = "fixture/1"

    def log_message(self, *args):  # quiet
        pass

    def do_GET(self):  # noqa: N802
        srv = self.server
        srv.requests.append((self.path, dict(self.headers)))
        parts = self.path.strip("/").split("/", 1)
        route, name = parts[0], (parts[1] if len(parts) > 1 else "")
        path = srv.root / name
        if route.startswith("r") and route[1:].isdigit():
            n = int(route[1:])
            target = f"/r{n - 1}/{name}" if n > 1 else f"/f/{name}"
            self.send_response(302)
            self.send_header("Location", target)
            self.end_headers()
            return
        if route == "private":
            self.send_response(302)
            self.send_header("Location", "http://10.0.0.1/f/x")
            self.end_headers()
            return
        if route.startswith("s") and route[1:].isdigit():
            self.send_response(int(route[1:]))
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not path.is_file():
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if route not in ("gzip", "short", "slow"):
            # plain routes stream from disk: the real 8 GiB tests must not hold the file in RAM
            self.send_response(200)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Content-Length", str(path.stat().st_size))
            if route == "ref":
                self.send_header("X-Seen-Referer", self.headers.get("Referer", ""))
            self.end_headers()
            with path.open("rb") as fh:
                shutil.copyfileobj(fh, self.wfile, 1 << 20)
            return
        data = path.read_bytes()
        if route == "gzip":
            body = gzip.compress(data)
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if route == "short":
            self.send_response(200)
            self.send_header("Content-Length", str(len(data) + 1000))
            self.end_headers()
            self.wfile.write(data[: len(data) // 2])
            self.wfile.flush()
            self.close_connection = True
            return
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(data)))
        if route == "ref":
            self.send_header("X-Seen-Referer", self.headers.get("Referer", ""))
        self.end_headers()
        if route == "slow":
            for i in range(0, len(data), 4096):
                self.wfile.write(data[i : i + 4096])
                time.sleep(srv.slow_s)
            return
        self.wfile.write(data)


class FixtureServer:
    def __init__(self, root: Path) -> None:
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.httpd.root = root
        self.httpd.requests = []
        self.httpd.slow_s = 0.01
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.httpd.server_address[1]}"

    def url(self, route: str, name: str) -> str:
        return f"{self.base}/{route}/{name}"

    @property
    def requests(self):
        return self.httpd.requests

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
