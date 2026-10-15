"""Typed sources and acquisition (design §5, P9-D10/D24).

``external_http`` (created only by intake from ``encode.requested``): streamed
GET with ``Accept-Encoding: identity``; the **raw served bytes** are read
(``decode_content=False``) and hashed — never content-decoded bytes; a
non-identity ``Content-Encoding`` is refused; redirects are followed by hand
(≤ ``acquire.max_redirects``) and **every hop** passes the SSRF guard (V1
``acquisition.ssrf_guard``, reused as code); ``Referer`` per
``strict-origin-when-cross-origin``; declared length checked first, then
counted, bounded by 8 GiB (P8 ``hash_max_bytes``); the SHA-256 must equal the
request's ContentId digest.

``internal_object`` (created only by the target manager): read through the
MinIO SDK from the allowlisted bucket and ``ref/<sha256>`` key — never HTTP.

DNS rebinding (the guard resolves, the HTTP library resolves again) is **not**
closed here; it stays deferred to P14 (P8 handoff, plan).
"""

from __future__ import annotations

import hashlib
import ipaddress
import re
import shutil
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from antipiracy_contracts.events.fingerprinting import EncodeFailure as F

from acquisition.errors import UnsafeDestinationError
from acquisition.ssrf_guard import is_unsafe_address, resolve_addresses
from fingerprinter.config import Settings
from fingerprinter.errors import EncodeError, StorageUnavailableError
from fingerprinter.objects import ObjectMissingError, Objects

_REDIRECTS = {301, 302, 303, 307, 308}
_REF_KEY = re.compile(r"^ref/[0-9a-f]{64}$")
CHUNK = 1 << 20


@dataclass(frozen=True)
class Acquired:
    path: Path
    size: int
    sha256: str
    source_key: str
    final_url: str | None = None
    redirects: int = 0


def referer_for(page: str | None, target: str) -> str | None:
    """``strict-origin-when-cross-origin``: full URL same-origin, origin only
    cross-origin, nothing on an https → http downgrade."""
    if not page:
        return None
    p, t = urlsplit(page), urlsplit(target)
    if p.scheme == "https" and t.scheme == "http":
        return None
    if (p.scheme, p.hostname, p.port) == (t.scheme, t.hostname, t.port):
        return urlunsplit((p.scheme, p.netloc, p.path or "/", p.query, ""))
    return urlunsplit((p.scheme, p.netloc, "/", "", ""))


class Acquirer:
    def __init__(self, settings: Settings, objects: Objects | None) -> None:
        self.cfg = settings.acquire
        self.settings = settings
        self.objects = objects
        self.allow = [ipaddress.ip_network(c) for c in self.cfg.ssrf_allow_cidrs]
        self.session = requests.Session()
        self.session.trust_env = False

    # --- entry point -----------------------------------------------------------

    def acquire(self, source_key: str, doc: dict[str, Any], digest_hex: str, dest: Path) -> Acquired:
        dest.parent.mkdir(parents=True, exist_ok=True)
        kind = doc.get("type")
        if kind == "external_http":
            return self._http(source_key, doc, digest_hex, dest)
        if kind == "internal_object":
            return self._internal(source_key, doc, digest_hex, dest)
        raise EncodeError(F.INTERNAL_ERROR, f"unknown source type {kind!r}", retryable=False, scope="source")

    def _check_space(self, dest: Path, declared: int | None) -> None:
        if declared is None:
            return
        free = shutil.disk_usage(dest.parent).free
        if declared > min(free, self.settings.scratch_max_bytes):
            raise EncodeError(F.INTERNAL_ERROR, "scratch_space", retryable=True, scope="attempt")

    # --- external HTTP -------------------------------------------------------

    def _guard(self, url: str) -> None:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise EncodeError(F.SOURCE_UNREACHABLE, "non-http locator", retryable=False, scope="source")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        try:
            addresses = resolve_addresses(parts.hostname, port)
        except (socket.gaierror, OSError) as exc:
            raise EncodeError(F.SOURCE_UNREACHABLE, f"dns: {exc}", retryable=True, scope="source") from exc
        for addr in addresses:
            if any(addr in net for net in self.allow if addr.version == net.version):
                continue
            if is_unsafe_address(addr):
                raise EncodeError(
                    F.SOURCE_FORBIDDEN, "ssrf: disallowed destination", retryable=False, scope="source"
                )

    def _http(self, source_key: str, doc: dict[str, Any], digest_hex: str, dest: Path) -> Acquired:
        url = str(doc["media"]["locator"]["url"])
        page = (doc.get("referer") or {}).get("url")
        deadline = time.monotonic() + self.cfg.timeout_s
        hops = 0
        seen = {url}
        while True:
            try:
                self._guard(url)
            except UnsafeDestinationError as exc:
                raise EncodeError(F.SOURCE_FORBIDDEN, f"ssrf: {exc}", retryable=False, scope="source") from exc
            headers = {
                "User-Agent": self.cfg.user_agent,
                "Accept": "*/*",
                "Accept-Encoding": "identity",
            }
            ref = referer_for(page, url)
            if ref:
                headers["Referer"] = ref
            try:
                resp = self.session.get(
                    url,
                    headers=headers,
                    stream=True,
                    allow_redirects=False,
                    timeout=(self.cfg.connect_timeout_s, self.cfg.read_timeout_s),
                )
            except requests.RequestException as exc:
                raise EncodeError(
                    F.SOURCE_UNREACHABLE, f"{type(exc).__name__}", retryable=True, scope="source"
                ) from exc
            try:
                location = resp.headers.get("location")
                if resp.status_code in _REDIRECTS and location:
                    target = urljoin(url, location.strip())
                    if urlsplit(target).scheme not in ("http", "https"):
                        raise EncodeError(F.SOURCE_UNREACHABLE, "redirect to non-http", retryable=False, scope="source")
                    if target in seen or hops >= self.cfg.max_redirects:
                        raise EncodeError(F.SOURCE_UNREACHABLE, "redirect loop/limit", retryable=False, scope="source")
                    seen.add(target)
                    hops += 1
                    url = target
                    continue
                return self._body(resp, source_key, digest_hex, dest, deadline, url, hops)
            finally:
                resp.close()

    def _body(
        self,
        resp: requests.Response,
        source_key: str,
        digest_hex: str,
        dest: Path,
        deadline: float,
        url: str,
        hops: int,
    ) -> Acquired:
        status = resp.status_code
        if status in (401, 403, 451):
            raise EncodeError(F.SOURCE_FORBIDDEN, f"http {status}", retryable=False, scope="source")
        if status in (404, 410):
            raise EncodeError(F.SOURCE_UNREACHABLE, f"http {status}", retryable=False, scope="source")
        if status >= 500 or status == 429:
            raise EncodeError(F.SOURCE_UNREACHABLE, f"http {status}", retryable=True, scope="source")
        if status != 200:
            raise EncodeError(F.SOURCE_UNREACHABLE, f"http {status}", retryable=False, scope="source")
        encoding = resp.headers.get("content-encoding", "").strip().lower()
        if encoding and encoding != "identity":
            raise EncodeError(
                F.UNSUPPORTED_FORMAT, f"content-encoding {encoding[:30]}", retryable=False, scope="source"
            )
        length = resp.headers.get("content-length")
        declared = int(length) if length and length.strip().isdigit() else None
        if declared is not None and declared > self.cfg.max_bytes:
            raise EncodeError(F.TOO_LARGE, f"declared {declared} bytes", retryable=False, scope="content")
        self._check_space(dest, declared)
        sha = hashlib.sha256()
        count = 0
        try:
            with dest.open("wb") as out:
                while True:
                    if time.monotonic() > deadline:
                        raise EncodeError(F.SOURCE_UNREACHABLE, "acquire timeout", retryable=True, scope="source")
                    chunk = resp.raw.read(CHUNK, decode_content=False)
                    if not chunk:
                        break
                    count += len(chunk)
                    if count > self.cfg.max_bytes:
                        raise EncodeError(F.TOO_LARGE, f"over {self.cfg.max_bytes} bytes", retryable=False, scope="content")
                    sha.update(chunk)
                    out.write(chunk)
        except (requests.RequestException, OSError) as exc:
            if isinstance(exc, OSError) and exc.errno == 28:  # ENOSPC
                raise EncodeError(F.INTERNAL_ERROR, "scratch_space", retryable=True) from exc
            raise EncodeError(F.SOURCE_UNREACHABLE, f"read: {type(exc).__name__}", retryable=True, scope="source") from exc
        except Exception as exc:  # urllib3 protocol errors (reset, incomplete read)
            if isinstance(exc, EncodeError):
                raise
            raise EncodeError(F.SOURCE_UNREACHABLE, f"read: {type(exc).__name__}", retryable=True, scope="source") from exc
        if declared is not None and count != declared:
            raise EncodeError(F.SOURCE_UNREACHABLE, f"short read {count}/{declared}", retryable=True, scope="source")
        got = sha.hexdigest()
        if got != digest_hex:
            raise EncodeError(F.SOURCE_UNREACHABLE, "content_mismatch", retryable=False, scope="source")
        return Acquired(dest, count, got, source_key, url, hops)

    # --- internal object -----------------------------------------------------

    def validate_internal(self, doc: dict[str, Any]) -> None:
        bucket, key = doc.get("bucket"), str(doc.get("key", ""))
        if self.objects is None or bucket != self.objects.cfg.targets_bucket:
            raise EncodeError(F.INTERNAL_ERROR, "internal source outside allowlist", retryable=False, scope="source")
        if not _REF_KEY.fullmatch(key) or key != f"ref/{doc.get('sha256')}":
            raise EncodeError(F.INTERNAL_ERROR, "internal key not ref/<sha256>", retryable=False, scope="source")
        if int(doc.get("size", 0)) > self.cfg.max_bytes:
            raise EncodeError(F.TOO_LARGE, "internal object over limit", retryable=False, scope="content")

    def _internal(self, source_key: str, doc: dict[str, Any], digest_hex: str, dest: Path) -> Acquired:
        self.validate_internal(doc)
        assert self.objects is not None
        self._check_space(dest, int(doc["size"]))
        sha = hashlib.sha256()
        count = 0
        try:
            with dest.open("wb") as out:
                for chunk in self.objects.stream(doc["bucket"], doc["key"]):
                    count += len(chunk)
                    if count > self.cfg.max_bytes:
                        raise EncodeError(F.TOO_LARGE, "internal object over limit", retryable=False, scope="content")
                    sha.update(chunk)
                    out.write(chunk)
        except ObjectMissingError as exc:
            raise EncodeError(F.INTERNAL_ERROR, "reference_missing", retryable=False, scope="source") from exc
        except StorageUnavailableError as exc:
            raise EncodeError(F.INTERNAL_ERROR, "storage_unavailable", retryable=True, scope="source") from exc
        got = sha.hexdigest()
        if got != digest_hex or count != int(doc["size"]):
            raise EncodeError(F.SOURCE_UNREACHABLE, "content_mismatch", retryable=False, scope="source")
        return Acquired(dest, count, got, source_key)
