"""HTTP(S) media acquisition: URL in, validated local MediaArtifact out.

Independent of every fingerprinting concern (Phase 5 objective) — this
module only knows how to turn a URL into a validated local file safely.
Nothing here imports `worker`/`work_queue`; the queue-side wiring lives in
`worker/acquisition_handler.py`.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Callable, Mapping, Optional, Sequence
from urllib.parse import urljoin, urlparse

import requests
import urllib3.exceptions

from acquisition.artifact import MediaArtifact
from acquisition.errors import (
    ClientError,
    ConnectionTimeoutError,
    GoneError,
    InvalidMediaError,
    NetworkError,
    NotFoundError,
    PermanentAcquisitionError,
    RateLimitedError,
    ReadTimeoutError,
    RedirectLimitExceededError,
    ServerError,
    SizeLimitExceededError,
    UnexpectedStatusError,
    UnsupportedContentTypeError,
    UnsupportedSchemeError,
)
from acquisition.validation import probe_media

DEFAULT_ALLOWED_SCHEMES: Sequence[str] = ("http", "https")
DEFAULT_ALLOWED_CONTENT_TYPE_PREFIXES: Sequence[str] = ("video/", "image/", "audio/")
DEFAULT_MAX_REDIRECTS = 5
DEFAULT_MAX_BYTES = 100 * 1024 * 1024  # 100 MiB
DEFAULT_CONNECT_TIMEOUT_S = 5.0
DEFAULT_READ_TIMEOUT_S = 30.0
DEFAULT_CHUNK_SIZE = 64 * 1024

_REDIRECT_CODES = (301, 302, 303, 307, 308)


class MediaAcquirer:
    """Downloads a media URL to a validated local temp file.

    Streams to disk with a bounded size, never trusting Content-Length,
    Content-Type, or file extension on their own — the size limit is
    enforced against actual bytes written, and content-type/media validity
    are checked against what was actually downloaded.
    """

    def __init__(
        self,
        connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S,
        read_timeout_s: float = DEFAULT_READ_TIMEOUT_S,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_redirects: int = DEFAULT_MAX_REDIRECTS,
        allowed_schemes: Sequence[str] = DEFAULT_ALLOWED_SCHEMES,
        allowed_content_type_prefixes: Sequence[str] = DEFAULT_ALLOWED_CONTENT_TYPE_PREFIXES,
        temp_dir: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        session: Optional[requests.Session] = None,
        validate: bool = True,
        validator: Optional[Callable[[Path], Mapping[str, object]]] = None,
        user_agent: str = "fingerprinter-acquirer/1",
    ):
        self._connect_timeout_s = connect_timeout_s
        self._read_timeout_s = read_timeout_s
        self._max_bytes = max_bytes
        self._max_redirects = max_redirects
        self._allowed_schemes = tuple(s.lower() for s in allowed_schemes)
        self._allowed_content_type_prefixes = tuple(allowed_content_type_prefixes)
        self._temp_dir = temp_dir
        self._chunk_size = chunk_size
        self._session = session or requests.Session()
        self._validate = validate
        self._validator = validator or probe_media
        self._user_agent = user_agent

    def acquire(self, url: str) -> MediaArtifact:
        """Download `url` to a local temp file and validate it.

        Raises a TransientAcquisitionError or PermanentAcquisitionError
        subclass on any failure (see acquisition/errors.py); the temp file
        is always cleaned up before raising. Returns a MediaArtifact on
        success — the caller owns cleanup of that file from here on.
        """
        started = time.time()
        current_url = url
        redirects = 0

        while True:
            self._check_scheme(current_url)
            response = self._send(current_url)

            if response.status_code in _REDIRECT_CODES:
                location = response.headers.get("Location")
                response.close()
                if not location:
                    raise PermanentAcquisitionError(
                        f"redirect from {current_url} had no Location header"
                    )
                redirects += 1
                if redirects > self._max_redirects:
                    raise RedirectLimitExceededError(
                        f"exceeded max_redirects={self._max_redirects} starting from {url}"
                    )
                current_url = urljoin(current_url, location)
                continue
            break

        self._classify_status(response)
        content_type = self._check_content_type(response)

        try:
            local_path, byte_size, checksum = self._stream_to_disk(response)
        finally:
            response.close()

        media_metadata = None
        if self._validate:
            try:
                media_metadata = self._validator(local_path)
            except InvalidMediaError:
                self._safe_unlink(local_path)
                raise

        return MediaArtifact(
            local_path=local_path,
            original_url=url,
            final_url=current_url,
            content_type=content_type,
            byte_size=byte_size,
            checksum_sha256=checksum,
            acquisition_duration_s=time.time() - started,
            media_metadata=media_metadata,
        )

    def _check_scheme(self, url: str) -> None:
        scheme = urlparse(url).scheme.lower()
        if scheme not in self._allowed_schemes:
            raise UnsupportedSchemeError(f"unsupported URL scheme: {scheme!r} in {url}")

    def _send(self, url: str) -> requests.Response:
        try:
            return self._session.get(
                url,
                stream=True,
                allow_redirects=False,
                timeout=(self._connect_timeout_s, self._read_timeout_s),
                headers={"User-Agent": self._user_agent},
            )
        except requests.exceptions.ConnectTimeout as exc:
            raise ConnectionTimeoutError(f"connection timeout to {url}") from exc
        except requests.exceptions.ReadTimeout as exc:
            raise ReadTimeoutError(f"read timeout waiting for headers from {url}") from exc
        except requests.exceptions.ConnectionError as exc:
            raise NetworkError(f"connection failed to {url}: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise NetworkError(f"request failed for {url}: {exc}") from exc

    def _classify_status(self, response: requests.Response) -> None:
        code = response.status_code
        if 200 <= code < 300:
            return
        response.close()
        url = response.url
        if code == 404:
            raise NotFoundError(code, f"404 Not Found: {url}")
        if code == 410:
            raise GoneError(code, f"410 Gone: {url}")
        if code == 429:
            raise RateLimitedError(code, f"429 Too Many Requests: {url}")
        if 500 <= code < 600:
            raise ServerError(code, f"{code} server error: {url}")
        if 400 <= code < 500:
            raise ClientError(code, f"{code} client error: {url}")
        raise UnexpectedStatusError(code, f"unexpected status {code}: {url}")

    def _check_content_type(self, response: requests.Response) -> str:
        raw = response.headers.get("Content-Type", "")
        content_type = raw.split(";")[0].strip().lower()
        if content_type and not any(
            content_type.startswith(prefix) for prefix in self._allowed_content_type_prefixes
        ):
            response.close()
            raise UnsupportedContentTypeError(f"unsupported content type: {content_type!r}")
        # A missing/generic header is not, on its own, grounds to reject —
        # it is never trusted as proof of validity either; probe_media()
        # is the authority on whether the bytes are usable media.
        return content_type or "application/octet-stream"

    def _stream_to_disk(self, response: requests.Response):
        fd, path_str = tempfile.mkstemp(
            dir=self._temp_dir, prefix="fingerprinter-acq-", suffix=".media"
        )
        path = Path(path_str)
        sha256 = hashlib.sha256()
        total = 0
        try:
            with os.fdopen(fd, "wb") as f:
                for chunk in response.iter_content(chunk_size=self._chunk_size):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > self._max_bytes:
                        raise SizeLimitExceededError(
                            f"exceeded max_bytes={self._max_bytes} while downloading {response.url}"
                        )
                    f.write(chunk)
                    sha256.update(chunk)
        except requests.exceptions.ReadTimeout as exc:
            self._safe_unlink(path)
            raise ReadTimeoutError(f"read timeout while streaming {response.url}") from exc
        except requests.exceptions.ConnectionError as exc:
            self._safe_unlink(path)
            # urllib3 raises ReadTimeoutError for a stalled body read; requests
            # re-wraps it as a plain ConnectionError instead of ReadTimeout, so
            # unwrap one level to classify it correctly (transient either way,
            # but ReadTimeoutError is the more specific/useful classification).
            if isinstance(exc.args[0] if exc.args else None, urllib3.exceptions.ReadTimeoutError):
                raise ReadTimeoutError(f"read timeout while streaming {response.url}") from exc
            raise NetworkError(f"connection interrupted while streaming {response.url}: {exc}") from exc
        except Exception:
            self._safe_unlink(path)
            raise
        return path, total, sha256.hexdigest()

    @staticmethod
    def _safe_unlink(path: Path) -> None:
        path.unlink(missing_ok=True)
