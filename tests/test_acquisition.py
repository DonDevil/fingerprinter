"""Phase 5: media acquisition tests.

Runs entirely against the local HTTP test server (tests/media_test_server.py)
and tiny synthetic media fixtures — no external network access.
"""
import hashlib

import pytest
import requests

from acquisition import (
    ClientError,
    ConnectionTimeoutError,
    GoneError,
    InvalidMediaError,
    MediaAcquirer,
    NotFoundError,
    PermanentAcquisitionError,
    RateLimitedError,
    ReadTimeoutError,
    RedirectLimitExceededError,
    ServerError,
    SizeLimitExceededError,
    TransientAcquisitionError,
    UnsupportedContentTypeError,
    UnsupportedSchemeError,
)
from tests.media_test_server import MIN_PNG


def _acquirer(**overrides):
    # allow_private_networks=True: this suite runs entirely against the
    # loopback tests/media_test_server.py fixture, which the SSRF guard
    # (Phase 13A) would otherwise reject by default — a narrow, explicit
    # test-only opt-out, not the production default.
    defaults = dict(
        connect_timeout_s=2.0,
        read_timeout_s=2.0,
        max_redirects=5,
        max_bytes=10 * 1024 * 1024,
        allow_private_networks=True,
    )
    defaults.update(overrides)
    return MediaAcquirer(**defaults)


def test_successful_http_download(media_server):
    acquirer = _acquirer()
    url = media_server.url("/ok")
    artifact = acquirer.acquire(url)
    try:
        assert artifact.local_path.exists()
        assert artifact.byte_size == len(MIN_PNG)
        assert artifact.content_type == "image/png"
        assert artifact.checksum_sha256 == hashlib.sha256(MIN_PNG).hexdigest()
        assert artifact.original_url == url
        assert artifact.final_url == url
        assert artifact.acquisition_duration_s >= 0
        assert artifact.media_metadata is not None  # ffprobe found the PNG stream
    finally:
        artifact.cleanup()


def test_redirect_handling(media_server):
    acquirer = _acquirer()
    url = media_server.url("/redirect/3")
    artifact = acquirer.acquire(url)
    try:
        assert artifact.original_url == url
        assert artifact.final_url == media_server.url("/ok")
        assert artifact.byte_size == len(MIN_PNG)
    finally:
        artifact.cleanup()


def test_final_redirected_url_is_recorded(media_server):
    acquirer = _acquirer()
    url = media_server.url("/redirect/2")
    artifact = acquirer.acquire(url)
    try:
        assert artifact.original_url == url
        assert artifact.final_url == media_server.url("/ok")
        assert artifact.final_url != artifact.original_url
    finally:
        artifact.cleanup()


def test_redirect_limit_exceeded(media_server):
    acquirer = _acquirer(max_redirects=2)
    with pytest.raises(RedirectLimitExceededError) as exc_info:
        acquirer.acquire(media_server.url("/redirect-loop"))
    assert isinstance(exc_info.value, PermanentAcquisitionError)


def test_connection_timeout_maps_to_transient():
    # A real OS-level connect-timeout is not reliably reproducible in a
    # sandboxed/local-only test environment, so this exercises the mapping
    # via a fake transport that raises what requests raises on a real one.
    class FakeSession:
        def get(self, *args, **kwargs):
            raise requests.exceptions.ConnectTimeout("simulated connect timeout")

    acquirer = MediaAcquirer(session=FakeSession(), allow_private_networks=True)
    with pytest.raises(ConnectionTimeoutError) as exc_info:
        acquirer.acquire("http://127.0.0.1:9/unreachable")
    assert isinstance(exc_info.value, TransientAcquisitionError)


def test_read_timeout(media_server):
    acquirer = _acquirer(read_timeout_s=0.3, connect_timeout_s=2.0)
    with pytest.raises(ReadTimeoutError) as exc_info:
        acquirer.acquire(media_server.url("/slow"))
    assert isinstance(exc_info.value, TransientAcquisitionError)


def test_maximum_size_enforced_while_streaming(media_server):
    acquirer = _acquirer(max_bytes=512)
    with pytest.raises(SizeLimitExceededError) as exc_info:
        acquirer.acquire(media_server.url("/large"))
    assert isinstance(exc_info.value, PermanentAcquisitionError)


def test_unsupported_content_type(media_server):
    acquirer = _acquirer()
    with pytest.raises(UnsupportedContentTypeError):
        acquirer.acquire(media_server.url("/badtype"))


def test_http_404_classified_permanent(media_server):
    acquirer = _acquirer()
    with pytest.raises(NotFoundError) as exc_info:
        acquirer.acquire(media_server.url("/notfound"))
    assert isinstance(exc_info.value, PermanentAcquisitionError)
    assert exc_info.value.status_code == 404


def test_http_410_classified_permanent(media_server):
    acquirer = _acquirer()
    with pytest.raises(GoneError) as exc_info:
        acquirer.acquire(media_server.url("/gone"))
    assert isinstance(exc_info.value, PermanentAcquisitionError)


def test_http_5xx_classified_transient(media_server):
    acquirer = _acquirer()
    with pytest.raises(ServerError) as exc_info:
        acquirer.acquire(media_server.url("/error"))
    assert isinstance(exc_info.value, TransientAcquisitionError)
    assert exc_info.value.status_code == 500


def test_http_429_classified_transient(media_server):
    acquirer = _acquirer()
    with pytest.raises(RateLimitedError) as exc_info:
        acquirer.acquire(media_server.url("/toomany"))
    assert isinstance(exc_info.value, TransientAcquisitionError)
    assert exc_info.value.status_code == 429


def test_corrupt_media_rejected(media_server):
    acquirer = _acquirer()
    with pytest.raises(InvalidMediaError) as exc_info:
        acquirer.acquire(media_server.url("/corrupt"))
    assert isinstance(exc_info.value, PermanentAcquisitionError)


def test_checksum_correctness(media_server):
    acquirer = _acquirer()
    artifact = acquirer.acquire(media_server.url("/ok"))
    try:
        assert artifact.checksum_sha256 == hashlib.sha256(MIN_PNG).hexdigest()
    finally:
        artifact.cleanup()


def test_temp_file_cleaned_up_on_size_limit_failure(media_server, tmp_path):
    acquirer = _acquirer(max_bytes=512, temp_dir=str(tmp_path))
    with pytest.raises(SizeLimitExceededError):
        acquirer.acquire(media_server.url("/large"))
    assert list(tmp_path.iterdir()) == []


def test_temp_file_cleaned_up_on_corrupt_media_failure(media_server, tmp_path):
    acquirer = _acquirer(temp_dir=str(tmp_path))
    with pytest.raises(InvalidMediaError):
        acquirer.acquire(media_server.url("/corrupt"))
    assert list(tmp_path.iterdir()) == []


def test_artifact_remains_available_until_explicit_cleanup(media_server):
    acquirer = _acquirer()
    artifact = acquirer.acquire(media_server.url("/ok"))
    assert artifact.local_path.exists()
    assert artifact.local_path.read_bytes() == MIN_PNG  # still readable, not touched
    artifact.cleanup()
    assert not artifact.local_path.exists()
    artifact.cleanup()  # idempotent, no error


def test_unsupported_scheme_rejected():
    acquirer = _acquirer()
    with pytest.raises(UnsupportedSchemeError) as exc_info:
        acquirer.acquire("ftp://example.com/video.mp4")
    assert isinstance(exc_info.value, PermanentAcquisitionError)
