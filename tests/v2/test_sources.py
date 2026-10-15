"""Typed sources and raw identity-byte acquisition (design §5, TS-1…TS-9).

A loopback fixture server stands in for the web; the SSRF allowlist exempts
only 127.0.0.0/8 (dev/test setting), so 10.0.0.0/8 etc. stay forbidden.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os

import pytest

from fingerprinter.config import Settings
from fingerprinter.errors import EncodeError
from fingerprinter.objects import Objects
from fingerprinter.requests import internal_source
from fingerprinter.sources import Acquirer, referer_for
from tests.v2.conftest import INTEGRATION, integration
from tests.v2.helpers import request_env


def _settings(**acq):
    s = Settings.from_env()
    return s.model_copy(update={"acquire": s.acquire.model_copy(update={"ssrf_allow_cidrs": ["127.0.0.0/8"], **acq})})


def _doc(env):
    from fingerprinter.requests import external_source

    src = external_source(env, __import__("datetime").datetime.now())
    return src.key, src.doc


def _acquire(web, tmp_path, name, data, route="f", acquirer=None, digest=None, referer=None):
    (web.httpd.root / name).write_bytes(data)
    env = request_env(data, web.url(route, name), referer=referer)
    key, doc = _doc(env)
    acq = acquirer or Acquirer(_settings(), None)
    return acq.acquire(key, doc, digest or hashlib.sha256(data).hexdigest(), tmp_path / "out" / name)


def test_referer_policy_strict_origin_when_cross_origin():
    page = "https://site.example/watch/1?x=2#frag"
    assert referer_for(page, "https://site.example/v.mp4") == "https://site.example/watch/1?x=2"
    assert referer_for(page, "https://cdn.example/v.mp4") == "https://site.example/"
    assert referer_for(page, "http://site.example/v.mp4") is None  # https → http
    assert referer_for(None, "https://x/") is None


def test_ts1_raw_identity_bytes_hashed_and_verified(web, tmp_path):
    data = os.urandom(300_000)
    got = _acquire(web, tmp_path, "a.bin", data)
    assert got.sha256 == hashlib.sha256(data).hexdigest() and got.size == len(data)
    assert got.path.read_bytes() == data
    (_, headers), = [r for r in web.requests if r[0].endswith("a.bin")]
    assert headers["Accept-Encoding"] == "identity"


def test_ts2_redirects_followed_ssrf_checked_and_referer_per_hop(web, tmp_path):
    data = b"redirected" * 1000
    got = _acquire(web, tmp_path, "r.bin", data, route="r3", referer=web.base + "/page/1")
    assert got.redirects == 3
    seen = [h.get("Referer") for p, h in web.requests]
    assert all(r == web.base + "/page/1" for r in seen)  # same origin: full URL
    web.requests.clear()
    _acquire(web, tmp_path, "r2.bin", data, route="r1", referer="http://localhost:1/page")
    assert {h.get("Referer") for _, h in web.requests} == {"http://localhost:1/"}  # cross origin


def test_redirect_limit(web, tmp_path):
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "x.bin", b"x", route="r9")
    assert e.value.failure.value == "source_unreachable" and not e.value.retryable


def test_ts3_redirect_to_private_network_forbidden(web, tmp_path):
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "p.bin", b"x", route="private")
    assert e.value.failure.value == "source_forbidden" and not e.value.retryable
    # without the dev allowlist even loopback is refused
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "q.bin", b"x", acquirer=Acquirer(Settings.from_env(), None))
    assert e.value.failure.value == "source_forbidden"


def test_ts4_content_encoding_refused_never_hashed_decoded(web, tmp_path):
    data = b"compressible " * 5000
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "g.bin", data, route="gzip")
    assert e.value.failure.value == "unsupported_format" and not e.value.retryable
    assert gzip.compress(data) != data


def test_ts5_digest_mismatch_is_content_mismatch(web, tmp_path):
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "m.bin", b"served bytes", digest="0" * 64)
    assert e.value.detail == "content_mismatch" and e.value.scope == "source" and not e.value.retryable


def test_ts6_size_limit_exact_and_plus_one(web, tmp_path):
    limit = 100_000
    acq = Acquirer(_settings(max_bytes=limit), None)
    assert _acquire(web, tmp_path, "eq.bin", b"e" * limit, acquirer=acq).size == limit
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "big.bin", b"b" * (limit + 1), acquirer=acq)
    assert e.value.failure.value == "too_large" and e.value.scope == "content"
    assert Settings.from_env().acquire.max_bytes == 8 * 1024**3  # approved bound, not 100 MiB


@pytest.mark.slow
@pytest.mark.skipif(os.environ.get("RUN_P9_SLOW") != "1", reason="writes 8 GiB to scratch (RUN_P9_SLOW=1)")
def test_ts6_real_8gib_boundary(web, tmp_path):
    """Sparse files served over loopback: exactly 8 GiB accepted, 8 GiB + 1 refused."""
    limit = 8 * 1024**3
    exact = web.httpd.root / "exact.bin"
    with exact.open("wb") as fh:
        fh.truncate(limit)
    sha = hashlib.sha256()
    with exact.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 24), b""):
            sha.update(chunk)
    env = request_env("sha256:" + sha.hexdigest(), web.url("f", "exact.bin"))
    key, doc = _doc(env)
    got = Acquirer(_settings(), None).acquire(key, doc, sha.hexdigest(), tmp_path / "o" / "exact")
    assert got.size == limit
    got.path.unlink()
    over = web.httpd.root / "over.bin"
    with over.open("wb") as fh:
        fh.truncate(limit + 1)
    env = request_env("sha256:" + "1" * 64, web.url("f", "over.bin"))
    key, doc = _doc(env)
    with pytest.raises(EncodeError) as e:
        Acquirer(_settings(), None).acquire(key, doc, "1" * 64, tmp_path / "o" / "over")
    assert e.value.failure.value == "too_large"  # refused from the declared length


@pytest.mark.parametrize("route,failure,retryable", [
    ("s404", "source_unreachable", False), ("s410", "source_unreachable", False),
    ("s403", "source_forbidden", False), ("s401", "source_forbidden", False),
    ("s451", "source_forbidden", False), ("s503", "source_unreachable", True),
    ("short", "source_unreachable", True),
])
def test_http_status_and_interrupted_reads(web, tmp_path, route, failure, retryable):
    with pytest.raises(EncodeError) as e:
        _acquire(web, tmp_path, "s.bin", b"z" * 50_000, route=route)
    assert e.value.failure.value == failure and e.value.retryable is retryable


def test_connection_refused_is_retryable(tmp_path):
    env = request_env(b"x", "http://127.0.0.1:9/nothing.bin")
    key, doc = _doc(env)
    with pytest.raises(EncodeError) as e:
        Acquirer(_settings(), None).acquire(key, doc, "0" * 64, tmp_path / "x")
    assert e.value.failure.value == "source_unreachable" and e.value.retryable


@pytest.fixture
def objects():
    if not INTEGRATION:
        pytest.skip("needs MinIO")
    s = Settings.from_env()
    o = Objects.connect(s.minio)
    o.ensure_buckets()
    return o


@integration
def test_ts7_internal_object_read_via_sdk_never_http(objects, web, tmp_path):
    data = os.urandom(200_000)
    sha = hashlib.sha256(data).hexdigest()
    bucket = objects.cfg.targets_bucket
    objects.put_bytes(bucket, f"ref/{sha}", data, sha)
    src = internal_source(bucket, f"ref/{sha}", len(data), sha, "tgt/1")
    got = Acquirer(_settings(), objects).acquire(src.key, src.doc, sha, tmp_path / "i")
    assert got.sha256 == sha and web.requests == []
    objects.delete(bucket, f"ref/{sha}")


@integration
def test_ts8_internal_source_outside_allowlist_rejected(objects, tmp_path):
    sha = "a" * 64
    acq = Acquirer(_settings(), objects)
    for doc in (
        internal_source(objects.cfg.representations_bucket, f"ref/{sha}", 1, sha, "x").doc,
        internal_source(objects.cfg.targets_bucket, "rep/../../etc", 1, sha, "x").doc,
        internal_source(objects.cfg.targets_bucket, f"ref/{'b' * 64}", 1, sha, "x").doc,
    ):
        with pytest.raises(EncodeError) as e:
            acq.acquire("obj:x", doc, sha, tmp_path / "x")
        assert not e.value.retryable


def test_ts8_events_can_only_create_external_sources():
    from fingerprinter.requests import external_source

    env = request_env(b"x", "http://127.0.0.1:19000/fingerprinter-targets/ref/" + "a" * 64)
    src = external_source(env, __import__("datetime").datetime.now())
    assert src.type == "external_http" and src.key.startswith("http:")  # SSRF-guarded HTTP, never SDK


@integration
def test_ts9_missing_internal_object_and_transient_storage(objects, tmp_path):
    sha = "c" * 64
    src = internal_source(objects.cfg.targets_bucket, f"ref/{sha}", 10, sha, "x")
    with pytest.raises(EncodeError) as e:
        Acquirer(_settings(), objects).acquire(src.key, src.doc, sha, tmp_path / "m")
    assert e.value.detail == "reference_missing" and not e.value.retryable
    down = Objects.connect(objects.cfg.model_copy(update={"endpoint": "127.0.0.1:9"}))
    down.client._http.connection_pool_kw["retries"] = None  # fail fast
    with pytest.raises(EncodeError) as e:
        Acquirer(_settings(), down).acquire(src.key, src.doc, sha, tmp_path / "t")
    assert e.value.detail == "storage_unavailable" and e.value.retryable


def test_large_media_streamed_with_bounded_memory(web, tmp_path):
    import resource

    data = os.urandom(64 * 1024 * 1024)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    got = _acquire(web, tmp_path, "large.bin", data)
    assert got.size == len(data)
    del data
    grown_mib = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before) / 1024
    assert grown_mib < 200  # file + 64 MiB test buffer; acquisition itself streams in 1 MiB chunks
    json.dumps({"rss_growth_mib": grown_mib})
