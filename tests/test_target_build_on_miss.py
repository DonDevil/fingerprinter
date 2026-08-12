"""Phase 10 — TargetRegistry.get_or_build_segment_embedding: cache-first,
build-on-miss-under-lock resolution (design-proposal-1 §8's "Build-on-miss
race" guard), applied to Phase 9's segment representation.

No DINOv2, no real media — synthetic segments and a synthetic `build`
callback, mirroring tests/test_segment_cache.py's style; `target/lock.py`
itself is covered separately in tests/test_target_lock.py.
"""
import threading
import time

import pytest

from embedding.result import SegmentEmbedding
from target.cache import FilesystemEmbeddingCache
from target.keys import target_lock_key
from target.lock import RedisLock
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.versioning import EmbeddingSpec, cache_entry_key

SPEC = EmbeddingSpec(
    model_id="dinov2-synthetic",
    model_version="v1",
    embedding_schema_version=1,
    preprocessing_config={},
    sampling_config={"segment_duration_s": 5.0, "frame_selection": "segment_start"},
)

SEGMENTS = (
    SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),
    SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(0.4, 0.5, 0.6)),
)
COARSE_VECTOR = (0.4, 0.5, 0.6)


def _write(tmp_path, name="movie.mp4", content: bytes = b"target bytes"):
    path = tmp_path / name
    path.write_bytes(content)
    return path


@pytest.fixture
def registry(redis_client, tmp_path):
    pooled = FilesystemEmbeddingCache(tmp_path / "embedding-cache")
    segments = FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")
    return TargetRegistry(redis_client, pooled, segments)


def test_cache_hit_never_calls_build(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path)))
    registry.register_segment_embedding("target-1", "v1", SPEC, SEGMENTS, COARSE_VECTOR)

    calls = {"n": 0}

    def build(record):
        calls["n"] += 1
        return SEGMENTS, COARSE_VECTOR

    entry = registry.get_or_build_segment_embedding("target-1", "v1", SPEC, build)

    assert calls["n"] == 0
    assert len(entry.segments) == 2


def test_cache_miss_builds_exactly_once_and_registers(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path)))

    calls = {"n": 0}

    def build(record):
        calls["n"] += 1
        assert record.target_id == "target-1"
        return SEGMENTS, COARSE_VECTOR

    entry = registry.get_or_build_segment_embedding("target-1", "v1", SPEC, build)

    assert calls["n"] == 1
    assert len(entry.segments) == 2
    assert registry.has_compatible_segment_embedding("target-1", "v1", SPEC) is True

    # a second call is now a pure cache hit
    entry2 = registry.get_or_build_segment_embedding("target-1", "v1", SPEC, build)
    assert calls["n"] == 1
    assert entry2.segments == entry.segments


def test_unknown_target_raises_keyerror_without_calling_build(registry):
    calls = {"n": 0}

    def build(record):
        calls["n"] += 1
        return SEGMENTS, COARSE_VECTOR

    with pytest.raises(KeyError):
        registry.get_or_build_segment_embedding("nope", "v1", SPEC, build)
    assert calls["n"] == 0


def test_without_segment_cache_raises_runtimeerror(redis_client, tmp_path):
    pooled = FilesystemEmbeddingCache(tmp_path / "embedding-cache")
    registry_without_segments = TargetRegistry(redis_client, pooled)  # segment_cache defaults to None

    with pytest.raises(RuntimeError):
        registry_without_segments.get_or_build_segment_embedding(
            "target-1", "v1", SPEC, lambda record: (SEGMENTS, COARSE_VECTOR)
        )


def test_build_exception_releases_lock_for_a_later_retry(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path)))

    def failing_build(record):
        raise RuntimeError("simulated embedding failure")

    with pytest.raises(RuntimeError):
        registry.get_or_build_segment_embedding("target-1", "v1", SPEC, failing_build)

    entry = registry.get_or_build_segment_embedding(
        "target-1", "v1", SPEC, lambda record: (SEGMENTS, COARSE_VECTOR)
    )
    assert len(entry.segments) == 2


def test_concurrent_miss_builds_only_once(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path)))

    calls = {"n": 0}
    started = threading.Event()

    def slow_build(record):
        started.set()
        calls["n"] += 1
        time.sleep(0.3)
        return SEGMENTS, COARSE_VECTOR

    results = []

    def winner():
        results.append(registry.get_or_build_segment_embedding("target-1", "v1", SPEC, slow_build))

    def loser():
        assert started.wait(timeout=2)
        time.sleep(0.05)  # let the winner actually acquire the lock first
        results.append(
            registry.get_or_build_segment_embedding(
                "target-1", "v1", SPEC, slow_build, poll_interval_s=0.05, poll_timeout_s=5.0
            )
        )

    t1 = threading.Thread(target=winner)
    t2 = threading.Thread(target=loser)
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert calls["n"] == 1
    assert len(results) == 2
    assert len(results[0].segments) == 2
    assert len(results[1].segments) == 2


def test_lock_wait_timeout_raises_without_calling_build(redis_client, registry, tmp_path):
    record = registry.register_target("target-1", "v1", str(_write(tmp_path)))

    # Simulate another worker holding the build lock indefinitely.
    key = target_lock_key(cache_entry_key("target-1", "v1", record.content_sha256, SPEC))
    holder = RedisLock(redis_client, key)
    assert holder.acquire(ttl_ms=5000) is True

    def build(record):
        raise AssertionError("build must not be called by the waiting loser")

    with pytest.raises(TimeoutError):
        registry.get_or_build_segment_embedding(
            "target-1", "v1", SPEC, build, poll_interval_s=0.05, poll_timeout_s=0.2
        )
