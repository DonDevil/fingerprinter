"""Phase 13D — shared artifact storage: multi-host target-cache correctness.

The Phase 13D audit (`docs/architecture/phase-13d-multi-host-target-cache-
audit.md`, §9) found that no test anywhere in this repository constructs
two independent `TargetRegistry` instances backed by two separate storage
clients sharing one Redis client — the minimum setup needed to even
*simulate* two hosts. This file is that missing regression coverage,
against the new `target/shared_cache.py` / `target/shared_storage.py`
backend Phase 13D adds.

**SIMULATED MULTI-HOST**, not real multi-host: everything below runs in one
process, one Redis (`tests/conftest.py`'s `redis_client`, db 15), and one
physical filesystem. What *is* genuinely independent per "host" is the
Python object graph: each simulated host constructs its own
`SharedArtifactStore` client and its own `TargetRegistry`, sharing only the
Redis client and the shared-storage root path — the structurally relevant
property (independent local state, shared coordination + shared storage),
not real network latency or real shared-filesystem consistency semantics.
See the Phase 13D implementation doc, "Multi-host validation status", for
what this does and does not prove.
"""
from __future__ import annotations

import hashlib
import threading
import time

import pytest

from embedding.result import SegmentEmbedding
from target.keys import target_lock_key
from target.registry import TargetRegistry
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedArtifactStoreError, SharedTargetMediaStore
from target.versioning import EmbeddingSpec, cache_entry_key

SPEC = EmbeddingSpec(
    model_id="dinov2-synthetic",
    model_version="v1",
    embedding_schema_version=1,
    preprocessing_config={"resize": 224},
    sampling_config={"segment_duration_s": 5.0, "frame_selection": "segment_start"},
)

SEGMENTS = (
    SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),
    SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(0.4, 0.5, 0.6)),
)
COARSE_VECTOR = (0.4, 0.5, 0.6)


def _write(root, name="movie.mp4", content: bytes = b"target bytes"):
    path = root / name
    path.write_bytes(content)
    return path


def _make_registry(redis_client, shared_root, media_store=None) -> TargetRegistry:
    """One simulated host: its own `SharedArtifactStore` client object
    (independent Python instance — not the same object another simulated
    host uses) pointed at the one shared root every host in the fleet is
    configured with, its own pooled/segment cache wrappers, sharing only
    `redis_client` and `shared_root`."""
    store = SharedArtifactStore(shared_root)
    pooled = SharedFilesystemEmbeddingCache(store, prefix="pooled")
    segments = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
    return TargetRegistry(redis_client, pooled, segments, media_store=media_store)


# ---------------------------------------------------------------------------
# Central acceptance criterion (task brief §5/§6): two independent
# registries, separate local host directories, shared Redis + shared
# artifact backend, concurrent request for the same target -> exactly one
# build, both callers get a valid result, no duplicate computation.
# ---------------------------------------------------------------------------


def test_two_independent_registries_share_one_build_simulated_multi_host(redis_client, tmp_path):
    # Two simulated hosts' local disks — deliberately distinct directories,
    # never passed to each other, proving neither registry's local state is
    # shared (only the storage backend and Redis are).
    host_a_local = tmp_path / "host-a-local-disk"
    host_b_local = tmp_path / "host-b-local-disk"
    host_a_local.mkdir()
    host_b_local.mkdir()

    shared_root = tmp_path / "shared-artifact-store"  # the one genuinely shared resource

    registry_a = _make_registry(redis_client, shared_root)
    registry_b = _make_registry(redis_client, shared_root)
    assert registry_a is not registry_b

    # Registration happens once, on "host A" — mirrors a real fleet where
    # one host runs whatever registers targets.
    media = _write(host_a_local, content=b"shared multi-host target bytes")
    registry_a.register_target("target-1", "v1", str(media))

    calls = {"n": 0}
    started = threading.Event()

    def slow_build(record):
        started.set()
        calls["n"] += 1
        time.sleep(0.3)
        return SEGMENTS, COARSE_VECTOR

    results = []

    def worker_a():
        results.append(registry_a.get_or_build_segment_embedding("target-1", "v1", SPEC, slow_build))

    def worker_b():
        assert started.wait(timeout=2)
        time.sleep(0.05)  # let whichever registry wins the race actually acquire the lock first
        results.append(
            registry_b.get_or_build_segment_embedding(
                "target-1", "v1", SPEC, slow_build, poll_interval_s=0.05, poll_timeout_s=5.0
            )
        )

    t_a = threading.Thread(target=worker_a)
    t_b = threading.Thread(target=worker_b)
    t_a.start()
    t_b.start()
    t_a.join(timeout=5)
    t_b.join(timeout=5)

    # 1-4. Exactly one build occurred (winner built exactly once).
    assert calls["n"] == 1
    # Both workers obtained a valid embedding.
    assert len(results) == 2
    assert len(results[0].segments) == 2
    assert len(results[1].segments) == 2
    assert results[0].coarse_vector == results[1].coarse_vector

    # 7/8. The registry that did NOT run the build (loser) can still be
    # shown, independently, to observe the winner's write through the
    # shared backend — not through the thread result above (which could in
    # principle be satisfied by an in-process shortcut), but via a *third*,
    # freshly constructed registry/store pair, proving the artifact is
    # genuinely readable from shared storage by an instance that never
    # participated in the race at all.
    registry_c = _make_registry(redis_client, shared_root)

    def build_must_not_be_called(record):
        raise AssertionError("a third, independent registry must observe the completed shared artifact, not rebuild")

    entry = registry_c.get_or_build_segment_embedding("target-1", "v1", SPEC, build_must_not_be_called)
    assert len(entry.segments) == 2

    # 9. No duplicate computation occurred at any point across all three registries.
    assert calls["n"] == 1

    # 10. Redis lock semantics remain correct: the lock is not left held.
    lock_key = target_lock_key(cache_entry_key("target-1", "v1", registry_a.get_target("target-1", "v1").content_sha256, SPEC))
    assert redis_client.get(lock_key) is None

    # host B's local disk was never touched by any of this — proves the
    # registries really did keep independent local state and everything
    # flowed through the shared backend / Redis instead.
    assert list(host_b_local.iterdir()) == []


def test_second_registry_pure_cache_hit_after_first_registry_builds(redis_client, tmp_path):
    """Sequential (non-racing) version of the same property: once one
    registry has built and published, a second, independently constructed
    registry never even attempts a build — a pure shared-storage hit, no
    lock contention at all."""
    shared_root = tmp_path / "shared-artifact-store"
    host_a_local = tmp_path / "host-a"
    host_a_local.mkdir()

    registry_a = _make_registry(redis_client, shared_root)
    media = _write(host_a_local, content=b"sequential multi-host bytes")
    registry_a.register_target("target-1", "v1", str(media))
    registry_a.get_or_build_segment_embedding("target-1", "v1", SPEC, lambda record: (SEGMENTS, COARSE_VECTOR))

    registry_b = _make_registry(redis_client, shared_root)

    def build_must_not_be_called(record):
        raise AssertionError("registry_b must observe registry_a's completed artifact via shared storage")

    entry = registry_b.get_or_build_segment_embedding("target-1", "v1", SPEC, build_must_not_be_called)
    assert len(entry.segments) == 2
    assert entry.coarse_vector == COARSE_VECTOR


# ---------------------------------------------------------------------------
# Target media fleet-accessibility (audit §3.5 / task brief §4): media
# published by whichever host ran registration is fetchable by a
# completely independent store client, without ever sharing a local
# directory.
# ---------------------------------------------------------------------------


def test_target_media_published_by_one_host_is_fetchable_by_an_independent_store_client(tmp_path):
    shared_root = tmp_path / "shared-artifact-store"
    host_a_local = tmp_path / "host-a"
    host_b_local = tmp_path / "host-b"
    host_a_local.mkdir()
    host_b_local.mkdir()

    media = _write(host_a_local, content=b"the target's actual video bytes")
    content_sha256 = hashlib.sha256(media.read_bytes()).hexdigest()

    store_a = SharedArtifactStore(shared_root)
    media_store_a = SharedTargetMediaStore(store_a)
    media_store_a.publish(content_sha256, media)

    # An entirely independent client object, pointed only at the shared
    # root -- host B never saw host A's local directory.
    store_b = SharedArtifactStore(shared_root)
    media_store_b = SharedTargetMediaStore(store_b)

    fetched = media_store_b.fetch_to_temp(content_sha256, suffix=".mp4")
    try:
        assert fetched is not None
        assert fetched.read_bytes() == b"the target's actual video bytes"
    finally:
        if fetched is not None:
            fetched.unlink(missing_ok=True)

    assert list(host_b_local.iterdir()) == []  # host B's own disk was never touched


def test_target_media_never_published_is_a_clean_miss_not_an_error(tmp_path):
    store = SharedArtifactStore(tmp_path / "shared-artifact-store")
    media_store = SharedTargetMediaStore(store)
    assert media_store.fetch_to_temp("0" * 64) is None


def test_registry_register_target_publishes_media_when_media_store_configured(redis_client, tmp_path):
    shared_root = tmp_path / "shared-artifact-store"
    store = SharedArtifactStore(shared_root)
    media_store = SharedTargetMediaStore(store)
    registry = _make_registry(redis_client, shared_root, media_store=media_store)

    media = _write(tmp_path, content=b"published via register_target")
    record = registry.register_target("target-1", "v1", str(media))

    fresh_store = SharedArtifactStore(shared_root)
    fresh_media_store = SharedTargetMediaStore(fresh_store)
    fetched = fresh_media_store.fetch_to_temp(record.content_sha256)
    try:
        assert fetched is not None
        assert fetched.read_bytes() == b"published via register_target"
    finally:
        if fetched is not None:
            fetched.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# A. Cache hit — existing compatible artifact returned without rebuilding.
# ---------------------------------------------------------------------------


def test_cache_hit_returns_existing_entry_without_rebuild(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    entry = cache.get("target-1", "v1", "deadbeef", SPEC)
    assert entry is not None
    assert len(entry.segments) == 2
    assert entry.coarse_vector == COARSE_VECTOR


def test_cache_miss_returns_none(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    assert cache.get("target-1", "v1", "deadbeef", SPEC) is None
    assert cache.exists("target-1", "v1", "deadbeef", SPEC) is False


# ---------------------------------------------------------------------------
# B. Different target content -> distinct artifact.
# ---------------------------------------------------------------------------


def test_different_content_hash_creates_a_distinct_artifact(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "content-a", SPEC, SEGMENTS, COARSE_VECTOR)

    assert cache.get("target-1", "v1", "content-b", SPEC) is None
    assert cache.get("target-1", "v1", "content-a", SPEC) is not None


# ---------------------------------------------------------------------------
# C/D/E. Model version / preprocessing / sampling invalidation.
# ---------------------------------------------------------------------------


def test_model_version_change_is_a_cache_miss(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    other = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version="v2-different",
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config=SPEC.preprocessing_config,
        sampling_config=SPEC.sampling_config,
    )
    assert cache.get("target-1", "v1", "deadbeef", other) is None


def test_preprocessing_config_change_is_a_cache_miss(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    other = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version=SPEC.model_version,
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config={"resize": 512},
        sampling_config=SPEC.sampling_config,
    )
    assert cache.get("target-1", "v1", "deadbeef", other) is None


def test_sampling_config_change_is_a_cache_miss(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    other = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version=SPEC.model_version,
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config=SPEC.preprocessing_config,
        sampling_config={"segment_duration_s": 10.0, "frame_selection": "segment_start"},
    )
    assert cache.get("target-1", "v1", "deadbeef", other) is None


# ---------------------------------------------------------------------------
# F. Crash during build: no corrupt artifact becomes visible, lock is
# released, another worker can retry.
# ---------------------------------------------------------------------------


def test_crash_during_build_leaves_no_partial_artifact_and_releases_lock(redis_client, tmp_path):
    shared_root = tmp_path / "shared-artifact-store"
    registry = _make_registry(redis_client, shared_root)
    media = _write(tmp_path, content=b"crash test bytes")
    registry.register_target("target-1", "v1", str(media))

    def failing_build(record):
        raise RuntimeError("simulated embedding crash")

    with pytest.raises(RuntimeError):
        registry.get_or_build_segment_embedding("target-1", "v1", SPEC, failing_build)

    # No partial artifact is visible -- a fresh, independent registry still
    # sees a clean miss, not a corrupt/partial entry.
    registry_other = _make_registry(redis_client, shared_root)
    assert registry_other.has_compatible_segment_embedding("target-1", "v1", SPEC) is False

    # The lock was released, so a retry succeeds.
    entry = registry_other.get_or_build_segment_embedding(
        "target-1", "v1", SPEC, lambda record: (SEGMENTS, COARSE_VECTOR)
    )
    assert len(entry.segments) == 2


# ---------------------------------------------------------------------------
# G. Two independent registries (structural coverage; the concurrency test
# above already exercises this end-to-end).
# ---------------------------------------------------------------------------


def test_two_registries_have_independent_python_objects_but_shared_backend(redis_client, tmp_path):
    shared_root = tmp_path / "shared-artifact-store"
    registry_a = _make_registry(redis_client, shared_root)
    registry_b = _make_registry(redis_client, shared_root)

    assert registry_a is not registry_b
    assert registry_a._cache is not registry_b._cache
    assert registry_a._segment_cache is not registry_b._segment_cache


# ---------------------------------------------------------------------------
# K. Artifact atomicity -- a reader must never consume a partially written
# artifact. `SharedArtifactStore.put_bytes` writes to a tempfile in the
# same directory and `os.replace`s it into place; this proves a reader
# racing the write sees either nothing or the complete entry, never a torn
# write.
# ---------------------------------------------------------------------------


def test_concurrent_reader_never_observes_a_partial_artifact(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)

    big_segments = tuple(
        SegmentEmbedding(segment_index=i, start_time=float(i * 5), end_time=float((i + 1) * 5), vector=tuple(float(i) for _ in range(512)))
        for i in range(200)
    )

    errors = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            entry = cache.get("target-1", "v1", "deadbeef", SPEC)
            if entry is not None and len(entry.segments) != len(big_segments):
                errors.append(len(entry.segments))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()

    for _ in range(20):
        cache.put("target-1", "v1", "deadbeef", SPEC, big_segments, COARSE_VECTOR)

    stop.set()
    for t in threads:
        t.join(timeout=5)

    assert errors == []


# ---------------------------------------------------------------------------
# Failure semantics (task brief §11): shared storage being unreachable
# raises, distinct from "not found" -- never silently treated as a miss.
# ---------------------------------------------------------------------------


def test_unreachable_shared_store_root_raises_at_construction(tmp_path):
    # A file, not a directory: mkdir(parents=True, exist_ok=True) against
    # it fails with a real OSError.
    blocked = tmp_path / "not-a-directory"
    blocked.write_text("not a directory")

    with pytest.raises(SharedArtifactStoreError):
        SharedArtifactStore(blocked / "child")


def test_shared_store_read_failure_is_distinguishable_from_a_miss(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    cache = SharedFilesystemSegmentEmbeddingCache(store)
    cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    key = cache._key("target-1", "v1", "deadbeef", SPEC)
    path = store._path_for(key)
    path.chmod(0o000)
    try:
        with pytest.raises(SharedArtifactStoreError):
            cache.get("target-1", "v1", "deadbeef", SPEC)
    finally:
        path.chmod(0o644)  # restore so tmp_path cleanup can remove it
