"""Phase 6: target identity, versioning, embedding cache, registry.

No DINOv2, no real media — tiny synthetic files and tiny synthetic vectors
throughout, per the phase brief.
"""
import hashlib
import json
import os

import pytest
from redis import Redis

from target.cache import FilesystemEmbeddingCache
from target.identity import sha256_file
from target.keys import target_embeddings_key
from target.registry import TargetRegistry
from target.versioning import EmbeddingSpec

SPEC = EmbeddingSpec(
    model_id="dinov2-synthetic",
    model_version="v1",
    embedding_schema_version=1,
    preprocessing_config={"resize": 224},
    sampling_config={"fps": 2.0},
)

VECTOR = [0.1, 0.2, 0.3]


def _write(tmp_path, name, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return path


@pytest.fixture
def cache(tmp_path):
    return FilesystemEmbeddingCache(tmp_path / "embedding-cache")


@pytest.fixture
def registry(redis_client, cache):
    return TargetRegistry(redis_client, cache)


def test_register_target(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"hello world")

    record = registry.register_target("target-1", "v1", str(media), media_metadata={"duration_s": 12.0})

    assert record.target_id == "target-1"
    assert record.target_version == "v1"
    assert record.content_sha256 == hashlib.sha256(b"hello world").hexdigest()
    assert record.media_metadata == {"duration_s": 12.0}


def test_retrieve_target(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"hello world")
    registry.register_target("target-1", "v1", str(media))

    fetched = registry.get_target("target-1", "v1")

    assert fetched is not None
    assert fetched.target_id == "target-1"
    assert fetched.content_sha256 == hashlib.sha256(b"hello world").hexdigest()


def test_retrieve_missing_target_returns_none(registry):
    assert registry.get_target("nope", "v1") is None


def test_target_identity_independent_of_filename(registry, tmp_path):
    a = _write(tmp_path, "a.mp4", b"identical bytes")
    b = _write(tmp_path, "totally-different-name.mp4", b"identical bytes")

    record_a = registry.register_target("target-a", "v1", str(a))
    record_b = registry.register_target("target-b", "v1", str(b))

    assert record_a.content_sha256 == record_b.content_sha256

    matches = registry.find_by_content_hash(record_a.content_sha256)
    ids = {(m.target_id, m.target_version) for m in matches}
    assert ids == {("target-a", "v1"), ("target-b", "v1")}


def test_identical_content_produces_expected_content_hash(tmp_path):
    media = _write(tmp_path, "clip.bin", b"some deterministic bytes")

    assert sha256_file(media) == hashlib.sha256(b"some deterministic bytes").hexdigest()


def test_target_version_represented_correctly(registry, tmp_path):
    media_v1 = _write(tmp_path, "v1.mp4", b"version one content")
    media_v2 = _write(tmp_path, "v2.mp4", b"version two content")

    registry.register_target("target-1", "v1", str(media_v1))
    registry.register_target("target-1", "v2", str(media_v2))

    v1 = registry.get_target("target-1", "v1")
    v2 = registry.get_target("target-1", "v2")

    assert v1.target_version == "v1"
    assert v2.target_version == "v2"
    assert v1.content_sha256 != v2.content_sha256


def test_compatible_embedding_cache_hit(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))

    registry.register_embedding("target-1", "v1", SPEC, VECTOR)

    assert registry.has_compatible_embedding("target-1", "v1", SPEC) is True
    entry = registry.get_compatible_embedding("target-1", "v1", SPEC)
    assert entry is not None
    assert entry.vector == tuple(VECTOR)


def test_cache_miss_for_different_model_version(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)

    other_model_version = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version="v2-different",
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config=SPEC.preprocessing_config,
        sampling_config=SPEC.sampling_config,
    )

    assert registry.has_compatible_embedding("target-1", "v1", other_model_version) is False


def test_cache_miss_for_different_preprocessing_config(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)

    other_preprocessing = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version=SPEC.model_version,
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config={"resize": 512},
        sampling_config=SPEC.sampling_config,
    )

    assert registry.has_compatible_embedding("target-1", "v1", other_preprocessing) is False


def test_cache_miss_for_different_target_content_hash(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"original bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)
    assert registry.has_compatible_embedding("target-1", "v1", SPEC) is True

    # Overwrite the file's bytes without bumping target_version — a
    # re-registration must invalidate the previously cached embedding.
    media.write_bytes(b"different bytes now")
    registry.register_target("target-1", "v1", str(media))

    assert registry.has_compatible_embedding("target-1", "v1", SPEC) is False


def test_synthetic_embedding_can_be_stored_and_retrieved(cache):
    cache.put("target-1", "v1", "deadbeef", SPEC, VECTOR)

    entry = cache.get("target-1", "v1", "deadbeef", SPEC)

    assert entry is not None
    assert entry.vector == tuple(VECTOR)
    assert entry.target_id == "target-1"
    assert entry.content_sha256 == "deadbeef"


def test_corrupted_cache_entry_is_rejected(cache):
    cache.put("target-1", "v1", "deadbeef", SPEC, VECTOR)
    path = cache._path_for("target-1", "v1", "deadbeef", SPEC)

    path.write_text("{not valid json")
    assert cache.get("target-1", "v1", "deadbeef", SPEC) is None

    path.write_text(json.dumps({"cache_entry_schema_version": 1, "target_id": "target-1"}))
    assert cache.get("target-1", "v1", "deadbeef", SPEC) is None


def test_cache_metadata_identifies_the_embedding_representation(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)

    meta = registry._redis.hget(target_embeddings_key("target-1", "v1"), SPEC.spec_key())
    assert meta is not None
    parsed = json.loads(meta)
    assert parsed["model_id"] == SPEC.model_id
    assert parsed["model_version"] == SPEC.model_version
    assert parsed["embedding_schema_version"] == SPEC.embedding_schema_version
    assert "cached_at" in parsed


def test_registry_and_cache_work_independently(tmp_path):
    # Cache alone, no Redis/registry involved at all.
    standalone_cache = FilesystemEmbeddingCache(tmp_path / "standalone-cache")
    standalone_cache.put("t", "v1", "abc123", SPEC, VECTOR)
    assert standalone_cache.exists("t", "v1", "abc123", SPEC) is True

    # Registry alone, backed by a fake cache that never touches disk.
    class _NullCache:
        def get(self, *a, **k):
            return None

        def put(self, *a, **k):
            raise AssertionError("registry-only test must not touch the cache")

        def exists(self, *a, **k):
            return False

    r = Redis.from_url(os.environ.get("FINGERPRINTER_TEST_REDIS_URL", "redis://localhost:6379/15"), decode_responses=True)
    r.flushdb()
    try:
        reg = TargetRegistry(r, _NullCache())
        media = _write(tmp_path, "movie.mp4", b"independent bytes")
        record = reg.register_target("target-x", "v1", str(media))
        assert reg.get_target("target-x", "v1") == record
        assert reg.has_compatible_embedding("target-x", "v1", SPEC) is False
    finally:
        r.flushdb()
        r.close()


def test_repeated_lookup_does_not_recompute_or_store_unnecessarily(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))

    put_calls = []
    original_put = registry._cache.put

    def counting_put(*args, **kwargs):
        put_calls.append(1)
        return original_put(*args, **kwargs)

    registry._cache.put = counting_put

    registry.register_embedding("target-1", "v1", SPEC, VECTOR)
    assert len(put_calls) == 1

    for _ in range(5):
        assert registry.has_compatible_embedding("target-1", "v1", SPEC) is True
        entry = registry.get_compatible_embedding("target-1", "v1", SPEC)
        assert entry.vector == tuple(VECTOR)

    assert len(put_calls) == 1  # lookups never re-store

    content_index_key_members = registry._redis.smembers(
        f"fingerprint:target:content:{registry.get_target('target-1', 'v1').content_sha256}"
    )
    assert len(content_index_key_members) == 1  # no duplicate index entries from repeated lookups
