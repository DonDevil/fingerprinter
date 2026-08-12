"""Phase 9 — segment-level embedding cache + registry integration.

Mirrors `tests/test_target.py`'s style (no DINOv2, no real media — tiny
synthetic segment vectors) for the new `SegmentEmbeddingCache` /
`FilesystemSegmentEmbeddingCache` (`target/segment_cache.py`) and the
`TargetRegistry` methods that use it (`register_segment_embedding`,
`get_compatible_segment_embedding`, `has_compatible_segment_embedding`).
"""
import json

import pytest

from embedding.result import SegmentEmbedding
from target.cache import FilesystemEmbeddingCache
from target.keys import target_segment_embeddings_key
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.versioning import EmbeddingSpec

SPEC = EmbeddingSpec(
    model_id="dinov2-synthetic",
    model_version="v1",
    embedding_schema_version=1,
    preprocessing_config={"resize": 224},
    sampling_config={"segment_duration_s": 5.0, "frame_selection": "segment_start"},
)

SEGMENTS = [
    SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),
    SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(0.4, 0.5, 0.6)),
    SegmentEmbedding(segment_index=2, start_time=10.0, end_time=15.0, vector=(0.7, 0.8, 0.9)),
]
COARSE_VECTOR = [0.4, 0.5, 0.6]


def _write(tmp_path, name, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return path


@pytest.fixture
def segment_cache(tmp_path):
    return FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")


@pytest.fixture
def pooled_cache(tmp_path):
    return FilesystemEmbeddingCache(tmp_path / "embedding-cache")


@pytest.fixture
def registry(redis_client, pooled_cache, segment_cache):
    return TargetRegistry(redis_client, pooled_cache, segment_cache)


def test_segments_can_be_stored_and_retrieved(segment_cache):
    segment_cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    entry = segment_cache.get("target-1", "v1", "deadbeef", SPEC)

    assert entry is not None
    assert len(entry.segments) == 3
    assert entry.segments[0].segment_index == 0
    assert entry.segments[0].vector == (0.1, 0.2, 0.3)
    assert entry.segments[2].end_time == 15.0
    assert entry.coarse_vector == tuple(COARSE_VECTOR)


def test_segment_cache_miss_returns_none(segment_cache):
    assert segment_cache.get("target-1", "v1", "deadbeef", SPEC) is None
    assert segment_cache.exists("target-1", "v1", "deadbeef", SPEC) is False


def test_segment_cache_one_file_per_target_not_per_segment(segment_cache, tmp_path):
    segment_cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    files = list((tmp_path / "segment-cache").glob("*.json"))
    assert len(files) == 1  # not one file per segment


def test_segment_cache_miss_for_different_sampling_config(segment_cache):
    segment_cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)

    other = EmbeddingSpec(
        model_id=SPEC.model_id,
        model_version=SPEC.model_version,
        embedding_schema_version=SPEC.embedding_schema_version,
        preprocessing_config=SPEC.preprocessing_config,
        sampling_config={"segment_duration_s": 10.0, "frame_selection": "segment_start"},
    )
    assert segment_cache.get("target-1", "v1", "deadbeef", other) is None


def test_corrupted_segment_cache_entry_is_rejected(segment_cache):
    segment_cache.put("target-1", "v1", "deadbeef", SPEC, SEGMENTS, COARSE_VECTOR)
    path = segment_cache._path_for("target-1", "v1", "deadbeef", SPEC)

    path.write_text("{not valid json")
    assert segment_cache.get("target-1", "v1", "deadbeef", SPEC) is None

    path.write_text(json.dumps({"segment_cache_entry_schema_version": 1, "target_id": "target-1"}))
    assert segment_cache.get("target-1", "v1", "deadbeef", SPEC) is None


def test_registry_register_and_retrieve_segment_embedding(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))

    registry.register_segment_embedding("target-1", "v1", SPEC, SEGMENTS, COARSE_VECTOR)

    assert registry.has_compatible_segment_embedding("target-1", "v1", SPEC) is True
    entry = registry.get_compatible_segment_embedding("target-1", "v1", SPEC)
    assert entry is not None
    assert len(entry.segments) == 3

    meta = registry._redis.hget(target_segment_embeddings_key("target-1", "v1"), SPEC.spec_key())
    assert meta is not None
    parsed = json.loads(meta)
    assert parsed["segment_count"] == 3


def test_registry_without_segment_cache_reports_no_compatible_segments(redis_client, pooled_cache):
    registry_without_segments = TargetRegistry(redis_client, pooled_cache)  # segment_cache defaults to None

    assert registry_without_segments.has_compatible_segment_embedding("target-1", "v1", SPEC) is False
    with pytest.raises(RuntimeError):
        registry_without_segments.register_segment_embedding("target-1", "v1", SPEC, SEGMENTS, COARSE_VECTOR)


def test_pooled_and_segment_caches_are_independent(registry, tmp_path):
    """Phase 6's pooled-vector cache and Phase 9's segment cache are
    separate collaborators — registering one must not affect the other."""
    media = _write(tmp_path, "movie.mp4", b"target bytes")
    registry.register_target("target-1", "v1", str(media))

    registry.register_segment_embedding("target-1", "v1", SPEC, SEGMENTS, COARSE_VECTOR)

    assert registry.has_compatible_embedding("target-1", "v1", SPEC) is False
    assert registry.has_compatible_segment_embedding("target-1", "v1", SPEC) is True
