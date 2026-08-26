"""Target-management design doc — TargetRegistry-level lifecycle coverage:
on_conflict policy, the stale content-index fix, the list index, metadata
patching, delete (including target-exclusive cache cleanup and shared-media
reference counting), reindex, and lifecycle-lock concurrency.

Mirrors tests/test_target.py's style (no DINOv2, no real media — tiny
synthetic files and vectors) and tests/test_target_build_on_miss.py's
threading.Event synchronization pattern for the concurrency cases.
"""
import threading
import time

import pytest

from target.cache import FilesystemEmbeddingCache
from target.errors import TargetAlreadyExistsError, TargetLockTimeoutError, TargetNotFoundError
from target.keys import target_content_index_key, target_index_key, target_record_lock_key
from target.lock import RedisLock
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedTargetMediaStore
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
def segment_cache(tmp_path):
    return FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")


@pytest.fixture
def registry(redis_client, cache, segment_cache):
    return TargetRegistry(redis_client, cache, segment_cache)


# ---------------------------------------------------------------------------
# on_conflict policy
# ---------------------------------------------------------------------------


def test_register_target_default_on_conflict_is_replace_backward_compatible(registry, tmp_path):
    import hashlib

    media = _write(tmp_path, "movie.mp4", b"original bytes")
    original = registry.register_target("target-1", "v1", str(media))

    media.write_bytes(b"different bytes now")
    record = registry.register_target("target-1", "v1", str(media))  # no on_conflict passed

    assert record.content_sha256 == hashlib.sha256(b"different bytes now").hexdigest()
    assert record.content_sha256 != original.content_sha256
    assert record.created_at == original.created_at  # created_at preserved across replace


def test_register_target_on_conflict_reject_raises_on_different_content(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"original bytes")
    original = registry.register_target("target-1", "v1", str(media))

    media.write_bytes(b"different bytes now")
    with pytest.raises(TargetAlreadyExistsError):
        registry.register_target("target-1", "v1", str(media), on_conflict="reject")

    # Existing record is completely untouched.
    unchanged = registry.get_target("target-1", "v1")
    assert unchanged.content_sha256 == original.content_sha256


def test_register_target_on_conflict_reject_allows_identical_content_retry(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"identical bytes")
    first = registry.register_target("target-1", "v1", str(media), on_conflict="reject")
    second = registry.register_target("target-1", "v1", str(media), on_conflict="reject")

    assert first.created_at == second.created_at
    assert second.content_sha256 == first.content_sha256


def test_register_target_invalid_on_conflict_value_raises(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    with pytest.raises(ValueError):
        registry.register_target("target-1", "v1", str(media), on_conflict="bogus")


# ---------------------------------------------------------------------------
# Stale content-index regression (audit S4)
# ---------------------------------------------------------------------------


def test_content_changing_reregistration_removes_stale_content_index_entry(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"original bytes")
    original = registry.register_target("target-1", "v1", str(media))
    old_hash = original.content_sha256

    media.write_bytes(b"different bytes now")
    registry.register_target("target-1", "v1", str(media))  # on_conflict="replace" default

    # The old hash's reverse-index set no longer contains this target.
    assert registry.find_by_content_hash(old_hash) == []
    assert registry._redis.smembers(target_content_index_key(old_hash)) == set()


def test_content_changing_reregistration_does_not_touch_other_targets_sharing_old_hash(registry, tmp_path):
    shared_bytes = b"shared original bytes"
    media_a = _write(tmp_path, "a.mp4", shared_bytes)
    media_b = _write(tmp_path, "b.mp4", shared_bytes)
    registry.register_target("target-a", "v1", str(media_a))
    registry.register_target("target-b", "v1", str(media_b))

    media_a.write_bytes(b"target-a now has new content")
    registry.register_target("target-a", "v1", str(media_a))

    # target-b still correctly shows up under the old shared hash.
    remaining = registry.find_by_content_hash(
        __import__("hashlib").sha256(shared_bytes).hexdigest()
    )
    ids = {(r.target_id, r.target_version) for r in remaining}
    assert ids == {("target-b", "v1")}


# ---------------------------------------------------------------------------
# List index
# ---------------------------------------------------------------------------


def test_list_targets_empty_when_no_targets(registry):
    assert registry.list_targets() == []


def test_list_targets_multiple_targets_and_versions_deterministic(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    registry.register_target("blast", "v2", str(_write(tmp_path, "b.mp4", b"b")))
    registry.register_target("avatar", "v1", str(_write(tmp_path, "c.mp4", b"c")))

    pairs = [(r.target_id, r.target_version) for r in registry.list_targets()]
    assert pairs == [("avatar", "v1"), ("blast", "v1"), ("blast", "v2")]

    # Determinism: calling again with no intervening writes gives the same order.
    assert [(r.target_id, r.target_version) for r in registry.list_targets()] == pairs


def test_list_targets_skips_stale_member_whose_record_is_missing(registry, tmp_path):
    registry.register_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))

    # Simulate a crash between SADD and DEL: delete the record hash directly,
    # bypassing delete_target, leaving the index member stale.
    from target.keys import target_key

    registry._redis.delete(target_key("blast", "v1"))

    assert registry.list_targets() == []
    # The stale index membership itself is still there (only the record vanished).
    assert registry._redis.smembers(target_index_key()) != set()


# ---------------------------------------------------------------------------
# Metadata update
# ---------------------------------------------------------------------------


def test_update_target_metadata_shallow_merge(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media), media_metadata={"genre": "action"})

    updated = registry.update_target_metadata("target-1", "v1", set_fields={"region": "IN"})

    assert updated.media_metadata == {"genre": "action", "region": "IN"}


def test_update_target_metadata_remove_fields(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target(
        "target-1", "v1", str(media), media_metadata={"genre": "action", "region": "IN"}
    )

    updated = registry.update_target_metadata("target-1", "v1", remove_fields=["region"])

    assert updated.media_metadata == {"genre": "action"}


def test_update_target_metadata_set_and_remove_same_key_ends_up_removed(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media), media_metadata={"genre": "action"})

    updated = registry.update_target_metadata(
        "target-1", "v1", set_fields={"genre": "comedy"}, remove_fields=["genre"]
    )

    assert "genre" not in updated.media_metadata


def test_update_target_metadata_preserves_identity_fields_and_timestamps(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    original = registry.register_target("target-1", "v1", str(media))
    time.sleep(0.01)

    updated = registry.update_target_metadata("target-1", "v1", set_fields={"genre": "action"})

    assert updated.target_id == original.target_id
    assert updated.target_version == original.target_version
    assert updated.media_path == original.media_path
    assert updated.content_sha256 == original.content_sha256
    assert updated.created_at == original.created_at
    assert updated.updated_at > original.updated_at


def test_update_target_metadata_missing_target_raises_not_found(registry):
    with pytest.raises(TargetNotFoundError):
        registry.update_target_metadata("nope", "v1", set_fields={"a": "b"})


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


def test_delete_target_removes_registry_record_and_both_indexes(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    record = registry.register_target("target-1", "v1", str(media))

    registry.delete_target("target-1", "v1")

    assert registry.get_target("target-1", "v1") is None
    assert registry.list_targets() == []
    assert registry.find_by_content_hash(record.content_sha256) == []


def test_delete_target_removes_target_exclusive_pooled_and_segment_cache(registry, tmp_path, cache, segment_cache):
    from embedding.result import SegmentEmbedding

    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)
    segments = (SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),)
    registry.register_segment_embedding("target-1", "v1", SPEC, segments, (0.1, 0.2, 0.3))

    assert cache.exists("target-1", "v1", registry.get_target("target-1", "v1").content_sha256, SPEC)

    content_sha256 = registry.get_target("target-1", "v1").content_sha256
    registry.delete_target("target-1", "v1")

    assert cache.exists("target-1", "v1", content_sha256, SPEC) is False
    assert segment_cache.exists("target-1", "v1", content_sha256, SPEC) is False


def test_delete_target_missing_raises_not_found(registry):
    with pytest.raises(TargetNotFoundError):
        registry.delete_target("nope", "v1")


def test_delete_target_does_not_affect_another_target(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    registry.register_target("target-2", "v1", str(_write(tmp_path, "b.mp4", b"b")))

    registry.delete_target("target-1", "v1")

    assert registry.get_target("target-1", "v1") is None
    assert registry.get_target("target-2", "v1") is not None
    assert [r.target_id for r in registry.list_targets()] == ["target-2"]


def test_delete_target_retains_shared_media_referenced_by_another_target(redis_client, tmp_path):
    store = SharedArtifactStore(tmp_path / "shared-store")
    media_store = SharedTargetMediaStore(store)
    pooled = SharedFilesystemEmbeddingCache(store, prefix="pooled")
    segments = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
    registry = TargetRegistry(redis_client, pooled, segments, media_store=media_store)

    shared_bytes = b"shared media bytes"
    media_a = _write(tmp_path, "a.mp4", shared_bytes)
    media_b = _write(tmp_path, "b.mp4", shared_bytes)
    record_a = registry.register_target("target-a", "v1", str(media_a))
    registry.register_target("target-b", "v1", str(media_b))

    registry.delete_target("target-a", "v1")

    fetched = media_store.fetch_to_temp(record_a.content_sha256)
    try:
        assert fetched is not None  # still retained -- target-b references it
    finally:
        if fetched is not None:
            fetched.unlink(missing_ok=True)


def test_delete_target_removes_shared_media_when_last_reference_gone(redis_client, tmp_path):
    store = SharedArtifactStore(tmp_path / "shared-store")
    media_store = SharedTargetMediaStore(store)
    pooled = SharedFilesystemEmbeddingCache(store, prefix="pooled")
    segments = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
    registry = TargetRegistry(redis_client, pooled, segments, media_store=media_store)

    media = _write(tmp_path, "a.mp4", b"exclusive media bytes")
    record = registry.register_target("target-a", "v1", str(media))

    registry.delete_target("target-a", "v1")

    assert media_store.fetch_to_temp(record.content_sha256) is None


def test_delete_target_preserves_historical_results(redis_client, registry, tmp_path):
    """Result records (fingerprint:result:*) are keyed by job_id, not
    target-owned -- delete_target must never touch them."""
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    result_key = "fingerprint:result:job-123"
    redis_client.hset(result_key, mapping={"target_id": "target-1", "target_version": "v1", "outcome": "match"})

    registry.delete_target("target-1", "v1")

    assert redis_client.hgetall(result_key) == {"target_id": "target-1", "target_version": "v1", "outcome": "match"}


# ---------------------------------------------------------------------------
# Reindex
# ---------------------------------------------------------------------------


def test_reindex_backfills_pre_existing_records_and_is_idempotent(registry, tmp_path):
    registry.register_target("target-1", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    # Simulate pre-migration state: the index existed pre-phase, so wipe it.
    registry._redis.delete(target_index_key())
    assert registry.list_targets() == []

    dry_run_result = registry.reindex(dry_run=True)
    assert dry_run_result.added == [("target-1", "v1")]
    assert registry.list_targets() == []  # dry-run wrote nothing

    real_result = registry.reindex(dry_run=False)
    assert real_result.added == [("target-1", "v1")]
    assert [(r.target_id, r.target_version) for r in registry.list_targets()] == [("target-1", "v1")]

    second_result = registry.reindex(dry_run=False)
    assert second_result.added == []  # idempotent -- nothing new to add
    assert second_result.found == [("target-1", "v1")]


def test_reindex_never_touches_embeddings_or_content_index(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))
    registry.register_embedding("target-1", "v1", SPEC, VECTOR)

    before = registry.get_compatible_embedding("target-1", "v1", SPEC)
    registry.reindex(dry_run=False)
    after = registry.get_compatible_embedding("target-1", "v1", SPEC)

    assert before == after


# ---------------------------------------------------------------------------
# Lifecycle-lock concurrency
# ---------------------------------------------------------------------------


def test_concurrent_delete_target_only_one_succeeds(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    outcomes = []

    def attempt():
        try:
            registry.delete_target("target-1", "v1")
            outcomes.append("deleted")
        except TargetNotFoundError:
            outcomes.append("not_found")

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert sorted(outcomes) == ["deleted", "not_found"]
    assert registry.get_target("target-1", "v1") is None


def test_lifecycle_lock_timeout_raises_without_mutating(registry, tmp_path, redis_client, monkeypatch):
    import target.registry as registry_module

    monkeypatch.setattr(registry_module, "LIFECYCLE_LOCK_POLL_TIMEOUT_S", 0.3)
    monkeypatch.setattr(registry_module, "LIFECYCLE_LOCK_POLL_INTERVAL_S", 0.05)

    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    holder = RedisLock(redis_client, target_record_lock_key("target-1", "v1"))
    assert holder.acquire(ttl_ms=5000) is True
    try:
        with pytest.raises(TargetLockTimeoutError):
            registry.update_target_metadata("target-1", "v1", set_fields={"a": "b"})
    finally:
        holder.release()

    # Nothing was written while the lock was held by someone else.
    assert registry.get_target("target-1", "v1").media_metadata == {}
