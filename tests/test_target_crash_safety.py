"""Crash / partial-failure behavior for the target lifecycle's mutating
operations: register_target (create), update_target_metadata, delete_target,
and the cache/shared-media primitives they call.

Redis + local/shared filesystem cannot be made into one distributed
transaction (target-management design doc, S12/S24) -- these tests prove
the specific claims the design and implementation docs make about what
happens when a failure lands mid-sequence: which states are fully
recoverable, which states are safe-but-requires-a-retry-to-finish, and
which states leak a resource without corrupting anything. Failures are
injected by monkeypatching a single call inside the real `TargetRegistry`
methods (never by editing production code), mirroring the fault-injection
style `tests/test_shared_target_storage.py` already uses for its own
unreachable-store tests.
"""
import stat

import pytest

from target.cache import FilesystemEmbeddingCache
from target.errors import TargetLockTimeoutError
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedArtifactStoreError, SharedTargetMediaStore


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


class _BoomAfter:
    """Wraps a bound method so the (n+1)th call onward raises `exc`
    instead of running the real implementation -- simulates a crash/error
    partway through a multi-step operation without touching production
    code."""

    def __init__(self, real, boom_on_call: int, exc: Exception):
        self._real = real
        self._boom_on_call = boom_on_call
        self._exc = exc
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.calls >= self._boom_on_call:
            raise self._exc
        return self._real(*args, **kwargs)


# ---------------------------------------------------------------------------
# CREATE (register_target) crash safety
# ---------------------------------------------------------------------------


def test_register_target_lock_released_when_redis_write_fails(registry, tmp_path, monkeypatch):
    media = _write(tmp_path, "movie.mp4", b"bytes")

    boom = _BoomAfter(registry._redis.hset, boom_on_call=1, exc=RuntimeError("simulated redis failure"))
    monkeypatch.setattr(registry._redis, "hset", boom)

    with pytest.raises(RuntimeError):
        registry.register_target("target-1", "v1", str(media))

    assert registry.get_target("target-1", "v1") is None  # nothing was written

    monkeypatch.undo()  # restore the real hset before retrying
    record = registry.register_target("target-1", "v1", str(media))  # lock must not still be held
    assert record.target_id == "target-1"


def test_register_target_reject_conflict_writes_absolutely_nothing(registry, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"original bytes")
    original = registry.register_target("target-1", "v1", str(media))

    media.write_bytes(b"different bytes now")
    new_hash = __import__("hashlib").sha256(b"different bytes now").hexdigest()

    from target.errors import TargetAlreadyExistsError

    with pytest.raises(TargetAlreadyExistsError):
        registry.register_target("target-1", "v1", str(media), on_conflict="reject")

    # No trace of the rejected content anywhere -- not the record, not
    # either index.
    assert registry.get_target("target-1", "v1").content_sha256 == original.content_sha256
    assert registry.find_by_content_hash(new_hash) == []
    assert [(r.target_id, r.target_version) for r in registry.list_targets()] == [("target-1", "v1")]


def test_register_target_crash_between_content_index_and_list_index_write_is_reindex_repairable(
    registry, tmp_path, monkeypatch
):
    """A crash between the content-index SADD (succeeds) and the list-index
    SADD (never runs) leaves a target that's fully functional -- gettable,
    findable by content hash -- except invisible to list_targets(), exactly
    the same "unrepaired pre-migration gap" state reindex() already exists
    to fix (target-management design doc, S8/S21)."""
    media = _write(tmp_path, "movie.mp4", b"bytes")

    boom = _BoomAfter(registry._redis.sadd, boom_on_call=2, exc=RuntimeError("simulated crash"))
    monkeypatch.setattr(registry._redis, "sadd", boom)

    with pytest.raises(RuntimeError):
        registry.register_target("target-1", "v1", str(media))

    monkeypatch.undo()

    record = registry.get_target("target-1", "v1")
    assert record is not None  # HSET committed
    assert registry.find_by_content_hash(record.content_sha256) != []  # content-index SADD committed
    assert registry.list_targets() == []  # list-index SADD never ran

    result = registry.reindex(dry_run=False)
    assert result.added == [("target-1", "v1")]
    assert [(r.target_id, r.target_version) for r in registry.list_targets()] == [("target-1", "v1")]


# ---------------------------------------------------------------------------
# UPDATE crash safety
# ---------------------------------------------------------------------------


def test_update_target_metadata_lock_released_and_no_partial_write_when_redis_fails(registry, tmp_path, monkeypatch):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media), media_metadata={"genre": "action"})

    boom = _BoomAfter(registry._redis.hset, boom_on_call=1, exc=RuntimeError("simulated redis failure"))
    monkeypatch.setattr(registry._redis, "hset", boom)

    with pytest.raises(RuntimeError):
        registry.update_target_metadata("target-1", "v1", set_fields={"region": "IN"})

    monkeypatch.undo()

    # The failed write left metadata exactly as it was before the call.
    assert registry.get_target("target-1", "v1").media_metadata == {"genre": "action"}

    # Lock was released -- a subsequent call is not blocked/timed out.
    updated = registry.update_target_metadata("target-1", "v1", set_fields={"region": "IN"})
    assert updated.media_metadata == {"genre": "action", "region": "IN"}


# ---------------------------------------------------------------------------
# DELETE crash safety
# ---------------------------------------------------------------------------


def test_delete_target_cache_deletion_failure_leaves_all_redis_state_untouched(registry, tmp_path, monkeypatch):
    """Cache cleanup runs before any Redis mutation in delete_target -- if
    it fails, nothing about the target's Redis-visible state has changed
    yet, and the target remains fully intact and re-deletable."""
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    from target.versioning import EmbeddingSpec

    spec = EmbeddingSpec(model_id="m", model_version="v1", embedding_schema_version=1)
    registry.register_embedding("target-1", "v1", spec, [0.1, 0.2])

    def boom(*args, **kwargs):
        raise OSError("simulated filesystem failure deleting cache entry")

    monkeypatch.setattr(registry._cache, "delete", boom)

    with pytest.raises(OSError):
        registry.delete_target("target-1", "v1")

    # Nothing in Redis was touched -- record, both indexes, all intact.
    record = registry.get_target("target-1", "v1")
    assert record is not None
    assert [(r.target_id, r.target_version) for r in registry.list_targets()] == [("target-1", "v1")]
    assert registry.find_by_content_hash(record.content_sha256) != []

    # Lock released -- retry is possible immediately.
    monkeypatch.undo()
    registry.delete_target("target-1", "v1")
    assert registry.get_target("target-1", "v1") is None


def test_delete_target_crash_between_index_removal_and_hash_deletion_is_retry_safe(registry, tmp_path, monkeypatch):
    """Known narrow window: SREM (content index, list index) are separate
    Redis round trips from the pipelined HDEL that follows. A crash exactly
    between them leaves the target invisible to list_targets()/
    find_by_content_hash() but still resolvable via a direct get_target()
    (the record hash itself was never deleted). This is the one delete_target
    intermediate state that is *not* fully equivalent to "already deleted" --
    documented here, not silently assumed away. It self-heals: calling
    delete_target() again completes the delete correctly."""
    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    class _BoomPipeline:
        def delete(self, *a, **k):
            return self

        def execute(self):
            raise RuntimeError("simulated crash before the pipelined HDEL commits")

    monkeypatch.setattr(registry._redis, "pipeline", lambda: _BoomPipeline())

    with pytest.raises(RuntimeError):
        registry.delete_target("target-1", "v1")

    monkeypatch.undo()

    # The narrow, documented partial state:
    assert registry.list_targets() == []  # index memberships already removed
    assert registry.get_target("target-1", "v1") is not None  # but the record hash itself survives

    # Self-heals: a second delete_target() call finishes the job.
    registry.delete_target("target-1", "v1")
    assert registry.get_target("target-1", "v1") is None


def test_delete_target_shared_media_deletion_failure_leaves_redis_clean_and_blob_merely_leaked(
    redis_client, tmp_path, monkeypatch
):
    """The last step of delete_target (shared-media cleanup) failing after
    every Redis mutation already committed must not corrupt anything or
    resurrect the target -- the target is genuinely gone from Redis's
    perspective, and the only cost is a leaked (not corrupted) blob."""
    store = SharedArtifactStore(tmp_path / "shared-store")
    media_store = SharedTargetMediaStore(store)
    pooled = SharedFilesystemEmbeddingCache(store, prefix="pooled")
    segments = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
    registry = TargetRegistry(redis_client, pooled, segments, media_store=media_store)

    media = _write(tmp_path, "movie.mp4", b"exclusive media bytes")
    record = registry.register_target("target-1", "v1", str(media))

    def boom(*a, **k):
        raise SharedArtifactStoreError("simulated unreachable shared mount")

    monkeypatch.setattr(media_store, "delete", boom)

    with pytest.raises(SharedArtifactStoreError):
        registry.delete_target("target-1", "v1")

    # Redis-visible state: fully deleted, exactly as if nothing had failed.
    assert registry.get_target("target-1", "v1") is None
    assert registry.list_targets() == []
    assert registry.find_by_content_hash(record.content_sha256) == []

    # The blob itself is leaked, not corrupted -- still fetchable intact by
    # a fresh, independent store client.
    monkeypatch.undo()
    fresh_store = SharedArtifactStore(tmp_path / "shared-store")
    fresh_media_store = SharedTargetMediaStore(fresh_store)
    fetched = fresh_media_store.fetch_to_temp(record.content_sha256)
    try:
        assert fetched is not None
        assert fetched.read_bytes() == b"exclusive media bytes"
    finally:
        if fetched is not None:
            fetched.unlink(missing_ok=True)


def test_delete_target_lock_timeout_leaves_target_fully_intact(registry, tmp_path, redis_client, monkeypatch):
    import target.registry as registry_module
    from target.keys import target_record_lock_key
    from target.lock import RedisLock

    monkeypatch.setattr(registry_module, "LIFECYCLE_LOCK_POLL_TIMEOUT_S", 0.3)
    monkeypatch.setattr(registry_module, "LIFECYCLE_LOCK_POLL_INTERVAL_S", 0.05)

    media = _write(tmp_path, "movie.mp4", b"bytes")
    registry.register_target("target-1", "v1", str(media))

    holder = RedisLock(redis_client, target_record_lock_key("target-1", "v1"))
    assert holder.acquire(ttl_ms=5000) is True
    try:
        with pytest.raises(TargetLockTimeoutError):
            registry.delete_target("target-1", "v1")
    finally:
        holder.release()

    # Never got past acquiring the lock -- target is completely untouched.
    assert registry.get_target("target-1", "v1") is not None
    assert registry.list_targets() != []


# ---------------------------------------------------------------------------
# Cache / shared-storage delete() primitives, directly
# ---------------------------------------------------------------------------


def test_filesystem_cache_delete_twice_is_idempotently_false_the_second_time(cache):
    from target.versioning import EmbeddingSpec

    spec = EmbeddingSpec(model_id="m", model_version="v1", embedding_schema_version=1)
    cache.put("target-1", "v1", "deadbeef", spec, [0.1, 0.2])

    assert cache.delete("target-1", "v1", "deadbeef", spec) is True
    assert cache.delete("target-1", "v1", "deadbeef", spec) is False  # already gone, not an error


def test_shared_artifact_store_delete_raises_distinguishably_when_unlink_fails(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    store.put_bytes("media/deadbeef", b"some bytes")

    # Make the containing directory read-only so unlink() fails with
    # PermissionError -- store.delete() must raise SharedArtifactStoreError,
    # not silently report "already absent" or leak a raw OSError.
    parent = store._path_for("media/deadbeef").parent
    parent.chmod(stat.S_IRUSR | stat.S_IXUSR)
    try:
        with pytest.raises(SharedArtifactStoreError):
            store.delete("media/deadbeef")
    finally:
        parent.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)  # restore so tmp_path cleanup works


def test_shared_artifact_store_delete_of_absent_key_is_false_not_an_error(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    assert store.delete("media/never-published") is False


def test_shared_target_media_store_delete_removes_published_media(tmp_path):
    store = SharedArtifactStore(tmp_path / "store")
    media_store = SharedTargetMediaStore(store)
    media = _write(tmp_path, "movie.mp4", b"bytes")
    media_store.publish("deadbeef", media)

    assert media_store.delete("deadbeef") is True
    assert media_store.fetch_to_temp("deadbeef") is None
    assert media_store.delete("deadbeef") is False  # idempotent
