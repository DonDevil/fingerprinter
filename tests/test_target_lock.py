"""Phase 10 — target/lock.py's RedisLock: mutual exclusion, owner-scoped
release, TTL expiry. No embedding/target concerns here — this is the bare
Redis primitive `TargetRegistry.get_or_build_segment_embedding` builds on
(see tests/test_target_build_on_miss.py for that integration).
"""
import time

from target.lock import RedisLock


def test_acquire_succeeds_when_unheld(redis_client):
    lock = RedisLock(redis_client, "test:lock:a")
    assert lock.acquire(ttl_ms=5000) is True
    assert lock.owned is True


def test_second_acquire_fails_while_held(redis_client):
    lock_a = RedisLock(redis_client, "test:lock:b")
    lock_b = RedisLock(redis_client, "test:lock:b")

    assert lock_a.acquire(ttl_ms=5000) is True
    assert lock_b.acquire(ttl_ms=5000) is False
    assert lock_b.owned is False


def test_release_frees_the_lock_for_another_acquirer(redis_client):
    lock_a = RedisLock(redis_client, "test:lock:c")
    lock_b = RedisLock(redis_client, "test:lock:c")

    assert lock_a.acquire(ttl_ms=5000) is True
    assert lock_a.release() is True
    assert lock_b.acquire(ttl_ms=5000) is True


def test_release_without_acquire_is_a_safe_noop(redis_client):
    lock = RedisLock(redis_client, "test:lock:d")
    assert lock.release() is False


def test_release_is_idempotent(redis_client):
    lock = RedisLock(redis_client, "test:lock:idempotent")
    assert lock.acquire(ttl_ms=5000) is True
    assert lock.release() is True
    assert lock.release() is False  # already released, no token to release again


def test_release_does_not_remove_a_lock_it_no_longer_owns(redis_client):
    """Simulates the TTL-expiry race: lock_a's key expired and lock_b won
    it before lock_a called release(); lock_a's stale release() must not
    delete lock_b's still-live lock (the whole reason release() is a
    compare-and-delete, not a plain DEL)."""
    lock_a = RedisLock(redis_client, "test:lock:e")
    assert lock_a.acquire(ttl_ms=5000) is True

    redis_client.delete("test:lock:e")  # simulate TTL expiry without waiting one out
    lock_b = RedisLock(redis_client, "test:lock:e")
    assert lock_b.acquire(ttl_ms=5000) is True

    assert lock_a.release() is False
    assert redis_client.get("test:lock:e") is not None


def test_lock_expires_after_ttl(redis_client):
    lock_a = RedisLock(redis_client, "test:lock:f")
    assert lock_a.acquire(ttl_ms=200) is True

    time.sleep(0.35)

    lock_b = RedisLock(redis_client, "test:lock:f")
    assert lock_b.acquire(ttl_ms=5000) is True
