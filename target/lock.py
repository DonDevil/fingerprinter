"""Redis `SET NX PX` distributed lock — the build-on-miss race guard from
`docs/design/design-proposal-1.md` §8 ("Build-on-miss race"): multiple
workers, possibly on different machines, can miss the same target
embedding cache key at the same time. Only one should pay for the build;
the rest should wait for its result rather than duplicating the work.

Deliberately minimal — not a general-purpose distributed-lock library, and
not Redlock (no multi-instance quorum, no auto-renewal/watchdog). Just the
single "one builder, N waiters" primitive `target/registry.py`'s
build-on-miss path needs: `acquire` (SET NX PX with a unique owner token)
and `release` (Lua compare-and-delete), the same CAS spirit as
`worker/fingerprint_worker.py`'s Lua scripts — a caller can only release
the lock it still owns, never one that expired and was re-acquired by
someone else.

Known limitation (see phase-10 doc, "Limitations"): the lock does not
auto-extend. A build that runs longer than `ttl_ms` loses the lock out
from under it, and a second worker can then acquire it and start a
redundant build. Callers must pick `ttl_ms` generously relative to
expected build time; this module does not solve that, only documents it.
"""
from __future__ import annotations

import uuid
from typing import Optional

from redis import Redis

_RELEASE_IF_OWNER = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


class RedisLock:
    """One acquire/release pair against a single Redis key. Not reentrant."""

    def __init__(self, redis_client: Redis, key: str):
        self._redis = redis_client
        self._key = key
        self._token: Optional[str] = None
        self._release_script = redis_client.register_script(_RELEASE_IF_OWNER)

    @property
    def key(self) -> str:
        return self._key

    @property
    def owned(self) -> bool:
        return self._token is not None

    def acquire(self, ttl_ms: int) -> bool:
        """Try to acquire the lock. Returns True iff this call won it."""
        token = uuid.uuid4().hex
        acquired = self._redis.set(self._key, token, nx=True, px=ttl_ms)
        if acquired:
            self._token = token
            return True
        return False

    def release(self) -> bool:
        """Release the lock iff this instance still owns it (token
        matches). Safe to call even if `acquire()` never succeeded, or was
        already released — both are no-ops."""
        if self._token is None:
            return False
        released = bool(self._release_script(keys=[self._key], args=[self._token]))
        self._token = None
        return released
