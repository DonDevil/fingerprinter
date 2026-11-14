"""Redis coordination (design §4.3): one time source, leases, the rebuildable queue.

Everything here is **rebuildable** and never authoritative: the queue is rebuilt
from ``encode_due``; a lost lease only causes duplicate work, which the Scylla
LWTs make harmless. Redis ``TIME`` is the clock for every lease, including the
ones stored in Scylla (B.4 #3).
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import redis

from fingerprinter.config import RedisConfig
from fingerprinter.errors import StorageUnavailableError


def utc(value: datetime | None) -> datetime | None:
    """Scylla's driver returns naive UTC datetimes; make them aware."""
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def ms(value: datetime) -> datetime:
    """Truncate to the millisecond (Scylla timestamp precision) so equality conditions hold."""
    return value.replace(microsecond=value.microsecond // 1000 * 1000)


def uuid7() -> str:
    millis = time.time_ns() // 1_000_000
    value = (millis & ((1 << 48) - 1)) << 80 | int.from_bytes(uuid.uuid4().bytes[6:], "big")
    value &= ~(0xF << 76)
    value |= 7 << 76
    value &= ~(0x3 << 62)
    value |= 0x2 << 62
    return str(uuid.UUID(int=value))


class Clock:
    def now(self) -> datetime:
        raise NotImplementedError


class RedisClock(Clock):
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    def now(self) -> datetime:
        try:
            sec, usec = self._client.time()
        except redis.RedisError as exc:
            raise StorageUnavailableError(f"redis TIME: {exc}") from exc
        return ms(datetime.fromtimestamp(int(sec) + int(usec) / 1e6, tz=UTC))


class ShiftedClock(Clock):
    """Test clock injection: another clock plus a movable offset (TE-1, TD-1/2)."""

    def __init__(self, base: Clock) -> None:
        self.base = base
        self.offset = timedelta(0)

    def now(self) -> datetime:
        return ms(self.base.now() + self.offset)

    def advance(self, **kwargs: float) -> None:
        self.offset += timedelta(**kwargs)


_RENEW = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""
_POP_AND_LEASE = """
local items = redis.call('ZPOPMIN', KEYS[1], 1)
if #items == 0 then return nil end
local member = items[1]
local lease = ARGV[1] .. member
if redis.call('SET', lease, ARGV[2], 'NX', 'PX', ARGV[3]) then return member end
return {member, 'leased'}
"""


class Coord:
    def __init__(self, client: redis.Redis, cfg: RedisConfig) -> None:
        self.client = client
        self.cfg = cfg
        self.prefix = cfg.prefix
        self._renew = client.register_script(_RENEW)
        self._release = client.register_script(_RELEASE)
        self._pop = client.register_script(_POP_AND_LEASE)
        self.clock: Clock = RedisClock(client)

    @classmethod
    def connect(cls, cfg: RedisConfig) -> Coord:
        client = redis.Redis(
            host=cfg.host,
            port=cfg.port,
            db=cfg.db,
            password=cfg.password,
            socket_timeout=cfg.socket_timeout_s,
            socket_connect_timeout=cfg.socket_timeout_s,
        )
        return cls(client, cfg)

    def key(self, *parts: str) -> str:
        return self.prefix + ":".join(parts)

    def stream(self, name: str) -> str:
        return self.cfg.stream_prefix + name

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except redis.RedisError as exc:
            raise StorageUnavailableError(f"redis: {exc}") from exc

    # --- leases (avoidance only; correctness is in Scylla) -----------------

    def acquire(self, key: str, token: str, ttl_ms: int) -> bool:
        return bool(self.call(self.client.set, key, token, nx=True, px=int(ttl_ms)))

    def renew(self, key: str, token: str, ttl_ms: int) -> bool:
        return bool(self.call(self._renew, keys=[key], args=[token, int(ttl_ms)]))

    def release(self, key: str, token: str) -> bool:
        return bool(self.call(self._release, keys=[key], args=[token]))

    def holder(self, key: str) -> str | None:
        value = self.call(self.client.get, key)
        return value.decode() if isinstance(value, bytes) else value

    # --- work queue ---------------------------------------------------------

    @property
    def queue_key(self) -> str:
        return self.key("q", "encode")

    def enqueue(self, member: str, score: float) -> None:
        self.call(self.client.zadd, self.queue_key, {member: score})

    def queue_depth(self) -> int:
        return int(self.call(self.client.zcard, self.queue_key))

    def pop_and_lease(self, token: str, ttl_ms: int) -> tuple[str, bool] | None:
        """ZPOPMIN + SET work lease (one script). ``(member, leased_by_me)``."""
        reply = self.call(
            self._pop, keys=[self.queue_key], args=[self.key("lease", "req", ""), token, ttl_ms]
        )
        if reply is None:
            return None
        if isinstance(reply, list):
            return reply[0].decode(), False
        return reply.decode(), True

    def work_lease_key(self, member: str) -> str:
        return self.key("lease", "req", member)
