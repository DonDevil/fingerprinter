"""Job producer: crawler-side library that XADDs jobs onto the stream.

Knows nothing about workers, claims, or state — creation is fire-and-forget.
"""
from __future__ import annotations

from redis import Redis

from work_queue.jobs import Job
from work_queue.keys import DEFAULT_PRIORITY, stream_key


class JobProducer:
    def __init__(self, redis_client: Redis, priority: str = DEFAULT_PRIORITY):
        self._redis = redis_client
        self._stream = stream_key(priority)

    def enqueue(self, job: Job) -> str:
        """XADD the job onto the stream. Returns the stream entry id."""
        return self._redis.xadd(self._stream, job.to_stream_fields())
