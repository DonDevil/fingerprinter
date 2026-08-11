"""Job state tracking: fingerprint:job:{job_id}:state hash.

Per the architecture proposal, state is written on claim (not on creation —
the stream entry alone is the job until a worker touches it).
"""
from __future__ import annotations

import time
from typing import Optional

from redis import Redis

from work_queue.keys import state_key


class JobStatus:
    CLAIMED = "claimed"
    COMPLETED = "completed"
    REJECTED = "rejected"


class JobStateStore:
    def __init__(self, redis_client: Redis):
        self._redis = redis_client

    def mark_claimed(self, job_id: str, worker_id: str, attempt: int) -> None:
        self._redis.hset(
            state_key(job_id),
            mapping={
                "status": JobStatus.CLAIMED,
                "worker_id": worker_id,
                "attempt": str(attempt),
                "claimed_at": str(time.time()),
            },
        )

    def mark_completed(self, job_id: str) -> None:
        self._redis.hset(
            state_key(job_id),
            mapping={"status": JobStatus.COMPLETED, "completed_at": str(time.time())},
        )

    def mark_rejected(self, job_id: str, reason: str) -> None:
        self._redis.hset(
            state_key(job_id),
            mapping={"status": JobStatus.REJECTED, "reason": reason},
        )

    def get(self, job_id: str) -> Optional[dict]:
        data = self._redis.hgetall(state_key(job_id))
        return data or None
