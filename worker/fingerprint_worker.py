"""Fingerprint worker: consumer-group claim, state tracking, ack, shutdown.

This is the Redis-only side of the contract. It does not know about crawler
Python modules or crawler SQLite databases — Redis is the only coordination
surface it talks to.
"""
from __future__ import annotations

import os
import socket
import threading
from dataclasses import dataclass
from typing import Callable, Optional

from redis import Redis
from redis.exceptions import ResponseError

from work_queue.jobs import Job, JobValidationError
from work_queue.keys import CONSUMER_GROUP, DEFAULT_PRIORITY, stream_key
from work_queue.state import JobStateStore


@dataclass(frozen=True)
class ClaimedEntry:
    """One claimed stream entry: either a valid Job or a rejected malformed one."""

    entry_id: str
    job: Optional[Job]
    error: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        return self.job is not None


def default_consumer_name() -> str:
    return f"worker-{socket.gethostname()}-{os.getpid()}-{threading.get_ident()}"


class Worker:
    def __init__(
        self,
        redis_client: Redis,
        consumer_name: Optional[str] = None,
        priority: str = DEFAULT_PRIORITY,
        block_ms: int = 5000,
    ):
        self._redis = redis_client
        self._stream = stream_key(priority)
        self._consumer_name = consumer_name or default_consumer_name()
        self._block_ms = block_ms
        self._state = JobStateStore(redis_client)
        self._stop_event = threading.Event()
        self._ensure_group()

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    def _ensure_group(self) -> None:
        try:
            self._redis.xgroup_create(self._stream, CONSUMER_GROUP, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def claim_one(self) -> Optional[ClaimedEntry]:
        """Block for up to block_ms for one new job.

        Returns None on timeout (no job available in the window). A malformed
        entry is rejected and ACKed immediately rather than returned for
        retry — it can never become valid by redelivery.
        """
        response = self._redis.xreadgroup(
            CONSUMER_GROUP,
            self._consumer_name,
            {self._stream: ">"},
            count=1,
            block=self._block_ms,
        )
        if not response:
            return None

        _, entries = response[0]
        entry_id, raw_fields = entries[0]

        try:
            job = Job.from_stream_fields(raw_fields)
        except JobValidationError as exc:
            self._state.mark_rejected(raw_fields.get("job_id") or entry_id, str(exc))
            self._redis.xack(self._stream, CONSUMER_GROUP, entry_id)
            return ClaimedEntry(entry_id=entry_id, job=None, error=str(exc))

        attempt = self._next_attempt(job.job_id)
        self._state.mark_claimed(job.job_id, self._consumer_name, attempt)
        return ClaimedEntry(entry_id=entry_id, job=job)

    def _next_attempt(self, job_id: str) -> int:
        existing = self._state.get(job_id)
        previous = int(existing["attempt"]) if existing and "attempt" in existing else 0
        return previous + 1

    def ack(self, entry: ClaimedEntry) -> None:
        """Mark the job completed and XACK the stream entry."""
        if entry.job is not None:
            self._state.mark_completed(entry.job.job_id)
        self._redis.xack(self._stream, CONSUMER_GROUP, entry.entry_id)

    def stop(self) -> None:
        """Request the run loop to exit after its current blocking read."""
        self._stop_event.set()

    def run(self, handler: Callable[[Job], None]) -> None:
        """Claim, hand valid jobs to handler, ack, until stop() is called."""
        while not self._stop_event.is_set():
            entry = self.claim_one()
            if entry is None or not entry.is_valid:
                continue
            handler(entry.job)
            self.ack(entry)
