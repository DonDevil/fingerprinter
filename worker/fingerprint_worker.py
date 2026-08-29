"""Fingerprint worker: consumer-group claim, state tracking, ack, shutdown.

This is the Redis-only side of the contract. It does not know about crawler
Python modules or crawler SQLite databases — Redis is the only coordination
surface it talks to.
"""
from __future__ import annotations

import json
import os
import socket
import threading
import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from redis import Redis
from redis.exceptions import ResponseError

from work_queue.jobs import Job, JobValidationError
from work_queue.keys import (
    CONSUMER_GROUP,
    DEFAULT_PRIORITY,
    match_index_key,
    result_key,
    results_stream_key,
    retry_zset_key,
    state_key,
    stream_key,
)
from work_queue.results import Result, ResultRecord
from work_queue.state import JobStateStore


class TransientFailure(Exception):
    """Handler raises this for a retryable failure (schedules a retry).

    `error_type` (Phase 13C) is an optional, safe-to-log classification of
    the underlying cause — conventionally the caught exception's class name
    (e.g. "ConnectionTimeoutError") — kept separate from `message`, which
    may embed a media URL (see `acquisition/acquirer.py`'s exception text)
    and must never reach structured logs/metrics. Defaults to
    "unclassified_error" when a caller doesn't supply one, so every failure
    is still classified as *something* rather than silently dropped from
    aggregate error-category counts.
    """

    def __init__(self, message: str, error_type: Optional[str] = None):
        super().__init__(message)
        self.error_type = error_type or "unclassified_error"


class PermanentFailure(Exception):
    """Handler raises this for a non-retryable failure (terminal, no retry).

    Any *other* exception a handler raises is not caught here at all — it
    propagates and the entry is simply never acked, which makes an
    unclassified failure indistinguishable from a worker crash. Phase 2's
    XAUTOCLAIM-based recovery already handles that case, so there is no
    separate "unknown failure" path to build.

    See `TransientFailure.error_type` above — same convention here.
    """

    def __init__(self, message: str, error_type: Optional[str] = None):
        super().__init__(message)
        self.error_type = error_type or "unclassified_error"


class WorkerObserver:
    """No-op observability hook (Phase 13C). `Worker` calls these methods at
    existing claim/reclaim/finalize boundaries — this base class does
    nothing, so a `Worker` constructed without an `observer` behaves
    identically to every pre-Phase-13C test and call site. The concrete,
    logging/counting implementation lives in `worker/observability.py`
    (imported nowhere in this module, to keep this file's only dependency
    direction outward — observability code depends on this module's types,
    never the reverse).

    Every method here mirrors one lifecycle boundary `Worker` already has;
    no new state or control flow is introduced by adding these calls.
    """

    def on_job_claimed(self, *, job: Job, attempt: int) -> None:
        pass

    def on_job_reclaimed(self, *, job: Job, attempt: int) -> None:
        pass

    def on_job_rejected(self, *, entry_id: str, error: str, source: str) -> None:
        pass

    def on_job_completed(
        self, *, job: Job, attempt: int, latency_ms: Optional[float], decision: Optional[str] = None
    ) -> None:
        pass

    def on_job_failed(self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]) -> None:
        pass

    def on_job_retry_scheduled(
        self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]
    ) -> None:
        pass

    def on_job_permanently_failed(
        self, *, job: Job, attempt: int, error_type: str, latency_ms: Optional[float]
    ) -> None:
        pass

    def on_loop_tick(self) -> None:
        pass


# Atomically finalizes a claim: completes the state hash and XACKs the entry
# only if `attempt` still matches what's on record. A worker that lost
# ownership via XAUTOCLAIM (which bumps attempt on reclaim) has a stale
# attempt and this is a no-op for it — critically, it also skips the XACK,
# so a stale worker can never remove the new owner's still-in-flight PEL
# entry out from under it.
_COMPLETE_IF_CURRENT = """
local current = redis.call('HGET', KEYS[1], 'attempt')
if current == false or current ~= ARGV[3] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'completed', 'completed_at', ARGV[4])
redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
return 1
"""

# Same CAS-then-finalize shape as _COMPLETE_IF_CURRENT, but the terminal
# state is a scheduled retry instead of a completion: state hash write,
# ZADD into the retry ZSET, and XACK of the original entry all happen in
# one atomic script so a crash can never land between "job left the
# stream/PEL" and "job is in the retry ZSET".
#
# KEYS: [1] state_key  [2] stream_key  [3] retry_zset_key
# ARGV: [1] group  [2] entry_id  [3] expected_attempt  [4] now
#       [5] due_at (zset score)  [6] retry_payload (json)  [7] reason
_SCHEDULE_RETRY_IF_CURRENT = """
local current = redis.call('HGET', KEYS[1], 'attempt')
if current == false or current ~= ARGV[3] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'retry_scheduled', 'last_failure', ARGV[7],
    'scheduled_at', ARGV[4], 'next_attempt_at', ARGV[5])
redis.call('ZADD', KEYS[3], ARGV[5], ARGV[6])
redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
return 1
"""

# Same CAS shape again, for terminal (permanent, or max_attempts exhausted)
# failure: state hash write + XACK, gated on the same fencing check.
#
# KEYS: [1] state_key  [2] stream_key
# ARGV: [1] group  [2] entry_id  [3] expected_attempt  [4] failed_at  [5] reason
_FAIL_IF_CURRENT = """
local current = redis.call('HGET', KEYS[1], 'attempt')
if current == false or current ~= ARGV[3] then
    return 0
end
redis.call('HSET', KEYS[1], 'status', 'failed', 'failure_reason', ARGV[5], 'failed_at', ARGV[4])
redis.call('XACK', KEYS[2], ARGV[1], ARGV[2])
return 1
"""

# Atomically commits a handler's Result as the job's durable, terminal
# outcome. Same CAS gate as _COMPLETE_IF_CURRENT, but does three writes
# instead of one, all inside one Redis-atomic script execution: the result
# hash, the state hash (status=completed), and an XADD onto the results
# stream, followed by the XACK. Gating all of it — including the XADD — on
# the same attempt check as the XACK closes the same hole Phase 2 closed
# for plain completion: a stale worker can't write a result, can't emit a
# stream event for one, and can't remove the current owner's PEL entry.
#
# A MATCH decision additionally gets one more write in the same atomic
# script: a ZADD into the per-target match index (`work_queue.keys.
# match_index_key`), member=job_id, score=processing_completed_at (ARGV[4],
# already computed for the state/result writes above -- no new argument).
# Gated on the same CAS as everything else here, so the index can never
# desync from the result it indexes: either both are written, or neither is.
#
# KEYS: [1] state_key  [2] result_key  [3] stream_key  [4] results_stream_key
#       [5] match_index_key
# ARGV: [1] group  [2] entry_id  [3] expected_attempt  [4] completed_at
#       [5] result_fields (json map)  [6] event_fields (json map)
_COMMIT_RESULT_IF_CURRENT = """
local current = redis.call('HGET', KEYS[1], 'attempt')
if current == false or current ~= ARGV[3] then
    return false
end
redis.call('HSET', KEYS[1], 'status', 'completed', 'completed_at', ARGV[4])

local result_fields = cjson.decode(ARGV[5])
local hset_args = {'HSET', KEYS[2]}
for k, v in pairs(result_fields) do
    table.insert(hset_args, k)
    table.insert(hset_args, v)
end
redis.call(unpack(hset_args))

local event_fields = cjson.decode(ARGV[6])
local xadd_args = {'XADD', KEYS[4], '*'}
for k, v in pairs(event_fields) do
    table.insert(xadd_args, k)
    table.insert(xadd_args, v)
end
local event_id = redis.call(unpack(xadd_args))

if result_fields.decision == 'match' then
    redis.call('ZADD', KEYS[5], ARGV[4], result_fields.job_id)
end

redis.call('XACK', KEYS[3], ARGV[1], ARGV[2])
return event_id
"""

# Promotes one due retry back onto the stream. ZREM-then-XADD in a single
# script: the ZREM is the atomic "claim" of this retry (a racing/duplicate
# promotion attempt sees removed==0 and does nothing), and because both
# commands run inside one Redis-atomic script execution, there is no gap
# between "left the retry ZSET" and "back on the stream" for a promoter
# crash to land in.
#
# KEYS: [1] retry_zset_key  [2] stream_key
# ARGV: [1] member (json-encoded job stream fields)
_PROMOTE_RETRY = """
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then
    return false
end
local fields = cjson.decode(ARGV[1])
local args = {'XADD', KEYS[2], '*'}
for k, v in pairs(fields) do
    table.insert(args, k)
    table.insert(args, v)
end
return redis.call(unpack(args))
"""


@dataclass(frozen=True)
class ClaimedEntry:
    """One claimed stream entry: either a valid Job or a rejected malformed one."""

    entry_id: str
    job: Optional[Job]
    error: Optional[str] = None
    attempt: Optional[int] = None
    # Phase 13C: wall-clock-independent claim timestamp (time.monotonic()),
    # used only to compute claim-to-finalize latency for observability.
    # Optional/defaulting to None keeps every pre-existing direct
    # ClaimedEntry(...) construction (tests, this module) unaffected.
    claimed_at: Optional[float] = None

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
        lease_ms: int = 30_000,
        reclaim_interval_ms: Optional[int] = None,
        reclaim_batch_size: int = 10,
        retry_base_delay_s: float = 1.0,
        retry_max_delay_s: float = 60.0,
        retry_batch_size: int = 50,
        observer: Optional[WorkerObserver] = None,
    ):
        self._redis = redis_client
        self._stream = stream_key(priority)
        self._retry_zset = retry_zset_key(priority)
        self._results_stream = results_stream_key(priority)
        self._consumer_name = consumer_name or default_consumer_name()
        self._block_ms = block_ms
        self._lease_ms = lease_ms
        self._reclaim_interval_s = (
            reclaim_interval_ms if reclaim_interval_ms is not None else lease_ms
        ) / 1000
        self._reclaim_batch_size = reclaim_batch_size
        self._retry_base_delay_s = retry_base_delay_s
        self._retry_max_delay_s = retry_max_delay_s
        self._retry_batch_size = retry_batch_size
        self._state = JobStateStore(redis_client)
        self._observer = observer if observer is not None else WorkerObserver()
        self._stop_event = threading.Event()
        self._last_reclaim_at = 0.0
        self._complete_script = redis_client.register_script(_COMPLETE_IF_CURRENT)
        self._schedule_retry_script = redis_client.register_script(_SCHEDULE_RETRY_IF_CURRENT)
        self._fail_script = redis_client.register_script(_FAIL_IF_CURRENT)
        self._promote_script = redis_client.register_script(_PROMOTE_RETRY)
        self._commit_result_script = redis_client.register_script(_COMMIT_RESULT_IF_CURRENT)
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
            self._observer.on_job_rejected(entry_id=entry_id, error=str(exc), source="claim")
            return ClaimedEntry(entry_id=entry_id, job=None, error=str(exc))

        attempt = self._next_attempt(job.job_id)
        self._state.mark_claimed(job.job_id, self._consumer_name, attempt)
        self._observer.on_job_claimed(job=job, attempt=attempt)
        return ClaimedEntry(entry_id=entry_id, job=job, attempt=attempt, claimed_at=time.monotonic())

    def _next_attempt(self, job_id: str) -> int:
        existing = self._state.get(job_id)
        previous = int(existing["attempt"]) if existing and "attempt" in existing else 0
        return previous + 1

    def reclaim_stale(self) -> List[ClaimedEntry]:
        """XAUTOCLAIM entries idle longer than lease_ms onto this consumer.

        Mirrors claim_one()'s malformed-vs-valid handling: malformed entries
        are rejected and ACKed on the spot, valid ones get a bumped attempt
        (this is a reprocessing, not a first attempt) and are returned for
        the normal handler/ack path.
        """
        _, claimed, _ = self._redis.xautoclaim(
            self._stream,
            CONSUMER_GROUP,
            self._consumer_name,
            min_idle_time=self._lease_ms,
            start_id="0-0",
            count=self._reclaim_batch_size,
        )

        results: List[ClaimedEntry] = []
        for entry_id, raw_fields in claimed:
            if not raw_fields:
                # Entry was trimmed/deleted from the stream but still lingered
                # in the PEL; XAUTOCLAIM already dropped it there, nothing to do.
                continue
            try:
                job = Job.from_stream_fields(raw_fields)
            except JobValidationError as exc:
                self._state.mark_rejected(raw_fields.get("job_id") or entry_id, str(exc))
                self._redis.xack(self._stream, CONSUMER_GROUP, entry_id)
                self._observer.on_job_rejected(entry_id=entry_id, error=str(exc), source="reclaim")
                results.append(ClaimedEntry(entry_id=entry_id, job=None, error=str(exc)))
                continue

            attempt = self._next_attempt(job.job_id)
            self._state.mark_claimed(job.job_id, self._consumer_name, attempt)
            self._observer.on_job_reclaimed(job=job, attempt=attempt)
            results.append(ClaimedEntry(entry_id=entry_id, job=job, attempt=attempt, claimed_at=time.monotonic()))
        return results

    def ack(self, entry: ClaimedEntry) -> bool:
        """Mark the job completed and XACK the stream entry.

        Returns False without acking or overwriting state if `entry.attempt`
        is no longer the current attempt on record — that means ownership
        moved on via XAUTOCLAIM and this call is a stale worker's finalize
        attempt, which must not clobber the new owner's outcome.
        """
        if entry.job is None:
            self._redis.xack(self._stream, CONSUMER_GROUP, entry.entry_id)
            return True

        completed = self._complete_script(
            keys=[state_key(entry.job.job_id), self._stream],
            args=[CONSUMER_GROUP, entry.entry_id, str(entry.attempt), str(time.time())],
        )
        return bool(completed)

    def commit_result(self, entry: ClaimedEntry, result: Result) -> Optional[str]:
        """Atomically commit a handler's Result as the job's durable,
        terminal outcome: write the result hash, mark job state completed,
        XADD a result-stream event, ZADD into the per-target match index iff
        the decision is MATCH, and XACK the original entry — all in one
        Redis-atomic script, gated by the same attempt fencing as ack().

        Returns the result-stream event id, or None if `entry.attempt` is
        no longer current (stale worker) — in which case nothing was
        written at all: no result, no state change, no XACK.
        """
        job = entry.job
        record = ResultRecord(
            job_id=job.job_id,
            media_evidence_id=job.media_evidence_id,
            target_id=job.target_id,
            target_version=job.target_version,
            attempt=entry.attempt,
            worker_id=self._consumer_name,
            result=result,
        )
        event_id = self._commit_result_script(
            keys=[
                state_key(job.job_id),
                result_key(job.job_id),
                self._stream,
                self._results_stream,
                match_index_key(job.target_id, job.target_version),
            ],
            args=[
                CONSUMER_GROUP,
                entry.entry_id,
                str(entry.attempt),
                str(result.processing_completed_at),
                json.dumps(record.to_hash_fields()),
                json.dumps(record.to_event_fields()),
            ],
        )
        return event_id

    def _backoff_seconds(self, attempt: int) -> float:
        """Exponential backoff for the attempt that just failed: base * 2^(attempt-1), capped at max."""
        delay = self._retry_base_delay_s * (2 ** (attempt - 1))
        return min(delay, self._retry_max_delay_s)

    def _elapsed_ms(self, entry: ClaimedEntry) -> Optional[float]:
        """Claim-to-now latency in ms, or None if `entry` predates Phase
        13C's `claimed_at` field (e.g. a test-constructed ClaimedEntry)."""
        if entry.claimed_at is None:
            return None
        return (time.monotonic() - entry.claimed_at) * 1000

    def _handle_transient_failure(
        self, entry: ClaimedEntry, reason: str, error_type: str = "unclassified_error"
    ) -> bool:
        """Schedule a retry, or finalize as failed if max_attempts is exhausted.

        max_attempts is the maximum number of total processing attempts
        (the first claim counts as attempt 1, same as Phase 1/2's existing
        counter) — so a transient failure on attempt N schedules a retry
        only while N < max_attempts.
        """
        job = entry.job
        if entry.attempt >= job.max_attempts:
            failed = self._fail(entry, f"max_attempts ({job.max_attempts}) exhausted: {reason}")
            if failed:
                self._observer.on_job_permanently_failed(
                    job=job, attempt=entry.attempt, error_type=error_type, latency_ms=self._elapsed_ms(entry)
                )
            return failed

        now = time.time()
        due_at = now + self._backoff_seconds(entry.attempt)
        payload = json.dumps(job.to_stream_fields())
        scheduled = self._schedule_retry_script(
            keys=[state_key(job.job_id), self._stream, self._retry_zset],
            args=[CONSUMER_GROUP, entry.entry_id, str(entry.attempt), str(now), str(due_at), payload, reason],
        )
        if scheduled:
            self._observer.on_job_retry_scheduled(
                job=job, attempt=entry.attempt, error_type=error_type, latency_ms=self._elapsed_ms(entry)
            )
        return bool(scheduled)

    def _fail(self, entry: ClaimedEntry, reason: str) -> bool:
        failed = self._fail_script(
            keys=[state_key(entry.job.job_id), self._stream],
            args=[CONSUMER_GROUP, entry.entry_id, str(entry.attempt), str(time.time()), reason],
        )
        return bool(failed)

    def promote_due_retries(self, now: Optional[float] = None, count: Optional[int] = None) -> int:
        """Move retry-ZSET entries whose due time has passed back onto the stream.

        Safe to call from any worker at any time — retry state lives
        entirely in Redis, so a promoter process restarting mid-work loses
        nothing (the next call, from this or any other worker, just picks
        up what's still due). Promotion is idempotent per member: see
        _PROMOTE_RETRY.
        """
        now = time.time() if now is None else now
        limit = self._retry_batch_size if count is None else count
        due = self._redis.zrangebyscore(self._retry_zset, min="-inf", max=now, start=0, num=limit)

        promoted = 0
        for member in due:
            new_id = self._promote_script(keys=[self._retry_zset, self._stream], args=[member])
            if new_id:
                promoted += 1
        return promoted

    def process_claim(
        self, entry: ClaimedEntry, handler: Callable[[Job], Optional[Result]]
    ) -> None:
        """Run handler on a claimed entry and finalize it: commit-result /
        ack / retry / fail.

        This is the single dispatch point for "what happens after a
        handler runs" — used by run() for freshly claimed entries and by
        the stale-reclaim path alike, so a reclaimed job goes through
        exactly the same handling as a fresh claim. A handler that returns
        a Result gets it committed atomically with completion
        (commit_result); a handler that returns nothing just gets a plain
        ack(), unchanged from Phase 1-3.
        """
        try:
            outcome = handler(entry.job)
        except TransientFailure as exc:
            self._handle_transient_failure(entry, str(exc), error_type=exc.error_type)
        except PermanentFailure as exc:
            failed = self._fail(entry, str(exc))
            if failed:
                self._observer.on_job_failed(
                    job=entry.job, attempt=entry.attempt, error_type=exc.error_type,
                    latency_ms=self._elapsed_ms(entry),
                )
        else:
            if isinstance(outcome, Result):
                event_id = self.commit_result(entry, outcome)
                if event_id:
                    self._observer.on_job_completed(
                        job=entry.job, attempt=entry.attempt, latency_ms=self._elapsed_ms(entry),
                        decision=outcome.decision,
                    )
            else:
                completed = self.ack(entry)
                if completed:
                    self._observer.on_job_completed(
                        job=entry.job, attempt=entry.attempt, latency_ms=self._elapsed_ms(entry), decision=None,
                    )

    def stop(self) -> None:
        """Request the run loop to exit after its current blocking read."""
        self._stop_event.set()

    def _maybe_reclaim_stale(self, handler: Callable[[Job], None]) -> None:
        now = time.monotonic()
        if now - self._last_reclaim_at < self._reclaim_interval_s:
            return
        self._last_reclaim_at = now
        for entry in self.reclaim_stale():
            if not entry.is_valid:
                continue
            self.process_claim(entry, handler)

    def run(self, handler: Callable[[Job], None]) -> None:
        """Claim, hand valid jobs to handler, ack/retry/fail, until stop().

        Per the architecture proposal (§3), a worker runs XAUTOCLAIM for
        stale entries and promotes due retries before blocking on new work.
        """
        while not self._stop_event.is_set():
            self._observer.on_loop_tick()
            self._maybe_reclaim_stale(handler)
            if self._stop_event.is_set():
                break
            self.promote_due_retries()
            if self._stop_event.is_set():
                break
            entry = self.claim_one()
            if entry is None or not entry.is_valid:
                continue
            self.process_claim(entry, handler)
