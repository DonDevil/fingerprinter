"""Phase 3: retry/backoff contract tests.

Uses small test-only retry_base_delay_s / retry_max_delay_s so due-time
waits stay in the low-hundreds-of-milliseconds range instead of arbitrary
sleeps.
"""
import time

import pytest

from work_queue.jobs import Job
from work_queue.keys import CONSUMER_GROUP, retry_zset_key, state_key, stream_key
from work_queue.producer import JobProducer
from work_queue.state import JobStatus
from worker.fingerprint_worker import PermanentFailure, TransientFailure, Worker

BLOCK_MS = 200
LEASE_MS = 30_000  # not under test here; keep well above test durations
BASE_DELAY_S = 0.1
MAX_DELAY_S = 0.3


def _worker(redis_client, consumer_name="w1", **overrides):
    kwargs = dict(
        block_ms=BLOCK_MS,
        lease_ms=LEASE_MS,
        retry_base_delay_s=BASE_DELAY_S,
        retry_max_delay_s=MAX_DELAY_S,
    )
    kwargs.update(overrides)
    return Worker(redis_client, consumer_name=consumer_name, **kwargs)


def _fails_transiently(job: Job) -> None:
    raise TransientFailure("synthetic transient failure")


def _fails_permanently(job: Job) -> None:
    raise PermanentFailure("synthetic permanent failure")


def test_transient_failure_schedules_retry(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(entry, _fails_transiently)

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.RETRY_SCHEDULED
    assert state["last_failure"] == "synthetic transient failure"
    assert float(state["next_attempt_at"]) > time.time()

    assert redis_client.zcard(retry_zset_key()) == 1
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0  # original entry was ACKed, not left dangling


def test_retry_not_promoted_before_due_time(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, _fails_transiently)

    promoted = worker.promote_due_retries()

    assert promoted == 0
    assert redis_client.zcard(retry_zset_key()) == 1


def test_retry_promoted_after_due_time_and_can_be_claimed_again(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    assert entry.attempt == 1
    worker.process_claim(entry, _fails_transiently)

    time.sleep(BASE_DELAY_S + 0.15)
    promoted = worker.promote_due_retries()

    assert promoted == 1
    assert redis_client.zcard(retry_zset_key()) == 0

    retry_entry = worker.claim_one()
    assert retry_entry is not None
    assert retry_entry.is_valid
    assert retry_entry.job.job_id == sample_job.job_id
    assert retry_entry.attempt == 2  # attempt incremented on reclaim-via-retry


def test_transient_failure_then_retry_then_success(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _fails_transiently)

    time.sleep(BASE_DELAY_S + 0.15)
    assert worker.promote_due_retries() == 1

    retry_entry = worker.claim_one()
    assert retry_entry.attempt == 2
    worker.process_claim(retry_entry, lambda job: None)  # succeeds this time

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0
    assert redis_client.zcard(retry_zset_key()) == 0


def test_permanent_failure_is_terminal_no_retry_scheduled(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(entry, _fails_permanently)

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.FAILED
    assert state["failure_reason"] == "synthetic permanent failure"
    assert redis_client.zcard(retry_zset_key()) == 0
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0


def test_max_attempts_prevents_further_retries(redis_client, make_job):
    job = make_job(job_id="job-max-attempts", max_attempts=2)
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    assert entry.attempt == 1
    worker.process_claim(entry, _fails_transiently)  # attempt 1 < max_attempts 2: retry

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.RETRY_SCHEDULED

    time.sleep(BASE_DELAY_S + 0.15)
    assert worker.promote_due_retries() == 1
    retry_entry = worker.claim_one()
    assert retry_entry.attempt == 2

    worker.process_claim(retry_entry, _fails_transiently)  # attempt 2 >= max_attempts 2: terminal

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.FAILED
    assert "max_attempts (2) exhausted" in state["failure_reason"]
    assert redis_client.zcard(retry_zset_key()) == 0


def _observed_delay(redis_client, job_id: str) -> float:
    state = redis_client.hgetall(state_key(job_id))
    return float(state["next_attempt_at"]) - float(state["scheduled_at"])


def test_backoff_increases_and_caps_at_max_delay(redis_client, make_job):
    # base=0.1, max=0.3 -> expected delays: 0.1, 0.2, 0.3 (capped from 0.4), 0.3 (capped)
    job = make_job(job_id="job-backoff", max_attempts=5)
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()  # attempt 1
    worker.process_claim(entry, _fails_transiently)
    assert _observed_delay(redis_client, job.job_id) == pytest.approx(BASE_DELAY_S, abs=0.01)

    time.sleep(BASE_DELAY_S + 0.1)
    worker.promote_due_retries()
    entry = worker.claim_one()  # attempt 2
    worker.process_claim(entry, _fails_transiently)
    assert _observed_delay(redis_client, job.job_id) == pytest.approx(BASE_DELAY_S * 2, abs=0.02)

    time.sleep(BASE_DELAY_S * 2 + 0.1)
    worker.promote_due_retries()
    entry = worker.claim_one()  # attempt 3
    worker.process_claim(entry, _fails_transiently)
    # base * 2^2 = 0.4, capped to MAX_DELAY_S = 0.3
    assert _observed_delay(redis_client, job.job_id) == pytest.approx(MAX_DELAY_S, abs=0.02)


def test_duplicate_promotion_does_not_create_duplicate_retry_work(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, _fails_transiently)

    time.sleep(BASE_DELAY_S + 0.15)
    first = worker.promote_due_retries()
    second = worker.promote_due_retries()  # simulates a duplicate/racing promotion pass

    assert first == 1
    assert second == 0  # already promoted; nothing left to promote

    length = redis_client.xlen(stream_key())
    assert length == 2  # original entry + exactly one promoted retry entry


def test_promoter_restart_does_not_lose_scheduled_retry(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = _worker(redis_client, consumer_name="worker-a")
    entry = worker_a.claim_one()
    worker_a.process_claim(entry, _fails_transiently)

    time.sleep(BASE_DELAY_S + 0.15)

    # A brand-new Worker instance stands in for a promoter process restart:
    # nothing about retry state lives in worker_a's memory.
    worker_b = _worker(redis_client, consumer_name="worker-b")
    promoted = worker_b.promote_due_retries()

    assert promoted == 1
    retry_entry = worker_b.claim_one()
    assert retry_entry is not None
    assert retry_entry.job.job_id == sample_job.job_id
    assert retry_entry.attempt == 2


def test_stale_worker_fencing_still_works_after_retry(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = _worker(redis_client, consumer_name="worker-a")
    worker_b = _worker(redis_client, consumer_name="worker-b")

    stale_entry = worker_a.claim_one()  # attempt 1
    worker_a.process_claim(stale_entry, _fails_transiently)  # attempt 1 -> retry scheduled

    time.sleep(BASE_DELAY_S + 0.15)
    assert worker_b.promote_due_retries() == 1
    retry_entry = worker_b.claim_one()  # attempt 2, owned by worker_b now
    assert retry_entry.attempt == 2

    worker_b.process_claim(retry_entry, lambda job: None)  # worker_b succeeds
    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED

    # worker_a — holding only its stale attempt=1 ClaimedEntry from before the
    # retry — must not be able to override worker_b's outcome, whichever way
    # it tries to finalize.
    stale_ack = worker_a.ack(stale_entry)
    assert stale_ack is False
    worker_a.process_claim(stale_entry, _fails_transiently)  # stale retry-schedule attempt too

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED
    assert state["attempt"] == "2"
    assert redis_client.zcard(retry_zset_key()) == 0
