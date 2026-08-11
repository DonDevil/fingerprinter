"""Phase 4: result contract tests.

Synthetic results only, against the same local Redis db 15 as Phase 1-3.
"""
import time

import pytest

from work_queue.jobs import Job
from work_queue.keys import CONSUMER_GROUP, result_key, results_stream_key, state_key, stream_key
from work_queue.producer import JobProducer
from work_queue.results import Result, ResultDecision, ResultStore
from work_queue.state import JobStatus
from worker.fingerprint_worker import TransientFailure, Worker

BLOCK_MS = 200
LEASE_MS = 150  # short so crash-recovery tests don't need long sleeps


def _worker(redis_client, consumer_name="w1", **overrides):
    kwargs = dict(block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    kwargs.update(overrides)
    return Worker(redis_client, consumer_name=consumer_name, **kwargs)


def _synthetic_result(decision: str, **overrides) -> Result:
    now = time.time()
    defaults = dict(
        decision=decision,
        algorithm="synthetic-v0",
        processing_started_at=now - 0.01,
        processing_completed_at=now,
        confidence=0.91 if decision != ResultDecision.PROCESSING_FAILURE else None,
        summary=None,
    )
    defaults.update(overrides)
    return Result(**defaults)


def test_successful_result_is_persisted(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(entry, lambda job: _synthetic_result(ResultDecision.MATCH))

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record is not None
    assert record["job_id"] == sample_job.job_id
    assert record["media_evidence_id"] == sample_job.media_evidence_id
    assert record["target_id"] == sample_job.target_id
    assert record["target_version"] == sample_job.target_version
    assert record["attempt"] == "1"
    assert record["worker_id"] == worker.consumer_name
    assert record["result_version"] == "1"
    assert record["algorithm"] == "synthetic-v0"
    assert float(record["processing_duration"]) >= 0

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED


def test_result_stream_event_is_emitted(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(entry, lambda job: _synthetic_result(ResultDecision.MATCH))

    events = redis_client.xrange(results_stream_key())
    assert len(events) == 1
    _, fields = events[0]
    assert fields["job_id"] == sample_job.job_id
    assert fields["attempt"] == "1"
    assert fields["decision"] == ResultDecision.MATCH
    assert fields["result_version"] == "1"


def test_match_result_is_represented_correctly(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(
        entry, lambda job: _synthetic_result(ResultDecision.MATCH, confidence=0.97)
    )

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["decision"] == ResultDecision.MATCH
    assert record["confidence"] == "0.97"


def test_no_match_result_is_represented_correctly(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(
        entry, lambda job: _synthetic_result(ResultDecision.NO_MATCH, confidence=0.03)
    )

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["decision"] == ResultDecision.NO_MATCH
    assert record["confidence"] == "0.03"


def test_processing_failure_result_is_represented_correctly(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(
        entry,
        lambda job: _synthetic_result(
            ResultDecision.PROCESSING_FAILURE, summary="synthetic decode error"
        ),
    )

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["decision"] == ResultDecision.PROCESSING_FAILURE
    assert "confidence" not in record  # no confidence to report
    assert record["summary"] == "synthetic decode error"
    # the job still reached a terminal, completed state -- processing_failure
    # is a result, not a retryable/permanent worker failure.
    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED


def test_result_contains_attempt_information(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)

    # fail transiently once so the successful attempt is attempt 2
    entry = worker.claim_one()
    assert entry.attempt == 1
    worker.process_claim(entry, lambda job: (_ for _ in ()).throw(TransientFailure("retry me")))

    time.sleep(worker._retry_base_delay_s + 0.15)
    assert worker.promote_due_retries() == 1
    retry_entry = worker.claim_one()
    assert retry_entry.attempt == 2

    worker.process_claim(retry_entry, lambda job: _synthetic_result(ResultDecision.MATCH))

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["attempt"] == "2"

    events = redis_client.xrange(results_stream_key())
    assert events[-1][1]["attempt"] == "2"


def test_stale_attempt_cannot_overwrite_newer_result(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = _worker(redis_client, consumer_name="worker-a")
    worker_b = _worker(redis_client, consumer_name="worker-b")

    stale_entry = worker_a.claim_one()  # attempt 1, worker-a "crashes" after this
    assert stale_entry.attempt == 1

    time.sleep((LEASE_MS / 1000) + 0.1)
    reclaimed = worker_b.reclaim_stale()
    assert len(reclaimed) == 1
    new_entry = reclaimed[0]
    assert new_entry.attempt == 2

    committed = worker_b.commit_result(new_entry, _synthetic_result(ResultDecision.MATCH))
    assert committed is not None

    # worker-a's stale attempt=1 commit must be rejected outright: no write.
    stale_result = worker_a.commit_result(stale_entry, _synthetic_result(ResultDecision.NO_MATCH))
    assert stale_result is None

    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["attempt"] == "2"
    assert record["decision"] == ResultDecision.MATCH  # worker-b's result stands
    assert record["worker_id"] == "worker-b"


def test_stale_attempt_cannot_ack_or_remove_newer_pel_entry(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = _worker(redis_client, consumer_name="worker-a")
    worker_b = _worker(redis_client, consumer_name="worker-b")

    stale_entry = worker_a.claim_one()  # attempt 1, worker-a "crashes" after this

    time.sleep((LEASE_MS / 1000) + 0.1)
    new_entry = worker_b.reclaim_stale()[0]  # attempt 2, PEL now owned by worker-b

    # worker-b has NOT finalized yet -- entry is still pending, owned by worker-b.
    pending_before = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending_before["pending"] == 1

    # worker-a's stale finalize attempt must not touch worker-b's PEL entry.
    stale_result = worker_a.commit_result(stale_entry, _synthetic_result(ResultDecision.MATCH))
    assert stale_result is None

    pending_after = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending_after["pending"] == 1  # still pending, untouched by worker-a

    detail = redis_client.xpending_range(stream_key(), CONSUMER_GROUP, "-", "+", 10)
    assert detail[0]["consumer"] == "worker-b"

    # worker-b can still finalize normally afterwards.
    committed = worker_b.commit_result(new_entry, _synthetic_result(ResultDecision.MATCH))
    assert committed is not None
    assert redis_client.xpending(stream_key(), CONSUMER_GROUP)["pending"] == 0


def test_duplicate_result_events_can_be_identified_safely(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    # Simulate an at-least-once redelivery of the same commit (e.g. client
    # retried after an ambiguous network response) by committing twice for
    # the same still-current attempt.
    first = worker.commit_result(entry, _synthetic_result(ResultDecision.MATCH))
    second = worker.commit_result(entry, _synthetic_result(ResultDecision.MATCH))
    assert first is not None
    assert second is not None
    assert first != second  # two distinct stream entries

    events = redis_client.xrange(results_stream_key())
    assert len(events) == 2
    identities = {(fields["job_id"], fields["attempt"]) for _, fields in events}
    assert identities == {(sample_job.job_id, "1")}  # same identity -- safe to dedupe on

    # the durable result hash is unaffected by the duplicate -- last write wins,
    # same content either way.
    record = ResultStore(redis_client).get(sample_job.job_id)
    assert record["decision"] == ResultDecision.MATCH


def test_result_remains_available_after_worker_exits(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: _synthetic_result(ResultDecision.MATCH))
    del worker  # worker process "exits" -- nothing lives in its memory

    fresh_client_view = ResultStore(redis_client).get(sample_job.job_id)
    assert fresh_client_view is not None
    assert fresh_client_view["decision"] == ResultDecision.MATCH


def test_completed_job_always_has_a_result_under_normal_operation(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = _worker(redis_client)
    entry = worker.claim_one()

    worker.process_claim(entry, lambda job: _synthetic_result(ResultDecision.NO_MATCH))

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED
    assert ResultStore(redis_client).get(sample_job.job_id) is not None
    assert redis_client.xpending(stream_key(), CONSUMER_GROUP)["pending"] == 0
    assert redis_client.xlen(results_stream_key()) == 1
