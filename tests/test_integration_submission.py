"""Phase 12 — `integration.submission.FingerprintJobSubmitter`.

Covers: candidate validation ("invalid job schema"), deterministic job
identity + duplicate suppression ("idempotency", "duplicate job
submission"), backpressure admission control, priority-stream routing, and
Redis namespace isolation from the crawler's own `crawler:*`/`evidence:*`
keys (phase-12 doc, "Redis namespaces").
"""
from __future__ import annotations

import pytest

from integration.backpressure import count_outstanding
from integration.candidate import FingerprintCandidate, FingerprintPriority
from integration.idempotency import derive_job_id
from integration.keys import submission_marker_key
from integration.submission import FingerprintJobSubmitter, SubmissionOutcome
from work_queue.jobs import Job
from work_queue.keys import stream_key
from worker.fingerprint_worker import Worker


def _candidate(**overrides) -> FingerprintCandidate:
    defaults = dict(
        candidate_url="https://pirate.example/clip.mp4",
        media_evidence_id="evidence-1",
        media_type="video",
        source_domain="pirate.example",
        target_id="target-1",
        target_version="v1",
    )
    defaults.update(overrides)
    return FingerprintCandidate(**defaults)


# -- validation ---------------------------------------------------------


def test_invalid_scheme_is_rejected_before_touching_redis(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate(candidate_url="ftp://pirate.example/clip.mp4")

    result = submitter.submit(candidate)

    assert result.outcome == SubmissionOutcome.REJECTED_INVALID
    assert result.job_id is None
    assert redis_client.xlen(stream_key()) == 0


def test_empty_required_field_is_rejected(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate(target_id="")

    result = submitter.submit(candidate)

    assert result.outcome == SubmissionOutcome.REJECTED_INVALID
    assert "target_id" in result.detail


# -- happy path + job mapping --------------------------------------------


def test_submit_enqueues_a_job_matching_the_candidate(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate()

    result = submitter.submit(candidate)

    assert result.outcome == SubmissionOutcome.ENQUEUED
    assert result.job_id == derive_job_id(candidate)
    assert result.entry_id is not None

    entries = redis_client.xrange(stream_key())
    assert len(entries) == 1
    _, fields = entries[0]
    job = Job.from_stream_fields(fields)
    assert job.job_id == result.job_id
    assert job.media_url == candidate.candidate_url
    assert job.media_evidence_id == candidate.media_evidence_id
    assert job.target_id == candidate.target_id
    assert job.target_version == candidate.target_version
    assert job.techniques == candidate.techniques
    assert job.max_attempts == candidate.max_attempts
    assert job.schema_version == 1


def test_priority_selects_the_stream(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    high = _candidate(candidate_url="https://pirate.example/high.mp4", priority=FingerprintPriority.HIGH)
    low = _candidate(candidate_url="https://pirate.example/low.mp4", priority=FingerprintPriority.LOW)

    submitter.submit(high)
    submitter.submit(low)

    assert redis_client.xlen(stream_key("high")) == 1
    assert redis_client.xlen(stream_key("low")) == 1
    assert redis_client.xlen(stream_key()) == 0  # default/normal stream untouched


# -- idempotency / duplicate submission -----------------------------------


def test_identical_candidate_resolves_to_the_same_job_id(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    a = submitter.submit(_candidate(media_evidence_id="evidence-A"))
    b = submitter.submit(_candidate(media_evidence_id="evidence-B"))  # rediscovered on a different page

    assert a.job_id == b.job_id  # same (url, target_id, target_version, techniques)


def test_duplicate_submission_is_suppressed_and_enqueues_only_once(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate()

    first = submitter.submit(candidate)
    second = submitter.submit(candidate)

    assert first.outcome == SubmissionOutcome.ENQUEUED
    assert second.outcome == SubmissionOutcome.DUPLICATE_SUPPRESSED
    assert second.job_id == first.job_id
    assert redis_client.xlen(stream_key()) == 1  # not 2


def test_different_target_version_is_a_different_job(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    v1 = submitter.submit(_candidate(target_version="v1"))
    v2 = submitter.submit(_candidate(target_version="v2"))

    assert v1.job_id != v2.job_id
    assert v1.outcome == SubmissionOutcome.ENQUEUED
    assert v2.outcome == SubmissionOutcome.ENQUEUED
    assert redis_client.xlen(stream_key()) == 2


def test_submission_marker_is_set_under_the_fingerprint_namespace(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate()

    result = submitter.submit(candidate)

    marker_key = submission_marker_key(result.job_id)
    assert marker_key.startswith("fingerprint:submission:")
    assert redis_client.exists(marker_key) == 1
    assert redis_client.ttl(marker_key) > 0


# -- backpressure -----------------------------------------------------------


def test_submission_is_rejected_once_outstanding_jobs_reach_the_limit(redis_client):
    submitter = FingerprintJobSubmitter(redis_client, max_outstanding_jobs=1)
    first = submitter.submit(_candidate(candidate_url="https://pirate.example/one.mp4"))
    second = submitter.submit(_candidate(candidate_url="https://pirate.example/two.mp4"))

    assert first.outcome == SubmissionOutcome.ENQUEUED
    assert second.outcome == SubmissionOutcome.REJECTED_BACKPRESSURE
    assert redis_client.xlen(stream_key()) == 1

    # No submission marker was left behind for the rejected candidate, so a
    # later retry (once the backlog drains) is not falsely deduped.
    assert redis_client.exists(submission_marker_key(second.job_id)) == 0


def test_backpressure_clears_once_the_job_is_claimed_and_acked(redis_client):
    submitter = FingerprintJobSubmitter(redis_client, max_outstanding_jobs=1)
    submitter.submit(_candidate(candidate_url="https://pirate.example/one.mp4"))
    assert count_outstanding(redis_client, "default") == 1

    worker = Worker(redis_client, block_ms=100)
    entry = worker.claim_one()
    worker.ack(entry)

    assert count_outstanding(redis_client, "default") == 0
    retry = submitter.submit(_candidate(candidate_url="https://pirate.example/two.mp4"))
    assert retry.outcome == SubmissionOutcome.ENQUEUED


def test_rejected_backpressure_candidate_can_be_resubmitted_later(redis_client):
    submitter = FingerprintJobSubmitter(redis_client, max_outstanding_jobs=1)
    submitter.submit(_candidate(candidate_url="https://pirate.example/one.mp4"))
    blocked = submitter.submit(_candidate(candidate_url="https://pirate.example/two.mp4"))
    assert blocked.outcome == SubmissionOutcome.REJECTED_BACKPRESSURE

    worker = Worker(redis_client, block_ms=100)
    worker.ack(worker.claim_one())

    retried = submitter.submit(_candidate(candidate_url="https://pirate.example/two.mp4"))
    assert retried.outcome == SubmissionOutcome.ENQUEUED
    assert retried.job_id == blocked.job_id


# -- namespace isolation ------------------------------------------------


def test_integration_writes_never_touch_crawler_or_evidence_keys(redis_client):
    """The crawler's own namespaces (`crawler:*` frontier,
    `evidence:*` media-evidence/job-queue — see phase-12 doc, "Current
    crawler architecture") must be untouched by anything this module
    writes. Simulates a shared Redis deployment by pre-seeding
    representative crawler keys, then asserts they survive a full
    submit + claim + ack cycle byte-for-byte."""
    redis_client.sadd("crawler:urls:known", "https://pirate.example/clip.mp4")
    redis_client.zadd("crawler:domain:pirate.example:queue", {"https://pirate.example/clip.mp4": 10_000_001})
    redis_client.hset("evidence:asset:aid-1", mapping={"canonical_url": "https://pirate.example/clip.mp4"})
    redis_client.zadd("evidence:jobs:queue", {"aid-1": 10_000_001})
    before = {
        key: redis_client.type(key) == "hash" and redis_client.hgetall(key) or redis_client.smembers(key)
        if redis_client.type(key) in ("hash", "set")
        else redis_client.zrange(key, 0, -1, withscores=True)
        for key in ("crawler:urls:known", "crawler:domain:pirate.example:queue", "evidence:asset:aid-1", "evidence:jobs:queue")
    }

    submitter = FingerprintJobSubmitter(redis_client)
    result = submitter.submit(_candidate())
    worker = Worker(redis_client, block_ms=100)
    worker.ack(worker.claim_one())

    for key, snapshot in before.items():
        if redis_client.type(key) == "hash":
            assert redis_client.hgetall(key) == snapshot
        elif redis_client.type(key) == "set":
            assert redis_client.smembers(key) == snapshot
        else:
            assert redis_client.zrange(key, 0, -1, withscores=True) == snapshot

    # And every key this test's submit/claim/ack cycle created is under fingerprint:*.
    all_keys = redis_client.keys("*")
    new_keys = [
        k for k in all_keys if not k.startswith("crawler:") and not k.startswith("evidence:")
    ]
    assert new_keys  # sanity: something was created
    assert all(k.startswith("fingerprint:") for k in new_keys)
    assert result.outcome == SubmissionOutcome.ENQUEUED
