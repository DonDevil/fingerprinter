"""Phase 12 — `integration.outcome.resolve_outcome`.

Exercises the full JobStatus/ResultDecision -> FingerprintOutcome mapping
(phase-12 doc, "Result schema") without any real media/DINOv2 — synthetic
handlers standing in for a real fingerprint pipeline, mirroring
tests/test_retry.py's style. The MATCH/NO_MATCH cases (which require a real
`Result`) are covered end-to-end with real inference in
tests/test_integration_e2e.py.
"""
from __future__ import annotations

import time

import pytest

from integration.candidate import FingerprintCandidate
from integration.outcome import TERMINAL_OUTCOMES, FingerprintOutcome, resolve_outcome
from integration.submission import FingerprintJobSubmitter, SubmissionOutcome
from work_queue.keys import retry_zset_key
from work_queue.results import Result, ResultDecision
from worker.fingerprint_worker import PermanentFailure, TransientFailure, Worker

BLOCK_MS = 200
BASE_DELAY_S = 0.1
MAX_DELAY_S = 0.3


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


def _worker(redis_client, **overrides):
    kwargs = dict(block_ms=BLOCK_MS, retry_base_delay_s=BASE_DELAY_S, retry_max_delay_s=MAX_DELAY_S)
    kwargs.update(overrides)
    return Worker(redis_client, **kwargs)


def _submit(redis_client, **overrides):
    submitter = FingerprintJobSubmitter(redis_client)
    result = submitter.submit(_candidate(**overrides))
    assert result.outcome == SubmissionOutcome.ENQUEUED
    return result.job_id


# -- non-terminal ---------------------------------------------------------


def test_unclaimed_job_is_pending(redis_client):
    job_id = _submit(redis_client)

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.PENDING
    assert view.outcome not in TERMINAL_OUTCOMES


def test_claimed_but_unfinished_job_is_pending(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    worker.claim_one()

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.PENDING


# -- retryable --------------------------------------------------------------


def test_transient_failure_resolves_to_retryable_error(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: (_ for _ in ()).throw(TransientFailure("network blip")))

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.RETRYABLE_ERROR
    assert view.outcome not in TERMINAL_OUTCOMES
    assert view.reason == "network blip"


# -- permanent ----------------------------------------------------------


def test_permanent_worker_failure_resolves_to_permanent_error(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: (_ for _ in ()).throw(PermanentFailure("404 not found")))

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.PERMANENT_ERROR
    assert view.outcome in TERMINAL_OUTCOMES
    assert view.reason == "404 not found"


def test_max_attempts_exhausted_resolves_to_permanent_error(redis_client):
    submitter = FingerprintJobSubmitter(redis_client)
    candidate = _candidate(candidate_url="https://pirate.example/single-attempt.mp4", max_attempts=1)
    submission = submitter.submit(candidate)
    assert submission.outcome == SubmissionOutcome.ENQUEUED

    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: (_ for _ in ()).throw(TransientFailure("t1")))

    view = resolve_outcome(redis_client, submission.job_id)

    assert view.outcome == FingerprintOutcome.PERMANENT_ERROR
    assert "max_attempts (1) exhausted" in view.reason


def test_processing_failure_result_resolves_to_permanent_error(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    result = Result(
        decision=ResultDecision.PROCESSING_FAILURE,
        algorithm="dinov2",
        processing_started_at=time.time(),
        processing_completed_at=time.time(),
        summary="candidate embedding failed: corrupt media",
    )
    worker.process_claim(entry, lambda job: result)

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.PERMANENT_ERROR
    assert view.outcome in TERMINAL_OUTCOMES
    assert "corrupt media" in view.reason


# -- skipped ----------------------------------------------------------------


def test_malformed_stream_entry_resolves_to_skipped(redis_client):
    from work_queue.keys import stream_key

    entry_id = redis_client.xadd(stream_key(), {"job_id": "malformed-job", "media_url": "https://x/y"})
    worker = _worker(redis_client)
    worker.claim_one()  # rejects + acks the malformed entry

    view = resolve_outcome(redis_client, "malformed-job")

    assert view.outcome == FingerprintOutcome.SKIPPED
    assert view.outcome in TERMINAL_OUTCOMES


def test_plain_ack_with_no_result_resolves_to_skipped(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    worker.process_claim(entry, lambda job: None)  # legacy plain-ack handler, Phase 1-3 contract

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.SKIPPED


# -- match / no_match (synthetic Result, real MATCH/NO_MATCH covered in e2e) --


def test_match_result_resolves_to_match(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    result = Result(
        decision=ResultDecision.MATCH,
        algorithm="dinov2",
        processing_started_at=time.time(),
        processing_completed_at=time.time(),
        confidence=0.97,
        summary="dinov2=match (score=0.9700)",
        evidence="[]",
    )
    worker.process_claim(entry, lambda job: result)

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.MATCH
    assert view.outcome in TERMINAL_OUTCOMES
    assert view.confidence == pytest.approx(0.97)
    assert view.algorithm == "dinov2"
    assert view.target_id == "target-1"
    assert view.target_version == "v1"


def test_no_match_result_resolves_to_no_match(redis_client):
    job_id = _submit(redis_client)
    worker = _worker(redis_client)
    entry = worker.claim_one()
    result = Result(
        decision=ResultDecision.NO_MATCH,
        algorithm="dinov2",
        processing_started_at=time.time(),
        processing_completed_at=time.time(),
        confidence=0.10,
    )
    worker.process_claim(entry, lambda job: result)

    view = resolve_outcome(redis_client, job_id)

    assert view.outcome == FingerprintOutcome.NO_MATCH
    assert view.outcome in TERMINAL_OUTCOMES


# -- correlation --------------------------------------------------------------


def test_outcome_view_carries_job_id_and_media_evidence_id_through(redis_client):
    job_id = _submit(redis_client, media_evidence_id="evidence-correlate")
    worker = _worker(redis_client)
    entry = worker.claim_one()
    result = Result(
        decision=ResultDecision.MATCH,
        algorithm="dinov2",
        processing_started_at=time.time(),
        processing_completed_at=time.time(),
        confidence=1.0,
    )
    worker.process_claim(entry, lambda job: result)

    view = resolve_outcome(redis_client, job_id)

    assert view.job_id == job_id
    assert view.media_evidence_id == "evidence-correlate"
