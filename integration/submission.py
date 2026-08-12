"""`FingerprintJobSubmitter` — the crawler-facing entry point of the Phase
12 integration boundary.

This is `docs/design/design-proposal-1.md` §2's "Job producer (library used
by crawler)" component, built out from Phase 1's bare
`work_queue.producer.JobProducer` (fire-and-forget `XADD`, no dedup, no
admission control — still used internally here, unchanged) into something
safe to call from a crawler that may:

- discover the same candidate URL more than once (idempotency, mandatory
  per the phase brief);
- discover candidates faster than the fingerprint worker fleet can drain
  them (backpressure, mandatory per the phase brief).

Idempotency mechanism (phase brief, "Idempotency"): `job_id` is a
deterministic hash of `(candidate_url, target_id, target_version,
techniques)` (`integration.idempotency.derive_job_id`), and a `SET NX EX`
marker at `integration.keys.submission_marker_key(job_id)` makes "has this
exact unit of work already been submitted" an atomic, single-round-trip
Redis check — no distributed transaction, matching the brief's explicit
preference. The marker is deliberately separate from
`work_queue.state.JobStateStore` (`fingerprint:job:{job_id}:state`), which
Phase 1 only writes at *claim* time, not at enqueue time — without this
marker, two submissions racing before either is claimed would both pass
straight through to `XADD`, producing two stream entries for the same
`job_id` that would then race each other through the attempt-counting
logic in `worker/fingerprint_worker.py`. See module-level ordering note in
`submit()` for why the marker is only set *after* the backpressure check,
and released on any failure to actually enqueue.

Backpressure mechanism: see `integration/backpressure.py`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from redis import Redis

from integration.backpressure import DEFAULT_MAX_OUTSTANDING_JOBS, count_outstanding
from integration.candidate import PRIORITY_STREAM_NAMES, CandidateValidationError, FingerprintCandidate
from integration.idempotency import derive_job_id
from integration.keys import submission_marker_key
from work_queue.jobs import Job
from work_queue.producer import JobProducer

# How long a submission marker survives. Must comfortably outlast the
# longest realistic job lifetime (claim + retries + backoff) so a job that
# is still legitimately in flight is never mistaken for "never submitted"
# and double-enqueued — but should not persist forever, since a
# (candidate_url, target_id, target_version, techniques) tuple that
# resolved to NO_MATCH weeks ago may legitimately deserve re-checking (e.g.
# after a target re-registration bumps content but not target_version, or
# simply as periodic re-verification). PROVISIONAL: 24h is generous
# relative to every retry/backoff/lock timeout measured or configured
# elsewhere in this project (max backoff 60s default, target lock TTL
# 600s, Phase 11 §17) with wide margin for a slow/contended fleet, but has
# no labeled data behind the specific number — see phase-12 doc.
DEFAULT_SUBMISSION_MARKER_TTL_S = 24 * 60 * 60


class SubmissionOutcome(str, Enum):
    ENQUEUED = "enqueued"
    DUPLICATE_SUPPRESSED = "duplicate_suppressed"
    REJECTED_BACKPRESSURE = "rejected_backpressure"
    REJECTED_INVALID = "rejected_invalid"


@dataclass(frozen=True)
class SubmissionResult:
    outcome: SubmissionOutcome
    job_id: Optional[str]
    entry_id: Optional[str] = None  # Redis Stream entry id, only set for ENQUEUED
    detail: Optional[str] = None


class FingerprintJobSubmitter:
    def __init__(
        self,
        redis_client: Redis,
        max_outstanding_jobs: int = DEFAULT_MAX_OUTSTANDING_JOBS,
        submission_marker_ttl_s: int = DEFAULT_SUBMISSION_MARKER_TTL_S,
    ):
        self._redis = redis_client
        self._max_outstanding_jobs = max_outstanding_jobs
        self._submission_marker_ttl_s = submission_marker_ttl_s

    def submit(self, candidate: FingerprintCandidate) -> SubmissionResult:
        """Validate, dedup, admission-control, and (if all three pass)
        enqueue `candidate` as a fingerprint job.

        Ordering is deliberate: validate (free) -> check backpressure
        (read-only) -> claim the dedup marker (the one write before
        enqueue) -> enqueue. Checking backpressure *before* claiming the
        marker means a rejected submission leaves no marker behind, so a
        caller that retries later (e.g. once the backlog drains) is not
        incorrectly treated as a duplicate of a submission that never
        actually happened.
        """
        try:
            candidate.validate()
        except CandidateValidationError as exc:
            return SubmissionResult(SubmissionOutcome.REJECTED_INVALID, job_id=None, detail=str(exc))

        job_id = derive_job_id(candidate)
        priority_name = PRIORITY_STREAM_NAMES[candidate.priority]

        outstanding = count_outstanding(self._redis, priority_name)
        # `None` means Redis could not report a reliable backlog figure
        # (see `count_outstanding`'s docstring) — fail toward *rejecting*
        # rather than silently disabling the bound the brief mandates.
        if outstanding is None or outstanding >= self._max_outstanding_jobs:
            return SubmissionResult(
                SubmissionOutcome.REJECTED_BACKPRESSURE,
                job_id=job_id,
                detail=f"outstanding={outstanding!r} >= max_outstanding_jobs={self._max_outstanding_jobs}",
            )

        if not self._claim_submission_marker(job_id):
            return SubmissionResult(SubmissionOutcome.DUPLICATE_SUPPRESSED, job_id=job_id)

        job = Job(
            job_id=job_id,
            media_evidence_id=candidate.media_evidence_id,
            media_url=candidate.candidate_url,
            media_type=candidate.media_type,
            source_domain=candidate.source_domain,
            target_id=candidate.target_id,
            target_version=candidate.target_version,
            techniques=tuple(candidate.techniques),
            max_attempts=candidate.max_attempts,
        )
        try:
            entry_id = JobProducer(self._redis, priority=priority_name).enqueue(job)
        except Exception:
            # The marker promised "this will be enqueued"; since it wasn't,
            # release it so a caller's retry isn't falsely suppressed as a
            # duplicate of a submission that never reached the stream.
            self._redis.delete(submission_marker_key(job_id))
            raise

        return SubmissionResult(SubmissionOutcome.ENQUEUED, job_id=job_id, entry_id=entry_id)

    def _claim_submission_marker(self, job_id: str) -> bool:
        """Atomic `SET NX EX`: True iff this call is the first to claim
        `job_id`'s submission marker. The stored value (submission time) is
        diagnostic only — never read back for correctness, only presence
        matters."""
        claimed = self._redis.set(
            submission_marker_key(job_id), str(time.time()), nx=True, ex=self._submission_marker_ttl_s
        )
        return bool(claimed)
