"""`resolve_outcome` — the crawler-facing view of a fingerprint job's
current state, folding `work_queue.state.JobStateStore` and
`work_queue.results.ResultStore` into the five-way outcome the phase brief
requires ("Result contract": "clearly distinguish MATCH, NO_MATCH, SKIPPED,
RETRYABLE_ERROR, PERMANENT_ERROR ... do not overload a single boolean").

This module adds no new Redis writes and does not change
`work_queue.results.Result`/`ResultDecision` (Phase 4/10's three-way
`match`/`no_match`/`processing_failure` contract) or
`work_queue.state.JobStatus` (Phase 1-3's five-way
`claimed`/`completed`/`rejected`/`retry_scheduled`/`failed`) — per the
phase brief, "Do not redesign the existing Phase 10 TechniqueEvidence/
aggregation contract unless integration reveals an actual incompatibility"
and integration revealed no such incompatibility, only a *need for a wider
view spanning both existing contracts*, which is exactly what a
crawler/downstream consumer needs and neither existing contract alone
provides (`JobStatus` doesn't know about match/no-match; `ResultDecision`
doesn't exist until a job actually completes; a job stuck in
`retry_scheduled` or a `rejected` malformed entry has *no* `Result` at all).

Mapping (see phase-12 doc, "Result schema" for the full justification of
each row):

    JobStatus                         -> FingerprintOutcome
    ---------------------------------------------------------------
    (no state hash yet: unclaimed)    -> PENDING
    claimed                           -> PENDING
    retry_scheduled                   -> RETRYABLE_ERROR
    rejected                          -> SKIPPED
    failed                            -> PERMANENT_ERROR
    completed + ResultDecision.MATCH               -> MATCH
    completed + ResultDecision.NO_MATCH             -> NO_MATCH
    completed + ResultDecision.PROCESSING_FAILURE   -> PERMANENT_ERROR
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from redis import Redis

from work_queue.results import ResultDecision, ResultStore
from work_queue.state import JobStateStore, JobStatus


class FingerprintOutcome(str, Enum):
    PENDING = "pending"  # not yet a terminal outcome: unclaimed, claimed, or a live retry-in-waiting
    MATCH = "match"
    NO_MATCH = "no_match"
    SKIPPED = "skipped"  # will never run: a malformed job contract (JobStatus.REJECTED)
    RETRYABLE_ERROR = "retryable_error"  # currently mid-retry; not yet terminal, but not simply "processing" either
    PERMANENT_ERROR = "permanent_error"


# Outcomes a caller should stop polling/waiting on — everything else may
# still change.
TERMINAL_OUTCOMES = frozenset(
    {FingerprintOutcome.MATCH, FingerprintOutcome.NO_MATCH, FingerprintOutcome.SKIPPED, FingerprintOutcome.PERMANENT_ERROR}
)


@dataclass(frozen=True)
class FingerprintOutcomeView:
    """Everything a crawler/downstream consumer needs to associate a
    fingerprint outcome with the candidate/target it submitted (phase
    brief, "Result contract" and "Observability") without reaching into
    `work_queue` internals itself."""

    job_id: str
    outcome: FingerprintOutcome
    media_evidence_id: Optional[str] = None
    target_id: Optional[str] = None
    target_version: Optional[str] = None
    attempt: Optional[int] = None
    worker_id: Optional[str] = None
    confidence: Optional[float] = None
    summary: Optional[str] = None
    evidence: Optional[str] = None  # JSON string, Phase 10's per-technique detail — passed through verbatim
    algorithm: Optional[str] = None
    processing_started_at: Optional[float] = None
    processing_completed_at: Optional[float] = None
    reason: Optional[str] = None  # failure/rejection reason text, when applicable


def resolve_outcome(redis_client: Redis, job_id: str) -> FingerprintOutcomeView:
    state = JobStateStore(redis_client).get(job_id)

    if state is None or state.get("status") == JobStatus.CLAIMED:
        return FingerprintOutcomeView(
            job_id=job_id,
            outcome=FingerprintOutcome.PENDING,
            attempt=int(state["attempt"]) if state and "attempt" in state else None,
            worker_id=state.get("worker_id") if state else None,
        )

    status = state.get("status")

    if status == JobStatus.REJECTED:
        return FingerprintOutcomeView(job_id=job_id, outcome=FingerprintOutcome.SKIPPED, reason=state.get("reason"))

    if status == JobStatus.RETRY_SCHEDULED:
        return FingerprintOutcomeView(
            job_id=job_id,
            outcome=FingerprintOutcome.RETRYABLE_ERROR,
            attempt=int(state["attempt"]) if "attempt" in state else None,
            reason=state.get("last_failure"),
        )

    if status == JobStatus.FAILED:
        return FingerprintOutcomeView(
            job_id=job_id,
            outcome=FingerprintOutcome.PERMANENT_ERROR,
            attempt=int(state["attempt"]) if "attempt" in state else None,
            reason=state.get("failure_reason"),
        )

    # status == JobStatus.COMPLETED: a Result was committed, look at its decision.
    record = ResultStore(redis_client).get(job_id)
    if record is None:
        # A plain ack() (no Result) completed the job — Phase 1-3's
        # original contract, still valid for a handler that returns
        # nothing. Nothing to report beyond "it finished with no verdict".
        return FingerprintOutcomeView(
            job_id=job_id,
            outcome=FingerprintOutcome.SKIPPED,
            attempt=int(state["attempt"]) if "attempt" in state else None,
            reason="job completed with no committed Result",
        )

    decision = record.get("decision")
    if decision == ResultDecision.MATCH:
        outcome = FingerprintOutcome.MATCH
    elif decision == ResultDecision.NO_MATCH:
        outcome = FingerprintOutcome.NO_MATCH
    else:  # ResultDecision.PROCESSING_FAILURE
        outcome = FingerprintOutcome.PERMANENT_ERROR

    return FingerprintOutcomeView(
        job_id=job_id,
        outcome=outcome,
        media_evidence_id=record.get("media_evidence_id"),
        target_id=record.get("target_id"),
        target_version=record.get("target_version"),
        attempt=int(record["attempt"]) if "attempt" in record else None,
        worker_id=record.get("worker_id"),
        confidence=float(record["confidence"]) if record.get("confidence") else None,
        summary=record.get("summary"),
        evidence=record.get("evidence"),
        algorithm=record.get("algorithm"),
        processing_started_at=float(record["processing_started_at"]) if record.get("processing_started_at") else None,
        processing_completed_at=(
            float(record["processing_completed_at"]) if record.get("processing_completed_at") else None
        ),
        reason=record.get("summary") if outcome == FingerprintOutcome.PERMANENT_ERROR else None,
    )
