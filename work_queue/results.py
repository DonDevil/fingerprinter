"""Result schema for the fingerprint result contract.

A `Result` is what a handler hands back to the worker after it *finishes
running* against a claimed job. It is deliberately distinct from
TransientFailure/PermanentFailure (Phase 3): those describe the worker
failing to run the job at all (retry it, or give up). A `Result` describes
what the fingerprinting logic concluded once it did run — including the
case where it ran to completion but couldn't reach a determination
(`processing_failure`, e.g. corrupt media, algorithm error), which is a
result, not a retryable/permanent worker failure.

`Result` only carries fields the handler actually computes. `ResultRecord`
is the durable record: a `Result` plus the job-queue context (identity,
attempt, worker_id) that the handler doesn't own, assembled by
`Worker.commit_result()` — so a handler never has to duplicate fields it
didn't compute, and the schema stays usable if a later phase's handler
combines multiple techniques into one `Result` (e.g. a `algorithm` value
like "dinov2+phash").

Phase 10 adds `evidence`: an optional JSON-encoded string carrying the
per-technique detail (score, matcher_version, localization, ...) that
`confidence`/`summary` alone can't hold — the `evidence json` field
`docs/design/design-proposal-1.md` §4 specified but Phase 4 had nothing to
populate it with yet. See `matching/aggregation.py`, which is what
actually builds it from one or more technique-specific match results
(currently just Phase 9's `TemporalMatchResult`; later phases' audio/OCR/
watermark evidence folds in the same way, one list entry each).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

from redis import Redis

from work_queue.keys import result_key

RESULT_SCHEMA_VERSION = 1


class ResultDecision:
    """Three-way outcome. Never collapse this to a boolean: a failure to
    reach a determination is not the same thing as a confident non-match."""

    MATCH = "match"
    NO_MATCH = "no_match"
    PROCESSING_FAILURE = "processing_failure"


@dataclass(frozen=True)
class Result:
    """What a handler concluded. No job/queue identity here — see ResultRecord."""

    decision: str
    algorithm: str
    processing_started_at: float
    processing_completed_at: float
    confidence: Optional[float] = None
    summary: Optional[str] = None
    evidence: Optional[str] = None

    @property
    def processing_duration(self) -> float:
        return self.processing_completed_at - self.processing_started_at


@dataclass(frozen=True)
class ResultRecord:
    """The durable, per-job record: a Result plus the queue context it was
    produced under. This is what gets HSET onto `result_key(job_id)`."""

    job_id: str
    media_evidence_id: str
    target_id: str
    target_version: str
    attempt: int
    worker_id: str
    result: Result
    result_version: int = RESULT_SCHEMA_VERSION

    def to_hash_fields(self) -> dict[str, str]:
        fields = {
            "job_id": self.job_id,
            "media_evidence_id": self.media_evidence_id,
            "target_id": self.target_id,
            "target_version": self.target_version,
            "attempt": str(self.attempt),
            "worker_id": self.worker_id,
            "result_version": str(self.result_version),
            "decision": self.result.decision,
            "algorithm": self.result.algorithm,
            "processing_started_at": str(self.result.processing_started_at),
            "processing_completed_at": str(self.result.processing_completed_at),
            "processing_duration": str(self.result.processing_duration),
        }
        if self.result.confidence is not None:
            fields["confidence"] = str(self.result.confidence)
        if self.result.summary:
            fields["summary"] = self.result.summary
        if self.result.evidence:
            fields["evidence"] = self.result.evidence
        return fields

    def to_event_fields(self) -> dict[str, str]:
        """Fields for the result-stream event. Deliberately minimal — just
        enough for a downstream consumer to (a) identify the event via
        `(job_id, attempt)` for dedup, and (b) fetch the full durable
        record from `result_key(job_id)`. Not a copy of the full record."""
        return {
            "job_id": self.job_id,
            "media_evidence_id": self.media_evidence_id,
            "target_id": self.target_id,
            "attempt": str(self.attempt),
            "decision": self.result.decision,
            "result_version": str(self.result_version),
            "worker_id": self.worker_id,
            "completed_at": str(self.result.processing_completed_at),
        }


class ResultStore:
    """Read-only accessor for the durable per-job result hash."""

    def __init__(self, redis_client: Redis):
        self._redis = redis_client

    def get(self, job_id: str) -> Optional[Mapping[str, str]]:
        data = self._redis.hgetall(result_key(job_id))
        return data or None
