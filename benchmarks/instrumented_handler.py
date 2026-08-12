"""A per-stage-timed mirror of `worker/matching_handler.py::build_matching_handler`'s
handler body.

Phase 11's brief requires separate acquisition/probing/embedding/matching/
persistence timings, but the production handler (correctly, for
production) only returns a `Result` — it has no reason to expose internal
stage timings to a caller. Rather than add instrumentation hooks to
production code ("do not refactor existing modules simply to make
benchmarking easier" — phase-11 brief), this module duplicates the
handler's *call sequence* against the exact same production primitives
(`MediaAcquirer.acquire`, `DINOv2EmbeddingEngine.embed_video_segments`,
`TargetRegistry.get_or_build_segment_embedding`, `match_segments`,
`combine`), wrapping each call in a `Stopwatch`, and mirrors its
Transient/PermanentFailure error-mapping table exactly (see that module's
docstring) so a benchmark run classifies outcomes identically to
production.

**Must be kept in sync with `worker/matching_handler.py` by hand** if that
file's orchestration changes — there is no way around this duplication
without adding hooks to production code, which this phase's brief
explicitly avoids doing speculatively. This is benchmark-only code; it is
never imported by `worker/`, `embedding/`, `matching/`, or `target/`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from acquisition import MediaAcquirer, PermanentAcquisitionError, TransientAcquisitionError
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from embedding.errors import InferenceError, UnsupportedMediaError
from matching.aggregation import DINOV2_TEMPORAL_TECHNIQUE, combine, temporal_match_to_evidence
from matching.config import MatcherConfig
from matching.matcher import match_segments
from target.registry import TargetRegistry
from work_queue.jobs import Job
from work_queue.results import Result, ResultDecision
from worker.fingerprint_worker import PermanentFailure, TransientFailure
from worker.matching_handler import _target_artifact


@dataclass
class StageTimings:
    acquire_s: Optional[float] = None
    candidate_embed_s: Optional[float] = None
    target_resolve_s: Optional[float] = None  # whole get_or_build_segment_embedding call
    target_build_s: Optional[float] = None  # only set if THIS call actually ran build()
    match_s: Optional[float] = None
    aggregate_s: Optional[float] = None
    handler_total_s: Optional[float] = None
    # filled by the caller/driver, not this module:
    claim_s: Optional[float] = None
    commit_s: Optional[float] = None
    # classification, inferred from which stages ran / how long they took:
    target_built_by_this_call: bool = False
    outcome: str = "unknown"  # "match" | "no_match" | "processing_failure" | "transient_failure" | "permanent_failure"
    error: Optional[str] = None


def run_instrumented(
    job: Job,
    acquirer: MediaAcquirer,
    engine: DINOv2EmbeddingEngine,
    registry: TargetRegistry,
    matcher_config: Optional[MatcherConfig] = None,
) -> "tuple[Optional[Result], StageTimings]":
    """Returns `(result_or_none, timings)`. `result_or_none` is `None` iff
    the job failed (transient or permanent) — timings.outcome/.error
    describe why, mirroring how `Worker.process_claim` would have
    dispatched the same exception."""
    matcher_config = matcher_config or MatcherConfig()
    t = StageTimings()
    handler_start = time.monotonic()

    if DINOV2_TEMPORAL_TECHNIQUE not in job.techniques:
        t.outcome = "permanent_failure"
        t.error = "technique not implemented"
        t.handler_total_s = time.monotonic() - handler_start
        return None, t

    started = time.time()

    t0 = time.monotonic()
    try:
        artifact = acquirer.acquire(job.media_url)
    except TransientAcquisitionError as exc:
        t.acquire_s = time.monotonic() - t0
        t.outcome = "transient_failure"
        t.error = str(exc)
        t.handler_total_s = time.monotonic() - handler_start
        return None, t
    except PermanentAcquisitionError as exc:
        t.acquire_s = time.monotonic() - t0
        t.outcome = "permanent_failure"
        t.error = str(exc)
        t.handler_total_s = time.monotonic() - handler_start
        return None, t
    t.acquire_s = time.monotonic() - t0

    try:
        t0 = time.monotonic()
        try:
            candidate = engine.embed_video_segments(artifact)
        except (UnsupportedMediaError, InferenceError) as exc:
            t.candidate_embed_s = time.monotonic() - t0
            t.outcome = "processing_failure"
            t.error = str(exc)
            t.handler_total_s = time.monotonic() - handler_start
            return (
                Result(
                    decision=ResultDecision.PROCESSING_FAILURE,
                    algorithm=DINOV2_TEMPORAL_TECHNIQUE,
                    processing_started_at=started,
                    processing_completed_at=time.time(),
                    summary=f"candidate embedding failed: {exc}",
                ),
                t,
            )
        t.candidate_embed_s = time.monotonic() - t0

        build_ran = {"flag": False, "duration_s": None}

        def build(record):
            build_ran["flag"] = True
            b0 = time.monotonic()
            target_result = engine.embed_video_segments(_target_artifact(record))
            build_ran["duration_s"] = time.monotonic() - b0
            return target_result.segments, target_result.coarse_vector

        t0 = time.monotonic()
        try:
            target_entry = registry.get_or_build_segment_embedding(
                job.target_id, job.target_version, candidate.to_embedding_spec(), build
            )
        except KeyError as exc:
            t.target_resolve_s = time.monotonic() - t0
            t.outcome = "permanent_failure"
            t.error = str(exc)
            t.handler_total_s = time.monotonic() - handler_start
            return None, t
        except TimeoutError as exc:
            t.target_resolve_s = time.monotonic() - t0
            t.outcome = "transient_failure"
            t.error = str(exc)
            t.handler_total_s = time.monotonic() - handler_start
            return None, t
        except UnsupportedMediaError as exc:
            t.target_resolve_s = time.monotonic() - t0
            t.outcome = "permanent_failure"
            t.error = str(exc)
            t.handler_total_s = time.monotonic() - handler_start
            return None, t
        except InferenceError as exc:
            t.target_resolve_s = time.monotonic() - t0
            t.outcome = "transient_failure"
            t.error = str(exc)
            t.handler_total_s = time.monotonic() - handler_start
            return None, t
        t.target_resolve_s = time.monotonic() - t0
        t.target_built_by_this_call = build_ran["flag"]
        t.target_build_s = build_ran["duration_s"]

        t0 = time.monotonic()
        temporal_result = match_segments(
            target_segments=list(target_entry.segments),
            candidate_segments=list(candidate.segments),
            target_id=job.target_id,
            target_version=job.target_version,
            candidate_id=job.media_evidence_id,
            config=matcher_config,
            target_coarse_vector=target_entry.coarse_vector,
            candidate_coarse_vector=candidate.coarse_vector,
        )
        t.match_s = time.monotonic() - t0

        t0 = time.monotonic()
        evidence = temporal_match_to_evidence(temporal_result)
        result = combine([evidence], processing_started_at=started, processing_completed_at=time.time())
        t.aggregate_s = time.monotonic() - t0

        t.outcome = "match" if result.decision == ResultDecision.MATCH else "no_match"
        t.handler_total_s = time.monotonic() - handler_start
        return result, t
    finally:
        artifact.cleanup()
