"""Phase 10 — the fingerprint-worker handler Phase 9 left unwired (see its
§16/§18): claim -> acquire candidate -> embed candidate segments -> resolve
target segments (cache-first, build-on-miss under a lock) -> match_segments
-> fold into a `Result` via `matching.aggregation`, inside the
worker/handler contract Phases 1-4 established.

Mirrors `worker/acquisition_handler.py`'s shape (acquire, map acquisition
errors onto Transient/PermanentFailure, always clean up the artifact) and
extends it with the fingerprinting Phase 5's handler was explicitly a
stand-in for.

Error-mapping decisions (unresolved by any earlier phase — see phase-10
doc, "Error mapping", for the full reasoning):

- Acquisition failures: unchanged from Phase 5 — Transient/Permanent-
  AcquisitionError -> TransientFailure/PermanentFailure.
- Candidate embedding failures (`UnsupportedMediaError`/`InferenceError`
  on the *candidate*): become `Result(decision=PROCESSING_FAILURE)`, not a
  raised worker failure. `work_queue.results.Result`'s own docstring names
  exactly this case ("corrupt media, algorithm error") as what
  `PROCESSING_FAILURE` exists for — a completed job with unusable evidence
  is more useful downstream than a silent terminal failure with no result
  at all. This intentionally diverges from `embedding/errors.py`'s Phase-7
  mapping table (written before `PROCESSING_FAILURE` had anywhere to be
  written to); that table's literal mapping still governs the *target*
  side below, which is a different situation.
- Target-side embedding failures (the target's own registered media is
  corrupt, or the model errors on it while building the target's segment
  cache): `PermanentFailure`/`TransientFailure` per `embedding/errors.py`'s
  table, applied literally. This is not the candidate's fault and not
  evidence about it — it's an operational/config problem (a broken target
  registration) that will fail identically for every job against this
  target until ops fixes it, so it belongs at the job/worker level, not
  folded into a per-candidate `Result`.
- Unknown `target_id`/`target_version` (`KeyError` from the registry):
  `PermanentFailure` — not retryable, a routing/config problem.
- Build-on-miss lock wait timeout (`TimeoutError` — another worker is
  still building): `TransientFailure` — a retry may simply land after the
  other worker finishes.
- `job.techniques` not naming this handler's technique
  (`matching.aggregation.DINOV2_TEMPORAL_TECHNIQUE`): `PermanentFailure` —
  this worker has nothing to run for the job as specified.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Optional
from urllib.parse import urlparse

from acquisition import MediaAcquirer, PermanentAcquisitionError, TransientAcquisitionError
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from embedding.errors import InferenceError, UnsupportedMediaError
from matching.aggregation import DINOV2_TEMPORAL_TECHNIQUE, combine, temporal_match_to_evidence
from matching.config import MatcherConfig
from matching.matcher import match_segments
from matching.result import TemporalMatchResult
from target.artifact import target_media_artifact as _target_artifact
from target.registry import TargetRegistry
from target.shared_storage import SharedArtifactStoreError, SharedTargetMediaStore
from work_queue.jobs import Job
from work_queue.results import Result, ResultDecision
from worker.fingerprint_worker import PermanentFailure, TransientFailure
from worker.observability import log_event

# `_target_artifact` now lives in `target/artifact.py` (target-eager-build
# audit, Part B §B.4.G) so `target/build.py`'s explicit build command can
# share it instead of duplicating it; re-exported under its original name
# here since existing callers (benchmarks/*.py) import it from this module.

logger = logging.getLogger(__name__)


def _noop_stage_recorder(stage: str, duration_s: float) -> None:
    pass


# ---------------------------------------------------------------------------
# DEBUG-only diagnostics (observability audit). Every helper below is
# reached only when `logger.isEnabledFor(logging.DEBUG)` is true -- normal
# (INFO) runs pay for exactly that one boolean check per call site and never
# construct these log records at all. None of this changes claim/retry/
# match/cache semantics; it only reports on decisions the surrounding code
# already made.
# ---------------------------------------------------------------------------


def _redact_url(url: str) -> str:
    """Best-effort, safe-to-log summary of a candidate media URL for DEBUG
    output: strips userinfo/query/fragment (which can carry tokens/
    signed-URL secrets) and caps length. `worker/observability.py`'s
    `_ERROR_CATEGORY_MAP` comment documents why the *full* URL must never
    reach logs from a failure message; this keeps that same policy for the
    new DEBUG-only success-path logging added here."""
    try:
        parsed = urlparse(url)
        summary = f"{parsed.scheme}://{parsed.hostname or ''}{parsed.path}"
    except ValueError:
        summary = url
    return summary if len(summary) <= 200 else summary[:197] + "..."


def _log_stage_failure(job_id: str, stage: str, exc: Exception) -> None:
    log_event(logger, "stage_failed", level=logging.DEBUG, job_id=job_id, stage=stage, error_type=type(exc).__name__)


def _make_progress_logger(stage: str, job_id: str) -> Callable[[int, int], None]:
    """DEBUG-only per-frame progress reporting for `DINOv2EmbeddingEngine`'s
    optional `on_frame` callback (embedding/dinov2_engine.py) -- logs at
    roughly 10 evenly-spaced checkpoints regardless of segment count, not
    once per frame, so a long target build doesn't flood the log."""

    def _on_frame(index: int, total: int) -> None:
        step = max(1, total // 10)
        if index % step == 0 or index == total:
            log_event(
                logger, "embedding_progress", level=logging.DEBUG,
                job_id=job_id, stage=stage, frame=index, total=total,
                percent=round(100.0 * index / total, 1) if total else None,
            )

    return _on_frame


def _debug_kwargs(on_frame: Optional[Callable[[int, int], None]]) -> dict:
    """`embed_video_segments(..., on_frame=...)` is an additive keyword
    (embedding/dinov2_engine.py) -- only pass it through when a real
    callback exists, so a stand-in engine that doesn't know about
    `on_frame` (used by several tests) is unaffected on the normal,
    non-debug path."""
    return {"on_frame": on_frame} if on_frame is not None else {}


def _round_or_none(value: Optional[float], ndigits: int = 4) -> Optional[float]:
    return round(value, ndigits) if value is not None else None


def _log_matching_debug(
    job: Job, result: TemporalMatchResult, config: MatcherConfig, engine: DINOv2EmbeddingEngine, duration_s: float
) -> None:
    """Logs the match metrics that already exist on `TemporalMatchResult`
    (matching/result.py) -- no new scoring, no new algorithm. Both target-
    coverage variants the observability audit distinguishes are included
    (§7): `target_coverage_hits` (matched_segment_count / total_target_
    segments -- only segments actually part of the winning run) and
    `target_coverage_span` (the run's target-timeline span / total_target_
    segments -- includes gaps `MatcherConfig.max_index_gap` tolerated
    inside the run). Logging one alone, unlabeled, risks being read as "the"
    coverage percentage when the algorithm actually supports two distinct,
    both-correct answers to "how much of the target matched".
    """
    total_target = result.total_target_segments
    total_candidate = result.total_candidate_segments

    target_coverage_hits = result.matched_segment_count / total_target if total_target else None
    candidate_coverage = result.matched_segment_count / total_candidate if total_candidate else None

    target_coverage_span = None
    if result.target_start is not None and result.target_end is not None and total_target:
        segment_duration_s = engine.segment_sampling_config.segment_duration_s
        if segment_duration_s > 0:
            span_segments = (result.target_end - result.target_start) / segment_duration_s
            target_coverage_span = span_segments / total_target

    log_event(
        logger, "matching_completed", level=logging.DEBUG,
        job_id=job.job_id, target_id=job.target_id, target_version=job.target_version,
        target_segment_count=total_target,
        candidate_segment_count=total_candidate,
        matched_segment_count=result.matched_segment_count,
        target_coverage_hits=_round_or_none(target_coverage_hits),
        target_coverage_span=_round_or_none(target_coverage_span),
        candidate_coverage=_round_or_none(candidate_coverage),
        mean_similarity=_round_or_none(result.mean_similarity),
        coarse_similarity=_round_or_none(result.coarse_similarity),
        temporal_offset_s=_round_or_none(result.temporal_offset_s),
        similarity_threshold=config.segment_similarity_threshold,
        min_matched_segments=config.min_matched_segments,
        decision="MATCH" if result.matched else "NO_MATCH",
        duration_s=round(duration_s, 3),
    )


def build_matching_handler(
    acquirer: MediaAcquirer,
    engine: DINOv2EmbeddingEngine,
    registry: TargetRegistry,
    matcher_config: Optional[MatcherConfig] = None,
    stage_recorder: Optional[Callable[[str, float], None]] = None,
    media_store: Optional[SharedTargetMediaStore] = None,
) -> Callable[[Job], Result]:
    """Returns a handler for `Worker.process_claim`/`Worker.run`.

    `engine` is used for *both* the candidate and the target-build path,
    so both sides always share the same `SegmentSamplingConfig` (same
    `segment_duration_s`) — required for the temporal offset model to be
    meaningful at all (phase-09 doc §16's "offset model assumes equal
    segment duration on both sides" limitation), and automatic here rather
    than something a caller could get wrong by passing mismatched configs.

    `stage_recorder` (Phase 13C, optional) is called as
    `stage_recorder(stage_name, duration_s)` after each existing stage
    boundary below (media_acquisition, candidate_embedding,
    target_resolution, matching, aggregation) — a pure instrumentation
    hook, no stage's own logic changes. `target_resolution` times the
    whole cache-lookup-or-build call (`_resolve_target_segments`): the
    registry doesn't report whether it hit cache or built on miss, so
    build time isn't separable from lookup time without modifying
    `target/registry.py` — a known measurement gap, documented rather than
    solved by refactoring. Defaults to a no-op so every existing call site
    (tests, benchmarks) is unaffected.

    `media_store` (Phase 13D, optional) lets a build-on-miss winner fetch a
    target's media from shared storage when it's absent on this host — see
    `_target_artifact`. `None` (the default) leaves target-build behavior
    exactly as before Phase 13D, so every existing call site is unaffected.
    """
    matcher_config = matcher_config or MatcherConfig()
    record_stage = stage_recorder or _noop_stage_recorder

    def handler(job: Job) -> Result:
        started = time.time()
        debug = logger.isEnabledFor(logging.DEBUG)

        if DINOV2_TEMPORAL_TECHNIQUE not in job.techniques:
            raise PermanentFailure(
                f"job {job.job_id!r} requested techniques {job.techniques!r}, "
                f"none of which this worker implements ({DINOV2_TEMPORAL_TECHNIQUE!r})",
                error_type="UnsupportedTechnique",
            )

        if debug:
            log_event(
                logger, "job_processing_started", level=logging.DEBUG,
                job_id=job.job_id, target_id=job.target_id, target_version=job.target_version,
                candidate_url=_redact_url(job.media_url), techniques=list(job.techniques),
            )

        stage_started = time.monotonic()
        try:
            artifact = acquirer.acquire(job.media_url)
        except TransientAcquisitionError as exc:
            record_stage("media_acquisition", time.monotonic() - stage_started)
            if debug:
                _log_stage_failure(job.job_id, "media_acquisition", exc)
            raise TransientFailure(str(exc), error_type=type(exc).__name__) from exc
        except PermanentAcquisitionError as exc:
            record_stage("media_acquisition", time.monotonic() - stage_started)
            if debug:
                _log_stage_failure(job.job_id, "media_acquisition", exc)
            raise PermanentFailure(str(exc), error_type=type(exc).__name__) from exc
        acquisition_duration = time.monotonic() - stage_started
        record_stage("media_acquisition", acquisition_duration)
        if debug:
            log_event(
                logger, "candidate_acquired", level=logging.DEBUG, job_id=job.job_id,
                content_type=artifact.content_type, byte_size=artifact.byte_size,
                duration_s=round(acquisition_duration, 3),
            )

        try:
            stage_started = time.monotonic()
            on_frame = _make_progress_logger("candidate_embedding", job.job_id) if debug else None
            try:
                candidate = engine.embed_video_segments(artifact, **_debug_kwargs(on_frame))
            except (UnsupportedMediaError, InferenceError) as exc:
                record_stage("candidate_embedding", time.monotonic() - stage_started)
                if debug:
                    _log_stage_failure(job.job_id, "candidate_embedding", exc)
                return Result(
                    decision=ResultDecision.PROCESSING_FAILURE,
                    algorithm=DINOV2_TEMPORAL_TECHNIQUE,
                    processing_started_at=started,
                    processing_completed_at=time.time(),
                    summary=f"candidate embedding failed: {exc}",
                )
            candidate_embedding_duration = time.monotonic() - stage_started
            record_stage("candidate_embedding", candidate_embedding_duration)
            if debug:
                log_event(
                    logger, "candidate_embedded", level=logging.DEBUG, job_id=job.job_id,
                    candidate_segment_count=len(candidate.segments),
                    duration_s=round(candidate_embedding_duration, 3),
                )

            stage_started = time.monotonic()
            target_entry = _resolve_target_segments(
                engine, registry, job.target_id, job.target_version, candidate, media_store,
                job_id=job.job_id, debug=debug,
            )
            record_stage("target_resolution", time.monotonic() - stage_started)

            stage_started = time.monotonic()
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
            matching_duration = time.monotonic() - stage_started
            record_stage("matching", matching_duration)
            if debug:
                _log_matching_debug(job, temporal_result, matcher_config, engine, matching_duration)

            stage_started = time.monotonic()
            evidence = temporal_match_to_evidence(temporal_result)
            result = combine([evidence], processing_started_at=started, processing_completed_at=time.time())
            record_stage("aggregation", time.monotonic() - stage_started)
            return result
        finally:
            artifact.cleanup()

    return handler


def _resolve_target_segments(
    engine, registry, target_id, target_version, candidate, media_store=None,
    *, job_id: Optional[str] = None, debug: bool = False,
):
    spec = candidate.to_embedding_spec()

    cache_status = "unknown"
    if debug:
        cache_status = "hit" if registry.has_compatible_segment_embedding(target_id, target_version, spec) else "miss"
        log_event(
            logger, "target_resolution_started", level=logging.DEBUG, job_id=job_id,
            target_id=target_id, target_version=target_version, cache_status=cache_status,
        )

    def build(record):
        artifact, is_temp = _target_artifact(record, media_store)
        on_frame = _make_progress_logger("target_build", job_id) if debug else None
        try:
            target_result = engine.embed_video_segments(artifact, **_debug_kwargs(on_frame))
            return target_result.segments, target_result.coarse_vector
        finally:
            if is_temp:
                artifact.cleanup()

    try:
        entry = registry.get_or_build_segment_embedding(target_id, target_version, spec, build)
    except KeyError as exc:
        if debug:
            _log_stage_failure(job_id, "target_resolution", exc)
        raise PermanentFailure(str(exc), error_type=type(exc).__name__) from exc
    except TimeoutError as exc:
        if debug:
            _log_stage_failure(job_id, "target_resolution", exc)
        raise TransientFailure(str(exc), error_type=type(exc).__name__) from exc
    except UnsupportedMediaError as exc:
        if debug:
            _log_stage_failure(job_id, "target_resolution", exc)
        raise PermanentFailure(
            f"target {target_id!r} version {target_version!r} media is unusable: {exc}",
            error_type=type(exc).__name__,
        ) from exc
    except InferenceError as exc:
        if debug:
            _log_stage_failure(job_id, "target_resolution", exc)
        raise TransientFailure(
            f"target {target_id!r} version {target_version!r} embedding failed: {exc}",
            error_type=type(exc).__name__,
        ) from exc
    except SharedArtifactStoreError as exc:
        # Phase 13D: the shared artifact store (embedding cache and/or
        # target media) was unreachable — an infra blip, not a fact about
        # this job or this target. Retry through the existing job retry
        # machinery rather than treating it as a permanent routing/media
        # problem (audit §11: never silently fall back to a host-local
        # cache and call the result distributed).
        if debug:
            _log_stage_failure(job_id, "target_resolution", exc)
        raise TransientFailure(str(exc), error_type=type(exc).__name__) from exc

    if debug:
        log_event(
            logger, "target_resolved", level=logging.DEBUG, job_id=job_id,
            target_id=target_id, target_version=target_version,
            target_segment_count=len(entry.segments), cache_status=cache_status,
        )
    return entry
