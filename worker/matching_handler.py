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

import mimetypes
import time
from pathlib import Path
from typing import Callable, Optional

from acquisition import MediaAcquirer, PermanentAcquisitionError, TransientAcquisitionError
from acquisition.artifact import MediaArtifact
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from embedding.errors import InferenceError, UnsupportedMediaError
from matching.aggregation import DINOV2_TEMPORAL_TECHNIQUE, combine, temporal_match_to_evidence
from matching.config import MatcherConfig
from matching.matcher import match_segments
from target.identity import TargetRecord
from target.registry import TargetRegistry
from work_queue.jobs import Job
from work_queue.results import Result, ResultDecision
from worker.fingerprint_worker import PermanentFailure, TransientFailure


def _target_artifact(record: TargetRecord) -> MediaArtifact:
    """Wraps a registered target's on-disk media as a `MediaArtifact` so
    the same `DINOv2EmbeddingEngine.embed_video_segments` call path used
    for candidates also embeds targets — one embedding code path, not two.
    `content_type` is guessed from the file extension (targets don't carry
    a stored content_type, unlike an acquired `MediaArtifact`); this
    handler only supports video targets, so anything that doesn't guess to
    `video/*` will correctly fail `embed_video_segments`'s own check."""
    content_type = mimetypes.guess_type(record.media_path)[0] or "video/mp4"
    return MediaArtifact(
        local_path=Path(record.media_path),
        original_url=f"local-target://{record.target_id}/{record.target_version}",
        final_url=f"local-target://{record.target_id}/{record.target_version}",
        content_type=content_type,
        byte_size=0,
        checksum_sha256=record.content_sha256,
        acquisition_duration_s=0.0,
    )


def _noop_stage_recorder(stage: str, duration_s: float) -> None:
    pass


def build_matching_handler(
    acquirer: MediaAcquirer,
    engine: DINOv2EmbeddingEngine,
    registry: TargetRegistry,
    matcher_config: Optional[MatcherConfig] = None,
    stage_recorder: Optional[Callable[[str, float], None]] = None,
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
    """
    matcher_config = matcher_config or MatcherConfig()
    record_stage = stage_recorder or _noop_stage_recorder

    def handler(job: Job) -> Result:
        started = time.time()

        if DINOV2_TEMPORAL_TECHNIQUE not in job.techniques:
            raise PermanentFailure(
                f"job {job.job_id!r} requested techniques {job.techniques!r}, "
                f"none of which this worker implements ({DINOV2_TEMPORAL_TECHNIQUE!r})",
                error_type="UnsupportedTechnique",
            )

        stage_started = time.monotonic()
        try:
            artifact = acquirer.acquire(job.media_url)
        except TransientAcquisitionError as exc:
            record_stage("media_acquisition", time.monotonic() - stage_started)
            raise TransientFailure(str(exc), error_type=type(exc).__name__) from exc
        except PermanentAcquisitionError as exc:
            record_stage("media_acquisition", time.monotonic() - stage_started)
            raise PermanentFailure(str(exc), error_type=type(exc).__name__) from exc
        record_stage("media_acquisition", time.monotonic() - stage_started)

        try:
            stage_started = time.monotonic()
            try:
                candidate = engine.embed_video_segments(artifact)
            except (UnsupportedMediaError, InferenceError) as exc:
                record_stage("candidate_embedding", time.monotonic() - stage_started)
                return Result(
                    decision=ResultDecision.PROCESSING_FAILURE,
                    algorithm=DINOV2_TEMPORAL_TECHNIQUE,
                    processing_started_at=started,
                    processing_completed_at=time.time(),
                    summary=f"candidate embedding failed: {exc}",
                )
            record_stage("candidate_embedding", time.monotonic() - stage_started)

            stage_started = time.monotonic()
            target_entry = _resolve_target_segments(engine, registry, job.target_id, job.target_version, candidate)
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
            record_stage("matching", time.monotonic() - stage_started)

            stage_started = time.monotonic()
            evidence = temporal_match_to_evidence(temporal_result)
            result = combine([evidence], processing_started_at=started, processing_completed_at=time.time())
            record_stage("aggregation", time.monotonic() - stage_started)
            return result
        finally:
            artifact.cleanup()

    return handler


def _resolve_target_segments(engine, registry, target_id, target_version, candidate):
    spec = candidate.to_embedding_spec()

    def build(record):
        target_result = engine.embed_video_segments(_target_artifact(record))
        return target_result.segments, target_result.coarse_vector

    try:
        return registry.get_or_build_segment_embedding(target_id, target_version, spec, build)
    except KeyError as exc:
        raise PermanentFailure(str(exc), error_type=type(exc).__name__) from exc
    except TimeoutError as exc:
        raise TransientFailure(str(exc), error_type=type(exc).__name__) from exc
    except UnsupportedMediaError as exc:
        raise PermanentFailure(
            f"target {target_id!r} version {target_version!r} media is unusable: {exc}",
            error_type=type(exc).__name__,
        ) from exc
    except InferenceError as exc:
        raise TransientFailure(
            f"target {target_id!r} version {target_version!r} embedding failed: {exc}",
            error_type=type(exc).__name__,
        ) from exc
