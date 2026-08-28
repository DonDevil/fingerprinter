"""Explicit, operator-triggered target segment-embedding build
(`docs/architecture/target-eager-build-audit.md`, Part B).

Root cause this exists to address (audit Part A): the fingerprint worker
builds a target's segment embeddings lazily, on the first job claimed
against it (`worker.matching_handler._resolve_target_segments` ->
`TargetRegistry.get_or_build_segment_embedding`), bounded by
`embedding.frames.DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S` like all other runtime
processing. For a long target (e.g. a full-length movie), that first build
can run past that timeout, failing a live job instead of surfacing the
problem at a controlled time. `build_target()` below runs the same
extraction with no subprocess timeout at all (see `build()`'s
`embed_video_segments(..., timeout=None)` call below) — an intentional,
operator-controlled offline/preprocessing operation is allowed to take as
long as it takes; only runtime worker processing needs to stay bounded.

`build_target()` below is *not* a new build mechanism -- it is the exact
same `TargetRegistry.get_or_build_segment_embedding` call
`_resolve_target_segments` already makes, invoked proactively from an
operator command instead of reactively from a job handler (audit §B.3).
No new registry, cache, or lock logic is introduced; idempotency,
crash-safety, and concurrency-with-a-live-worker are inherited unchanged
from that existing method (audit §B.4.E/H).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

from embedding.result import SEGMENT_EMBEDDING_SCHEMA_VERSION
from target.artifact import target_media_artifact
from target.errors import TargetNotFoundError
from target.registry import TargetRegistry
from target.segment_cache import SegmentEmbeddingCacheEntry
from target.shared_storage import SharedTargetMediaStore
from target.versioning import EmbeddingSpec

if TYPE_CHECKING:
    # Deferred so importing this module never pulls in torch/transformers —
    # matches `target/cli.py`'s own "other subcommands stay torch-free"
    # property (see that module's docstring).
    from embedding.dinov2_engine import DINOv2EmbeddingEngine

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BuildResult:
    """Outcome of `build_target()`.

    `already_built` distinguishes a pure cache hit (no ffmpeg/model work
    performed this call) from a fresh build, so a caller (the CLI) can
    report each distinctly rather than always claiming to have just built
    something. Determined by checking the cache immediately before the
    cache-first `get_or_build_segment_embedding` call; under the ordinary,
    single-operator use this command is for, that check is authoritative.
    In the narrow case of a concurrent build finishing in the gap between
    that check and the call, this may under-report `already_built` for a
    build this call didn't actually perform — a purely cosmetic
    inaccuracy, since `get_or_build_segment_embedding`'s own lock is what
    actually guarantees at most one build happens either way."""

    target_id: str
    target_version: str
    already_built: bool
    entry: SegmentEmbeddingCacheEntry


def _segment_spec_for_engine(engine: "DINOv2EmbeddingEngine") -> EmbeddingSpec:
    """The compatibility key this build will produce, derived entirely from
    the engine's own fixed configuration (audit §B.2/§B.4.A: every field of
    `VideoSegmentEmbeddingResult.to_embedding_spec()` comes from the engine,
    not from anything candidate-specific) -- no candidate/job is needed to
    know what spec an eager build should check for or register under."""
    return EmbeddingSpec(
        model_id=engine.model_id,
        model_version=engine.model_version,
        embedding_schema_version=SEGMENT_EMBEDDING_SCHEMA_VERSION,
        preprocessing_config=engine.preprocessing_config.to_dict(),
        sampling_config=engine.segment_sampling_config.to_dict(),
    )


def build_target(
    registry: TargetRegistry,
    engine: "DINOv2EmbeddingEngine",
    target_id: str,
    target_version: str,
    media_store: Optional[SharedTargetMediaStore] = None,
    on_frame: Optional[Callable[[int, int], None]] = None,
) -> BuildResult:
    """Eagerly build (or confirm already-built) a target's segment
    embeddings, ahead of any live fingerprint job.

    `on_frame` (optional; observability audit, "Progress Display") is
    forwarded as-is to `DINOv2EmbeddingEngine.embed_video_segments` when a
    build actually runs -- see that method's docstring. `None` (the
    default) preserves this function's exact prior behavior and calling
    convention.

    Raises:
        target.errors.TargetNotFoundError: `(target_id, target_version)`
            is not registered.
        embedding.errors.UnsupportedMediaError: the target's media is
            missing, not a video, or undecodable. ffmpeg segment extraction
            itself runs with no subprocess timeout here (unlike the
            worker's lazy build-on-miss path), so an over-long file cannot
            raise this for timing out -- that was the exact failure this
            command exists to surface at an operator-controlled time
            instead of inside a live job (audit Part A).
        embedding.errors.InferenceError: the model failed during a forward
            pass on otherwise-valid frames -- retryable, not a permanent
            fact about the target.
        target.shared_storage.SharedArtifactStoreError: the shared
            artifact store (target media and/or embedding cache) was
            unreachable.
        TimeoutError: another process is already building this exact
            target/spec and did not finish within the poll budget.

    Idempotent (audit §B.4.E): if a compatible segment embedding already
    exists for `(target_id, target_version, content_sha256, spec)`, this
    returns immediately with `already_built=True` and never calls the
    embedding engine or touches the build lock.
    """
    record = registry.get_target(target_id, target_version)
    if record is None:
        raise TargetNotFoundError(f"unknown target: {target_id!r} version {target_version!r}")

    spec = _segment_spec_for_engine(engine)
    already_built = registry.has_compatible_segment_embedding(target_id, target_version, spec)
    logger.debug(
        "target %s/%s: compatible segment embedding %s",
        target_id, target_version, "already cached" if already_built else "not cached, build required",
    )

    def build(record):
        artifact, is_temp = target_media_artifact(record, media_store)
        try:
            frame_kwargs = {"on_frame": on_frame} if on_frame is not None else {}
            # Explicit, operator-triggered preprocessing (module docstring)
            # -- unlike the worker's lazy build-on-miss path, this command
            # deliberately imposes no ffmpeg subprocess timeout, so a
            # full-length target is allowed to take as long as it actually
            # takes to decode rather than failing at
            # embedding.frames.DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S.
            result = engine.embed_video_segments(artifact, timeout=None, **frame_kwargs)
            return result.segments, result.coarse_vector
        finally:
            if is_temp:
                artifact.cleanup()

    started = time.monotonic()
    try:
        entry = registry.get_or_build_segment_embedding(target_id, target_version, spec, build)
    except KeyError as exc:
        # Only reachable if the target was deleted between the get_target()
        # check above and this call -- translate to the same typed error
        # the up-front check would have raised.
        raise TargetNotFoundError(str(exc)) from exc
    logger.debug(
        "target %s/%s: resolved %d segment(s) in %.2fs (%s)",
        target_id, target_version, len(entry.segments), time.monotonic() - started,
        "cache hit" if already_built else "built",
    )

    return BuildResult(
        target_id=target_id, target_version=target_version, already_built=already_built, entry=entry
    )
