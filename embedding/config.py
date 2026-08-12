"""Explicit, serializable configuration for the DINOv2 embedding engine.

Both configs here exist for exactly one reason: Phase 6's `EmbeddingSpec`
(`target/versioning.py`) requires `preprocessing_config` and
`sampling_config` as plain, JSON-serializable mappings so that changing
either invalidates incompatible cached embeddings. `to_dict()` on each is
what feeds `EmbeddingSpec` directly — see `embedding/result.py`.

Nothing here is DINOv2-specific by name (no torch/transformers import), so
a future model-specific engine could reuse the same shapes if its
preprocessing happens to match.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class PreprocessingConfig:
    """Deterministic image preprocessing applied before the model sees a
    frame. Every field here is a value that, if changed, changes the model's
    output — decode-only settings (e.g. which image codec a frame came from)
    deliberately don't belong here, matching Phase 6's
    `EmbeddingSpec.preprocessing_config` guidance.

    Defaults match `facebook/dinov2-base`'s shipped `BitImageProcessor`
    config (resize shortest edge to 256px, center-crop to 224x224, ImageNet
    mean/std normalization) — see phase-07 docs for how these were read off
    the processor rather than guessed.
    """

    resize_shortest_edge: int = 256
    crop_size: int = 224
    color_space: str = "RGB"
    resample: str = "bicubic"
    rescale_factor: float = 1.0 / 255.0
    normalize_mean: Tuple[float, float, float] = (0.485, 0.456, 0.406)
    normalize_std: Tuple[float, float, float] = (0.229, 0.224, 0.225)

    def to_dict(self) -> dict:
        return {
            "resize_shortest_edge": self.resize_shortest_edge,
            "crop_size": self.crop_size,
            "color_space": self.color_space,
            "resample": self.resample,
            "rescale_factor": self.rescale_factor,
            "normalize_mean": list(self.normalize_mean),
            "normalize_std": list(self.normalize_std),
        }


@dataclass(frozen=True)
class SamplingConfig:
    """Deterministic video frame sampling rule.

    Images bypass this entirely (a still image is one frame, no sampling
    decision to make). For video: extract frames at a fixed rate starting at
    t=0, in presentation order, capped at `max_frames` — no scene detection,
    no keyframe-only extraction, nothing content-dependent. Same media +
    same config always yields the same sampled frame set.
    """

    fps: float = 2.0
    max_frames: int = 32
    frame_selection: str = "uniform_time_from_start"
    aggregation: str = "mean_pool_l2_normalized"

    def to_dict(self) -> dict:
        return {
            "fps": self.fps,
            "max_frames": self.max_frames,
            "frame_selection": self.frame_selection,
            "aggregation": self.aggregation,
        }


# A still image has no sampling decisions to make; this constant is the
# `sampling_config` value `EmbeddingResult`/`EmbeddingSpec` record for
# image inputs so "no sampling was applied" is explicit and cacheable,
# never implicit/missing.
IMAGE_SAMPLING_CONFIG = SamplingConfig(fps=0.0, max_frames=1, frame_selection="single_image", aggregation="none")


# Phase 8 concluded (docs/architecture/history/phase-08-video-representation-investigation.md,
# "Recommended sampling strategy") that `SamplingConfig` above — fixed fps,
# hard-capped `max_frames`, always starting at t=0 — cannot support
# temporal/partial-clip matching: for any video longer than
# `max_frames / fps` seconds it only ever looks at a fixed opening window.
# `SegmentSamplingConfig` is the Phase 9 replacement *for the matching
# path*: uniform sampling across the *entire* video, chunked into
# fixed-duration segments, one representative frame per segment. It is a
# new, additive config (not a change to `SamplingConfig`) so Phase 7's
# existing pooled-vector contract/cache keys are untouched — see
# docs/architecture/phase-09-temporal-video-matching.md, "Segment
# representation decision".
@dataclass(frozen=True)
class SegmentSamplingConfig:
    """Deterministic segment-level video sampling rule.

    `segment_duration_s` is the ARCHITECTURAL knob Phase 8 deferred
    ("exact segment duration"). The default below is a PROVISIONAL
    heuristic (no labeled dataset exists yet to calibrate against the
    shortest pirated clip length worth detecting) — see the phase-09 doc
    for the full justification. Callers needing a different tradeoff
    should pass an explicit value rather than relying on the default.

    `frame_selection="segment_start"`: one frame is sampled at the start
    of each `segment_duration_s` window (via `ffmpeg -vf
    fps=1/segment_duration_s`, which samples from t=0 in presentation
    order with no frame cap — see `embedding/frames.py:extract_segment_frames`).
    This is intra-segment sampling density = 1 (a single representative
    frame per segment, not a multi-frame mean-pool within the segment) —
    the simplest option Phase 8 left open ("Deferred decisions"), chosen
    for architectural simplicity, not because it was measured against the
    mean-pool-within-segment alternative.
    """

    segment_duration_s: float = 5.0
    frame_selection: str = "segment_start"

    def to_dict(self) -> dict:
        return {
            "segment_duration_s": self.segment_duration_s,
            "frame_selection": self.frame_selection,
        }


# Provisional default segment duration in seconds. Exposed as a module
# constant (not just the dataclass default) so callers/tests/docs can
# reference "the current provisional default" by name instead of a bare
# literal. NOT empirically validated — see phase-09 doc, "Threshold
# status".
DEFAULT_SEGMENT_DURATION_S = 5.0
