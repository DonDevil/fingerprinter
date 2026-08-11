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
