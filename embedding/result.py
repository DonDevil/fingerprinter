"""The typed output of a DINOv2 embedding operation.

`EmbeddingResult` is the sole contract between `DINOv2EmbeddingEngine` and
everything downstream — most notably Phase 6's `TargetEmbeddingCache`,
which it converts into via `to_embedding_spec()`/`.vector`. It carries
exactly enough to reconstruct *how* the vector was produced (model,
preprocessing, sampling) so a later mismatch is detectable as a cache miss
rather than a silent wrong-shape comparison.

`target.versioning.EmbeddingSpec` is imported lazily, inside
`to_embedding_spec()`, rather than at module level: `target/__init__.py`
(Phase 6) eagerly imports `target.registry`, which depends on `redis`.
Deferring the import means simply constructing/using
`DINOv2EmbeddingEngine`/`EmbeddingResult` never pulls in `redis` — only a
caller that actually bridges to Phase 6's cache does, at the point it does
so. This is what keeps the engine itself Redis-independent, per the phase
brief, despite living one call away from a package that isn't.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Tuple

from embedding.config import PreprocessingConfig, SamplingConfig, SegmentSamplingConfig

if TYPE_CHECKING:
    from target.versioning import EmbeddingSpec

EMBEDDING_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EmbeddingResult:
    """One embedding representation of one `MediaArtifact`.

    - `vector` — the single representation to hand to
      `TargetEmbeddingCache.put()`. For an image, the model's direct output.
      For video, the aggregated (mean-pooled, then re-normalized if
      `normalized`) representation across sampled frames — see
      `aggregation_method` and phase-07 docs for exactly how.
    - `frame_vectors` — the per-frame vectors that produced `vector`, kept
      for diagnostics only. `None` for images (`frame_vectors == (vector,)`
      would just duplicate `vector`). Never sent to the cache — Phase 6's
      cache API stores one vector per representation, not a frame list.
    - `model_id` / `model_version` — which model/checkpoint produced this.
    - `embedding_schema_version` — bumped if the *meaning* of `vector`
      changes (e.g. pooling strategy), independent of model/preprocessing.
    - `dimensionality` — `len(vector)`, stored explicitly so a consumer can
      sanity-check without materializing the vector.
    - `normalized` — whether `vector` is L2-unit-norm.
    """

    vector: Tuple[float, ...]
    model_id: str
    model_version: str
    dimensionality: int
    normalized: bool
    preprocessing_config: PreprocessingConfig
    sampling_config: SamplingConfig
    media_type: str  # "image" or "video"
    frame_count: int
    aggregation_method: str
    embedding_schema_version: int = EMBEDDING_SCHEMA_VERSION
    frame_vectors: Optional[Tuple[Tuple[float, ...], ...]] = None
    inference_duration_s: float = 0.0

    def to_embedding_spec(self) -> "EmbeddingSpec":
        """The Phase 6 compatibility key for this exact representation.
        Cache reuse requires every one of these fields to match — see
        `target/versioning.py`."""
        from target.versioning import EmbeddingSpec

        return EmbeddingSpec(
            model_id=self.model_id,
            model_version=self.model_version,
            embedding_schema_version=self.embedding_schema_version,
            preprocessing_config=self.preprocessing_config.to_dict(),
            sampling_config=self.sampling_config.to_dict(),
        )


# Bumped independently of EMBEDDING_SCHEMA_VERSION: this is the meaning of
# *segment-level* output (one vector per fixed-duration window plus
# timing), a different interpretation than the single pooled `vector` the
# schema version above governs. See phase-09 doc, "Segment representation
# decision".
SEGMENT_EMBEDDING_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SegmentEmbedding:
    """One fixed-duration window's embedding — the Phase 9 fine-layer unit
    (docs/architecture/history/phase-08-video-representation-investigation.md,
    "Recommended representation", strategies C+E). `segment_index` is the
    0-based position in presentation order; `start_time`/`end_time` are
    seconds from the start of the source video. Never collapsed into a
    single pooled vector before matching — that's exactly what Phase 8
    concluded a matcher cannot be built on top of.
    """

    segment_index: int
    start_time: float
    end_time: float
    vector: Tuple[float, ...]

    def __post_init__(self) -> None:
        if self.segment_index < 0:
            raise ValueError(f"segment_index must be >= 0, got {self.segment_index}")
        if self.end_time < self.start_time:
            raise ValueError(f"segment end_time ({self.end_time}) < start_time ({self.start_time})")
        if not self.vector:
            raise ValueError("segment vector must not be empty")


@dataclass(frozen=True)
class VideoSegmentEmbeddingResult:
    """The full-duration, segment-level representation of one video
    `MediaArtifact` — the Phase 9 counterpart to `EmbeddingResult` for the
    matching path. `segments` is the primary (fine-layer) representation;
    `coarse_vector` is a mean-pool of `segments` (not a separate inference
    pass — see `DINOv2EmbeddingEngine.embed_video_segments`), retained only
    as a cheap screening signature per Phase 8's "coarse layer" conclusion.
    """

    segments: Tuple[SegmentEmbedding, ...]
    coarse_vector: Tuple[float, ...]
    model_id: str
    model_version: str
    dimensionality: int
    normalized: bool
    preprocessing_config: PreprocessingConfig
    segment_sampling_config: SegmentSamplingConfig
    total_duration_s: float
    embedding_schema_version: int = SEGMENT_EMBEDDING_SCHEMA_VERSION
    inference_duration_s: float = 0.0

    @property
    def segment_count(self) -> int:
        return len(self.segments)

    def to_embedding_spec(self) -> "EmbeddingSpec":
        """Compatibility key for this segment-level representation, reusing
        Phase 6's `EmbeddingSpec` shape (it is generic over
        `sampling_config: Mapping`, so `SegmentSamplingConfig.to_dict()`
        plugs in directly — no new versioning machinery needed, only a new
        storage shape; see `target/segment_cache.py`)."""
        from target.versioning import EmbeddingSpec

        return EmbeddingSpec(
            model_id=self.model_id,
            model_version=self.model_version,
            embedding_schema_version=self.embedding_schema_version,
            preprocessing_config=self.preprocessing_config.to_dict(),
            sampling_config=self.segment_sampling_config.to_dict(),
        )
