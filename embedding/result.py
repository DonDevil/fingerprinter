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

from embedding.config import PreprocessingConfig, SamplingConfig

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
