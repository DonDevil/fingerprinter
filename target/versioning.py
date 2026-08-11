"""Embedding compatibility semantics.

An embedding computed for a target is only safe to reuse when every
dimension that could change the stored vector still matches. Per the phase
brief, that is exactly:

- `content_sha256` of the target media (the bytes actually embedded) —
  lives on `TargetRecord`, not `EmbeddingSpec`, but is threaded through
  every cache operation (see `target/cache.py`) as a required match.
- `model_id` / `model_version` — which model produced the vector.
- `embedding_schema_version` — the shape/meaning of the stored vector
  itself (e.g. pooling strategy, dimensionality convention). Bumped when
  the *interpretation* of the stored numbers changes even if the model
  didn't.
- `preprocessing_config` — anything applied to the media before the model
  sees it that can change the output (resize, crop, normalization, color
  space). Only included if it affects the embedding; decode-only settings
  (e.g. container demux options) don't belong here.
- `sampling_config` — anything about *which* frames/segments were sampled
  that can change the stored representation (fps, frame count, clip
  selection strategy).

No other fields are invented here. In particular there is deliberately no
"embedding version" separate from `embedding_schema_version` — one field
already captures "does the stored representation still mean the same
thing," and a second would just be an unjustified duplicate.

Two `EmbeddingSpec`s are compatible iff every field above compares equal.
`spec_key()` gives a stable, deterministic identifier for exactly that
comparison — same fields in, same key out, regardless of dict insertion
order — used by the cache to derive a lookup/storage key.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class EmbeddingSpec:
    model_id: str
    model_version: str
    embedding_schema_version: int
    preprocessing_config: Mapping[str, object] = field(default_factory=dict)
    sampling_config: Mapping[str, object] = field(default_factory=dict)

    def spec_key(self) -> str:
        payload = json.dumps(
            {
                "model_id": self.model_id,
                "model_version": self.model_version,
                "embedding_schema_version": self.embedding_schema_version,
                "preprocessing_config": dict(self.preprocessing_config),
                "sampling_config": dict(self.sampling_config),
            },
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def to_metadata_fields(self) -> dict[str, object]:
        """Small, Redis-safe summary of this spec — no vector data. Used by
        the registry to record *what* is cached without duplicating the
        cache's own storage of the vector itself."""
        return {
            "model_id": self.model_id,
            "model_version": self.model_version,
            "embedding_schema_version": self.embedding_schema_version,
            "preprocessing_config": dict(self.preprocessing_config),
            "sampling_config": dict(self.sampling_config),
        }


def cache_entry_key(target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> str:
    """The full identity of one cached vector: target identity + content +
    embedding spec. This is "the exact target representation" the phase
    brief's cache abstraction must be able to answer reuse questions about."""
    payload = json.dumps(
        {
            "target_id": target_id,
            "target_version": target_version,
            "content_sha256": content_sha256,
            "spec_key": spec.spec_key(),
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
