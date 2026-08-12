"""Segment-level embedding cache — the Phase 9 storage type.

`TargetEmbeddingCache` (`target/cache.py`, Phase 6) is built around exactly
one vector per `(target_id, target_version, content_sha256, spec)`:
`EmbeddingCacheEntry.vector: tuple`, one JSON file per representation. Phase
8's investigation ("Implications for Phase 6 cache design") concluded this
shape cannot be extended to hold N segment vectors without breaking its own
contract — the field is singular by design (`FilesystemEmbeddingCache`
overwrites, never appends). Reusing it for segment storage would mean
either silently repurposing `vector` to hold a flattened/concatenated blob
(makes `EmbeddingCacheEntry.vector` mean two different things depending on
which spec produced it — the exact kind of implicit-meaning-shift Phase 6
explicitly avoided) or calling `put()` once per segment (reintroduces the
"54,000 separate files" problem Phase 8 measured for a 100-target library).

`SegmentEmbeddingCache` is therefore a new, parallel interface: one cache
entry, one file, holding the *entire* segment sequence (list of
`SegmentEmbedding`) plus the derived coarse vector for one
`(target_id, target_version, content_sha256, spec)`. It deliberately reuses
`target.versioning.EmbeddingSpec`/`cache_entry_key` unchanged — those are
already generic over `sampling_config: Mapping`, so
`SegmentSamplingConfig.to_dict()` plugs in directly. Only the *storage
shape* needed to change, not the compatibility-key machinery.

Same JSON-file-per-entry format as `target/cache.py` (not the
`.npy`/columnar format Phase 8 floated) — this phase's target-library scale
(see docs/architecture/phase-09-temporal-video-matching.md, "Complexity")
does not yet justify a binary format; one JSON file per target holding all
its segments already solves the file-count problem Phase 8 flagged (N
segments per target, not N files), which is the part that mattered at this
scale. Revisit if/when Phase 11 benchmarks show JSON (de)serialization cost
dominates.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Union

from embedding.result import SegmentEmbedding
from target.versioning import EmbeddingSpec, cache_entry_key

SEGMENT_CACHE_ENTRY_SCHEMA_VERSION = 1

_REQUIRED_ENTRY_FIELDS = (
    "segment_cache_entry_schema_version",
    "target_id",
    "target_version",
    "content_sha256",
    "model_id",
    "model_version",
    "embedding_schema_version",
    "preprocessing_config",
    "sampling_config",
    "segments",
    "coarse_vector",
    "created_at",
)


@dataclass(frozen=True)
class SegmentEmbeddingCacheEntry:
    target_id: str
    target_version: str
    content_sha256: str
    spec: EmbeddingSpec
    segments: tuple
    coarse_vector: tuple
    created_at: float


class SegmentEmbeddingCache(ABC):
    @abstractmethod
    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        """Return the cached entry iff it exists and every compatibility
        dimension matches exactly. `None` on miss, including corruption."""

    @abstractmethod
    def put(
        self,
        target_id: str,
        target_version: str,
        content_sha256: str,
        spec: EmbeddingSpec,
        segments: Sequence[SegmentEmbedding],
        coarse_vector: Sequence[float],
    ) -> SegmentEmbeddingCacheEntry:
        """Store (or overwrite) the full segment sequence + coarse vector
        for this exact representation, as one entry."""

    @abstractmethod
    def exists(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> bool:
        """Cheaper existence check than get() where a backend can offer one."""


class FilesystemSegmentEmbeddingCache(SegmentEmbeddingCache):
    """One JSON file per cached representation, named by
    `versioning.cache_entry_key(...)` — same key derivation as
    `FilesystemEmbeddingCache`, different (segment-sequence) payload shape.
    Development/single-host backend, see module docstring."""

    def __init__(self, cache_dir: Union[str, Path]):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> Path:
        key = cache_entry_key(target_id, target_version, content_sha256, spec)
        return self._cache_dir / f"{key}.segments.json"

    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        path = self._path_for(target_id, target_version, content_sha256, spec)
        return self._load_and_validate(path, target_id, target_version, content_sha256, spec)

    def exists(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> bool:
        return self.get(target_id, target_version, content_sha256, spec) is not None

    def put(
        self,
        target_id: str,
        target_version: str,
        content_sha256: str,
        spec: EmbeddingSpec,
        segments: Sequence[SegmentEmbedding],
        coarse_vector: Sequence[float],
    ) -> SegmentEmbeddingCacheEntry:
        segments = tuple(segments)
        coarse_vector = tuple(float(x) for x in coarse_vector)
        created_at = time.time()
        payload = {
            "segment_cache_entry_schema_version": SEGMENT_CACHE_ENTRY_SCHEMA_VERSION,
            "target_id": target_id,
            "target_version": target_version,
            "content_sha256": content_sha256,
            **spec.to_metadata_fields(),
            "segments": [
                {
                    "segment_index": s.segment_index,
                    "start_time": s.start_time,
                    "end_time": s.end_time,
                    "vector": list(s.vector),
                }
                for s in segments
            ],
            "coarse_vector": list(coarse_vector),
            "created_at": created_at,
        }
        path = self._path_for(target_id, target_version, content_sha256, spec)
        self._atomic_write(path, payload)
        return SegmentEmbeddingCacheEntry(
            target_id=target_id,
            target_version=target_version,
            content_sha256=content_sha256,
            spec=spec,
            segments=segments,
            coarse_vector=coarse_vector,
            created_at=created_at,
        )

    @staticmethod
    def _atomic_write(path: Path, payload: dict) -> None:
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, prefix=".tmp-", suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f)
            os.replace(tmp_path, path)
        except BaseException:
            Path(tmp_path).unlink(missing_ok=True)
            raise

    @staticmethod
    def _load_and_validate(
        path: Path, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        if not path.exists():
            return None
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        if any(field not in data for field in _REQUIRED_ENTRY_FIELDS):
            return None
        if data["segment_cache_entry_schema_version"] != SEGMENT_CACHE_ENTRY_SCHEMA_VERSION:
            return None
        if (
            data["target_id"] != target_id
            or data["target_version"] != target_version
            or data["content_sha256"] != content_sha256
            or data["model_id"] != spec.model_id
            or data["model_version"] != spec.model_version
            or data["embedding_schema_version"] != spec.embedding_schema_version
            or data["preprocessing_config"] != dict(spec.preprocessing_config)
            or data["sampling_config"] != dict(spec.sampling_config)
        ):
            return None
        if not isinstance(data["segments"], list) or not data["segments"]:
            return None
        if not isinstance(data["coarse_vector"], list) or not data["coarse_vector"]:
            return None

        try:
            segments = tuple(
                SegmentEmbedding(
                    segment_index=s["segment_index"],
                    start_time=s["start_time"],
                    end_time=s["end_time"],
                    vector=tuple(s["vector"]),
                )
                for s in data["segments"]
            )
        except (KeyError, TypeError, ValueError):
            return None

        return SegmentEmbeddingCacheEntry(
            target_id=data["target_id"],
            target_version=data["target_version"],
            content_sha256=data["content_sha256"],
            spec=spec,
            segments=segments,
            coarse_vector=tuple(data["coarse_vector"]),
            created_at=data["created_at"],
        )
