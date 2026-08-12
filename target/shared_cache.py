"""Phase 13D -- shared-storage-backed `TargetEmbeddingCache`/
`SegmentEmbeddingCache` implementations.

Per the Phase 13D audit's recommendation (§12) and the completion brief's
non-negotiable constraint: this does not redesign `cache_entry_key()`,
`TargetEmbeddingCache`, or `SegmentEmbeddingCache` (`target/cache.py`,
`target/segment_cache.py`, `target/versioning.py`, all unchanged). It
implements a new backend behind those same interfaces -- `TargetRegistry`
never references a filesystem/shared-storage class by name, only the two
ABCs (confirmed by the audit, §12) -- so only `worker/main.py`'s
`build_registry()` needs to change to wire this in.

Payload shape (JSON schema, required-fields validation, exact-match
compatibility re-check) is intentionally identical to
`FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` -- this is a
storage-location change, not a format change. `_load_and_validate` is
duplicated rather than imported from `target/cache.py`/`target/
segment_cache.py`: those two modules already duplicate this same shape
between themselves (Phase 9's own design choice, see `segment_cache.py`'s
module docstring), so this follows the codebase's existing pattern rather
than reaching into another module's private helpers.
"""
from __future__ import annotations

import json
import time
from typing import Optional, Sequence

from embedding.result import SegmentEmbedding
from target.cache import CACHE_ENTRY_SCHEMA_VERSION, EmbeddingCacheEntry, TargetEmbeddingCache
from target.segment_cache import (
    SEGMENT_CACHE_ENTRY_SCHEMA_VERSION,
    SegmentEmbeddingCache,
    SegmentEmbeddingCacheEntry,
)
from target.shared_storage import SharedArtifactStore
from target.versioning import EmbeddingSpec, cache_entry_key

_POOLED_ENTRY_FIELDS = (
    "cache_entry_schema_version",
    "target_id",
    "target_version",
    "content_sha256",
    "model_id",
    "model_version",
    "embedding_schema_version",
    "preprocessing_config",
    "sampling_config",
    "vector",
    "created_at",
)

_SEGMENT_ENTRY_FIELDS = (
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


class SharedFilesystemEmbeddingCache(TargetEmbeddingCache):
    """`TargetEmbeddingCache` over a `SharedArtifactStore` -- same one
    vector-per-entry shape as `FilesystemEmbeddingCache`, but the store is
    expected to be a genuinely shared mount (see `target/shared_storage.py`
    module docstring), so a build on host A is visible to a cache read on
    host B."""

    def __init__(self, store: SharedArtifactStore, prefix: str = "pooled"):
        self._store = store
        self._prefix = prefix

    def _key(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> str:
        key = cache_entry_key(target_id, target_version, content_sha256, spec)
        return f"{self._prefix}/{key}.json"

    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[EmbeddingCacheEntry]:
        data = self._store.get_bytes(self._key(target_id, target_version, content_sha256, spec))
        if data is None:
            return None
        return self._validate(data, target_id, target_version, content_sha256, spec)

    def exists(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> bool:
        return self.get(target_id, target_version, content_sha256, spec) is not None

    def put(
        self,
        target_id: str,
        target_version: str,
        content_sha256: str,
        spec: EmbeddingSpec,
        vector: Sequence[float],
    ) -> EmbeddingCacheEntry:
        vector = tuple(float(x) for x in vector)
        created_at = time.time()
        payload = {
            "cache_entry_schema_version": CACHE_ENTRY_SCHEMA_VERSION,
            "target_id": target_id,
            "target_version": target_version,
            "content_sha256": content_sha256,
            **spec.to_metadata_fields(),
            "vector": list(vector),
            "created_at": created_at,
        }
        self._store.put_bytes(
            self._key(target_id, target_version, content_sha256, spec),
            json.dumps(payload).encode("utf-8"),
        )
        return EmbeddingCacheEntry(
            target_id=target_id,
            target_version=target_version,
            content_sha256=content_sha256,
            spec=spec,
            vector=vector,
            created_at=created_at,
        )

    @staticmethod
    def _validate(
        data: bytes, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[EmbeddingCacheEntry]:
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return None

        if any(field not in parsed for field in _POOLED_ENTRY_FIELDS):
            return None
        if parsed["cache_entry_schema_version"] != CACHE_ENTRY_SCHEMA_VERSION:
            return None
        if (
            parsed["target_id"] != target_id
            or parsed["target_version"] != target_version
            or parsed["content_sha256"] != content_sha256
            or parsed["model_id"] != spec.model_id
            or parsed["model_version"] != spec.model_version
            or parsed["embedding_schema_version"] != spec.embedding_schema_version
            or parsed["preprocessing_config"] != dict(spec.preprocessing_config)
            or parsed["sampling_config"] != dict(spec.sampling_config)
        ):
            return None
        if not isinstance(parsed["vector"], list) or not parsed["vector"]:
            return None

        return EmbeddingCacheEntry(
            target_id=parsed["target_id"],
            target_version=parsed["target_version"],
            content_sha256=parsed["content_sha256"],
            spec=spec,
            vector=tuple(parsed["vector"]),
            created_at=parsed["created_at"],
        )


class SharedFilesystemSegmentEmbeddingCache(SegmentEmbeddingCache):
    """`SegmentEmbeddingCache` over a `SharedArtifactStore` -- the backend
    that matters in production (audit §1: the temporal-matching pipeline
    uses this cache exclusively, not the pooled one)."""

    def __init__(self, store: SharedArtifactStore, prefix: str = "segments"):
        self._store = store
        self._prefix = prefix

    def _key(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> str:
        key = cache_entry_key(target_id, target_version, content_sha256, spec)
        return f"{self._prefix}/{key}.segments.json"

    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        data = self._store.get_bytes(self._key(target_id, target_version, content_sha256, spec))
        if data is None:
            return None
        return self._validate(data, target_id, target_version, content_sha256, spec)

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
        self._store.put_bytes(
            self._key(target_id, target_version, content_sha256, spec),
            json.dumps(payload).encode("utf-8"),
        )
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
    def _validate(
        data: bytes, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            return None

        if any(field not in parsed for field in _SEGMENT_ENTRY_FIELDS):
            return None
        if parsed["segment_cache_entry_schema_version"] != SEGMENT_CACHE_ENTRY_SCHEMA_VERSION:
            return None
        if (
            parsed["target_id"] != target_id
            or parsed["target_version"] != target_version
            or parsed["content_sha256"] != content_sha256
            or parsed["model_id"] != spec.model_id
            or parsed["model_version"] != spec.model_version
            or parsed["embedding_schema_version"] != spec.embedding_schema_version
            or parsed["preprocessing_config"] != dict(spec.preprocessing_config)
            or parsed["sampling_config"] != dict(spec.sampling_config)
        ):
            return None
        if not isinstance(parsed["segments"], list) or not parsed["segments"]:
            return None
        if not isinstance(parsed["coarse_vector"], list) or not parsed["coarse_vector"]:
            return None

        try:
            segments = tuple(
                SegmentEmbedding(
                    segment_index=s["segment_index"],
                    start_time=s["start_time"],
                    end_time=s["end_time"],
                    vector=tuple(s["vector"]),
                )
                for s in parsed["segments"]
            )
        except (KeyError, TypeError, ValueError):
            return None

        return SegmentEmbeddingCacheEntry(
            target_id=parsed["target_id"],
            target_version=parsed["target_version"],
            content_sha256=parsed["content_sha256"],
            spec=spec,
            segments=segments,
            coarse_vector=tuple(parsed["coarse_vector"]),
            created_at=parsed["created_at"],
        )
