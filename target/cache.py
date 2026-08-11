"""Embedding cache abstraction.

Storage boundary (per the phase brief): Redis is the job/state/coordination
backend used throughout Phases 1-5 — small hashes, streams, ZSETs. It is
*not* the place for embedding vectors. `TargetEmbeddingCache` is a plain
Python interface with no Redis dependency; `FilesystemEmbeddingCache` is the
one implementation this phase ships, storing one small JSON file per cached
representation under a local directory. A later phase can add an
object/shared-storage-backed implementation of the same interface without
touching callers (`TargetRegistry`, and eventually the fingerprint worker) —
see `docs/architecture/history/phase-06-target-management-cache.md`.

The cache answers exactly one question per entry: "can this exact target
representation be reused?" That requires every compatibility dimension in
`target/versioning.py` (content hash, model identity/version, embedding
schema version, preprocessing config, sampling config) to match — get()
returns `None` for anything less than an exact match, including a corrupted
or partially-written entry (see `_load` below): a cache that can't fully
validate an entry cannot answer "yes," so it answers "no" and lets the
caller recompute, rather than guessing.
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

from target.versioning import EmbeddingSpec, cache_entry_key

CACHE_ENTRY_SCHEMA_VERSION = 1

_REQUIRED_ENTRY_FIELDS = (
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


@dataclass(frozen=True)
class EmbeddingCacheEntry:
    target_id: str
    target_version: str
    content_sha256: str
    spec: EmbeddingSpec
    vector: tuple
    created_at: float


class TargetEmbeddingCache(ABC):
    @abstractmethod
    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[EmbeddingCacheEntry]:
        """Return the cached entry iff it exists and every compatibility
        dimension matches exactly. `None` on miss, including corruption."""

    @abstractmethod
    def put(
        self,
        target_id: str,
        target_version: str,
        content_sha256: str,
        spec: EmbeddingSpec,
        vector: Sequence[float],
    ) -> EmbeddingCacheEntry:
        """Store (or overwrite) the vector for this exact representation."""

    @abstractmethod
    def exists(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> bool:
        """Cheaper existence check than get() where a backend can offer one."""


class FilesystemEmbeddingCache(TargetEmbeddingCache):
    """One JSON file per cached representation, named by
    `versioning.cache_entry_key(...)`. Development/single-host backend —
    see module docstring for the storage-boundary rationale."""

    def __init__(self, cache_dir: Union[str, Path]):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> Path:
        key = cache_entry_key(target_id, target_version, content_sha256, spec)
        return self._cache_dir / f"{key}.json"

    def get(
        self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec
    ) -> Optional[EmbeddingCacheEntry]:
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
        path = self._path_for(target_id, target_version, content_sha256, spec)
        self._atomic_write(path, payload)
        return EmbeddingCacheEntry(
            target_id=target_id,
            target_version=target_version,
            content_sha256=content_sha256,
            spec=spec,
            vector=vector,
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
    ) -> Optional[EmbeddingCacheEntry]:
        if not path.exists():
            return None
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        if any(field not in data for field in _REQUIRED_ENTRY_FIELDS):
            return None
        if data["cache_entry_schema_version"] != CACHE_ENTRY_SCHEMA_VERSION:
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
        if not isinstance(data["vector"], list) or not data["vector"]:
            return None

        return EmbeddingCacheEntry(
            target_id=data["target_id"],
            target_version=data["target_version"],
            content_sha256=data["content_sha256"],
            spec=spec,
            vector=tuple(data["vector"]),
            created_at=data["created_at"],
        )
