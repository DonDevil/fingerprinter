"""Minimal target registry.

Redis-backed, matching the storage pattern Phases 1-4 already established
for job/result state (`work_queue.state.JobStateStore`,
`work_queue.results.ResultStore`): small hashes under an explicit
namespace, no embedding data. The registry composes a `TargetRegistry`
(target identity/metadata) with an injected `TargetEmbeddingCache`
(vector storage) — the two are independent collaborators, not one system,
so either can be exercised/replaced without the other (see phase-06 docs).

Phase 9 adds an optional, separately-injected `SegmentEmbeddingCache`
(`target/segment_cache.py`) alongside the Phase 6 pooled-vector cache —
same "independent collaborator" pattern, not a replacement. It defaults to
`None` so existing `TargetRegistry(redis_client, cache)` call sites (Phase
6/7) are unaffected; only callers that need segment-level lookups for
matching pass one.
"""
from __future__ import annotations

import json
from typing import Optional

from redis import Redis

from target.cache import EmbeddingCacheEntry, TargetEmbeddingCache
from target.identity import TargetRecord, sha256_file
from target.keys import (
    decode_content_index_member,
    encode_content_index_member,
    target_content_index_key,
    target_embeddings_key,
    target_segment_embeddings_key,
    target_key,
)
from target.segment_cache import SegmentEmbeddingCache, SegmentEmbeddingCacheEntry
from target.versioning import EmbeddingSpec


class TargetRegistry:
    def __init__(
        self,
        redis_client: Redis,
        cache: TargetEmbeddingCache,
        segment_cache: Optional[SegmentEmbeddingCache] = None,
    ):
        self._redis = redis_client
        self._cache = cache
        self._segment_cache = segment_cache

    def register_target(
        self,
        target_id: str,
        target_version: str,
        media_path: str,
        media_metadata: Optional[dict] = None,
    ) -> TargetRecord:
        """Compute content identity from the file's bytes (never its
        filename/path) and upsert the (target_id, target_version) record.
        Re-registering the same (target_id, target_version) preserves the
        original `created_at` and only advances `updated_at`."""
        content_sha256 = sha256_file(media_path)
        existing = self.get_target(target_id, target_version)
        created_at = existing.created_at if existing is not None else None

        record = TargetRecord(
            target_id=target_id,
            target_version=target_version,
            media_path=str(media_path),
            content_sha256=content_sha256,
            media_metadata=media_metadata or {},
            **({"created_at": created_at} if created_at is not None else {}),
        )
        self._redis.hset(target_key(target_id, target_version), mapping=record.to_hash_fields())
        self._redis.sadd(
            target_content_index_key(content_sha256),
            encode_content_index_member(target_id, target_version),
        )
        return record

    def get_target(self, target_id: str, target_version: str) -> Optional[TargetRecord]:
        data = self._redis.hgetall(target_key(target_id, target_version))
        return TargetRecord.from_hash_fields(data) if data else None

    def find_by_content_hash(self, content_sha256: str) -> list[TargetRecord]:
        """All registered (target_id, target_version) pairs whose content is
        byte-identical to `content_sha256`, independent of filename."""
        members = self._redis.smembers(target_content_index_key(content_sha256))
        records = []
        for member in members:
            target_id, target_version = decode_content_index_member(member)
            record = self.get_target(target_id, target_version)
            if record is not None:
                records.append(record)
        return records

    def has_compatible_embedding(self, target_id: str, target_version: str, spec: EmbeddingSpec) -> bool:
        return self.get_compatible_embedding(target_id, target_version, spec) is not None

    def get_compatible_embedding(
        self, target_id: str, target_version: str, spec: EmbeddingSpec
    ) -> Optional[EmbeddingCacheEntry]:
        record = self.get_target(target_id, target_version)
        if record is None:
            return None
        return self._cache.get(target_id, target_version, record.content_sha256, spec)

    def register_embedding(
        self, target_id: str, target_version: str, spec: EmbeddingSpec, vector
    ) -> EmbeddingCacheEntry:
        """Store the vector in the embedding cache and record a small,
        vector-free metadata summary in Redis (what's cached, not the cached
        data) under `target_embeddings_key`."""
        record = self.get_target(target_id, target_version)
        if record is None:
            raise KeyError(f"unknown target: {target_id!r} version {target_version!r}")

        entry = self._cache.put(target_id, target_version, record.content_sha256, spec, vector)

        self._redis.hset(
            target_embeddings_key(target_id, target_version),
            spec.spec_key(),
            json.dumps({**spec.to_metadata_fields(), "cached_at": entry.created_at}, sort_keys=True),
        )
        return entry

    def has_compatible_segment_embedding(self, target_id: str, target_version: str, spec: EmbeddingSpec) -> bool:
        return self.get_compatible_segment_embedding(target_id, target_version, spec) is not None

    def get_compatible_segment_embedding(
        self, target_id: str, target_version: str, spec: EmbeddingSpec
    ) -> Optional[SegmentEmbeddingCacheEntry]:
        """Segment-level counterpart to `get_compatible_embedding`. Returns
        `None` (not an error) if no `segment_cache` was configured — same
        "answer no rather than guess" contract as the underlying cache."""
        if self._segment_cache is None:
            return None
        record = self.get_target(target_id, target_version)
        if record is None:
            return None
        return self._segment_cache.get(target_id, target_version, record.content_sha256, spec)

    def register_segment_embedding(
        self, target_id: str, target_version: str, spec: EmbeddingSpec, segments, coarse_vector
    ) -> SegmentEmbeddingCacheEntry:
        """Store the segment sequence + coarse vector and record a small,
        vector-free metadata summary in Redis, mirroring
        `register_embedding`. Requires a `segment_cache` to have been
        injected at construction time."""
        if self._segment_cache is None:
            raise RuntimeError("TargetRegistry was constructed without a segment_cache; cannot register segments")
        record = self.get_target(target_id, target_version)
        if record is None:
            raise KeyError(f"unknown target: {target_id!r} version {target_version!r}")

        entry = self._segment_cache.put(target_id, target_version, record.content_sha256, spec, segments, coarse_vector)

        self._redis.hset(
            target_segment_embeddings_key(target_id, target_version),
            spec.spec_key(),
            json.dumps(
                {**spec.to_metadata_fields(), "segment_count": len(entry.segments), "cached_at": entry.created_at},
                sort_keys=True,
            ),
        )
        return entry
