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

Phase 10 adds `get_or_build_segment_embedding`: the cache-first,
build-on-miss-under-lock resolution `docs/design/design-proposal-1.md` §8
describes and phase-09's own doc left unwired (see its §16/§18). It takes
a `build` callback rather than an embedding engine so this module still
never imports torch/transformers — the caller (a worker handler) owns
"how to embed," this module only owns "is it cached, and who gets to
build it if not."
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence, Tuple

from redis import Redis

from target.cache import EmbeddingCacheEntry, TargetEmbeddingCache
from target.errors import TargetAlreadyExistsError, TargetLockTimeoutError, TargetNotFoundError
from target.identity import TargetRecord, sha256_file
from target.keys import (
    decode_content_index_member,
    encode_content_index_member,
    target_content_index_key,
    target_embeddings_key,
    target_index_key,
    target_lock_key,
    target_record_lock_key,
    target_segment_embeddings_key,
    target_key,
)
from target.lock import RedisLock
from target.segment_cache import SegmentEmbeddingCache, SegmentEmbeddingCacheEntry
from target.shared_storage import SharedTargetMediaStore
from target.versioning import EmbeddingSpec, cache_entry_key

# Build-on-miss lock defaults (target/lock.py). PROVISIONAL HEURISTIC, not
# measured against a real embedding workload — see phase-10 doc,
# "Limitations": a full-length video's segment embedding pass can run well
# past a short TTL, so this is deliberately generous (minutes, not
# seconds) rather than tuned. Callers with a better estimate (e.g. from
# target duration) should pass explicit values.
DEFAULT_LOCK_TTL_MS = 600_000  # 10 minutes
DEFAULT_POLL_INTERVAL_S = 1.0
DEFAULT_POLL_TIMEOUT_S = 600.0  # 10 minutes

# Target-lifecycle lock defaults (target-management design doc, S9).
# Deliberately much shorter than the build-on-miss defaults above: a
# create/update/delete is a handful of Redis writes plus a few local/shared
# file deletes, not an embedding build -- 30s is generous headroom, not a
# tuned number, same "provisional heuristic" spirit as DEFAULT_LOCK_TTL_MS's
# own comment. Acquisition is operator-driven and low-frequency, so a loser
# should expect a short wait (seconds), not the minutes a build-on-miss
# loser is willing to poll for.
LIFECYCLE_LOCK_TTL_MS = 30_000  # 30 seconds
LIFECYCLE_LOCK_POLL_INTERVAL_S = 0.1
LIFECYCLE_LOCK_POLL_TIMEOUT_S = 5.0

_TARGET_RECORD_REQUIRED_HASH_FIELDS = frozenset(
    {"target_id", "target_version", "media_path", "content_sha256", "created_at", "updated_at"}
)


@dataclass(frozen=True)
class ReindexResult:
    """Outcome of `TargetRegistry.reindex()` (target-management design doc,
    S21). `found` is every target record discovered by the scan; `added` is
    the subset that was not already a member of `fingerprint:target:index`
    (in dry-run mode, what *would* have been added -- nothing is written)."""

    found: list
    added: list


class TargetRegistry:
    def __init__(
        self,
        redis_client: Redis,
        cache: TargetEmbeddingCache,
        segment_cache: Optional[SegmentEmbeddingCache] = None,
        media_store: Optional[SharedTargetMediaStore] = None,
    ):
        self._redis = redis_client
        self._cache = cache
        self._segment_cache = segment_cache
        self._media_store = media_store

    def register_target(
        self,
        target_id: str,
        target_version: str,
        media_path: str,
        media_metadata: Optional[dict] = None,
        *,
        on_conflict: str = "replace",
    ) -> TargetRecord:
        """Compute content identity from the file's bytes (never its
        filename/path) and upsert the (target_id, target_version) record.
        Re-registering the same (target_id, target_version) preserves the
        original `created_at` and only advances `updated_at`.

        Phase 13D: if a `media_store` was injected, the media bytes are
        also published into shared storage, content-addressed by
        `content_sha256` -- this is what lets a build-on-miss winner on a
        *different* host than whichever one ran registration still read the
        target's media (audit §3.5; see `target/shared_storage.py`'s
        `SharedTargetMediaStore`). A no-op collaborator (`media_store=None`,
        the default) leaves this call exactly as before Phase 13D.

        `on_conflict` (target-management design doc, S9) governs what
        happens when `(target_id, target_version)` already exists with
        *different* content:

        - `"replace"` (default -- preserves this method's pre-existing
          behavior for every direct caller): overwrite the record with the
          new content. Fixes a real bug present before this parameter
          existed -- the target's membership in its *previous* content
          hash's reverse-index set (`target_content_index_key`) is now
          removed before the new membership is written, so
          `find_by_content_hash` on the old hash no longer returns a target
          that no longer has that content.
        - `"reject"` (used by `TargetService.create_target`, never the
          default): raise `TargetAlreadyExistsError` and write nothing.
          Re-registering with *identical* content is always accepted under
          either policy -- there is no conflict when nothing is changing.

        The whole read-existing/decide/write sequence runs under a
        `RedisLock` scoped to this exact `(target_id, target_version)`
        (`target_record_lock_key`) -- the same lock `update_target_metadata`
        and `delete_target` use -- closing the pre-existing race where two
        callers registering the same identity with different content at the
        same moment could silently produce an order-dependent, unlocked
        last-write-wins outcome. Content hashing happens *before* the lock
        is acquired, so the lock is never held for however long streaming a
        large media file takes."""
        if on_conflict not in ("replace", "reject"):
            raise ValueError(f"on_conflict must be 'replace' or 'reject', got {on_conflict!r}")

        content_sha256 = sha256_file(media_path)
        if self._media_store is not None:
            self._media_store.publish(content_sha256, media_path)

        lock = self._acquire_lifecycle_lock(target_id, target_version)
        try:
            existing = self.get_target(target_id, target_version)
            created_at = existing.created_at if existing is not None else None

            if existing is not None and existing.content_sha256 != content_sha256:
                if on_conflict == "reject":
                    raise TargetAlreadyExistsError(
                        f"target {target_id!r} version {target_version!r} already exists with different "
                        f"content (existing content_sha256={existing.content_sha256!r}, "
                        f"new content_sha256={content_sha256!r}); register a new target_version instead"
                    )
                # on_conflict == "replace": this target's membership in the
                # *old* content hash's reverse index is now stale -- remove
                # it before writing the new record/index membership below.
                self._redis.srem(
                    target_content_index_key(existing.content_sha256),
                    encode_content_index_member(target_id, target_version),
                )

            record = TargetRecord(
                target_id=target_id,
                target_version=target_version,
                media_path=str(media_path),
                content_sha256=content_sha256,
                media_metadata=media_metadata or {},
                **({"created_at": created_at} if created_at is not None else {}),
            )
            self._redis.hset(target_key(target_id, target_version), mapping=record.to_hash_fields())
            member = encode_content_index_member(target_id, target_version)
            self._redis.sadd(target_content_index_key(content_sha256), member)
            self._redis.sadd(target_index_key(), member)
            return record
        finally:
            lock.release()

    def _acquire_lifecycle_lock(self, target_id: str, target_version: str) -> RedisLock:
        """Acquire the target-record lifecycle lock (target-management
        design doc, S9): try once, then poll briefly. Raises
        `TargetLockTimeoutError` rather than blocking indefinitely --
        lifecycle operations are operator-driven and low-frequency, so a
        loser should not expect to wait minutes the way a build-on-miss
        loser will."""
        lock = RedisLock(self._redis, target_record_lock_key(target_id, target_version))
        if lock.acquire(LIFECYCLE_LOCK_TTL_MS):
            return lock

        deadline = time.monotonic() + LIFECYCLE_LOCK_POLL_TIMEOUT_S
        while time.monotonic() < deadline:
            time.sleep(LIFECYCLE_LOCK_POLL_INTERVAL_S)
            if lock.acquire(LIFECYCLE_LOCK_TTL_MS):
                return lock

        raise TargetLockTimeoutError(
            f"timed out waiting for the lifecycle lock on target {target_id!r} version {target_version!r}"
        )

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

    def list_targets(self) -> list[TargetRecord]:
        """Every registered target, via `fingerprint:target:index`
        (target-management design doc, S8) -- O(number of registered
        targets), never a keyspace SCAN. `SMEMBERS` has no defined order, so
        results are sorted by `(target_id, target_version)` before
        resolution for deterministic output. A member whose record no
        longer exists (the only way to reach this state: a crash between
        the `SADD`/`DEL` pair in `register_target`/`delete_target`, or an
        unrepaired pre-migration gap -- see `target/cli.py`'s `reindex`) is
        silently skipped rather than failing the whole call."""
        members = self._redis.smembers(target_index_key())
        pairs = sorted(decode_content_index_member(member) for member in members)
        records = []
        for target_id, target_version in pairs:
            record = self.get_target(target_id, target_version)
            if record is not None:
                records.append(record)
        return records

    def update_target_metadata(
        self,
        target_id: str,
        target_version: str,
        set_fields: Optional[dict] = None,
        remove_fields: Optional[Sequence[str]] = None,
    ) -> TargetRecord:
        """Metadata-only patch (target-management design doc, S11): a
        shallow merge of `set_fields` into the existing `media_metadata`,
        followed by popping every key named in `remove_fields` (a key in
        both ends up removed). Never touches `media_path`, `content_sha256`,
        `target_id`, or `target_version` -- there is no parameter for any of
        them, so a content swap is structurally impossible through this
        method. `created_at` is preserved; `updated_at` advances. Raises
        `TargetNotFoundError` if the identity doesn't exist. Serialized by
        the same lifecycle lock `register_target`/`delete_target` use."""
        lock = self._acquire_lifecycle_lock(target_id, target_version)
        try:
            existing = self.get_target(target_id, target_version)
            if existing is None:
                raise TargetNotFoundError(f"unknown target: {target_id!r} version {target_version!r}")

            metadata = dict(existing.media_metadata)
            metadata.update(set_fields or {})
            for key in remove_fields or ():
                metadata.pop(key, None)

            record = TargetRecord(
                target_id=existing.target_id,
                target_version=existing.target_version,
                media_path=existing.media_path,
                content_sha256=existing.content_sha256,
                media_metadata=metadata,
                created_at=existing.created_at,
            )
            self._redis.hset(target_key(target_id, target_version), mapping=record.to_hash_fields())
            return record
        finally:
            lock.release()

    def _cached_embedding_specs(
        self, target_id: str, target_version: str
    ) -> Tuple[list[EmbeddingSpec], list[EmbeddingSpec]]:
        """Reconstruct every `EmbeddingSpec` cached for this target, from the
        small, vector-free Redis summary hashes `register_embedding`/
        `register_segment_embedding` already maintain -- the only place
        "which cache files does this target own" is knowable without
        scanning the filesystem (target-management design doc, S12).
        Returns (pooled_specs, segment_specs)."""

        def _specs(redis_hash_key: str) -> list[EmbeddingSpec]:
            return [
                EmbeddingSpec(
                    model_id=data["model_id"],
                    model_version=data["model_version"],
                    embedding_schema_version=data["embedding_schema_version"],
                    preprocessing_config=data.get("preprocessing_config", {}),
                    sampling_config=data.get("sampling_config", {}),
                )
                for data in (json.loads(raw) for raw in self._redis.hgetall(redis_hash_key).values())
            ]

        return (
            _specs(target_embeddings_key(target_id, target_version)),
            _specs(target_segment_embeddings_key(target_id, target_version)),
        )

    def delete_target(self, target_id: str, target_version: str) -> None:
        """Full target-lifecycle delete (target-management design doc,
        S12). Under the same lifecycle lock `register_target`/
        `update_target_metadata` use:

        1. Look up the record (`TargetNotFoundError` if missing).
        2/3/4. Read the pooled/segment embedding summary hashes and
           reconstruct the `EmbeddingSpec`s they describe.
        5/6. Delete the target-exclusive pooled/segment cache entries for
           each spec -- safe unconditionally, since `cache_entry_key`
           includes `(target_id, target_version)`, so these can never be
           shared with another target.
        7/8. Remove this target's membership from the content reverse index
           and the list index.
        9. Delete the target's own Redis hashes (record + both embedding
           summary hashes).
        10/11. Only after this target's own content-index membership is
           gone: check `find_by_content_hash` for any *other* target still
           referencing the same content, and delete the shared media blob
           only if none remain -- `SharedTargetMediaStore` is
           content-addressed only, so it can be shared across targets and
           must never be removed out from under one that still needs it.

        Does not touch `ResultRecord`s or queued/in-flight jobs (historical/
        in-flight, not target-owned) -- a job against the now-deleted target
        fails via the existing unknown-target -> PermanentFailure path, by
        design (target-management design doc, S13)."""
        lock = self._acquire_lifecycle_lock(target_id, target_version)
        try:
            record = self.get_target(target_id, target_version)
            if record is None:
                raise TargetNotFoundError(f"unknown target: {target_id!r} version {target_version!r}")

            pooled_specs, segment_specs = self._cached_embedding_specs(target_id, target_version)
            for spec in pooled_specs:
                self._cache.delete(target_id, target_version, record.content_sha256, spec)
            if self._segment_cache is not None:
                for spec in segment_specs:
                    self._segment_cache.delete(target_id, target_version, record.content_sha256, spec)

            member = encode_content_index_member(target_id, target_version)
            self._redis.srem(target_content_index_key(record.content_sha256), member)
            self._redis.srem(target_index_key(), member)

            pipe = self._redis.pipeline()
            pipe.delete(target_key(target_id, target_version))
            pipe.delete(target_embeddings_key(target_id, target_version))
            pipe.delete(target_segment_embeddings_key(target_id, target_version))
            pipe.execute()

            remaining = self.find_by_content_hash(record.content_sha256)
            if not remaining and self._media_store is not None:
                self._media_store.delete(record.content_sha256)
        finally:
            lock.release()

    def reindex(self, dry_run: bool = False) -> ReindexResult:
        """One-time, explicit migration/repair: backfill
        `fingerprint:target:index` from target records that predate it
        (target-management design doc, S21). The only place in this
        registry that performs a Redis keyspace `SCAN` -- never run as part
        of `list_targets`/`get_target`/`create_target`/etc., and never run
        automatically at construction or import time.

        For each key matching `fingerprint:target:*`, defensively confirms
        it is really a target record hash (Redis type `hash`, not the index
        Set itself or a `:content:<hash>` reverse-index Set; not a
        `:embeddings`/`:segment_embeddings` summary hash; and its fields
        parse as a well-formed `TargetRecord`) before treating it as one --
        this tolerates a `:` inside a legacy `target_id`/`target_version`
        that predates S6's charset validation, because it never parses the
        *key text* to recover the identity, only the hash's own stored
        `target_id`/`target_version` field values (per the design doc's
        explicit instruction).

        Purely additive: never deletes, modifies, or reinterprets any
        existing target record, embedding, cache entry, job, or result --
        the only Redis write this method ever issues is `SADD` into
        `fingerprint:target:index`, and `dry_run=True` skips even that.
        Idempotent: re-running finds nothing new to add once complete."""
        existing_members = self._redis.smembers(target_index_key())

        found: list = []
        added: list = []
        for key in self._redis.scan_iter(match="fingerprint:target:*"):
            if key == target_index_key():
                continue
            if key.endswith(":embeddings") or key.endswith(":segment_embeddings"):
                continue
            if self._redis.type(key) != "hash":
                continue

            data = self._redis.hgetall(key)
            if not _TARGET_RECORD_REQUIRED_HASH_FIELDS <= data.keys():
                continue
            try:
                record = TargetRecord.from_hash_fields(data)
            except (KeyError, ValueError):
                continue

            pair = (record.target_id, record.target_version)
            found.append(pair)
            member = encode_content_index_member(record.target_id, record.target_version)
            if member not in existing_members:
                added.append(pair)
                if not dry_run:
                    self._redis.sadd(target_index_key(), member)

        return ReindexResult(found=sorted(found), added=sorted(added))

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

    def get_or_build_segment_embedding(
        self,
        target_id: str,
        target_version: str,
        spec: EmbeddingSpec,
        build: Callable[[TargetRecord], Tuple[Sequence, Sequence[float]]],
        lock_ttl_ms: int = DEFAULT_LOCK_TTL_MS,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        poll_timeout_s: float = DEFAULT_POLL_TIMEOUT_S,
    ) -> SegmentEmbeddingCacheEntry:
        """Cache-first, build-on-miss-under-lock resolution of a target's
        segment embedding — `docs/design/design-proposal-1.md` §8's
        "Build-on-miss race" guard, applied to Phase 9's segment
        representation.

        `build` is called at most once per winning caller, with the
        already-fetched `TargetRecord` (so it never has to re-fetch what
        this method already looked up): it must return
        `(segments, coarse_vector)` for this exact target (e.g. by running
        `DINOv2EmbeddingEngine.embed_video_segments` against
        `record.media_path`). This module never imports an embedding
        engine itself — see class docstring.

        Flow: check the cache; on a hit, return immediately (no lock
        touched at all). On a miss, try to acquire
        `target/lock.py`'s `RedisLock` for this exact
        `(target_id, target_version, content_sha256, spec)` key.
        - Winner: double-checks the cache (another worker may have
          finished between this call's first check and winning the lock),
          builds on a second miss, registers the result, releases the
          lock in a `finally` so a `build` exception never leaves the lock
          held for its full TTL.
        - Loser: polls the cache every `poll_interval_s` until it appears
          or `poll_timeout_s` elapses, at which point it raises
          `TimeoutError` rather than duplicating the winner's build or
          blocking forever.
        """
        if self._segment_cache is None:
            raise RuntimeError("TargetRegistry was constructed without a segment_cache; cannot build segments")

        existing = self.get_compatible_segment_embedding(target_id, target_version, spec)
        if existing is not None:
            return existing

        record = self.get_target(target_id, target_version)
        if record is None:
            raise KeyError(f"unknown target: {target_id!r} version {target_version!r}")

        key = target_lock_key(cache_entry_key(target_id, target_version, record.content_sha256, spec))
        lock = RedisLock(self._redis, key)

        if lock.acquire(lock_ttl_ms):
            try:
                existing = self.get_compatible_segment_embedding(target_id, target_version, spec)
                if existing is not None:
                    return existing
                segments, coarse_vector = build(record)
                return self.register_segment_embedding(target_id, target_version, spec, segments, coarse_vector)
            finally:
                lock.release()

        deadline = time.monotonic() + poll_timeout_s
        while True:
            time.sleep(poll_interval_s)
            existing = self.get_compatible_segment_embedding(target_id, target_version, spec)
            if existing is not None:
                return existing
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out after {poll_timeout_s}s waiting for another worker to build the segment "
                    f"embedding for target {target_id!r} version {target_version!r}"
                )
