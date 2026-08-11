# Phase 6 — Target Management and Embedding Cache

## Objective

Build the target-management layer that prepares known target media for
future fingerprint comparison: target identity -> target version -> target
media -> embedding metadata/cache contract. No DINOv2 inference, no frame
extraction — synthetic vectors only. This phase proves the contract, not
the pipeline.

## Package layout

New top-level package `target/`, alongside `work_queue/`, `worker/`,
`acquisition/`, with no dependency on any of them:

- `target/identity.py` — `TargetRecord`, `sha256_file()`.
- `target/versioning.py` — `EmbeddingSpec`, `cache_entry_key()`.
- `target/cache.py` — `TargetEmbeddingCache` (ABC), `FilesystemEmbeddingCache`,
  `EmbeddingCacheEntry`.
- `target/keys.py` — Redis key conventions, matching `work_queue/keys.py`'s
  style.
- `target/registry.py` — `TargetRegistry`, composing Redis-backed metadata
  with an injected `TargetEmbeddingCache`.

## Target identity

`TargetRecord` fields, each justified by the brief: `target_id`,
`target_version`, `media_path` (informational — where the bytes currently
live, not identity), `content_sha256`, `media_metadata`, `created_at`,
`updated_at`. No model/embedding fields live on `TargetRecord` itself —
those belong to `EmbeddingSpec`, kept separate because a target's identity
doesn't change when a new model is added.

**Filename independence**: `sha256_file()` streams a file's bytes through
SHA-256 in 64 KiB chunks — it never touches the path or any filesystem
metadata (mtime, size). Two files with identical bytes under different
names hash identically; `TargetRegistry.find_by_content_hash()` proves this
by looking up registrations via a Redis `SET` keyed by content hash, not by
path.

`target_version` is a separate, caller-assigned label — the same field
`work_queue.jobs.Job` already carries from Phase 1. It is not derived from
content: two registrations can only share a `target_version` if the caller
intends them to. Content-based duplicate detection
(`find_by_content_hash`) is deliberately a different operation from
version equality, so a version bump and a content change can be reasoned
about independently.

## Target versioning / embedding compatibility

`EmbeddingSpec` carries exactly the dimensions the brief calls out as
required, no more:

- `model_id`, `model_version`
- `embedding_schema_version` — bumped when the *meaning* of the stored
  vector changes (pooling, dimensionality convention), even without a model
  change.
- `preprocessing_config` — anything applied before the model sees the media
  that can change the output (resize, crop, normalization). Decode-only
  settings that don't affect the embedding don't belong here.
- `sampling_config` — anything about which frames/segments were sampled
  (fps, frame count, clip selection).

**What invalidates an embedding** (all must match for reuse — any mismatch
is a cache miss):

1. `content_sha256` of the target media — lives on `TargetRecord`, not
   `EmbeddingSpec`, but is threaded through every cache call explicitly
   (`get`/`put`/`exists` all take it as a parameter). This catches the case
   where a caller overwrites a target's file in place without bumping
   `target_version` — the content hash changes, so the previously cached
   embedding is invalidated even though `(target_id, target_version)`
   didn't change (`test_cache_miss_for_different_target_content_hash`).
2. `model_id` + `model_version`
3. `embedding_schema_version`
4. `preprocessing_config`
5. `sampling_config`

No other version field exists. In particular there's no separate
"embedding version" beyond `embedding_schema_version` — one field already
answers "does the stored representation still mean the same thing."

`EmbeddingSpec.spec_key()` and `versioning.cache_entry_key()` give
deterministic, dict-order-independent identifiers for a spec and for a
full (target, content, spec) representation respectively, via
`json.dumps(..., sort_keys=True)` + SHA-256.

## Embedding cache

`TargetEmbeddingCache` (ABC) declares `get`/`put`/`exists`, each taking
`(target_id, target_version, content_sha256, spec)` — the brief's example
signature extended with `content_sha256` explicitly, since it's a required
compatibility dimension, not implied by `target_version` alone (see above).

`FilesystemEmbeddingCache` is the one implementation this phase ships: one
JSON file per cached representation, named by `cache_entry_key(...)`,
containing the vector plus every compatibility field (so a corrupted or
hand-edited file can be caught by re-validating on read, not just trusted
by filename). Writes are atomic (`tempfile.mkstemp` in the same directory +
`os.replace`) so a crash mid-write can never leave a partially-written file
at the final path.

**Corruption handling**: `get()` treats anything short of a fully valid,
exactly-matching entry as a miss — invalid JSON, missing required keys,
schema-version mismatch, or any compatibility field mismatch all return
`None`, never raise. The cache's only job is answering "can this exact
representation be reused" — a file it can't confidently validate can't
answer "yes," so it answers "no" and lets the caller recompute
(`test_corrupted_cache_entry_is_rejected`).

## Storage boundary (Redis vs. embedding cache)

Explicit, per the brief:

```
Redis
  = jobs / state / coordination / small metadata
    (fingerprint:target:{id}:{version}      -> TargetRecord fields, a Hash)
    (fingerprint:target:content:{sha256}    -> Set of "id\x1fversion" members)
    (fingerprint:target:{id}:{version}:embeddings -> Hash: spec_key -> small JSON metadata)

embedding storage (this phase: local filesystem)
  = cache/artifact storage
    (one JSON file per (target, content, spec) representation, vector included)
```

Redis never holds a vector — only a small metadata summary
(`EmbeddingSpec.to_metadata_fields()` + `cached_at`) recording *what* is
cached, keyed by `spec_key()`, so a caller can inspect what representations
exist for a target without touching the cache backend at all
(`test_cache_metadata_identifies_the_embedding_representation`).

`FilesystemEmbeddingCache` is a plain Python class with no Redis import —
`TargetRegistry` is the only thing that composes the two, via constructor
injection (`TargetRegistry(redis_client, cache)`). A later phase can add an
object/shared-storage-backed implementation of `TargetEmbeddingCache`
without changing `TargetRegistry`, the fingerprint worker, or any Redis
key — only the constructor call that chooses which cache to inject.

## Registry design

`TargetRegistry` composes Redis-backed target metadata with an injected
`TargetEmbeddingCache` — two independent collaborators, not one system
(`test_registry_and_cache_work_independently` exercises each half without
the other). Operations:

1. `register_target(target_id, target_version, media_path, media_metadata=None)`
   — hashes the file, upserts the `TargetRecord` (preserving `created_at`
   across re-registration of the same `(target_id, target_version)`), and
   adds it to the content-hash index.
2. `get_target(target_id, target_version)` — `None` if not registered.
3. `find_by_content_hash(content_sha256)` — all registrations sharing exact
   content, independent of filename.
4. `has_compatible_embedding` / `get_compatible_embedding(target_id,
   target_version, spec)` — looks up the target's current
   `content_sha256`, then delegates to `cache.get`/`exists`.
5. `register_embedding(target_id, target_version, spec, vector)` — writes
   the vector via `cache.put`, then records the small metadata summary in
   Redis. Raises `KeyError` if the target isn't registered yet (an
   embedding can't outlive the identity it's for).

## Tests

`tests/test_target.py`, 15 tests, run against the same local Redis db 15 as
Phases 1-5 (`FINGERPRINTER_TEST_REDIS_URL`), tiny synthetic files
(`tmp_path`) and a 3-float synthetic vector throughout:

1. `test_register_target`
2. `test_retrieve_target` (+ `test_retrieve_missing_target_returns_none`)
3. `test_target_identity_independent_of_filename`
4. `test_identical_content_produces_expected_content_hash`
5. `test_target_version_represented_correctly`
6. `test_compatible_embedding_cache_hit`
7. `test_cache_miss_for_different_model_version`
8. `test_cache_miss_for_different_preprocessing_config`
9. `test_cache_miss_for_different_target_content_hash`
10. `test_synthetic_embedding_can_be_stored_and_retrieved`
11. `test_corrupted_cache_entry_is_rejected`
12. `test_cache_metadata_identifies_the_embedding_representation`
13. `test_registry_and_cache_work_independently`
14. `test_repeated_lookup_does_not_recompute_or_store_unnecessarily` — wraps
    `cache.put` with a call counter; five repeated `has_compatible_embedding`/
    `get_compatible_embedding` calls after one `register_embedding` never
    trigger a second `put`, and the content-hash index set stays
    single-membership across repeated registration.

Run: `.venv/bin/python -m pytest tests/` — all 71 tests pass (56 Phase 1-5
+ 15 Phase 6).

## Limitations

- **No integration with `acquisition.MediaArtifact` yet.**
  `MediaArtifact` (Phase 5) already computes `checksum_sha256` for
  downloaded candidate media, but `TargetRegistry.register_target()` always
  re-hashes from a local path — there's no path that reuses an already-
  known checksum. Targets in this phase are assumed to be locally-supplied
  reference files, not acquisition output; wiring those together (if
  targets are ever downloaded rather than provided locally) is deferred.
- **`register_target` re-hashes the whole file on every call.** No
  mtime/size short-circuit before hashing (deliberately — the old
  prototype's `SMART_EMBEDDING_SCENARIOS.md` cache used file mtime/size as
  part of its cache key, which is exactly the filename/host-dependent
  shortcut this phase's brief asked to avoid). Fine at synthetic/dev scale;
  a large real target file would pay a full read on every re-registration.
- **`FilesystemEmbeddingCache.exists()` is just `get() is not None`** — no
  cheaper existence-only path (e.g. a stat-only check). Acceptable for a
  small local JSON file; a future object-storage backend would likely want
  a real HEAD-style check instead of a full body fetch.
- **No cache eviction/retention policy.** Entries accumulate under the
  cache directory indefinitely — same unbounded-growth limitation Phase 4
  noted for the results stream.
- **No concurrent-write protection beyond atomic rename.** Two processes
  computing and `put()`-ting the same `(target, content, spec)` key
  concurrently will both succeed (last writer wins, no torn file) but
  neither is told about the race — acceptable since embeddings for
  identical inputs should be reproducible/interchangeable, unlike job
  state.
- **`target_version` is a free-form string with no ordering/comparison
  semantics** — "which version is newest" isn't answerable from
  `target_version` alone, only from `created_at`/`updated_at`. Not required
  by this phase's brief.
- **No target deletion/deregistration API** — only register/get/find. Not
  required by this phase's minimal-registry scope.
- **`\x1f`-joined content-index members** assume `target_id`/
  `target_version` never contain that byte. Not enforced/validated; fine
  for synthetic/dev identifiers, would want explicit validation before
  accepting untrusted input for either field.

## Deferred work

Same bucket as Phases 1-5, still unchanged: DINOv2 model loading, GPU
inference, frame extraction, FAISS, similarity scoring, temporal matching,
pHash, audio fingerprints, crawler integration, distributed deployment, GPU
scheduling, a distributed object-storage backend, monitoring. Also still
deferred: rewriting the top-level architecture document, and connecting
this phase's `TargetEmbeddingCache`/`TargetRegistry` to an actual
fingerprint-worker handler (Phase 4/5's `Worker.process_claim()` has no
target-aware handler yet — that wiring is a future phase, once real
embeddings exist to look up).
