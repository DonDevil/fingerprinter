# Target Management Design — CRUD / Multi-Target Operator Interface

## 1. Status

**DESIGN ONLY. No production code, tests, configuration, or Redis state was
modified to produce this document.** This document is the direct successor
to `docs/architecture/target-management-audit.md` (audit-only, read-only).
Every claim below is labeled:

- **VERIFIED FROM SOURCE** — read directly in the file/line cited (by this
  document or by the audit it builds on).
- **DESIGN DECISION** — a choice made in this document, with rationale, that
  is not dictated by existing code.
- **INFERRED** — a reasonable conclusion from source, not directly asserted.
- **NOT IMPLEMENTED** — does not exist yet; this document specifies what the
  next phase should build.

Nothing in this document has been built. It is a specification for the
**next** phase, to be implemented only after review.

## 2. Design objective

Give an operator (today: a human via CLI; tomorrow: a dashboard via an HTTP
layer that does not exist yet) a complete, safe target lifecycle — create,
list, get, update metadata, delete — for multiple simultaneous targets,
through one clean application-level boundary (`TargetService`) that hides
`TargetRegistry`, Redis, and cache internals from every caller above it.

This is additive. It does not redesign `TargetRegistry`, `target/keys.py`,
`target/versioning.py`, the cache/segment-cache/shared-storage classes, the
work queue, the worker, or the crawler-facing contract.

## 3. Existing architecture constraints (from the audit, VERIFIED FROM SOURCE)

- Identity is content-addressed: `content_sha256` = `sha256_file(media_path)`,
  never the filename (`target/identity.py:29-37`).
- `target_id`/`target_version` are caller-assigned, opaque, unvalidated
  strings today; `target_key()` is an unescaped `:`-join
  (`target/keys.py:13-14`) — a real collision class.
- `TargetRegistry.register_target()` is a full-record upsert: re-registering
  the same `(target_id, target_version)` preserves `created_at`, overwrites
  everything else, including silently replacing content
  (`target/registry.py:74-112`). No lock guards this read-modify-write.
- `register_target` `SADD`s the new content hash into
  `target_content_index_key()` but never `SREM`s the previous hash on a
  content-changing re-registration — a stale reverse-index entry (audit §4).
- `find_by_content_hash()` already exists and is the correct primitive for
  reference-counting `SharedTargetMediaStore` blobs before deleting them
  (audit §7, §11).
- No cache class has a delete/remove method today: `FilesystemEmbeddingCache`,
  `FilesystemSegmentEmbeddingCache`, `SharedFilesystemEmbeddingCache`,
  `SharedFilesystemSegmentEmbeddingCache` (`target/cache.py`,
  `target/segment_cache.py`, `target/shared_cache.py`) each implement only
  `get`/`put`/`exists`. `SharedArtifactStore`/`SharedTargetMediaStore`
  (`target/shared_storage.py`) likewise have no delete. **VERIFIED FROM
  SOURCE** (read in full).
- `target/lock.py`'s `RedisLock` is a plain `SET NX PX` / Lua-CAS-release
  primitive, not reentrant, already proven correct for build-on-miss
  (`target/registry.py:249-260`). No new locking primitive is needed.
- `target_embeddings_key`/`target_segment_embeddings_key` Redis hashes store,
  per cached representation, `spec_key() -> json(spec.to_metadata_fields())`
  (`target/registry.py:153-157,191-198`) — this JSON carries every field
  `EmbeddingSpec` needs to be reconstructed exactly
  (`model_id`, `model_version`, `embedding_schema_version`,
  `preprocessing_config`, `sampling_config` — `target/versioning.py:42-47`).
  This is what makes it possible to know *which* cache files a target owns
  without scanning the filesystem.
- The existing exception convention in this repo is a small, flat
  `SomethingError(BuiltinError)` per module — `JobValidationError(ValueError)`
  (`work_queue/jobs.py:49`), `CandidateValidationError(ValueError)`
  (`integration/candidate.py:45`), `ConfigError(ValueError)`
  (`worker/main.py:98`), `SharedArtifactStoreError(OSError)`
  (`target/shared_storage.py:59`) — never a deep hierarchy. This design
  follows that convention.
- `worker/main.py`'s `WorkerConfig`/`build_redis_client`/`build_registry`
  (`worker/main.py:122-319`) is the existing env-var-driven wiring pattern
  for constructing a `TargetRegistry` (`REDIS_URL`, `TARGET_CACHE_PATH`,
  `SHARED_ARTIFACT_STORE_PATH` — documented in `docs/usage.md`'s "Register a
  target" / "Start a worker" sections). `worker/main.py` imports
  `embedding.dinov2_engine.DINOv2EmbeddingEngine` at module scope
  (`worker/main.py:44`) — a heavy ML dependency (torch/transformers) with no
  relevance to metadata CRUD. **This matters for the CLI design (§19).**

## 4. Proposed architecture

```
Dashboard (future) / HTTP API (future) / CLI (this phase)
                       │
                       ▼
                 TargetService              (target/service.py — NEW)
                       │
                       ▼
                 TargetRegistry             (target/registry.py — EXTENDED)
                       │
        ┌──────────────┼──────────────────────┐
        ▼              ▼                      ▼
 Redis (hashes,   Embedding /             SharedTargetMediaStore
 content index,   segment caches          (content-addressed,
 list index)      (local or shared)       shared across targets)
```

`TargetService` is the **only** new module-level surface. Everything below
it is the existing architecture plus small, additive methods.

### 4.1 Responsibility split

| Layer | Owns | Must NOT know about |
|---|---|---|
| **TargetService** | Operator-facing lifecycle semantics: input validation (charset/length), typed errors, the create-vs-replace policy decision, translating "not found" into a typed error for mutations. | Redis keys, Redis Set membership, cache file layout, `cache_entry_key()`, lock keys, `SharedArtifactStore` paths. |
| **TargetRegistry** | Atomicity/locking for any mutation to a `(target_id, target_version)` identity; all three Redis structures that belong to that identity (record hash, content-reverse-index membership, list-index membership); orchestrating cache/media cleanup on delete by calling its own injected `_cache`/`_segment_cache`/`_media_store` collaborators. | Operator-facing validation policy (charset rules, "id/version must not contain `:`" is enforced one layer up, not baked into the registry itself — see §10). |
| **Caches / `SharedTargetMediaStore`** | Their own storage format, file paths, content-addressing. Gain one additive `delete()`/`delete_bytes()` method each; nothing else changes. | Target lifecycle semantics, Redis. |

A future dashboard (or an HTTP layer added later as a thin wrapper) can rely
on: `TargetService` being the complete lifecycle surface, every method
raising one of a small set of typed errors (§18), every returned object
being a plain, already-immutable `TargetRecord` — never a Redis key, a Set
member, or a filesystem path it has to interpret.

### 4.2 Why not put lifecycle methods directly on `TargetRegistry`?

`TargetRegistry`'s own module docstring already frames it as the identity/
versioning/cache-compatibility layer, composed with independent
collaborators (cache, segment cache, media store) — not an operator-facing
API. This design keeps that shape but **does** add the handful of
list/update/delete primitives to `TargetRegistry` itself (§4.1), because
those primitives need direct access to the same Redis client and injected
caches `register_target` already has, and because keeping "is this mutation
atomic and race-free" entirely inside `TargetRegistry` means the guarantee
holds for **any** caller (tests, benchmarks, a future direct script), not
only callers that go through `TargetService`. `TargetService` then owns only
the *policy* layered on top (validation, create-vs-conflict semantics) —
this is the deviation from the audit's §16 sketch (which put more logic
directly in `TargetService`), and it is deliberate: it keeps one lock owner
and one source of Redis-invariant truth (§9).

## 5. TargetService API

```python
# target/service.py (NEW)

class TargetService:
    def __init__(self, registry: TargetRegistry): ...

    def create_target(
        self,
        target_id: str,
        target_version: str,
        media_path: Union[str, Path],
        metadata: Optional[dict] = None,
    ) -> TargetRecord: ...

    def list_targets(self) -> list[TargetRecord]: ...

    def get_target(self, target_id: str, target_version: str) -> Optional[TargetRecord]: ...

    def update_target_metadata(
        self,
        target_id: str,
        target_version: str,
        set_fields: Optional[dict] = None,
        remove_fields: Optional[Sequence[str]] = None,
    ) -> TargetRecord: ...

    def delete_target(self, target_id: str, target_version: str) -> None: ...
```

**Deviation from the audit's sketch, justified:**
- No `redis_client` constructor parameter. §4.2 moves list-index and
  content-index ownership into `TargetRegistry`, so `TargetService` never
  touches Redis directly — it only ever calls `TargetRegistry` methods. This
  is a *stronger* version of the audit's own boundary goal ("the future
  dashboard must not need to manipulate Redis keys... directly"), applied to
  `TargetService` itself.
- `update_target_metadata` takes `set_fields`/`remove_fields` instead of a
  single `metadata: dict`, because "can metadata keys be deleted" (§11) needs
  an explicit answer, and a merge-only dict can set but never remove a key.

## 6. Validation contract (`target_id` / `target_version`)

**DESIGN DECISION.** One shared validator, applied identically to both
fields (the audit found no evidence either field needs different rules):

- Non-empty after no normalization is applied (no auto-trim — see below).
- Charset: `^[A-Za-z0-9._-]+$` — letters, digits, `.`, `_`, `-`. This
  explicitly excludes `:` (closes the `target_key()` collision class, audit
  §13) and, because the regex is a strict allow-list, automatically excludes
  whitespace and control characters without a separate check.
- Max length: 128 characters. Purely a hygiene bound (Redis itself has no
  such limit) — far larger than any realistic slug (`tamil_blasters`,
  `movie-2026`), chosen to cap key size and reject pathological input, not
  to constrain real usage.
- **Whitespace is rejected, not silently stripped.** `" blast"` and
  `"blast "` are both invalid. Silently normalizing would let two operators
  who typed visually-different strings collide on the same identity without
  either of them noticing — worse than a loud rejection.

Canonical error: `TargetValidationError` (ValueError subclass, §18), message
shape: `f"invalid {field_name}: {value!r} — must be 1-128 characters from [A-Za-z0-9._-]"`.

This validation lives in `target/service.py` as a private helper
(`_validate_identifier(value, field_name)`), used by `create_target` only —
`get_target`/`list_targets`/`update_target_metadata`/`delete_target` do not
need to re-validate an id/version that must already have passed validation
to exist as a registered target (an id that fails the new charset rule
cannot have been created by `TargetService.create_target` after this phase
ships; a pre-existing target created directly via `TargetRegistry.
register_target` with a `:`-containing id, if one somehow exists, is handled
by the migration story in §21, not by rejecting reads of it).

**Deliberately not enforced inside `TargetRegistry.register_target` itself**
(DESIGN DECISION): the audit scoped this fix to "the operator-interface
boundary" (audit §13, §19), and `register_target` has no production call
site — only tests/benchmarks call it directly, with today's coexisting
`:`-free ids. Enforcing charset validation inside `TargetRegistry` would
touch a method with existing test coverage the phase brief says not to
modify. Residual risk: a caller that bypasses `TargetService` and calls
`TargetRegistry.register_target` directly can still create a colliding id.
This is accepted, matching the audit's own scoping.

## 7. Create semantics

```python
def create_target(self, target_id, target_version, media_path, metadata=None) -> TargetRecord
```

1. Validate `target_id`, `target_version` (§6) → `TargetValidationError`.
2. Validate `media_path` (§17) → `TargetMediaError`:
   - must exist,
   - must be a regular file (not a directory, not a special file),
   - must be non-empty (`st_size > 0`),
   - must be readable (a 1-byte probe read; any `OSError` is caught and
     re-raised as `TargetMediaError`, never leaked raw — closes audit §13's
     "raw filesystem exceptions" gap).
3. Validate `metadata` is `None` or a `dict` → `TargetValidationError`
   otherwise (no further shape constraints; it is opaque, caller-owned data,
   same as today).
4. Call `TargetRegistry.register_target(target_id, target_version,
   media_path, media_metadata=metadata, on_conflict="reject")` (§9 — the new
   `on_conflict` parameter, `TargetRegistry`-owned locking).

### Idempotency / duplicate-identity behavior — the key decision

**DESIGN DECISION**, directly answering the phase brief's explicit
instruction not to silently retain the dangerous "re-register to swap
content" behavior:

- If `(target_id, target_version)` **does not exist**: create it. Normal
  path, identical to today's `register_target` first-call behavior.
- If it **exists and the new file's `content_sha256` matches the existing
  record's**: **idempotent no-op-content** — `create_target` still runs
  (metadata is refreshed from the call's `metadata` argument, `updated_at`
  advances), but no content swap occurs because none is happening — the
  bytes are identical. Safe to retry.
- If it **exists and the new file's `content_sha256` differs**:
  `create_target` raises `TargetAlreadyExistsError` and writes nothing. It
  does **not** silently replace the content behind an existing version.

To put different content under the same `target_id`, the caller registers
it under a **new `target_version`** — already fully supported, zero registry
changes (audit §6's own recommendation). To change only metadata, call
`update_target_metadata` (§11). There is no fourth operation and no
generic "replace" flag on `create_target`.

This distinguishes the three operations the brief asked for explicitly:
**create new target/version** (this section) vs. **update metadata** (§11)
vs. **register new content under a new version** (just `create_target`
again with a different `target_version` — not a new method).

`TargetRegistry.register_target` itself keeps its **existing** `on_conflict
="replace"` default (full upsert, content-swap allowed) for backward
compatibility with existing tests/direct callers (audit §4's cited test,
`test_cache_miss_for_different_target_content_hash`, depends on this).
`TargetService.create_target` is the only caller that passes
`on_conflict="reject"`. See §9 for exactly how this is implemented
race-free.

### created_at / updated_at

Unchanged from today: `created_at` is preserved across any successful write
to an existing identity (idempotent-content path); `updated_at` always
advances to the write time. A rejected (`TargetAlreadyExistsError`) call
touches neither.

### Missing media_path — table

| Condition | Error |
|---|---|
| Path does not exist | `TargetMediaError` — "does not exist" |
| Path is a directory | `TargetMediaError` — "is a directory, not a file" |
| Path is an empty file | `TargetMediaError` — "is empty" |
| Path exists, is a file, non-empty, but unreadable (permissions) | `TargetMediaError` — wraps the `OSError` as `__cause__`, message does not leak more of the raw traceback than the reason |

## 8. List / index design

**New Redis Set: `fingerprint:target:index`.**

- **Member encoding: reuse `target.keys.encode_content_index_member` /
  `decode_content_index_member` verbatim** (audit's own recommendation,
  §5). No new encoding scheme, no rename in this phase — the function names
  say "content_index" but the encoding itself (`f"{id}\x1f{version}"`) is
  generic; a rename is a legitimate future cleanup but not required, and
  keeping the name avoids touching a function with zero external
  callers-by-name today (verified: no test or module outside
  `target/keys.py`/`target/registry.py` imports these two functions by
  name) for a purely cosmetic reason.
- **`SADD`ed inside `TargetRegistry.register_target`**, alongside the
  existing content-index `SADD`, under the same lock (§9) — so the index is
  populated for **every** successful registration, including direct
  `register_target` callers (tests, benchmarks), not only calls that went
  through `TargetService`. This keeps the invariant "every registered
  target appears in the list index" true regardless of caller, matching how
  the content-index invariant already works today.
- **`SREM`ed inside `TargetRegistry.delete_target`** (§12).
- **Stale member handling:** `list_targets()` decodes every member, calls
  `get_target()` for each, and **silently skips** any member whose record is
  missing (defensive, not expected in steady state — the only way to reach
  this state is a crash between the `SADD`/`DEL` pair, or an unrepaired
  pre-migration gap, see §21). A stale member does not fail the whole `list`
  call.
- **Ordering:** `SMEMBERS` has no defined order. `list_targets()` sorts the
  decoded `(target_id, target_version)` tuples lexicographically before
  resolving records, giving deterministic output regardless of Redis's
  internal Set iteration order, at O(number of targets) cost — cheap at this
  scale.
- **Pagination: explicitly not implemented this phase.** Expected scale is
  target-management scale (tens to low hundreds), not crawler-URL scale
  (§20 of the brief). `list_targets()` returns a plain, fully-materialized
  `list[TargetRecord]`; a caller needing a page can slice it client-side.
  Revisit only if a real deployment shows this list growing past what fits
  comfortably in memory/one response — not speculated on here.

This keeps `list_targets()` at **O(number of registered targets)**, never
O(keyspace) — the audit explicitly ruled out `SCAN fingerprint:target:*` for
exactly this reason (ambiguous key parsing given unescaped `:`, and an
O(keyspace) cost), and this design does not revisit that call.

## 9. Registration concurrency (closes audit §12)

**Lock owner: `TargetRegistry`, not `TargetService`.** One `RedisLock`
scope, `target_lock_key` is **not** reused here (that key is scoped to a
build-on-miss `cache_entry_key`, a different identity granularity) — a new,
narrower key is used instead:

```
fingerprint:lock:target-record:{target_id}:{target_version}
```

(New helper, `target/keys.py::target_record_lock_key(target_id,
target_version)` — kept syntactically distinct from `target_lock_key()`'s
`cache_entry_key`-based scope so a metadata-lifecycle lock and a
build-on-miss lock for the same target can never collide or be confused.)

**Every mutation to a `(target_id, target_version)` identity acquires this
same lock**: `register_target`, the new `update_target_metadata`, and the
new `delete_target`. This is the single invariant that makes create-vs-
create, create-vs-delete, delete-vs-delete, and delete-vs-update races all
safe using one mechanism.

- **TTL:** a new constant, `LIFECYCLE_LOCK_TTL_MS = 30_000` (30s) —
  deliberately much shorter than `DEFAULT_LOCK_TTL_MS` (10 minutes, sized
  for an embedding *build*). Lifecycle operations are a handful of Redis
  writes plus a few local/shared file deletes; 30s is generous headroom, not
  a tuned number, same "provisional heuristic" spirit as the existing
  constant's own comment.
- **Acquisition:** try once; on failure, poll every 0.1s up to a 5s total
  budget (new constants `LIFECYCLE_LOCK_POLL_INTERVAL_S = 0.1`,
  `LIFECYCLE_LOCK_POLL_TIMEOUT_S = 5.0`) — short, because lifecycle
  operations are operator-driven and low-frequency, not a hot path where a
  loser should expect to wait minutes. On timeout: raise
  `TargetLockTimeoutError` (§18).
- **Release:** always in a `finally`, mirroring `get_or_build_segment_
  embedding`'s existing pattern exactly.
- **Failure behavior:** if the lock is acquired but the wrapped operation
  raises partway through, `finally` still releases it — a failed create/
  update/delete never leaves the identity stuck locked for the full TTL.

`register_target`'s new signature:

```python
def register_target(
    self,
    target_id: str,
    target_version: str,
    media_path: str,
    media_metadata: Optional[dict] = None,
    on_conflict: str = "replace",   # NEW, default preserves today's behavior
) -> TargetRecord:
```

Internals, under the lock:
1. `content_sha256 = sha256_file(media_path)` (computed **before** acquiring
   the lock — hashing doesn't touch Redis and shouldn't hold the lock for
   however long it takes to stream a large file).
2. Acquire `RedisLock(target_record_lock_key(target_id, target_version))`.
3. `existing = self.get_target(...)` (now happens *inside* the lock — this
   is the fix for the read-modify-write race itself; today it happens
   outside any lock).
4. If `existing is not None` and `existing.content_sha256 != content_sha256`:
   - `on_conflict == "reject"` → raise `TargetContentConflictError` (used by
     `TargetService.create_target`; release lock via `finally`, write
     nothing).
   - `on_conflict == "replace"` (default) → proceed exactly as today, **plus
     the new fix**: `SREM` the target's member from `target_content_index_key
     (existing.content_sha256)` **before** writing the new record — this is
     the fix for audit §4's stale-index bug. If `existing.content_sha256 ==
     content_sha256` (identical bytes, no-op-content), no `SREM` is needed
     — nothing is stale.
5. `HSET` the record, `SADD` the (possibly new) content-index membership,
   `SADD` the list-index membership (idempotent even on re-registration —
   Set `SADD` of an already-present member is a no-op).
6. Release the lock (`finally`).

This is one atomic-under-lock sequence; steps 3-5 happen while holding the
lock, so two concurrent callers registering the same identity with
different content can no longer race to a silent, order-dependent outcome —
the second one either sees the first one's write (if it went through, e.g.
identical content) or blocks/times out waiting for the lock, never
interleaves.

## 10. Delete-vs-update-vs-register concurrency

Covered by §9's single lock scope: `update_target_metadata` and
`delete_target` both acquire the exact same `target_record_lock_key`. A
second `delete_target` on an already-deleted identity acquires the lock
(the first deleter's `finally` already released it), calls `get_target`,
finds nothing, and raises `TargetNotFoundError` — a clean, safe "duplicate
delete" outcome, not a crash or a partial re-delete. A `delete` racing an
`update_target_metadata` on the same identity is fully serialized by the
lock; whichever acquires first completes entirely before the other
proceeds.

## 11. Update semantics (metadata-only)

```python
def update_target_metadata(
    self, target_id, target_version, set_fields=None, remove_fields=None
) -> TargetRecord
```

- **New `TargetRegistry` method**, same lock scope as `register_target`
  (§9). Reads the existing record; if missing, raises `TargetNotFoundError`.
- **Patch semantics, not replacement:** `media_metadata` is updated as
  `{**existing.media_metadata, **(set_fields or {})}`, then any key named in
  `remove_fields` is popped. This is a **shallow** merge — nested dict
  values in `set_fields` fully replace the corresponding existing nested
  value, they are not recursively merged. (DESIGN DECISION: deep-merging
  caller-supplied dicts is a surprising, hard-to-predict semantic for
  opaque, caller-owned metadata; shallow merge matches plain
  `dict.update()`, the simplest contract that still supports incremental
  tagging like `--set genre=action`.)
- **Key deletion is explicit**, via `remove_fields` — not a sentinel value
  inside `set_fields`. A key in both `set_fields` and `remove_fields` is
  removed (removal is applied after the set-merge) — this ordering is
  arbitrary but must be picked; documented here so it is not ambiguous.
- **Never touches** `media_path`, `content_sha256`, `target_id`,
  `target_version`. There is no parameter for any of them — this is
  enforced by the method signature, not by a runtime check. Changing source
  media is **only** possible via `create_target` under a new
  `target_version` (§7) — reframed as "register a new content version," per
  the audit's own recommendation, not as a special case of update.
- `updated_at` advances; `created_at` is untouched.
- Not-found: `TargetNotFoundError`.
- Concurrency: serialized by `target_record_lock_key`, same as create/delete
  (§9/§10).

This directly closes audit §6's two findings: metadata is no longer
silently clobbered to `{}` by an update that doesn't mention it (there is no
"omit metadata" path anymore — `set_fields=None` means "change nothing"),
and there is no way to swap content through this method at all.

## 12. Delete semantics

```python
def delete_target(self, target_id, target_version) -> None   # TargetService
```

`TargetService.delete_target` is a thin wrapper: validates nothing further
(an id/version that doesn't parse as valid couldn't have been created, so a
lookup-miss on it is just "not found," not a separate validation error) and
calls `TargetRegistry.delete_target(target_id, target_version)`, translating
nothing — the registry method already raises the typed error.

**`TargetRegistry.delete_target` (NEW) — full sequence, under
`target_record_lock_key` (§9):**

1. `record = self.get_target(target_id, target_version)`. If `None`:
   release lock, raise `TargetNotFoundError`.
2. Read `target_embeddings_key(id, version)` and
   `target_segment_embeddings_key(id, version)` hashes (`HGETALL`) **before
   deleting anything** — this is the only place the set of cached
   `EmbeddingSpec`s for this target is knowable (§3, "no cache class can be
   scanned"). Reconstruct one `EmbeddingSpec` per hash field from its stored
   `to_metadata_fields()` JSON.
3. For each reconstructed pooled-cache spec: call the injected
   `self._cache.delete(target_id, target_version, record.content_sha256,
   spec)` (new method, §14) — target-exclusive by construction of
   `cache_entry_key`, unconditionally safe (audit §7's table). Same for each
   segment-cache spec via `self._segment_cache.delete(...)` if a
   `segment_cache` was configured.
4. `SREM` the target's member from `target_content_index_key
   (record.content_sha256)`.
5. `SREM` the target's member from `fingerprint:target:index` (§8).
6. `DEL` the three Redis hashes for this identity: `target_key(id,version)`,
   `target_embeddings_key(id,version)`, `target_segment_embeddings_key
   (id,version)` — issued as one `redis.pipeline()` (not `MULTI/EXEC`
   strictly required for correctness here since nothing outside this method
   reads these three keys as a transactional unit, but a pipeline is free
   and reduces round trips).
7. **Shared media reference check — the one artifact that isn't
   target-exclusive:** `remaining = self.find_by_content_hash
   (record.content_sha256)`. Because step 4 already removed this target's
   own membership, `remaining` reflects every *other* `(target_id,
   target_version)` still pointing at this content. If `remaining` is empty
   and `self._media_store is not None`: call `self._media_store.delete
   (record.content_sha256)` (new method, §14).
8. Release the lock (`finally`).

**Explicitly not touched (audit §7/§11, unchanged in this design):**
- `ResultRecord`s (`fingerprint:job:{job_id}:result`,
  `fingerprint:result:{job_id}`) — historical, keyed by `job_id`, not
  target-owned. No cascade-delete.
- Queued/in-flight jobs (Redis Stream entries) — see §13 for the explicit
  policy.
- `target/lock.py` build-on-miss locks — self-expiring by TTL, no action
  needed (audit's own conclusion, unchanged).

**Failure-partway-through behavior (DESIGN DECISION):** this sequence is
*not* one atomic transaction across Redis + two filesystem/shared-storage
backends — that would require distributed-transaction machinery this
architecture doesn't have and the brief explicitly forbids inventing. The
step ordering above is chosen so that a crash between steps leaves the
system in a **safe, not a corrupt**, state:
- Crash after step 3 (cache files deleted) but before step 6 (registry hash
  still exists): the target still resolves via `get_target`, but a
  subsequent embedding lookup for the now-deleted spec is simply a cache
  miss — indistinguishable from "never built," and the existing
  build-on-miss path already handles that correctly. No data-loss, no
  corruption — just a target still present with a colder cache than before
  the failed delete. An operator re-running `delete` completes it.
- Crash after step 6 (registry record gone) but before step 7 (shared media
  reference check never ran): the shared media blob is merely **leaked**
  (not deleted, not corrupted) — a disk-hygiene cost, not a correctness
  bug, and recoverable by a future GC sweep over `find_by_content_hash`
  results if one is ever needed (not built in this phase — no evidence of
  scale that justifies it yet).
- The one ordering that is **not** safe and is deliberately avoided: running
  step 7 (shared-media delete) *before* step 4 (this target's own `SREM`).
  Doing so would make `find_by_content_hash` still see this target as a
  referent, always concluding "still referenced," which would mean shared
  media for a genuinely-orphaned target is **never** collected. The order
  above (SREM before the reference check) is the one the audit itself
  flags as required (audit §7, closing paragraph).

## 13. Active-job deletion policy — explicit decision

**DESIGN DECISION: delete is allowed immediately. No rejection, no
soft-delete/inactive flag.** A queued or in-flight job that references a
just-deleted target fails the way it already does today for any unknown
target: `TargetRegistry.get_target()`/`get_or_build_segment_embedding()`
raises `KeyError` inside the matching handler, which the existing worker
error taxonomy maps to `PermanentFailure`
(`worker/matching_handler.py:231-232`, tested by
`tests/test_matching_handler.py::test_unknown_target_raises_permanent_failure`).
This path requires **zero new code** — deleting the registry record is
sufficient to make it fire.

**Why not reject deletion while jobs are queued/in-flight:** there is no
`target → job` reverse index anywhere in this codebase (audit §7, "no
target→job or target→result index exists"). Building one would mean adding
write-amplification to the job submission/claim path — a work-queue schema
change, explicitly out of scope for a target-management phase (brief §19).
Rejecting without a real reference count would mean either (a) a full Stream
scan on every delete (violates the performance constraint in §17 below) or
(b) a fake check that doesn't actually reflect in-flight jobs. Neither is
acceptable.

**Why not a soft-delete/inactive flag:** the brief explicitly warns against
inventing one "merely because it sounds robust." A soft-delete state adds a
new field, a new filtering rule in `get_target`/`list_targets`/matching, and
a new "is this target usable" question everywhere a target is read — a
second persistence model in miniature. The existing fail-closed
`KeyError → PermanentFailure` path is already correct (a job against a
missing target fails loudly, with no silent bad match) and already tested.
Hard delete plus the existing fail-closed path is strictly smaller and
reuses proven behavior instead of adding a new state machine.

**Tradeoff, stated plainly:** an operator who deletes a target with jobs
genuinely in flight will see those jobs fail (correctly, loudly, via the
existing retry/permanent-failure machinery) rather than being warned before
deleting. This is the accepted cost of the smallest correct policy; if a
future phase's operational experience shows this surprises operators
often enough to matter, a `target → active-job count` index is the natural
next step — not proposed here for lack of evidence it's needed (§29).

## 14. Cache cleanup — additive methods only

**No cache storage format changes.** Every cache class gains exactly one
new method, matching its own existing `get`/`put`/`exists` signature shape:

```python
# target/cache.py — TargetEmbeddingCache (ABC) and FilesystemEmbeddingCache
def delete(self, target_id, target_version, content_sha256, spec: EmbeddingSpec) -> bool:
    """Remove this exact representation if present. True iff something was
    deleted, False iff it was already absent — mirrors exists()'s contract,
    never raises for a plain miss."""
```

- `FilesystemEmbeddingCache.delete`: compute the same `_path_for(...)` `get`
  already uses; `path.unlink()` if it exists, return whether it did.
- `SharedFilesystemEmbeddingCache.delete` (`target/shared_cache.py`): needs
  a new primitive one layer down —

```python
# target/shared_storage.py — SharedArtifactStore
def delete(self, key: str) -> bool:
    """Remove the blob at `key` if present. True iff something was
    deleted. Raises SharedArtifactStoreError on an unreachable/unwritable
    store, same failure semantics as get_bytes/put_bytes — never conflates
    'absent' with 'store unreachable'."""
```

  then `SharedFilesystemEmbeddingCache.delete(...)` calls
  `self._store.delete(self._key(...))`.
- `FilesystemSegmentEmbeddingCache.delete` / `SharedFilesystemSegmentEmbeddingCache.delete`
  (`target/segment_cache.py` / `target/shared_cache.py`): identical shape,
  segment-cache key derivation.
- `SharedTargetMediaStore.delete(content_sha256: str) -> bool` (new, `target/
  shared_storage.py`): `self._store.delete(self._key(content_sha256))`.

**Nothing constructs a cache filename or `SharedArtifactStore` key outside
these classes.** `TargetRegistry.delete_target` (§12) calls `self._cache.
delete(...)` / `self._segment_cache.delete(...)` / `self._media_store.
delete(...)` — the same "already-injected collaborator" pattern
`register_embedding`/`register_segment_embedding` already use. `TargetService`
never touches any of this directly (§4.1's boundary table).

This is the "tiny additive cache deletion method" the brief anticipated
(§10) — confirmed necessary by reading all four cache classes plus
`SharedArtifactStore` in full: none have any delete-capable method today.

## 15. Content reverse-index repair

Covered in full in §9 (registration fix) and §12 step 4/7 (delete fix). No
separate repair mechanism is needed **going forward** — both write paths
that could leave the index stale are now fixed at the source (locked
SREM-before-SADD on content-changing re-registration; locked SREM-before-
reference-check on delete). §21 covers **pre-existing** stale state from
before this phase ships (a migration concern, not a new-code-path concern).

No general database migration framework is introduced. The repair described
in §21 is a small, one-time, read-mostly utility script — nothing that
runs as part of steady-state `TargetService` operation.

## 16. Error model

Six typed errors, all new, all in `target/service.py`, following the
existing repo convention of a flat `SomethingError(BuiltinError)` shape
(§3):

```python
class TargetServiceError(Exception):
    """Base class for every typed error TargetService/TargetRegistry's new
    lifecycle methods raise. Rarely raised directly."""

class TargetValidationError(TargetServiceError, ValueError):
    """target_id/target_version fails the charset/length contract (§6), or
    metadata is not a dict."""

class TargetMediaError(TargetServiceError, OSError):
    """media_path is missing, is a directory, is empty, or is unreadable —
    never a raw OSError/FileNotFoundError/IsADirectoryError leaks past
    create_target (closes audit §13)."""

class TargetNotFoundError(TargetServiceError, KeyError):
    """get_target's mutation-side counterparts (update/delete) reference a
    (target_id, target_version) that does not exist. (get_target itself
    returns None on a miss, not this — see §"Get semantics" below.)"""

class TargetAlreadyExistsError(TargetServiceError, ValueError):
    """create_target's own alias for the conflict below, raised by
    TargetService specifically (TargetRegistry raises
    TargetContentConflictError; TargetService re-raises it as this name so
    the public, documented TargetService surface uses lifecycle-shaped
    names). See note below — DESIGN DECISION on whether these are the same
    class."""

class TargetLockTimeoutError(TargetServiceError, TimeoutError):
    """Could not acquire target_record_lock_key within the poll budget
    (§9) — another create/update/delete is in progress."""
```

**Simplification (DESIGN DECISION):** rather than two names for the same
condition (`TargetContentConflictError` at the registry layer,
`TargetAlreadyExistsError` at the service layer), **use one class**,
`TargetAlreadyExistsError`, defined in `target/service.py` and imported by
`target/registry.py`. `TargetRegistry.register_target(..., on_conflict=
"reject")` raises it directly. This avoids a wrap-and-re-raise step and
keeps exactly one name for one condition, at the cost of `target/registry.py`
importing from `target/service.py` — acceptable since `target/service.py`
has no import that would create a cycle back into `registry.py` beyond this
one exception class (verified against the module list in §3: `service.py`'s
only planned imports are `TargetRegistry`, `TargetRecord`, and the error
classes it defines itself).

**Redis-unavailable / lock-timeout distinction:** a `redis.exceptions.
RedisError` (connection refused, timeout) from any underlying `Redis` call
is **not** wrapped or caught anywhere in this design — it propagates as-is,
exactly as it does today from `register_target`/`get_target`. Wrapping it
would hide the real cause (network/Redis-availability, an infrastructure
problem, not a target-lifecycle problem) behind a target-specific type. This
matches `build_redis_client`'s own existing fail-fast-on-`ping()` philosophy
(`worker/main.py:258-275`) — infra failures are not target errors.

**Invalid metadata:** `TargetValidationError` if `metadata`/`set_fields` is
not `None` or a `dict`, or if any key in `remove_fields` is not a string.
No deeper shape validation (metadata is intentionally opaque, matching
`media_metadata`'s existing "cheap media facts, e.g. ffprobe-style
container fields" role — audit/identity.py docstring).

This is six classes, not a hierarchy per error — deliberately minimal, per
the brief's explicit "keep the error model minimal and useful."

## 17. Security considerations (operator-local input, not SSRF)

- `media_path` is trusted, operator-supplied local filesystem input — same
  conclusion the audit already reached (§13): this is not the crawler's
  `acquisition/ssrf_guard.py` URL-facing threat model, and no equivalent
  traversal defense is warranted (ordinary exists/is-file/readable
  validation is sufficient, §7).
- No symlink-following restriction — matches audit's explicit conclusion
  that this isn't attacker-controlled input; the OS's default
  symlink-follow behavior on `open()` is left as-is.
- `target_id`/`target_version` charset validation (§6) is itself a security
  property, not just hygiene: it is what makes `target_key()`'s unescaped
  `:`-join collision-free at the operator boundary.
- Logging: error messages for `TargetMediaError` may include the offending
  `media_path` (an operator needs it to fix the problem) but never file
  contents. Routine success logging (create/update/delete) should prefer
  `target_id`/`target_version`/`content_sha256` over the full `media_path`
  where the identity alone is enough context — a DESIGN DECISION, not a
  hard requirement, since `media_path` here is not a secret, only
  unnecessary in the common case.
- No new sandboxing, permission system, or credential handling is
  introduced — none of the new surface touches a trust boundary that
  doesn't already exist.

## 18. CLI design

`target/cli.py` (NEW), `python -m target.cli`, stdlib `argparse` only —
matching the `python -m worker.main` convention.

```
python -m target.cli add MEDIA_PATH --id ID --version VERSION [--metadata KEY=VALUE ...] [--json]
python -m target.cli list [--json]
python -m target.cli get ID --version VERSION [--json]
python -m target.cli update-metadata ID --version VERSION [--set KEY=VALUE ...] [--unset KEY ...] [--json]
python -m target.cli delete ID --version VERSION [--json]
```

- `--metadata`/`--set KEY=VALUE`: repeatable, always parsed as a **raw
  string** value (no implicit JSON/type coercion). DESIGN DECISION: this is
  a convenience for flat operator tags (`--set genre=action --set
  region=IN`), not a general JSON editor. A caller needing structured
  metadata values should go through `TargetService.create_target(...,
  metadata={...})` directly (e.g. from a future HTTP layer, which accepts a
  real JSON body) — the CLI's `--set` is deliberately the smaller of the
  two, matching "do not over-engineer."
- `--json`: on every subcommand, switches stdout to a single JSON object
  (or array, for `list`) instead of the human-readable text form. Useful for
  scripting and for early dashboard-backend development against the same
  CLI before an HTTP layer exists.
- **Exit codes:** `0` success; `1` any `TargetServiceError` (message printed
  to stderr — human mode: `f"{type(exc).__name__}: {exc}"`; JSON mode:
  `{"error": type(exc).__name__, "message": str(exc)}` on stdout); `2`
  argparse's own usage-error exit (unchanged stdlib behavior, not
  redefined).
- **What the CLI does, precisely:** parse `sys.argv`, construct a
  `TargetRegistry`/`TargetService` pair from environment (below), call
  exactly one `TargetService` method, format its return value or catch its
  typed exception. Nothing else. No validation logic duplicated from
  `TargetService` (an invalid `--id` is rejected by `TargetService.
  create_target` itself, surfaced as a normal `TargetValidationError` →
  exit 1, not pre-checked by the CLI), no direct Redis/filesystem access, no
  SHA-256 computation.

### Wiring — deliberately does NOT reuse `worker.main`

**DESIGN DECISION, with rationale.** `worker/main.py` already has exactly
the `WorkerConfig.from_env()` / `build_redis_client()` / `build_registry()`
functions this CLI needs (§3) — but `worker/main.py` imports `embedding.
dinov2_engine.DINOv2EmbeddingEngine` at module scope, which pulls in
torch/transformers. Importing `worker.main` from `target/cli.py` would make
`python -m target.cli list` pay a heavy ML-framework import cost (and a
hard dependency on torch being installed) for a command that only reads
three small Redis hashes. That's a bad tradeoff for an operator metadata
tool that should also be runnable from a lightweight ops host with no GPU
stack.

Instead, `target/cli.py` has its own small, self-contained env-driven
wiring — same **env var names** as `worker/main.py` for operational
consistency (`REDIS_URL`, `TARGET_CACHE_PATH`, `SHARED_ARTIFACT_STORE_PATH`
— documented in `docs/usage.md`), same `Redis.from_url(...)` construction
+ `ping()` fail-fast pattern as `build_redis_client`, same shared-vs-local
cache selection as `build_registry` — roughly 20 lines, duplicated rather
than imported. This is a deliberate, small, one-directional duplication
(CLI depends on nothing worker-specific; `worker/main.py` is untouched by
this phase, zero regression risk to the production worker path).

**Operational note for the design's audience:** because cache cleanup on
`delete` and media publication on `create` operate on whatever cache/media
paths the CLI was wired to, **the CLI must be run with the same `REDIS_URL`
/ `TARGET_CACHE_PATH` / `SHARED_ARTIFACT_STORE_PATH` configuration as the
worker fleet it's managing targets for** — otherwise `delete_target` cleans
up a cache directory no worker is actually reading from. This is stated
explicitly in the CLI's own `--help` epilog, not just this document.

### Stdout format (human mode)

- `add`: one line per field of the created/unchanged record, plus a
  `status: created|unchanged` line.
- `list`: one line per target — `target_id  target_version  content_sha256
  [:12]  updated_at (ISO 8601)` — tab-separated, sorted per §8.
- `get`: `field: value` lines, `media_metadata` pretty-printed as JSON.
- `update-metadata`: the updated `media_metadata` as JSON, plus `updated_at`.
- `delete`: `deleted target_id/target_version`.
- Any error: nothing on stdout, `ErrorClassName: message` on stderr, exit 1.

## 19. Future dashboard boundary

A future HTTP API layer (not built in this phase) is a third, equally-thin
client of `TargetService`, exactly like the CLI:

```
CLI (target/cli.py)              HTTP handler (future, e.g. target/http.py)
        │                                      │
        └──────────────┬───────────────────────┘
                        ▼
                 TargetService   (target/service.py)
```

Each HTTP endpoint would: parse the request body/path params, call one
`TargetService` method, map its typed exception (§16) to an HTTP status
(`TargetValidationError`/`TargetMediaError` → 400, `TargetNotFoundError` →
404, `TargetAlreadyExistsError` → 409, `TargetLockTimeoutError` → 503,
uncaught `RedisError` → 500), serialize the returned `TargetRecord` to
JSON. None of this requires any change to `TargetService`, `TargetRegistry`,
or anything below it — the boundary this phase builds is already exactly
what an HTTP layer needs. This phase does not choose or scaffold a
framework; that's the future phase's decision to make.

## 20. Performance considerations

- `list_targets()`: O(number of registered targets) — one `SMEMBERS` on the
  new index (§8), then one `HGETALL` per member. No `SCAN`, no
  keyspace-wide operation, ever.
- `delete_target()`: reads exactly two small hashes (`target_embeddings_key`,
  `target_segment_embeddings_key`) to discover which cache files exist —
  bounded by the number of `EmbeddingSpec`s ever registered for that one
  target (small, not global), then does exactly that many file
  deletes/`SharedArtifactStore` calls. No filesystem-wide scan, matching
  the brief's explicit constraint.
- `update_target_metadata()`: one `HGETALL` + one `HSET`, under the lock.
  No embedding recomputation, no media rehashing — the method signature
  makes this structurally impossible (§11), not just a convention.
- `create_target()` on the idempotent-content path still re-hashes the
  file every call (unchanged from today's `register_target` — the audit
  notes this as existing behavior, "no short-circuit if the path is
  unchanged," and this phase does not change it; short-circuiting would
  require trusting a caller-supplied hash or an mtime check, both weaker
  than "always verify," and out of scope for a metadata-lifecycle phase).
- No network calls happen during a normal `list`/`get` beyond the Redis
  round trips already described.

## 21. Existing Redis migration / index bootstrap

**The problem:** `fingerprint:target:index` does not exist in any
already-deployed Redis instance. Every target registered before this phase
ships is invisible to `list_targets()` until it is backfilled into the new
index — even though its registry hash, content-index membership, and caches
are all still perfectly valid and unaffected.

**DESIGN DECISION — smallest safe approach: a one-time, explicit,
operator-run repair command, not automatic startup repair.**

```
python -m target.cli reindex [--dry-run]
```

- Behavior: the **only** place in this design that performs an
  `SCAN fingerprint:target:*` keyspace walk — explicitly justified as a
  one-time migration cost, not a steady-state operation (the audit's
  performance objection to `SCAN` was about *list*, not a one-time repair).
  For each matched key, parse it defensively: a key matching
  `fingerprint:target:{id}:{version}` (excluding the `:embeddings`/
  `:segment_embeddings`/`:content:...` suffix forms, and tolerating `:` inside
  `id`/`version` for this pass specifically, since pre-migration data may
  predate the charset fix in §6) is a candidate target record. Confirm it
  really is one by `HGETALL` and checking the hash has the `TargetRecord`
  shape (`target_id`, `target_version`, `content_sha256`, ... fields present
  and self-consistent with the key). `SADD` its member into
  `fingerprint:target:index` using `encode_content_index_member` on the
  record's own `target_id`/`target_version` fields (read from the hash
  values, not re-derived from the possibly-ambiguous key text).
- `--dry-run`: prints what would be added, writes nothing.
- Idempotent: safe to run repeatedly (re-adding an already-indexed member is
  a Set no-op) and safe to run against a Redis instance that has already
  been fully migrated (finds nothing new to add).
- **Never deletes, modifies, or reinterprets any existing target record,
  embedding, cache entry, job, or result.** Purely additive — one Set,
  populated.
- **Not run automatically on every `TargetService`/CLI startup** (DESIGN
  DECISION): silent startup repair that scans the keyspace on every process
  boot is exactly the "network calls during normal operation" / "keyspace
  scan" cost §20 rules out, and it would also mean an operator has no
  explicit, loggable moment where the migration happened — worse for
  auditability. A one-time documented command is smaller and clearer.

**Stale content-index repair (audit §4's pre-existing gap):** this design
does not add a bulk repair tool for reverse-index entries left stale by
`register_target` calls made *before* this phase's §9 fix. Rationale: a
stale content-index entry is a **latent, non-crashing** correctness gap
(audit's own words) — it can cause `find_by_content_hash()` to
over-report referents for an old, no-longer-current hash, which in turn
means `delete_target`'s shared-media reference check (§12 step 7) could
conclude "still referenced" for a blob that (from the stale entry's
perspective) genuinely still has a phantom referent — the failure mode is
"leak a blob a little longer than necessary," never "delete something
still in use." Given that failure direction is always safe, and no test or
production code currently reads `find_by_content_hash` results for any
decision more consequential than this one (verified in the audit), a
targeted repair tool is not proposed in this phase — flagged as an open
question in §29 for the next phase to revisit if it proves to matter in
practice.

## 22. Existing data compatibility

Nothing in this design deletes, renames, or reshapes any existing Redis key,
cache file, or `SharedArtifactStore` blob. Every existing `TargetRecord`,
embedding, segment embedding, shared media blob, job, and result survives
this phase's deployment untouched. The only new durable state is:
- One new Redis key pattern: `fingerprint:target:index` (empty until §21's
  `reindex` is run once).
- One new Redis lock key pattern: `fingerprint:lock:target-record:{id}:
  {version}` (transient, TTL'd, never persisted meaningfully).

A deployment can apply this phase's code, run `python -m target.cli reindex`
once, and have `list_targets()` immediately reflect every pre-existing
target — no downtime-requiring migration, no target namespace wipe.

## 23. Backwards compatibility

- `TargetRegistry.register_target`'s signature gains one new keyword-only
  parameter (`on_conflict: str = "replace"`) with a default that preserves
  its exact current behavior — every existing caller (including
  `tests/test_target.py`, benchmarks, `docs/usage.md`'s documented snippet)
  keeps working unchanged.
- `TargetRegistry.get_target`, `find_by_content_hash`,
  `register_embedding`, `get_compatible_embedding`,
  `register_segment_embedding`, `get_or_build_segment_embedding` — all
  unchanged.
- `target/keys.py`, `target/identity.py`, `target/versioning.py` — all
  unchanged (only additive `delete()` methods land in `target/cache.py`,
  `target/segment_cache.py`, `target/shared_cache.py`,
  `target/shared_storage.py`; existing methods on those classes are
  untouched).
- No existing test's assertions are invalidated by any change in this
  design (the one behavior that changes — `register_target`'s stale-index
  bug — is fixed underneath the existing tests, which don't assert the
  buggy behavior; they assert the *new-hash* cache invalidation, which
  still holds).

## 24. Test plan for the NEXT phase (not written in this phase)

**CREATE**
- Valid target creates successfully; record fields match input.
- Invalid `target_id`/`target_version` (`:`, whitespace, empty, >128 chars,
  control characters) → `TargetValidationError`, no Redis write.
- Missing file, directory, empty file, unreadable file → `TargetMediaError`,
  no Redis write.
- Re-`create_target` with identical bytes → idempotent, `created_at`
  preserved, `updated_at` advances, no `TargetAlreadyExistsError`.
- Re-`create_target` with different bytes, same `(id, version)` →
  `TargetAlreadyExistsError`, existing record unchanged, old content-index
  membership intact (nothing was touched).
- Same content under a new `target_version` → succeeds, both versions
  coexist, both show up under `find_by_content_hash`.
- Multiple distinct targets created in the same test → all independently
  gettable/listable.

**LIST**
- Multiple targets → all present, sorted deterministically.
- Multiple versions of one `target_id` → both present as distinct entries.
- Two calls with no intervening writes → identical output (determinism).
- A manually-injected stale index member (record deleted directly via
  `TargetRegistry`-internal Redis calls, bypassing `delete_target`) →
  `list_targets()` skips it, does not raise.

**GET**
- Existing `(id, version)` → matches what was created/last updated.
- Missing `(id, version)` → `None`, not an exception.

**UPDATE**
- `set_fields` merges into existing metadata (existing untouched keys
  survive).
- `remove_fields` removes named keys; a key in both `set_fields` and
  `remove_fields` ends up removed.
- `content_sha256`/`media_path`/`target_id`/`target_version` unchanged
  after any metadata update.
- `updated_at` advances; `created_at` does not.
- Missing target → `TargetNotFoundError`, no write.

**DELETE**
- Registry hash gone after delete (`get_target` → `None`).
- List-index membership gone (`list_targets()` no longer includes it).
- Content-index membership gone for that target's own hash.
- Target-exclusive pooled/segment cache files removed (both local and
  shared-backend configurations).
- Shared media **retained** when a second `(id, version)` still references
  the same `content_sha256`.
- Shared media **deleted** when no target references that hash anymore.
- `ResultRecord`s referencing the deleted target are still readable
  unchanged after delete.
- A job already enqueued against the deleted target, processed after
  deletion, fails as `PermanentFailure` via the existing unknown-target path
  (§13's policy, end-to-end).
- Deleting a missing target → `TargetNotFoundError`.

**CONCURRENCY**
- Two threads/processes `create_target`-ing the same `(id, version)` with
  different content simultaneously → exactly one succeeds, the other gets
  `TargetAlreadyExistsError` or, if it started first, succeeds and the
  other conflicts against it — never a silently-corrupted mixed record.
- `delete_target` racing `register_target`/`create_target` on the same
  identity → fully serialized, no interleaved partial state.
- Two concurrent `delete_target` calls on the same identity → exactly one
  succeeds, the other gets `TargetNotFoundError`, never a double-delete
  side effect (e.g. double `SREM` on shared media, which would be harmless
  here but should still be asserted as safe).
- `update_target_metadata` racing `delete_target` → serialized, no metadata
  write "resurrects" a field on an already-deleted record.

**REGRESSION**
- Content-changing `register_target` (default `on_conflict="replace"`,
  called directly, mirroring the existing
  `test_cache_miss_for_different_target_content_hash` pattern) no longer
  leaves a stale `target_content_index_key` entry for the *old* hash
  (`find_by_content_hash(old_hash)` no longer includes this target after
  the re-registration).
- Two targets with byte-identical content remain independently
  gettable/listable/deletable — deleting one does not affect the other's
  record, and does not delete their shared media blob.
- Cache keys remain collision-free across targets with colliding-looking
  but distinct `(id, version)` pairs once charset validation is enforced.
- Full existing suite (`tests/test_target.py`, `tests/test_target_lock.py`,
  `tests/test_target_build_on_miss.py`, `tests/test_shared_target_storage.py`,
  `tests/test_segment_cache.py`, `tests/test_matching_handler.py`,
  `tests/test_integration_e2e.py`) continues to pass unmodified.

**CLI**
- `add`/`list`/`get`/`update-metadata`/`delete` each in human and `--json`
  mode.
- Invalid arguments (missing required flag, unknown subcommand) → argparse's
  own exit code 2 behavior.
- Each `TargetServiceError` subtype surfaces as exit code 1 with the
  documented stderr/JSON shape.
- `reindex` (§21): populates the index from pre-existing keys,
  `--dry-run` writes nothing, idempotent on repeat runs.

None of the above is implemented in this phase.

## 25. Implementation file list

| File | Change | Responsibility | What does NOT change |
|---|---|---|---|
| `target/service.py` | **NEW** | `TargetService` class (§5); six error classes (§16); `_validate_identifier`/media validation helpers (§6/§7). | No Redis/filesystem access of its own — only calls `TargetRegistry`. |
| `target/registry.py` | Extended | `register_target` gains `on_conflict` param + internal locking + SREM-before-SADD fix (§9). New methods: `list_targets`, `update_target_metadata`, `delete_target` (§8/§11/§12), all lock-guarded via `target_record_lock_key`. Imports `TargetAlreadyExistsError`/`TargetNotFoundError` from `target/service.py`. | `get_target`, `find_by_content_hash`, `register_embedding`, `get_compatible_embedding`, `register_segment_embedding`, `get_or_build_segment_embedding` — all untouched. |
| `target/keys.py` | Extended | New `target_record_lock_key(target_id, target_version)` helper (§9). `target_content_index_key`, `target_key`, `encode_content_index_member`/`decode_content_index_member` reused verbatim for the list-index (§8) — no change needed there, they're already generic enough. | `target_key()`'s own `:`-join format — unchanged (the fix is validation at the boundary, §6, not a key-format change). |
| `target/cache.py` | Extended | `TargetEmbeddingCache.delete` abstract method + `FilesystemEmbeddingCache.delete` implementation (§14). | Storage format, `get`/`put`/`exists`, `_load_and_validate`. |
| `target/segment_cache.py` | Extended | Same shape as above, segment variant. | Same. |
| `target/shared_cache.py` | Extended | `SharedFilesystemEmbeddingCache.delete` / `SharedFilesystemSegmentEmbeddingCache.delete`, both call into `SharedArtifactStore.delete`. | Payload format, `_validate`. |
| `target/shared_storage.py` | Extended | `SharedArtifactStore.delete(key)` (new primitive); `SharedTargetMediaStore.delete(content_sha256)` (new, calls the above). | `publish`/`fetch_to_temp`, atomic-write pattern, failure semantics. |
| `target/cli.py` | **NEW** | argparse CLI (§18): `add`/`list`/`get`/`update-metadata`/`delete`/`reindex` subcommands; self-contained env wiring (deliberately not importing `worker.main`, §18). | Does not implement any lifecycle logic itself. |
| `docs/usage.md` | Documentation only | Replace "There is no CLI for this — call `TargetRegistry.register_target` directly" with the new `target.cli` usage, once implemented. | Not part of this design phase's code; noted for completeness of the eventual PR. |

No changes to: `target/identity.py`, `target/versioning.py`, `work_queue/*`,
`worker/*` (including `worker/main.py` itself — deliberately, §18),
`integration/*`, `matching/*`, any existing test file, any config file,
Redis itself.

## 26. Explicit out-of-scope items

- No HTTP framework, REST/GraphQL layer, or dashboard backend — §19
  describes the boundary only.
- No SQL, ORM, or second persistence system — Redis remains sole source of
  truth.
- No `target → job` or `target → result` reverse index — §13's policy is
  chosen specifically to avoid needing one.
- No soft-delete / inactive-target state.
- No bulk stale-content-index repair tool (§21's closing note) — flagged as
  an open question, not built.
- No rename of `encode_content_index_member`/`decode_content_index_member`
  (§8) — reused as-is.
- No change to `worker/main.py`, `worker/fingerprint_worker.py`,
  `worker/matching_handler.py`, or any work-queue module.
- No change to the crawler-facing `integration.submission`/
  `integration.candidate` boundary, and no submission-time target-existence
  check (the audit flagged this as a pre-existing, intentional, minimal
  boundary in phase-12 scope, not a target-management gap — unchanged here).
- No sandboxing, filesystem permission system, or SSRF-style defenses for
  `media_path` — it is trusted operator-local input (§17).
- No tests written in this phase (§24 specifies what the next phase must
  write).
- No object-storage/S3 backend, no new locking primitive, no binary cache
  format — all explicitly ruled out by the brief and not revisited.

## 27. Risks / unresolved questions

1. **`TargetAlreadyExistsError` cross-module import** (§16): defining it in
   `target/service.py` and importing it into `target/registry.py` is a
   slightly unusual direction (a lower-level module importing from a
   higher-level one). The alternative — define the six error classes in
   `target/registry.py` instead, and have `target/service.py` just
   re-export them — is equally valid and avoids the direction question
   entirely. **Recommend resolving this at implementation time in whichever
   direction the implementer finds reads more naturally**; it has no
   behavioral consequence, only where one class lives.
2. **Stale content-index repair for pre-existing data** (§21's closing
   note): deliberately not built this phase, on the reasoning that its
   failure mode (delayed shared-media GC, never incorrect deletion) is
   benign. Flagging for explicit sign-off rather than silently deciding —
   if there's a known deployment with a large number of historically
   content-swapped targets, this tradeoff may be worth revisiting.
3. **`update_target_metadata`'s shallow-merge choice** (§11): reasonable
   and simple, but if any current or near-future consumer of
   `media_metadata` expects nested structure (e.g. an ffprobe-style nested
   dict) where a partial nested update is a common operator need, shallow
   merge will surprise them (a `--set` of one nested key clobbers its
   siblings). No such consumer exists in the codebase today
   (`media_metadata` is opaque, read by nothing but `TargetRecord` itself)
   — flagged in case that changes before implementation.
4. **CLI env-wiring duplication vs. extraction** (§18): this design chose
   to duplicate ~20 lines of Redis/cache wiring in `target/cli.py` rather
   than extract `build_redis_client`/`build_registry` out of `worker/main.py`
   into a shared, import-light module both could use. The extraction would
   be a cleaner long-term shape but touches `worker/main.py`, which this
   phase's brief scoped as untouchable. Flagging as a reasonable future
   cleanup, not a requirement.
