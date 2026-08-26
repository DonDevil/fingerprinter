# Target Management Implementation — CRUD / Multi-Target Operator Interface

## 1. Status

**IMPLEMENTED.** This document describes the code actually merged for the
target lifecycle (create/list/get/update-metadata/delete + reindex),
following:

- `docs/architecture/target-management-audit.md` — read-only audit of the
  pre-existing `TargetRegistry` architecture.
- `docs/architecture/target-management-design.md` — the approved design
  this implementation follows.

Unlike those two documents, this one describes **what exists in the
repository today**, not a proposal. Every code excerpt below is copied from
the actual merged source, not reconstructed from memory. Three places where
the implementation deviates from the design document's literal text are
called out explicitly in §14, each with the reason.

## 2. What was delivered

| Capability | Entry point |
|---|---|
| Create a target (idempotent on identical content, rejects content swaps) | `TargetService.create_target` / `target.cli add` |
| List every registered target | `TargetService.list_targets` / `target.cli list` |
| Get one target | `TargetService.get_target` / `target.cli get` |
| Patch a target's metadata (set/remove, never content) | `TargetService.update_target_metadata` / `target.cli update-metadata` |
| Delete a target (cache cleanup, shared-media reference counting) | `TargetService.delete_target` / `target.cli delete` |
| One-time backfill of the new list index against pre-existing Redis data | `TargetService.reindex` / `target.cli reindex` |

Plus, underneath that surface: a registration-race fix, a stale
reverse-index fix, and four new `delete()` cache primitives — see §5-§8.

## 3. Architecture (as built)

```
target.cli (operator CLI, thin, stdlib argparse only)
        │
        ▼
target.service.TargetService (operator validation + policy)
        │
        ▼
target.registry.TargetRegistry (Redis + injected cache/media-store collaborators)
        │
   ┌────┼──────────────────────────┐
   ▼    ▼                          ▼
 Redis  TargetEmbeddingCache /     SharedTargetMediaStore
        SegmentEmbeddingCache      (content-addressed, shared
        (local or shared-backend)  across targets)
```

`TargetService` never imports `redis`. `target/cli.py` never imports
anything from `redis` except to construct a client and hand it to
`TargetRegistry` — it never issues a Redis command itself. Every Redis
command target lifecycle code issues lives inside `target/registry.py`.

## 4. New module: `target/errors.py`

```python
class TargetServiceError(Exception): ...
class TargetValidationError(TargetServiceError, ValueError): ...
class TargetMediaError(TargetServiceError, OSError): ...
class TargetNotFoundError(TargetServiceError, KeyError): ...
class TargetAlreadyExistsError(TargetServiceError, ValueError): ...
class TargetLockTimeoutError(TargetServiceError, TimeoutError): ...
```

Six flat classes, each multiply-inheriting `TargetServiceError` plus the
closest matching built-in exception — the same shape as this repository's
existing `JobValidationError(ValueError)`, `CandidateValidationError
(ValueError)`, `ConfigError(ValueError)`, `SharedArtifactStoreError
(OSError)`. A caller can catch the specific class, the shared
`TargetServiceError` base, or (because of the built-in parent) the familiar
Python type — `except KeyError` still catches `TargetNotFoundError`.

**Why its own module, not `target/service.py`:** `target/registry.py`
needs to *raise* `TargetAlreadyExistsError` and `TargetNotFoundError`
itself (see §7), and `target/service.py` is built on top of
`target/registry.py`. Putting the exceptions in `target/service.py` would
make the lower-level module import from the higher-level one. `target/
errors.py` has zero imports from anything else in `target/`, so both
`registry.py` and `service.py` import from it with no cycle.

## 5. `target/keys.py` additions

```python
def target_index_key() -> str:
    return "fingerprint:target:index"

def target_record_lock_key(target_id: str, target_version: str) -> str:
    return f"fingerprint:lock:target-record:{target_id}:{target_version}"
```

- `target_index_key()` — one unparameterized key: a Redis Set holding every
  registered `(target_id, target_version)` pair, member-encoded with the
  **existing** `encode_content_index_member`/`decode_content_index_member`
  helpers (unchanged, reused verbatim — no second encoding scheme).
- `target_record_lock_key(id, version)` — the lifecycle-lock namespace.
  Deliberately shaped differently from the pre-existing `target_lock_key
  (cache_key)` (which scopes the *build-on-miss* lock to a full
  `cache_entry_key` — target + content + embedding spec) so the two lock
  families can never collide on the same Redis key even for the same
  target.

Nothing about `target_key()`'s own `:`-joined format changed — the
`target_key()`-collision class the audit flagged is closed by validating
`target_id`/`target_version` at the `TargetService` boundary (§10), not by
reshaping the key.

## 6. Cache/storage `delete()` primitives

Four classes gained one additive method each, matching their existing
`get`/`put`/`exists` signature shape exactly. No storage format changed, no
existing method's behavior changed.

**`target/cache.py`** — `TargetEmbeddingCache` (ABC) gained an abstract
`delete`; `FilesystemEmbeddingCache` implements it:

```python
def delete(self, target_id: str, target_version: str, content_sha256: str, spec: EmbeddingSpec) -> bool:
    path = self._path_for(target_id, target_version, content_sha256, spec)
    if not path.exists():
        return False
    path.unlink()
    return True
```

**`target/segment_cache.py`** — identical shape on `SegmentEmbeddingCache`/
`FilesystemSegmentEmbeddingCache`.

**`target/shared_storage.py`** — `SharedArtifactStore` gained:

```python
def delete(self, key: str) -> bool:
    path = self._path_for(key)
    if not path.exists():
        return False
    try:
        path.unlink()
        return True
    except OSError as exc:
        raise SharedArtifactStoreError(f"failed to delete {key!r} from shared artifact store: {exc}") from exc
```

— same "absent (`False`) vs. unreachable (raises)" contract as
`get_bytes`/`put_bytes` already use, never conflating the two. And
`SharedTargetMediaStore`:

```python
def delete(self, content_sha256: str) -> bool:
    return self._store.delete(self._key(content_sha256))
```

**`target/shared_cache.py`** — `SharedFilesystemEmbeddingCache.delete` /
`SharedFilesystemSegmentEmbeddingCache.delete` both just forward to
`self._store.delete(self._key(...))`.

Return contract everywhere: `True` iff something was actually removed,
`False` iff it was already absent — idempotent, safe to call twice, never
raises for a plain miss (raising is reserved for the store itself being
unreachable, `SharedArtifactStoreError` only).

**Why these were needed:** verified from source before writing any of
this — none of the four cache classes, nor `SharedArtifactStore`/
`SharedTargetMediaStore`, had any delete-capable method before this
change.

## 7. `target/registry.py` — the locked lifecycle primitives

### 7.1 New module constants

```python
LIFECYCLE_LOCK_TTL_MS = 30_000  # 30 seconds
LIFECYCLE_LOCK_POLL_INTERVAL_S = 0.1
LIFECYCLE_LOCK_POLL_TIMEOUT_S = 5.0
```

Deliberately far shorter than the pre-existing build-on-miss defaults
(`DEFAULT_LOCK_TTL_MS = 600_000`, i.e. 10 minutes) — a create/update/delete
is a handful of Redis writes plus a few file deletes, not an embedding
build, and lifecycle operations are operator-driven, not a hot path where a
loser should expect to wait minutes.

### 7.2 The one lock, shared by every mutation

```python
def _acquire_lifecycle_lock(self, target_id: str, target_version: str) -> RedisLock:
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
```

`register_target`, `update_target_metadata`, and `delete_target` **all**
call this exact method, on the exact same key
(`target_record_lock_key(target_id, target_version)`), before touching any
Redis state, and release it in a `finally`. This single invariant — "every
mutation of one identity is serialized behind one lock" — is what makes
create-vs-create, create-vs-delete, delete-vs-delete, and delete-vs-update
all safe with no per-pair special-casing. `target/lock.py`'s `RedisLock`
itself was not modified; this reuses the existing `SET NX PX` / Lua-CAS
primitive as-is.

### 7.3 `register_target` — `on_conflict`, locking, and the stale-index fix

New signature (backward compatible — `on_conflict` is keyword-only with a
default that reproduces the pre-existing behavior exactly):

```python
def register_target(
    self,
    target_id: str,
    target_version: str,
    media_path: str,
    media_metadata: Optional[dict] = None,
    *,
    on_conflict: str = "replace",
) -> TargetRecord:
```

Body, in order:

1. Validate `on_conflict in ("replace", "reject")` (plain `ValueError` —
   this is a programming-contract check, not an operator-facing condition;
   `TargetService` never passes anything but `"reject"`).
2. `content_sha256 = sha256_file(media_path)` — **before** the lock, so the
   lock is never held for however long streaming a large file takes.
3. If a `media_store` was injected, publish the media (unchanged from
   before this phase — content-addressed and idempotent, so publishing
   speculatively before knowing whether the call will conflict is
   harmless).
4. Acquire the lifecycle lock.
5. Inside the lock: read the existing record. If one exists with a
   **different** `content_sha256`:
   - `on_conflict="reject"` → raise `TargetAlreadyExistsError`, write
     nothing.
   - `on_conflict="replace"` (default) → `SREM` this target's membership
     from the **old** content hash's reverse-index Set
     (`target_content_index_key(existing.content_sha256)`) *before*
     writing anything else. This is the fix for the audit's stale-index
     bug (§7.4 below).
6. Write the `TargetRecord` hash, `SADD` the (possibly new) content-index
   membership, `SADD` the list-index membership (`fingerprint:target:index`
   — idempotent even on re-registration, `SADD` of an already-present
   member is a no-op).
7. Release the lock (`finally`).

Every existing direct caller of `register_target` (tests, benchmarks,
`docs/usage.md`'s old snippet) is unaffected: `on_conflict` defaults to
`"replace"`, which is exactly what the method did before this phase, minus
the now-fixed stale-index bug.

### 7.4 The stale content-index bug, precisely

Before this phase: re-registering `(target_id, target_version)` with
different bytes `SADD`ed the *new* content hash's reverse-index Set but
never `SREM`ed the target's membership from the *old* one. So
`find_by_content_hash(<old hash>)` kept returning a target whose content no
longer matched that hash.

Fixed by the `SREM` in step 5 above, executed under the same lock that
makes the whole read-decide-write sequence atomic with respect to other
mutators of the same identity. Regression-tested in
`tests/test_target_lifecycle.py::test_content_changing_reregistration_removes_stale_content_index_entry`.

### 7.5 `list_targets`

```python
def list_targets(self) -> list[TargetRecord]:
    members = self._redis.smembers(target_index_key())
    pairs = sorted(decode_content_index_member(member) for member in members)
    records = []
    for target_id, target_version in pairs:
        record = self.get_target(target_id, target_version)
        if record is not None:
            records.append(record)
    return records
```

One `SMEMBERS` + one `HGETALL` per member — O(number of registered
targets), never a keyspace scan. `SMEMBERS` has no defined order, so
results are sorted by `(target_id, target_version)` before resolution,
giving deterministic output on every call. A member whose record no longer
exists (the only realistic cause: a crash between the `SADD`/`DEL` pair, or
an unrepaired pre-migration gap) is silently skipped rather than failing
the whole call.

### 7.6 `update_target_metadata`

```python
def update_target_metadata(
    self,
    target_id: str,
    target_version: str,
    set_fields: Optional[dict] = None,
    remove_fields: Optional[Sequence[str]] = None,
) -> TargetRecord:
```

Under the lifecycle lock: reads the existing record (`TargetNotFoundError`
if missing), builds `metadata = dict(existing.media_metadata)`, applies
`metadata.update(set_fields or {})` (shallow merge — a nested dict value in
`set_fields` replaces the corresponding existing value wholesale, it is not
recursively merged), then `metadata.pop(key, None)` for every key in
`remove_fields` (so a key present in *both* `set_fields` and
`remove_fields` ends up removed — set happens first, remove happens
second). Writes a new `TargetRecord` with `created_at=existing.created_at`
(preserved) and no `updated_at=` argument, so `TargetRecord`'s own
`default_factory=time.time` gives it a fresh value. There is no parameter
for `media_path`, `content_sha256`, `target_id`, or `target_version` — a
content swap is not merely disallowed by convention, it is structurally
impossible through this method's signature.

### 7.7 `delete_target`

```python
def delete_target(self, target_id: str, target_version: str) -> None:
```

Under the lifecycle lock, in this exact order:

1. `record = self.get_target(...)`; `TargetNotFoundError` if `None`.
2. `pooled_specs, segment_specs = self._cached_embedding_specs(target_id, target_version)` —
   reads `target_embeddings_key`/`target_segment_embeddings_key` (the
   small, vector-free Redis summary hashes `register_embedding`/
   `register_segment_embedding` already maintain) and reconstructs one
   `EmbeddingSpec` per stored entry from its `to_metadata_fields()` JSON
   (`model_id`, `model_version`, `embedding_schema_version`,
   `preprocessing_config`, `sampling_config`). This is the **only** way to
   know which cache files a target owns without scanning the filesystem —
   the summary hashes exist for exactly this reason.
3. For each pooled spec: `self._cache.delete(target_id, target_version,
   record.content_sha256, spec)`. For each segment spec (if a
   `segment_cache` was injected): the segment-cache equivalent. Safe
   unconditionally — `cache_entry_key` includes `(target_id,
   target_version)`, so these files can never be shared with another
   target.
4. `SREM` this target's membership from
   `target_content_index_key(record.content_sha256)`.
5. `SREM` this target's membership from `target_index_key()`.
6. `DEL` the three Redis hashes for this identity (`target_key`,
   `target_embeddings_key`, `target_segment_embeddings_key`), issued
   together through one `redis.pipeline()`.
7. `remaining = self.find_by_content_hash(record.content_sha256)` — because
   step 4 already removed *this* target's own membership, `remaining`
   reflects only *other* targets. If empty **and** a `media_store` was
   injected: `self._media_store.delete(record.content_sha256)`.
8. Release the lock (`finally`).

**The one ordering constraint that matters and is enforced by the code
above:** step 4 (removing this target's own content-index membership) runs
*before* step 7 (the reference check). Reversing that order would make
`find_by_content_hash` always see this target as a referent of its own
content, so shared media for a genuinely-orphaned target would never be
collected.

**Failure-partway-through is not distributed-transaction-safe by
construction — it's safe by *ordering*.** A crash after step 3 (cache
files gone) but before step 6 (registry hash still present) leaves a
target that still resolves via `get_target`, with any now-deleted spec
simply a cache miss — indistinguishable from "never built," which the
existing build-on-miss path already handles correctly. A crash after step
6 (registry gone) but before step 7 leaks the shared media blob (never
deleted, never corrupted — a disk-hygiene cost, not a correctness bug).
Nothing here needed a transaction coordinator; the ordering alone keeps
every intermediate state safe.

**Explicitly untouched, by design:** `ResultRecord`s
(`fingerprint:result:*`, `fingerprint:job:*:result` — historical, keyed by
`job_id`, not target-owned), queued/in-flight Redis Stream job entries (see
§9), and the pre-existing build-on-miss lock (`fingerprint:lock:target:*`
— self-expiring by TTL, nothing to clean up).

### 7.8 `reindex`

```python
def reindex(self, dry_run: bool = False) -> ReindexResult:
```

The **only** place in this codebase's target-management code that issues a
Redis keyspace `SCAN` (via `scan_iter(match="fingerprint:target:*")`) —
never run as part of `list_targets`/`get_target`/`create_target`/etc., and
never run automatically at construction, import, or CLI startup.

For each matched key: skip it if it's `target_index_key()` itself, or ends
with `:embeddings`/`:segment_embeddings` (the summary hashes), or its Redis
`TYPE` isn't `hash` (excludes the content-index Sets). For everything that
survives that filter, `HGETALL` it and check the result has all of
`{target_id, target_version, media_path, content_sha256, created_at,
updated_at}` present before attempting `TargetRecord.from_hash_fields`
(wrapped in `try/except (KeyError, ValueError)`, skipping anything
malformed rather than crashing the whole repair). The identity SADDed into
the index comes from the **hash's own stored `target_id`/`target_version`
field values**, never from parsing the Redis key text — this is what lets
the repair tolerate a `:` inside a legacy identifier that predates the
charset validation in §10, since a colon inside a legacy id would make the
key text ambiguous but never makes the hash's own fields ambiguous.

Returns `ReindexResult(found: list[(str, str)], added: list[(str, str)])`
— `found` is every valid record the scan turned up; `added` is the subset
that wasn't already indexed (in `dry_run=True` mode, what *would* have been
added — no `SADD` is issued in that mode). Idempotent: a second run against
an already-repaired instance finds `added == []`.

## 8. `target/service.py` — the operator boundary

### 8.1 Identifier validation

```python
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_IDENTIFIER_MAX_LENGTH = 128

def _validate_identifier(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= _IDENTIFIER_MAX_LENGTH)
        or not _IDENTIFIER_PATTERN.match(value)
    ):
        raise TargetValidationError(...)
```

One shared validator, applied identically to `target_id` and
`target_version`. The pattern is a strict allow-list (letters, digits,
`.`, `_`, `-`), so it rejects `:` (closing `target_key()`'s collision
class), whitespace, and control characters all in one check, with no
separate cases needed. No silent trimming — `" blast"` and `"blast "` are
both rejected, not normalized, so two operators who typed visibly different
strings can never silently collide on one identity. Length is capped at
128 characters — a hygiene bound (Redis itself has no such limit), chosen
to reject pathological input while comfortably fitting real slugs like
`tamil_blasters` or `movie-2026`.

Only called from `create_target` — `get_target`/`list_targets`/
`update_target_metadata`/`delete_target` don't re-validate an identifier
that would already have had to pass validation to exist as a target
created through this service.

### 8.2 Media validation

```python
def _validate_media_path(media_path: Union[str, Path]) -> Path:
```

In order: `path.exists()` → `TargetMediaError` if not; `path.is_dir()` →
`TargetMediaError` if so; `path.is_file()` → `TargetMediaError` if not (a
regular-file check, catches special files); `path.stat().st_size == 0` →
`TargetMediaError`; a 1-byte probe read (`open(path, "rb").read(1)`) inside
a `try/except OSError` → `TargetMediaError` wrapping the original as
`__cause__` if it fails. No raw `FileNotFoundError`/`IsADirectoryError`/
`OSError` ever escapes `create_target`.

### 8.3 `create_target` and the conflict policy

```python
def create_target(self, target_id, target_version, media_path, metadata=None) -> TargetRecord:
    _validate_identifier(target_id, "target_id")
    _validate_identifier(target_version, "target_version")
    path = _validate_media_path(media_path)
    metadata = _validate_metadata(metadata)

    return self._registry.register_target(
        target_id, target_version, str(path),
        media_metadata=metadata, on_conflict="reject",
    )
```

`TargetService.create_target` is the **only** caller anywhere in this
codebase that passes `on_conflict="reject"`. That one keyword argument is
the entire mechanism behind the three-way behavior:

| Existing record? | Content matches? | Outcome |
|---|---|---|
| No | — | Created normally |
| Yes | Identical (`content_sha256` equal) | Idempotent success — `created_at` preserved, `updated_at` advances, **metadata is replaced by whatever this call's `metadata` argument says** (an omitted/default `metadata=None` resets stored metadata to `{}` — this is a deliberate "this call declares the full desired state" contract for `create_target`, distinct from `update_target_metadata`'s patch semantics; see §8.4) |
| Yes | Different | `TargetAlreadyExistsError`, existing record completely untouched |

To register different content under the same `target_id`, call
`create_target` again with a new `target_version` — already fully
supported, no code path added for it. To change only metadata without
touching content, use `update_target_metadata` instead.

### 8.4 `update_target_metadata`, `delete_target`, `list_targets`, `get_target`, `reindex`

All five are thin: `update_target_metadata` validates `set_fields`/
`remove_fields` shape (`TargetValidationError` if `set_fields` isn't a
dict, or any `remove_fields` entry isn't a string) and delegates;
`delete_target`, `list_targets`, `reindex` delegate with no extra
validation; `get_target` delegates and returns `None` on a miss rather than
raising — deliberately different from the mutation methods, which raise
`TargetNotFoundError`. This asymmetry is intentional: a `get` miss is a
normal, expected outcome of a lookup (mirrors `dict.get()`), while a
`delete`/`update` targeting something that doesn't exist is an operator
error worth surfacing loudly.

## 9. Active-job deletion policy — what actually runs

No new code touches job/result state at all. Deleting a target's registry
record is sufficient by itself: `worker/matching_handler.py`'s existing
`TargetRegistry.get_target()`/`get_or_build_segment_embedding()` calls
raise `KeyError` for an identity that doesn't resolve, which the existing
worker error taxonomy already maps to `PermanentFailure` — the same path a
job against a target that was *never* registered has always taken. This
was proven, not just asserted, by adding one new test to the existing
`tests/test_matching_handler.py`:

```python
def test_deleted_target_raises_permanent_failure_same_as_unknown_target(engine, registry, make_job, tmp_path):
    registry.register_target("target-1", "v1", str(TINY_VIDEO))
    registry.delete_target("target-1", "v1")
    ...
    with pytest.raises(PermanentFailure):
        handler(job)
```

`worker/matching_handler.py` itself was not modified — this test only
proves the pre-existing code path still fires correctly once a target has
gone through the new `delete_target`.

## 10. `target/cli.py` — the thin client

### 10.1 Command reference

```
python -m target.cli add MEDIA_PATH --id ID --version VERSION [--metadata KEY=VALUE ...] [--json]
python -m target.cli list [--json]
python -m target.cli get ID --version VERSION [--json]
python -m target.cli update-metadata ID --version VERSION [--set KEY=VALUE ...] [--unset KEY ...] [--json]
python -m target.cli delete ID --version VERSION [--json]
python -m target.cli reindex [--dry-run] [--json]
```

Every subcommand maps to exactly one `TargetService` call
(`_cmd_add`/`_cmd_list`/`_cmd_get`/`_cmd_update_metadata`/`_cmd_delete`/
`_cmd_reindex` in `target/cli.py`, each under 20 lines: parse → one service
call → format). `--metadata`/`--set` values are always raw strings — no
implicit JSON/int/bool coercion; `KEY=VALUE` without an `=` is an
`argparse.ArgumentTypeError`, which argparse itself turns into a usage
error (exit 2).

### 10.2 Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | A `TargetServiceError` was raised, **or** `get` found nothing (a deliberate CLI-level choice — `get_target` itself returns `None`, not an error, but a missing-target `get` still exits nonzero so scripts can branch on it) |
| `2` | argparse usage error (missing required flag, unknown subcommand, bad `KEY=VALUE`) — stock `argparse` behavior, not reimplemented |

`main()`'s structure:

```python
def main(argv=None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)          # raises SystemExit(2) on its own for usage errors
    try:
        service = _build_service()
        return args.func(service, args) or 0
    except TargetServiceError as exc:
        ...
        return 1
```

A `redis.exceptions.RedisError` (Redis unreachable) or
`SharedArtifactStoreError` (configured shared-store path unreachable) is
**not** caught here — it propagates as an unhandled Python exception
(traceback + Python's own exit code 1). This mirrors the design's explicit
choice not to wrap infrastructure failures behind a target-specific error
type (`build_redis_client`'s own fail-fast-on-`ping()` philosophy in
`worker/main.py`).

### 10.3 Output shapes

Human mode: `field: value` lines for `add`/`get`; tab-separated
`target_id / target_version / content_sha256[:12] / updated_at` per line
for `list`; `media_metadata` + `updated_at` for `update-metadata`;
`deleted {id}/{version}` for `delete`.

`--json` mode: a single JSON object (array for `list`) written to stdout.
`add`/`get`/`update-metadata` emit the full record
(`target_id`, `target_version`, `media_path`, `content_sha256`,
`media_metadata`, `created_at`, `updated_at`) — `add` additionally includes
`"status": "ok"`. `delete` emits `{"status": "deleted", "target_id": ...,
"target_version": ...}`. `reindex` emits `{"dry_run": bool, "found": [...],
"added": [...]}`. An error in `--json` mode prints
`{"error": "<ExceptionClassName>", "message": "<str(exc)>"}` to **stdout**
(matching the JSON-mode convention that all machine-readable output, success
or failure, is one JSON value on stdout) rather than stderr, which is where
the human-mode error line goes instead.

### 10.4 Environment wiring — why it doesn't import `worker.main`

`target/cli.py` has its own ~30-line `_build_redis_client`/`_build_registry`
pair, reading the exact same environment variable names `worker/main.py`
already documents (`REDIS_URL`, `TARGET_CACHE_PATH`,
`SHARED_ARTIFACT_STORE_PATH`), with the same `Redis.from_url(...)` +
`ping()` fail-fast construction and the same shared-vs-local cache
selection `build_registry` uses. It deliberately does not import
`worker.main`, even though that module already has everything needed,
because `worker/main.py` imports `embedding.dinov2_engine.
DINOv2EmbeddingEngine` at module scope — pulling in torch/transformers — a
needless cost for a process that only reads/writes a handful of small
Redis hashes. `worker/main.py` itself was not touched by this
implementation at all. See §14.3 for an important caveat about how much
this actually saves.

**Operational requirement, stated in the module docstring, the `--help`
epilog, and `docs/usage.md`:** the CLI must be run with the same
`REDIS_URL`/`TARGET_CACHE_PATH`/`SHARED_ARTIFACT_STORE_PATH` configuration
as the worker fleet it's managing targets for — `delete_target`/
`create_target` operate on whatever cache/media paths this process
resolves to, and a mismatch means `delete` cleans up a directory no worker
reads from, or `create`'s media publish never reaches the fleet's actual
shared store.

## 11. Redis key reference

| Key | Type | Owner | Status |
|---|---|---|---|
| `fingerprint:target:{id}:{version}` | Hash | `target_key()` | Pre-existing, unchanged shape |
| `fingerprint:target:content:{content_sha256}` | Set | `target_content_index_key()` | Pre-existing; now correctly `SREM`ed on content-changing replace and on delete (§7.3, §7.4, §7.7) |
| `fingerprint:target:{id}:{version}:embeddings` | Hash | `target_embeddings_key()` | Pre-existing, unchanged shape |
| `fingerprint:target:{id}:{version}:segment_embeddings` | Hash | `target_segment_embeddings_key()` | Pre-existing, unchanged shape |
| `fingerprint:lock:target:{cache_key}` | String (SET NX PX) | `target_lock_key()` | Pre-existing build-on-miss lock, unchanged |
| **`fingerprint:target:index`** | **Set** | `target_index_key()` | **New** — the list index (§7.5) |
| **`fingerprint:lock:target-record:{id}:{version}`** | **String (SET NX PX)** | `target_record_lock_key()` | **New** — the lifecycle lock (§7.2) |

No existing key's shape, contents, or read/write pattern changed except for
the `SREM` fix to `target_content_index_key`.

## 12. Concurrency guarantees — what's proven, and by what

| Guarantee | Mechanism | Test |
|---|---|---|
| Two callers registering the same identity with different content never silently interleave | One `RedisLock` per identity, held across the whole read-decide-write sequence | `tests/test_target_lifecycle.py::test_register_target_on_conflict_reject_raises_on_different_content` (correctness); locking itself reuses `target/lock.py`, already proven race-free by `tests/test_target_lock.py` |
| Two concurrent deletes of the same identity: exactly one succeeds | Same lock; second acquirer sees `get_target() is None` post-lock | `test_concurrent_delete_target_only_one_succeeds` (two real threads) |
| A caller that can't acquire the lock within the poll budget fails loudly, doesn't corrupt state | `TargetLockTimeoutError`, nothing written before the lock is held | `test_lifecycle_lock_timeout_raises_without_mutating` |
| Delete vs. update, delete vs. register | Same lock scope covers all three methods | Enforced by construction (`_acquire_lifecycle_lock` shared by all three); not independently stress-tested beyond the two explicit concurrency tests above, since the mechanism is identical for every pair |

## 13. Test coverage delivered

| File | New/changed | Count | Covers |
|---|---|---|---|
| `tests/test_target_lifecycle.py` | New | 25 | `TargetRegistry`: `on_conflict` policy, stale-index regression, `list_targets` (ordering, stale-member skip), `update_target_metadata` (merge/remove/identity-preservation), `delete_target` (registry+index+cache+shared-media cleanup, historical-result preservation), `reindex` (backfill, idempotency, non-interference with embeddings), lock-timeout, concurrent delete |
| `tests/test_target_service.py` | New | 40 | `TargetService`: identifier validation (parametrized valid/invalid ids and versions), media validation (missing/directory/empty/unreadable), create idempotency/conflict/new-version paths, metadata validation, all five methods' pass-through and error-translation behavior |
| `tests/test_target_cli.py` | New | 13 | Every subcommand in human and `--json` mode, exit codes (0/1/2), `reindex --dry-run` → real → idempotent |
| `tests/test_matching_handler.py` | +1 test, 0 modified | 1 | Deleted-target job fails identically to a never-registered-target job (§9) |

**Full-suite results**, run against real Redis (test db 15, the existing
`redis_client` fixture):

- Baseline (before this implementation): **269 passed, 0 failed** —
  matches the count `docs/usage.md` already documented.
- After implementation: **348 passed, 0 failed** (269 + 79 new).
- Re-verified from a completely fresh, independently created virtualenv
  after implementation: **348 passed, 0 failed** again.

No existing test was modified or weakened.

## 14. Deviations from the design document

### 14.1 Exception module location

The design document (§16) proposed defining the six error classes in
`target/service.py` and importing `TargetAlreadyExistsError` into
`target/registry.py`, while flagging that direction as "slightly unusual"
and explicitly leaving the final call to implementation time. **Resolved
as: a standalone `target/errors.py`** that both `target/registry.py` and
`target/service.py` import from — no lower-level-imports-higher-level
direction, no behavioral difference from the design's intent.

### 14.2 Where `reindex`'s logic lives

The design document's §21 describes `reindex` entirely from the CLI's
perspective ("the only place in this design that performs a SCAN..."),
without stating whether the scan itself should be implemented inside
`target/cli.py` or below it. Implementing the scan directly in
`target/cli.py` would have meant the CLI issuing raw Redis commands
(`scan_iter`, `hgetall`, `type`, `sadd`) — in direct tension with the
"CLI must not manipulate Redis" rule stated elsewhere in the same design.
**Resolved as:** `TargetRegistry.reindex()` holds the actual scan/validate/
SADD logic; `TargetService.reindex()` is a one-line passthrough;
`target/cli.py`'s `reindex` subcommand calls `service.reindex(...)` like
every other command. Observable behavior at the CLI is unchanged from the
design's description — only which file the logic lives in changed, in
favor of the design's own stronger, more repeated principle (CLI stays
Redis-free).

### 14.3 The CLI's "avoid the heavy ML stack" goal only partially holds

Verified from source, not assumed: `target/registry.py` unconditionally
imports `target.segment_cache`, which unconditionally imports
`embedding.result.SegmentEmbedding`. Importing `embedding.result` first
executes `embedding/__init__.py` (Python always runs a package's `__init__`
before a submodule), and `embedding/__init__.py` eagerly imports
`embedding.dinov2_engine` — which imports `torch`, `numpy`, `transformers`,
and `PIL` at module scope. So constructing a `TargetRegistry` at all — with
or without going through `worker.main` — already pulls in the full ML
stack, regardless of what `target/cli.py` itself imports.

This means avoiding `import worker.main` (done, exactly as instructed)
does not, by itself, make `python -m target.cli list` avoid a torch import
today. The instruction to avoid `worker.main` was still followed exactly
(and remains the correct choice — it keeps `target/cli.py` free of
`worker/main.py`'s worker-process-specific configuration surface, and
would immediately start paying off if `embedding/__init__.py`'s eager
import were ever made lazy), but the stated benefit ("the CLI must remain
lightweight") is not fully realized by this change alone. Fixing
`embedding/__init__.py`'s import eagerness is a one-line, low-risk change
but sits outside every file this phase's scope named
(`target/*`, `docs/usage.md`), so it was not made. Flagged here rather
than silently claimed as achieved.

## 15. Backward compatibility

- `TargetRegistry.register_target`'s only signature change is one new
  keyword-only parameter (`on_conflict: str = "replace"`) whose default
  reproduces the exact pre-existing behavior. Every pre-existing call site
  — `tests/test_target.py`, `tests/test_target_build_on_miss.py`,
  `tests/test_shared_target_storage.py`, `tests/test_segment_cache.py`,
  `tests/test_matching_handler.py`, `tests/test_integration_e2e.py`,
  `benchmarks/bench_pipeline.py`, `benchmarks/bench_integration_overhead.py`
  — was grepped for positional-argument usage before this change; none
  pass a 5th positional argument, so none needed to change, and none did.
- `get_target`, `find_by_content_hash`, `register_embedding`,
  `get_compatible_embedding`, `register_segment_embedding`,
  `get_or_build_segment_embedding` — byte-for-byte unchanged.
- `target/identity.py`, `target/versioning.py` — not touched at all.
- Every pre-existing Redis key's shape and contents are unchanged (§11);
  no migration is required to keep pre-existing targets, embeddings,
  segment embeddings, shared media, jobs, or results working exactly as
  before. `python -m target.cli reindex` is only needed to make
  `list_targets()`/`target.cli list` see targets registered before this
  phase shipped — everything else about them (`get`, matching, caching)
  already worked with zero changes.

## 16. Known limitations

- No bulk repair tool for content-index entries left stale by
  content-changing `register_target` calls made *before* this phase's fix
  (§7.4 only prevents new staleness going forward). Per the design's own
  reasoning: the failure direction is always "delay a shared-media
  garbage-collection," never "delete something still referenced," so this
  was deliberately not built. Flagged as an open question in the design
  document, not resolved here.
- `docs/development.md` still cites the pre-implementation test count
  ("269 passed"); out of this phase's file scope (not named in the
  design's implementation file list), left unchanged.
- See §14.3 for the CLI's partial (not full) avoidance of the heavy ML
  import chain.

## 17. Explicit non-goals (confirmed absent)

No SQL/SQLite/ORM, no second persistence system, no HTTP/REST/GraphQL
layer, no dashboard, no web server, no message broker, no new locking
primitive (`target/lock.py`'s `RedisLock` is reused as-is), no soft-delete
or inactive-target state, no `target → job` or `target → result` reverse
index, no object-storage/S3 backend, no binary cache format, no automatic
startup migration/scan, no changes to `worker/*`, `work_queue/*`,
`matching/*`, `integration/*`, or any crawler code (none exists in this
repository).
