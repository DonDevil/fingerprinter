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
| `tests/test_embedding_lazy_import.py` | New (§18 follow-up) | 9 | `embedding/__init__.py`'s lazy `DINOv2EmbeddingEngine` exposure: subprocess-verified absence of `torch`/`transformers`/`numpy`/`PIL` after importing `target.cli`/`target.registry`/`target.service`/`embedding`, a full CLI command cycle in a subprocess, lazy-attribute correctness, `AttributeError` for unknown names |
| `tests/test_target_crash_safety.py` | New (§19 follow-up) | 12 | Fault-injected partial failures in `register_target`/`update_target_metadata`/`delete_target` and the cache/shared-media `delete()` primitives — see §19 |

**Full-suite results**, run against real Redis (test db 15, the existing
`redis_client` fixture):

- Baseline (before the first implementation pass): **269 passed, 0
  failed** — matches the count `docs/usage.md` already documented.
- After the first implementation pass: **348 passed, 0 failed** (269 + 79
  new).
- After the follow-up pass (§18/§19): **369 passed, 0 failed** (348 + 21
  new).
- Re-verified from a completely fresh, independently created virtualenv
  after each pass: same counts, both times.

No existing test was modified or weakened at any point across either
pass.

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

### 14.3 The CLI's "avoid the heavy ML stack" goal — resolved in a follow-up pass

The gap described in this section (as originally written, at the end of
the first implementation pass) has since been closed in a follow-up pass.
`embedding/__init__.py` now exposes `DINOv2EmbeddingEngine`/
`DEFAULT_MODEL_ID`/`DEFAULT_MODEL_REVISION` lazily via a PEP 562 module
`__getattr__` instead of importing them eagerly at package-import time —
see §18 below for the full account, and §19 for the verification that
`python -m target.cli` now runs its entire command cycle with zero
`torch`/`transformers`/`numpy`/`PIL` in `sys.modules`, proven from a
virtualenv containing only `redis`.

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
- **A narrow, self-healing `delete_target` crash window, found and
  documented by §19's fault-injection tests:** if the process is killed (or
  the Redis connection drops) *exactly* between the two `SREM` calls
  (content-index, list-index) and the pipelined `DEL` that follows, the
  target becomes invisible to `list_targets()`/`find_by_content_hash()` but
  still resolves via a direct `get_target()`, because the record hash
  itself was never deleted in that window. This is narrower than a general
  transaction problem — it is one specific, small gap between two
  already-committed `SREM`s and one not-yet-attempted pipeline — and it is
  fully self-healing: calling `delete_target()` again (an operator retry,
  or an automated one) completes the delete correctly, because
  `get_target()` still finds the record to delete. See
  `tests/test_target_crash_safety.py::test_delete_target_crash_between_index_removal_and_hash_deletion_is_retry_safe`.
  Not fixed here (no code changed to close it) — reordering would mean
  deleting the record hash *before* the index `SREM`s, which is itself
  worth an explicit design decision (it changes which artifact is "the
  source of truth for existence" during the operation) rather than a
  silent adjustment during a test-writing pass. Flagged for the next
  design review to decide on, not resolved unilaterally.

## 17. Explicit non-goals (confirmed absent)

No SQL/SQLite/ORM, no second persistence system, no HTTP/REST/GraphQL
layer, no dashboard, no web server, no message broker, no new locking
primitive (`target/lock.py`'s `RedisLock` is reused as-is), no soft-delete
or inactive-target state, no `target → job` or `target → result` reverse
index, no object-storage/S3 backend, no binary cache format, no automatic
startup migration/scan, no changes to `worker/*`, `work_queue/*`,
`matching/*`, `integration/*`, or any crawler code (none exists in this
repository).

## 18. Follow-up pass: eliminating the eager ML import chain

### 18.1 The problem, precisely

Verified from source: `target/registry.py` unconditionally imports
`target.segment_cache`, which unconditionally imports
`embedding.result.SegmentEmbedding`. Importing any submodule of a package
always runs that package's `__init__.py` first — and, before this pass,
`embedding/__init__.py` unconditionally imported `embedding.dinov2_engine`,
which imports `torch`, `numpy`, `transformers`, and `PIL` at module scope.
So constructing a `TargetRegistry` — and therefore running `target.cli` at
all, or `target.service`, or anything that touches the target lifecycle —
pulled in the full ML inference stack, even for an operation like
`target.cli list` that reads three small Redis hashes and touches no
embedding.

Grepped before making any change: nothing in this repository does
`import embedding` followed by attribute access (`embedding.
DINOv2EmbeddingEngine`, etc.) — every actual caller
(`worker/main.py`, `worker/matching_handler.py`, every benchmark, every
`test_embedding*`/`test_matching*` test) already does
`from embedding.dinov2_engine import ...` directly, a submodule import that
doesn't depend on anything `embedding/__init__.py` re-exports. The eager
re-export in `embedding/__init__.py` was therefore paying an import-time
cost that nothing in the codebase's actual call sites needed paid eagerly.

### 18.2 The fix

One file changed: `embedding/__init__.py`. `PreprocessingConfig`/
`SamplingConfig`/`SegmentSamplingConfig`/`IMAGE_SAMPLING_CONFIG`/
`DEFAULT_SEGMENT_DURATION_S` (from `embedding.config`), `EmbeddingResult`/
`SegmentEmbedding`/`VideoSegmentEmbeddingResult`/
`SEGMENT_EMBEDDING_SCHEMA_VERSION` (from `embedding.result`), and every
`*Error` class (from `embedding.errors`) stay exactly as eagerly imported
as before — none of those three submodules import anything heavy
(verified: `embedding/config.py` imports only `dataclasses`/`typing`;
`embedding/errors.py` imports nothing beyond `__future__`;
`embedding/result.py` imports only `embedding.config` plus stdlib). Only
`DINOv2EmbeddingEngine`, `DEFAULT_MODEL_ID`, and `DEFAULT_MODEL_REVISION`
— the three names that come from `embedding.dinov2_engine` — became lazy,
via a PEP 562 module-level `__getattr__`:

```python
_LAZY_DINOV2_ATTRS = frozenset({"DINOv2EmbeddingEngine", "DEFAULT_MODEL_ID", "DEFAULT_MODEL_REVISION"})

def __getattr__(name: str):
    if name in _LAZY_DINOV2_ATTRS:
        from embedding import dinov2_engine
        return getattr(dinov2_engine, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
```

`__all__` is unchanged — the same names are still declared as this
package's public surface, and `from embedding import DINOv2EmbeddingEngine`
(or `embedding.DINOv2EmbeddingEngine`) still works, resolving to the exact
same class object as `from embedding.dinov2_engine import
DINOv2EmbeddingEngine` (asserted by
`tests/test_embedding_lazy_import.py::test_lazy_dinov2_engine_attribute_still_resolves`)
— it is just resolved on first access instead of at package-import time.
An unrecognized attribute still raises `AttributeError`, not silently
returning `None` or swallowing the lookup.

This is why the fix satisfies "preserve all existing target behavior and
APIs": nothing that worked before behaves differently now, including the
one hypothetical caller pattern (`from embedding import X`) that the
package's own `__all__` had always advertised as supported but that no
code in this repository actually used.

### 18.3 Why not go further (and why that's the right stopping point)

This phase did not touch `embedding/dinov2_engine.py`, `embedding/
config.py`, `embedding/errors.py`, `embedding/result.py`, `embedding/
frames.py`, or any file outside `embedding/__init__.py` itself. The
instruction was explicit: inspect the dependency graph first, make the
smallest safe change, do not refactor the embedding subsystem broadly.
Once the graph was traced (§18.1) it was clear exactly one file needed to
change, and changing it in the smallest possible way (lazy re-export,
not deleting the re-export or restructuring `dinov2_engine.py` itself)
was sufficient to fully solve the stated problem — verified in §19.

### 18.4 Verification

**Proof the CLI now avoids the ML stack, from a virtualenv containing only
`redis` (no numpy, no torch, no transformers, no Pillow installed at
all):**

```
$ pip list
Package Version
------- -------
redis   8.1.0

$ REDIS_URL=... TARGET_CACHE_PATH=... python -m target.cli add movie.mp4 --id proof --version v1 --json
{"content_sha256": "...", "status": "ok", "target_id": "proof", ...}
$ python -m target.cli list --json
[{"target_id": "proof", ...}]
$ python -m target.cli delete proof --version v1 --json
{"status": "deleted", "target_id": "proof", "target_version": "v1"}
```

Every command succeeded — `add`, `list`, `delete` — with nothing beyond
`redis` installed. This is not merely "the import doesn't crash"; it is
the entire operator lifecycle running for real against real Redis with the
ML stack physically absent.

**Proof for a fully-installed environment too** (where the absence has to
be checked by inspecting `sys.modules`, since nothing *prevents* torch
from being imported if some other code path wanted it — the check is
"target-lifecycle code doesn't", not "torch can't be imported at all"):
`tests/test_embedding_lazy_import.py` spawns a fresh `subprocess` per
assertion (never an in-process check — by the time any single test runs in
the shared pytest session, other collected test modules have almost
certainly already imported torch, which would make an in-process
`sys.modules` check pass trivially regardless of whether the import graph
under test is actually torch-free) and asserts `torch`/`transformers`/
`numpy`/`PIL` are absent from that subprocess's `sys.modules` after:
importing `target.cli`, importing `target.registry`, importing
`target.service`, importing the bare `embedding` package, and running a
full `add`/`list`/`get`/`update-metadata`/`delete` cycle through
`target.cli.main()`. All 9 tests in that file pass, including this exact
verification.

## 19. Follow-up pass: crash/partial-failure test coverage

`tests/test_target_crash_safety.py` (12 tests) injects a failure at a
specific point inside a real `TargetRegistry` call — never by editing
production code, only by monkeypatching one Redis client method, one cache
method, or one shared-media-store method to raise partway through — and
asserts on the actual resulting state, cross-checked against what §7's
walkthrough and the design document already claimed. Coverage:

- **`register_target`:** lock is released (and the identity is not stuck)
  when the Redis write itself fails; an `on_conflict="reject"` conflict
  leaves no trace anywhere, not the record, not either index; a crash
  between the content-index `SADD` (committed) and the list-index `SADD`
  (never ran) leaves a target that is gettable and findable by content hash
  but invisible to `list_targets()` — and `reindex()` repairs it, proving
  the pre-existing migration tool doubles as a crash-recovery tool for this
  exact gap.
- **`update_target_metadata`:** lock released and no partial metadata
  persisted when the write fails.
- **`delete_target`:** a cache-deletion failure (step 3/4 of §7.7) leaves
  every Redis-visible piece of state completely untouched, and the delete
  is safely retryable; a failure of the pipelined `DEL` after the two
  `SREM`s already committed produces the narrow, self-healing partial state
  documented in §16; a shared-media-store failure after every Redis
  mutation has committed leaves the target genuinely gone from Redis's
  perspective with the blob merely leaked (fetched intact afterward by a
  fresh, independent store client, proving no corruption); a lock-timeout
  during delete leaves the target completely untouched (never got past
  acquiring the lock).
- **Cache/shared-storage primitives directly:** `FilesystemEmbeddingCache.
  delete` is idempotently `False` on a second call;
  `SharedArtifactStore.delete` raises `SharedArtifactStoreError` (not a
  raw `OSError`, not a silent `False`) when the underlying `unlink()`
  fails due to a read-only parent directory, and returns `False` (not an
  error) for a key that was never present; `SharedTargetMediaStore.delete`
  removes published media and is idempotent on a repeat call.

No production code was changed to write these tests. Where a test revealed
a genuine (if narrow) gap — the `delete_target` half-deleted window — it is
documented as a known, self-healing limitation (§16) rather than silently
patched.
