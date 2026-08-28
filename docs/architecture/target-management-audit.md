# Target Management Audit — CRUD / Multi-Target Operator Interface

## 1. Status

**AUDIT ONLY. No production code, tests, configuration, or Redis state was
modified to produce this document.** The full target-management test suite
(105 tests across the target-lifecycle files) and the full repository suite
(`tests/`, 369 tests) were **run**, read-only, against the existing local
Redis instance (test db, the pre-existing `redis_client` fixture) purely to
verify claims in this document — no test file, fixture, or Redis key was
edited, and no test asserts against anything but ephemeral state the suite
itself creates and tears down.

**This document supersedes the audit previously at this same path.**
Read `docs/architecture/target-management-design.md` and
`docs/architecture/target-management-implementation.md` first if you want
the full rationale trail — this document's job is narrower: confirm, by
reading the actual current source and running the actual current test
suite, whether what those two documents *claim* was built is what actually
exists on disk today, and to answer this phase's audit questionnaire
against that verified reality rather than against the documents' prose.

**Headline finding: the operator-facing CRUD/lifecycle interface this
audit brief asks about already exists, is already wired end-to-end, and is
already covered by a passing test suite.** A prior audit → design →
implementation cycle ran in this same repository (visible in
`docs/architecture/target-management-{audit,design,implementation}.md`,
and in `target/service.py`, `target/errors.py`, `target/cli.py`, and the
extended `target/registry.py`, `target/cache.py`, `target/segment_cache.py`,
`target/shared_cache.py`, `target/shared_storage.py`). Every capability
this brief's OBJECTIVE section asks for — add/list/get/modify/delete,
multi-target, an operator-facing boundary hiding Redis/cache/identity
internals — is **already implemented**, not merely designed. The gaps the
brief anticipates finding (no list, no delete, no update, unsafe
content-swap-via-reregistration, unlocked races, unescaped `:` collision)
were the findings of the *original* pre-implementation audit and have since
been closed. This document's contribution is independent verification of
that claim from the current source and a live test run — not a repeat
design exercise.

Scope: `target/`, `integration/`, `work_queue/`, `worker/`, `tests/`, plus
`docs/usage.md` and the three `docs/architecture/target-management-*.md`
documents. `old/` was not read (no relevance, matching the brief's own
guidance). The sibling `crawler/` directory was inspected only enough to
confirm it is a separate, independent git repository (see §9) — its
internals were not audited, per this phase's scope (`fingerprinter/`,
which has its own `.git`).

Every claim below is labeled:

- **VERIFIED FROM SOURCE** — read directly in the file/line cited, this session.
- **VERIFIED BY TEST** — an existing test asserts this behavior, and the
  suite containing it was run, this session, and passed.
- **INFERRED** — a reasonable conclusion from source that isn't directly
  asserted by any single line or test.
- **NOT IMPLEMENTED** — searched for, confirmed absent, this session.
- **NOT VALIDATED** — plausible/intended, but no test or code path confirms
  it either way.

## 2. Audit objective

Determine, from the actual current source (not from the design/
implementation documents' prose, and not from assumption), whether a clean
operator/dashboard-facing lifecycle interface (create / list / get /
update / delete a target, explicitly supporting multiple simultaneous
targets) exists over the pre-existing `TargetRegistry`/cache/versioning
architecture, what its exact contract is, and what — if anything — remains
a genuine gap for a future phase.

## 3. Current target lifecycle (as it exists today)

**VERIFIED FROM SOURCE**, `target/service.py` (188 lines, read in full) and
`target/registry.py` (572 lines, read in full):

| Operation | Exists? | Callable |
|---|---|---|
| Create/register | Yes | `TargetService.create_target()` → `TargetRegistry.register_target(..., on_conflict="reject")` |
| List all | Yes | `TargetService.list_targets()` → `TargetRegistry.list_targets()` |
| Get one | Yes | `TargetService.get_target()` → `TargetRegistry.get_target()` |
| Find by content hash | Yes (pre-existing, unchanged) | `TargetRegistry.find_by_content_hash()` |
| Update (metadata-only, patch semantics) | Yes | `TargetService.update_target_metadata()` → `TargetRegistry.update_target_metadata()` |
| Update (new content) | Yes, only via a **new `target_version`** — content-swap under an existing version is now *rejected*, not silently allowed | `TargetService.create_target()` with a different `target_version` |
| Delete | Yes, with cache/shared-media cleanup | `TargetService.delete_target()` → `TargetRegistry.delete_target()` |
| One-time list-index backfill for pre-existing data | Yes | `TargetService.reindex()` → `TargetRegistry.reindex()` |

Two callable layers exist, by design:
- **`TargetRegistry`** (`target/registry.py`) — identity/versioning/
  cache-compatibility plus the actual Redis mutations and locking for every
  lifecycle operation. Still importable and usable directly (as tests and
  benchmarks already do), unchanged in its own responsibility scope.
- **`TargetService`** (`target/service.py`) — the operator-facing boundary:
  input validation (`target_id`/`target_version` charset+length,
  `media_path` filesystem checks, metadata shape), and the
  create-vs-conflict policy. **Never touches Redis, a cache path, or a
  `SharedArtifactStore` key directly** — confirmed by reading the file in
  full: its only imports from `target/` are `TargetRegistry`,
  `TargetRecord`, `ReindexResult`, and the error classes.

A CLI (`target/cli.py`, `python -m target.cli`) sits on top of
`TargetService` and is the first concrete operator entry point —
see §17.

## 4. Current registration API

**Callable:** `TargetService.create_target(target_id, target_version,
media_path, metadata=None) -> TargetRecord` (`target/service.py:118-144`),
which validates and delegates to `TargetRegistry.register_target(...,
on_conflict="reject")` (`target/registry.py:105-194`). **VERIFIED FROM
SOURCE.**

- **`target_id`/`target_version` semantics:** caller-assigned opaque
  strings, still never derived from content or filename
  (`target/identity.py`, unchanged). **New at the `TargetService`
  boundary:** both must match `^[A-Za-z0-9._-]+$`, 1–128 characters, no
  leading/trailing whitespace tolerated (rejected, not stripped) —
  `target/service.py:52-65`. This closes the previously-identified
  `target_key()` unescaped-`:` collision class (§13). `TargetRegistry.
  register_target` itself still performs **no** charset validation — a
  caller that bypasses `TargetService` and calls it directly can still
  create a `:`-containing id. **VERIFIED FROM SOURCE**, and this residual
  gap is explicitly documented in the design doc (§6) as an accepted,
  scoped decision, not an oversight.
- **`media_path` semantics:** still informational only for identity
  (identity is `content_sha256`, never the path) but now validated before
  any registry call: must exist, must not be a directory, must be a
  regular file, must be non-empty, must pass a 1-byte readability probe —
  every failure raises `TargetMediaError`, never a raw `OSError`/
  `FileNotFoundError`/`IsADirectoryError` (`target/service.py:68-91`).
  **VERIFIED FROM SOURCE**, **VERIFIED BY TEST**
  (`tests/test_target_service.py`, media-validation parametrized cases —
  file ran and passed this session).
- **Content SHA-256 behavior:** unchanged — `sha256_file()` streams the
  file, computed before any lock is acquired
  (`target/registry.py:156-158`), still no short-circuit on an unchanged
  path (re-hashes every call).
- **Redis records created:** `HSET fingerprint:target:{id}:{version}`,
  `SADD fingerprint:target:content:{content_sha256}`, and (new)
  `SADD fingerprint:target:index` — all three under one `RedisLock`
  (`target/registry.py:188-194`, `target/keys.py:21-28`).
  **VERIFIED FROM SOURCE.**
- **Idempotency — now a three-way, explicit contract, not an implicit
  full-overwrite:**

  | Existing record? | Content matches? | Outcome |
  |---|---|---|
  | No | — | Created |
  | Yes | Identical `content_sha256` | Idempotent success — `created_at` preserved, `updated_at` advances, metadata **replaced** by whatever this call's `metadata` argument says (an omitted `metadata` resets stored metadata to `{}` — `create_target` declares full desired state; distinct from `update_target_metadata`'s patch semantics, see §6) |
  | Yes | Different | `TargetAlreadyExistsError`, existing record and both index memberships **completely untouched** |

  **VERIFIED FROM SOURCE** (`target/registry.py:160-194`), **VERIFIED BY
  TEST** (`tests/test_target_lifecycle.py`, `on_conflict` tests;
  `tests/test_target_service.py`, create-idempotency/conflict tests — both
  files ran and passed this session, 25 + 25 collected test functions,
  more individual cases via parametrization).
- **Same media bytes registered under different IDs:** still explicitly
  supported and detectable via `find_by_content_hash()`, unchanged
  behavior. Duplicate content across distinct targets is allowed by design.
- **Embeddings computed immediately or lazily:** still lazily, cache-first,
  build-on-miss — `register_target`/`create_target` never touch the
  embedding engine. Unchanged.

### Fixed: stale content-index entry (previously an open gap)

**VERIFIED FROM SOURCE.** The original audit's finding — a
content-changing re-registration left the *old* `content_sha256`'s reverse
index still pointing at the target — is fixed: `register_target`'s
`"replace"` path now `SREM`s the target's membership from the *old* hash's
`target_content_index_key` before writing the new one
(`target/registry.py:172-178`), under the same lifecycle lock so this is
race-free with respect to other mutators of the same identity.
**VERIFIED BY TEST**:
`tests/test_target_lifecycle.py::test_content_changing_reregistration_removes_stale_content_index_entry`
ran and passed this session. Only forward-going registrations are fixed —
any stale entry that predates this fix in an already-deployed Redis
instance is not retroactively repaired (documented open item, §15/§16).

## 5. Current listing/get capabilities

**`TargetService.list_targets() -> list[TargetRecord]`** exists
(`target/service.py:146-149` → `target/registry.py:233-250`).
**VERIFIED FROM SOURCE**, **VERIFIED BY TEST** (list-ordering,
multi-version-coexistence, and stale-member-skip tests in
`tests/test_target_lifecycle.py`, ran and passed).

- Backed by a dedicated Redis Set, `fingerprint:target:index`
  (`target/keys.py:21-28`), `SADD`ed inside `register_target` itself — so
  the invariant "every successfully registered target is listable" holds
  for *any* caller, including a direct `TargetRegistry.register_target`
  call (tests, benchmarks), not only calls that went through
  `TargetService`.
- **O(number of registered targets)** — one `SMEMBERS` plus one `HGETALL`
  per member, never a keyspace `SCAN`. Results are sorted by `(target_id,
  target_version)` before being returned, so output is deterministic
  despite `SMEMBERS`'s unordered nature. **VERIFIED FROM SOURCE.**
- **`TargetService.get_target(target_id, target_version) -> Optional[TargetRecord]`**
  exists, returns `None` on a miss rather than raising (deliberately
  different from the mutation methods — a `get` miss is a normal lookup
  outcome, not an operator error). **VERIFIED FROM SOURCE.**
- **"List all versions for one `target_id`"**: **not a separate method** —
  a caller filters `list_targets()`'s result by `target_id` client-side.
  This is a thin, reasonable simplification (expected scale is tens to low
  hundreds of targets, not crawler-URL scale — design doc §8), not a gap;
  flagged here only so it isn't mistaken for a missing capability.
- **"Is this target active"**: still **NOT IMPLEMENTED** — no
  active/inactive concept exists anywhere; a target either resolves via
  `get_target`/appears in `list_targets` or it doesn't. This is a
  deliberate design decision (§7, §11), not an oversight.
- **Pre-existing-data gap:** targets registered *before* this
  implementation shipped are invisible to `list_targets()` until
  `reindex()` is run once against that Redis instance (§12). Confirmed:
  `fingerprint:target:index` is populated only by `register_target`'s
  `SADD` and `reindex`'s backfill `SADD` — no other write path touches it.
  **This is a real, currently-live migration requirement for any
  already-deployed Redis instance with pre-existing targets that predates
  this feature**, not a hypothetical.

## 6. Current update capabilities

**Metadata-only update exists and has patch semantics**, closing the
original audit's finding that re-registration silently clobbered metadata.

**Callable:** `TargetService.update_target_metadata(target_id,
target_version, set_fields=None, remove_fields=None) -> TargetRecord`
(`target/service.py:155-172` → `target/registry.py:252-290`).
**VERIFIED FROM SOURCE.**

- Shallow-merges `set_fields` into existing `media_metadata`
  (`dict.update()` semantics — a nested value in `set_fields` replaces the
  corresponding existing value wholesale, not a recursive merge), then
  pops every key named in `remove_fields` (a key present in both ends up
  removed — set applied first, remove second).
- **Never touches** `media_path`, `content_sha256`, `target_id`,
  `target_version` — there is no parameter for any of them on this method,
  so a content swap is structurally impossible through this call, not
  merely disallowed by convention.
- `created_at` preserved, `updated_at` advances.
- Raises `TargetNotFoundError` (not a silent no-op or a fresh-create) if
  the identity doesn't exist.
- Serialized by the same lifecycle lock `register_target`/`delete_target`
  use, scoped to `target_record_lock_key(target_id, target_version)`
  (`target/keys.py:48-56`).

**VERIFIED BY TEST**: `tests/test_target_lifecycle.py`'s
`update_target_metadata` tests (merge, remove, key-in-both,
identity-preservation, not-found) and `tests/test_target_service.py`'s
equivalents — both files ran and passed this session.

Field mutability, current state:
- **Immutable by construction:** `target_id`, `target_version` (the key,
  never a stored-and-editable field), `created_at` (preserved across every
  mutation).
- **Derived, never settable:** `content_sha256` — always computed from
  `media_path`'s bytes; no code path anywhere accepts it as caller input.
- **Mutable via `update_target_metadata` (patch, not replace):**
  `media_metadata` only.
- **Mutable via `create_target` (a new `target_version`), never in
  place:** `media_path`/content. There is no "swap content under an
  existing version" operation reachable through `TargetService` — the only
  way to reach that behavior is to bypass `TargetService` and call
  `TargetRegistry.register_target(..., on_conflict="replace")` (the
  default) directly, which is retained **only** for backward compatibility
  with pre-existing direct callers (tests/benchmarks), not exposed as an
  operator operation.

**This fully resolves the original audit's §6 finding and recommendation**
— the two narrow operations it recommended (`update_target_metadata`, and
treating new content as a new `target_version`) are exactly what was built.

## 7. Current deletion capabilities

**Exists.** `TargetService.delete_target(target_id, target_version) ->
None` (`target/service.py:174-179` → `target/registry.py:319-373`).
**VERIFIED FROM SOURCE.**

Full sequence, under the lifecycle lock (`target/registry.py:346-373`):

1. Look up the record; `TargetNotFoundError` if missing.
2. Read the pooled/segment embedding summary hashes
   (`target_embeddings_key`/`target_segment_embeddings_key`) and
   reconstruct each cached `EmbeddingSpec` from its stored
   `to_metadata_fields()` JSON — the only way to know which cache files
   this target owns without a filesystem scan.
3. Delete each target-exclusive pooled/segment cache entry
   (`self._cache.delete(...)` / `self._segment_cache.delete(...)`) — safe
   unconditionally, since `cache_entry_key` bakes `(target_id,
   target_version)` into the filename/key, so these entries can never be
   shared across targets.
4. `SREM` this target's membership from the content-hash reverse index.
5. `SREM` this target's membership from the list index.
6. `DEL` the record hash + both embedding-summary hashes, in one Redis
   pipeline.
7. **Only after step 4**, call `find_by_content_hash(record.content_sha256)`
   — this now reflects only *other* targets, since this target's own
   membership is already gone. If empty **and** a `media_store` is
   configured, delete the shared media blob.

### Target-owned state inventory (re-verified, current source)

| Artifact | Owned exclusively by one `(target_id, target_version)`? | Deleted by `delete_target`? |
|---|---|---|
| Registry record hash | Yes | Yes (step 6) |
| Embedding/segment-embedding summary hashes | Yes | Yes (step 6) |
| Content-hash reverse-index membership | No — content-keyed, shared across any target with identical bytes | This target's own membership only (step 4), never the whole Set |
| List-index membership | Yes | Yes (step 5) |
| Local/shared pooled and segment cache files | Yes — `cache_entry_key` includes `(target_id, target_version)` | Yes, unconditionally (step 3) |
| **`SharedTargetMediaStore` raw media blob** | **No — keyed by `content_sha256` alone** | **Only if `find_by_content_hash` (post-SREM) shows no remaining referent** (step 7) |
| `ResultRecord`s | No — keyed by `job_id`, target fields are descriptive only | **Not touched** (by design) |
| Queued/in-flight Redis Stream job entries | No — keyed by stream position | **Not touched** (by design, §11) |
| Build-on-miss lock (`target/lock.py`) | Yes | Not applicable — self-expiring by TTL, nothing to clean up |

**The one artifact requiring a reference check before deletion —
`SharedTargetMediaStore` — is correctly reference-checked**, using the
already-existing `find_by_content_hash` primitive, with the ordering that
matters (SREM before the check) enforced by the code, not left to caller
discipline. **VERIFIED FROM SOURCE**, **VERIFIED BY TEST**:
`tests/test_target_lifecycle.py`'s delete tests include "shared media
retained when a second target still references it" and "shared media
deleted when the last referent is deleted" — ran and passed this session.

### A real, narrow, documented, self-healing gap

**VERIFIED FROM SOURCE and VERIFIED BY TEST**
(`tests/test_target_crash_safety.py::test_delete_target_crash_between_index_removal_and_hash_deletion_is_retry_safe`,
ran and passed): if the process crashes or the Redis connection drops
*exactly* between the two `SREM` calls (steps 4/5) and the pipelined `DEL`
that follows (step 6), the target becomes invisible to `list_targets()`
and `find_by_content_hash()` but the record hash itself is not yet
deleted, so a direct `get_target()` still resolves it. This is **narrow**
(a small window between two already-committed writes and one
not-yet-attempted one) and **self-healing** — calling `delete_target()`
again completes it correctly, because `get_target()` still finds the
record. Not fixed in this implementation; documented as a known,
accepted-risk limitation, not silently patched. This is the one concrete,
currently-live correctness caveat this audit found in the delete path.

**No target→job or target→result index exists** — unchanged from the
original audit's finding, and this is an explicit, reasoned policy
decision (§11 below), not an omission.

## 8. Multiple-target support

**Fully supported — unchanged conclusion from the original audit, now
additionally exercised by the new lifecycle code without any redesign.**

- Every target-related Redis key remains namespaced by `(target_id,
  target_version)` or `content_sha256` (`target/keys.py`, read in full) —
  including both **new** keys, `fingerprint:target:index` (one
  unparameterized Set, but its *members* are per-identity, not a
  collision) and `fingerprint:lock:target-record:{id}:{version}` (scoped
  per identity, deliberately shaped differently from the pre-existing
  `fingerprint:lock:target:{cache_key}` build-on-miss lock so the two
  families can never collide). **VERIFIED FROM SOURCE.**
- `work_queue.jobs.Job` and `integration.candidate.FingerprintCandidate`
  still require `target_id`/`target_version` as mandatory per-job/
  per-candidate fields (`work_queue/jobs.py:42-43,60-61`;
  `integration/candidate.py:104-105,123`) — **confirmed unchanged this
  session** by direct read; neither file was touched by the lifecycle
  implementation (matches the implementation doc's explicit "no changes to
  work_queue/integration" claim, §17 of this doc).
- `worker/matching_handler.py` still resolves `target_id`/`target_version`
  fresh per job, and still maps an unresolvable identity to
  `PermanentFailure` via `KeyError` (`worker/matching_handler.py:230-232`,
  confirmed unchanged this session). **VERIFIED BY TEST** —
  `tests/test_matching_handler.py` ran and passed, including the **new**
  `test_deleted_target_raises_permanent_failure_same_as_unknown_target`
  (line 198), which proves a target that went through the new
  `delete_target()` fails identically to a target that was simply never
  registered — the pre-existing fail-closed path was not modified and did
  not need to be.
- **Proof of coexistence, re-verified this session by running the suite:**
  `tests/test_integration_e2e.py` (multi-target, multi-worker scenarios)
  passed as part of the 369-test full-suite run.

**Conclusion, unchanged from the original audit and now further confirmed:
multi-target support is a property of the pre-existing data plane, and the
lifecycle implementation added on top of it did not need to touch — and in
fact did not touch — any of `work_queue/`, `worker/matching_handler.py`,
`worker/main.py`, or `integration/` to deliver create/list/get/update/
delete for arbitrarily many simultaneous targets.**

## 9. Crawler integration

**Confirmed this session:** `fingerprinter/` and `crawler/` are two
independent git repositories (`fingerprinter/.git`, `crawler/.git` both
exist as real, separate `.git` directories, not a shared monorepo root)
under a common, non-git-tracked parent directory
(`/home/dhanush/anti_piracy`). `integration/candidate.py`'s own docstring
refers to "the sibling crawler repo." This confirms the original audit's
conclusion: there is no crawler code inside `fingerprinter/`'s own
repository, and this phase's scope (target-management within
`fingerprinter/`) correctly does not touch it.

- `target_id`/`target_version` remain mandatory, per-candidate,
  caller-supplied fields with no default and no global fallback
  (`integration/candidate.py:104-105`, confirmed unchanged this session).
- The crawler (or any external caller of `integration.submission`) can
  already switch targets between calls with zero code changes on either
  side — unchanged conclusion.
- **What happens if the configured target does not exist:** unchanged —
  `FingerprintCandidate.validate()` still only checks non-empty strings,
  never calls the registry; a job against an unregistered target still
  enqueues successfully and fails worker-side as `PermanentFailure` once
  claimed. **This submission-time gap was explicitly scoped out of the
  target-management implementation** (design doc §26, confirmed against
  current source: `integration/candidate.py` was not modified by this
  phase) — it is a pre-existing, intentionally-minimal phase-12 boundary
  decision, not a target-management regression or omission.

## 10. Worker integration

- `worker/matching_handler.py` still receives `target_id`/`target_version`
  from `job.target_id`/`job.target_version` at call time, never from
  process-level configuration; `worker/main.py`'s `WorkerConfig` still has
  no target field. **Confirmed unchanged this session** — neither file
  appears in the target-lifecycle implementation's file list, and a direct
  read confirms no target-lifecycle import or call was added to either.
- **Deliberate, verified non-integration:** `target/cli.py` explicitly does
  **not** import `worker.main`, specifically to avoid pulling in
  `embedding.dinov2_engine`'s torch/transformers/numpy/Pillow dependency
  chain for a process that only reads/writes small Redis hashes
  (`target/cli.py:20-29`, confirmed by reading the module docstring and
  imports). **VERIFIED BY TEST**: `tests/test_embedding_lazy_import.py`
  (9 tests, ran and passed this session) proves, via fresh subprocesses,
  that `torch`/`transformers`/`numpy`/`PIL` are absent from `sys.modules`
  after importing `target.cli`/`target.registry`/`target.service` and
  running a full CLI command cycle.
- **Multiple targets concurrently:** unchanged conclusion — true at the
  fleet level (distinct worker processes on different targets
  simultaneously), not within a single `Worker.run()` loop (one job at a
  time, target-agnostic dispatch). This is the pre-existing deployment
  model, untouched by this phase.
- **Artifact isolation:** unchanged, still guaranteed by
  `cache_entry_key()` hashing `(target_id, target_version, content_sha256,
  spec)` together — confirmed this session that the new `delete()`
  primitives on every cache class take the exact same four-argument shape
  as `get`/`put`, so deletion is exclusive by the same construction that
  makes reads/writes exclusive.

## 11. Cache/artifact ownership

Covered in §7's table. The load-bearing invariant, re-verified this
session by reading all four cache/storage `delete()` implementations
(`target/cache.py:86-125`, `target/segment_cache.py:103-133`,
`target/shared_cache.py:95-191`, `target/shared_storage.py:107-223`):

**Every new `delete()` method mirrors its class's existing `get`/`put`
key-derivation exactly** — none of them independently reconstructs a path
or key; each calls the same private `_path_for(...)`/`_key(...)` helper
`get`/`put` already use. This means deletion cannot drift out of sync with
how entries are addressed, by construction, not by convention. Return
contract is uniform across all four: `True` iff something was actually
removed, `False` iff already absent (idempotent, safe to call twice,
never raises for a plain miss); `SharedArtifactStore.delete` raises
`SharedArtifactStoreError` — not a raw `OSError`, not a silent `False` —
only when the underlying store itself is unreachable/unwritable, the same
"absent vs. unreachable" distinction `get_bytes`/`put_bytes` already use.
**VERIFIED FROM SOURCE and VERIFIED BY TEST**
(`tests/test_target_crash_safety.py`'s direct primitive tests, ran and
passed).

**Content-addressed identity is preserved everywhere and was not
replaced** — confirmed this session: `target/identity.py`,
`target/versioning.py`, and `target_key()`'s own `:`-joined format are
byte-for-byte unchanged from before this implementation (the collision
risk they created is closed by validation at the `TargetService`
boundary, §13, not by reshaping the keys themselves — a smaller, correctly
scoped fix per the design doc's own reasoning).

## 12. Concurrency/idempotency

**The original audit's core finding — `register_target` had no lock around
its read-then-write, so two callers racing on the same identity with
different content could silently interleave — is fixed.**

- **One `RedisLock`, one key shape, shared by all three mutations:**
  `register_target`, `update_target_metadata`, and `delete_target` all
  call the same `_acquire_lifecycle_lock(target_id, target_version)`
  (`target/registry.py:196-215`), scoped to
  `target_record_lock_key(target_id, target_version)` — a key shape
  deliberately distinct from the pre-existing build-on-miss lock's key, so
  the two lock families can never collide even for the same target.
  **VERIFIED FROM SOURCE.**
- Acquisition: try once, then poll every 0.1s for up to 5s
  (`LIFECYCLE_LOCK_POLL_INTERVAL_S`/`_TIMEOUT_S`), raising
  `TargetLockTimeoutError` on exhaustion rather than blocking indefinitely
  — deliberately much shorter than the build-on-miss lock's 10-minute
  budget, since lifecycle operations are operator-driven, not a hot
  embedding-build path.
- Release is always in a `finally` in every one of the three methods —
  confirmed by reading each method body — so a failure partway through
  never leaves the identity stuck locked for the full TTL.
- **No new locking primitive was introduced** — `target/lock.py`'s
  `RedisLock` (`SET NX PX` / Lua-CAS release) is reused verbatim, the same
  primitive already proven race-free by the pre-existing
  `tests/test_target_lock.py`.

**VERIFIED BY TEST**, this session, with real concurrency (not simulated):
- `test_register_target_on_conflict_reject_raises_on_different_content`
  (correctness of the reject path).
- `test_concurrent_delete_target_only_one_succeeds` — two real threads,
  exactly one succeeds, the other gets `TargetNotFoundError`.
- `test_lifecycle_lock_timeout_raises_without_mutating` — a caller that
  can't acquire the lock within budget fails loudly and writes nothing.

All three are in `tests/test_target_lifecycle.py`, which ran and passed
this session as part of both the targeted (105-test) and full (369-test)
runs.

**Remaining, explicitly accepted concurrency gap:** the delete-path crash
window described in §7 (between the two `SREM`s and the pipelined `DEL`)
is not lock-related — it happens *while the lock is held* — it's a
partial-write-ordering gap, not a race between two callers. It is
self-healing by retry, as described.

## 13. Security/input validation

**The original audit's concrete finding — an unescaped `:` in
`target_key()` creating a real key-collision class — is closed at the
correct boundary.**

- `TargetService`'s `_validate_identifier` (`target/service.py:52-65`)
  enforces `^[A-Za-z0-9._-]+$`, 1–128 characters, on both `target_id` and
  `target_version`, applied identically to both fields, with no silent
  trimming (`" blast"` and `"blast "` both rejected, not normalized) —
  **VERIFIED FROM SOURCE**, **VERIFIED BY TEST** (parametrized
  valid/invalid-identifier cases in `tests/test_target_service.py`, ran
  and passed).
- **Residual scope, stated explicitly by design, not accidental:**
  `TargetRegistry.register_target` itself still performs no charset
  validation — a caller that constructs a `TargetRegistry` directly
  (bypassing `TargetService`) can still create a `:`-containing identity.
  This is an accepted, documented trade-off (the fix lives at the operator
  boundary, per the audit's own original scoping recommendation, not
  inside a method with pre-existing test coverage the brief said not to
  touch) — **not a false sense of security**, but worth stating plainly:
  the guarantee is "every target created through `TargetService`/the CLI
  is collision-safe," not "every target in Redis is."
- `media_path` validation (`target/service.py:68-91`) now covers:
  missing, directory, non-regular-file, empty, unreadable — every case
  raises `TargetMediaError` with the original `OSError` as `__cause__`
  where applicable, never a raw exception. **VERIFIED FROM SOURCE and
  VERIFIED BY TEST.**
- **Path traversal:** unchanged conclusion — `media_path` remains trusted,
  operator-local input (not attacker-controlled network input like
  `acquisition/ssrf_guard.py`'s URL-facing threat model), so no
  traversal-specific defense was added or is warranted. This session found
  nothing to contradict that conclusion.
- **Metadata shape:** `_validate_metadata`/`_validate_remove_fields`
  (`target/service.py:94-107`) reject non-dict `metadata`/`set_fields` and
  non-string `remove_fields` entries with `TargetValidationError`. No
  deeper shape constraint — metadata remains intentionally opaque,
  caller-owned data.

## 14. Existing test coverage

**Re-verified by running the suite this session, not by reading test file
names.**

| File | Ran this session | Result |
|---|---|---|
| `tests/test_target_lifecycle.py` | Yes | Passed (registry-level: on_conflict policy, stale-index regression, list ordering/stale-member-skip, metadata patch semantics, full delete sequence including shared-media reference counting, reindex, lock timeout, concurrent delete) |
| `tests/test_target_service.py` | Yes | Passed (service-level: identifier validation, media validation, create idempotency/conflict, metadata validation, pass-through/error-translation for every method) |
| `tests/test_target_cli.py` | Yes | Passed (every subcommand, human + `--json` mode, exit codes 0/1/2, reindex dry-run → real → idempotent) |
| `tests/test_target_crash_safety.py` | Yes | Passed (fault-injected partial failures across register/update/delete and the four cache `delete()` primitives — this is where the §7 crash-window gap was found and documented, not hidden) |
| `tests/test_embedding_lazy_import.py` | Yes | Passed (subprocess-verified absence of the ML stack from target-lifecycle code paths) |
| `tests/test_matching_handler.py` (incl. new deleted-target test) | Yes | Passed |
| Full `tests/` suite | Yes | **369 passed, 0 failed** — matches the count `docs/architecture/target-management-implementation.md` and `docs/usage.md` document |

**Genuinely missing, confirmed by absence this session:**
- No bulk repair tool/test for content-index entries left stale by
  content-changing registrations made *before* the §4 fix shipped (the
  fix is forward-only; documented as an accepted, low-severity limitation
  in the design and implementation docs, not silently unaddressed).
- No test asserting rejection of `target_id`/`target_version` collisions
  when a caller bypasses `TargetService` and calls `TargetRegistry.
  register_target` directly with a `:`-containing id (the residual gap in
  §13 has no regression test guarding it, though it is also not expected
  to regress since it was never fixed at that layer).
- No submission-time (`integration.candidate`) target-existence check or
  test — unchanged, out of scope for this phase by explicit prior
  decision (§9).
- No `target → job`/`target → result` reverse index or test — unchanged,
  explicit policy decision (§7 of this doc, §11 below), not an
  implementation gap.

## 15. Concrete gaps (current, re-audited)

Every gap the *original* pre-implementation audit found has been closed
**except** the two narrow, explicitly-documented, low-severity items
below — both were found by the implementation's own fault-injection test
pass (`tests/test_target_crash_safety.py`), not missed by it:

1. **Delete-path crash window** (§7): a crash between the two `SREM`s and
   the pipelined `DEL` in `delete_target` leaves a target invisible to
   `list_targets`/`find_by_content_hash` but still resolvable via
   `get_target`. Self-healing via retry. Not fixed; flagged for the next
   design review to decide whether reordering (deleting the record hash
   first) is worth the "what's the source of truth for existence
   mid-operation" trade-off that reordering would introduce.
2. **Pre-existing stale content-index entries are not retroactively
   repaired** (§4): the fix prevents *new* staleness; any staleness that
   already existed in a Redis instance before this phase shipped is
   unaddressed. Failure direction is always "delay shared-media GC,"
   never "delete something still referenced" — an accepted, bounded risk,
   not silently ignored.
3. **`TargetRegistry.register_target`'s charset validation gap** (§13):
   the collision-class fix lives only at the `TargetService` boundary: a
   direct `TargetRegistry` caller can still create a colliding identity.
   Explicit, scoped trade-off, not an oversight — but worth a future
   phase's explicit sign-off if direct-`TargetRegistry` operator use ever
   becomes a real pathway (today it is not; only tests/benchmarks call it
   directly).
4. **No submission-time target-existence check** in
   `integration.candidate`/`integration.submission` (§9) — unchanged,
   pre-existing, explicitly out of scope for target-management.
5. **No `target → job`/`target → result` reverse index** (§7, §11) — an
   explicit policy decision (hard-delete is allowed immediately; in-flight
   jobs against a deleted target fail loudly via the pre-existing
   `KeyError → PermanentFailure` path), not a missing feature.
6. **Pre-existing-Redis-instance migration is a manual step**: any
   already-deployed Redis instance with targets registered before this
   phase must have `python -m target.cli reindex` run once, or those
   targets stay invisible to `list_targets()` (their `get`/matching/
   caching behavior is completely unaffected — only listing is impacted).
   This is documented in the design doc §21 and the implementation doc
   §7.8, but is worth restating here as a live operational requirement,
   not just historical design commentary — if this repository's own
   Redis instance already has targets registered before this feature
   shipped, `reindex` needs to be run against it before `list` will show
   them. **NOT VALIDATED** whether this repository's actual deployed
   Redis instance (if any exists outside test db 15) currently needs this
   — outside this audit's scope to check (would require touching a
   production Redis instance, which the audit constraints forbid).

None of these six items requires new architecture, a new persistence
system, or a redesign of anything this phase's brief protects
(`TargetRegistry`, Redis schema, worker, crawler boundary, matching). Each
is either a documented, accepted trade-off or a small, targeted follow-up.

## 16. Minimal proposed implementation for the NEXT phase

**Because the CRUD/lifecycle interface itself is already built, "minimal
implementation" for the next phase means closing the residual items in
§15, not building the interface from scratch.** In priority order:

1. **(Optional, low urgency) Reorder or re-document the delete crash
   window** (§15.1): either accept it permanently as documented behavior
   (cheapest — it's already safe, just document it as a supported
   "delete is retry-safe" contract in `docs/usage.md`), or change the
   delete-step ordering to delete the record hash before the index
   `SREM`s, which flips which artifact is authoritative for "does this
   target exist" during a mid-crash window and deserves its own short
   design note before being changed, not a drive-by fix.
2. **(Operational, not code) Confirm whether any pre-existing deployed
   Redis instance needs a one-time `python -m target.cli reindex` run**,
   and run it if so. This is an operations task, not an implementation
   task.
3. **(Optional) A small regression test** asserting that
   `TargetRegistry.register_target` called directly (bypassing
   `TargetService`) with a `:`-containing id still succeeds today (pinning
   the documented, accepted residual behavior from §13/§15.3 so a future
   change to that method doesn't silently alter it without a conscious
   decision).
4. **(Optional, only if a real need appears) A bulk stale-content-index
   repair tool** for entries that predate the §4 fix — explicitly not
   recommended unless a concrete deployment shows this mattering; the
   failure direction is always benign (delayed GC).

**Nothing in this list requires touching `TargetRegistry`'s public
surface, the Redis schema, the worker, the crawler boundary, or the
matching pipeline.** No SQL/second persistence system, no HTTP framework,
no new locking primitive, and no soft-delete/inactive-state machinery are
warranted by anything found in this audit — consistent with the original
brief's explicit constraints, which the implementation already honored.

## 17. Proposed operator interface (already delivered)

**This section documents what exists, since the brief's proposed shape is
what was built — not a forward-looking proposal.**

`TargetService` (`target/service.py`) is the application-level boundary —
not new methods bolted onto `TargetRegistry`, not a bare script, not (yet)
an HTTP API. **VERIFIED FROM SOURCE.**

`target/cli.py` (`python -m target.cli`, stdlib `argparse` only, matching
the `python -m worker.main` convention) is the thin operator front-end:

```
python -m target.cli add MEDIA_PATH --id ID --version VERSION [--metadata KEY=VALUE ...] [--json]
python -m target.cli list [--json]
python -m target.cli get ID --version VERSION [--json]
python -m target.cli update-metadata ID --version VERSION [--set KEY=VALUE ...] [--unset KEY ...] [--json]
python -m target.cli delete ID --version VERSION [--json]
python -m target.cli reindex [--dry-run] [--json]
```

**Confirmed this session by reading `target/cli.py` in full:** every
subcommand calls exactly one `TargetService` method; the CLI issues zero
direct Redis commands and zero direct filesystem cache access — its only
filesystem/Redis-adjacent code is the environment-driven *construction* of
the `Redis`/`TargetRegistry` objects it hands to `TargetService`
(`_build_redis_client`/`_build_registry`, `target/cli.py:77-106`), which
is wiring, not lifecycle logic.

`docs/usage.md` documents this CLI as the primary registration path,
replacing the old "no CLI — call `TargetRegistry.register_target`
directly" language the original audit quoted. **VERIFIED FROM SOURCE**
(`docs/usage.md:89-180`, read this session).

## 18. Future dashboard boundary

**Already correctly shaped, verified this session.** `TargetService`
returns only plain, already-immutable `TargetRecord` dataclasses — never a
Redis key, Set member, or filesystem path — for every one of its five
lifecycle methods plus `reindex`. A caller of `TargetService` (the CLI
today; an HTTP handler tomorrow) needs to know nothing about:

- Redis key names or types (`target/keys.py` is imported by nothing
  outside `target/registry.py` and `target/registry.py`'s own tests).
- Cache file layout or `cache_entry_key()`'s hashing scheme.
- `SharedArtifactStore`/`SharedTargetMediaStore` paths or reference
  counting — `delete_target`'s reference check happens entirely inside
  `TargetRegistry`.
- DINOv2/embedding internals — `TargetService`/`target/cli.py` do not
  import the embedding engine at all (§10), confirmed by a live
  subprocess test.

A future HTTP layer would be a third, equally-thin client of
`TargetService`, exactly parallel to the CLI: parse the request, call one
`TargetService` method, map its typed exception (`target/errors.py`'s six
classes) to an HTTP status, serialize the returned `TargetRecord`. This
requires **zero changes** to anything below `TargetService` — the
boundary this audit is asked to evaluate the cleanliness of is already
exactly what such a layer needs. **No HTTP framework or web server exists
in this repository today** — confirmed by the absence of any such
dependency in `target/`'s imports.

## 19. Implementation scope for the NEXT phase

Given §15's findings, the next phase's scope should be limited to:

- A documented decision (not necessarily a code change) on the delete
  crash-window ordering question (§16.1).
- Confirming/running `reindex` against any pre-existing deployed Redis
  instance that needs it (§16.2) — an operational task.
- Optionally, one pinning regression test for the `TargetRegistry`-direct
  charset-bypass behavior (§16.3).
- Optionally, and only on concrete evidence of need, a stale-content-index
  bulk-repair tool (§16.4).

**Explicitly NOT in scope**, because it is already done: building
`create_target`/`list_targets`/`get_target`/`update_target_metadata`/
`delete_target`, the CLI, the identifier/media validation, the lifecycle
locking, the cache/shared-media `delete()` primitives, or any test
coverage for the above — all of this exists and passes today.

## 20. Explicit out-of-scope items

Unchanged from the original audit's scoping, and confirmed still true of
the actual delivered implementation (nothing in it violated these):

- No redesign of `TargetRegistry`'s existing methods, `target/keys.py`'s
  key *shapes*, `target/versioning.py`, or the Redis job/result schema.
- No SQLite or any second persistence system — Redis remains sole source
  of truth; confirmed, no new dependency was added anywhere in `target/`.
- No web framework or HTTP server — CLI + service layer only, confirmed
  by dependency inspection.
- No reference-counting infrastructure beyond reusing the pre-existing
  `find_by_content_hash` — confirmed, `delete_target` adds no new
  bookkeeping structure for this.
- No policy requiring rejection of deletion while jobs are queued/
  in-flight — the existing fail-closed `KeyError → PermanentFailure` path
  is reused as-is and was explicitly chosen over a soft-delete/inactive
  flag or a new reverse index (§7, §11), confirmed unchanged this session.
- No crawler-repo changes — confirmed, `fingerprinter/` and `crawler/` are
  separate repositories and neither this phase's implementation nor this
  audit touched the latter.
- No cascade-delete of `ResultRecord`s or queued jobs on target deletion —
  confirmed unchanged (`delete_target` never references
  `fingerprint:job:*`/`fingerprint:result:*` keys, verified by reading the
  method in full).
