# Target Management Audit — CRUD / Multi-Target Operator Interface

## 1. Status

**AUDIT ONLY. No production code, tests, configuration, or Redis state was
modified to produce this document.**

Scope: `target/`, `integration/`, `work_queue/`, `worker/`, plus every test
file that exercises target registration/identity/versioning/cache, and the
`docs/usage.md` / `docs/architecture/system-architecture.md` /
`docs/architecture/phase-12-crawler-fingerprinter-integration.md` /
`docs/architecture/phase-13d-*.md` narrative around them. `old/` was not
read except where a search hit landed there (one irrelevant hit, noted
below) — the phase brief says not to read it unless necessary, and nothing
here needed it.

Every claim below is labeled:

- **VERIFIED FROM SOURCE** — read directly in the file/line cited.
- **VERIFIED BY TEST** — an existing test in `tests/` asserts this behavior.
- **INFERRED** — a reasonable conclusion from source that isn't directly
  asserted by any single line or test.
- **NOT IMPLEMENTED** — searched for, confirmed absent.
- **NOT VALIDATED** — plausible/intended per docs or design, but no test or
  code path confirms it either way.

## 2. Audit objective

Determine what a clean, operator/dashboard-facing lifecycle interface
(create / list / get / update / delete a target, explicitly supporting
multiple simultaneous targets) would need to wrap, given the *existing*
`TargetRegistry`/cache/versioning architecture — without redesigning that
architecture, and without implementing anything yet.

## 3. Current target lifecycle (as it exists today)

**VERIFIED FROM SOURCE.** The only lifecycle operations that exist are:

| Operation | Exists? | Callable |
|---|---|---|
| Create/register | Yes | `TargetRegistry.register_target()` |
| Get one | Yes | `TargetRegistry.get_target()` |
| Find by content hash | Yes | `TargetRegistry.find_by_content_hash()` |
| List all | **No** | — |
| Update (metadata-only) | **No** | — |
| Update (new content, same version) | Yes, as a side effect of re-registration | `register_target()` again |
| Delete | **No** | — |

There is no production call site for `register_target` anywhere in this
repository — `target/shared_storage.py:184` says so explicitly in its own
docstring ("`register_target` has no production call site"), and
`docs/usage.md` (§"Register a target") confirms it's invoked by hand,
per-target, by whatever external process owns target ingestion: *"There is
no CLI for this — call `TargetRegistry.register_target` directly."*
**VERIFIED FROM SOURCE.**

## 4. Current registration API

**Callable:** `TargetRegistry.register_target(target_id: str, target_version: str, media_path: str, media_metadata: Optional[dict] = None) -> TargetRecord` (`target/registry.py:74-112`). **VERIFIED FROM SOURCE.**

- **`target_id` semantics:** caller-assigned opaque string identifying "the
  movie," never derived from content or filename (`target/identity.py:10-16`).
- **`target_version` semantics:** caller-assigned label, independent of
  content — two versions of the same `target_id` are expected to hold
  different content, but nothing enforces that (§7 below).
- **`media_path` semantics:** informational only — read once (streamed) to
  compute `content_sha256`, then stored verbatim on the record. Never
  contributes to identity (`target/identity.py:29-37`, `sha256_file`
  docstring: "Never reads the filename or any filesystem metadata").
- **Content SHA-256 behavior:** `sha256_file(media_path)` streams the file
  in 64 KiB chunks (`target/identity.py:26,29-37`). Computed synchronously,
  on every call, even for a re-registration of the same `(target_id,
  target_version)` — there is no short-circuit if the path is unchanged.
- **Redis records created:** `HSET fingerprint:target:{id}:{version}` (the
  record itself) and `SADD fingerprint:target:content:{content_sha256}`
  with an encoded `(id, version)` member (`target/keys.py:13-19`,
  `registry.py:107-111`). **VERIFIED FROM SOURCE.**
- **Local/shared cache artifacts:** **none**, at registration time. Only
  if a `media_store` (`SharedTargetMediaStore`) was injected does
  registration additionally push the raw media bytes into shared storage,
  keyed by `content_sha256` alone (`registry.py:93-95`,
  `target/shared_storage.py:176-221`). Embedding/segment caches are **not**
  touched by `register_target` at all.
- **Idempotency:** re-registering the same `(target_id, target_version)`
  is a safe upsert — `created_at` is preserved from the existing record,
  `updated_at` and every other field are overwritten (`registry.py:96-106`).
  **VERIFIED BY TEST** (`tests/test_target.py::test_cache_miss_for_different_target_content_hash`,
  lines 151-162): re-registering under the *same* `(target_id,
  target_version)` with **different bytes** is accepted with no error, and
  correctly invalidates any embedding previously cached for the old
  content hash.
- **Same media bytes registered under different IDs:** explicitly
  supported and detectable — `find_by_content_hash()` exists precisely for
  this. **VERIFIED BY TEST**
  (`test_target_identity_independent_of_filename`, lines 72-84): two
  different `target_id`s with byte-identical content both register
  successfully and both show up under `find_by_content_hash(hash)`.
  Duplicate content is allowed by design, not rejected.
- **Embeddings computed immediately or lazily:** lazily, cache-first,
  build-on-miss. Registration never calls the embedding engine.
  `docs/usage.md:121-124` states this explicitly, and it matches
  `get_or_build_segment_embedding`'s implementation (`registry.py:201-273`).
  **VERIFIED FROM SOURCE.**

### Gap found: stale content-index entries

**VERIFIED FROM SOURCE.** `register_target` only ever `SADD`s the *new*
`content_sha256`'s index set; it never `SREM`s the target's *previous*
`content_sha256` entry when content changes under a fixed `(target_id,
target_version)`. After the re-registration in
`test_cache_miss_for_different_target_content_hash`,
`find_by_content_hash(<old hash>)` would still return `(target-1, v1)`
even though `get_target("target-1", "v1").content_sha256` no longer equals
that hash. No test currently checks this (the existing test only checks
the *new* hash's cache behavior, not the old hash's index state). This is
a latent correctness gap in the reverse index, relevant to any future
delete/GC design, not a crash risk today because nothing currently reads
that stale entry for a decision.

## 5. Current listing/get capabilities

**`get_target(target_id, target_version) -> Optional[TargetRecord]`
exists** (`registry.py:114-116`) — single-record lookup only, by exact key.

**NOT IMPLEMENTED:**
- List all registered targets.
- List all versions for a given `target_id`.
- "Is this target active" (there is no active/inactive concept at all —
  a target either has a Redis hash or it doesn't).

Confirmed by exhaustive grep across `target/`, `worker/`, `integration/`,
`tests/` for `list_target`, `def list`, `TargetService`, `TargetManager`,
`enumerate` — zero relevant hits (`old/storage/file_retention.py` was the
only hit anywhere in the repo, and it's an unrelated legacy module).

A future listing implementation has two viable strategies given the
current key design:
1. **`SCAN fingerprint:target:*` and filter.** Technically possible
   without touching `TargetRegistry`, but fragile: `target_key()` is a
   plain f-string join on `:`, and nothing restricts `target_id`/
   `target_version` from containing `:` themselves (see §14) — so parsing
   a scanned key back into `(target_id, target_version)`, or reliably
   excluding the `:embeddings`/`:segment_embeddings` suffix keys, is
   ambiguous in the general case. Also an O(keyspace) SCAN, not O(targets).
2. **A small explicit index Set**, e.g. `fingerprint:target:index`,
   `SADD`ed alongside the existing `target_content_index_key` write in
   `register_target`, using the same `encode_content_index_member`
   encoding already in `target/keys.py`. This mirrors a pattern the
   codebase already trusts and avoids all key-parsing ambiguity.
   **Recommended approach** — see §16.

## 6. Current update capabilities

**NOT IMPLEMENTED** as a distinct operation. The only way to change an
existing target today is to call `register_target()` again for the same
`(target_id, target_version)`, which does a **full overwrite** of
`media_path`, `content_sha256` (via re-hashing whatever file is at
`media_path` now), and `media_metadata` — there is no partial/metadata-only
update path. Calling it again with `media_metadata=None` (the default)
**silently clobbers** any previously-stored metadata back to `{}`, since
the record is rebuilt from scratch each call (`registry.py:99-106`) and
`media_metadata or {}` has no memory of the prior value. **VERIFIED FROM
SOURCE**, not directly tested (no test re-registers *without* passing
metadata after having set some).

Field mutability, from source:
- **Immutable by construction:** `target_id`, `target_version` (they're the
  key, not stored-and-editable fields), `created_at` (preserved across
  re-registration).
- **Derived, not settable:** `content_sha256` — always recomputed from
  whatever bytes `media_path` points to; never accepted as a caller-supplied
  value anywhere in the codebase. Correctly treated as identity, not metadata.
- **Mutable only via full re-registration today:** `media_path`,
  `media_metadata`, `updated_at`.

**Recommendation (do not implement yet):** Do not invent a generic
`update_target()` that can silently swap content under an existing
`target_version` — that already works today via re-registration and is
exactly the operation §12 argues should be steered away from. The next
phase should expose two narrow, explicit operations instead:
1. A genuinely metadata-only patch (`update_target_metadata`) that reads
   the existing record, merges the caller's partial metadata dict, and
   writes back *without* touching `media_path`/`content_sha256`/re-hashing
   anything. This does not exist in `TargetRegistry` today and would be a
   small, additive method.
2. "New content" is reframed as "register a new `target_version`" — an
   operation that is already fully supported and requires zero registry
   changes.

## 7. Current deletion capabilities

**NOT IMPLEMENTED.** Confirmed by exhaustive grep (`delete`, `deregister`,
`remove_target`) across `target/`, `worker/`, `integration/`, `tests/` —
zero hits.

### Target-owned state inventory (VERIFIED FROM SOURCE)

| Artifact | Key | Owned exclusively by one `(target_id, target_version)`? | Safe to delete on target delete? |
|---|---|---|---|
| Registry record | `fingerprint:target:{id}:{version}` (Redis hash) | Yes | Yes |
| Embedding metadata summary | `fingerprint:target:{id}:{version}:embeddings` (Redis hash) | Yes | Yes |
| Segment embedding metadata summary | `fingerprint:target:{id}:{version}:segment_embeddings` (Redis hash) | Yes | Yes |
| Content-hash reverse index membership | `fingerprint:target:content:{content_sha256}` (Redis Set) | **No — content-keyed, one entry per `(id,version)` sharing that hash** | Only remove *this target's* member, never the whole set |
| Local pooled embedding cache file | `FilesystemEmbeddingCache`, named by `cache_entry_key(id, version, content_sha256, spec)` | Yes (id+version are part of the key) | Yes |
| Local segment embedding cache file | `FilesystemSegmentEmbeddingCache`, same keying | Yes | Yes |
| Shared pooled/segment cache (Phase 13D) | `SharedFilesystemEmbeddingCache`/`SharedFilesystemSegmentEmbeddingCache`, same `cache_entry_key` keying | Yes | Yes |
| **Shared raw target media** (Phase 13D) | `SharedTargetMediaStore`, keyed by **`content_sha256` only** (`target/shared_storage.py:199-200`) — no `target_id`/`target_version` in the key | **No** | **Not safely deletable without checking `find_by_content_hash` first** |
| Result records | `fingerprint:job:{job_id}:result`, `fingerprint:result:{job_id}` — carry `target_id`/`target_version` fields but are keyed by `job_id` | N/A (historical, not target-owned in a deletion sense) | Should **not** cascade-delete — see §11 |
| Queued/in-flight jobs | Redis Stream entries carrying `target_id`/`target_version` fields, keyed by stream position, not target | N/A | Should **not** be touched by delete — see §11 |
| `target/lock.py` build-on-miss lock | `fingerprint:lock:target:{cache_key}` | Yes | Self-expiring (TTL); no cleanup needed |

**The one artifact that genuinely requires a reference check before
deletion is `SharedTargetMediaStore`.** Everything else — including every
embedding/segment cache entry, local or shared — is exclusively owned by
its exact `(target_id, target_version, content_sha256, spec)` tuple by
construction (`target/versioning.py:75-88`, `cache_entry_key`), so two
different targets never collide on a cache file even if their content is
byte-identical. `SharedTargetMediaStore` is the sole exception because it
deliberately de-duplicates by content only (§8). A correct delete
implementation must call `find_by_content_hash(record.content_sha256)`
after removing the target's own registry record and index membership, and
only delete the shared media blob if no other `(target_id, target_version)`
still references that hash. **This does not require a general reference-
counting system** — `find_by_content_hash` (already implemented) is
sufficient, provided the content-index `SREM` for the deleted target
happens *before* the check.

**No target→job or target→result index exists.** There is currently no
way to answer "which jobs/results reference target X" without a full scan
of job/result keyspace. See §11 for what this means for delete safety.

## 8. Multiple-target support

**Already fully supported, proven from source and tests. No redesign
needed.**

- **Redis key design:** every target-related key is namespaced by
  `(target_id, target_version)` or `content_sha256` — never a
  singleton/global key (`target/keys.py`, entire file).
- **Job schema:** `work_queue.jobs.Job` carries `target_id`/`target_version`
  as required, per-job fields (`work_queue/jobs.py:36-46,54-63`) — not
  worker/process configuration. **VERIFIED FROM SOURCE.**
- **Candidate/submission API:** `integration.candidate.FingerprintCandidate`
  requires `target_id`/`target_version` per candidate, validated non-empty
  (`integration/candidate.py:104-105,123-125`). **VERIFIED FROM SOURCE.**
- **Matching handler:** resolves `job.target_id`/`job.target_version`
  fresh, per job, at handler-invocation time (`worker/matching_handler.py:189,197-198`)
  — no cached "current target" state anywhere in the handler or `Worker`.
- **Proof of coexistence:** `tests/test_integration_e2e.py` registers
  `"target-1"`, `"target-2"`, and `"target-unrelated"` in the same Redis
  instance and exercises cross-target no-match and multi-worker scenarios
  (lines 91-264). **VERIFIED BY TEST.**
- **Worker is target-agnostic:** confirmed both in source (no target
  reference anywhere in `worker/main.py`'s wiring) and in
  `docs/usage.md:83-85`: *"a worker process is target-agnostic: it just
  processes whatever jobs arrive on the Redis stream."*

**Conclusion: the gap is entirely in the missing operator-facing lifecycle
surface (list/delete/safe-update), not in the underlying multi-target
data-plane, which already works correctly for arbitrarily many
simultaneous targets.**

## 9. Crawler integration

**There is no crawler code in this repository.** `find` confirms the only
hit for "crawl" anywhere outside `old/` is
`docs/architecture/phase-12-crawler-fingerprinter-integration.md` itself.
The crawler lives in a separate, sibling repository
(`integration/candidate.py`'s own docstring, lines ~16-22, refers to *"the
sibling crawler repo"*). What exists on this side is the boundary the
crawler is expected to call: `integration.submission.FingerprintJobSubmitter.submit()`.

- `target_id`/`target_version` are mandatory, per-candidate,
  caller-supplied fields with no default and no global fallback
  (`integration/candidate.py:104-105`). **VERIFIED FROM SOURCE.**
- The crawler (or any caller) can already switch targets between calls
  with zero code changes — it's a plain function argument on
  `FingerprintCandidate`, not configuration baked into the submitter or
  worker.
- **What happens if the configured target does not exist:**
  `FingerprintCandidate.validate()` only checks that `target_id`/
  `target_version` are non-empty strings — it never calls
  `TargetRegistry.get_target()` (**VERIFIED FROM SOURCE**, `candidate.py:110-132`
  has no registry import at all). So a job against an unregistered target
  **enqueues successfully** (`SubmissionOutcome.ENQUEUED`) and only fails
  once a worker claims it and the matching handler calls the registry,
  which raises `KeyError` → `PermanentFailure`
  (`worker/matching_handler.py:231-232`, **VERIFIED BY TEST**
  `tests/test_matching_handler.py::test_unknown_target_raises_permanent_failure`,
  lines 187-195). This is fail-closed and correct (no silent bad match),
  but it means a typo'd `target_id` is only caught after the job traverses
  the full queue, not at submission time. **No test exercises the
  submission-time behavior** (no `test_submit_rejects_unknown_target` or
  equivalent exists) — this is a gap in test coverage of an already-known,
  intentionally-minimal boundary (the phase-12 brief explicitly scoped
  `FingerprintCandidate.validate()` to cheap, local, pre-Redis checks only).

## 10. Worker integration

- `worker/matching_handler.py::build_matching_handler` receives
  `target_id`/`target_version` from `job.target_id`/`job.target_version`
  at call time (lines 189, 197-198) — never from process-level
  configuration. `worker/main.py`'s `WorkerConfig` has no target field at
  all (`docs/usage.md`'s env var table confirms this — no `TARGET_ID`
  variable exists).
- **Multiple targets concurrently:** true at the fleet level — distinct
  worker *processes* (or consumer-group members) can each be mid-job
  against a different target at the same instant (`docs/usage.md`'s
  "multiple worker processes on one host" example +
  `tests/test_integration_e2e.py::test_multiple_workers_process_distinct_jobs_without_duplication`,
  **VERIFIED BY TEST**). A single `Worker.run()` loop processes one job at
  a time (its own dispatch loop, `worker/fingerprint_worker.py`), so
  concurrency across targets comes from running multiple worker processes,
  not from one process handling two targets simultaneously — this matches
  the deployment model documented throughout, not a limitation specific to
  target management.
- **Artifact isolation:** guaranteed by construction — `cache_entry_key()`
  hashes `(target_id, target_version, content_sha256, spec)` together
  (`target/versioning.py:75-88`), so no two distinct targets can ever
  collide on a cache entry, and the build-on-miss lock
  (`target/lock.py`) is scoped to that same exact tuple
  (`registry.py:249`), correctly serializing concurrent builds *of the
  same target* while never blocking unrelated targets against each other.
  **VERIFIED BY TEST**: `tests/test_target_build_on_miss.py::test_concurrent_miss_builds_only_once`.

## 11. Cache/artifact ownership

Covered in detail in §7's table. Summary of the invariant that matters
most for a future delete implementation:

**Content-addressed identity is preserved everywhere and must not be
replaced.** `sha256_file()` never reads filename/path/mtime
(`target/identity.py:29-37`), and every cache key downstream of it
(`cache_entry_key`) is a pure function of `(target_id, target_version,
content_sha256, spec)` — never hostname, PID, or local timestamp
(`target/shared_storage.py:44-48` states this explicitly as the property
that makes cross-host sharing correct). The **one place** this invariant
intentionally trades exclusivity for deduplication is
`SharedTargetMediaStore`, which addresses raw media by `content_sha256`
alone, by design (`target/shared_storage.py:176-192`'s docstring explains
this was chosen because no per-target media-acquisition source exists to
justify per-target storage). This is not a bug — it is a deliberate,
documented dedup choice — but it is the one place a future delete
operation must reference-check (via the already-existing
`find_by_content_hash`) rather than delete unconditionally.

## 12. Concurrency/idempotency

- `register_target` performs plain `HSET`/`SADD` — **no `WATCH`/CAS/lock**
  around the read-then-write of `created_at` (`registry.py:96-111`
  reads `get_target` then writes, non-atomically). Two callers racing to
  register the *same* `(target_id, target_version)` with *different*
  media at the same moment: last `HSET` wins silently, no error, no test
  covers this. This mirrors the acceptable "last write wins" pattern used
  elsewhere in this codebase for genuinely idempotent writes (e.g., the
  content index `SADD`, which *is* safe to race because Set membership is
  idempotent regardless of order) — but here the two writers may be
  registering **different content** under the same identity, so the race
  has an observable, non-idempotent outcome (which content wins is
  order-dependent). This is a **real, unaddressed gap**, not a
  false-positive concern.
- `register_embedding`/`register_segment_embedding` are effectively safe
  under race because the underlying cache `put()` calls are last-write-wins
  overwrites of the *same* content for the *same* key (idempotent by
  construction, since `spec`+`content_sha256` pin the input).
- The build-on-miss path (`get_or_build_segment_embedding`) already has
  correct locking (`target/lock.py`'s `RedisLock`, **VERIFIED BY TEST**).
  There is no analogous lock around plain `register_target`.
- **Smallest fix for the next phase** (not implemented here): wrap
  `register_target`'s read-modify-write in the *same* `RedisLock`
  primitive already used for build-on-miss, scoped to
  `target_key(target_id, target_version)`. No new locking primitive is
  needed — this reuses `target/lock.py` verbatim.
- Delete-vs-register and delete-vs-delete races are **not applicable
  today** since delete doesn't exist; whatever locking scheme is chosen
  for delete in the next phase should reuse the same `RedisLock` scoped to
  the same key, for the same reason.

## 13. Security/input validation

- **`target_id`/`target_version` format:** unconstrained. The only check
  anywhere is "non-empty string" (`integration/candidate.py:123-125`).
  Neither `TargetRegistry.register_target` nor `TargetRecord` validates
  charset or length.
- **Concrete collision risk found:** `target_key()` is
  `f"fingerprint:target:{target_id}:{target_version}"`
  (`target/keys.py:13-14`), a plain `:`-joined f-string, and nothing
  prevents `target_id`/`target_version` from containing `:` themselves.
  `target_id="a:b", target_version="c"` and `target_id="a",
  target_version="b:c"` both produce the literal key
  `"fingerprint:target:a:b:c"` — a genuine key-collision class, not
  hypothetical. (`target/keys.py`'s own docstring for
  `encode_content_index_member` already acknowledges ids/versions "may
  contain `:`" and works around it there with a unit-separator encoding —
  but `target_key()` itself has no equivalent safeguard.) **This is a
  concrete requirement the next phase must enforce**: restrict
  `target_id`/`target_version` to a safe charset (e.g. no `:`, no control
  characters) at the operator-interface boundary.
- **Filesystem input (`media_path`):** `sha256_file` does a plain
  `open(path, "rb")`. A directory raises `IsADirectoryError`, a missing
  file raises `FileNotFoundError`, a huge file streams safely in 64 KiB
  chunks (no memory blowup), a symlink is followed by the OS with no
  special handling either way. **None of these are caught or mapped** by
  `TargetRegistry` today — `register_target` has no `try`/`except` of its
  own, so any of these propagate as raw Python exceptions to whatever
  calls it (today: only tests/benchmarks call it directly). The future
  operator-facing `create_target` should validate (exists, is a regular
  file, is readable, non-empty) and raise a typed, structured error rather
  than leaking raw `OSError`s — but this is implementation-phase work.
- **Path traversal:** `media_path` is a trusted, operator-supplied local
  filesystem path (the process that owns target ingestion), not
  attacker-controlled network input — unlike `acquisition/ssrf_guard.py`'s
  URL-facing protections (which exist for candidate URLs, a genuinely
  untrusted input), there is no equivalent traversal concern here to
  "fix"; ordinary file-exists/readable validation is sufficient.
- **Malformed target IDs/versions:** no length cap exists anywhere; an
  operator interface should probably impose one (e.g. matching common
  Redis-key-friendly conventions) but no concrete requirement beyond the
  `:`/collision issue above is evidenced by the source.

## 14. Existing test coverage

**Strong:**
- Registration, identity (filename-independence), content-hash dedup
  across IDs, versioning coexistence, re-registration/idempotency and its
  cache-invalidation effect (`tests/test_target.py`, `test_target_lock.py`).
- Embedding cache compatibility matching on every `EmbeddingSpec`
  dimension (`test_target.py`, `test_shared_target_storage.py`).
- Build-on-miss locking, including concurrent-miss-builds-once and
  lock-timeout behavior (`tests/test_target_build_on_miss.py`).
- Multi-host shared-storage simulation, including partial-write and
  unreachable-store failure semantics (`tests/test_shared_target_storage.py`).
- Multi-target coexistence and cross-target no-match, missing-target
  permanent failure, multi-worker distinct-job processing
  (`tests/test_integration_e2e.py`, `tests/test_matching_handler.py`).

**Missing (confirmed by absence, not inferred):**
- No test for listing (operation doesn't exist).
- No test for delete (operation doesn't exist).
- No test for concurrent `register_target` races on the same
  `(target_id, target_version)` with differing content (§12).
- No test for submission-time missing-target validation (§9) — by design,
  since that check doesn't exist yet either.
- No test for metadata-only update (operation doesn't exist).
- No test asserting the stale content-index entry left behind by a
  content-changing re-registration (§4's gap) — the *cache* invalidation
  is tested; the *index* staleness is not.

## 15. Concrete gaps (summary)

1. No list-all / list-versions-for-id capability. (§5)
2. No delete capability, and no target→job or target→result index to
   support safe/informed deletion decisions. (§7, §11)
3. No metadata-only update; only path is full re-registration, which
   silently clobbers `media_metadata` if omitted and can silently swap
   content under an existing `target_version`. (§6)
4. Stale content-index (`target_content_index_key`) entries after a
   content-changing re-registration under a fixed `(id, version)`. (§4)
5. `register_target` has no concurrency guard — same-identity races with
   differing content are last-write-wins, silently. (§12)
6. `target_id`/`target_version` have no charset restriction, and a
   concrete key-collision class exists via unescaped `:` in `target_key()`.
   (§13)
7. No submission-time existence check for `target_id`/`target_version` —
   bad targets are only caught worker-side, after full queue traversal.
   (§9 — likely acceptable given the phase-12 boundary's intentional
   minimalism, but worth the next phase deciding explicitly rather than
   by omission.)
8. `SharedTargetMediaStore` dedups by content only, so it's the one
   artifact type a delete implementation cannot remove unconditionally —
   requires a `find_by_content_hash` check first. (§7)

## 16. Minimal proposed implementation (for the NEXT phase — not built here)

A single new module, `target/service.py`, exposing a `TargetService` class
that **composes** the existing `TargetRegistry` + caches +
`SharedTargetMediaStore` (all unchanged) and adds exactly the operations
that don't exist:

```
class TargetService:
    def __init__(self, registry: TargetRegistry, redis_client: Redis): ...

    def create_target(self, target_id, target_version, media_path, metadata=None) -> TargetRecord
        # thin wrapper over register_target(); adds id/version charset
        # validation (§13) and typed errors for bad media_path (§13).

    def list_targets(self) -> list[TargetRecord]
        # backed by a new small index Set, e.g. fingerprint:target:index,
        # SADD'ed in create_target alongside the existing content-index
        # SADD, using the same encode/decode helpers target/keys.py
        # already has for the content index. No new Redis data model
        # beyond "one more Set of the same shape."

    def get_target(self, target_id, target_version) -> Optional[TargetRecord]
        # pass-through to TargetRegistry.get_target.

    def update_target_metadata(self, target_id, target_version, metadata: dict) -> TargetRecord
        # new, small TargetRegistry method: merge into existing
        # media_metadata, do NOT touch media_path/content_sha256/re-hash.

    def delete_target(self, target_id, target_version) -> None
        # 1. SREM the target's member from target_content_index_key(hash)
        #    and from the new list-index Set.
        # 2. DELETE the target/embeddings/segment_embeddings hashes.
        # 3. Delete the target's own (id,version)-exclusive cache files
        #    (local and/or shared — safe unconditionally, §7).
        # 4. find_by_content_hash(hash) on the *remaining* index; only if
        #    empty, delete the SharedTargetMediaStore blob for that hash.
        # 5. Do NOT touch ResultRecords or queued jobs (§11) — they are
        #    historical/in-flight, not target-owned.
```

This requires exactly one small addition to `TargetRegistry` itself
(`update_target_metadata`, plus reusing `find_by_content_hash` which
already exists) and one new Redis Set (the list-index) — no redesign of
`target_key()`, `cache_entry_key()`, the Redis schema, the worker, the
crawler-facing contract, or matching. `TargetService.register_target`'s
concurrency gap (§12) should be closed by wrapping the read-then-write in
`target/lock.py`'s existing `RedisLock`, scoped to `target_key(id,
version)` — no new locking primitive.

## 17. Proposed operator interface

`TargetService` (§16) is the right layer — not new methods bolted onto
`TargetRegistry` itself (which should stay focused on identity/versioning/
cache-compatibility, per its own module docstring's "independent
collaborator" philosophy), not a bare CLI script, and not an HTTP API yet.

A small CLI (`python -m target.cli`, argparse, stdlib only — matching the
`python -m worker.main` convention `docs/usage.md` already documents) is
appropriate as a thin front-end over `TargetService`, e.g.:

```
python -m target.cli add /path/movie.mp4 --id blast --version v1
python -m target.cli list
python -m target.cli get blast --version v1
python -m target.cli update-metadata blast --version v1 --set key=value
python -m target.cli delete blast --version v1
```

The CLI must call `TargetService`, never touch Redis/filesystem directly
— this is what makes a future HTTP layer or dashboard backend a second,
equally-thin caller of the same service rather than a second
implementation of target lifecycle logic.

## 18. Future dashboard boundary

`TargetRegistry` already returns typed dataclasses (`TargetRecord`,
`EmbeddingCacheEntry`, `SegmentEmbeddingCacheEntry`) rather than raw Redis
structures for every operation it supports — the boundary is already clean
for create/get. The only reason a dashboard would need to know about Redis
keys, cache file layout, or embedding internals today is that list/delete
don't exist yet. `TargetService` (§16) closes that gap without changing
what `TargetRegistry` already hides well. A dashboard backend should call
`TargetService`, never `TargetRegistry`/`SharedArtifactStore`/Redis
directly — same rule as the CLI.

## 19. Implementation scope for the NEXT phase

In scope (per this audit's findings):
- `target/service.py`: `TargetService` with `create_target`, `list_targets`,
  `get_target`, `update_target_metadata`, `delete_target`.
- One small additive method on `TargetRegistry`: metadata-only update.
- One new Redis Set (target list index), written alongside the existing
  content-index write, using the existing encode/decode helpers.
- `target_id`/`target_version` charset validation at the `TargetService`
  boundary (reject `:` and control characters — closes §13's collision
  class).
- Typed validation errors for bad `media_path` (missing/directory/empty)
  at the `TargetService` boundary.
- `RedisLock`-guarded `register_target`/`create_target` to close the
  race in §12.
- A thin `target/cli.py` (argparse) front-end over `TargetService`.
- Tests for every new operation, plus regression tests for the two gaps
  found in this audit that predate it (stale content-index entry on
  content-changing re-registration; register-target race).

## 20. Explicit out-of-scope items

- No redesign of `TargetRegistry`, `target/keys.py`, `target/versioning.py`,
  `target/cache.py`, `target/segment_cache.py`, `target/shared_storage.py`,
  the Redis job/result schema, the worker, or the matching pipeline.
- No SQLite or any second persistence system for target metadata — Redis
  remains the sole source of truth, per the existing architecture.
- No web framework or HTTP server in this phase — CLI + service layer only,
  ready for an HTTP layer to be added later as a thin wrapper.
- No reference-counting infrastructure beyond reusing the existing
  `find_by_content_hash` for the one artifact that needs it
  (`SharedTargetMediaStore`).
- No policy decision on rejecting deletion while jobs are queued/in-flight
  — current fail-closed behavior (`KeyError` → `PermanentFailure`) is
  already safe and sufficient; a soft-delete/inactive flag is flagged as
  an **open question** for the next phase to decide explicitly, not a
  requirement derived from this audit.
- No crawler-repo changes — the crawler lives outside this repository and
  already supports arbitrary `target_id`/`target_version` per call with no
  code changes needed on either side.
- No cascade-delete of `ResultRecord`s or queued jobs on target deletion.
