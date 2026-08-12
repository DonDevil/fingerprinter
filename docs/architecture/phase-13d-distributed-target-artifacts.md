# Phase 13D — Distributed Target Artifact Storage

## Status: IMPLEMENTED (single-process simulated multi-host; real multi-host deployment REQUIRES MULTI-HOST VALIDATION)

This is the implementation phase for the blocker the Phase 13D audit
(`docs/architecture/phase-13d-multi-host-target-cache-audit.md`) traced in
full detail and did not fix. The audit remains unchanged and is the
investigation record; this document is the implementation record. Git
revision this phase started from: `c3bfdd5`.

---

## 1. Problem

Restated from the audit (§4/§5), precisely: `target/lock.py`'s `RedisLock`
correctly serializes *who builds* a target's embedding across any number
of hosts — `SET NX PX` is a genuine fleet-wide primitive. But the artifact
it protects, `FilesystemSegmentEmbeddingCache`/`FilesystemEmbeddingCache`
(`target/segment_cache.py`, `target/cache.py`), writes to whichever host's
local disk happens to win the lock. No code path ever propagates that
write to any other host. A losing host's poll loop is checking a resource
structurally incapable of ever reflecting the winner's result — after its
poll times out (`DEFAULT_POLL_TIMEOUT_S = 600.0`), it duplicates the build
from scratch. A second, adjacent gap (audit §3.5): `TargetRecord.media_path`
is likewise host-local, with no acquisition mechanism for a host that
doesn't have the file — so even a fixed embedding cache wouldn't let a
losing host build the target at all.

---

## 2. Audit finding

Restated from the audit's own restatement (§12): the fix is not a lock
redesign — the lock already does the right thing once the thing it
protects is genuinely shared. The fix is a storage-backend swap behind the
existing `TargetEmbeddingCache`/`SegmentEmbeddingCache` ABCs
(`target/cache.py`, `target/segment_cache.py`), which `TargetRegistry`
(`target/registry.py`) and `worker/matching_handler.py` reference only by
interface, never by concrete class — confirmed unchanged by this phase's
own inspection of those two call sites.

---

## 3. Architecture decision

**Chosen: Option A1 — shared filesystem-backed storage**
(`target/shared_storage.py`'s `SharedArtifactStore`, a directory-based,
content-addressed blob store), implementing new
`TargetEmbeddingCache`/`SegmentEmbeddingCache` classes
(`target/shared_cache.py`'s `SharedFilesystemEmbeddingCache`/
`SharedFilesystemSegmentEmbeddingCache`).

### Why this, not Redis (Option B)

The audit's own performance section (§10) extrapolates ~5 GiB of embedding
vector data at production scale (10,000 targets × ~30 segments). Redis in
this system is currently an in-memory coordination/job-state store with a
negligible footprint (one small hash + one lock key per target — audit
§2). Moving ~5 GiB of vector data into that same Redis instance would make
memory pressure from the embedding cache compete directly with the memory
this system's job-claim/lease/retry correctness already depends on — a
different, larger operational risk than what this phase is trying to
close, and the audit explicitly says not to choose Redis merely because it
already exists. Rejected.

### Why not object storage (Option A2 / S3-compatible)

`requirements.txt` (VERIFIED by direct inspection this session) has no
S3/object-storage client dependency (no `boto3`, no equivalent), and no
document in this repository (`design-proposal-1.md`, any `phase-*.md`)
names a provisioned object-storage endpoint. The task brief is explicit:
do not invent credentials, buckets, or endpoints that don't exist. Not
chosen — but the storage boundary this phase builds
(`SharedArtifactStore`'s three-method interface: `get_bytes`/`put_bytes`/
`exists`, plus `put_file`/`get_file` for larger media) is exactly the
shape a future S3-backed implementation would need to fill in; nothing
else in the codebase would need to change (confirmed by this phase's own
`worker/main.py` wiring — swapping the backend is one function,
`build_registry()`).

### Why A1 over "do nothing until real infrastructure is provisioned"

The task brief's own fallback instruction: "If the repository does not
currently contain enough information to make a production infrastructure
decision, choose the smallest backend abstraction that allows the
implementation to proceed cleanly and explicitly document the remaining
infrastructure dependency." A1 requires zero new dependencies, reuses the
exact atomic-write primitive (`tempfile.mkstemp` + `os.replace`) the
existing per-host caches already use and this codebase already trusts, and
is a pure deployment-configuration decision away from being genuinely
multi-host — an operator points `SHARED_ARTIFACT_STORE_PATH` at a real
NFS/cluster-filesystem mount. That remaining dependency (a provisioned
shared mount) is real and explicitly **not** solved by this phase — see
§11 below.

### Rejected alternative: "just repoint `TARGET_CACHE_PATH`"

The audit itself notes (§12) that `FilesystemEmbeddingCache`, pointed at a
shared mount, would be mechanically sufficient for embeddings. Not chosen
as the actual implementation for three reasons: (1) `TARGET_CACHE_PATH`'s
own default (`./target_cache`, a relative path — audit §1) actively
discourages accidental sharing, so conflating it with the shared-storage
config would be an easy footgun; (2) fail-fast behavior differs — a
dev-mode local cache should silently `mkdir` a missing directory, a
shared-storage client should refuse to start if its configured root is
unreachable (§11); (3) a distinct class name
(`SharedFilesystemEmbeddingCache` vs. `FilesystemEmbeddingCache`) makes
`worker/main.py`'s backend selection legible at the call site instead of
being an invisible property of *which path happened to be configured*.

---

## 4. Final architecture

```text
Host A                                    Host B
  |                                         |
  +-- TargetRegistry -----+                 +-- TargetRegistry -----+
  |     (own Python obj)  |                 |     (own Python obj)  |
  |                       |                 |                       |
  +-- RedisLock ----------+-- shared Redis --+-- RedisLock ----------+
  |     "who builds?"                              "who builds?"
  |
  +-- SharedFilesystemEmbeddingCache --+   +-- SharedFilesystemEmbeddingCache
  +-- SharedFilesystemSegmentEmbeddingCache |   (own client objects, host B)
  |     (own client objects, host A)   |
  |                                    |
  +-- SharedArtifactStore(shared_root) +---+---- SharedArtifactStore(shared_root)
                                            |
                                  SHARED MOUNT (NFS / cluster FS —
                                  operator-provisioned, see §13)
                                            |
                                  embedding vectors (pooled + segment)
                                  target media (content-addressed)
```

Redis and shared storage remain **separate concerns**, per the task
brief's own required distinction (§17): Redis answers "who is allowed to
build," shared storage answers "where is the completed artifact." Neither
call site was merged; `RedisLock` (`target/lock.py`) is byte-for-byte
unchanged.

---

## 5. Data flow

1. Caller resolves a target's segment embedding via
   `TargetRegistry.get_or_build_segment_embedding` (`target/registry.py`,
   **unchanged this phase**).
2. Cache check: `self._segment_cache.get(...)` — now
   `SharedFilesystemSegmentEmbeddingCache.get`, which reads
   `SharedArtifactStore.get_bytes(f"segments/{cache_entry_key}.segments.json")`.
   Hit -> return immediately, no lock touched (unchanged control flow).
3. Miss -> `RedisLock.acquire()` on `target_lock_key(cache_entry_key(...))`
   (unchanged).
4. Winner: double-checks the cache (unchanged), calls `build(record)`
   (the injected callback — `worker/matching_handler.py`'s closure,
   updated this phase to resolve target media through shared storage
   too — see §10), then `register_segment_embedding`, which calls
   `self._segment_cache.put(...)` -> `SharedArtifactStore.put_bytes(...)`
   -> tempfile + `os.replace` into the shared mount. Lock released in
   `finally` (unchanged).
5. Loser: polls `get_compatible_segment_embedding` (unchanged code path,
   now reading the shared store) until it observes the winner's write —
   **this is the fix**: the loser's read and the winner's write now
   target the same physical storage, not two different hosts' disks.

---

## 6. Cache identity

**Unchanged.** `target.versioning.cache_entry_key()` and `EmbeddingSpec`
(`target/versioning.py`) were not touched. `SharedFilesystemEmbeddingCache`/
`SharedFilesystemSegmentEmbeddingCache` derive their storage key from
`cache_entry_key(target_id, target_version, content_sha256, spec)` exactly
as the filesystem backends do — same SHA-256-derived key, same required-
field validation, same exact-match compatibility re-check
(`_validate` in `target/shared_cache.py` mirrors `_load_and_validate` in
`target/cache.py`/`target/segment_cache.py` field-for-field). No host-
specific component (hostname, PID, worker ID, timestamp) enters the key —
verified by inspection, matching the audit's own §3 finding.

---

## 7. Redis lock interaction

**Unchanged.** `RedisLock` (`target/lock.py`) was not modified. The
build-on-miss control flow in `TargetRegistry.get_or_build_segment_embedding`
(`target/registry.py`) was not modified. The only change in that file is
additive: an optional `media_store` constructor parameter (defaults to
`None`, matching the existing `segment_cache: Optional[...] = None`
pattern) used solely by `register_target` to publish media — the lock/
build-on-miss method itself is untouched.

---

## 8. Target-media accessibility

**Option 1 chosen** (task brief §4): target media is stored in the same
shared artifact storage the embedding caches use, content-addressed by
`content_sha256` — the same field `cache_entry_key()` already uses for
identity, via `target/shared_storage.py`'s `SharedTargetMediaStore`.

Rejected: a `MediaAcquirer`-style analog for targets (Option 3). The audit
(§3.5) confirmed `register_target()` has **zero production call sites** —
there is no existing operator tool or URL source to acquire target media
from in the first place, so building a downloader would mean inventing a
source this codebase gives no evidence of, which the task brief explicitly
forbids ("do not invent a target URL where none exists").

Wiring (both additive, both backward compatible via `= None` defaults):

- `TargetRegistry.register_target()` (`target/registry.py`): if a
  `media_store` was injected at construction, publishes the media bytes to
  shared storage, keyed by the same `content_sha256` just computed for the
  `TargetRecord`, right after hashing.
- `worker/matching_handler.py`'s `_target_artifact()`: if
  `record.media_path` doesn't exist locally and a `media_store` was
  passed, fetches a temp copy from shared storage before falling through
  to the existing `MediaArtifact` construction. `media_store=None` (no
  shared storage configured) leaves this function's behavior **exactly**
  as before Phase 13D — the caller falls through untouched and
  `embed_video_segments` raises `UnsupportedMediaError` on its own
  existence check, unchanged.

**Explicit remaining integration item**: `register_target()` itself is
still not called from any production code path (unchanged from the
audit's finding) — this phase makes the *mechanism* fleet-safe
(publish-on-register, fetch-on-miss), verified by a direct test
(`tests/test_shared_target_storage.py::
test_registry_register_target_publishes_media_when_media_store_configured`
and the independent-client fetch test), but does not itself wire a
registration entrypoint, because none exists to wire — that remains
future operator tooling's responsibility, as it was before this phase.

---

## 9. Failure semantics

`SharedArtifactStoreError` (`target/shared_storage.py`, subclasses
`OSError`) is raised — never silently swallowed into a `None`/miss — when
the shared store is unreachable:

- At construction (`SharedArtifactStore.__init__`): if the configured root
  can't be created/accessed. `worker/main.py`'s `build_registry()`/
  `build_media_store()` propagate this; `main()`'s existing broad
  `except Exception` (unchanged) logs `worker_fatal_error` and exits `1` —
  a worker that can't reach its shared storage refuses to start rather
  than silently falling back to a host-local cache and calling itself
  distributed (the exact failure mode the task brief's §11 forbids).
- At read/write time: `get_bytes`/`put_bytes`/`get_file`/`put_file` wrap
  the underlying `OSError` and re-raise as `SharedArtifactStoreError`.
  `worker/matching_handler.py`'s `_resolve_target_segments` now has an
  explicit `except SharedArtifactStoreError` clause mapping it to
  `TransientFailure` — retried through the **existing** job retry
  machinery (`work_queue`'s attempt/backoff logic, untouched), per the
  task brief's "no new retry system" constraint. This reuses
  `embedding/errors.py`'s established Transient/Permanent classification
  pattern rather than inventing a new one.

A **miss** (key absent) remains a clean `None`/`False`, distinguished in
code and by test (`test_shared_store_read_failure_is_distinguishable_from_a_miss`)
from an **unreachable store** (exception).

---

## 10. Atomicity guarantees

Identical mechanism to the pre-existing filesystem caches, applied to the
shared root: `SharedArtifactStore.put_bytes`/`put_file` write to a
`tempfile.mkstemp` in the *same directory* as the final path, then
`os.replace()` into place. A reader (`get_bytes`/`get_file`) either sees
nothing (file absent) or a complete entry — `os.replace` is atomic on a
POSIX filesystem; there is no code path that can expose a partial write.
Verified by `tests/test_shared_target_storage.py::
test_concurrent_reader_never_observes_a_partial_artifact` (4 concurrent
reader threads racing 20 writes of a 200-segment payload against 1
writer). Idempotency: writing identical bytes under the same key twice is
a safe no-op replace (verified implicitly by every cache-hit test —
repeated `put()` calls with the same deterministic payload never fail or
corrupt state).

**Carried-over caveat from the audit (§11, A1 sub-flavor), not
independently re-verified this phase**: `os.replace()`'s atomicity is a
POSIX-single-filesystem guarantee. Real NFS deployments have historically
had client-side attribute-caching windows in some configurations where a
rename on one client isn't instantly visible to another client's already-
open directory listing. This would be a **bounded** visibility delay, not
this system's current **permanent** invisibility — a real improvement
either way — but the exact bound depends on the operator's specific NFS
version/mount options, which this sandbox cannot test.
**REQUIRES MULTI-HOST VALIDATION.**

---

## 11. Configuration

One new environment variable, deliberately independent of `REDIS_URL` and
`TARGET_CACHE_PATH` (task brief §12: "independent of Redis job/coordination
configuration"):

| Variable | Default | Effect |
| --- | --- | --- |
| `SHARED_ARTIFACT_STORE_PATH` | unset | Unset: `build_registry()` constructs the original `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` pair rooted at `TARGET_CACHE_PATH` — single-host-only, exactly the pre-Phase-13D behavior, unchanged. Set to a directory: `build_registry()` constructs `SharedFilesystemEmbeddingCache`/`SharedFilesystemSegmentEmbeddingCache`/`SharedTargetMediaStore` rooted there instead — **this path MUST be a genuinely shared mount (NFS, cluster filesystem) across every worker host in the fleet.** This codebase cannot verify that at runtime; it can only verify the path is reachable at startup (§9). |

No credentials, bucket names, hostnames, or cloud endpoints were added —
none exist to configure for a filesystem-path backend. `config_snapshot()`
(`worker/main.py`) logs `shared_artifact_store_path` at startup alongside
the existing configuration fields, so an operator can see from the
existing structured logs which backend a given worker process is actually
running.

---

## 12. Tests

New file: `tests/test_shared_target_storage.py` (16 tests). Also extended:
`tests/test_worker_main.py` (+6 tests: config default/override for the new
field, `build_registry`/`build_media_store` backend selection, fail-fast
on an unreachable configured path).

| Requirement (task brief §6/§7) | Test |
| --- | --- |
| Two independent registries, separate local state, shared Redis + shared backend, concurrent same-target request -> exactly one build | `test_two_independent_registries_share_one_build_simulated_multi_host` |
| Second registry observes first registry's result without rebuilding (sequential) | `test_second_registry_pure_cache_hit_after_first_registry_builds` |
| A. Cache hit | `test_cache_hit_returns_existing_entry_without_rebuild` |
| B. Different target content | `test_different_content_hash_creates_a_distinct_artifact` |
| C. Model version invalidation | `test_model_version_change_is_a_cache_miss` |
| D. Preprocessing invalidation | `test_preprocessing_config_change_is_a_cache_miss` |
| E. Sampling invalidation | `test_sampling_config_change_is_a_cache_miss` |
| F. Crash during build | `test_crash_during_build_leaves_no_partial_artifact_and_releases_lock` |
| G. Two independent registries (structural) | `test_two_registries_have_independent_python_objects_but_shared_backend` |
| H. Concurrent same-target request | (same as the central acceptance test above) |
| K. Artifact atomicity | `test_concurrent_reader_never_observes_a_partial_artifact` |
| Target media, independent client fetch | `test_target_media_published_by_one_host_is_fetchable_by_an_independent_store_client` |
| Target media, clean miss | `test_target_media_never_published_is_a_clean_miss_not_an_error` |
| Target media, registry wiring | `test_registry_register_target_publishes_media_when_media_store_configured` |
| Failure semantics: unreachable store raises at construction | `test_unreachable_shared_store_root_raises_at_construction` |
| Failure semantics: read failure ≠ miss | `test_shared_store_read_failure_is_distinguishable_from_a_miss` |

I. Existing matching pipeline / J. existing E2E behavior: **not
duplicated** — `tests/test_matching_handler.py` and
`tests/test_integration_e2e.py` were not modified (they construct
`FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` directly,
unaffected by this phase's additive changes) and both re-run clean (§13).

### Results

```text
tests/test_shared_target_storage.py ................       16 passed
tests/test_worker_main.py           .............................  29 passed
tests/test_segment_cache.py         ..............               14 passed
tests/test_target.py                ...............               15 passed
tests/test_target_build_on_miss.py  .......                        7 passed
tests/test_target_lock.py           .......                        7 passed
tests/test_matching_handler.py      ....                           4 passed
tests/test_integration_e2e.py       .......                        7 passed
```

Full suite: `269` collected, `268 passed, 1 failed`. The one failure,
`tests/test_worker_observability.py::
test_health_summary_not_emitted_before_interval_elapses`, is **pre-
existing** — reproduced identically on `c3bfdd5` (the commit this phase
started from, before any Phase 13D change) via `git stash`; unrelated to
target caching, not touched by this phase. No skipped tests, no
environment-dependent tests beyond the pre-existing real-Redis dependency
every test file in this suite already has (`tests/conftest.py`, db 15).

---

## 13. Benchmarks

`benchmarks/` was not modified except to keep two pre-existing scripts
(`bench_pipeline.py`, `bench_integration_overhead.py`,
`instrumented_handler.py`) compiling against `_target_artifact`'s new
return shape (see §14) — no new benchmark harness was added to that
directory, per the task brief's "do not run huge benchmark matrices"
constraint. A focused, synthetic (no DINOv2/torch) comparison script was
run once from the scratch directory:

```text
cache-hit latency (local filesystem, n=50):  mean=1.932ms  median=1.572ms
cache-hit latency (shared storage,   n=50):  mean=1.973ms  median=1.716ms

cache-write latency (local filesystem, n=50): mean=12.042ms  median=11.632ms
cache-write latency (shared storage,   n=50): mean=5.554ms   median=2.838ms

cold build, register+build+register (local filesystem):  911.2ms
cold build, register+build+register (shared storage):    903.6ms

concurrent two-registry same-target miss (SIMULATED MULTI-HOST):
  total wall time: 914.7ms, builds executed: 1, results obtained: 2
  (loser overhead beyond winner's build time: ~14.7ms)
```

**MEASURED** (this session, synthetic 30-segment/768-dim payloads, a
0.9s sleep standing in for Phase 11's measured real DINOv2 build time of
0.875-1.418s — see audit §10):

- Cache-hit latency is statistically indistinguishable between the old and
  new backend (~2ms either way) — expected, since both are local-disk
  reads in this single-machine test; the shared backend's actual
  production cost is real network/NFS latency, which this sandbox cannot
  produce (**REQUIRES MULTI-HOST VALIDATION**).
- Cache-write latency for the shared backend was *lower* in this run
  (5.5ms mean vs. 12.0ms local) — not a meaningful architectural claim,
  most likely filesystem-cache warmth/noise between two back-to-back
  temporary-directory runs on the same disk; reported as measured, not
  interpreted as "faster."
- The concurrent two-registry miss — the scenario that, under the **old**
  architecture, costs a 10-minute stall plus a full duplicate ~0.9-1.4s
  build (audit §5, §10) — now costs **one build's wall time plus ~15ms**:
  the loser observes the winner's completed shared artifact almost
  immediately instead of ever duplicating the work. This is the single
  number this phase exists to produce.

**INFERRED, not measured**: real NFS/cluster-filesystem read/write latency
at production request rates, real network transfer cost, and behavior
under actual cross-machine clock/timing conditions — all **REQUIRE
MULTI-HOST VALIDATION**, consistent with the audit's own §10/§14 caveats,
unchanged by this phase because no real second host was available to test
against.

---

## 14. Production changes (exact diff surface)

**New files:**

- `target/shared_storage.py` — `SharedArtifactStore`, `SharedArtifactStoreError`, `SharedTargetMediaStore`.
- `target/shared_cache.py` — `SharedFilesystemEmbeddingCache`, `SharedFilesystemSegmentEmbeddingCache`.

**Modified, additive-only (all new parameters default to the pre-existing
behavior):**

- `target/registry.py` — `TargetRegistry.__init__` gains `media_store: Optional[SharedTargetMediaStore] = None`; `register_target()` publishes media when one is configured.
- `worker/matching_handler.py` — `_target_artifact()` now returns `(MediaArtifact, is_temp: bool)` instead of `MediaArtifact` (see below), accepts an optional `media_store`; `build_matching_handler()` gains `media_store: Optional[SharedTargetMediaStore] = None`; `_resolve_target_segments()` threads it through and gains an `except SharedArtifactStoreError` clause.
- `worker/main.py` — new `shared_artifact_store_path` config field (+ `SHARED_ARTIFACT_STORE_PATH` env var, + `config_snapshot()` field); new `build_media_store()`; `build_registry()` branches on the new config field; `main()` constructs and threads the media store through to `build_matching_handler()`.

**Fixed as a consequence of `_target_artifact`'s signature change** (it now
returns a tuple so callers can know whether to clean up a fetched temp
file — a persistent `record.media_path` file must never be deleted, unlike
a temp copy fetched from shared storage): `benchmarks/bench_pipeline.py`,
`benchmarks/bench_integration_overhead.py`,
`benchmarks/instrumented_handler.py` each had one call site unpacking the
new tuple. No behavioral change to any of those scripts otherwise.

**Not touched:** `target/cache.py`, `target/segment_cache.py`,
`target/lock.py`, `target/versioning.py`, `target/identity.py`,
`target/keys.py` — every file the task brief named as non-negotiable to
preserve. `tests/test_segment_cache.py`, `tests/test_target.py`,
`tests/test_target_build_on_miss.py`, `tests/test_target_lock.py`,
`tests/test_matching_handler.py`, `tests/test_integration_e2e.py` — none
modified, all still pass unmodified (§12).

---

## 15. Limitations

- **Real multi-host visibility/latency is unvalidated** — everything in
  §12/§13 runs in one process against one physical filesystem. The
  "simulated multi-host" pattern (independent `SharedArtifactStore`/
  `TargetRegistry` Python objects, same underlying path) proves the
  structurally relevant property (independent local state + shared
  coordination + shared storage behaves correctly) but cannot exercise
  real network partitions, real NFS consistency semantics, or clock skew.
  **REQUIRES MULTI-HOST VALIDATION.**
- **No shared-storage infrastructure is provisioned by this phase** —
  `SHARED_ARTIFACT_STORE_PATH` must point at a real shared mount an
  operator sets up independently; this codebase cannot create, verify, or
  enforce that the mount is genuinely shared (only that it's reachable at
  startup — §9/§11).
- **Target registration remains unwired in production** (carried over from
  the audit, §3.5/§8 above) — `register_target()` still has no production
  caller. This phase makes the media-publication *mechanism* correct and
  tested, not a registration pipeline.
- **No local read-through cache was added**, deliberately, per the audit's
  own recommendation (§12) and the task brief's over-engineering
  constraint (§9) — every cache read goes to the shared store directly.
  The audit's own numbers suggest this is likely negligible against the
  ~900ms dominant embedding-build cost, but that inference, too,
  **REQUIRES MULTI-HOST VALIDATION** against real network latency.
- **Lock TTL/heartbeat behavior is unchanged** and out of this phase's
  scope, per the task brief (§9's explicit "do NOT implement... lock
  heartbeat redesign").

---

## 16. Multi-host validation status

**SIMULATED MULTI-HOST.**

Every test and benchmark in this document ran in one process against one
Redis instance (`tests/conftest.py`, db 15 for tests / db 14 for the
benchmark script) and one physical filesystem, using independently
constructed `SharedArtifactStore`/`TargetRegistry` objects to model
"separate hosts." No second physical machine, container with a genuinely
separate filesystem, or real network-attached shared storage was used.
**REAL MULTI-HOST VALIDATED: no.**

---

## 17. Future work

- Provision and validate against a real NFS/cluster-filesystem mount (or,
  if infrastructure evidence later justifies it, implement a
  `SharedArtifactStore` variant for a real object-storage backend behind
  the same three-method interface) — genuinely closes the "REQUIRES
  MULTI-HOST VALIDATION" items above.
- A target-registration entrypoint/tool (out of this phase's scope,
  `register_target()` remains uncalled in production, unchanged from the
  audit).
- If a future phase measures shared-storage read latency as a real
  bottleneck under production request rates, revisit the deliberately-
  deferred local read-through cache — not before it's measured as
  necessary.
