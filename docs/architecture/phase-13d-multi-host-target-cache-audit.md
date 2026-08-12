# Phase 13D — Multi-Host Target Cache Audit

## Status: AUDIT ONLY

No production code, tests, benchmarks, or Redis state were modified during
this task. The only file created is this document. **Git revision
audited:** `c3bfdd5` ("phase13c fix"), working tree clean at audit start
and end.

## 0. Scope

This audit answers one question: **can two or more real production
workers on different machines safely share target embeddings without
duplicated expensive computation, stale/inconsistent cache state, or
filesystem assumptions that only work on a single host?**

It supersedes nothing — it sharpens and extends Phase 13's own §3
("Target-cache audit (multi-host)"), which already identified this as
**blocker #2**, by tracing the implementation in full detail against this
task's specific checklist (identity, locking, atomicity, deployment
assumptions, test coverage, performance, candidate architectures) and by
surfacing one adjacent, previously-undocumented finding (§3.5 below:
target media itself, not just its embedding, is host-local).

**Method:** direct inspection of `target/cache.py`, `target/segment_cache.py`,
`target/registry.py`, `target/lock.py`, `target/keys.py`,
`target/versioning.py`, `target/identity.py`, `worker/matching_handler.py`,
`worker/main.py`, `tests/test_target_lock.py`,
`tests/test_target_build_on_miss.py`, `tests/test_segment_cache.py`,
`tests/test_target.py`, `tests/test_integration_e2e.py`,
`tests/test_matching_handler.py`, `benchmarks/bench_pipeline.py`,
`docs/architecture/phase-13-production-hardening.md`, and
`docs/architecture/history/phase-06-target-management-cache.md`. No new
code was written, no benchmark was run, no repository-wide re-audit was
performed — this reuses Phase 13's own findings as a baseline and goes
one layer deeper only where this task's checklist required it.

**Labels used throughout**, matching Phase 13's own vocabulary:
**VERIFIED** (read directly in the source this session), **MEASURED**
(a number actually recorded by Phase 11/13C, cited not re-run),
**INFERRED** (a logical consequence of the verified code, not itself
executed), **UNAVAILABLE** (cannot be determined by static inspection),
**REQUIRES MULTI-HOST VALIDATION** (needs a real multi-machine
environment this sandbox does not have).

---

## 1. Current target-cache architecture

```
worker/main.py: build_registry(redis_client, config)
  |
  v
TargetRegistry(redis_client, pooled_cache, segment_cache)
  |
  +--> Redis (fingerprint:target:* namespace)
  |      - target_key                       -> TargetRecord (Hash)
  |      - target_content_index_key         -> Set of "id\x1fversion"
  |      - target_embeddings_key            -> Hash: spec_key -> metadata JSON (no vector)
  |      - target_segment_embeddings_key    -> Hash: spec_key -> metadata JSON (no vector)
  |      - target_lock_key                  -> RedisLock (SET NX PX + Lua CAS release)
  |
  +--> FilesystemEmbeddingCache(base/"pooled")           [local disk, this host only]
  |      one JSON file per (target_id, target_version, content_sha256, spec)
  |
  +--> FilesystemSegmentEmbeddingCache(base/"segments")  [local disk, this host only]
  |      one JSON file per (target_id, target_version, content_sha256, spec),
  |      holding the full segment list + coarse vector — the representation
  |      the production matching handler actually uses
  |
  +--> DINOv2EmbeddingEngine.embed_video_segments(_target_artifact(record))
         reads record.media_path directly off local disk (see §3.5)
```

`worker/matching_handler.py:157` (`_resolve_target_segments`) is the one
production call site of `TargetRegistry.get_or_build_segment_embedding`
(`target/registry.py:188-259`) — the pooled `TargetEmbeddingCache`
(`target/cache.py`) exists and is fully tested but has **no production
call site**; the temporal-matching pipeline (Phase 9/10, what
`worker/main.py` actually wires up) uses the segment cache exclusively.
This audit therefore treats `get_or_build_segment_embedding` /
`FilesystemSegmentEmbeddingCache` as the architecture that matters in
production, while noting the pooled cache shares the identical design
(same file shape, same missing cross-host visibility) and would need the
identical fix if it were ever wired in.

`worker/main.py:269-273` (`build_registry`) is the **only** production
call site that constructs a `TargetRegistry` — confirmed by grep, mirroring
Phase 13B's own confirmation that `Redis(...)` has exactly one call site.
`TARGET_CACHE_PATH` (default `./target_cache`, a relative path — VERIFIED,
`worker/main.py:65`) is split into `<path>/pooled` and `<path>/segments`.

---

## 2. Storage / ownership map

| State item | Lives in | Shared same-host (2 processes) | Shared cross-host | Authoritative | Can go stale | Two workers create simultaneously? |
|---|---|---|---|---|---|---|
| `TargetRecord` (identity, `media_path`, `content_sha256`, timestamps) | Redis Hash (`target_key`) | Yes | Yes | Yes | No (re-registration overwrites; `created_at` preserved) | Yes — `HSET` is last-writer-wins, no lock; benign (Phase 6 doc §"Limitations" already accepts this for identical bytes) |
| Content-hash index (`target_content_index_key`) | Redis Set | Yes | Yes | Yes | No | Yes — `SADD` is idempotent, no lock needed |
| Embedding metadata summary (`target_embeddings_key`, `target_segment_embeddings_key`) | Redis Hash | Yes | Yes | **No — write-only.** Nothing in `TargetRegistry` ever reads it back for a hit/miss decision (VERIFIED, `target/registry.py:150-161` has no reference to `target_segment_embeddings_key`) | N/A (never consulted, so "staleness" is undefined for it) | Yes — `HSET`, no lock |
| Build lock (`target_lock_key`) | Redis key, `SET NX PX` | Yes | Yes | Yes, for "who builds" only | Can expire mid-build (§6) | No — that is the lock's entire purpose |
| **Embedding vector data (pooled)** | Local filesystem, one JSON file/entry | Yes (same disk) | **No** | **Yes, but only for whichever host wrote it** | N/A per-host; a host that never sees the write has no concept of "stale," it has a permanent miss | Yes on different hosts — last-writer-wins **per host**, i.e. never conflicts because they never see each other |
| **Segment embedding vector data** | Local filesystem, one JSON file/entry | Yes (same disk) | **No** | Same as above | Same as above | Same as above |
| **Raw target media bytes** (`record.media_path`) | Local filesystem, path recorded in Redis as a plain string | Yes (same disk) | **No — see §3.5** | Whichever host the path was written on | N/A | N/A — not a cache, a precondition |
| `DINOv2EmbeddingEngine` (loaded model weights) | Process memory | No | No | N/A (deterministic given model_id/version) | N/A | N/A |

The single load-bearing fact of this table: **the only two things Redis
holds about an embedding are its lock and a write-only metadata summary —
never the vector bytes.** This matches Phase 6's original storage-boundary
design (Redis = coordination/small-metadata, filesystem = vector bytes)
and was explicitly correct for the single-host deployment Phases 1-12
validated. It is also exactly the fact that makes it multi-host-unsafe,
because "coordination is Redis-backed" and "cache storage is Redis-backed"
were conflated in Phase 11/12's earlier framing and are not the same
property (§4 below formalizes this).

---

## 3. Cache identity correctness

`target.versioning.cache_entry_key(target_id, target_version, content_sha256, spec)`
(VERIFIED, `target/versioning.py:75-88`) — `sha256(json.dumps({target_id,
target_version, content_sha256, spec_key}, sort_keys=True))`.

- **Deterministic**: yes — pure function of its four inputs, `sort_keys=True`
  removes dict-order sensitivity.
- **Stable across hosts/processes**: yes — no host-specific input (no
  hostname, PID, timestamp, or filesystem path) feeds the key. Two hosts
  computing the key for the same logical target always agree.
- **Includes target media identity**: yes, via `content_sha256`
  (`sha256_file`, streamed off file bytes only — VERIFIED,
  `target/identity.py:29-37` — never touches filename/mtime/size).
- **Includes embedding-model identity/version**: yes, via
  `EmbeddingSpec.spec_key()` (`model_id`, `model_version`).
- **Includes preprocessing/segment configuration**: yes,
  `preprocessing_config` and `sampling_config` are both part of
  `spec_key()` and independently re-checked field-by-field on every read
  (`_load_and_validate`, `target/cache.py:161-177` and
  `target/segment_cache.py:191-208`).
- **Two incompatible embeddings colliding**: not possible via the key
  alone (SHA-256 over the full compatibility tuple), **and** even a
  hypothetical hash collision would still be caught, because
  `_load_and_validate` re-verifies every compatibility field against the
  file's own stored values before returning a hit — a filename collision
  cannot silently masquerade as a match.
- **Model/config change silently reusing an old embedding**: not possible
  — a changed `model_version`/`embedding_schema_version`/
  `preprocessing_config`/`sampling_config` changes `spec_key()`, which
  changes the filename, which is simply a fresh cache miss (this is
  exactly Phase 6's designed invalidation mechanism, unchanged).

**Conclusion: cache identity is portable and correct across hosts as
designed.** This is not part of the blocker. The blocker is purely about
*where the bytes named by this key are stored*, not *what the key means*.
**VERIFIED.**

---

## 3.5. Adjacent finding: raw target media is also host-local (not previously documented)

Not identified as a distinct issue in Phase 13's own §3 (which addressed
only the *embedding* cache). Tracing the build path fully:

`_resolve_target_segments`'s `build(record)` closure
(`worker/matching_handler.py:187-189`) calls
`engine.embed_video_segments(_target_artifact(record))`.
`_target_artifact` (`worker/matching_handler.py:65-82`) wraps
`record.media_path` **directly as a local path** — `Path(record.media_path)`,
no acquisition/download step, no `MediaAcquirer` involvement (that class
is for candidates only). `embed_video_segments`
(`embedding/dinov2_engine.py:244-245`) checks
`artifact.local_path.exists()` and raises `UnsupportedMediaError` — mapped
to **`PermanentFailure`**, not retryable — if the file is absent (VERIFIED
by direct read this session).

Confirmed by grep (`register_target` call sites, all of `tests/` and
`benchmarks/`): **there is no production call site for
`TargetRegistry.register_target()` anywhere in this repository.** Target
registration is an entirely out-of-band, unwired operation — some future
operator tool or script is expected to call it, pointing `media_path` at
wherever the raw target file happens to sit *on whichever single host that
tool ran on*.

**Consequence:** even a fully correct, shared embedding-cache backend
(§11's candidate architectures) does not by itself make the system
multi-host-safe, because the *build* step for a cache miss on a losing
host still requires that host to have local read access to
`record.media_path`. If that path is a plain local filesystem path only
populated on the host that ran the (currently nonexistent) registration
tool, any other host attempting a build — whether because it is
genuinely the first to see the target, or because it is duplicating a
stale build under the current architecture's failure mode (§5) — fails
**permanently**, not merely redundantly. This is a sharper failure mode
than "wasted duplicate compute": it is "hard failure with no retry path,"
and it exists independently of whatever fix §12 recommends for the
embedding vectors themselves.

**Classification: a second, currently-undocumented multi-host blocker,
adjacent to but distinct from blocker #2 as Phase 13 scoped it.** Not
scored as part of "blocker #2 resolved/not resolved" below, since the task
brief scoped this audit to the *embedding/cache* architecture specifically
— but any Phase 13D implementation plan that fixes only the embedding
cache and declares multi-host solved would be incomplete. Recorded here so
it is not silently missed. **VERIFIED** by code inspection; not previously
named in `phase-13-production-hardening.md` or `phase-11-performance-benchmarks.md`.

---

## 4. Redis coordination vs. cache storage — explicit distinction

| | Redis-backed? | What it actually holds |
|---|---|---|
| Distributed **coordination** (who builds) | **Yes** | `target_lock_key` — a token, nothing else. Fleet-wide correct as a primitive (§6). |
| Distributed **cache storage** (the vector bytes) | **No** | Filesystem only, per-host. The `target_embeddings_key`/`target_segment_embeddings_key` hashes are metadata *about* what's cached (spec fields, segment count, `cached_at`) — never the vector, and never read back by any code path that decides hit/miss. |

**This is the audit's central finding, restated precisely**: the lock
genuinely is a fleet-wide primitive — `SET key token NX PX ttl` and a Lua
compare-and-delete are ordinary Redis commands with no locality assumption
(`target/lock.py`'s own docstring already states this design intent, and
it is accurate). But the lock protects access to a resource
(`self._segment_cache`, an injected `FilesystemSegmentEmbeddingCache`)
that is *not* the shared resource the lock's cross-host semantics assume
it is. A Redis lock does not make what it guards distributed merely by
being itself distributed — **VERIFIED**, this is not an inference, it is
a direct reading of `get_or_build_segment_embedding`'s three call sites
into `self._segment_cache` (`target/registry.py:150-161` for the
loser-poll path, `:228` for the pre-lock check, `:241` for the
post-lock-win double-check) — all three call the same method on the same
injected, host-local object.

---

## 5. Multi-host failure-mode trace

Scenario: Host A / Worker A and Host B / Worker B each receive a job
referring to target T for the first time; neither has a local cache entry.

1. **A requests target T.** Cache check (`get_compatible_segment_embedding`)
   misses locally. A calls `RedisLock.acquire()` on
   `target_lock_key(cache_entry_key(T, ...))` and wins (first to the Redis
   key). **VERIFIED.**
2. **B requests target T**, at approximately the same time or any time
   before A's build finishes. B's local cache check also misses (B's disk
   never had this entry). B calls `RedisLock.acquire()` on the *same* key
   and **loses** — `SET NX` is a single global Redis operation; there is
   exactly one winner regardless of which host called it. **VERIFIED.**
3. **Does both download/read the target?** Only A does, at this point —
   B is now in the polling branch, not the build branch. **VERIFIED.**
4. **Does both compute DINOv2 embeddings?** Not yet — only A, right now.
   (See step 7 for what happens after B's poll times out.)
5. **Does Redis coordinate them?** Yes, correctly, for the *lock*
   acquisition itself — exactly one of A/B is told "you won." **VERIFIED.**
6. **Does the existing build lock work across hosts?** Yes, as a raw
   mutual-exclusion primitive — Redis does not distinguish "two threads in
   one process," "two processes on one host," or "two hosts" as callers of
   `SET NX`; correctness of *who wins* is identical in all three cases.
   **VERIFIED** for the primitive itself; **REQUIRES MULTI-HOST
   VALIDATION** only for cross-machine *timing* (network RTT to acquire),
   not for correctness.
7. **Where is the resulting embedding stored?** On A's local disk only —
   `self._segment_cache.put(...)` inside `register_segment_embedding`
   writes to whichever `FilesystemSegmentEmbeddingCache` instance A's
   process holds, which is rooted at A's `TARGET_CACHE_PATH`. **VERIFIED.**
8. **Can B see A's result?** **No.** B's poll loop
   (`get_or_build_segment_embedding`'s loser branch,
   `target/registry.py:249-259`) calls
   `get_compatible_segment_embedding` every `poll_interval_s`, which reads
   `self._segment_cache.get(...)` against **B's own** cache directory — a
   path on B's disk that A never wrote to. This is not "eventually
   consistent," it is **structurally incapable of becoming consistent** —
   there is no code path, at any polling interval or timeout, that would
   let B observe A's filesystem write. **VERIFIED**, this is the exact
   mechanism Phase 13 §3 already identified; this audit confirms it by
   independently re-tracing the same three call sites.
9. **Can A see B's result?** Symmetric — not applicable in this trace
   since B never builds in this window, but the same structural fact holds
   in reverse for any later target where B wins instead.
10. **What happens when B's poll times out?** After `poll_timeout_s`
    (default `DEFAULT_POLL_TIMEOUT_S = 600.0`, i.e. 10 minutes —
    `target/registry.py:57`, and this is the value actually in effect
    since `_resolve_target_segments` calls with every default) B's
    `get_or_build_segment_embedding` raises `TimeoutError`, mapped by
    `_resolve_target_segments`'s `except TimeoutError` to
    `TransientFailure` (`worker/matching_handler.py:195-196`) — a
    scheduled retry via the existing retry/backoff machinery (Phase 3),
    consuming one of `max_attempts` (default 3, per-job). **VERIFIED.**
11. **On retry**, B's job is re-claimed (by B or any other worker) and
    `get_or_build_segment_embedding` runs again. By now A's `RedisLock`
    has long since released (build took ~0.9-1.4s per Phase 11 §15/§17,
    nowhere near the 10-minute TTL) or would still be held only if A's
    build were pathologically slow. Assuming the common case (A finished
    minutes ago), the retrying worker's own lock-acquire attempt now wins
    — **and duplicates A's build from scratch**, because it still cannot
    see A's filesystem result. **INFERRED** directly from the verified
    control flow above (not independently re-run this session — Phase 11
    §16 measured the underlying lock-contention mechanics at n=4 on one
    host, which this trace extends by reasoning, not new execution).
12. **What happens if A crashes while holding the coordination lock?**
    The lock has no auto-renewal (`target/lock.py`'s own docstring states
    this as a known, documented limitation). A crash mid-build leaves the
    lock to expire naturally at its TTL (default 600,000ms); until then,
    every other host polling for T is blocked waiting either for the
    (never-coming) filesystem write or the TTL expiry, whichever a fresh
    lock-acquire attempt would encounter. No fencing token or heartbeat
    exists to detect the crash early — this is unchanged, pre-existing
    behavior, not new to the multi-host case, and was already flagged as
    "not solved, only documented" in `target/lock.py`'s own module
    docstring. **VERIFIED** (code has no renewal/heartbeat mechanism).
13. **What happens if the local filesystem cache exists on A but not B?**
    This is the steady-state condition, not an edge case — by
    construction, *every* target's cache exists only on whichever host(s)
    happened to build it, forever, for the life of the deployment (no
    background replication process exists). **VERIFIED.**

**Net effect across a fleet, restated from Phase 13 §3 and confirmed by
this independent trace**: not "N hosts each independently and immediately
build their own copy" (Phase 12's original, milder framing) but "the first
host builds immediately; every other host's first job against that target
stalls up to 10 minutes, burns a retry attempt, and *then* duplicates the
build" — worse for both latency and correctness-adjacent availability than
either "no lock at all" or "a genuinely shared cache."

---

## 6. Concurrency / lock semantics

`target.lock.RedisLock` (VERIFIED, `target/lock.py`):

- **Lock key**: `fingerprint:lock:target:{cache_entry_key(...)}` — scoped
  to one exact `(target_id, target_version, content_sha256, spec)`
  representation, not the whole target (`target/keys.py:29-35`).
- **Acquisition command**: `SET key token NX PX ttl_ms` (`lock.py:57`) —
  single atomic Redis command, race-free by construction.
- **Ownership mechanism**: a random `uuid4().hex` token generated per
  `acquire()` call, compared on release.
- **TTL**: caller-supplied, default `DEFAULT_LOCK_TTL_MS = 600_000` (10
  minutes) from `target/registry.py:55`.
- **Release mechanism**: Lua compare-and-delete (`_RELEASE_IF_OWNER`,
  `lock.py:29-34`) — `GET` then conditional `DEL`, atomic as one script
  execution. Cannot delete a key it does not currently own (proven by
  `tests/test_target_lock.py::test_release_does_not_remove_a_lock_it_no_longer_owns`).
- **Behavior on timeout (loser side)**: polls, then raises `TimeoutError`
  after `poll_timeout_s` — never blocks indefinitely (§5, step 10).
- **Behavior if owner crashes**: lock sits until TTL expiry; no
  heartbeat/renewal exists (§5, step 12) — a **known, documented,
  unchanged limitation**, not something this audit newly discovered.
- **Behavior if the lock expires while computation continues**: a second
  worker can acquire the now-free key and start a **second, concurrent**
  build while the first is still running. Neither `RedisLock` nor
  `TargetRegistry` detects this — the original holder's eventual
  `release()` call is a no-op by then (token mismatch, VERIFIED via the
  same CAS test above), so it silently fails to release what it no longer
  owns rather than corrupting the second holder's lock. **Two builders can
  run concurrently in this specific race** — bounded by requiring a build
  to run longer than a 10-minute TTL, which Phase 11 measured real builds
  at ~0.9-1.4s (§10 below), i.e. a ~430-670x margin under measured
  conditions. **INFERRED** to still hold; **REQUIRES MULTI-HOST
  VALIDATION** for pathological real-world build times (a much longer
  target video, a much slower/loaded host) that this sandbox cannot
  produce.
- **Can two workers enter the build section simultaneously under normal
  (non-crash, non-TTL-expiry) conditions?** No — `SET NX` guarantees
  exactly one winner. **VERIFIED**, and independently proven by
  `tests/test_target_build_on_miss.py::test_concurrent_miss_builds_only_once`
  (see §9 for this test's actual scope/limits).

**Explicit distinction the task brief requires:**
- Lock prevents duplicate computation: **yes, within the lock's live
  window**, and only for hosts that actually reach the point of losing the
  *acquire* race while the winner is still building — a host that arrives
  *after* the winner's build finished and released does not "lose" the
  lock, it wins a *fresh* acquire and duplicates the work anyway, because
  nothing before the acquire ever checked "did someone already finish
  this, elsewhere" in a way that could see a cross-host result (§5).
- Lock prevents duplicate cache writes: **only incidentally** — it happens
  to serialize writes on a single host because it serializes the calls
  that lead to writes, but it provides no protection whatsoever against
  two *different* hosts each independently writing their own
  "first-and-only" copy, because from each host's own perspective it
  genuinely was the first and only writer (it never saw the other).
- Lock provides fencing: **no** — no fencing token is checked by the
  storage write itself (`FilesystemSegmentEmbeddingCache.put()` accepts
  any caller unconditionally); the lock's only enforcement is at
  *acquisition* time, not at *write* time.

---

## 7. Cache corruption / atomicity

`FilesystemSegmentEmbeddingCache._atomic_write` /
`FilesystemEmbeddingCache._atomic_write` (VERIFIED, both identical in
shape — `target/cache.py:138-147`, `target/segment_cache.py:168-177`):
`tempfile.mkstemp(dir=path.parent, ...)` + write + `os.replace(tmp_path, path)`,
with the temp file unlinked on any exception. This is **temp-file +
atomic rename**, not direct-to-final-path and not append-based.

- **Worker A crashes halfway through writing**: the partial data lives
  only in the `.tmp-*.json` file, never at the final path — `os.replace`
  never ran, so the final path either doesn't exist yet (first write) or
  still holds its previous valid contents (overwrite case, not applicable
  here since each key is written once). A reader can never observe a
  torn/partial file at the real path. **VERIFIED**, and this is a local
  (single-filesystem) guarantee `os.replace` provides on POSIX systems.
- **Worker B reads the same cache file** (same-host case, e.g. two
  processes sharing one `TARGET_CACHE_PATH`): B either sees nothing (file
  absent) or a fully-formed valid entry — never a half-written one.
  **VERIFIED**, this is exactly what atomic rename is for, and Phase 6's
  original docstring already claims this correctly.
- **Host A loses power mid-write**: same guarantee, assuming the
  underlying filesystem honors rename atomicity across a crash (true for
  ext4/xfs with default mount options; **UNAVAILABLE** to verify for any
  filesystem this sandbox doesn't run — see §8's NFS caveat, which is a
  distinct, cross-host concern, not a same-host crash-atomicity concern).
- **Two processes write the same target simultaneously (same host)**:
  last-writer-wins, no torn file (both `os.replace` calls are independently
  atomic; whichever lands last determines the final content) — this is the
  exact, already-documented Phase 6 limitation ("No concurrent-write
  protection beyond atomic rename... acceptable since embeddings for
  identical inputs should be reproducible/interchangeable").
- **Can a partially-written embedding ever be consumed?** No, on a single
  POSIX filesystem, by construction of the write path. **VERIFIED for the
  local case.** This part of the architecture is **not** the multi-host
  blocker — atomicity is solid; *visibility* across hosts is the actual
  gap (§4/§5).

---

## 8. Multi-host deployment assumptions

Every assumption below is implicit in `TARGET_CACHE_PATH` meaning "a local
directory on whichever host reads this env var":

| Assumption | Holds for single host? | Holds for multi-process, same host? | Holds across hosts? |
|---|---|---|---|
| Same filesystem | trivially yes | yes (same mount) | **No, unless deliberately shared** |
| Same mount | trivially yes | yes | **No by default** |
| Same path string means the same bytes | yes | yes | **No** — `TARGET_CACHE_PATH=/data/target_cache` is just a string; nothing checks or enforces that two hosts' `/data/target_cache` refer to the same storage |
| Same cache directory contents | yes | yes | **No** — each host accumulates only what it personally built |
| Same model files (for `DINOv2EmbeddingEngine`) | yes (out of this audit's scope — `EMBEDDING_DEVICE`/model loading is unchanged Phase 7/11 territory) | yes | **REQUIRES MULTI-HOST VALIDATION** — not audited here beyond noting `model_id`/`model_version` are part of the cache key, so a mismatched model would at least manifest as cache misses, not silent wrong-vector reuse |
| Same OS path semantics | yes (this project targets Linux only, per Phase 13C's own stated deployment target) | yes | Would matter only in a hypothetical mixed-OS fleet, which nothing in this codebase's configuration or docs suggests is a real deployment shape — **not a practical concern**, noted only for completeness |
| Same filesystem permissions | yes | yes, if same user/group | **REQUIRES MULTI-HOST VALIDATION** — depends entirely on how a future shared-storage mount is provisioned, not decidable from this codebase |

**Explicit statement, per the audit brief's direct question**: `TARGET_CACHE_PATH=/some/path`
means **the same physical storage** only if an operator has independently
arranged a shared mount (NFS, a cluster filesystem, a FUSE-mounted object
store, etc.) at that exact path on every host — **nothing in this
codebase arranges, verifies, or even documents that requirement today.**
As of this audit, every real deployment of `worker/main.py` (§19 of
`phase-13-production-hardening.md`, VERIFIED unchanged this session) uses
each host's own local disk by default (`DEFAULT_TARGET_CACHE_PATH =
"./target_cache"`, a relative path resolved against each process's own
CWD — actively *discourages* accidental sharing, since two hosts' `./`
are never the same directory even by coincidence).

- **What works on one host**: everything — identity, locking, atomicity,
  hit/miss correctness, invalidation. Confirmed correct and tested
  throughout Phases 1-13C.
- **What works with multiple processes on one host**: everything above,
  plus genuine build-once-per-target coordination (the lock's cross-process
  guarantee is real at this scope) — **provided** those processes are
  configured with the same `TARGET_CACHE_PATH` (true by default if started
  by the same supervisor/config, per Phase 13B's entrypoint design).
- **What works across multiple hosts today, unmodified**: only the
  **lock** (mutual exclusion for "who builds") and the **Redis metadata**
  (informational, never consulted for hit/miss). The actual cached vectors
  and the raw target media (§3.5) do **not** work across hosts without an
  operator-provisioned shared mount this codebase neither requires nor
  configures.

---

## 9. Existing test coverage

| Test file | What it covers | Classification |
|---|---|---|
| `tests/test_target_lock.py` (7 tests) | `RedisLock` acquire/release/TTL/CAS-release semantics, one shared `redis_client` fixture, sequential (not concurrent) calls except implicitly via the TTL-expiry test | **single-process** |
| `tests/test_target_build_on_miss.py::test_concurrent_miss_builds_only_once` | Two `threading.Thread`s calling `get_or_build_segment_embedding` | **single-process, multi-thread, one shared `TargetRegistry` instance and one shared filesystem cache directory** (`registry` fixture, `tmp_path`-scoped, constructed once and passed to both threads) — **not** even "two independent same-host processes/registries," let alone multi-host |
| `tests/test_target_build_on_miss.py` (remaining 6 tests) | Cache-hit-skips-build, build-once-and-registers, unknown-target KeyError, missing-segment-cache RuntimeError, build-exception-releases-lock, lock-wait-timeout | **single-process**, one shared registry/cache throughout |
| `tests/test_segment_cache.py` | `FilesystemSegmentEmbeddingCache` contract (get/put/exists, corruption handling, compatibility mismatches) | **single-process** |
| `tests/test_target.py` (15 tests, incl. `test_repeated_lookup_does_not_recompute_or_store_unnecessarily`) | `TargetRegistry`/`FilesystemEmbeddingCache` identity, versioning, pooled-cache hit/miss | **single-process** |
| `tests/test_matching_handler.py`, `tests/test_integration_e2e.py` | Full handler pipeline including target resolution, always via one `TargetRegistry` built from one `tmp_path`-scoped pair of cache directories | **single-process** |
| `benchmarks/bench_pipeline.py` Workload C (`run_contention_workload`) | **Real separate OS processes** (VERIFIED by re-reading Phase 11 §16's description: "n processes simultaneously call...") contending for the same target | **multi-process, same host** — the strongest concurrency evidence that exists anywhere in this codebase, but still same-host (same `TARGET_CACHE_PATH`, same disk) |

**Not one test anywhere in this repository constructs two independent
`TargetRegistry` instances backed by two separate filesystem cache
directories sharing a single Redis client** — the minimum setup needed to
even *simulate* two hosts (each host's local disk is distinct; only Redis
is genuinely shared). Confirmed by grep across every test file that
constructs a `TargetRegistry`/`FilesystemEmbeddingCache`/
`FilesystemSegmentEmbeddingCache`: every single fixture uses exactly one
cache-directory pair. **VERIFIED.**

**Exact missing tests for multi-host correctness** (a same-codebase
simulation ceiling, not real multi-host):

1. Two `TargetRegistry` instances, two separate `tmp_path`-rooted cache
   directories, one shared `redis_client` — the closest this codebase can
   get to "simulated multi-host" without real separate machines. Currently
   **does not exist anywhere.**
2. The above, repeating `test_concurrent_miss_builds_only_once`'s scenario
   — today this would almost certainly **demonstrate the bug** (two
   builds, not one) rather than prove correctness, since it exercises
   exactly the structural gap §5 traces.
3. Real multi-host execution (two actual machines, or two containers with
   genuinely separate filesystems, both pointed at one real Redis
   instance) — **REQUIRES MULTI-HOST VALIDATION**, not something any
   change to this repository's test suite alone can provide. The most this
   codebase's test suite can ever assert is "two independent
   cache-directory instances sharing one Redis behave as spec'd" — actual
   cross-machine timing (network RTT to Redis, clock skew, real shared
   storage I/O latency) is out of reach of any test that runs on one
   machine, no matter how it's simulated.

---

## 10. Performance implications

Reusing Phase 11/13C's own measurements (MEASURED there, cited here — no
new benchmark run this session, per the task brief):

- **One target embedding build**: `target_build_s` mean **0.875-1.418s**
  (Phase 11 §15, Workload B cold-cache, n=8) — essentially identical to
  `candidate_embed_s`, since it's the same `embed_video_segments` call.
- **N duplicate builds**: under the current architecture, this is exactly
  N x the above **per target**, once per host that ever misses it — not
  once total. At fleet scale this is the direct cost of blocker #2 as
  traced in §5.
- **Cache-hit cost (local, same host)**: 2.14ms mean (Phase 11 §14,
  Workload A warm) — the number a *working* multi-host cache-hit path
  would need to stay close to for a losing host's request to be cheap
  rather than a duplicate ~1s build.
- **Storage size per target**: 107,697-203,684 bytes (~107-199 KiB) for
  6-12 segments, i.e. **~17 KiB/segment** as uncompressed JSON floats
  (Phase 11 §18, MEASURED). At a stated production-scale extrapolation
  (10,000 targets x ~30 segments average) this is **~5 GiB** total
  (Phase 11 §18, INFERRED, not measured at that scale).
- **Redis's current footprint per target**: one small metadata hash, no
  vector data (Phase 6/9 design, unchanged) — negligible.

**What storing the actual embeddings centrally would trade CPU savings
for**, evaluated against the numbers above:

- **Redis memory**: if vectors moved into Redis (Option B, §11), the ~5
  GiB extrapolated library size becomes ~5 GiB of **Redis process memory**
  (Redis is in-memory; unlike a filesystem cache, this is not "disk space
  that happens to exist," it directly competes with the memory this same
  Redis instance uses for job streams/state/locks) — a materially
  different operational profile for the one Redis instance this whole
  system already depends on for job coordination correctness.
- **Network transfer / Redis bandwidth**: each cache hit would become a
  network round-trip transferring ~17-200 KiB, instead of a 2.14ms local
  file read. At realistic per-job rates (Phase 11 §14: Redis coordination
  today costs 0.71ms/job combined for claim+commit, a number this system's
  own numbers already treat as negligible against the ~900ms embedding
  cost) an extra sub-hundred-KiB Redis transfer per job is **plausibly
  still negligible** against the dominant ~900ms embedding cost — but this
  is **INFERRED by extrapolation from Phase 11's own "everything but
  embedding is <5%" finding, not measured directly for this specific
  access pattern.**
- **Serialization/deserialization**: JSON (de)serialization of a ~100-200
  KiB payload is the same order of work the cache already does today on
  every local hit (2.14ms figure already includes this) — moving the
  bytes over a socket first adds latency but not meaningfully more
  (de)serialization cost than what's already measured as negligible.
- **Startup latency**: not applicable — nothing about the embedding cache
  is read at process startup in the current design (`build_registry`
  constructs the cache objects, not their contents).

**No solution is chosen by this section — this is characterization only,
per the task brief.**

---

## 11. Candidate architectures

| | A. Shared filesystem/object storage | B. Redis-backed embedding cache | C. Redis coordination + per-host filesystem (current) | D. Hybrid: Redis coordination/metadata + shared object storage |
|---|---|---|---|---|
| Multi-host correctness | Yes, if the mount/store is genuinely shared and read-after-write consistent | Yes | **No — this is the current, broken state**, kept only as the baseline row | Yes — mechanically the same correctness property as A, described with the coordination/storage roles made explicit |
| Duplicate computation | Eliminated once storage is genuinely shared (same "one build, N hits" property Workload C already proved *within* a host) | Eliminated | **Not eliminated — the entire finding of this audit** | Eliminated |
| Cache durability | As durable as the chosen backend (object storage: typically very high; NFS: depends on operator's storage backend) | As durable as Redis's own persistence config (AOF/RDB) — **this system does not currently rely on Redis for durable data**, only ephemeral coordination/small state; embeddings would be the first genuinely durable *data* asset stored there, a different operational category than job/lock state | Durable per-host, permanently invisible cross-host (not a durability problem, a visibility problem) | Same as A |
| Network cost per hit | One read per cache **miss on this host** (i.e., paid once per target per host, not once per job, if a local read-through layer is added — see recommendation) or once per job if reading the shared store directly every time | One Redis round-trip per job (every hit, not just first-per-host) | Zero (but wrong) | Same as A |
| Redis memory impact | None — Redis footprint unchanged from today | **Significant** — ~5 GiB at the stated production-scale extrapolation (§10), growing with the target library, competing with this Redis instance's existing job/lock/state workload | None | None — Redis keeps its current tiny metadata-only footprint |
| Implementation complexity | Low — implements the existing `TargetEmbeddingCache`/`SegmentEmbeddingCache` ABCs; **zero change** to `target/registry.py` or `worker/matching_handler.py` (confirmed, Phase 13 §12's own "Required fixes" table already reached this conclusion independently) | Moderate — same ABC-swap simplicity for the interface, but now also requires deciding a Redis data-structure encoding for a large binary/JSON blob, and reasoning about Redis memory/eviction policy for a new kind of data this instance has never stored before | None (nothing to implement — this is the status quo) | Low — same as A; "hybrid" here just names the pattern (Redis stays coordination-only, storage is genuinely shared) that A already is |
| Failure behavior | A shared-store outage fails builds/hits, surfaced as the existing `TransientFailure`/`PermanentFailure` mapping already in place (`_resolve_target_segments`'s existing exception handling does not need to change shape, only the exception types a new backend raises) | Same category of failure, but now coupled to the same Redis instance job-claim/lease/retry correctness depends on — a large embedding-cache workload degrading Redis (memory pressure, slow commands) has blast radius onto job coordination itself, which does not exist today | Current: "fails" silently as a 10-minute stall + duplicate build, never a hard error, which is arguably worse (undetected latency debt) than an honest failure | Same as A |
| Operational complexity | Requires provisioning real shared storage (NFS mount, or an S3-compatible endpoint + credentials) — a new piece of infrastructure this project does not currently have | No new infrastructure (reuses existing Redis), but requires monitoring/capacity-planning Redis memory for a new, larger-than-before data category | None (already running) | Same as A |
| Compatible with current `TargetRegistry` API | **Yes, unmodified** — this is true of A/B/D equally, the whole point of the ABC boundary Phase 6 built | Yes, unmodified | N/A | Yes, unmodified |

Two sub-flavors of Option A/D worth distinguishing (this audit does not
select between them, since the choice depends on infrastructure this
codebase's own docs do not name as already available):

- **A1 — network filesystem mount** (e.g. NFS): the *smallest possible*
  change — `TARGET_CACHE_PATH` simply points at a mounted shared
  directory, and `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache`
  need not change at all. **Caveat, not previously documented in this
  codebase**: `os.replace()`'s atomicity guarantee is a POSIX-single-filesystem
  guarantee; NFS rename semantics have historically had client-side
  attribute-caching windows in some configurations/versions where a
  rename on one client is not immediately visible to another client's
  already-open directory listing or cached negative lookup — this would
  reintroduce a *bounded* visibility delay (not the current architecture's
  *permanent* invisibility) rather than fully solving cross-host
  visibility instantly. **REQUIRES MULTI-HOST VALIDATION** against the
  specific NFS version/mount options an operator would actually use; this
  audit does not have an NFS environment to test against.
- **A2 — object storage** (S3-compatible): requires a new
  `TargetEmbeddingCache`/`SegmentEmbeddingCache` implementation (not just
  a path change), but avoids the NFS caching caveat — a completed `PUT` is
  immediately visible to a subsequent `GET` for a new key on modern
  S3-compatible stores (strong read-after-write consistency for new
  objects is the current behavior of AWS S3 and most compatible
  implementations, though this varies by *specific* backend and is a
  property of whichever object store is actually chosen, not something
  this codebase can guarantee in the abstract).

Not evaluated further because nothing in this repository's docs
(`design-proposal-1.md`, Phase 1-13 docs) names an object-storage or NFS
endpoint as already provisioned — that choice sits outside this audit's
evidence and is properly an infrastructure decision, not a code one.

**No option is selected in this section — comparison only, per the task
brief.**

---

## 12. Recommendation

### CURRENT ARCHITECTURE IS NOT MULTI-HOST SAFE

**Precise blocker** (restating §4/§5 exactly): the Redis-backed build-on-miss
lock correctly serializes *who is allowed to build* across any number of
hosts, but the artifact it protects — the segment embedding cache — is a
per-host local filesystem, and no code path ever propagates a completed
build's result to any host other than the one that built it. A losing
host's poll loop is checking a resource that is structurally incapable of
ever reflecting a winning host's result. This degrades a multi-host fleet
from "one build total" (the correct, single-host-proven property) to "one
build immediately, then every other host stalls up to 10 minutes before
independently duplicating it" — worse than either no locking at all or a
genuinely shared cache.

A second, adjacent, previously-undocumented gap (§3.5) means this is not
even the complete story: raw target media (`record.media_path`) is
likewise host-local with no replication/acquisition mechanism, so even a
fixed embedding-cache backend does not by itself guarantee a losing host
can successfully build target T's embedding at all.

### Smallest production-grade fix

**Swap the storage backend, keep everything else.** Per §11's own
implementation-complexity comparison and matching Phase 13's own
"Required fixes" table (`phase-13-production-hardening.md` §13, row "#2
Multi-host cache"): implement a new `TargetEmbeddingCache`/
`SegmentEmbeddingCache` (Option A/D — a genuinely shared backend, filesystem-
or object-storage-based depending on what infrastructure is actually
available) and change exactly one call site — `worker/main.py`'s
`build_registry()` — to construct it instead of
`FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache`. Confirmed by
this audit's own tracing (§1, §4): `TargetRegistry`, `RedisLock`, and
`worker/matching_handler.py` never reference the filesystem cache
implementation by name anywhere outside `worker/main.py`'s construction
call — the ABC boundary (`target/cache.py`'s `TargetEmbeddingCache`,
`target/segment_cache.py`'s `SegmentEmbeddingCache`) is real and already
isolates this exactly as Phase 6 designed it to.

Explicitly, per the task brief's required elements:

- **Source of truth**: the new shared storage backend becomes
  authoritative for embedding vector bytes. Redis remains authoritative
  for target identity/metadata and lock coordination — **unchanged**.
- **What remains local**: nothing needs to, for correctness — `TARGET_CACHE_PATH`
  (or its replacement configuration) would point at the shared resource
  directly. An optional, *not required for correctness*, future
  optimization would be a local read-through cache layered in front of
  the shared store to avoid repeat network reads for the same target on
  the same host across many jobs — explicitly **not proposed as part of
  the minimal fix**, since §10's own numbers suggest the added network
  cost is likely negligible against the ~900ms dominant embedding cost,
  and adding a local cache layer would reintroduce exactly the kind of
  two-tier-cache invalidation complexity this fix is trying to avoid.
- **How concurrent builds are coordinated**: unchanged — the existing
  `RedisLock`/`get_or_build_segment_embedding` control flow already does
  the right thing *once the thing it protects is genuinely shared*; the
  bug is entirely in what's on the other side of the ABC, not in the
  coordination logic itself.
- **How failures/crashes are handled**: the lock's existing, documented
  no-auto-renewal limitation (§6, §12 of `target/lock.py`'s own docstring)
  is unchanged and out of this fix's scope — Phase 11 §17's own analysis
  (600s TTL against ~0.9-1.4s observed builds, ~430-670x margin) already
  concluded this is not currently a practical risk; nothing in this audit
  changes that conclusion.
- **How cache identity is preserved**: unchanged — `cache_entry_key()`
  (§3) is already host-agnostic and correct; no change needed.
- **How old/incompatible embeddings are invalidated**: unchanged —
  `_load_and_validate`'s field-by-field compatibility re-check (§3) is
  backend-agnostic and would need to be reimplemented identically (same
  required-fields check, same rejection-of-anything-less-than-exact-match
  contract) in whatever new backend is written — this is a **porting
  requirement** for the implementation phase, not a design change.

**The §3.5 adjacent finding (target media acquisition) is explicitly not
resolved by this recommendation** and must be scoped separately —
resolving it likely requires either (a) target media also living in the
same shared storage the embedding cache now uses, with `_target_artifact`
reading from there instead of a bare local path, or (b) a
`MediaAcquirer`-style acquisition step for targets analogous to what
already exists for candidates. Neither is designed here; this audit only
establishes that it exists and is not automatically fixed by §12's main
recommendation.

---

## 13. Required test plan for the implementation phase

Not implemented this phase, per instructions. Specified for Phase 13D's
implementation step:

1. **Same-host, two-worker cache miss**: two `Worker`/handler instances
   (or two direct `TargetRegistry` calls) sharing one `TargetRegistry`
   instance and one cache backend — extends the existing
   `test_concurrent_miss_builds_only_once` pattern, should continue
   passing unmodified against the new backend (regression guard).
2. **Multi-process concurrent cache miss**: extends
   `benchmarks/bench_pipeline.py`'s Workload C pattern (real separate
   processes) against the new backend — confirms same-host multi-process
   behavior is unchanged (still exactly one build).
3. **Two workers requesting the same target simultaneously, via two
   independent `TargetRegistry` instances backed by two separate storage
   roots (the "simulated multi-host" ceiling identified in §9)** — this
   is the test that does not exist anywhere today and is the direct
   regression guard for the bug this audit traces.
4. **Only one target embedding build occurs** — assertable via a call
   counter on the injected `build` callback, same style as existing tests,
   but across the two-registry setup from #3.
5. **Second worker (second registry instance) observes/reuses the
   completed result** — the core correctness assertion the current
   architecture fails; must pass against the new backend.
6. **First worker crashes during target build** — simulate by raising
   inside the `build` callback after the lock is held but before
   `register_segment_embedding` completes; assert the lock is released
   (existing `test_build_exception_releases_lock_for_a_later_retry`
   pattern) and that no partial artifact is visible to the second registry
   instance (extends #7 below to the cross-instance case).
7. **Lock expires/recovery occurs correctly**: simulate (as
   `tests/test_target_build_on_miss.py::test_lock_wait_timeout_raises_without_calling_build`
   already does for the single-registry case) a held lock across two
   registry instances, confirm the waiting instance still respects
   `poll_timeout_s` and does not duplicate work prematurely.
8. **Incomplete cache artifact is never consumed**: for whatever new
   backend is chosen, an equivalent to the existing
   `_atomic_write`/temp-file-and-rename (filesystem) or an equivalent
   atomic-PUT guarantee (object storage) must be proven — e.g. a test that
   kills the write mid-flight (or mocks a partial upload) and asserts a
   concurrent reader never observes a partial/corrupt entry. The exact
   mechanism depends on which backend is chosen (§11) and cannot be fully
   specified until then.
9. **Different target identities do not collide**: port
   `test_cache_miss_for_different_target_content_hash` and the
   `EmbeddingSpec`-mismatch tests (`test_cache_miss_for_different_model_version`,
   `test_cache_miss_for_different_preprocessing_config`) against the new
   backend — should pass unmodified in behavior, since `cache_entry_key()`
   itself does not change.
10. **Model/configuration identity prevents incompatible reuse**: same
    port as #9, specifically for `model_id`/`model_version`/
    `embedding_schema_version` changes.
11. **Existing single-worker cache-hit behavior remains unchanged**: the
    full existing `tests/test_segment_cache.py` and relevant
    `tests/test_target_build_on_miss.py` suites should pass unmodified
    (module-level parametrization over backend, or a direct port) against
    the new backend — proves no regression for the single-host case this
    system has relied on through Phase 1-13C.
12. **Existing matching results remain unchanged**: `tests/test_matching_handler.py`
    and `tests/test_integration_e2e.py` should pass unmodified once
    `worker/main.py`'s `build_registry()` is repointed — these tests
    exercise the full handler path and would catch any behavioral drift
    the new backend introduces into the actual match decision, not just
    the cache layer in isolation.

**Genuine multi-host execution**: not possible in this development
environment (single machine, no second host, no provisioned shared
storage). Test #3-#5's two-registry-instance pattern is the **strongest
same-codebase simulation possible** — it correctly exercises "two
independent local states, one shared coordination layer," which is the
structurally relevant property, but it cannot exercise real network
latency, real shared-storage consistency semantics (§11's NFS
read-after-write caveat), clock/NTP skew, or connection-level Redis
behavior under true cross-machine conditions. **Actual multi-host
validation REQUIRES MULTI-HOST VALIDATION** — a real second machine (or
container with a genuinely separate filesystem) against the same Redis
instance and the same provisioned shared storage, not faked by this
sandbox.

---

## 14. Known uncertainties

- Real cross-machine lock-acquisition timing (network RTT effect on the
  ~1s-scale build window) — **REQUIRES MULTI-HOST VALIDATION**.
- Whether an NFS-backed Option A1 introduces a practically-observable
  visibility delay on the operator's actual NFS version/mount options —
  **REQUIRES MULTI-HOST VALIDATION**, no NFS environment available here.
- Whether Redis memory pressure from a hypothetical Option B deployment
  would measurably affect job-coordination latency on the *same* Redis
  instance — **UNAVAILABLE**, not modeled or measured this session.
- Real network transfer cost of a shared-storage read per job at
  production request rates — **INFERRED** likely negligible from Phase
  11's own "<5% of job time for everything but embedding" finding, not
  independently measured for this specific access pattern.
- The §3.5 target-media-acquisition gap's own resolution design is
  entirely unscoped by this audit — **UNAVAILABLE**, deliberately left
  for a future task brief to define, not guessed at here.
- Whether any object-storage or NFS infrastructure is actually available
  to this project — **UNAVAILABLE** from this codebase's own
  documentation; an infrastructure/ops question outside this audit's
  evidence.

---

**This document is an audit only. Blocker #2 (multi-host target cache) is
confirmed, not resolved. No implementation work was performed. Awaiting
instruction before starting Phase 13D's implementation.**
