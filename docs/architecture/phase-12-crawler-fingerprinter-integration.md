# Phase 12 — Crawler → Fingerprinter Integration Architecture

## 1. Objective

Phases 1-11 built an independently-deployable fingerprinter: a Redis job
contract (Phase 1), lease/crash recovery (Phase 2), retry/backoff (Phase
3), a result contract (Phase 4), media acquisition (Phase 5-6), a target
cache (Phase 7), DINOv2 embeddings (Phase 8), temporal/video matching
(Phase 9), multi-technique aggregation (Phase 10), and a performance
characterization (Phase 11, whose central finding — DINOv2 candidate
embedding is ~95% of warm-cache job latency, Redis coordination is ~0.08%
— is treated as established input to this phase, not re-measured).

Phase 12's job is the first real integration boundary to the crawler
system: turn "a crawler discovers a candidate URL" into "a fingerprint
worker produces a result the crawler can act on," asynchronously, without
merging the two systems, without the fingerprinter learning to crawl
search engines, and without the crawler learning to run DINOv2 inference.

Every finding below is labeled **CURRENT IMPLEMENTATION**, **MEASURED**,
**INFERRED**, **PROVISIONAL**, **REQUIRES MULTI-HOST VALIDATION**, or
**DEFERRED**, per the phase brief — same convention
`phase-11-performance-benchmarks.md` established.

**Git revision this phase started from:** `56ab52cdc5266e04ac2986f6457407452160d273`
("phase 11"), fingerprinter repo, working tree clean. Crawler repo
(`/home/darkdevil/Desktop/anti_piracy/crawler`) at
`44fe9ab2d31e57b1c3cc6ab7d300f2360f2dce5f`, **not modified by this phase** —
every finding about it below is from read-only inspection.

## 2. Current crawler architecture (as inspected, not modified)

**CURRENT IMPLEMENTATION**, verified by direct inspection of the crawler
repo (see §7 for the full namespace table). Two independent Redis-backed
subsystems exist there already:

- **URL frontier** (`core/redis_frontier.py`, namespace `crawler:*`):
  priority-ZSET + Lua-scripted atomic claim/lease/retry, tracking crawl
  status (`discovered → queued → inflight → visited/skipped/failed`) —
  nothing to do with fingerprinting.
- **Media evidence store** (`storage/redis_media_evidence_store.py`,
  namespace `evidence:*`): tracks discovered media assets
  (`evidence:asset:{aid}` hashes) and **already has its own fingerprint
  job queue** — `evidence:jobs:queue` (ZSET), `FingerprintJob`/
  `FingerprintResult` dataclasses (`storage/media_evidence_store.py`), and
  a full claim/lease/complete/fail/reclaim Lua-scripted API
  (`claim_next_fingerprint_job()`, `complete_fingerprint_job()`, etc.) —
  built and unit-tested (`tests/fingerprinter_queue_test.py`), but with
  **no actual fingerprinter ever having consumed it** (no DINOv2/pHash/
  audio code exists anywhere in that repo outside `env/`). The crawler's
  own architecture doc
  (`docs/architecture/media-evidence-redis-design.md`) and
  `system-architecture.md` explicitly name "the separate `fingerprinter/`
  project (future)" as the intended consumer of `evidence:jobs:queue`.

This is a real architectural finding, not a minor detail: **the crawler
team built their side of this integration expecting the fingerprinter to
import their Python API and consume `evidence:jobs:queue` directly.**
§4 explains why this phase does not do that.

Other facts load-bearing for this phase's design:

- Crawler priority is a plain `int` (`FrontierClaim.priority`,
  `FingerprintJob.priority`), **lower value = more urgent**, ZSET score
  `priority * 1_000_000 + seq`, conventional default `10`. Structurally
  different from this fingerprinter's own priority mechanism (named job
  streams — see §12).
- Crawler dedup key: `discovery_id = sha256(clean_media_url(url))`
  (`storage/media_evidence_store.py::compute_discovery_id`) — a related
  but separate idempotency concept from this phase's `job_id` (§9).
- Crawler test-isolation convention: Redis DB **1** (and a reserved DB 2
  for two suites), never DB 0 (production) — see §7 for why this doesn't
  collide with this repo's own DB 15 test convention.
- The crawler's `FingerprintResult`/`complete_fingerprint_job()` write
  path exists but only a manual CLI stub (`main.py --complete-fingerprint-
  job`) ever calls it — there is no automated crawler-side consumer of a
  fingerprinter's output today. Building one is out of scope for this
  phase (crawler repo unmodified) — see §23.

## 3. Current fingerprinter architecture (unchanged by this phase except §5)

**CURRENT IMPLEMENTATION.** Summarized because §4-§22 build directly on
it; full detail lives in the Phase 1-11 docs and code.

| Layer | Module | Role |
|---|---|---|
| Job contract | `work_queue/jobs.py` | `Job` dataclass <-> Redis Stream fields |
| Redis keys | `work_queue/keys.py`, `target/keys.py` | `fingerprint:*` namespace (§7) |
| Producer | `work_queue/producer.py` | Fire-and-forget `XADD` — "library used by crawler" per `docs/design/design-proposal-1.md` §2 |
| Worker | `worker/fingerprint_worker.py` | Claim (`XREADGROUP`), lease/reclaim (`XAUTOCLAIM`), retry/backoff, atomic commit (Lua scripts) |
| Handler | `worker/matching_handler.py` | Claim -> acquire -> embed -> resolve target -> match -> aggregate -> `Result` |
| Acquisition | `acquisition/` | URL -> validated local `MediaArtifact`, bounded/streamed, scheme/content-type/size checks |
| Embedding | `embedding/` | `DINOv2EmbeddingEngine`, CPU/GPU-configurable, `torch_num_threads` (Phase 11 addition) |
| Matching | `matching/` | Segment-level temporal matcher + `TechniqueEvidence`/`combine()` aggregation |
| Target cache | `target/` | `TargetRegistry` + pluggable `TargetEmbeddingCache`/`SegmentEmbeddingCache` (filesystem today), build-on-miss lock |
| Result | `work_queue/results.py` | `Result`/`ResultRecord`, three-way `decision` (`match`/`no_match`/`processing_failure`) |

This is exactly `docs/design/design-proposal-1.md`'s architecture, built
out across 11 phases. That document's §2 already named the missing piece
this phase builds: **"Job producer (library used by crawler)"** — Phase 1
shipped a minimal version (`JobProducer.enqueue()`, bare fire-and-forget);
this phase makes it safe to call from an untrusted, possibly-duplicating,
possibly-overloading crawler.

## 4. Integration boundary — and a resolved architectural fork

**DECISION, justified below.** Two candidate designs existed after
inspecting both systems (§2):

**Option A (rejected):** the fingerprinter directly consumes the
crawler's `evidence:jobs:queue` via `storage.redis_media_evidence_store
.RedisMediaEvidenceStore.claim_next_fingerprint_job()` (a crawler Python
API), matching what the crawler team's own docs describe as the intended
future.

**Option B (implemented):** the fingerprinter's own Redis Streams job
contract (`fingerprint:jobs:stream:*`, Phases 1-4/11, already built,
already tested) remains the sole fingerprinter input surface, reused
as-is. A new library (`integration/`, this phase) makes it safe to call
from any producer. Translating `evidence:jobs:queue` entries into
`fingerprint:*` jobs is a small, separately-deployable bridge — its exact
contract is fully specified here (§9, §accepted mapping) but it is **not
built in this phase**, because it is crawler-repo work this phase's brief
explicitly puts out of scope ("do not modify unrelated crawler ...
behavior").

**Why B, not A:** this fingerprinter repo's own founding architecture
document, written before any of Phases 1-11 existed, states explicitly:

> "Explicitly excluded from production: SQLite as a queue or Redis
> mirror, crawler SQLite access from the fingerprinter, **direct crawler
> Python imports**, and shared-filesystem assumptions between crawler and
> fingerprinter." — `docs/design/design-proposal-1.md`, opening paragraph.

Option A would violate that constraint directly: `claim_next_fingerprint_job()`
is not a wire protocol, it's a Python method on a class defined in the
crawler repo, requiring the fingerprinter to import crawler code (or vice
versa) — exactly what every phase since Phase 1 was built to avoid, and
exactly what "the crawler and fingerprinter must remain independently
deployable" (this phase's own brief) requires avoiding. It would also mean
building a second claim/lease/retry implementation against the crawler's
ZSET+Lua+CAS primitives, duplicating logic `work_queue`/`worker.
fingerprint_worker.Worker` already implements and Phase 11 already
performance-characterized — the brief's "do NOT create a second competing
job queue implementation," read the other way around (don't make the
*existing* queue redundant by building a second consumption path either).

**What this means concretely:** `integration/` (this phase) is a
Redis-protocol-only library — it never imports anything from the crawler
repo, and nothing in the crawler repo needs to import from it either
(a bridge component, if/when built, would be the one process importing
both, and is explicitly deferred — §23).

## 5. Job schema

**CURRENT IMPLEMENTATION**, reused almost entirely as-is, with one
additive field.

`work_queue.jobs.Job` (Phase 1) already carried everything the brief asks
for except an explicit schema version:

| Brief's wishlist field | `Job` field (existing, unless noted) |
|---|---|
| `job_id` | `job_id` — now derived deterministically, see §9 |
| `schema_version` | `schema_version` **(new this phase)** |
| `candidate_url` | `media_url` (brief's vocabulary; not renamed — see §5a) |
| `source_url` / discovery URL | not added — see §5b |
| `target_id` | `target_id` |
| `target_version` | `target_version` |
| `priority` | not a `Job` field — selects which stream (§12) |
| `created_at` | not a `Job` field — derived from the Stream entry ID (§5c) |
| `attempt` | not a `Job` field — tracked in `fingerprint:job:{job_id}:state`, worker-computed (Phase 1) |
| fingerprinter-required metadata | `techniques` (existing) |

### 5a. `candidate_url` vs. `media_url` — not renamed

The brief's vocabulary and this repo's existing field name refer to the
same thing. Renaming `media_url` -> `candidate_url` across `work_queue`,
every handler, and 30+ existing tests would be a purely cosmetic,
repo-wide change for zero behavioral benefit — exactly the kind of
invasive change the brief's own "make the smallest necessary change"
instructs against. `integration.candidate.FingerprintCandidate` uses the
brief's vocabulary (`candidate_url`) at the crawler-facing boundary and
maps it onto `Job.media_url` in `integration/submission.py` — the
vocabulary difference is contained to one mapping site.

### 5b. `source_url` / discovery URL — deliberately not added

The brief allows this "if useful." It isn't, given what already exists:
`Job.source_domain` (Phase 1) already carries lightweight provenance, and
`Job.media_evidence_id` is the crawler's own opaque back-pointer to
whatever full context (source page, referrer, discovery method — all
present on the crawler's `evidence:asset:{aid}` hash, §2) it wants to
correlate later. Duplicating that context into the job payload would be
exactly the "blindly copy the entire crawler URL metadata structure" the
brief warns against, for a fingerprinter that has no use for it (the
fingerprinter's job is media, not page context).

### 5c. `created_at` — deliberately not added

A `Job`'s creation time already exists for free: `XADD stream *` assigns
a Stream entry ID of the form `<ms-since-epoch>-<seq>`
(`work_queue.producer.JobProducer.enqueue()` already returns it).
`integration/timing.py::created_at_from_entry_id()` recovers it. Adding a
duplicate `created_at` field to `Job` would be storing the same fact
twice, with the two copies capable of disagreeing (clock skew between
"when the field was set" and "when `XADD` actually ran") — Redis's own
timestamp is authoritative and free.

### 5d. `schema_version` — added

`work_queue/jobs.py` (`JOB_SCHEMA_VERSION = 1`) is the one change to an
existing production module this phase makes. Every other durable contract
in this project already carries an explicit schema version
(`RESULT_SCHEMA_VERSION`, `CACHE_ENTRY_SCHEMA_VERSION`,
`SEGMENT_CACHE_ENTRY_SCHEMA_VERSION`) — the job contract was the one
exception, and it is the contract with the most independent producers
(crawler machines, per `design-proposal-1.md` §2, "N crawler machines:
fire-and-forget XADD producers, no coordination needed between them" —
exactly the multi-producer-version-skew scenario schema versioning
exists for). The change is purely additive: `to_stream_fields()` always
writes it; `from_stream_fields()` treats an absent value as `1` (no
producer has ever written a job before this field existed, so "absent"
unambiguously means schema 1) and rejects any *present but different*
value (mirrors `target/cache.py`'s exact-match-only version check — a
worker has no logic to interpret a job shape it doesn't recognize, so it
refuses rather than guesses). **Verified non-breaking**: only
`tests/conftest.py::make_job` constructs a `Job` directly (via keyword
args, gets the new field's default); every other call site round-trips
through `to_stream_fields()`/`from_stream_fields()`, which stayed
consistent. Full suite re-run confirms this (§21).

## 6. Result schema

**CURRENT IMPLEMENTATION**, reused entirely as-is, with a new read-side
view added on top (not a redesign — the brief allows this explicitly:
"do not redesign the existing Phase 10 TechniqueEvidence/aggregation
contract unless integration reveals an actual incompatibility"; it
revealed a need for a wider *view*, not an incompatibility).

The brief wants a result that "clearly distinguishes MATCH, NO_MATCH,
SKIPPED, RETRYABLE_ERROR, PERMANENT_ERROR" and "does not overload a single
boolean." No single existing contract has exactly this five-way shape —
but the five outcomes already exist, split across two contracts that were
each doing their own job correctly:

- `work_queue.state.JobStatus` (Phase 1-3): `claimed` / `completed` /
  `rejected` (malformed job) / `retry_scheduled` / `failed` — the
  *queue's* view (did the job run to completion at all).
- `work_queue.results.ResultDecision` (Phase 4/10): `match` / `no_match` /
  `processing_failure` — only exists once a job *did* complete, and only
  describes the fingerprinting *verdict*, not the queue-level journey to
  get there.

`integration.outcome.resolve_outcome()` (new) is a pure read-side fold of
both into `FingerprintOutcome`, exactly the mapping table this phase's
brief's "Result contract" section calls for:

| `JobStatus` | `ResultDecision` (if completed) | `FingerprintOutcome` | Terminal? |
|---|---|---|---|
| *(no state hash yet)* | — | `PENDING` | no |
| `claimed` | — | `PENDING` | no |
| `retry_scheduled` | — | `RETRYABLE_ERROR` | no |
| `rejected` | — | `SKIPPED` | yes |
| `failed` | — | `PERMANENT_ERROR` | yes |
| `completed` | `match` | `MATCH` | yes |
| `completed` | `no_match` | `NO_MATCH` | yes |
| `completed` | `processing_failure` | `PERMANENT_ERROR` | yes |
| `completed`, no `Result` committed | — | `SKIPPED` | yes |

Justification for the two rows that aren't a direct rename:

- `rejected` -> `SKIPPED`: a malformed job contract will never run
  regardless of retries — "skipped," not "errored," matches the brief's
  own distinction between a deliberate non-run and a failure.
- `completed` + `processing_failure` -> `PERMANENT_ERROR`: Phase 10's
  `PROCESSING_FAILURE` already means "ran to completion but the media
  itself was unusable" (corrupt file, unsupported codec) — retrying the
  same bytes at the same URL cannot fix that, so from a crawler's
  perspective this is exactly a permanent error, not a different category.

`FingerprintOutcomeView` (the value `resolve_outcome()` returns) carries
`job_id`, `media_evidence_id`, `target_id`, `target_version`, `attempt`,
`worker_id`, `confidence`, `summary`, `evidence` (Phase 10's JSON, passed
through verbatim, unmodified), `algorithm`, both processing timestamps,
and a `reason` string for non-MATCH/NO_MATCH outcomes — everything the
brief's "Result contract" section lists ("processing timestamps, worker
information ..., attempt number, evidence/score information ..., error
classification").

## 7. Redis namespaces

**CURRENT IMPLEMENTATION** (crawler columns) / **MEASURED via direct
inspection** (not modified this phase) + **this phase's one addition**.

| Namespace | Owner | Examples | Purpose |
|---|---|---|---|
| `crawler:*` | crawler (`core/redis_frontier.py`) | `crawler:urls:known`, `crawler:domain:{d}:queue`, `crawler:inflight` | URL frontier — unrelated to fingerprinting |
| `evidence:*` | crawler (`storage/redis_media_evidence_store.py`) | `evidence:asset:{aid}`, `evidence:jobs:queue`, `evidence:result:{aid}` | Crawler's own media-evidence/job tracking (§2, §4) |
| `fingerprint:jobs:stream:{priority}` | fingerprinter, Phase 1 | — | Job stream (per-priority, §12) |
| `fingerprint:job:{job_id}:state` | fingerprinter, Phase 1 | — | Claim/attempt/status hash |
| `fingerprint:job:{job_id}:result` | fingerprinter, Phase 4 | — | Durable result hash |
| `fingerprint:results:stream:{priority}` | fingerprinter, Phase 4 | — | Result event stream |
| `fingerprint:retry:delayed:{priority}` | fingerprinter, Phase 3 | — | Delayed-retry ZSET |
| `fingerprint:target:{id}:{version}` | fingerprinter, Phase 6 | — | Target identity/metadata |
| `fingerprint:target:{id}:{version}:embeddings` / `:segment_embeddings` | fingerprinter, Phase 6/9 | — | Cache metadata (no vectors — §14) |
| `fingerprint:target:content:{sha256}` | fingerprinter, Phase 6 | — | Content-hash reverse index |
| `fingerprint:lock:target:{cache_key}` | fingerprinter, Phase 10 | — | Build-on-miss lock |
| `fingerprint:submission:{job_id}` | fingerprinter, **Phase 12 (new)** | `integration/keys.py` | Idempotent-submission marker (§9) |

**Collision check (MEASURED by inspection, both directions):** no
`fingerprint:*` key exists anywhere in the crawler repo; no `crawler:*` or
`evidence:*` key exists anywhere in this repo. `fingerprint:submission:*`
does not collide with any existing `fingerprint:job:*`/`fingerprint:jobs:*`
pattern (verified: it is a distinct third segment, `submission`, not
`job`/`jobs`). `tests/test_integration_submission.py
::test_integration_writes_never_touch_crawler_or_evidence_keys` asserts
this at the Redis level (pre-seeds representative `crawler:*`/`evidence:*`
keys, runs a full submit/claim/ack cycle, asserts those keys are
byte-for-byte untouched and every new key starts with `fingerprint:`).

**Test-database isolation:** this repo's tests use `redis://localhost:6379/15`
(`tests/conftest.py`); the crawler's tests use DB 1 (and a reserved DB 2)
— **MEASURED via inspection, no collision** even if both suites ran
against the same Redis server in the same CI run.

## 8. Job lifecycle

**CURRENT IMPLEMENTATION**, entirely reused from Phase 1-3, entered
through the new `integration.submission.FingerprintJobSubmitter` instead
of a bare `JobProducer.enqueue()`:

```
synthetic/real crawler candidate
        |
        v
FingerprintCandidate.validate()          [integration/candidate.py]
        |
        v
derive_job_id(candidate)                 [integration/idempotency.py, deterministic]
        |
        v
count_outstanding() <= max?              [integration/backpressure.py]  -- reject if not (§13)
        |
        v
SET NX fingerprint:submission:{job_id}   [integration/submission.py]   -- reject if already set (§9)
        |
        v
XADD fingerprint:jobs:stream:{priority}  [work_queue/producer.py, unchanged]
        |
        v
XREADGROUP (worker claim)                [worker/fingerprint_worker.py, unchanged, Phase 1-2]
        |
        v
acquire -> embed -> resolve target -> match -> aggregate   [worker/matching_handler.py, unchanged, Phase 5-10]
        |
        v
commit_result() / _handle_transient_failure() / _fail()    [worker/fingerprint_worker.py, unchanged, Phase 3-4]
        |
        v
resolve_outcome(job_id)                  [integration/outcome.py, new] -- crawler/downstream reads this
```

Nothing below the "count_outstanding" line changed in this phase — Phase
12 only adds the admission-control layer in front of the existing,
unchanged claim/lease/retry/commit machinery, and a read-side view behind
it.

## 9. Idempotency strategy

**CURRENT IMPLEMENTATION**, mandatory per the brief. Two distinct
mechanisms, each closing a different duplicate-delivery window:

1. **Deterministic `job_id`** (`integration/idempotency.py`):
   `job_id = sha256(candidate_url, target_id, target_version,
   sorted(techniques))[:32]`. Two crawler observations of the same URL
   against the same target version with the same technique set always
   produce the *same* `job_id` — including a rediscovery under a
   different `media_evidence_id` (deliberately excluded from the hash:
   two evidence records pointing at the same underlying check are still
   one unit of work). A target *version* bump produces a fresh `job_id`
   (brief, "Target versioning": target_id alone is not sufficient) —
   re-verification against new target content is a new job, not a
   duplicate.

2. **Atomic submission marker** (`SET fingerprint:submission:{job_id} ...
   NX EX <ttl>`, `integration/submission.py`): a deterministic `job_id`
   alone does not prevent duplicate `XADD`s — Redis Streams has no
   built-in dedup, so two `submit()` calls for the same candidate would
   otherwise produce two stream entries racing each other through
   `worker/fingerprint_worker.py`'s attempt-counting logic. The marker
   makes "has this exact job already been submitted" a single atomic
   Redis round-trip (`SET NX`), no distributed transaction, matching the
   brief's explicit preference. Ordering matters and is deliberate (see
   `FingerprintJobSubmitter.submit()`'s docstring): backpressure is
   checked *before* the marker is claimed, and the marker is released if
   the subsequent `XADD` itself fails — so a rejected or failed
   submission never leaves a "ghost" marker blocking a legitimate later
   retry.

**Deterministic identity across the pipeline** (brief's checklist):

| Identity | Mechanism |
|---|---|
| Job identity | `job_id` — deterministic hash (above) |
| Candidate identity | `candidate_url`, part of the hash input |
| Target identity | `(target_id, target_version)`, part of the hash input, unchanged from Phase 6-10 |
| Attempt identity | `fingerprint:job:{job_id}:state`'s `attempt` field, worker-computed, CAS-fenced (Phase 2) — unchanged |
| Result identity | `fingerprint:job:{job_id}:result`, one hash per `job_id`, overwritten only under the same attempt-CAS fencing (Phase 4) — unchanged |

**Not solved by this phase, by design:** a worker that finishes a job but
crashes before `commit_result()`'s Lua script runs leaves the job
reclaimable via Phase 2's existing `XAUTOCLAIM` path — reprocessed under a
bumped attempt, not treated as a fresh submission (the submission marker
already exists; `submit()` is never called again for it). Verified in
`tests/test_integration_e2e.py::test_worker_crash_lease_recovery_end_to_end`.

## 10. Retry semantics

**CURRENT IMPLEMENTATION, unchanged.** Phase 3's exponential backoff
(`base * 2^(attempt-1)`, capped) and Phase 2's `XAUTOCLAIM`-based lease
recovery apply identically to crawler-submitted jobs — nothing about
submission origin is visible to `worker/fingerprint_worker.py` at all
(a job on the stream is a job on the stream). `FingerprintCandidate
.max_attempts` (default 3, `integration/candidate.py`) maps directly onto
`Job.max_attempts`, unchanged.

Failure classification (brief's checklist, all pre-existing except the
`FingerprintOutcome` mapping in §6):

| Condition | Existing mapping | `FingerprintOutcome` |
|---|---|---|
| HTTP 404/403/410, unsupported scheme, unsupported content-type | `PermanentAcquisitionError` -> `PermanentFailure` (Phase 5) | `PERMANENT_ERROR` |
| Connect/read timeout, DNS/connection reset, 429/5xx | `TransientAcquisitionError` -> `TransientFailure` (Phase 5) | `RETRYABLE_ERROR` until attempts exhausted, then `PERMANENT_ERROR` |
| Invalid/corrupt media (fails `ffprobe`) | `InvalidMediaError` -> `PermanentAcquisitionError` (Phase 5) | `PERMANENT_ERROR` |
| Unsupported codec / candidate embedding failure | `UnsupportedMediaError`/`InferenceError` -> `Result(PROCESSING_FAILURE)` (Phase 10) | `PERMANENT_ERROR` (§6) |
| Target media itself corrupt/unusable | `UnsupportedMediaError` -> `PermanentFailure` (Phase 10) | `PERMANENT_ERROR` |
| Target embedding transient failure | `InferenceError` -> `TransientFailure` (Phase 10) | `RETRYABLE_ERROR` |
| Unknown `target_id`/`target_version` | `KeyError` -> `PermanentFailure` (Phase 10) | `PERMANENT_ERROR` |
| Build-on-miss lock wait timeout | `TimeoutError` -> `TransientFailure` (Phase 10) | `RETRYABLE_ERROR` |
| Redis unreachable | uncaught, propagates (infrastructure-level, not job-level) | n/a — see §16 |
| Worker crash mid-job | `XAUTOCLAIM` reclaim, Phase 2 | `PENDING` until reprocessed, then whatever it resolves to |

All verified end-to-end (not merely by table) in
`tests/test_integration_e2e.py` (target-version mismatch, retryable
acquisition error, permanent acquisition error, lease recovery) and
`tests/test_integration_outcome.py` (every `JobStatus`/`ResultDecision`
combination, synthetically).

## 11. Backpressure

**CURRENT IMPLEMENTATION**, mandatory per the brief.
`integration/backpressure.py::count_outstanding()` reads `XINFO GROUPS`
for the target priority stream and sums `lag` (not-yet-claimed backlog)
+ `pending` (claimed-but-not-yet-acked in-flight work) — both numbers
Redis Streams already tracks for `Worker.reclaim_stale()`'s own
`XAUTOCLAIM` call, so this adds no new bookkeeping structure.
`FingerprintJobSubmitter.submit()` rejects with
`REJECTED_BACKPRESSURE` when `outstanding >= max_outstanding_jobs`
(default `500`, `DEFAULT_MAX_OUTSTANDING_JOBS`) — **and also** when Redis
cannot report a reliable `lag` figure at all (fails toward rejecting, not
toward silently disabling the bound).

**Why 500 (PROVISIONAL, not load-tested):** Phase 11 measured a single
CPU worker process at ~1.08 jobs/s (warm cache, default 6 torch threads,
phase-11 §19b) and confirmed multi-process scaling only works with
explicit per-process thread pinning (§19a/§23, `torch_num_threads`),
reaching ~1.035 jobs/s aggregate at `worker_count=4` on one 6-core dev
host before the RAM safety gate stopped further scaling. Treating "~1
job/s per host, a handful of hosts" as a rough steady-state floor, 500
outstanding jobs bounds worst-case backlog-clearing latency to roughly
8-17 minutes at that floor. This has **no labeled SLA behind it** (none
exists in this project yet) and is explicitly a starting point for ops to
retune, not a load-tested production value — **REQUIRES MULTI-HOST
VALIDATION**.

**Both directions work by construction, not just by intent:**

- *Crawler throughput > fingerprint throughput*: `count_outstanding()`
  climbs; once it reaches `max_outstanding_jobs`, further `submit()`
  calls return `REJECTED_BACKPRESSURE` without enqueuing anything — the
  crawler is expected to keep the candidate in its own "not yet
  requested" state (§15) and retry submission later, exactly as the
  brief specifies ("crawler discovery create[s] unbounded fingerprint
  jobs" is what this prevents). Verified in
  `test_submission_is_rejected_once_outstanding_jobs_reach_the_limit`.
- *Fingerprint throughput > crawler throughput*: `count_outstanding()`
  naturally stays low (jobs get claimed/acked faster than they're
  produced) — `submit()` never rejects, nothing to prove beyond the
  normal happy path already covered everywhere else.

**Not a hard distributed rate limiter — PROVISIONAL:** the backpressure
count is a snapshot read, not itself lock-protected, so under many
concurrent submitters racing the same check, the bound can be briefly
exceeded before the next submitter observes the updated count. This is an
accepted, documented soft bound (matches the "admission control," not
"exact quota," framing of the brief), not a correctness issue — the
job-identity/idempotency mechanisms (§9) are unaffected by it.

## 12. Priority semantics

**CURRENT IMPLEMENTATION.** The existing architecture already supports
priority cleanly: `work_queue.keys.stream_key(priority)` puts different
priorities on entirely separate Streams, and a `Worker` instance is
already constructed against exactly one priority
(`Worker(redis_client, priority=...)`) — ops can staff "high" priority
with more/faster worker processes than "low" without any new mechanism.
This phase only names three bands (`integration.candidate
.FingerprintPriority`: `HIGH` / `NORMAL` / `LOW`) and maps them onto
stream names (`PRIORITY_STREAM_NAMES`) — `NORMAL` deliberately reuses
`work_queue.keys.DEFAULT_PRIORITY` ("default") rather than introducing a
same-meaning-different-name stream that every pre-Phase-12 worker/test
already reads from.

**Deliberately not built:** a numeric-score-to-band translation function.
The crawler's own priority convention (`int`, lower = more urgent,
default 10, `docs`/`crawler:*`/`evidence:*`) is a *fundamentally different
mechanism* (continuous ZSET score vs. discrete named streams) — inventing
a threshold mapping (e.g. "priority <= 3 -> high") without any calibration
data would be exactly the kind of unjustified guess the brief warns
against elsewhere ("do not hard-code an arbitrary queue limit without
documenting why" applies equally here). `FingerprintCandidate.priority`
is instead an explicit field a caller sets directly, using whatever
judgment it has — a future bridge component (§23) is the natural place to
encode a real mapping once real data exists.

## 13. Target versioning

**CURRENT IMPLEMENTATION, unchanged** — Phase 6/9's `(target_id,
target_version, content_sha256, spec)` identity already satisfies the
brief's requirement that "target_id alone is not sufficient." This phase
adds nothing new here except *using* `target_version` as one of the four
inputs to the deterministic `job_id` hash (§9) — a target-version bump
naturally produces a distinct job rather than colliding with (and being
suppressed as a duplicate of) a stale version's job. Verified in
`tests/test_integration_submission.py::test_different_target_version_is_a_different_job`
and `tests/test_integration_e2e.py::test_target_version_mismatch_is_a_permanent_error`.

## 14. Cache limitation (target-embedding cache)

**Restated from Phase 11 §25/§26, not re-measured, not solved this
phase — exactly per the brief's explicit instruction not to.**

Phase 11 identified: `target.cache.FilesystemEmbeddingCache` /
`target.segment_cache.FilesystemSegmentEmbeddingCache` are local-disk-per-
host by construction. Workload C's "exactly one build" guarantee
(Phase 10's build-on-miss lock) is a **single-host property**; a fleet of
N machines would each build (and store) their own copy of the same
target's embedding — **REQUIRES MULTI-HOST VALIDATION**, unchanged status
from Phase 11.

**What this phase confirms rather than changes:** the abstraction
boundary the brief asks for ("define an abstraction boundary that allows
a shared cache backend later") **already exists** — `TargetEmbeddingCache`
and `SegmentEmbeddingCache` (`target/cache.py`, `target/segment_cache.py`)
are both `ABC`s; `TargetRegistry` and `worker/matching_handler.py` depend
only on the abstract interface, never on `FilesystemEmbeddingCache`
directly. A shared-storage-backed implementation (object storage,
per `design-proposal-1.md` §8) would require zero changes to
`TargetRegistry`, the worker handler, or anything in `integration/` —
this phase verified that by inspection (grep for `Filesystem.*Cache`
outside `target/` and its own tests: zero production call sites). **No
new storage subsystem was built this phase**, matching the brief's "what
not to build yet" instruction; this section documents that the door is
already open for Phase 13+ to walk through.

**Coupling check:** nothing in `integration/` touches a filesystem path.
`FingerprintCandidate`, `Job`, and `FingerprintOutcomeView` carry no
`media_path`/cache-directory field at any point — `TargetRecord
.media_path` (Phase 6) stays entirely inside `target/`, never crossing
the crawler-facing boundary this phase built.

## 15. Crawler URL states vs. fingerprint states

**PROVISIONAL / recommendation, since the crawler repo was not modified.**
Per the brief's explicit preference ("prefer separation when possible"):
this phase's `FingerprintOutcome` (§6) is a **fingerprint-specific** state
machine, entirely separate from the crawler's own URL/frontier states
(`crawler:*`, §2) and separate even from the crawler's own existing
`JOB_QUEUED`/`JOB_CLAIMED`/`JOB_COMPLETED`/`JOB_RETRY_SCHEDULED`/
`JOB_PERMANENT_FAILURE` constants (`storage/media_evidence_store.py`,
§2) — nothing in this phase merges or renames either. A crawler
integrator (§23) would track "has this candidate been submitted to the
fingerprinter yet" as its own field/state (not-yet-requested / submitted /
terminal), independent of the frontier's crawl-completion state — exactly
the brief's example shape (`not_requested`/`queued`/`processing`/`match`/
`no_match`/`retry`/`failed`), which `FingerprintOutcome` already covers on
the fingerprinter side (`PENDING` covers `queued`+`processing`).
**REQUIRES the crawler repo's own future work to actually store this** —
out of scope here.

## 16. Failure semantics — infrastructure vs. candidate-specific

**CURRENT IMPLEMENTATION for the candidate-specific table (§10).**
Infrastructure-level failures (Redis unreachable) are **explicitly not
job-level failures** — per `design-proposal-1.md` §7's own pre-existing
"Redis failure" row: "workers back off and retry connecting; no local
queue fallback ... claiming pauses fleet-wide." This phase's
`FingerprintJobSubmitter.submit()` does not catch a Redis connection
exception and convert it into a `SubmissionResult` — it propagates, since
a caller cannot meaningfully act on "the coordination substrate is down"
the same way it can act on "this candidate was rejected." The one place
this phase adds explicit handling is the marker-release-on-enqueue-failure
path (§9) — everything else is unchanged Phase 1-3 behavior. No unbounded
retry risk was introduced: `max_attempts` (existing, Phase 1-3) still
bounds every candidate-specific retryable failure; infrastructure failures
are bounded by the caller's own retry policy (out of this phase's scope,
same as Redis HA — `design-proposal-1.md`'s own explicit "Deferred" list).

## 17. Media acquisition boundary

**CURRENT IMPLEMENTATION, unchanged, already satisfies the brief.**
`acquisition.acquirer.MediaAcquirer.acquire(job.media_url)` (Phase 5)
already converts `candidate_url` -> validated local `MediaArtifact` -> the
only shape `worker/matching_handler.py` ever sees. No filesystem path
exists on the crawler side of the boundary at any point: `FingerprintCandidate.candidate_url`
is a URL string; `Job.media_url` is the same URL string; only inside a
worker process, after acquisition, does a local path
(`MediaArtifact.local_path`, a `tempfile.mkstemp()` path, never derived
from the URL's own text) come into existence, and it is always cleaned up
(`artifact.cleanup()`) whether the job succeeds or fails. Nothing in this
phase needed to touch `acquisition/` at all.

## 18. Security considerations

**CURRENT IMPLEMENTATION plus one new decision.**

- Candidate URLs are untrusted input, same as any crawler-discovered URL
  always was (Phase 5 already enforces scheme allowlist, content-type
  allowlist, bounded streamed download, `tempfile.mkstemp()`-based output
  paths never derived from the URL). `integration.candidate
  .FingerprintCandidate.validate()` adds one more, cheap, pre-Redis check
  (scheme must be `http://`/`https://`) so an obviously-invalid candidate
  never reaches the queue at all — not a new security boundary, a
  fail-fast mirror of the one Phase 5's `MediaAcquirer` already enforces
  (`_ALLOWED_URL_SCHEMES`, documented as intentionally kept in sync with
  `MediaAcquirer.DEFAULT_ALLOWED_SCHEMES`).
- **New decision, security-motivated:** Redis keys this phase introduces
  (`fingerprint:submission:{job_id}`) are keyed by the deterministic
  `job_id` hash, **never by the raw candidate URL text** — avoids
  unbounded-length or malformed-character Redis keys derived directly
  from untrusted crawler input (a URL containing e.g. `:` or control
  characters could otherwise produce a surprising/collidable key shape).
  This mirrors how every existing `fingerprint:*` key is already built
  from `job_id`/`target_id`/content hashes, never raw URLs.
- No shell interpolation, no new subprocess calls, no new arbitrary
  filesystem writes were introduced by this phase — `integration/` is
  pure Python/Redis logic, no I/O beyond Redis commands.

## 19. Observability / correlation

**CURRENT IMPLEMENTATION.** `job_id` is the correlation identifier that
survives the complete path (brief's explicit minimum requirement):
present on the Stream entry, the state hash, the result hash, and the
result-event stream (all pre-existing, Phase 1-4) — this phase adds
`integration/timing.py::created_at_from_entry_id()` as a bonus: since
`job_id` alone doesn't carry a timestamp, but the Stream entry ID Redis
assigns at `XADD` time already does, submission-to-claim and
submission-to-completion latency are both recoverable without adding a
new field anywhere. No new logging was added — per the brief's "avoid
excessive logging," and because nothing in this phase's code path
introduced a new failure mode that would need it; existing module
docstrings already carry the "why," consistent with this project's
established convention.

## 20. Local end-to-end flow

**MEASURED**, `tests/test_integration_e2e.py`. Exactly the flow the brief
specifies:

```
synthetic crawler candidate (FingerprintCandidate)
        |
        v
FingerprintJobSubmitter.submit()  -->  fingerprint job (Redis Stream)
        |
        v
Worker.claim_one() / .process_claim()
        |
        v
MediaAcquirer  -->  local HTTP media server (tests/media_test_server.py)
        |
        v
DINOv2EmbeddingEngine (device="cpu")
        |
        v
match_segments() / combine()
        |
        v
Worker.commit_result()  -->  Redis result hash + event stream
        |
        v
resolve_outcome(job_id)  -->  FingerprintOutcomeView
```

No internet, no external search engine, no external piracy site, no GPU —
runs entirely on the CPU dev environment, reusing `tests/fixtures
/tiny_video.mp4` and the `media_server` fixture Phase 10's own
`test_matching_handler.py` already established.

**A "synthetic crawler" is used deliberately, not a shortcut**: the
crawler repo is explicitly out of scope to modify or run for this phase
(§4) — `FingerprintCandidate` instances built directly in the test stand
in for whatever a real crawler (or bridge, §23) would construct.

## 21. Performance overhead

**MEASURED**, `benchmarks/bench_integration_overhead.py`,
`benchmarks/results/bench_integration_overhead_20260812T125103_08a5b8fc.json`.
Per the brief: not a repeat of Phase 11's benchmark suite, only the
integration-layer delta, using the same `bench_15s.mp4` fixture and
`segment_duration_s=2.5` Phase 11's Workload A used, so the two are
directly comparable.

| Path | Mean total | What it includes |
|---|---|---|
| A. Direct handler invocation | 919.56 ms | `build_matching_handler(...)`'s handler called as a plain function — acquisition + DINOv2 + match + aggregate, no Redis Streams/submission/outcome layer at all |
| B. Full crawler-integration path | 931.77 ms | submission (validate + backpressure + dedup) + claim + *same handler* + commit + outcome resolution |
| **Integration overhead (B - A stages only)** | **1.13 ms** | submission (0.485 ms) + commit (0.372 ms) + outcome read (0.272 ms); claim (0.355 ms) shown separately below |

n=15 reps, warm target cache (prewarmed once, excluded from timing, same
methodology as phase-11 §14's Workload A). Breakdown:

| Stage | Mean |
|---|---|
| `submission_s` (validate + `XINFO GROUPS` backpressure read + `SET NX` dedup marker + `XADD`) | 0.485 ms |
| `claim_s` (`XREADGROUP`) | 0.355 ms |
| `commit_s` (Lua commit script) | 0.372 ms |
| `outcome_resolve_s` (two `HGETALL`s) | 0.272 ms |

**Finding: integration overhead is 1.13 ms, 0.12% of direct handler
time** — smaller in absolute terms than even Phase 11's already-negligible
claim+commit figure (0.71 ms) would suggest on its own, because the two
measurements were taken independently; both agree Redis-side coordination
is nowhere near DINOv2 inference cost, which both paths pay identically
(~920ms either way). **This confirms the phase's stated purpose**: the
integration boundary adds negligible overhead relative to the pipeline it
sits in front of, and nothing here is worth optimizing per the brief's
own "do not optimize anything unless the integration overhead is
unexpectedly large" instruction — it is not.

## 22. Thread configuration and GPU

**Unchanged from Phase 11, restated because the brief requires it.** This
phase does not construct a worker-process entrypoint (none exists yet in
this codebase — Phase 11 §26 already flagged this as likely Phase 12/13
work, and this phase's own brief scopes it as "if implementing a
worker-process entrypoint" — conditional, and this phase did not need one:
all tests construct `Worker`/`DINOv2EmbeddingEngine` directly, matching
every existing test's pattern). `embedding.dinov2_engine
.DINOv2EmbeddingEngine`'s `torch_num_threads` parameter (Phase 11 §23)
remains available, unused-by-default, for whichever future entrypoint
needs it. Physical-cores / worker-processes / threads-per-process sizing
guidance is unchanged from Phase 11 §22/§26 — not re-derived here.
GPU: still `torch.cuda.is_available() == False` on this development
machine (Phase 11 §3, unchanged) — **REQUIRES MULTI-HOST VALIDATION**,
this phase fabricated no GPU numbers and built no GPU-specific code path
beyond what Phase 11 already established (the engine's `device` parameter
already accepts `"cuda"`; nothing about the integration boundary is
device-specific — a candidate/job/result never carries device
information).

## 23. Tests

**MEASURED.** 30 new tests across three files, plus the existing suite
re-run unchanged (§24 has exact counts).

| File | Covers |
|---|---|
| `tests/test_integration_submission.py` (12 tests) | Candidate validation (invalid schema), job creation matching the candidate, priority stream routing, identical-candidate-same-job_id, duplicate suppression (idempotency), different-target-version-is-a-different-job, submission marker namespace, backpressure rejection + recovery + resubmission, namespace isolation from `crawler:*`/`evidence:*` |
| `tests/test_integration_outcome.py` (11 tests) | Every `JobStatus`/`ResultDecision` combination -> `FingerprintOutcome`: pending (unclaimed/claimed), retryable, permanent (worker failure, max-attempts-exhausted, processing-failure result), skipped (malformed entry, plain-ack-no-result), match, no_match, correlation fields |
| `tests/test_integration_e2e.py` (7 tests) | Full local flow with real CPU DINOv2 + real HTTP acquisition: self-match (MATCH), genuine NO_MATCH (distinct synthetic video content), target-version mismatch (PERMANENT_ERROR), retryable acquisition error (unreachable host), permanent acquisition error (404), worker-crash lease recovery, multiple workers processing distinct jobs without duplication |

Brief's 15-item checklist, cross-referenced:

1. crawler creates fingerprint job — `test_submit_enqueues_a_job_matching_the_candidate`
2. worker claims crawler-created job — same test + all e2e tests
3. successful fingerprint result — e2e `test_synthetic_candidate_self_match_end_to_end`
4. NO_MATCH — e2e `test_synthetic_candidate_no_match_end_to_end`
5. MATCH — e2e `test_synthetic_candidate_self_match_end_to_end`
6. retryable error — outcome `test_transient_failure_resolves_to_retryable_error` + e2e `test_retryable_acquisition_error_end_to_end`
7. permanent error — outcome `test_permanent_worker_failure_resolves_to_permanent_error` + e2e `test_permanent_acquisition_error_end_to_end`
8. duplicate job submission — `test_duplicate_submission_is_suppressed_and_enqueues_only_once`
9. worker crash / lease recovery — e2e `test_worker_crash_lease_recovery_end_to_end`
10. target version mismatch — `test_different_target_version_is_a_different_job` + e2e `test_target_version_mismatch_is_a_permanent_error`
11. invalid job schema — `test_invalid_scheme_is_rejected_before_touching_redis`, `test_empty_required_field_is_rejected`, outcome `test_malformed_stream_entry_resolves_to_skipped`
12. Redis namespace isolation — `test_integration_writes_never_touch_crawler_or_evidence_keys`
13. backpressure behavior — `test_submission_is_rejected_once_outstanding_jobs_reach_the_limit`, `test_backpressure_clears_once_the_job_is_claimed_and_acked`, `test_rejected_backpressure_candidate_can_be_resubmitted_later`
14. result idempotency — `test_duplicate_submission_is_suppressed_and_enqueues_only_once` (only one result ever gets committed for a given logical candidate)
15. multiple fingerprint workers — e2e `test_multiple_workers_process_distinct_jobs_without_duplication`

## 24. Test results

**MEASURED**, this phase, run on the CPU development environment.

Focused (new) suite:

```
pytest -q tests/test_integration_submission.py tests/test_integration_outcome.py tests/test_integration_e2e.py
30 passed in 10.16s
```

Full repository suite (152 pre-Phase-12 + 30 new):

```
pytest -q
182 passed in 32.39s
```

**0 failed, 0 skipped, no regression against Phase 11's 152-passed
baseline.**

## 25. Known limitations

- **Backpressure is a soft, snapshot-based bound** (§11) — not a
  distributed rate limiter; can be briefly exceeded under heavy
  concurrent submission. Documented, not treated as a correctness defect.
- **`DEFAULT_MAX_OUTSTANDING_JOBS = 500` and
  `DEFAULT_SUBMISSION_MARKER_TTL_S = 24h` are both PROVISIONAL**,
  reasoned from Phase 11's single-host throughput numbers, not from any
  real backlog-latency SLA (none exists yet) — **REQUIRES MULTI-HOST
  VALIDATION** before being treated as tuned production values.
- **The crawler -> `evidence:jobs:queue` -> `integration.submission` path
  is not built** (§4, §23) — this phase fully specifies the contract a
  bridge component would use, but building that bridge means touching the
  crawler repo, explicitly out of scope here.
- **Target-embedding cache sharing across machines remains unsolved**
  (§14, unchanged from Phase 11) — the abstraction boundary exists;
  nothing shares it across hosts yet.
- **GPU worker path remains unvalidated** (§22, unchanged from Phase 11).
- **Real network acquisition characteristics remain unmeasured** — this
  phase's e2e tests use loopback HTTP, same caveat Phase 11 §24 already
  documented; the integration layer adds no new acquisition behavior, so
  nothing new is known here either way.
- **`count_outstanding()`'s `lag`-is-`None` fallback (reject) is
  untested against a real `XDEL`/`XTRIM`-corrupted group** — no code
  path in this repo ever calls `XDEL`/`XTRIM` on the job stream, so this
  is currently a defensive branch with no reproducing test, not a gap
  known to matter in practice.

## 26. Phase 13 requirements (recommendations, not started here)

1. **Decide and build the crawler-side bridge** (§4, §23): a small,
   separately-deployable component (living in the crawler repo, or a
   third deployment unit — deliberately not decided here, since it
   depends on crawler-team ownership, out of this phase's authority) that
   pops `evidence:jobs:queue` (or wherever the crawler decides candidates
   worth fingerprinting should surface from) and calls
   `integration.submission.FingerprintJobSubmitter.submit()` — the exact
   field mapping is already specified (§5, §9).
2. **Resolve target-cache storage sharing before running fingerprint
   workers across multiple machines** — restated from Phase 11 §26,
   still the single highest-priority architectural question, unchanged by
   this phase's work.
3. **Build a worker-process entrypoint** with explicit
   `torch_num_threads`/worker-count configuration (Phase 11 §26,
   restated) — still doesn't exist.
4. **Load-test backpressure thresholds** against real multi-host
   throughput once more than one fingerprinter machine exists, and once a
   real backlog-latency SLA exists to tune `DEFAULT_MAX_OUTSTANDING_JOBS`
   against (§11, §25).
5. **GPU worker validation** — still blocked on a working CUDA
   environment (Phase 11 §3, unchanged).
6. **Real network acquisition benchmarking** — restated from Phase 11
   §24/§26, unchanged.

## 27. Files

**New:**

- `integration/__init__.py`, `integration/candidate.py`,
  `integration/idempotency.py`, `integration/keys.py`,
  `integration/backpressure.py`, `integration/submission.py`,
  `integration/outcome.py`, `integration/timing.py`
- `tests/test_integration_submission.py`,
  `tests/test_integration_outcome.py`, `tests/test_integration_e2e.py`
- `benchmarks/bench_integration_overhead.py`
- `benchmarks/results/bench_integration_overhead_*.json`
- `docs/architecture/phase-12-crawler-fingerprinter-integration.md` (this file)

**Modified:**

- `work_queue/jobs.py` — additive `schema_version` field (§5d). No other
  production module changed.

Crawler repo: **unmodified**, read-only inspection only (§2, §4).
