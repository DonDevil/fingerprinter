# System Architecture

This is the current-state architecture of the fingerprinter, as it exists in
this repository today. It supersedes the phase documents as the place to
understand "how does this system work right now" — the phase documents
(`docs/architecture/phase-*.md`, `docs/architecture/history/`) remain the
detailed, dated engineering record of *how* each piece was built and
*why* specific decisions were made, and are linked from the relevant
sections below.

## 1. What this repository is

The fingerprinter is a standalone, independently-deployable worker fleet
that consumes fingerprint jobs from Redis, downloads the candidate media a
job points at, computes a DINOv2 visual embedding, compares it against a
registered target video's embeddings, and writes back a match/no-match
result. It has no HTTP API and no CLI beyond the worker process itself —
Redis Streams is the entire input and output surface.

It is one of two independent repositories in the anti-piracy project:

- **`crawler`** (sibling repository, separate git history, not part of this
  repository) — discovers candidate URLs, crawls them, and records media
  **evidence**.
- **fingerprinter** (this repository) — consumes fingerprint jobs and
  determines whether a piece of evidence matches a registered target.

See [§9, Relationship to the crawler](#9-relationship-to-the-crawler-repository)
for exactly how the two communicate (and, as of today, how they mostly
don't yet).

## 2. Pipeline overview

```text
Redis Stream (fingerprint:jobs:stream:{priority})
        │  XREADGROUP (consumer group "fingerprinter-workers")
        ▼
   Worker.claim_one() / reclaim_stale()
        │  hands a validated Job to the handler
        ▼
   worker/matching_handler.py: build_matching_handler(...)
        │
        ├─ 1. MediaAcquirer.acquire(job.media_url)        (acquisition/)
        │      → validated local MediaArtifact
        │
        ├─ 2. DINOv2EmbeddingEngine.embed_video_segments() (embedding/)
        │      → candidate segment embeddings + coarse vector
        │
        ├─ 3. TargetRegistry.get_or_build_segment_embedding() (target/)
        │      → target segment embeddings (cache-first, build-on-miss
        │        under a Redis lock; may itself call the embedding engine
        │        against the target's own media)
        │
        ├─ 4. matching.matcher.match_segments()            (matching/)
        │      → TemporalMatchResult
        │
        └─ 5. matching.aggregation.combine()                (matching/)
               → work_queue.results.Result (match / no_match /
                 processing_failure)
        ▼
   Worker.commit_result() — atomic: result hash + job state + XADD onto
   the results stream + XACK, all in one Redis script
```

Every stage above is timed independently when observability is enabled —
see [§8](#8-observability).

## 3. Redis Streams job model

Redis is the *only* coordination surface between job producers (the
crawler, or any other future producer) and this fleet's workers. There is
no other database, no shared filesystem assumption for coordination, and
no direct Python import across a process boundary.

### Why Redis Streams + consumer groups

A Stream (`fingerprint:jobs:stream:{priority}`) is an append-only log;
a **consumer group** (`fingerprinter-workers`, fixed, one group for the
whole fleet — `work_queue/keys.py`) tracks, per entry, which consumer
claimed it and whether it has been acknowledged. This gives the fleet:

- **At-least-once delivery.** `XREADGROUP ... >` claims a new entry and
  adds it to the group's **PEL** (Pending Entries List) — the entry is
  not removed from the stream, only marked "owned by this consumer, not
  yet acked." If the worker crashes before acking, the entry is still in
  the PEL for another consumer to pick up later.
- **Crash recovery via `XAUTOCLAIM`.** Every worker's run loop periodically
  (`reclaim_interval_ms`, defaulting to `lease_ms`) calls `XAUTOCLAIM` for
  any PEL entry idle longer than `lease_ms` — i.e. claimed by *some*
  consumer (possibly one that has since died) longer ago than a lease is
  allowed to last. `XAUTOCLAIM` reassigns it to the calling consumer and
  bumps a Redis-tracked delivery counter. This is the *lease* mechanism:
  there is no heartbeat and no explicit lease-renewal call — a job is
  simply "at risk of reclaim" once it's been claimed longer than
  `lease_ms` without being acked.
- **Fencing via an application-level attempt counter.** Redis's own PEL
  delivery counter is not, by itself, enough to guarantee a stale worker
  can never finalize a job out from under whoever reclaimed it. This
  project layers its own `attempt` counter on top (`work_queue/state.py`,
  `JobStateStore`), incremented on every claim/reclaim and written to
  `fingerprint:job:{job_id}:state`. Every terminal Lua script in
  `worker/fingerprint_worker.py` (`_COMPLETE_IF_CURRENT`,
  `_SCHEDULE_RETRY_IF_CURRENT`, `_FAIL_IF_CURRENT`,
  `_COMMIT_RESULT_IF_CURRENT`) starts with a compare-and-swap: it only
  proceeds (write state, XADD a result, XACK) if the `attempt` it was
  called with still matches what's on record. A worker that lost
  ownership via `XAUTOCLAIM` holds a stale `attempt` and every one of
  these calls becomes a safe no-op for it — critically, it also never
  reaches the `XACK`, so a stale worker can never remove the new owner's
  still-in-flight PEL entry.

### Lifecycle of one job

1. **Enqueue** — a producer (`work_queue.producer.JobProducer`, or the
   higher-level `integration.submission.FingerprintJobSubmitter`, see
   [§9](#9-relationship-to-the-crawler-repository)) `XADD`s a `Job`
   (`work_queue/jobs.py`) onto `fingerprint:jobs:stream:{priority}`. This
   is fire-and-forget from the producer's point of view; nothing about
   claim/lease/retry exists yet.
2. **Claim** — `Worker.claim_one()` blocks on `XREADGROUP` for up to
   `block_ms`. A malformed entry (fails `Job.from_stream_fields`
   validation) is rejected and `XACK`ed immediately — it can never become
   valid through redelivery, so there is no retry path for it. A valid
   entry increments `attempt` in `JobStateStore` and is handed to the
   handler.
3. **Process** — the handler (`worker/matching_handler.py`) runs the
   pipeline in [§2](#2-pipeline-overview) and returns a `Result`, or
   raises `TransientFailure`/`PermanentFailure`
   (`worker/fingerprint_worker.py`).
4. **Finalize** — exactly one of:
   - **Commit** (`commit_result`): result hash write + job state
     `completed` + `XADD` onto `fingerprint:results:stream:{priority}` +
     `XACK`, atomically, gated by the attempt-fencing check above.
   - **Retry** (`TransientFailure`, `attempt < job.max_attempts`): job
     state set to `retry_scheduled`, a JSON-encoded copy of the job's
     stream fields is `ZADD`ed into `fingerprint:retry:delayed:{priority}`
     with score = `now + backoff`, and the original entry is `XACK`ed —
     also atomic, also fenced. Backoff is exponential:
     `base_delay_s * 2^(attempt-1)`, capped at `max_delay_s` (defaults
     1s base, 60s cap).
   - **Permanent failure** (`PermanentFailure`, or a `TransientFailure`
     whose `attempt >= job.max_attempts`): job state set to `failed`,
     original entry `XACK`ed. No further processing.
   - **Reject** (malformed entry): handled entirely inside `claim_one`/
     `reclaim_stale`, before a handler ever runs.
5. **Retry promotion** — any worker, on every loop iteration, calls
   `promote_due_retries()`: `ZRANGEBYSCORE` the retry ZSET for members due
   by now, and for each, an atomic `ZREM`-then-`XADD` script moves it back
   onto the live stream. The `ZREM`'s return value is the atomic "claim"
   of that promotion — a second worker racing the same promotion sees
   `removed == 0` and does nothing, so a retry is never promoted twice.
6. **Reclaim** — see "Crash recovery via `XAUTOCLAIM`" above. A reclaimed
   entry goes through exactly the same `process_claim` finalize path as a
   fresh claim (`Worker._maybe_reclaim_stale`), so there is only one
   finalize code path in the whole system, not two.

### Malformed-job handling

A stream entry that fails `Job.from_stream_fields` validation (missing
required field, non-integer `max_attempts`, empty `techniques`, or a
`schema_version` present but not equal to the one this worker
implements) is recorded as `rejected` in `JobStateStore` and `XACK`ed
immediately, whether encountered on first claim or via `XAUTOCLAIM`. It
is never retried — malformed data does not become well-formed by being
redelivered — and it is not silently dropped either: `rejected` state is
durable and observable (`on_job_rejected`, see
[§8](#8-observability)).

## 4. Job and result schemas

- **Job** (`work_queue/jobs.py`) is immutable once enqueued — a stream
  entry *is* the job spec. Required fields: `job_id`,
  `media_evidence_id`, `media_url`, `media_type`, `source_domain`,
  `target_id`, `target_version`, `techniques`, `max_attempts`, plus an
  additive `schema_version` (absent = schema 1, the only schema that has
  ever existed; present-but-mismatched is rejected outright rather than
  guessed at).
- **Result** (`work_queue/results.py`) is a three-way decision —
  `match`, `no_match`, or `processing_failure` — deliberately never
  collapsed to a boolean, since "the pipeline could not reach a
  determination" (corrupt media, an embedding failure on the candidate)
  is not the same fact as a confident non-match. `ResultRecord` adds the
  job-queue context (`attempt`, `worker_id`, ...) a handler doesn't
  itself compute, and is what actually gets written to
  `fingerprint:result:{job_id}` and summarized onto
  `fingerprint:results:stream:{priority}`.

## 5. Matching pipeline detail

### Media acquisition (`acquisition/`)

`MediaAcquirer.acquire(url)` downloads a candidate to a local temp file:
scheme allowlist (`http`/`https` only), bounded redirects (default 5),
streamed-to-disk with a hard byte cap enforced against actual bytes
written (never `Content-Length`), then `ffprobe`-based validation
(`acquisition/validation.py`) that the bytes actually decode as media.
See [§7](#7-ssrf--outbound-fetch-security) for the destination-safety
checks layered into this same call path.

### Embedding (`embedding/`)

`DINOv2EmbeddingEngine` (`embedding/dinov2_engine.py`) wraps
`facebook/dinov2-base` (768-dim ViT-B/14), pinned to a fixed model
revision so weights can't silently drift under a running deployment.
Images embed as a single frame; video is decoded via `ffmpeg` into a
deterministic frame sequence (`embedding/frames.py`) and pooled.

For the matching path specifically, `embed_video_segments` produces
**segment embeddings**: the video is chunked into fixed-duration segments
(`SegmentSamplingConfig.segment_duration_s`, default 5.0s, one
representative frame per segment via `ffmpeg -vf fps=1/segment_duration_s`)
plus one whole-video coarse (mean-pooled) vector. Segment-level
representation is what makes partial-clip / temporal-offset matching
possible at all — a single pooled vector for an entire target movie
cannot localize where in that movie a short clip came from. See
`docs/architecture/history/phase-08-video-representation-investigation.md`
and `docs/architecture/history/phase-09-temporal-video-matching.md` for
the investigation and design behind this.

### Target registry and cache (`target/`)

`TargetRegistry` (`target/registry.py`) is Redis-backed metadata (target
identity, content hash, small per-spec cache summaries — never vector
data in Redis itself) composed with an injected embedding cache
collaborator. `get_or_build_segment_embedding` is the cache-first,
build-on-miss-under-lock resolution:

1. Check the cache; return immediately on a hit (no lock touched).
2. On a miss, try to acquire a Redis `SET NX PX` lock
   (`target/lock.py`, `RedisLock`) scoped to this exact
   `(target_id, target_version, content_sha256, spec)`.
3. **Winner:** double-checks the cache (another worker may have finished
   between the first check and winning the lock), builds on a second
   miss by calling back into the embedding engine, registers the result,
   releases the lock in a `finally`.
4. **Loser:** polls the cache every `poll_interval_s` (default 1s) until
   the winner's result appears or `poll_timeout_s` elapses (default
   600s), then raises `TimeoutError` rather than duplicating the build or
   blocking forever. A `TimeoutError` here is mapped to `TransientFailure`
   by the handler — a retry may simply land after the winner finishes.

The lock does not auto-extend; a build that runs longer than its TTL
(default 10 minutes) loses the lock and a second worker can start a
redundant build. See `target/lock.py`'s module docstring.

### Matching (`matching/`)

`matching.matcher.match_segments` compares candidate segment embeddings
against target segment embeddings under `MatcherConfig`
(`matching/config.py`) — cosine similarity thresholds, a minimum run
length of temporally-consistent matched segments, offset tolerance, and
max index gap. **Every threshold in `MatcherConfig` is a PROVISIONAL
HEURISTIC, not calibrated against a labeled dataset** — no such dataset
exists in this project yet. See `matching/config.py`'s module docstring
and `docs/architecture/history/phase-09-temporal-video-matching.md`,
"Threshold status," before treating any default there as validated.

### Aggregation (`matching/aggregation.py`)

`combine()` folds one or more technique-specific match results (today:
only the DINOv2 temporal result) into a single `Result`, building the
`evidence` JSON field the result schema reserves for per-technique
detail. This is the seam a future second technique (audio, pHash,
watermark) would fold into — architecture only, nothing beyond DINOv2 is
implemented today.

## 6. Distributed target artifact storage (Phase 13D)

A Redis lock correctly serializes *who builds* a target's segment
embedding fleet-wide — but the artifact that lock protects was, before
Phase 13D, stored on whichever host's local disk happened to build it
(`FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache`,
`target/cache.py`/`target/segment_cache.py`). A losing host's poll loop
could win the *coordination* race and still never observe the winner's
result, because the result never left the winner's disk. A second,
related gap: `TargetRecord.media_path` is itself host-local, so even a
fixed embedding cache would not let a losing host build the target at
all if the raw target media never reached it.

`target/shared_storage.py`'s `SharedArtifactStore` is the fix: a generic,
content-addressed blob store backed by a directory that **must be a
genuinely shared mount** (NFS, a cluster filesystem, or equivalent)
across every worker host. It is deliberately the smallest abstraction
that solves this without inventing infrastructure this project has no
evidence of (no S3-compatible client is a dependency today).

- **`shared_artifact_store_path` unset (the default):** every worker
  behaves exactly as before Phase 13D — host-local
  `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` under
  `target_cache_path`. **Not multi-host-safe.** Fine for a single-host
  deployment or local development.
- **`shared_artifact_store_path` set:** every worker switches to
  `SharedFilesystemEmbeddingCache`/`SharedFilesystemSegmentEmbeddingCache`
  (embeddings) and `SharedTargetMediaStore` (raw target media, published
  at target-registration time), both backed by the same
  `SharedArtifactStore`. Content-addressed by `content_sha256` /
  `target.versioning.cache_entry_key()` — pure functions of
  target/spec identity, no hostname/PID/timestamp — so two hosts
  computing a key for the same logical artifact always agree.
- **Failure semantics matter:** a shared-store read/write that fails
  because the mount is unreachable raises `SharedArtifactStoreError` — it
  is never silently treated as a cache miss, and this module never falls
  back to a local-only directory and calls itself distributed anyway.
  `worker/matching_handler.py` maps that exception onto
  `TransientFailure`, retried through the existing job retry machinery.

**Validation status: IMPLEMENTED — SIMULATED MULTI-HOST (multiple
`TargetRegistry` instances against one Redis, pointed at one shared
directory, inside a single test process). Real multi-host deployment
(genuinely separate machines, a genuinely shared network filesystem)
REQUIRES MULTI-HOST VALIDATION that has not been performed.** See
`docs/architecture/phase-13d-multi-host-target-cache-audit.md` (the
audit that found the gap) and
`docs/architecture/phase-13d-distributed-target-artifacts.md` (the
implementation and its simulated multi-host test) for the full detail.
Do not describe this as validated against real separate hosts anywhere
downstream of this document.

## 7. SSRF / outbound fetch security

`acquisition/ssrf_guard.py` closes the gap where a job's `media_url` (or
a redirect target reached through it) could point the acquirer at
internal infrastructure. It is invoked on the initial URL and on every
redirect hop, before that hop is fetched.

- **Scheme allowlist:** `http`/`https` only (`MediaAcquirer._check_scheme`).
- **Resolved-address validation:** before connecting, the hostname is
  resolved via `socket.getaddrinfo` and every returned address is checked
  against `is_unsafe_address` — rejected if loopback, private (RFC 1918),
  link-local, unspecified, multicast, IANA-reserved, or in
  `100.64.0.0/10` (RFC 6598 carrier-grade NAT space, not classified as
  private by the stdlib `ipaddress` module but never a legitimate public
  destination). A hostname with multiple A/AAAA records is rejected if
  *any* resolved address is unsafe, since nothing here controls which
  record the real HTTP connection ends up using.
- **Redirects re-checked, not trusted:** each hop re-resolves and
  re-validates independently — an externally-reachable URL that 302s to
  an internal address is rejected at the redirect hop, not followed.
- **Content bounds:** a hard byte cap enforced against bytes actually
  written (not `Content-Length`), a bounded redirect count, and
  `ffprobe`-based validation that the downloaded bytes actually decode as
  media.
- **`allow_private_networks`** is a narrow, explicit constructor opt-out
  for test/dev fixtures. It must never be the production default and
  `worker/main.py` never sets it.

**Known, deliberately unsolved limitation — DNS rebinding / TOCTOU.**
This module re-resolves DNS itself immediately before each connection
attempt, but `requests`/urllib3 performs its own, independent resolution
a moment later when it actually opens the socket. An attacker who
controls DNS for the candidate's hostname and changes the answer between
those two lookups is **not** defeated by this check alone. Fully closing
that gap requires pinning the actual connection to the address validated
here (a custom transport adapter) — a materially larger change,
deliberately out of scope for the current implementation. Do not
describe DNS rebinding as solved anywhere downstream of this document.
See `acquisition/ssrf_guard.py`'s module docstring and
`docs/architecture/phase-13-production-hardening.md`, "Phase 13A," for
the full reasoning.

## 8. Observability

`worker/observability.py` (Phase 13C) turns the worker's existing
claim/reclaim/reject/complete/fail/retry lifecycle boundaries into:

- **Structured JSON logs** — one JSON object per line
  (`worker.observability.JsonFormatter`), every event carrying
  `hostname`/`pid`/`consumer_name`/`namespace` for fleet-wide
  attribution once logs are shipped somewhere that aggregates them (this
  repository does not ship a log aggregator or dashboard — see the task
  scope note at the top of `docs/architecture/phase-13-production-
  hardening.md`, "Phase 13C").
- **Process-local counters** (`WorkerCounters`): jobs claimed / reclaimed
  / completed / failed / rejected / retried / permanently-failed, active
  jobs, total attempts.
- **Bounded-sample latency stats** (`BoundedLatencyStats`): exact
  count/min/max/avg plus p50/p95/p99 approximated from the most recent
  2000 samples (not every sample ever recorded — deliberately bounded
  memory, resets on worker restart).
- **Per-pipeline-stage timing** (`record_stage_duration`, called from
  `worker/matching_handler.py`): `media_acquisition`,
  `candidate_embedding`, `target_resolution`, `matching`, `aggregation`.
  `target_resolution` times the whole cache-lookup-or-build call — cache
  hit and build-on-miss time are not currently separable without
  modifying `target/registry.py`, a known, documented measurement gap.
- **Periodic health summaries** (`worker_health` events, every
  `WORKER_OBSERVABILITY_INTERVAL_MS`): counters, average/p95 completion
  latency, a bounded Redis Streams health snapshot (`XLEN`, `XINFO
  GROUPS`/`CONSUMERS`, one `XPENDING` range read — no `SCAN`, no
  per-job walk), and process resource metrics (RSS current/peak, CPU
  time, instantaneous CPU%, thread count, open FD count — stdlib only,
  no `psutil` dependency).
- **A machine-readable run record**, written atomically on shutdown to
  `WORKER_RUN_OUTPUT` if set — full configuration snapshot, timing,
  counters, per-error-category counts, latency stats, per-stage metrics,
  a final Redis health snapshot, resource metrics, and shutdown
  reason/cleanliness. A lightweight `<path>.marker` file is written at
  startup and removed only on a clean run-record write — an external
  tool can treat "marker present, no matching clean run record newer
  than it" as "worker started but did not shut down cleanly" (e.g.
  `SIGKILL`, power loss) without any Redis-side state.

All of this is **worker-local** (this process only, since process start).
Fleet-wide aggregation across workers/hosts is explicitly out of scope —
the schema is shaped to make a later aggregation step straightforward,
not to perform one. See [§16 of the Phase 13 doc](phase-13-production-hardening.md)
and `docs/usage.md` for how to enable and read this output.

## 9. Relationship to the crawler repository

The crawler (`/home/darkdevil/Desktop/anti_piracy/crawler` in this
project's layout — a separate git repository, independent deployment,
**not** part of this repository) discovers candidate URLs and records
**media evidence**: a URL plus context/metadata. Its own README describes
a production architecture where a shared Redis instance coordinates both
its URL frontier (`crawler:*` namespace) and its media evidence store
(`evidence:*` namespace), and states plainly that fingerprinting is "not
implemented in this repository. It is a separate, not-yet-built project."

**This repository is that separate project — but the two are not wired
together today.** Specifically:

- The crawler's own evidence store maintains its own fingerprint-job
  queue (`evidence:jobs:queue`, a Redis ZSET with Lua-scripted
  claim/lease/CAS, mirroring the frontier's own coordination model) that
  the crawler team's documentation describes a future fingerprinter
  consuming directly via their Python API
  (`claim_next_fingerprint_job()` and friends).
- This repository's founding design document
  (`docs/design/design-proposal-1.md`) independently and explicitly
  excludes "direct crawler Python imports" and "shared-filesystem
  assumptions between crawler and fingerprinter" from production —
  written before the above was discovered, and still the governing
  constraint.
- **The resolution:** this repository's own Redis Streams job contract
  (`fingerprint:jobs:stream:*`, `work_queue/`, unchanged since the
  earliest phase) remains the *sole* input surface a producer talks to.
  `integration.submission.FingerprintJobSubmitter` (`integration/`,
  built in the Phase 12 work) is the intended entry point for any
  producer — crawler or otherwise — that wants idempotent,
  backpressure-aware submission rather than talking to
  `work_queue.producer.JobProducer` directly:
  - **Idempotency:** `job_id` is a deterministic hash of
    `(candidate_url, target_id, target_version, techniques)`
    (`integration.idempotency.derive_job_id`); a `SET NX EX` marker makes
    "has this exact unit of work already been submitted" a single atomic
    Redis round-trip. The marker's TTL (24h, PROVISIONAL) is deliberately
    generous relative to every other retry/backoff/lock timeout
    configured in this project.
  - **Backpressure:** `count_outstanding` reads `lag + pending` from the
    same consumer-group data `Worker.reclaim_stale()` already relies on
    (no new bookkeeping structure), and submission is rejected once
    outstanding work reaches `DEFAULT_MAX_OUTSTANDING_JOBS` (500,
    PROVISIONAL, not load-tested against real multi-host throughput —
    see `integration/backpressure.py`'s module docstring).
- **What does not exist today:** a bridge component that reads the
  crawler's `evidence:jobs:queue` and calls `FingerprintJobSubmitter.
  submit()` on its behalf. This was deliberately deferred — it requires
  touching the crawler repository (to drain its queue) and belongs, by
  this project's own precedent, to whoever owns crawler deployment,
  calling this repository's public `integration/` API rather than either
  repository importing the other's internals. **No such bridge exists
  in either repository as of this writing.** Until it does, jobs must be
  submitted to this fleet via `FingerprintJobSubmitter`/`JobProducer`
  from something other than the crawler's own queue-draining logic (a
  manual script, a test producer, or a future bridge process).
- **Result correlation, not evidence-store writes:** `FingerprintCandidate.
  media_evidence_id` (`integration/candidate.py`) is carried through to
  every `Result`/`ResultRecord` this fleet produces, purely as an opaque
  reference back to whatever the crawler (or another caller) wants to
  update — an `evidence:asset:{aid}` hash in the crawler repository, for
  instance. This repository never reads or writes anything in the
  crawler's Redis namespace; it only carries the identifier through.

**Namespace separation (for operators sharing one Redis instance across
both systems):** this repository uses the `fingerprint:*` key prefix
exclusively (`work_queue/keys.py`, `target/keys.py`,
`integration/keys.py`). The crawler uses `crawler:*` (URL frontier) and
`evidence:*` (media evidence + its own job queue). No key collisions
exist between the two prefixes. Test suites also use disjoint logical
Redis databases by convention: this repository's tests default to
`redis://localhost:6379/15`, its benchmarks to `/14`; consult the crawler
repository's own documentation for its conventions before running both
test suites against the same Redis server.

## 10. What is and isn't validated

See the [production status table](../../README.md#production-status) in
the root README for the authoritative, up-to-date summary. In short: the
single-host pipeline (acquisition → embedding → matching → aggregation →
result commit), the Redis job/retry/crash-recovery contract, and SSRF
hardening are implemented and covered by an automated test suite. Matcher
thresholds are provisional heuristics, not calibrated against labeled
data. Multi-host target-artifact sharing is implemented and covered by a
simulated multi-host test, not a real multi-host deployment. GPU
execution is implemented by code inspection only and has never been run
against real CUDA hardware in this project's own benchmarking. None of
this should be read as "production-validated" merely because unit tests
pass — see the status table for the precise claim being made about each
piece.
