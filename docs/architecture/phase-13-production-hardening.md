# Phase 13 — Production-Readiness Audit

## 0. Scope

This is an **audit only**. No production code was modified. The task was
to inspect the current source and tests (not the Phase 1-12 documentation
alone) and determine, for each of six areas, whether a Phase 11/12-flagged
risk is real, whether it is a production blocker, and what evidence
supports that conclusion.

**Git revision audited:** `59f2938` ("phase 12"), working tree clean at
audit start. No commits were made during this phase.

**Method:** direct inspection of every production module under
`acquisition/`, `embedding/`, `matching/`, `target/`, `work_queue/`,
`worker/`, `integration/`; inspection of the corresponding tests; one full
test-suite run (evidence, not a new benchmark); one interpreter check of
`torch.cuda.is_available()`. No new code, no new tests, no crawler-repo
access (not needed — none of the six areas required it), no fabricated
numbers.

**Labels used throughout**, per the audit brief:

- **MEASURED** — directly observed this session (test run, interpreter
  check, `grep`/`find` inventory).
- **INFERRED** — a logical consequence of reading the actual source code,
  not independently executed (e.g. "worker X blocks for Y seconds" derived
  from reading the call chain and its default arguments).
- **PROVISIONAL** — a judgment call with a stated rationale but no load
  data behind it (mostly inherited from Phase 11/12, not re-derived here).
- **REQUIRES MULTI-HOST VALIDATION** — cannot be resolved by single-host
  code inspection; needs a real multi-machine environment.
- **DEFERRED** — explicitly out of this phase's scope.

Classification vocabulary (assigned to every finding below): **PRODUCTION
BLOCKER**, **CORRECT**, **ACCEPTABLE LIMITATION**, **OPERATIONAL
FOLLOW-UP**, **REQUIRES MULTI-HOST VALIDATION**, **FUTURE OPTIMIZATION**.

## 1. Current production architecture (as it exists today, unchanged)

Unchanged from `docs/architecture/phase-12-crawler-fingerprinter-integration.md`
§3/§7 — restated only as the baseline this audit measures against:

| Layer | Module | State |
|---|---|---|
| Job contract | `work_queue/jobs.py` | Complete, schema-versioned |
| Producer | `work_queue/producer.py`, `integration/submission.py` | Complete |
| Worker | `worker/fingerprint_worker.py` | Complete as a *library class* — no process wrapper |
| Handler | `worker/matching_handler.py` | Complete |
| Acquisition | `acquisition/` | Complete |
| Embedding | `embedding/` | Complete, CPU validated, GPU code path present/unvalidated |
| Target cache | `target/` | Complete, single-host only (§3 below) |
| Backpressure/idempotency | `integration/` | Complete |
| **Worker process entrypoint** | — | **Does not exist. Verified by inventory (§2).** |
| **Observability/operator tooling** | — | **Does not exist. Verified by inventory (§7).** |

## 2. Worker/deployment audit

**MEASURED (file inventory):**

```
find . -iname "*entrypoint*" -o -iname "*__main__*" -o -iname "run_worker*" -o -iname "worker_main*"
  -> (no matches outside old/)
grep -rl "if __name__" **/*.py
  -> only benchmarks/*.py (bench_embedding, bench_pipeline, bench_matching, bench_integration_overhead, gen_test_video)
grep -rl "SIGTERM\|SIGINT\|signal\." **/*.py
  -> only a comment ("... without any local signal.") in embedding/dinov2_engine.py — zero actual signal handling anywhere
```

There is no script, module, or packaging artifact anywhere in this repo
(outside `benchmarks/`, which are throwaway measurement harnesses, and
`old/`, the pre-Phase-1 prototype) that starts a `Worker`, runs its `run()`
loop, and keeps a process alive against real traffic. This is not a
regression — Phase 11 §26 and Phase 12 §22/§26 both already named it as
unbuilt — but this audit is the first to look at it head-on rather than as
a footnote.

**Process model — PRODUCTION BLOCKER.** `worker.fingerprint_worker.Worker`
is a well-built library class (`run()`, `stop()`, `claim_one()`,
`reclaim_stale()`, `promote_due_retries()` are all correct and covered by
`tests/test_worker.py`, `tests/test_crash_recovery.py` — see below). But a
library class is not a deployable unit. Nothing in this repo:
- constructs a `Redis` client with production-appropriate connection
  settings (pool size, `retry_on_timeout`, `health_check_interval` — none
  of these are referenced anywhere in production code; every call site
  that builds a `Redis` client is a test or a benchmark);
- constructs a `DINOv2EmbeddingEngine`/`MediaAcquirer`/`TargetRegistry`
  with production configuration (model path, cache directory,
  `torch_num_threads`) from environment/config rather than hand-written
  Python;
- calls `worker.run(handler)` and keeps the process alive;
- reads `torch_num_threads` from anything (env var, CLI flag, config
  file) — the parameter exists on `DINOv2EmbeddingEngine` (Phase 11) but
  nothing outside `benchmarks/bench_pipeline.py`'s measurement harness
  ever sets it.

**Worker identity — CORRECT, as far as it goes.**
`default_consumer_name()` (`worker/fingerprint_worker.py:178`) returns
`worker-{hostname}-{pid}-{thread_ident}` — unique per host+process+thread,
which is exactly what Redis Streams' consumer-group model needs for
`XREADGROUP`/`XAUTOCLAIM` to attribute PEL entries correctly.
`tests/test_worker.py::test_multiple_workers_do_not_receive_the_same_job_simultaneously`
and `tests/test_crash_recovery.py::test_multiple_workers_do_not_repeatedly_steal_a_live_reclaimed_job`
verify this holds for two `Worker` instances sharing one Redis client.
This is a correct primitive; it is simply never exercised by a real
multi-process/multi-host deployment because none exists yet.

**Graceful shutdown — CORRECT for what's built, incomplete for
production.** `Worker.stop()` sets a `threading.Event`; `run()` checks it
between each of the three phases of its loop
(`_maybe_reclaim_stale` / `promote_due_retries` / `claim_one`), so a
`stop()` call lets the current blocking `XREADGROUP` (bounded by
`block_ms`, default 5000ms) finish and then exits without acking
in-flight, unfinished work —
`tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work`
and `tests/test_worker.py::test_graceful_worker_shutdown` both verify
this directly. **What's missing is the wiring that would call `stop()` at
all**: no `signal.signal(SIGTERM, ...)` / `signal.signal(SIGINT, ...)`
handler exists anywhere, because there is no process for a signal to be
delivered to. `Worker.stop()` is a sound building block for a graceful
shutdown a future entrypoint would need to wire up in ~10 lines
(`signal.signal(signal.SIGTERM, lambda *_: worker.stop())`); it is not
itself a complete shutdown story.

**Startup/shutdown failure handling — OPERATIONAL FOLLOW-UP (not a
blocker, but real).** `Worker.__init__` calls `_ensure_group()`
(`xgroup_create`) synchronously; if Redis is unreachable at construction
time, the exception propagates uncaught. `DINOv2EmbeddingEngine.__init__`
similarly lets `ModelLoadError`/`DeviceUnavailableError` propagate. Both
are reasonable "fail fast at startup" behaviors *in principle*, but
without an entrypoint there is no defined retry/backoff/crash-loop policy
around them, and no readiness signal (health check endpoint, PID file,
`/ready`) an orchestrator (systemd, Kubernetes) could use. This is
explicitly deferred, not solved, by `design-proposal-1.md`'s own scope
("Redis HA" listed as deferred there too) — consistent, not a new gap.

**Torch thread configuration / CPU oversubscription — PRODUCTION BLOCKER
if multiple worker processes are ever run per host without an
entrypoint.** `DINOv2EmbeddingEngine.torch_num_threads` (Phase 11) is a
real, tested fix — but it is a constructor parameter nobody calls with a
non-default value in any production path, because there is no production
path. Phase 11 §19a measured the failure mode this guards against as
severe (~15x throughput collapse from N processes x torch-default threads
oversubscribing physical cores) — INFERRED to still apply unchanged,
since `embedding/dinov2_engine.py` is byte-for-byte unmodified from what
Phase 11 measured against. **This is a footgun, not a fixed problem**:
the fix exists in the constructor's parameter list, but nothing forces or
even guides a future entrypoint's author to actually pass it. Until an
entrypoint exists that plumbs `torch_num_threads` from
worker-count-per-host down to every `DINOv2EmbeddingEngine` it
constructs, "run more than one worker process per host" silently
reproduces Phase 11's worst-measured regression.

**Redis connections — OPERATIONAL FOLLOW-UP.** Every production module
(`Worker`, `TargetRegistry`, `JobProducer`, `FingerprintJobSubmitter`,
`integration.backpressure.count_outstanding`) takes an already-constructed
`redis.Redis` as a constructor/function argument — clean dependency
injection, verified by grep (zero `Redis(...)`/`Redis.from_url(...)`
call sites outside `tests/` and `benchmarks/`). This is good design for
testability but means connection-pool sizing, TLS, auth, and
reconnect/retry policy are 100% unowned by this codebase today — entirely
a future entrypoint's responsibility. Not a blocker in the sense of
"broken code" (there is no broken code here — there is no code at all in
this spot), but it is one of the concrete gaps #3 in §12 (Required Fixes)
must close.

## 3. Target-cache audit (multi-host)

**MEASURED (code inspection, `target/lock.py`, `target/registry.py`,
`target/cache.py`).** This is the audit's most consequential finding, and
it sharpens — not just confirms — Phase 11 §25/26 and Phase 12 §14.

**The build-on-miss lock itself is genuinely fleet-wide.**
`target.lock.RedisLock` is `SET key token NX PX ttl_ms` / a Lua
compare-and-delete release — both are ordinary Redis commands with no
locality assumption; two processes on two different hosts talking to the
same Redis instance contend for the same key exactly as two threads in
one process do. `tests/test_target_lock.py` and
`tests/test_target_build_on_miss.py::test_concurrent_miss_builds_only_once`
verify the *locking* mechanism correctly serializes builders (via two
threads sharing one Redis client, which is sufficient to prove the Redis
side of the mechanism — Redis does not distinguish "two threads" from
"two hosts"). **CORRECT**, as a Redis primitive.

**But the thing the lock is supposed to protect is not fleet-wide, and
this defeats the lock's actual purpose across hosts.** Trace
`TargetRegistry.get_or_build_segment_embedding`
(`target/registry.py:188-259`):

1. Cache check (`get_compatible_segment_embedding`) → calls
   `self._segment_cache.get(...)` directly.
2. Lock winner: builds, calls `register_segment_embedding`, which does
   **two** writes — `self._segment_cache.put(...)` (the vector data) and
   a small Redis hash write (`target_segment_embeddings_key`, vector-free
   metadata only, per `target/cache.py`'s own storage-boundary docstring).
3. Lock loser: polls `get_compatible_segment_embedding` in a loop —
   **the exact same call as step 1**, which reads `self._segment_cache.get(...)`
   and **never reads the Redis metadata hash written in step 2 at all** —
   `get_compatible_segment_embedding`'s implementation
   (`target/registry.py:150-161`) has no code path that touches
   `target_segment_embeddings_key`.

`self._segment_cache` in every current wiring is
`target.segment_cache`'s filesystem-backed implementation (mirroring
`FilesystemEmbeddingCache`, `target/cache.py:86-187`) — **one JSON file
per cache entry on local disk.** On a single host, steps 1-3 all read the
same filesystem, so this works exactly as designed. **On two different
hosts, step 3's read can never see step 2's write** — host B's local disk
does not contain the file host A just wrote. The Redis metadata hash
Phase 6-9 write on every `register_embedding`/`register_segment_embedding`
call exists (INFERRED, by design, per its own docstring) purely as a
"what's cached" index for observability/debugging — nothing in
`TargetRegistry` ever reads it back to answer a hit/miss question.

**Consequence, INFERRED from the code (not yet MEASURED against a real
second host — REQUIRES MULTI-HOST VALIDATION to confirm timing, but the
qualitative behavior follows directly from the call chain above):** on a
losing host, `get_or_build_segment_embedding` does not "wait briefly, then
transparently reuse the other host's result" — it polls a cache slot that
*structurally cannot* become populated by another host's build, for the
full `poll_timeout_s` (default `DEFAULT_POLL_TIMEOUT_S = 600.0`, i.e. 10
minutes — `target/registry.py:57`, and `worker/matching_handler.py`'s
`_resolve_target_segments` calls `get_or_build_segment_embedding` with
every default, so this is the value that actually applies), then raises
`TimeoutError`, which `matching_handler.py:161-162` maps to
`TransientFailure` — a scheduled retry, not a hard failure, but one that
first burned up to 10 minutes of worker time doing nothing but polling a
cache slot it could never win.

This is materially worse than Phase 12 §14's characterization ("a fleet
of N machines would each build their own copy of the same target's
embedding," which reads as "N independent, immediate, parallel builds").
The actual behavior is: **the first host to hit a given target builds
immediately; every other host's first job against that same target stalls
for up to 10 minutes before it even starts its own (duplicate) build on
retry** (once its own `RedisLock` TTL-window opportunity arrives, since
the original winner's lock has long since expired or released by then).
Net effect across a fleet: not "N parallel builds," but "1 immediate
build + (N-1) sequential ~10-minute stalls before their own duplicate
builds," which is worse for both latency and throughput than either "no
lock at all" (N immediate parallel duplicate builds, no stalling) or "a
truly shared cache" (1 build, N fast hits).

**Classification:**
- Single-host deployment (the only kind validated through Phase 1-12):
  **CORRECT.** The lock, the cache, and the registry all behave exactly
  as documented and tested.
- Multi-host deployment: **PRODUCTION BLOCKER**, not merely the
  "acceptable limitation" framing Phase 11/12 used. A fleet-wide
  first-touch-per-target stall of up to 10 minutes per host, landing as a
  retryable failure that consumes one of `max_attempts` (default 3) per
  occurrence, is a functional correctness/availability problem for any
  crawler workload that fans a busy target out across more than one
  fingerprinter host — not just a performance inefficiency.
- The abstraction boundary (`TargetEmbeddingCache`/`SegmentEmbeddingCache`
  as `ABC`s, `TargetRegistry` depending only on the interface) is real and
  confirmed by inspection (zero non-test/non-target production references
  to `Filesystem.*Cache`) — **CORRECT**, and it is what makes the fix
  (§12) a swap-the-implementation change, not a redesign.

## 4. Backpressure audit

**MEASURED (code inspection, `integration/backpressure.py`,
`integration/submission.py`).**

**`count_outstanding` — CORRECT as a soft bound.** `lag + pending` read
from `XINFO GROUPS` is exactly the two numbers Redis Streams tracks for
`Worker.reclaim_stale()`'s own `XAUTOCLAIM` call — no parallel bookkeeping
was invented, so there is no way for this number to drift out of sync
with reality the way a separately-maintained counter could. The
`lag is None` fallback (reject, not silently disable) is a defensively
correct choice; it is **untested against a real corrupting condition**
(no `XDEL`/`XTRIM` call exists anywhere in this codebase to trigger it) —
consistent with Phase 12 §25's own admission. **CORRECT, with one
untested defensive branch — OPERATIONAL FOLLOW-UP**, not a blocker (the
branch fails toward the safe direction even if never exercised).

**Duplicate submission handling — CORRECT.** The `SET NX EX` marker
(`integration/submission.py:146-154`) is a single atomic Redis round-trip;
`derive_job_id` (`integration/idempotency.py`) is a pure function of
`(candidate_url, target_id, target_version, sorted(techniques))` with no
I/O, so two processes computing it for the same candidate always agree
before either touches Redis. The ordering (validate → backpressure check
→ claim marker → XADD → release marker on XADD failure) closes the
"rejected submission leaves a poison marker" failure mode by construction
— verified in `tests/test_integration_submission.py`'s dedup and
backpressure-recovery tests (`182 passed`, MEASURED this session, §11).

**Can multiple crawler producers safely submit jobs concurrently? —
CORRECT, with one documented, accepted soft-bound caveat.** Nothing in
`FingerprintJobSubmitter.submit()` assumes a single caller: the dedup
marker's atomicity is what actually prevents a double-enqueue under
concurrent submitters (not the backpressure check, which is explicitly a
snapshot read — `integration/backpressure.py`'s own docstring says so).
Two producers racing the *backpressure* check (not the dedup marker) can
each see `outstanding < max` and both proceed, so the 500-job cap can be
briefly exceeded under heavy concurrent submission — this is Phase 12's
own documented "admission control, not exact quota" framing
(`integration/backpressure.py:1-16`), and this audit found no evidence it
is anything worse than that: correctness (idempotency, at-most-once
enqueue per job_id) is unaffected by the count being soft; only the
*bound's* tightness is soft. **ACCEPTABLE LIMITATION**, exactly as
Phase 12 characterized it — this audit's inspection did not surface a
sharper problem here the way it did for §3's target cache.

**`DEFAULT_MAX_OUTSTANDING_JOBS = 500` / `DEFAULT_SUBMISSION_MARKER_TTL_S
= 24h` — PROVISIONAL, unchanged.** Both are reasoned (not measured)
extrapolations from Phase 11's single-host throughput figure, explicitly
labeled as such in `integration/backpressure.py`'s own comment and
`integration/submission.py`'s own comment. This audit found no new
evidence to tighten or loosen either number — **REQUIRES MULTI-HOST
VALIDATION**, unchanged from Phase 12 §25.

## 5. GPU audit

**MEASURED, this session:** `python -c "import torch; torch.cuda.is_available()"`
→ `False`, on this development machine — identical to every prior
phase's finding, confirming nothing about the environment changed.

**Code inspection, `embedding/dinov2_engine.py:56-143`:**
- `device="cpu"` → `torch.device("cpu")`, unconditional.
- `device="cuda"` → raises `DeviceUnavailableError` immediately if
  `torch.cuda.is_available()` is `False`, rather than silently falling
  back to CPU — **CORRECT**: a caller that explicitly asked for GPU and
  silently got CPU would have no way to notice a misconfigured deployment
  until throughput looked wrong.
- `device="auto"` (the default) → GPU if available, else CPU — reasonable
  default, **CORRECT**.
- Per-inference GPU hygiene: `_embed_pil_image` explicitly moves inputs to
  `self.device`, calls `torch.cuda.empty_cache()` after each image when on
  CUDA, and deletes tensor references (`del outputs, cls_token, inputs`)
  before that — **CORRECT** as written; this is exactly the kind of
  hygiene that matters for long-running GPU worker processes, though it
  has never run against a real GPU to confirm it prevents the memory
  growth it's clearly written to prevent.

**No GPU benchmark numbers exist anywhere in this repo**, and none are
fabricated by this audit. `torch_num_threads` (the CPU-oversubscription
fix, §2) is explicitly a no-op on the GPU path (it configures the CPU
thread pool) — nothing in the GPU code path is device-count- or
worker-count-aware in a way that would need an equivalent fix, as far as
static inspection can determine.

**Classification: REQUIRES MULTI-HOST/GPU VALIDATION**, exactly per the
audit brief's instruction for this condition — this audit neither
discovered a code defect in the GPU path nor can confirm it works, because
no CUDA device exists in this environment to run it against. Not a
blocker in the sense of "known broken"; a blocker in the sense of
"unvalidated and unvalidatable here."

## 6. Media acquisition robustness audit

**MEASURED (code inspection, `acquisition/acquirer.py`,
`acquisition/validation.py`, `acquisition/artifact.py`,
`acquisition/errors.py`).** This is the strongest-built area of the whole
system, with one clear gap.

| Concern | Finding | Classification |
|---|---|---|
| Connect/read timeouts | `(connect_timeout_s=5.0, read_timeout_s=30.0)` passed to every `requests` call, both defaults finite | CORRECT |
| Redirects | Bounded (`max_redirects=5`, default), scheme re-checked on *every* hop (`_check_scheme` inside the `while True` loop, `acquirer.py:101-119`) — a redirect cannot smuggle a `file://`/`ftp://` hop past the scheme allowlist | CORRECT |
| Size limit | Enforced against actual streamed bytes (`total += len(chunk)` inside the write loop), never trusts `Content-Length` — a server that lies about `Content-Length` or omits it entirely still gets cut off at `max_bytes` (100 MiB default) | CORRECT |
| Content-type | Checked against response header (allowlist: `video/`, `image/`, `audio/` prefixes) *and* independently validated against actual bytes via `ffprobe` — a mislabeled or absent header is not trusted alone, but also not rejected alone (`_check_content_type`'s own comment explains this deliberately) | CORRECT |
| Corruption | `probe_media` (`ffprobe -show_streams`) raises `InvalidMediaError` for undecodable/empty/no-stream files, bounded by a 5s subprocess timeout so a pathological file can't hang acquisition indefinitely | CORRECT |
| Cleanup | Every failure path in `_stream_to_disk` (`ReadTimeout`, `ConnectionError`, any other `Exception`) calls `_safe_unlink` before re-raising; `MediaArtifact.cleanup()` is idempotent and called from `matching_handler.py`'s `finally` block regardless of success/failure | CORRECT |
| Retry | Not the acquirer's own job — it raises typed `Transient`/`PermanentAcquisitionError` subclasses and `worker/fingerprint_worker.py`'s existing retry/backoff (Phase 3, unchanged) drives retries at the job level | CORRECT (retry lives at the right layer) |
| Temp-file safety | `tempfile.mkstemp(..., prefix="fingerprinter-acq-", suffix=".media")` — path never derived from the URL's own text, so a URL containing path-traversal-shaped characters can't influence where the file lands | CORRECT |
| **SSRF / internal-network access** | **No IP-based restriction of any kind.** `grep` for `ssrf`/`is_private`/`169.254`/`127.0.0.1`/`ip_address` across `acquisition/`, `integration/`, `worker/` returns zero matches. The scheme allowlist (`http`/`https`) and content-type/media validation say nothing about *where* the URL resolves. A candidate URL that resolves to `127.0.0.1`, an RFC1918 address, a cloud metadata endpoint (`169.254.169.254`), or any other internally-reachable host is fetched exactly like any external URL — including through a redirect chain the crawler never directly submitted (an externally-reachable URL that 302s to an internal address is followed automatically, since `_check_scheme` only checks the scheme, not the resolved host, on each hop). | **PRODUCTION BLOCKER** |

**Why SSRF is classified as a blocker, not a lesser category:** the
system's entire purpose is to fetch attacker-adjacent URLs — `candidate_url`
values originate from crawled, potentially-adversarial third-party
content (piracy sites), which is a meaningfully different trust model than
"URLs a trusted internal service constructs." A pirate site (or anyone who
can influence what the crawler discovers) fully controls where a
`candidate_url` — or, more subtly, a redirect target the fingerprinter's
own acquirer follows on that site's behalf — points. Nothing in this
codebase's threat model documentation (`design-proposal-1.md`, phase-12
§18 "Security considerations") claims SSRF protection is out of scope or
handled elsewhere (phase-12 §18 discusses scheme validation and Redis-key
construction, not resolved-host validation) — this is a genuine, unaddressed
gap, not a previously-accepted tradeoff. It was not surfaced by any prior
phase's audit.

**Not fabricated/tested against real external hosts, per the audit
brief's explicit instruction** ("do not access random external websites")
— this finding is from static code inspection only, not a live SSRF probe.

## 7. Observability / operational-readiness audit

**MEASURED (inventory + code inspection).**

```
grep -rl "import logging\|logger\." **/*.py   -> zero matches in production code
grep -rn "print(" **/*.py (excluding tests)   -> only benchmarks/*.py
find . -iname "*cli*" -o -iname "*admin*" -o -iname "*metrics*" -o -iname "*dashboard*" -o -iname "*health*"
                                               -> zero matches outside old/
```

**What an operator *can* currently determine, using only what this repo
ships (all via raw `redis-cli`, nothing packaged):**

| Question | Answer available? | How |
|---|---|---|
| Is a specific job pending/claimed/retrying/done? | Yes | `HGETALL fingerprint:job:{job_id}:state`, or `integration.outcome.resolve_outcome()` if called from Python |
| What did a completed job decide? | Yes | `HGETALL fingerprint:job:{job_id}:result` |
| Queue backlog size (lag + pending)? | Yes | `XINFO GROUPS fingerprint:jobs:stream:{priority}` — exactly what `integration.backpressure.count_outstanding` already reads |
| Which worker owns a stuck job / how stale is it? | Yes, partially | `XPENDING fingerprint:jobs:stream:{priority} fingerprinter-workers` gives consumer + idle time; `state.worker_id` gives the same worker identity Phase 1 already stores |
| Aggregate throughput (jobs/sec) over time? | **No** | No metric is ever recorded anywhere; would require scraping the results stream's timestamps manually |
| Per-worker resource usage (CPU/RAM/GPU)? | **No** | Nothing in this repo touches `psutil`/`nvidia-smi`/cgroup accounting — Phase 11's RAM safety gate (`bench_pipeline.py`) is a benchmark-only heuristic, not a production signal |
| Structured logs / error traces for a failed job? | **No** | Zero logging infrastructure; the only trace of *why* a job failed is the short `reason`/`failure_reason`/`summary` string already stored in Redis state/result hashes — adequate for a human doing a manual `HGETALL`, not searchable/aggregable |
| Alerting on a stuck fleet / growing backlog? | **No** | Nothing exports any of the above to a metrics backend (Prometheus, StatsD, CloudWatch, ...) |

**Classification:** the *data* an operator needs mostly already exists in
Redis (job_id correlation end-to-end, worker_id, attempt, timestamps, are
all real, tested, Phase 1-12 facts — **CORRECT** as far as data
availability goes). What's missing is entirely the *tooling* layer on top
of that data: **OPERATIONAL FOLLOW-UP** for logging/structured error
detail (would improve debugging, doesn't block correctness), but
**PRODUCTION BLOCKER** for the complete absence of any throughput/backlog/
resource visibility an on-call operator could act on without hand-running
Redis commands — running this fleet today would mean nobody notices a
stalled backlog, a crash-looping worker, or the §3/§6 failure modes above
until a human happens to check.

## 8. Distributed-safety review (cross-cutting)

Synthesizing §2-§7 against the specific distributed-systems properties a
crawler-driven, multi-host deployment needs:

| Property | Status | Evidence |
|---|---|---|
| At-most-once job claim (no two workers process the same stream entry concurrently) | CORRECT | `XREADGROUP`/CAS-fenced Lua scripts, Phase 1-3, re-verified this session via `test_worker.py`/`test_crash_recovery.py` passing |
| Crash-safe reclaim (a dead worker's job is recovered, not lost) | CORRECT | `XAUTOCLAIM`-based `reclaim_stale()`, Phase 2, re-verified this session |
| At-least-once, deduplicated submission | CORRECT | `SET NX` marker + deterministic `job_id`, Phase 12, re-verified this session |
| Fleet-wide admission control | ACCEPTABLE LIMITATION (soft bound, documented) | §4 |
| Fleet-wide build-once cache semantics | **PRODUCTION BLOCKER for multi-host** | §3 |
| Fleet-wide safe outbound fetch (no internal-network exposure) | **PRODUCTION BLOCKER** | §6 |
| Fleet-wide operational visibility | **PRODUCTION BLOCKER** | §7 |
| A deployable, signal-aware, correctly-configured worker process | **PRODUCTION BLOCKER** | §2 |

Four independent, real production blockers. None of them require a
redesign of the coordination core (job claim/lease/retry/commit, Phase
1-4, is sound throughout and this audit found no defect in it) — every
one is either a missing outer layer (entrypoint, observability) or a
localized fix within an already-abstracted boundary (target cache
backend, acquirer host validation).

## 9. Findings table (all findings, most severe first)

| # | Area | Finding | Classification |
|---|---|---|---|
| 1 | Worker/deployment | No worker process entrypoint exists anywhere in the repo; the system cannot be run in production today | PRODUCTION BLOCKER |
| 2 | Target cache | Build-on-miss lock is fleet-wide (Redis-correct) but the cache it protects is host-local; multi-host losers stall up to 10 min per job before duplicating the build anyway | PRODUCTION BLOCKER (multi-host only; single-host CORRECT) |
| 3 | Media acquisition | No SSRF / internal-network-resolution protection on outbound candidate URL fetches (including redirect targets) | PRODUCTION BLOCKER |
| 4 | Observability | No throughput/backlog/resource metrics, no logging, no alerting — only raw Redis introspection | PRODUCTION BLOCKER |
| 5 | Worker/deployment | `torch_num_threads` fix exists but nothing plumbs it; multi-process-per-host deployment silently reproduces Phase 11's ~15x oversubscription regression | PRODUCTION BLOCKER (conditional: only if/when multiple worker processes per host are deployed without a config-aware entrypoint) |
| 6 | Worker/deployment | No SIGTERM/SIGINT handling (the `stop()` primitive it would call is correct and tested) | OPERATIONAL FOLLOW-UP (blocked on #1 — moot until an entrypoint exists) |
| 7 | Worker/deployment | No startup-failure/crash-loop/readiness-probe policy | OPERATIONAL FOLLOW-UP |
| 8 | Worker/deployment | No production Redis connection configuration (pooling, retry, TLS, auth) | OPERATIONAL FOLLOW-UP |
| 9 | Backpressure | `count_outstanding`'s `lag is None` fallback path is untested (no code path triggers it today) | OPERATIONAL FOLLOW-UP |
| 10 | Backpressure | Soft bound can be briefly exceeded under concurrent submitters | ACCEPTABLE LIMITATION |
| 11 | Backpressure | `DEFAULT_MAX_OUTSTANDING_JOBS`/`DEFAULT_SUBMISSION_MARKER_TTL_S` untuned against real load | REQUIRES MULTI-HOST VALIDATION |
| 12 | GPU | Device selection/hygiene code is correct by inspection but never run against real CUDA hardware | REQUIRES MULTI-HOST/GPU VALIDATION |
| 13 | Job claim/lease/retry/commit core | No defects found this session; re-verified via full test-suite pass | CORRECT |
| 14 | Idempotency/backpressure core | No defects found this session; re-verified via full test-suite pass | CORRECT |
| 15 | Acquisition (timeouts/redirects/size/corruption/cleanup) | No defects found; all bounded and tested | CORRECT |

## 10. Production blockers (exact list)

1. **No worker process entrypoint.** The system has no way to run in
   production at all.
2. **Multi-host target-cache stalls/duplication** (only applies once more
   than one fingerprinter host is deployed against the same target).
3. **No SSRF protection on outbound acquisition.**
4. **No operational observability** (metrics, logging, alerting).
5. **CPU-oversubscription footgun** — conditional on #1: only a live risk
   once multiple worker processes per host are actually deployed, which
   requires the (currently nonexistent) entrypoint to happen at all. Listed
   separately because the *fix* (plumbing `torch_num_threads`) is a
   distinct, entrypoint-adjacent piece of work from #1 itself.

## 11. Acceptable limitations (not blockers)

- Backpressure is a soft, snapshot-based admission bound, not a hard
  distributed rate limiter — documented, and this audit found no evidence
  it compromises correctness (only bound tightness).
- Target-cache abstraction is filesystem-local **for single-host
  deployments**, which is the only configuration validated through Phase
  1-12 — acceptable as the system stands today, becomes blocker #2 above
  only once multi-host is attempted.
- `count_outstanding`'s defensive `lag is None` branch is unexercised —
  fails toward the safe direction (reject) even though untested.
- GPU code path is unvalidated for lack of hardware, not for any known
  defect.

## 12. Deferred work (explicitly out of scope for Phase 13 audit and for
the fixes that would follow it, restated/confirmed from Phase 12 §26,
still true)

- Crawler-side bridge (`evidence:jobs:queue` → `FingerprintJobSubmitter`)
  — crawler-repo work, different ownership.
- Load-testing backpressure thresholds against real multi-host throughput
  and a real backlog-latency SLA (neither exists yet).
- Real network acquisition benchmarking (this audit's acquisition
  findings are from code inspection + the existing loopback-HTTP test
  suite, not live internet traffic — consistent with the "no random
  external websites" instruction).
- GPU load/throughput benchmarking (blocked on hardware access).

## 13. Required fixes (files that would need modification — for a future
implementation step; **nothing here was implemented this phase**)

| Blocker | Files needing changes | Nature of change |
|---|---|---|
| #1 No entrypoint | New: `worker/main.py` (or similar) | New module: parse config/env, construct `Redis`/`DINOv2EmbeddingEngine`/`MediaAcquirer`/`TargetRegistry`/`Worker`, wire `SIGTERM`/`SIGINT` → `worker.stop()`, run `worker.run(handler)`, structured startup-failure logging |
| #2 Multi-host cache | New: e.g. `target/redis_cache.py` (or object-storage-backed) implementing `TargetEmbeddingCache`/`SegmentEmbeddingCache`; **no change needed** to `target/registry.py`, `worker/matching_handler.py`, or the lock — the `ABC` boundary already isolates this (confirmed §3) | New backend implementation only |
| #3 SSRF | `acquisition/acquirer.py` (`_send`/`_check_scheme` or a new pre-connect hook), possibly `acquisition/validation.py` | Add resolved-IP validation before connecting on the initial request *and* every redirect hop — reject loopback/link-local/private/reserved ranges by default, configurable allowlist for legitimate internal test targets |
| #4 Observability | New: a metrics-emission module (e.g. `observability/metrics.py`) called from `worker/fingerprint_worker.py`'s `run()`/`process_claim()`/`commit_result()`; new: structured logging calls (`import logging`) at the same call sites | Additive instrumentation; no change to existing control flow/return values |
| #5 Thread config | Same new entrypoint as #1 — plumb a `torch_num_threads` value (derived from configured worker-count-per-host) into every `DINOv2EmbeddingEngine(...)` construction | Config plumbing, no engine-internal change (parameter already exists) |

## 14. Tests needed for each fix (for the future implementation step)

| Fix | Tests needed |
|---|---|
| #1 Entrypoint | Process starts, connects, claims a job end-to-end (extends existing e2e pattern); SIGTERM during an in-flight job does not ack it (mirrors `test_graceful_shutdown_does_not_ack_unfinished_work` at the process level, e.g. via `subprocess`); startup failure (bad Redis URL / bad model path) exits non-zero with a clear error, does not hang |
| #2 Shared cache backend | Same contract tests `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` already have, run against the new backend (interface parity); a genuine two-Redis-client (simulating two hosts) test proving a build on "host A" becomes visible to "host B" without a timeout — the test this audit could not run because no second cache implementation exists yet |
| #3 SSRF | Loopback/private/link-local URL rejected before any request is sent; a redirect chain that starts external and 302s to an internal address is rejected at the redirect hop, not just the initial URL; legitimate external URL still succeeds (regression guard against over-blocking) |
| #4 Observability | Metrics emitted with expected labels/values for each terminal outcome (match/no_match/permanent_error/retryable); log line emitted on job failure includes job_id/reason (correlatable, not just present) |
| #5 Thread config | Entrypoint config parsing test: given worker-count-per-host, the constructed engine's `torch_num_threads` matches the expected per-process allocation |

## 15. What can safely remain deferred

- Everything in §12 (crawler bridge, load testing, real-network
  benchmarking, GPU load testing) — none of it blocks a correct,
  single-host production deployment, and all of it requires resources
  (a second repo's cooperation, real traffic, real GPU hardware) this
  audit does not have.
- `count_outstanding`'s untested `lag is None` branch — safe as written,
  add a test opportunistically, not urgently.
- Backpressure/marker-TTL retuning — genuinely cannot be done without
  real load data; premature tuning here would just be a different
  unjustified guess replacing the current one.

## 16. Is the current fingerprinter production-ready?

**No — for a multi-host, internet-facing crawler-driven deployment**,
which is the deployment this project has been building toward since
`design-proposal-1.md`. The coordination core (claim/lease/retry/commit,
idempotency, backpressure-as-soft-bound) is genuinely solid and this audit
found no defect in it. But four independent gaps — no deployable process,
no protection against the outbound fetcher being pointed at internal
infrastructure, no way to observe the fleet operationally, and a cache
architecture that actively fights multi-host operation rather than merely
underperforming at it — mean the system as it stands cannot be safely
turned on against real, adversarial-influenced crawler input across more
than one host today.

**A single-host deployment fetching only pre-vetted (non-adversarial) URLs
would be closer to ready** — blockers #2 and #5 don't apply to one host,
and #3 (SSRF) is the only one that doesn't depend on "multi-host" as a
qualifier at all. Even then, #1 (no entrypoint) and #4 (no observability)
still block calling the system "production-ready" in any deployment
shape — they are unconditional gaps, not multi-host-specific ones.

## 17. Recommended implementation order for the blockers

1. **SSRF fix (#3)** first — smallest, most self-contained change (one
   module), zero dependency on anything else being built, and it is a
   security gap that exists even in the "closest to ready" single-host
   shape named in §16.
2. **Worker entrypoint (#1)**, immediately combined with **thread
   config plumbing (#5)** — they share the same new module and #5 has no
   meaning without #1 existing first. This is the item that turns
   "a well-tested library" into "a thing that can run."
3. **Observability (#4)** — build it into the entrypoint from #2 as it's
   written, not bolted on after; cheapest to do while that code is
   already being touched, and an entrypoint deployed without it would
   immediately create the exact "nobody notices a stalled fleet" risk
   §7 describes.
4. **Multi-host target cache (#2)** last among the blockers — it is the
   largest, most architecturally significant change (a new storage
   backend, plus real multi-host testing to validate it, which requires
   #1 to already exist to even deploy a second host's worker process
   against). It is also the one Phase 11 and Phase 12 already
   independently flagged as the standing highest-priority open question,
   which this audit confirms rather than overturns — this audit's
   contribution is sharpening *why* it's urgent (active stalls, not just
   duplicated work), not changing its position in the queue.

Steps 1-3 do not depend on each other in a way that forbids reordering,
but 4 genuinely cannot be validated without 1 existing first.

---

**This was an audit only. No code changed. Awaiting instruction before
implementing any of the fixes in §13.**
