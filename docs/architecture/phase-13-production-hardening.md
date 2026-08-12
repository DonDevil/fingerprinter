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

## 18. Phase 13A — SSRF hardening

**Implemented this phase.** Addresses §9 finding #3 / §10 blocker #3 only
— the other three blockers (worker entrypoint, observability, multi-host
target cache) are untouched, per this phase's scope.

### Original vulnerability

`MediaAcquirer.acquire()` (`acquisition/acquirer.py`) validated URL
*scheme* (`http`/`https` only) and re-validated it on every redirect hop,
but never validated *where* a URL — or a redirect target reached through
it — actually resolved to. `candidate_url` values come from crawled,
adversarial third-party content, so a pirate site (or anyone who
influences what the crawler discovers) fully controlled the destination
the acquirer would connect to, including via a redirect chain the
acquirer follows automatically.

### Attack scenario

1. A crawled page's `candidate_url` (or a URL it 302-redirects to) points
   at `http://127.0.0.1:6379/`, `http://169.254.169.254/latest/meta-data/`,
   or an RFC1918 address reachable from the fingerprinter host's network.
2. Nothing in the old acquirer rejected this — scheme was `http`, which is
   allowed. `_send()` would connect and stream whatever came back through
   the same content-type/ffprobe validation as legitimate media.
3. Depending on what's reachable, this ranges from an internal service
   probe (port-scan-by-timing) to exfiltrating cloud metadata/credentials
   if the response body is echoed anywhere back to the crawler pipeline.

### Chosen security model

Resolved-IP allowlisting by exclusion: resolve the hostname of the
initial URL and of every redirect hop, and reject the hop before
connecting if any resolved address is loopback, RFC1918-private,
link-local, unspecified, multicast, or otherwise reserved. The check
operates on the **resolved address**, never on the hostname string —
per the task brief's explicit instruction, `hostname.startswith(...)`-
style string matching was rejected as insufficient and is not used
anywhere in this implementation.

Default production policy (`allow_private_networks=False`, the
constructor default) denies all of the above unconditionally. A single
explicit, narrowly-scoped constructor flag (`allow_private_networks:
bool = False`) is the only opt-out — it is passed `True` only by test
fixtures that intentionally target the loopback test server
(`tests/media_test_server.py`) and by the three loopback-only
benchmarks; it is never the default and nothing sets it globally.

### Implementation

- **New module: `acquisition/ssrf_guard.py`.** `is_unsafe_address(addr)`
  classifies one resolved IP using Python's `ipaddress` module
  (`is_loopback`, `is_private`, `is_link_local`, `is_unspecified`,
  `is_multicast`, `is_reserved`), plus one explicit extra range not
  covered by stdlib (see IPv4 handling below). `resolve_addresses(...)`
  wraps a `socket.getaddrinfo`-shaped resolver call. `validate_destination(...)`
  resolves a hostname and raises `UnsafeDestinationError` if *any*
  returned address is unsafe — conservative on purpose, since nothing in
  this module controls which of several A/AAAA records the real HTTP
  connection ends up using.
- **`acquisition/acquirer.py`:** one new private method,
  `_check_destination(url)`, called from the same `while True:` loop in
  `acquire()` that already calls `_check_scheme(url)` on every iteration
  — so it runs against the initial URL and against every redirect target
  `current_url` is reassigned to, with zero change to the loop's control
  flow. Two new constructor parameters: `allow_private_networks: bool =
  False` and `resolver: Optional[Resolver] = None` (defaults to
  `socket.getaddrinfo`; overridable for tests — see DNS handling below).
  No existing method signature, redirect-limit logic, size-limit
  streaming, content-type check, or ffprobe validation was touched.
- **`acquisition/errors.py`:** one new class, `UnsafeDestinationError(PermanentAcquisitionError)`.

### IPv4 handling

Covered via `ipaddress.IPv4Address`'s built-in classification: loopback
(`127.0.0.0/8`), RFC1918 private (`10.0.0.0/8`, `172.16.0.0/12`,
`192.168.0.0/16`), link-local (`169.254.0.0/16`, including the
`169.254.169.254` cloud-metadata address), unspecified (`0.0.0.0`),
multicast (`224.0.0.0/4`), and reserved (`240.0.0.0/4`). One gap was
found and closed explicitly: **`100.64.0.0/10` (RFC 6598,
carrier-grade-NAT/shared address space) is not flagged private or
reserved by Python 3.12's `ipaddress` module** (confirmed by direct
interpreter check this session — MEASURED) despite never being a
legitimate public server address; `ssrf_guard._EXTRA_UNSAFE_NETWORKS`
adds it back explicitly. `test_carrier_grade_nat_range_rejected` in
`tests/test_acquisition_ssrf.py` pins this.

### IPv6 handling

Covered via the IPv6 equivalents: loopback (`::1`), unique-local
(`fd00::/8`, RFC 4193 — Python classifies this as `is_private`, the IPv6
analogue of RFC1918), link-local (`fe80::/10`), unspecified (`::`), and
multicast (`ff00::/8`). IPv4-mapped IPv6 addresses (`::ffff:127.0.0.1`)
are unwrapped to their embedded IPv4 address before classification
(`addr.ipv4_mapped`), rather than relying on the raw v6 flags. **MEASURED,
this session:** Python 3.12's `ipaddress` module already marks the
*entire* `::ffff:0:0/96` block `is_reserved=True` unconditionally (it's
IANA special-purpose space) — so an unwrapped mapped-loopback address like
`::ffff:127.0.0.1` was already caught (`is_reserved=True`) even without
this module's explicit unwrap step. What the unwrap step actually fixes
is the opposite failure mode: *without* it, `is_reserved=True` would also
reject a mapped address embedding a genuinely public IPv4 address (e.g.
`::ffff:8.8.8.8`), which is over-blocking, not an SSRF gap. Unwrapping and
classifying the embedded v4 address directly gives the precise answer in
both directions — confirmed by direct interpreter check this session
(`::ffff:8.8.8.8` unwraps to public/safe; `::ffff:10.1.2.3` unwraps to
private/unsafe) — and keeps the policy's "don't reject a hostname merely
because of an unusual representation" requirement intact for this one
edge case.

### Redirect handling

Unchanged mechanics (bounded by `max_redirects`, `Location` re-joined via
`urljoin`); the only addition is that `_check_destination` now runs
before `_send()` on *every* hop the existing loop already visits,
including hop 2+. `test_external_url_redirecting_to_loopback_is_rejected`
and `test_external_url_redirecting_to_private_address_is_rejected`
(`tests/test_acquisition_ssrf.py`) prove the internal hop is rejected —
and that the fake transport is never even called for it — without
touching the redirect-following logic itself.
`test_normal_redirect_between_two_public_hosts_still_functions` and
`test_redirect_handling`/`test_final_redirected_url_is_recorded` (both
pre-existing, `tests/test_acquisition.py`) prove ordinary redirects are
unaffected.

### DNS handling and known TOCTOU/rebinding limitation

`_check_destination` re-resolves DNS itself (via the injectable
`resolver`, defaulting to `socket.getaddrinfo`) immediately before each
connection attempt — not once at the top of `acquire()` and reused, so a
redirect to a different hostname gets its own fresh resolution and its
own check.

**This does not fully close DNS rebinding — classified explicitly, per
the task brief's instruction not to claim more than is true:**
`requests`/urllib3 performs its *own*, independent DNS resolution a
moment later when it actually opens the socket inside `_send()`. Nothing
in this implementation pins the real TCP connection to the address this
module validated. An attacker who controls authoritative DNS for the
candidate's hostname, serves a public IP to this module's lookup, and
then serves a different (internal) IP to `requests`' lookup a few
milliseconds later — classic DNS rebinding, typically via a very low TTL
— is **not** defeated by this check alone.

- **Classification: ACCEPTABLE LIMITATION / DEFERRED to a future phase.**
  Closing it fully requires pinning the actual socket connection to the
  address this module already validated — e.g. a custom
  `requests.adapters.HTTPAdapter`/`urllib3` connection pool that connects
  to the validated IP directly while still presenting the original
  hostname for the `Host` header and TLS SNI/certificate verification.
  That is a materially larger change to the HTTP transport itself, which
  the task brief explicitly said not to undertake in this pass
  ("do NOT perform a huge HTTP-client rewrite during this task").
  Recorded here as **DEFERRED**, not silently absent.
- This is a narrower window than "no protection at all": it requires the
  attacker's DNS infrastructure to race two lookups a few milliseconds
  apart, versus the pre-Phase-13A state where a static internal IP in a
  redirect target worked unconditionally, every time.

### Error classification / retry behavior

`UnsafeDestinationError` subclasses `PermanentAcquisitionError` (not a
new error root). `worker/acquisition_handler.py`'s existing
`except PermanentAcquisitionError` catch-all (unchanged — it already
matched on the base class, not each subclass by name) maps it onto
`PermanentFailure` automatically, so a job whose URL resolves unsafely
fails immediately with no retry — exactly the "should not retry a
malicious URL indefinitely" requirement. A separate, pre-existing
concern is kept separate: a hostname that fails to resolve at all
(`socket.gaierror`) is **not** treated as an SSRF finding — it's mapped
to `NetworkError` (transient), consistent with how a real connection-time
DNS failure was already classified before this change.

### Configuration

No new configuration system. One constructor parameter
(`allow_private_networks: bool = False`) is the entire surface;
production callers that never set it get the safe default. No
environment variable, config file, or allowlist of "permitted internal
hosts" was introduced — not needed by anything in the current
architecture (§13's required fixes list no legitimate production need for
the fingerprinter to fetch from internal addresses).

### Tests

New file: `tests/test_acquisition_ssrf.py` (22 tests, all passing):

| # | Test(s) | Covers |
|---|---|---|
| 1 | `test_ipv4_loopback_rejected` (2 cases) | IPv4 loopback |
| 2 | `test_ipv6_loopback_rejected` | IPv6 loopback |
| 3 | `test_rfc1918_private_ipv4_rejected` (3 cases) | RFC1918 private IPv4 |
| 4 | `test_ipv6_private_local_address_rejected` | IPv6 unique-local (RFC 4193) |
| 5 | `test_link_local_address_rejected` (2 cases: v4 incl. cloud metadata, v6) | Link-local |
| 6 | `test_unspecified_address_rejected` (2 cases) | Unspecified (`0.0.0.0`, `::`) |
| 7 | `test_multicast_and_reserved_addresses_rejected` (3 cases) | Multicast/reserved v4+v6 |
| — | `test_carrier_grade_nat_range_rejected` | RFC 6598 CGNAT gap closed explicitly |
| 8 | `test_normal_public_destination_remains_allowed` | Public destination still allowed |
| 9 | `test_external_url_redirecting_to_loopback_is_rejected` | External→loopback redirect rejected |
| 10 | `test_external_url_redirecting_to_private_address_is_rejected` | External→private redirect rejected |
| 11 | `test_normal_redirect_between_two_public_hosts_still_functions` | Normal redirect unaffected |
| — | `test_dns_resolution_failure_maps_to_transient_network_error` | Unresolvable host stays transient, not SSRF |
| — | `test_normal_loopback_destination_is_allowed_when_explicitly_opted_in` | Opt-out itself still works |
| — | `test_existing_redirect_and_content_tests_are_unaffected_by_default_policy` | Hostless/malformed URL still a plain permanent error |

Direct-address cases (1, 2, 3, 4, 5, 6, 7) use literal IP URLs against a
`_NeverCalledSession` that fails the test if `_send()` is ever reached —
proving the check runs before any connection attempt, with zero real
network I/O (`getaddrinfo` on a numeric address never queries DNS).
Redirect cases (9, 10, 11) use an in-process fake `requests.Session`
(`_ScriptedSession`) plus an injected fake `resolver` — dependency
injection, not real DNS or a modified hosts file, per the task brief's
explicit instruction. Point 12 ("existing media acquisition tests still
pass") — see §Verification below; five pre-existing test files
(`tests/test_acquisition.py`, `tests/test_integration_e2e.py`,
`tests/test_matching_handler.py`, `tests/test_worker_acquisition.py`, and
three loopback-only benchmark scripts) were updated to pass
`allow_private_networks=True` explicitly, since they intentionally target
the loopback `media_test_server.py` fixture that the new default would
otherwise (correctly) reject.

### Performance impact

**MEASURED** (small controlled benchmark, this session): 2,000
`socket.getaddrinfo("127.0.0.1", 80, proto=socket.IPPROTO_TCP)` calls
(the literal-IP case, which never touches the network) averaged **3.13
µs/call**. **INFERRED** for the hostname-resolution case (not separately
benchmarked): one additional `getaddrinfo` call of the same kind
`requests`/urllib3 already performs internally for the real connection a
moment later — not a new category of cost, roughly doubling the number of
DNS round-trips per hop rather than adding a new kind of work. Not
optimized further; not a meaningful cost next to a network fetch that is
already bounded by 5s connect / 30s read timeouts.

### Verification

Full commands run this session, exact counts:

```
python -m pytest tests/test_acquisition_ssrf.py -v
  -> 22 passed

python -m pytest tests/test_acquisition.py tests/test_worker_acquisition.py -v
  -> 21 passed

python -m pytest -q   (full suite)
  -> 204 passed, 0 failed, 0 skipped
```

No pre-existing failures were hidden; there were none to hide (full suite
was green before and after this change).

### Files changed

- `acquisition/ssrf_guard.py` (new)
- `acquisition/acquirer.py` (`_check_destination`, two new constructor
  params, one new call site in `acquire()`'s existing loop)
- `acquisition/errors.py` (`UnsafeDestinationError`)
- `acquisition/__init__.py` (export)
- `tests/test_acquisition_ssrf.py` (new, 22 tests)
- `tests/test_acquisition.py`, `tests/test_integration_e2e.py`,
  `tests/test_matching_handler.py`, `tests/test_worker_acquisition.py`,
  `benchmarks/bench_pipeline.py`, `benchmarks/bench_integration_overhead.py`
  (each: pass `allow_private_networks=True` explicitly, since they target
  the loopback test fixture)

### Is the SSRF blocker resolved?

**Yes, for the vulnerability as scoped and classified in §6/§9/§10**: a
crawler-supplied `candidate_url`, or a redirect target reached through
one, that resolves to loopback/RFC1918/link-local/unspecified/
multicast/reserved space is now rejected by default, on the initial URL
and every redirect hop, based on the resolved address rather than
hostname text. This closes the specific gap §6 identified: "no IP-based
restriction of any kind."

**Not fully closed: DNS-rebinding TOCTOU** (see above) — a narrower,
explicitly-classified residual risk (ACCEPTABLE LIMITATION / DEFERRED),
not silently claimed as solved. Recommended for Phase 13B or a later
pass if this system's threat model requires defeating an adversary who
also controls low-TTL authoritative DNS for candidate hostnames.

### Recommendation for Phase 13B

Per §17's original ordering, the next blocker in priority is **the worker
process entrypoint (#1)**, ideally combined with the CPU-thread-config
plumbing (#5) since they share the same new module. This SSRF pass did
not touch `worker/`, so that work is fully unblocked and independent of
everything done here. If a stronger SSRF guarantee (closing the
DNS-rebinding gap) becomes a priority before the entrypoint work, it
would be a self-contained follow-up to `acquisition/acquirer.py` (a
custom transport adapter pinning the validated IP) and would not require
revisiting anything else touched in this phase.

---

## 19. Phase 13B — Production worker entrypoint

**Implemented this phase.** Addresses §9 findings #1 and #5 / §10 blockers
#1 and #5 only. Blockers #2 (multi-host target cache) and #4
(observability) are untouched, per this phase's scope.

### Process architecture

One new module, `worker/main.py`, with no new classes in `worker/` or
elsewhere — it is purely a composition root. It constructs, in the same
dependency order Phase 12's tests and benchmarks already use (confirmed by
inspecting `tests/test_matching_handler.py` and
`benchmarks/bench_pipeline.py` before writing this):

```
Redis client (this module's own Redis(...)/from_url(...) call site)
  -> MediaAcquirer(max_bytes=...)
  -> TargetRegistry(redis_client,
                     FilesystemEmbeddingCache(cache_path/pooled),
                     FilesystemSegmentEmbeddingCache(cache_path/segments))
  -> DINOv2EmbeddingEngine(device=..., torch_num_threads=...)
  -> build_matching_handler(acquirer, engine, registry)
  -> Worker(redis_client, consumer_name=..., block_ms=..., lease_ms=...,
            reclaim_interval_ms=...)
  -> install_shutdown_handlers(worker)   # SIGTERM/SIGINT -> worker.stop()
  -> worker.run(handler)                  # blocks until stop()
  -> redis_client.close()
```

No existing class gained a new constructor parameter and no existing
constructor signature changed — `worker/main.py` only calls what Phase
6/9/10/12 already built, exactly as `worker/matching_handler.py`'s own
docstring already specified the pipeline it expects to be driven by.
**IMPLEMENTED.**

### Exact command to start a worker

```
python -m worker.main
```

Configuration is entirely environment-variable driven (see below); there
is no required CLI argument.

### Configuration

| Variable | Default | Maps to |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | `Redis.from_url(...)` |
| `WORKER_CONSUMER_NAME` | unset -> `Worker`'s own `default_consumer_name()` | `Worker(consumer_name=...)` |
| `WORKER_LEASE_MS` | `30000` (mirrors `Worker.__init__`'s own default) | `Worker(lease_ms=...)` |
| `WORKER_BLOCK_MS` | `5000` (mirrors `Worker.__init__`'s own default) | `Worker(block_ms=...)` |
| `WORKER_RECLAIM_INTERVAL_MS` | unset -> `None` (`Worker` itself then defaults it to `lease_ms`) | `Worker(reclaim_interval_ms=...)` |
| `WORKER_MAX_ATTEMPTS` | unset | **accepted and validated, but not wired to anything — see "Known limitations"** |
| `EMBEDDING_DEVICE` | `auto` (mirrors `DINOv2EmbeddingEngine`'s own default) | `DINOv2EmbeddingEngine(device=...)` |
| `TORCH_NUM_THREADS` | `1` (this entrypoint's own safe default — see "CPU thread configuration") | `DINOv2EmbeddingEngine(torch_num_threads=...)` |
| `TARGET_CACHE_PATH` | `./target_cache` (this entrypoint's own default — none existed before) | `FilesystemEmbeddingCache`/`FilesystemSegmentEmbeddingCache` base dir, split into `<path>/pooled` and `<path>/segments`, mirroring `benchmarks/bench_pipeline.py`'s own `_build_registry` layout |
| `MEDIA_MAX_BYTES` | `104857600` (100 MiB, mirrors `MediaAcquirer`'s own `DEFAULT_MAX_BYTES`) | `MediaAcquirer(max_bytes=...)` |

Every default that already existed in the library (lease/block ms, device,
media size) is copied from that library's own default, not re-invented.
Two defaults are new because no prior default existed anywhere:
`TORCH_NUM_THREADS=1` (deliberately conservative — see below) and
`TARGET_CACHE_PATH=./target_cache` (an arbitrary but clearly-documented
relative path an operator is expected to override in any real deployment).
**IMPLEMENTED.**

All ten values are validated before anything is constructed — bad
integers, an out-of-range device string, a non-`redis://`/`rediss://`/
`unix://` URL, or a non-positive thread count all raise a `ConfigError`
with every problem listed in one message, and `main()` logs it and returns
`1` without attempting to connect to anything. **IMPLEMENTED, tested**
(`tests/test_worker_main.py::test_invalid_config_raises_config_error`,
parametrized over all ten failure shapes).

### Redis connection ownership

`worker/main.py` is the **only** production call site for
`Redis(...)`/`Redis.from_url(...)` — confirmed by re-running the same grep
the Phase 13 audit (§2) used before writing this module. Every downstream
component (`Worker`, `TargetRegistry`) still takes an already-constructed
client, unchanged. Connection settings, checked against the installed
redis-py version (8.1.0) before use rather than assumed:

| Setting | Value | Why |
|---|---|---|
| `decode_responses` | `True` | Every existing production/test call site (`tests/conftest.py`, `benchmarks/bench_pipeline.py`) uses this; `Job.from_stream_fields`, `JobStateStore`, etc. all assume `str`, not `bytes`, fields. Confirmed by grep before use — this is not a new convention. |
| `socket_connect_timeout` | `5.0s` | Bounds how long startup can hang if Redis is unreachable — fixed, not exposed as an env var, to keep the configuration surface minimal per this phase's scope. |
| `socket_timeout` | `10.0s` | Bounds any single blocking Redis call other than `XREADGROUP`'s own explicit `block_ms` argument. |
| `health_check_interval` | `30s` | Detects a silently-dead connection during idle periods between jobs. |
| `retry_on_timeout` | `True` | One transparent retry on a timed-out socket op — conservative, not a retry supervisor. |

All four were confirmed present on the installed `redis.Redis.__init__`
signature before being used (not assumed compatible). A `client.ping()`
call immediately after construction turns "Redis unreachable" into a fast,
explicit, clearly-logged failure rather than a delayed failure inside
`Worker.__init__`'s `xgroup_create`. **IMPLEMENTED, tested**
(`tests/test_worker_main.py::test_main_returns_nonzero_when_redis_unreachable`
and the process-level
`test_worker_process_exits_nonzero_fast_when_redis_unreachable`).

Redis HA/failover, TLS, and auth beyond what's embeddable in `REDIS_URL`
remain **DEFERRED** — explicitly out of scope per the task brief.

### Worker identity

Unchanged mechanism: `WORKER_CONSUMER_NAME` unset -> `Worker` itself calls
`default_consumer_name()` (`worker-{hostname}-{pid}-{thread_ident}`),
unique per host+process+thread as Phase 1/2 already established. An
explicit `WORKER_CONSUMER_NAME` is passed through verbatim. **Deployment
rule (must be followed by whoever runs multiple worker processes):** if
`WORKER_CONSUMER_NAME` is set explicitly, it must be unique across every
simultaneously-running worker process/host sharing the same Redis —
Redis Streams' consumer-group model (`XREADGROUP`/`XAUTOCLAIM`) attributes
PEL ownership by consumer name, and a collision would let two processes
silently share one consumer's claimed-entry bookkeeping. Leaving it unset
(the default) avoids this entirely, since `default_consumer_name()` is
already unique per process. **IMPLEMENTED, tested**
(`test_explicit_worker_identity_is_honored`,
`test_default_worker_identity_matches_existing_pattern`).

### Signal handling / shutdown behavior

`SIGTERM` and `SIGINT` both call the same handler, which calls
`worker.stop()` and nothing else — confirmed by a test that installs the
handler against a mock `Worker` and asserts only `.stop()` was called,
never `.ack()`/`.commit_result()`/`.run()`
(`test_signal_handlers_call_worker_stop_and_only_stop`). This preserves
`Worker.stop()`'s existing, already-tested semantics unchanged
(`tests/test_worker.py::test_graceful_worker_shutdown`,
`tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work`):
the current blocking `XREADGROUP` (bounded by `WORKER_BLOCK_MS`) finishes,
any job already claimed in that same loop iteration is still handed to the
handler and finalized normally (this is `Worker.run()`'s existing,
unmodified control flow — the signal handler does not interrupt an
in-flight handler call, by design, matching the task's explicit
instruction not to terminate abruptly from the handler), and the loop then
exits on its next top-of-loop check. The process exits only after
`worker.run()` returns; nothing acks or force-completes work merely
because a signal arrived. **IMPLEMENTED, tested** at the unit level
directly, and at the process level via
`test_worker_process_starts_against_real_redis_and_shuts_down_on_sigterm`
(real subprocess, real Redis, real `SIGTERM`, asserts exit code 0 and the
expected log lines).

### Startup failure behavior

Three failure classes, all fail fast, none hang, none retry:

1. **Invalid configuration** — caught before any component is
   constructed, logged, `main()` returns `1`.
2. **Redis unreachable** — `build_redis_client`'s `ping()` raises
   `redis.exceptions.RedisError`; caught, logged (URL with credentials
   redacted), `main()` returns `1`. Bounded by the 5s connect timeout, not
   indefinite.
3. **Any other component construction failure** (model load failure,
   device unavailable, cache directory not writable, etc.) — caught by one
   deliberately broad `except Exception` around the
   acquirer/registry/engine/handler/worker construction block, logged with
   the exception type and message, the already-open Redis client is
   closed, `main()` returns `1`.

No retry/backoff/crash-loop policy is built here — per the task's explicit
instruction, an external process supervisor (systemd, Kubernetes, etc.) is
expected to own restart policy; that is unchanged from the Phase 13 audit's
own classification of this as an "operational follow-up," not solved by
this phase, just no longer *blocked* by the absence of an entrypoint.
**IMPLEMENTED, tested** (`test_main_returns_nonzero_on_invalid_config`,
`test_main_returns_nonzero_when_redis_unreachable`,
`test_main_returns_nonzero_and_closes_redis_when_component_construction_fails`,
and the process-level `test_worker_process_exits_nonzero_fast_when_redis_unreachable`
/ `test_worker_process_exits_nonzero_on_bad_config`).

### torch_num_threads configuration / CPU sizing guidance

**This is the fix for blocker #5.** `TORCH_NUM_THREADS` is read, validated
(`>= 1`), and passed to every `DINOv2EmbeddingEngine` this entrypoint
constructs — no code path constructs the engine without it flowing
through. **IMPLEMENTED, tested**
(`test_torch_num_threads_is_passed_to_engine_constructor`,
`test_default_torch_num_threads_is_one_not_unbounded`).

**Default value: `1`.** Derived directly from Phase 11's own measurements
(`docs/architecture/phase-11-performance-benchmarks.md` §19a/§19b), not
invented:

- §19b **MEASURED** that N worker processes each left at torch's own
  default (physical-core-count) thread pool causes catastrophic
  oversubscription — 15x slower per job and net-*negative* scaling at just
  4 processes on a 6-physical-core machine.
- §19a **MEASURED** that pinning each process to exactly 1 thread
  ("isolated-1thread") is the configuration that actually scales usefully
  across multiple processes (0.81 efficiency at 4 workers, still improving
  when the benchmark's RAM safety gate stopped it, not a CPU ceiling).
- §12 **MEASURED** the cost of this safety: a *lone* worker on an
  otherwise-idle host is ~3.6x slower per job at 1 thread than at 6
  (physical-core-count).

This entrypoint has no visibility into how many other worker processes
will run on the same host — that is a deployment-time fact, not something
`worker/main.py` can infer — so it defaults to the value Phase 11 measured
as *safe regardless of worker count* rather than the value that is fastest
*only* for a single worker. An operator who knows exactly one worker
process runs on a given host should set `TORCH_NUM_THREADS` explicitly
(e.g. to the host's physical core count) to recover that ~3.6x. **This
default is INFERRED to still apply, not re-measured this session** —
`embedding/dinov2_engine.py` is unmodified since Phase 11 measured against
it, so Phase 11's numbers are expected to still hold, but no new benchmark
was run in this phase (out of scope, per the task brief).

**CPU sizing rule an operator must apply manually (not automated by this
entrypoint):**

```
worker processes per host  x  TORCH_NUM_THREADS per process  <=  host's physical core count
```

This entrypoint does **not** read `os.cpu_count()` or otherwise guess a
"correct" value from worker-count-per-host, because it has no reliable way
to know how many *other* worker processes (started by a separate
supervisor, possibly on a schedule this module never sees) will share the
host — guessing wrong in either direction is worse than requiring an
explicit, documented operator decision. This mirrors the task's own
instruction not to assume worker-count equals core-count. **DEFERRED**:
an orchestration layer that *does* have that visibility (Kubernetes
resource requests, a systemd template unit with a known replica count)
could compute and inject `TORCH_NUM_THREADS` automatically — explicitly
out of scope for this phase (no Kubernetes/systemd work here).

### GPU distinction

`EMBEDDING_DEVICE` is passed straight through to
`DINOv2EmbeddingEngine(device=...)`, whose existing `_resolve_device`
logic (unmodified) governs `auto`/`cpu`/`cuda` selection exactly as before
— this phase adds no new device logic. `TORCH_NUM_THREADS` controls
`torch.set_num_threads(...)`, which governs **CPU-side** thread pools
(BLAS/tensor ops on CPU tensors, and CPU-side work even in a `cuda` run
such as preprocessing); it does **not** configure CUDA streams, GPU
memory, or any GPU-specific concurrency. Setting `EMBEDDING_DEVICE=cuda`
with a `TORCH_NUM_THREADS` override is accepted and passed through
unchanged, but **this phase makes no claim that GPU operation was
exercised or validated** — consistent with the Phase 13 audit's own
classification (§5, §9 finding #12): GPU code path correctness is by
inspection only, and throughput/concurrency behavior on GPU **REQUIRES GPU
VALIDATION** this project does not have hardware for. Nothing in this
phase changes that classification.

### Resource ownership / shutdown

Only the `Redis` client is owned and explicitly closed by this entrypoint
(`redis_client.close()`, in a `finally` after `worker.run()` returns, and
also on every startup-failure exit path). Inspected and found to need no
explicit cleanup: `DINOv2EmbeddingEngine` (an in-memory torch model,
reclaimed by normal process exit — no `close()`/`__del__` contract
exists), `TargetRegistry`/`FilesystemEmbeddingCache`/
`FilesystemSegmentEmbeddingCache` (no persistent handles held between
calls; each read/write opens and closes its own file), `MediaAcquirer`
(holds an internal `requests.Session()` with no public `close()`/accessor
— out of scope to add one, since the task explicitly disallows modifying
`acquisition/`; its connection pool is reclaimed at process exit like any
other process-local resource). **IMPLEMENTED / INFERRED** (no cleanup bug
was found, not proven impossible by a dedicated stress test).

### Tests

New file: `tests/test_worker_main.py` (24 tests, all passing):

| Area | Test(s) |
|---|---|
| Config defaults | `test_config_defaults_when_env_is_empty` |
| Config overrides | `test_config_overrides_from_env` |
| Invalid config -> `ConfigError` | `test_invalid_config_raises_config_error` (parametrized, 11 cases) |
| Invalid config -> `main()` returns 1 | `test_main_returns_nonzero_on_invalid_config` |
| `torch_num_threads` plumbing | `test_torch_num_threads_is_passed_to_engine_constructor`, `test_default_torch_num_threads_is_one_not_unbounded` |
| Worker identity | `test_explicit_worker_identity_is_honored`, `test_default_worker_identity_matches_existing_pattern` |
| Signal handling | `test_signal_handlers_call_worker_stop_and_only_stop` |
| Startup failure (unit) | `test_main_returns_nonzero_when_redis_unreachable`, `test_main_returns_nonzero_and_closes_redis_when_component_construction_fails` |
| Process-level: start + graceful SIGTERM shutdown | `test_worker_process_starts_against_real_redis_and_shuts_down_on_sigterm` |
| Process-level: startup failure | `test_worker_process_exits_nonzero_fast_when_redis_unreachable`, `test_worker_process_exits_nonzero_on_bad_config` |

The process-level tests launch `python -m worker.main` as a real
subprocess against the shared test Redis (`tests/conftest.py`'s db 15,
flushed by the existing `redis_client` fixture), poll `XINFO GROUPS` for
the consumer group the process should create, then send a real
`SIGTERM`/rely on an unreachable port — no external infrastructure, no
CUDA hardware required. **IMPLEMENTED, MEASURED** (all 24 pass; full
counts below).

**Known test-coverage gap, explained rather than silently skipped:** a
true process-level test of "SIGTERM during an in-flight job does not ack
it" (mirroring `test_graceful_shutdown_does_not_ack_unfinished_work` at
the process level, as the task's suggested test list names) is **not**
included. Reaching that state requires the subprocess to actually acquire
candidate media mid-job; the only acquisition target available in this
sandbox is the loopback `tests/media_test_server.py`, and this
entrypoint's `MediaAcquirer` is deliberately constructed with Phase 13A's
SSRF guard at its correct, unmodified production default
(`allow_private_networks=False`), which rejects loopback addresses by
design — so a real subprocess run through `worker/main.py` cannot reach
that test server at all, and no external network target is available to
this sandbox either. The underlying guarantee this would exercise is
already covered two other ways: (1) `Worker.run()`'s control flow itself
is untouched by this phase and already covered by
`tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work`
at the library level; (2) this phase's own
`test_signal_handlers_call_worker_stop_and_only_stop` proves the new
signal-wiring code itself cannot be the source of a force-ack bug, since
it is asserted to call nothing but `.stop()`. **DEFERRED**: a genuine
process-level version of this test would need either a
non-network-dependent way to stall a handler mid-flight (e.g. an
injectable slow target-cache build) or a documented, test-only relaxation
of the SSRF default — neither was judged worth doing in this pass.

### Testing discipline — exact commands and counts run this session

```
python -m pytest tests/test_worker_main.py -q
  -> 24 passed

python -m pytest tests/test_worker.py tests/test_worker_acquisition.py \
  tests/test_crash_recovery.py tests/test_matching_handler.py \
  tests/test_embedding.py tests/test_retry.py -q
  -> 56 passed

python -m pytest -q   (full suite)
  -> 228 passed, 0 failed, 0 skipped
```

No pre-existing failures were hidden; there were none to hide (full suite
was green before and after this change — 204 passed pre-Phase-13B, 228
passed after, the +24 delta being exactly the new file).

### Files changed

- `worker/main.py` (new — the entrypoint; no other file in `worker/`,
  `target/`, `acquisition/`, `embedding/`, or `integration/` was modified)
- `tests/test_worker_main.py` (new, 24 tests)
- `docs/architecture/phase-13-production-hardening.md` (this section)

### Known limitations

- **`WORKER_MAX_ATTEMPTS` is accepted, validated, and logged as a warning
  if set, but has no effect.** `max_attempts` is a per-`Job` field
  (`work_queue/jobs.py`) set by the *producer* at submission time
  (`Job(max_attempts=...)`), not a `Worker` constructor parameter — there
  is no existing hook in `Worker.__init__`/`Worker.run()` for a
  worker-level default to attach to, and inventing one would be exactly
  the "guess constructor arguments" / scope-creep the task brief warned
  against. Kept in the configuration surface (per the task's explicit
  minimum-variable list) purely for forward documentation parity; an
  operator who sets it gets a clear startup-time log line explaining why
  it did nothing, not silent, unexplained no-op behavior.
- Redis HA, TLS/auth beyond `REDIS_URL`, and connection-pool tuning beyond
  the four fixed settings above remain **DEFERRED**.
- `TORCH_NUM_THREADS` sizing across a fleet is a manual operator
  responsibility (see "CPU sizing guidance"); no auto-detection was added.
- GPU operation is passed through unchanged and unvalidated — **REQUIRES
  GPU VALIDATION**, unchanged from the Phase 13 audit's own classification.
- The full claim -> acquire -> embed -> match -> ack path through the real
  subprocess entrypoint is untested in this sandbox (SSRF-guard/no-network
  constraint explained above under "Tests"); it is covered against a
  directly-constructed pipeline (not through `worker/main.py`) by
  `tests/test_matching_handler.py` and `benchmarks/bench_pipeline.py`.
- No metrics/structured alerting — Phase 13C's scope, unchanged.
- No multi-host target-cache backend — blocker #2, unchanged, still
  requires a live entrypoint to even test against (which now exists).

### Are blockers #1 and #5 resolved?

**Blocker #1 (no worker process entrypoint): Yes.** `python -m worker.main`
is a real, runnable process that connects to Redis, constructs the full
production pipeline in the established dependency order, claims and
processes jobs through the unmodified `Worker`/`build_matching_handler`
contract, and shuts down cleanly on `SIGTERM`/`SIGINT` without acking
in-flight work. Verified by real subprocess tests against real Redis, not
just unit-level mocking.

**Blocker #5 (CPU-oversubscription footgun, conditional on #1): Yes,
conditionally resolved the same way #1 is.** `torch_num_threads` is now
plumbed from an explicit, validated, safely-defaulted
`TORCH_NUM_THREADS` all the way to every `DINOv2EmbeddingEngine` this
entrypoint constructs — the "nothing forces or even guides a future
entrypoint's author to actually pass it" gap the audit named no longer
exists, because the entrypoint now exists and always passes it. What
remains an **operator responsibility, not a code gap**: sizing
`TORCH_NUM_THREADS` correctly against actual worker-count-per-host, which
this module cannot know on its own (see "CPU sizing guidance").

### Recommendation for Phase 13C

Per §17's original ordering, the next blocker is **observability (#4)**,
and the task brief that produced this phase's own recommendation was to
build it into this entrypoint "as it's written, not bolted on after" —
`worker/main.py` now exists as exactly the place Phase 13C's metrics/
structured-logging call sites belong (already has a `logger`, already logs
every lifecycle event named in §7's gap list at INFO/WARNING/ERROR, just
without metrics emission or an operator-facing dashboard yet). Blocker #2
(multi-host target cache) remains last per §17's reasoning, now
additionally justified by this phase: it requires a second real worker
process to test against for the first time, which only now exists.

## 20. Phase 13C — Production observability

**Implemented this phase.** Addresses §9 finding #4 / §10 blocker #4
(observability) only. Blocker #2 (multi-host target cache) is untouched,
per this phase's scope. No claim/lease/retry semantics, SSRF guard, or
`worker/main.py` dependency-construction order changed.

### Observability architecture

One new module, `worker/observability.py` — structured JSON logging,
process-local counters/latency, a stdlib-only resource sampler, a bounded
Redis Streams health-snapshot reader, and `ObservingWorkerObserver`, which
implements a new no-op `WorkerObserver` interface added to
`worker/fingerprint_worker.py`. `Worker` calls `WorkerObserver` methods at
its existing claim/reclaim/reject/complete/fail/retry-schedule/
permanently-fail boundaries; the default (no `observer` passed) is a pure
no-op, so every pre-13C `Worker(...)` call site (all of Phases 1-12's
tests) is byte-for-byte behaviorally unchanged. `worker/main.py` is the
only production call site that constructs a real `ObservingWorkerObserver`
and passes it in — mirroring how Phase 13B kept `Redis(...)` construction
to one call site. **IMPLEMENTED.**

Dependency direction is one-way: `worker/observability.py` imports
`worker.fingerprint_worker.WorkerObserver`/`work_queue.jobs.Job`;
`worker/fingerprint_worker.py` imports nothing from `worker/observability.py`
(confirmed by inspection — the base `WorkerObserver` class lives in
`fingerprint_worker.py` itself, not the other way around).

Pipeline-stage timing is a second, independent hook: `build_matching_handler`
(`worker/matching_handler.py`) gained an optional `stage_recorder` callback,
called as `stage_recorder(stage_name, duration_s)` around each of its
existing stage boundaries. Default `None` -> no-op, so
`tests/test_matching_handler.py`, `tests/test_integration_e2e.py`, and
`benchmarks/bench_integration_overhead.py` (all call `build_matching_handler`
positionally, without this new kwarg) are unaffected.

**No dependency was added.** `psutil` is not installed in this project
(`pip show psutil` -> not found, checked before writing any code this
phase) and the task brief requires proving the stdlib can't satisfy a
requirement before reaching for a library — it can (see "Process resource
metrics" below), so none was added.

### Event schema

Structured JSON logs, one object per line, via `worker.observability.
JsonFormatter` installed on the root logger by `worker/main.py`'s
`configure_logging()` (extends, not replaces, the project's only logging
stack — stdlib `logging` — per the task brief). Every event line has:
`timestamp`, `level`, `logger`, `event`, `message`, plus whatever
structured fields that event's call site attached. Every job-lifecycle and
worker-lifecycle event additionally carries: `worker_id`, `hostname`,
`pid`, `consumer_name`, `namespace` (`"fingerprint"`, the fixed Redis
key-prefix this project has always used), `stream`, `consumer_group` — the
full identity set §1/§13 require, attributable to exactly one process.
**IMPLEMENTED, tested**
(`tests/test_worker_observability.py::test_structured_event_is_valid_json_with_identity_fields`,
`::test_worker_process_emits_worker_started_and_worker_ready_as_valid_json`).

Event names actually emitted, each tied to a boundary the current code
already observes (no invented states):

| Event | Emitted by | When |
|---|---|---|
| `worker_started` | `worker/main.py` | after config validation, before Redis connect — carries the full config snapshot (§9 below) |
| `worker_ready` | `worker/main.py` | after all components construct successfully, right before `worker.run()` |
| `worker_shutdown_requested` | `worker/main.py`'s signal handler | on SIGTERM/SIGINT |
| `worker_stopped` | `ObservingWorkerObserver.emit_shutdown_summary` | after `worker.run()` returns, in `main()`'s `finally` |
| `worker_fatal_error` | `worker/main.py` | invalid config / Redis unreachable / component construction failure (three distinct `reason` values, see "Startup failure behavior" in §19, unchanged this phase) |
| `job_claimed` | `Worker.claim_one` | a fresh, valid claim |
| `job_reclaimed` | `Worker.reclaim_stale` | a valid entry recovered via XAUTOCLAIM |
| `job_rejected` | `Worker.claim_one`/`reclaim_stale` | a malformed stream entry (schema-invalid), `source` field distinguishes claim vs reclaim |
| `job_completed` | `Worker.process_claim` | handler returned normally and the finalize (ack or commit_result) actually took effect (not a stale no-op — see "Counters" below) |
| `job_failed` | `Worker.process_claim` | handler raised `PermanentFailure` |
| `job_retry_scheduled` | `Worker._handle_transient_failure` | handler raised `TransientFailure`, attempts remain |
| `job_permanently_failed` | `Worker._handle_transient_failure` | handler raised `TransientFailure`, `max_attempts` exhausted |
| `worker_health` | `ObservingWorkerObserver.on_loop_tick` | periodically, see §8 below |

No event exists for a state this codebase cannot currently observe (e.g.
no per-stage "cache hit vs build" event — see "Pipeline stage latency"
measurement gap below). **IMPLEMENTED.**

**What is never logged** (§1): `job.media_url`, the raw `str(exc)` failure
reason (which `acquisition/acquirer.py` proves can embed the candidate
URL, e.g. `"connection timeout to {url}"`), Redis credentials, and
tracebacks are never repeated per-event (only `exc_type`, once, if
present). The one exception is `job_rejected`'s `error` field: it carries
`JobValidationError`'s full message, which is safe by construction — that
message only ever names missing/invalid job-schema field names
(`work_queue/jobs.py`), never a URL. **IMPLEMENTED, tested**
(`test_malformed_job_rejection_increments_rejected_not_claimed` asserts
the field name appears; no test anywhere asserts a URL appears in an
event, and `TransientFailure`/`PermanentFailure` now carry a separate,
safe `error_type` — see "Error classification").

### Counters

`WorkerCounters` (process-local, plain ints under one `threading.Lock` —
`Worker.run()` is single-threaded, so this is defensive rather than load-
bearing, and it also protects a periodic health read from a torn state):
`jobs_claimed`, `jobs_reclaimed`, `jobs_completed`, `jobs_failed`,
`jobs_rejected`, `jobs_retried`, `jobs_permanently_failed`, `active_jobs`,
`total_job_attempts`. **IMPLEMENTED, tested** (10 of the 20 new tests in
`tests/test_worker_observability.py` exercise these directly against real
Redis through a real `Worker`).

Update points, chosen to update only at the *actual* finalize boundary,
not merely because a handler ran:

- `jobs_claimed`/`jobs_reclaimed`/`active_jobs`/`total_job_attempts`
  increment only for entries that parsed as a valid `Job` — a malformed
  entry increments `jobs_rejected` (and, if it arrived via `reclaim_stale`,
  `jobs_reclaimed` too — the stream entry genuinely was reclaimed by
  XAUTOCLAIM before being found malformed, but it never entered "active"
  processing, so `active_jobs` is untouched for it).
- `jobs_completed` increments only if `Worker.ack()`/`commit_result()`
  actually returned a truthy result — i.e., this worker's attempt was
  still current when it finalized. A **stale** worker's finalize attempt
  (attempt-fencing lost a race to a `XAUTOCLAIM`-based reclaim, Phase 2)
  returns `False`/`None` and increments nothing. **Tested**
  (`test_stale_finalize_does_not_double_count_completion`).
- `jobs_failed` (handler raised `PermanentFailure`), `jobs_retried`
  (`TransientFailure`, attempts remain), and `jobs_permanently_failed`
  (`TransientFailure`, `max_attempts` exhausted) are three **distinct**
  counters/events even though the first and third both write
  `status=failed` to the same Redis state hash internally (`Worker._fail`)
  — from an operator's perspective "the handler said don't retry" and "we
  retried until we ran out of attempts" are different failure modes, and
  the current code already knows which branch it took, so nothing had to
  be guessed. **Tested** (`test_permanent_failure_increments_jobs_failed`,
  `test_transient_failure_increments_jobs_retried`,
  `test_exhausted_retries_increments_jobs_permanently_failed`).
- `active_jobs` decrements on every one of the four terminal outcomes
  above (completed/failed/retried/permanently-failed) — since `Worker.run()`
  processes one job at a time synchronously, `active_jobs` is 0 or 1 from
  any single worker's own perspective, and every test above confirms it
  returns to 0. **Tested** (every counter test above asserts
  `active_jobs == 0` after finalize; §15 required test #7).
- `total_job_attempts` increments once per claim or reclaim of a valid
  entry — equivalently, once per handler invocation attempt.

New-claim vs stale-reclaim vs retry-redelivery are distinguished exactly
as far as the current implementation allows (per the task brief's "where
the current worker implementation allows that distinction" — no more):
a *fresh* claim (`jobs_claimed`) and a *stale-lease reclaim*
(`jobs_reclaimed`, via `XAUTOCLAIM`) are two different code paths and two
different counters, already. A *retry-redelivery* (a promoted-from-ZSET
entry coming back through `XREADGROUP`) is **not separately distinguished
from a fresh claim** — both go through `Worker.claim_one()` and increment
`jobs_claimed` — because the stream entry itself carries no marker saying
"this is attempt N of a retry" versus "this is attempt 1"; only
`entry.attempt` (already existing, Phase 1/2) distinguishes them, and that
is preserved and logged on every `job_claimed` event. **Documented gap**,
not invented: adding a true `job_retried_redelivery` event distinct from
`job_claimed` would require either a new stream-entry field (touches
`work_queue/jobs.py`, explicitly off-limits this phase) or inferring it
from `entry.attempt > 1` at claim time, which is already fully recoverable
from the existing `job_claimed` event's `attempt` field — so no dedicated
event was added, to avoid a second way of saying the same thing.

### Latency metrics

`BoundedLatencyStats` (per-metric: exact `count`/`min`/`max`/`avg`, plus
p50/p95/p99 approximated from a **bounded**, most-recent-2000-sample
window — never every sample ever recorded, per §3). Two metrics,
process-local, reset on worker restart:

- `claim_to_completion_ms` — recorded on every `job_completed`.
- `claim_to_failure_ms` — recorded on every `job_failed`,
  `job_retry_scheduled`, and `job_permanently_failed` alike (all three are
  "claim to non-success finalize").

Measured from `entry.claimed_at` (a new, optional, default-`None` field on
`ClaimedEntry`, set via `time.monotonic()` at claim/reclaim time — additive,
so every pre-existing direct `ClaimedEntry(...)` construction in the test
suite is unaffected) to the moment `Worker` calls the matching observer
hook. **IMPLEMENTED, tested**
(`test_successful_completion_increments_completion_counter_exactly_once_and_records_latency`;
§15 required test #8).

### Pipeline stage latency

`worker/matching_handler.py`'s `handler()` now times its own five existing
stage boundaries via the `stage_recorder` hook (no new boundaries invented,
no refactor beyond adding `time.monotonic()` pairs around code that was
already there):

| Stage | Boundary measured |
|---|---|
| `media_acquisition` | `acquirer.acquire(job.media_url)` |
| `candidate_embedding` | `engine.embed_video_segments(artifact)` on the candidate |
| `target_resolution` | `_resolve_target_segments(...)` — see measurement gap below |
| `matching` | `match_segments(...)` |
| `aggregation` | `temporal_match_to_evidence` + `combine` |

**Measurement gap, documented per the task brief rather than closed by
refactoring:** `target_resolution` times the *whole* cache-lookup-or-build
call. `TargetRegistry.get_or_build_segment_embedding` (`target/registry.py`,
off-limits this phase except for a "very small hook", and this didn't
justify one) does not report back whether it returned on a cache hit or
after winning the build-on-miss lock and running a full target embedding
pass — those two cases have very different costs (a `HGET`-equivalent read
vs. a full DINOv2 embedding run) but are not separable from outside without
modifying `target/registry.py`'s return contract. An operator investigating
whether "target resolution" time is dominated by cache hits or cold builds
must currently cross-reference `target_resolution`'s p95/max against
whether the target cache directory (`TARGET_CACHE_PATH`) was freshly
populated. **UNAVAILABLE**, not fabricated.

Similarly, "media probing/artifact creation" (ffprobe-based validation
inside `acquisition.acquirer.MediaAcquirer.acquire`) is **not** separable
from "download" — both happen inside the single `acquire()` call this
phase is allowed to time as one unit (`acquisition/` is off-limits except
for a small hook, and none was judged necessary for this one metric).
Folded into `media_acquisition`. **UNAVAILABLE** as a separate figure.

These two stage-timing gaps directly answer the task brief's question
"where is job time being spent?" to stage granularity (acquisition vs.
candidate embedding vs. target resolution vs. matching vs. aggregation)
but not to the finer "cache hit vs. build" / "download vs. probe"
granularity — **IMPLEMENTED at stage granularity, UNAVAILABLE at finer
granularity** without touching off-limits modules.

### Error classification

`_ERROR_CATEGORY_MAP` in `worker/observability.py` maps an exception's
**type name only** (never its message — see "What is never logged" above)
onto one of: `permanent_acquisition_failure`, `transient_acquisition_failure`,
`media_validation_failure`, `embedding_failure`, `malformed_job`,
`redis_coordination_failure` (the target build-lock's `TimeoutError`),
falling back to `unclassified_error` for anything not in the table — never
silently guessed. Powered by a small, additive change to
`TransientFailure`/`PermanentFailure` (`worker/fingerprint_worker.py`):
both gained an optional `error_type` constructor parameter (default
`"unclassified_error"`), and every raise site in
`worker/matching_handler.py` now passes `error_type=type(exc).__name__`
alongside the **unchanged** message string — so this is purely additive:
`TransientFailure("msg")` (every existing call site, all of
`tests/test_retry.py`/`tests/test_integration_outcome.py`/
`tests/test_results.py`) still works exactly as before, and the Redis
`failure_reason` field a job's state hash has always stored is
byte-for-byte unchanged. **IMPLEMENTED, tested**
(`test_permanent_failure_increments_jobs_failed` asserts the exact category
for a synthetic `NotFoundError`; `classify_error_type` is exercised
directly).

Aggregate category counts are exposed in the health summary/run record's
`error_categories` map (§8/§11), not as a fixed dataclass field — an
open-ended `Dict[str, int]`, since the category set is a fixed lookup
table, not a schema.

### Redis health / queue observability

`worker.observability.redis_health_snapshot(redis_client, stream, group,
consumer_name)` — three bounded Redis Streams reads, no SCAN, no per-job
walk, no new Redis data structure (§6):

- `stream_length` — `XLEN`.
- `group_lag` / `group_pending` — from `XINFO GROUPS` (the exact same
  `lag`/`pending` fields `integration/backpressure.py`'s
  `count_outstanding` already reads — this function reuses the same Redis-
  computed values, just exposes both separately instead of pre-summed).
- `consumer_pending` — this consumer's own pending count, from
  `XINFO CONSUMERS`.
- `oldest_pending_idle_ms` — from a **`count=1`** `XPENDING` range read
  (`XPENDING key group - + 1`), which returns the single oldest-by-ID
  pending entry and its `time_since_delivered` (exactly "idle time").
  Bounded regardless of PEL size — never a full PEL walk.

Measured cost: **~0.16-0.20ms per call** (local Redis, empty-to-small
PEL — see "Overhead" below) — cheap enough to call once per health
interval (default 60s) on a long-running worker. **IMPLEMENTED, tested**
(`test_redis_health_snapshot_reads_stream_and_pel_state`,
`test_redis_health_snapshot_empty_stream`; §15 required test #10).

`ObservingWorkerObserver._safe_redis_snapshot` wraps this in a
try/except on `redis.exceptions.RedisError`, returning `None` (not
fabricating a snapshot) and logging a `worker_health` warning if Redis is
transiently unreachable during a health tick — a health/run-record
`"redis": null` is a legitimate, documented "unavailable this sample", not
a bug.

**Redis-global vs. worker-local, explicitly (§13):** every field this
function returns is **Redis-global** (the whole consumer group's state,
not this worker's private counters) except `consumer_pending`, which is
specific to the `consumer_name` passed in. Everything under `counters`/
`latency`/`pipeline_stage_metrics` elsewhere in this phase is
**worker-local**. Fleet-wide aggregation (summing `redis_health_snapshot`
results, which would double-count `stream_length`/`group_pending` if done
naively across workers sharing one stream) is explicitly **DEFERRED** —
not attempted this phase.

### Process resource metrics

`worker.observability.ResourceSampler`, standard library only
(`resource.getrusage`, `/proc/self/status`, `/proc/self/fd`,
`threading.active_count()`) — **no new dependency added**; `psutil` is
not installed in this project (confirmed via `pip show psutil` before
writing any code this phase), and the stdlib already answers everything
§7 asks for on this project's only deployment target (Linux):

| Field | Source | Notes |
|---|---|---|
| `rss_current_bytes` | `/proc/self/status`'s `VmRSS` | `None` on any non-Linux platform or read failure — never guessed |
| `rss_peak_bytes` | `resource.getrusage(RUSAGE_SELF).ru_maxrss`, running max across samples | KB->bytes on Linux; the class also handles macOS's already-bytes convention, documented in code, though this project only runs on Linux today |
| `cpu_time_s` | `ru_utime + ru_stime` | **Cumulative** process CPU time since start — monotonically increasing, explicitly **not** a percentage |
| `cpu_percent` | `(cpu_time delta) / (wall-clock delta)` between two consecutive samples | `None` on the first-ever sample (no prior point to diff against) — never fabricated; this is the instantaneous/sample utilization figure, kept explicitly distinct from `cpu_time_s` per the task brief's explicit warning not to confuse the two (same distinction the crawler benchmark work required) |
| `thread_count` | `threading.active_count()` | |
| `open_fds` | `len(os.listdir("/proc/self/fd"))` | `None` on any read failure |

Sampled only on a health tick or at shutdown/run-record time (§8/§10/§11)
— never at high frequency, never per-job. **IMPLEMENTED, tested**
(`test_resource_sampler_collects_expected_fields`; §15 required test #11).

### Health interval

`WORKER_OBSERVABILITY_INTERVAL_MS` (default `60000` = 60s) — checked once
per `Worker.run()` loop iteration via a new `observer.on_loop_tick()` call
(one line added to `Worker.run()`'s existing `while` loop; no new thread,
no new blocking call — `run()`'s existing `block_ms`-bounded `XREADGROUP`
already caps loop-iteration latency at ~5s by default, so a 60s interval
is serviced with comfortable granularity without needing a background
timer thread). When due, `ObservingWorkerObserver.emit_health_summary()`
gathers a fresh `redis_health_snapshot` + `ResourceSampler.sample()` +
current counters/latency and emits one `worker_health` event — never one
log line per job or per loop iteration. **IMPLEMENTED, tested**
(`test_health_summary_is_emitted_with_expected_fields`,
`test_health_summary_not_emitted_before_interval_elapses`).

### Configuration snapshot

`worker/main.py`'s `config_snapshot(config, consumer_name)` builds the
dict logged as `worker_started`'s `configuration` field and embedded in
every run record's `configuration` field: `worker_id`, `redis_endpoint`
(credential-redacted via the existing, unmodified `_redact_redis_url`),
`redis_db`, `namespace`, `stream`, `consumer_group`, `lease_ms`,
`block_ms`, `reclaim_interval_ms`, `embedding_device`, `torch_num_threads`,
`target_cache_path`, `media_max_bytes`, `observability_interval_ms`,
`run_output`. No Redis password/token ever appears — **IMPLEMENTED,
tested** (`test_config_snapshot_does_not_leak_redis_credentials`; §15
required test #2).

Two new environment variables, both optional, validated the same way
every other `WorkerConfig` field is:

| Variable | Default | Validation |
|---|---|---|
| `WORKER_OBSERVABILITY_INTERVAL_MS` | `60000` | must be `> 0` |
| `WORKER_RUN_OUTPUT` | unset (no file written) | none — any path string; directories created on first write |

### Shutdown summary

`ObservingWorkerObserver.emit_shutdown_summary(shutdown_reason, clean)` —
called from `worker/main.py`'s `finally` block after `worker.run()`
returns — emits one `worker_stopped` event with: `uptime_s`, every
counter, `average_job_latency_ms`/`p95_job_latency_ms`, `peak_rss_bytes`,
`cpu_time_s`, `shutdown_reason`, `clean`. **IMPLEMENTED, tested**
(`test_shutdown_summary_is_emitted`).

Every code path that exits before `worker.run()` is ever reached (invalid
config, Redis unreachable) logs a `worker_fatal_error` event instead and
is clearly distinguished — `main()` never emits a `worker_stopped`/
"graceful" event for a run that never started. The one path that
constructs an `ObservingWorkerObserver` before failing (component
construction failure, e.g. a model-load error) best-effort writes a run
record with `shutdown.reason="fatal_startup_error"` and
`shutdown.clean=false` if `WORKER_RUN_OUTPUT` is set (wrapped in
`contextlib.suppress(Exception)` — a failure to *write the failure record*
must never mask or replace the original fatal error's own non-zero exit).

### Machine-readable run record

`WORKER_RUN_OUTPUT=/path/to/run.json` (optional; unset by default — no
file is written unless an operator asks for one). On a clean shutdown,
`ObservingWorkerObserver.write_run_record(...)` builds and atomically
writes (`tempfile.mkstemp` in the same directory + `os.replace`, so a
crash mid-write can never leave a truncated/corrupt file at the real path)
a JSON object shaped:

```
{
  "metadata": {worker_id, hostname, pid, consumer_name, namespace, stream, consumer_group, schema_version},
  "configuration": {...config_snapshot(), see above...},
  "timing": {started_at, ended_at, uptime_s},
  "counters": {...WorkerCounters, see "Counters"...},
  "error_categories": {category: count, ...},
  "latency": {"claim_to_completion_ms": {count,min,max,avg,p50,p95,p99,sample_window}, "claim_to_failure_ms": {...}},
  "pipeline_stage_metrics": {"media_acquisition": {...}, "candidate_embedding": {...}, "target_resolution": {...}, "matching": {...}, "aggregation": {...}},
  "redis": {stream_length, group_lag, group_pending, consumer_pending, oldest_pending_idle_ms} | null,
  "resources": {rss_current_bytes, rss_peak_bytes, cpu_time_s, cpu_percent, thread_count, open_fds},
  "shutdown": {"reason": "...", "clean": true|false}
}
```

`null` is used for any value this run genuinely could not measure (e.g.
`redis` if the final snapshot attempt hit a `RedisError`, `rss_current_bytes`
on a platform without `/proc`) — never fabricated. **IMPLEMENTED, tested**
(`test_run_json_is_generated_atomically_for_clean_shutdown`; §15 required
test #14).

### Crash semantics

A `SIGKILL`/power loss/terminal crash cannot run any Python code, so no
final run record can ever be written for that run — this module does not
claim otherwise. Instead, `write_startup_marker(path, configuration)`
writes `<WORKER_RUN_OUTPUT>.marker` (same atomic-write helper) once, right
before `worker.run()` starts (only if `WORKER_RUN_OUTPUT` is set). On a
**clean** shutdown, `write_run_record` removes that marker after
successfully writing the real run record. The resulting, externally
checkable states:

| Marker file | Run-output file | Interpretation |
|---|---|---|
| absent | absent | worker never started (or `WORKER_RUN_OUTPUT` unset) |
| present | absent | worker started, has **not** (yet, or ever) shut down cleanly — still running, or crashed |
| absent | present, `shutdown.clean=true` | worker ran and exited cleanly |
| present | present | a **prior** run's marker was never cleaned up by a **subsequent** crash — an operator should treat this as "crashed after at least one prior clean run", i.e. compare the run record's `timing.ended_at` against the marker's `started_at` |

No Redis state is involved — this is a plain local file, per the task
brief's explicit "keep this lightweight... do not build distributed state
tracking in Redis". **IMPLEMENTED, tested**
(`test_incomplete_run_is_not_falsely_marked_successful` — real `SIGKILL`
against a real subprocess, asserts the marker survives and no `run.json`
is ever produced; §15 required test #15).

### Multi-worker / multi-host correctness

Every event, health summary, and run record carries `hostname`, `pid`,
`consumer_name`/`worker_id` (§13) — `WorkerIdentity.build()` captures these
once at observer construction via `socket.gethostname()`/`os.getpid()`.
`ObservingWorkerObserver`'s own counters/latency are never shared or
aggregated across workers — each process constructs its own instance
against its own in-memory state, matching the fact that `Worker.run()`
already only ever coordinates through Redis, never through shared Python
state. Explicitly, per field group:

- **Worker-local**: `counters`, `latency`, `pipeline_stage_metrics`,
  `resources` — exist only in this process's memory, reset on restart.
- **Redis-global**: `redis_health_snapshot`'s `stream_length`, `group_lag`,
  `group_pending` — the whole consumer group's state, visible identically
  to every worker that queries it.
- **Redis-global, but consumer-scoped**: `consumer_pending` — Redis-held,
  but specific to the querying worker's own `consumer_name`.
- **Fleet-derived** (not computed by this phase): anything requiring
  aggregation across multiple workers' run records/health logs (e.g.
  "total fleet throughput") — **DEFERRED**, per the task brief's explicit
  "fleet aggregation is a future analysis concern", left as a downstream
  log-ingestion/analysis-tool responsibility. The schema above (uniform
  `metadata`/`counters`/`latency` shape, identical across every worker) is
  designed to make that straightforward later, not to perform it now.

### Overhead

**Methodology:** a local, synthetic (no real network/embedding) A/B
comparison — 500 iterations of `Worker.claim_one()` +
`Worker.process_claim(entry, lambda job: None)` against the real test
Redis (`redis://localhost:6379/15`), once with `observer=None` (baseline)
and once with a real `ObservingWorkerObserver` writing JSON-formatted log
lines to a discarded (`io.StringIO`) stream — so the comparison pays the
real cost of counter updates, lock acquisition, latency-stat recording,
and JSON serialization, not just a no-op stub. A trivial handler
(`lambda job: None`) isolates observability's own cost from real
acquisition/embedding cost, which this measurement deliberately excludes
(that cost is unaffected by this phase — no `acquisition/`/`embedding/`
code changed). Not part of the permanent test/benchmark suite, per the
task brief's "do NOT run a large benchmark" — a one-off local script, run
three times for stability.

**Measured** (three runs, local dev machine):

| Run | Baseline (no observer) | With `ObservingWorkerObserver` | Delta/job |
|---|---|---|---|
| 1 | 219.2us/job | 289.0us/job | 69.8us |
| 2 | 222.9us/job | 302.2us/job | 79.2us |
| 3 | 225.4us/job | 307.9us/job | 82.5us |

`redis_health_snapshot()` (paid once per health interval, default 60s —
not per job): **0.16-0.20ms per call**, averaged over 20 calls.

**Conclusion:** ~70-83us of observability overhead per job, against a
*synthetic, no-real-work* Redis-round-trip-only loop of ~220us/job — i.e.
observability roughly doubles the cost of doing *nothing but Redis
bookkeeping*. Against a **real** job, this is negligible: Phase 11 measured
real single-worker throughput at ~1.08 jobs/s warm-cache (~926ms/job,
dominated by CPU embedding) — 80us against 926ms is **~0.009% of a real
job's processing time**, far below what any of this project's own
benchmark methodology would treat as a meaningful signal versus noise.
The periodic Redis health snapshot (~0.2ms every 60s) is likewise
immaterial. **MEASURED**, not asserted without evidence; the target
stated in the task brief ("negligible overhead relative to media
acquisition/embedding") is met.

### Tests

New file: `tests/test_worker_observability.py` (20 tests, all passing),
covering all 15 areas the task brief named as a minimum:

| # | Area | Test(s) |
|---|---|---|
| 1 | Structured startup event | `test_structured_event_is_valid_json_with_identity_fields`, `test_worker_process_emits_worker_started_and_worker_ready_as_valid_json` |
| 2 | Config snapshot doesn't leak credentials | `test_config_snapshot_does_not_leak_redis_credentials` |
| 3 | Job claim increments counter | `test_job_claim_increments_counter` |
| 4 | Successful completion increments completion counter exactly once | `test_successful_completion_increments_completion_counter_exactly_once_and_records_latency`, `test_stale_finalize_does_not_double_count_completion` |
| 5 | Failure increments correct counter | `test_permanent_failure_increments_jobs_failed`, `test_transient_failure_increments_jobs_retried`, `test_exhausted_retries_increments_jobs_permanently_failed` |
| 6 | Reclaim increments reclaim counter | `test_reclaim_increments_reclaim_counter` |
| 7 | `active_jobs` returns to zero | asserted in every counter test above |
| 8 | Latency is recorded | `test_successful_completion_increments_completion_counter_exactly_once_and_records_latency` |
| 9 | Health summary is emitted | `test_health_summary_is_emitted_with_expected_fields`, `test_health_summary_not_emitted_before_interval_elapses` |
| 10 | Redis pending/stream metrics read correctly | `test_redis_health_snapshot_reads_stream_and_pel_state`, `test_redis_health_snapshot_empty_stream` |
| 11 | Resource metrics are collected | `test_resource_sampler_collects_expected_fields` |
| 12 | Shutdown summary is emitted | `test_shutdown_summary_is_emitted` |
| 13 | SIGTERM still calls `Worker.stop()`, doesn't ACK | `test_sigterm_handler_still_only_calls_stop` (regression guard specific to this phase's changes; the deeper guarantee remains covered by Phase 13B's own `test_signal_handlers_call_worker_stop_and_only_stop` and `tests/test_crash_recovery.py::test_graceful_shutdown_does_not_ack_unfinished_work`, both still passing unmodified) |
| 14 | Run JSON generated for clean shutdown | `test_run_json_is_generated_atomically_for_clean_shutdown` |
| 15 | Incomplete/crashed run not falsely marked successful | `test_incomplete_run_is_not_falsely_marked_successful` (real subprocess, real `SIGKILL`) |
| extra | Malformed-job rejection doesn't inflate `jobs_claimed`, logs safely | `test_malformed_job_rejection_increments_rejected_not_claimed` |

Plus `test_worker_process_emits_worker_started_and_worker_ready_as_valid_json`
which parses the real subprocess's stdout as JSON lines end-to-end.

### Testing discipline — exact commands and counts run this session

```
python -m pytest tests/test_worker_observability.py -q
  -> 20 passed

python -m pytest tests/test_worker_main.py tests/test_worker.py \
    tests/test_crash_recovery.py tests/test_retry.py -q
  -> 47 passed

python -m pytest -q   (full suite)
  -> 248 passed, 0 failed, 0 skipped
```

No pre-existing failures were hidden; there were none to hide (228 passed
pre-Phase-13C, 248 passed after — the +20 delta is exactly the new file;
every other file's test count is unchanged).

### Files changed

- `worker/observability.py` (new — JSON logging, counters, latency,
  resource sampler, Redis health snapshot, `ObservingWorkerObserver`,
  atomic run-record/marker file writes)
- `worker/fingerprint_worker.py` (additive only: `error_type` on
  `TransientFailure`/`PermanentFailure`, `claimed_at` on `ClaimedEntry`,
  new no-op `WorkerObserver` base class, an `observer` constructor
  parameter defaulting to `None`, observer calls at existing
  claim/reclaim/finalize boundaries, one `observer.on_loop_tick()` call in
  `run()`'s loop — no claim/lease/retry/commit logic changed)
- `worker/matching_handler.py` (additive only: optional `stage_recorder`
  parameter, `time.monotonic()` pairs around existing stage boundaries,
  `error_type=type(exc).__name__` added to every existing
  `raise Transient/PermanentFailure(...)` call — messages unchanged)
- `worker/main.py` (JSON logging, config snapshot logging, observer
  construction and wiring into `Worker`/`build_matching_handler`, startup
  marker, shutdown summary + run record write, two new config variables)
- `tests/test_worker_observability.py` (new, 20 tests)
- `docs/architecture/phase-13-production-hardening.md` (this section)

### Enabling this in a real run

```
WORKER_OBSERVABILITY_INTERVAL_MS=60000 \
WORKER_RUN_OUTPUT=/var/run/fingerprinter/worker-1.json \
python -m worker.main
```

Structured JSON logs go to stdout unconditionally (no flag needed — this
phase makes JSON the worker's only log format, same as Phase 13B's plain
logs were the only format before). `WORKER_RUN_OUTPUT` is the only opt-in:
unset, no file is ever written, matching this phase's "no file I/O unless
asked" default.

### Known limitations

- **Pipeline stage timing cannot separate cache-hit from build-on-miss**
  inside `target_resolution`, nor "download" from "probe" inside
  `media_acquisition`, without a hook into `target/registry.py`/
  `acquisition/acquirer.py` this phase judged not worth the off-limits
  exception (see "Pipeline stage latency" above). **UNAVAILABLE** at that
  finer granularity, not fabricated.
- **Retry-redelivery is not a distinct counter/event from a fresh claim**
  (see "Counters" above) — recoverable from `entry.attempt` on the
  existing `job_claimed` event, not a separate first-class metric.
  **DEFERRED**, judged not to need its own event.
- **Fleet-wide aggregation is not implemented** — every metric here is
  worker-local or single-consumer-scoped (see "Multi-worker/multi-host
  correctness"). Explicitly **DEFERRED**, per the task brief.
- **`WORKER_RUN_OUTPUT`'s marker-vs-record reconciliation is a manual
  operator/tooling responsibility** — this phase writes the two files
  correctly and atomically but does not ship a reader/analyzer for them
  (the task brief explicitly disallows building a dashboard).
- Redis HA/TLS/connection-pool tuning, GPU-specific metrics (`nvidia-smi`/
  CUDA memory), Kubernetes/systemd integration, and multi-host target
  cache remain **DEFERRED**, unchanged from Phase 13B's own list — none of
  those are in this phase's scope either.
- The health/run-record `redis` section, when populated, reflects a
  **single point-in-time snapshot** taken at the moment of the health tick
  or shutdown — not a time series. An operator wanting backlog-over-time
  needs to collect successive `worker_health` log lines (already
  timestamped) from wherever the JSON logs are shipped to, which this
  phase does not build a collector for (explicitly out of scope — "do not
  create a dashboard").

### Is blocker #4 resolved?

**Yes, to the extent a single-worker, log/file-based observability layer
can resolve it without a metrics backend.** Every question §7 of the
Phase 13 audit named as unanswered is now answerable from real structured
output this codebase produces on its own: throughput (`jobs_completed`
count + `latency` stats over a time window of `worker_health` lines),
backlog (`redis.stream_length`/`group_pending`/`group_lag` in the same
lines), resource usage (`resources.rss_peak_bytes`/`cpu_percent`/
`thread_count`), and structured, safely-loggable error detail
(`error_type`/`error_category` per failed job, not just a Redis
`failure_reason` string). What remains missing is exactly what the task
brief scoped out: a metrics backend/dashboard/alerting system to *consume*
this output automatically — this phase produces the signal, not the
alerting pipeline on top of it, per explicit instruction ("Do NOT add a
large monitoring framework", "Do NOT create a dashboard").

### Recommendation for Phase 13D

Per §17's original ordering, the one remaining blocker is **#2, the
multi-host target cache** — now the last blocker precisely because, per
Phase 13B's own recommendation, it needs a second real worker process to
test against, which has existed since Phase 13B and is now additionally
observable (a Phase 13D implementation could use this phase's
`pipeline_stage_metrics.target_resolution` timings and `redis_health_
snapshot` output directly to validate whatever cache-coherence fix it
adds, rather than needing to build throwaway instrumentation for that
validation from scratch).

---

**Phase 13 audit + Phase 13A SSRF hardening + Phase 13B production worker
entrypoint + Phase 13C production observability are all reflected above.
The one remaining blocker (#2, multi-host target cache) is unimplemented —
awaiting instruction before starting it.**

**Update, Phase 13D:** blocker #2 has a deeper audit
(`docs/architecture/phase-13d-multi-host-target-cache-audit.md`) and an
implementation (`docs/architecture/phase-13d-distributed-target-
artifacts.md`) — see that document for the shared-storage backend, the
target-media fleet-accessibility fix, and the multi-host simulation test
this section's "Recommendation for Phase 13D" anticipated.
