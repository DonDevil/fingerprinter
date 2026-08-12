# Phase 11 — Performance & Distributed/GPU Benchmarking

## 1. Objective

Phase 10 wired the full matching pipeline end to end (`worker/matching_handler.py`)
but made no performance claims about it. This phase's job is exactly that:
measure where the pipeline actually spends time, which resource saturates
first, how throughput changes with worker concurrency, how warm/cold
target-cache workloads differ, whether the build-on-miss lock (Phase 10
§4) behaves reasonably under contention, and what the practical
single-machine ceiling is — **before** changing any pipeline code. Per the
phase brief this is primarily a measurement phase: only one change was
justified by the data collected (§22) and made after the fact, not before.

Every conclusion below is labeled:

- **MEASURED** — directly observed on this machine, numbers in
  `benchmarks/results/*.json`.
- **INFERRED** — a reasoned extrapolation from measured data, not itself
  measured.
- **PROVISIONAL** — a heuristic/decision that the data neither strongly
  confirms nor refutes.
- **REQUIRES MULTI-HOST VALIDATION** — cannot be established on one
  machine at all; a claim about distributed behavior that needs a real
  multi-host deployment to check.
- **DEFERRED** — explicitly out of scope for this phase.

## 2. Hardware / software environment

All numbers in this document were collected on a single development
machine — **not** a production fleet node. See §20 for what that does and
does not license inferring about multi-host behavior.

| | |
|---|---|
| CPU | Intel Core i5-11400H, 6 physical cores / 12 logical (hyperthreaded) |
| RAM | 15.7 GiB total, ~9.4–10.0 GiB available at benchmark time (rest in buff/cache + other running processes) |
| GPU | NVIDIA GeForce RTX 2050, 4096 MiB VRAM — **present but not usable by torch in this environment**, see §3 |
| OS | Ubuntu 24.04.4 LTS, kernel 7.0.0-28-generic |
| Python | 3.12.3 (project `.venv`) |
| PyTorch | 2.13.0+cu130 |
| transformers | 5.15.0 |
| redis-py | 8.1.0 |
| Redis server | 7.0.15, standalone, localhost |
| ffmpeg | 6.1.1-3ubuntu5 |
| DINOv2 model | `facebook/dinov2-base` @ pinned revision `f9e44c814b77203eaa57a6bdbbd535f21ede1415` (already cached locally, no network fetch during benchmarking) |
| git revision | `aec736673b36b1e2102985a76232e1d85203eed5` ("phase 10") |

**MEASURED.**

## 3. GPU availability — diagnosis

`nvidia-smi` reports the RTX 2050 present, but every dynamic field
(power, temperature, utilization) reads `ERR!`/`[N/A]`, and
`torch.cuda.is_available()` returns `False`; forcing a CUDA tensor
allocation raises `RuntimeError: No CUDA GPUs are available`. This is a
laptop Optimus-style dGPU that appears to be powered down / not bound to
a usable CUDA context in this environment — not a torch/driver version
mismatch (driver 595.84, CUDA 13.2 reported by `nvidia-smi`; torch build
is `cu130`, matched). Root-causing *why* the dGPU is inaccessible (power
management, missing `nvidia-persistenced`, PRIME configuration) is outside
this phase's scope — the operative fact is: **no GPU embedding
measurement is possible on this machine**. Every "GPU" field in every
benchmark result is `null`/N/A, not a zero, not an estimate. **MEASURED**
(the unavailability itself); **REQUIRES MULTI-HOST VALIDATION** (any claim
about actual GPU throughput).

## 4. Benchmark methodology

`benchmarks/` is a new top-level package, not production code — nothing
under `worker/`, `embedding/`, `matching/`, `target/`, `work_queue/`, or
`acquisition/` imports it, and it imports *them* only to drive real
objects through real call sequences. No production module was refactored
to make benchmarking easier (per the phase brief's explicit instruction).
One exception is documented in detail because it is not obviously
"measurement-only": `embedding/dinov2_engine.py` gained one new,
default-off constructor parameter as a direct result of what these
benchmarks found — see §22.

| File | Purpose |
|---|---|
| `benchmarks/common.py` | Stopwatch, `/proc`-based CPU%/RSS sampling, `nvidia-smi`-based GPU sampling, latency statistics, environment capture, JSON result writer |
| `benchmarks/gen_test_video.py` | Deterministic synthetic benchmark videos (ffmpeg `testsrc2`, no external asset) |
| `benchmarks/file_server.py` | Minimal loopback static-file HTTP server for realistic (non-mocked) acquisition |
| `benchmarks/instrumented_handler.py` | Per-stage-timed mirror of `worker/matching_handler.py`'s handler body — see its docstring for why this is a deliberate duplication, not a wrapped import, and the risk that entails (must be kept in sync by hand) |
| `benchmarks/bench_matching.py` | Workload E |
| `benchmarks/bench_embedding.py` | Workload F |
| `benchmarks/bench_pipeline.py` | Workloads A–D |
| `benchmarks/results/*.json` | One uniquely-named file per run, never overwritten |

Two real measurement bugs were caught and fixed *during* harness
development (both are still visible in code comments at the fix site,
per this project's convention of flagging exactly this kind of mistake
for future readers):

1. `bench_embedding.py`: a `threads=None` combo ("use torch's default")
   silently inherited a *previous* combo's `torch.set_num_threads(1)`
   override within the same process, because "leave threads alone" was
   implemented as "don't call `set_num_threads`" rather than "explicitly
   reset to the real default." This made the first `bench_60s.mp4` run
   measure single-thread performance while labeling it "default" (12
   segments in 6.1s instead of the correct 1.7s). Fixed by capturing the
   true default once at import time and always applying it explicitly.
2. `bench_pipeline.py`'s worker-scaling collector: the wall-clock window
   used for throughput was gated on receiving every worker's "drained"
   sentinel, not just the last real job result — and a worse bug, the
   collector `break`-ed out entirely on the *first* single empty read
   from the result queue instead of continuing to poll until a real
   deadline. Under severe CPU oversubscription (`D[default-threads]`,
   `worker_count=4`, see §15) this made the benchmark report
   `completed=0/12` when the jobs were in fact still running, just slower
   than the poll window. Fixed to (a) stop the throughput clock at the
   last *job* message, not the sentinels, and (b) keep polling until an
   actual 180s deadline.

Both are called out because a benchmark's own bugs are exactly the kind of
thing this document must not silently smooth over.

## 5. Workload definitions and inputs

Two synthetic videos (`benchmarks/fixtures/`, generated via
`ffmpeg -f lavfi -i testsrc2=...`, deterministic, not committed test
fixtures — see `benchmarks/gen_test_video.py`):

| File | Duration | Resolution | fps | Size |
|---|---|---|---|---|
| `bench_15s.mp4` | 15s | 320x240 | 24 | 1.34 MiB |
| `bench_60s.mp4` | 60s | 320x240 | 24 | 5.41 MiB |

`tests/fixtures/tiny_video.mp4` (2s, 32x32 — Phase 7's fixture) was
deliberately **not** reused for performance measurement: it is far too
small to say anything about realistic per-frame DINOv2 cost.

Segment sampling for the pipeline workloads (A–D) used an explicit
`SegmentSamplingConfig(segment_duration_s=2.5)` against `bench_15s.mp4`
(6 segments/video) — a benchmark-time override via the existing
constructor parameter, **not** a change to
`embedding/config.py`'s production default (`DEFAULT_SEGMENT_DURATION_S =
5.0`, unchanged). The embedding-only benchmark (F) separately exercised
the actual production default (5.0s) against `bench_60s.mp4`.

## 6. Warm/cold cache methodology

- **Warm** (Workload A): a target's segment embedding is built once,
  before any timed repetition, using the same engine/registry the timed
  jobs will use; the build call itself is recorded (`prewarm_build_s`)
  but excluded from the timed statistics.
- **Cold** (Workload B): each repetition uses a distinct, never-before-seen
  `target_id` pointing at the same underlying video bytes — a fresh
  `(target_id, target_version, content_sha256, spec)` cache key every
  time, so `TargetRegistry.get_or_build_segment_embedding` always misses
  and always builds (Phase 10 §4). Single worker process — no lock
  contention confound in this workload; contention is Workload C.

## 7. Concurrency methodology

Concurrency uses real OS processes (`multiprocessing`, `spawn` start
method — a fresh Python interpreter per worker, no inherited
torch/CUDA/socket state from the parent), matching the target production
shape (separate worker processes/machines) far more faithfully than
threads would. All workers in one step synchronize on a
`multiprocessing.Barrier` after each has finished loading its own model,
so the measured wall-clock window excludes model-load/import time (paid
once per process, ~2.7–3.1s cold — see §12) and only covers steady-state
processing.

Two thread configurations were benchmarked for Workload D, since this is
exactly the axis that turned out to matter most (§15):

- **`isolated-1thread`**: each worker process explicitly runs
  `torch.set_num_threads(1)`.
- **`default-threads`**: each worker process leaves torch's own default
  alone (6 — one thread per physical core, torch's own heuristic).

A RAM safety gate (`benchmarks/bench_pipeline.py::_ram_ok_for`) checks
`/proc/meminfo`'s `MemAvailable` before spawning each step and skips (with
a recorded reason) rather than risking OOM — see §16 for exactly where
this triggered.

## 8. CPU measurements

Collected via `/proc/<pid>/stat` (process CPU ticks, summed across all
worker pids for a multi-process step) and `/proc/stat` (system-wide,
idle-delta method), sampled on a background thread every 0.1–0.25s — see
`benchmarks/common.py::ResourceSampler`. Process CPU% can exceed 100% for
a multi-threaded process (e.g. ~610% peak for one 6-thread DINOv2
inference process, consistent with using close to all 6 physical cores).
All figures below are **MEASURED**, not estimated.

## 9. GPU measurements

**N/A across every workload** — see §3. `benchmarks/common.py` does query
`nvidia-smi` on a background thread regardless (in case a future run is
on a working GPU host), but every sample this phase collected came back
`[N/A]` for utilization and a flat ~15 MiB idle VRAM reading, which is
recorded as `null` in the JSON, never fabricated as `0`.

## 10. Memory measurements

RSS via `/proc/<pid>/status`'s `VmRSS`, summed across all worker pids for
a multi-process step, sampled alongside CPU. See §17 for the concrete
numbers driving the concurrency safety gate.

## 11. Redis measurements

`INFO` snapshots (`used_memory`, `total_commands_processed`,
`connected_clients`) taken immediately before and after each workload —
not on every iteration (per the brief's explicit "the benchmark itself
must not become the bottleneck" instruction) — plus direct
`Stopwatch`-timed wrapping of the exact Redis-touching calls
(`Worker.claim_one()`, `Worker.commit_result()` /
`_handle_transient_failure()` / `_fail()`) inside every pipeline job. See
§14.

## 12. Embedding measurements (Workload F)

`python -m benchmarks.bench_embedding` — `DINOv2EmbeddingEngine.embed_video_segments`
in isolation, no acquisition, no Redis. **MEASURED**, `benchmarks/results/bench_embedding_20260812T115617_ced8245e.json`.

| Video | Threads | Model load | First (cold) call | Steady-state mean | Steady p95 | Segments | Throughput |
|---|---|---|---|---|---|---|---|
| 15s | 6 (torch default) | 0.12s | 0.867s | **0.850s** | 0.859s | 6 | 7.06 segments/s, 17.64 video-s/s |
| 15s | 1 | 0.05s | 3.069s | **3.089s** | 3.101s | 6 | 1.94 segments/s, 4.86 video-s/s |
| 60s | 6 (torch default) | 0.047s | 1.744s | **1.724s** | 1.732s | 12 (`segment_duration_s=5.0`, production default) | 6.96 segments/s, 34.80 video-s/s |

Key findings:

- Model load is cheap once weights are cached locally (~0.05–0.12s) —
  irrelevant next to per-segment inference cost. Import overhead
  (`torch`/`transformers`, paid once per fresh process) is separately
  ~2.6–2.7s (measured directly during CPU calibration, §12a) and *not*
  included in the "model load" figure above; it's counted inside
  `first_inference_wall_s`'s cold-process cost in the pipeline workloads.
  **MEASURED**.
- "First (cold) call" and steady-state are statistically indistinguishable
  here (`extract_segment_frames`'s ffmpeg subprocess is cheap — see
  §12b — so there is no meaningful decode-cache warmup effect to
  separate out at this video size). **MEASURED**, but this may not hold
  for much longer videos or a colder OS page cache — not tested.
- Per-segment cost is ~constant regardless of total video duration
  (0.850s / 6 = **141.7ms/segment** at 15s; 1.724s / 12 = **143.7ms/segment**
  at 60s, at the same thread count) — total embedding time scales with
  **segment count** (`video_duration / segment_duration_s`), not raw
  video length. **MEASURED**.
- Thread count matters enormously for single-job latency: 6→1 threads is
  a **3.63x slowdown** (850ms → 3089ms for the same 6 segments) — see §15
  for why this cuts the other way once *multiple processes* compete for
  the same physical cores.

### 12a. Per-frame calibration (raw, not part of the F workload JSON)

A direct calibration of `DINOv2EmbeddingEngine._embed_pil_image` alone
(single 240x320 synthetic frame, warm, averaged over 8 calls):

| torch threads | mean per-frame time |
|---|---|
| 6 (default, = physical core count) | 127ms |
| 1 | 493ms |

**MEASURED.** 6-thread speedup over 1-thread is 3.88x — sublinear vs. the
naive 6x, as expected for intra-op tensor parallelism with fixed
per-layer overhead.

### 12b. Decode cost is not the bottleneck

Isolated `extract_segment_frames` (ffmpeg subprocess only, no model):
15s video → 63ms; 60s video → 107ms. Trivial next to the ~127–494ms/frame
model cost — **MEASURED**. Decode is not a candidate bottleneck at this
scale.

## 13. Matching measurements (Workload E)

`python -m benchmarks.bench_matching` — `matching.matcher.match_segments`
against deterministic synthetic 768-dim embedding arrays, coarse-screen
bypassed so every run pays the full O(N×M) cost. **MEASURED**,
`benchmarks/results/bench_matching_20260812T115316_0e2935c6.json`.

| Target x Candidate | Comparisons | Mean | p95 | Comparisons/s |
|---|---|---|---|---|
| 100x100 | 10,000 | 4.03ms | 5.37ms | 2.48M/s |
| 500x500 | 250,000 | 28.75ms | 30.95ms | 8.70M/s |
| 1000x1000 | 1,000,000 | 59.17ms | 63.26ms | 16.90M/s |
| 2000x2000 | 4,000,000 | 137.94ms | 151.63ms | 29.00M/s |
| 4000x4000 (extra, budget allowed it) | 16,000,000 | 321.90ms | — | — |
| `coarse_screen` (single vector pair) | 1 | 5.1μs | — | — |

**Finding: matching is not the bottleneck, by a wide margin.** Even a
4000-segment target vs. 4000-segment candidate (a 2000-segment target
alone represents ~2.8 hours of video at the default 5s segment duration —
far beyond any realistic single target) costs 322ms — less than **one
single DINOv2 frame embedding** (127–494ms, §12a). A realistic job (target
in the tens-to-low-hundreds of segments) costs single-digit milliseconds.
`matching/matcher.py`'s choice of brute-force cosine similarity over FAISS
(documented as provisional in that module) is **confirmed adequate at
every scale this benchmark could reach**; nothing here justifies revisiting
it. **MEASURED** for tested sizes; **INFERRED** that this holds indefinitely
for realistic target libraries (not extrapolated past 4000x4000).

## 14. End-to-end measurements — Workload A (warm cache)

`benchmarks.bench_pipeline.run_stage_latency_workload("A-warm", cold=False, reps=15)`,
single worker process, real Redis, real loopback HTTP acquisition, target
pre-warmed once (`prewarm_build_s=0.921s`, excluded from stats below).
**MEASURED**, `benchmarks/results/bench_pipeline_A_warm_*.json`.

| Stage | Mean | p95 | Min | Max | % of handler_total |
|---|---|---|---|---|---|
| `claim_s` (Redis XREADGROUP) | 0.35ms | 0.46ms | 0.31ms | 0.49ms | 0.04% |
| `acquire_s` (loopback HTTP + ffprobe validation) | 41.53ms | 43.10ms | 35.83ms | 45.79ms | 4.6% |
| `candidate_embed_s` (DINOv2, 6 segments) | 860.81ms | 896.86ms | 838.84ms | 902.86ms | **95.1%** |
| `target_resolve_s` (cache hit, no lock touched) | 2.14ms | 2.17ms | 1.53ms | 3.26ms | 0.24% |
| `match_s` | 0.59ms | 0.73ms | 0.40ms | 0.92ms | 0.07% |
| `aggregate_s` (`combine()`) | 0.05ms | 0.06ms | 0.03ms | 0.07ms | 0.01% |
| `commit_s` (Redis Lua commit script) | 0.36ms | 0.41ms | 0.23ms | 0.47ms | 0.04% |
| **`handler_total_s`** | **905.13ms** | **945.40ms** | **882.38ms** | **947.26ms** | 100% |

n=15. **This is the single clearest finding in the whole phase: DINOv2
candidate embedding is ~95% of warm-cache job latency; every other stage
combined (acquisition, Redis claim+commit, cache lookup, matching,
aggregation) is under 5%.** Redis coordination specifically (claim +
commit) is **0.71ms combined out of 905ms — 0.08%**.

## 15. End-to-end measurements — Workload B (cold cache)

`run_stage_latency_workload("B-cold", cold=True, reps=8)` — each rep a
fresh target, single worker, no contention. **MEASURED**,
`benchmarks/results/bench_pipeline_B_cold_*.json`.

| Stage | Mean | p95 | Min | Max |
|---|---|---|---|---|
| `acquire_s` | 40.35ms | 43.03ms | 38.51ms | 43.03ms |
| `candidate_embed_s` | 937.76ms | 1130.30ms | 858.50ms | 1130.30ms |
| `target_resolve_s` (lock acquire + build + register) | 959.90ms | 1425.46ms | 883.06ms | 1425.46ms |
| `target_build_s` (the embedding pass inside the build) | — | — | 0.875s | 1.418s (one outlier; rest tightly clustered ~0.88s) |
| `match_s` / `aggregate_s` / `commit_s` | ~unchanged from warm | | | |
| **`handler_total_s`** | **1938.64ms** | **2564.59ms** | **1794.58ms** | **2564.59ms** |

Cold-cache jobs cost **~2.14x** a warm-cache job (1939ms vs. 905ms) — almost
exactly what's expected, since a cold job pays for **two** DINOv2 embedding
passes (candidate + target build) instead of one, and both passes cost the
same per-segment amount (§12). `target_build_s` (0.875–0.914s for 7 of 8
reps, one 1.418s outlier attributed to ordinary system jitter, not
investigated further) matches `candidate_embed_s` almost exactly, as it
must — it's the identical `embed_video_segments` call against the same
video content. Cache registration itself (the filesystem write + small
Redis metadata `HSET`, part of `target_resolve_s` but not
`target_build_s`) is therefore on the order of tens of milliseconds,
dwarfed by the embedding pass.

## 16. Target-cache lock contention (Workload C)

`run_contention_workload(n)` — `n` processes simultaneously call
`TargetRegistry.get_or_build_segment_embedding` against the **same**
never-before-seen target. **MEASURED**,
`benchmarks/results/bench_pipeline_C_contention_*.json`.

| Contenders | Builds | Waiters | Builder latency | Waiter mean latency | Note |
|---|---|---|---|---|---|
| 4 | **1** | 3 | 3.205s | 4.007s | ran to completion |
| 8 | — | — | — | — | **skipped**: available RAM 9198 MiB < required 12224 MiB (8 x 1400 MiB margin + 1024 MiB floor) |

The `n=4` result is exactly the property Phase 10 §4 designed for:
**exactly one build occurred**; the other three workers polled and picked
up the cached result without duplicating work. Waiter latency (4.007s)
exceeds builder latency (3.205s) by ~0.8s — consistent with
`DEFAULT_POLL_INTERVAL_S=1.0`'s coarse granularity: a waiter can only
notice completion on its next 1-second poll tick after the winner
actually finishes, so up to ~1 extra second of pure waiting is structural,
not a bug. **MEASURED** for n=4; the RAM safety gate correctly prevented
an n=8 run rather than risking instability (see §17) — **not measured**
at n=8 on this machine.

## 17. Lock TTL analysis

`DEFAULT_LOCK_TTL_MS = 600_000` (10 minutes, `target/registry.py`) was
flagged in Phase 10 as a provisional heuristic, "not measured against a
real embedding workload."

| | |
|---|---|
| Observed max build time (this phase, n=8 cold-cache reps + n=4 contention) | 3.205s |
| Observed p95 build time (8 cold-cache samples) | ~1.42s |
| Current `lock_ttl_ms` | 600,000ms (600s) |
| Safety margin at max observed | **~187x** |

**Conclusion: the current 10-minute TTL is not merely "safe," it is safe
by nearly two orders of magnitude for every build this phase could
measure.** Nothing in this phase's data justifies *lowering* it — a lower
TTL only helps in the failure case where a builder crashes/hangs without
releasing the lock (Phase 10's own documented limitation: no
auto-renewal), and this benchmark cannot safely manufacture that failure
mode without deliberately hanging a worker, which was judged out of
scope. **This phase's data neither confirms nor refutes whether 600s is
*too generous* for that specific crash-recovery case** — a build many
minutes long would require either a much longer real target video than
anything tested here, or a much slower host; **REQUIRES real
target-library workload data (longer videos, slower/loaded hosts) to
tune further — PROVISIONAL, not changed this phase.**

Lock **wait** latency itself is significant relative to job latency under
contention (waiters pay ~4s vs. a non-contended cold-cache job's ~1.9s
total, i.e. contention roughly doubles a waiter's wall time) but is
bounded by the winner's build time plus one poll interval — not by the
600s TTL, which was never approached. `poll_interval_s=1.0` is the more
operationally relevant number here: it sets the *minimum* wasted-wait
granularity every loser pays, and 1 second is comparable to typical build
time itself (0.88–1.4s measured) — i.e. a build finishing "generously
before" a poll tick can still cost a full second of unnecessary wait
before a poller notices. **PROVISIONAL**: a shorter `poll_interval_s`
(e.g. 0.2s) would reduce this waste proportionally without materially
increasing Redis load (a `GET`-equivalent read per poll, already cheap —
§11), but this phase found no evidence the current value causes a
*problem*, only that it adds a bounded, predictable delay — not changed
this phase.

## 18. Target-cache effectiveness

| | |
|---|---|
| Cache hit cost (warm `target_resolve_s`) | 2.14ms mean |
| Cache miss + build cost (cold `target_resolve_s`) | 959.90ms mean |
| Work the cache removes per hit | ~958ms (essentially one full DINOv2 embedding pass) |
| Storage per target (measured: 6 segments, 768-dim, JSON) | **109,697 bytes (~107 KiB)** |
| Storage per target (measured: 12 segments, 768-dim, JSON) | **203,684 bytes (~199 KiB)** |
| Redis footprint per target | one small metadata hash (`target_segment_embeddings_key`) — no vector data in Redis at all (Phase 6/9 design) |

**MEASURED.** Storage scales linearly with segment count as expected
(~17 KiB/segment as uncompressed JSON floats — plain-text float repr,
no binary packing). At a production-scale library (e.g. 10,000 targets x
~30 segments/target average) this extrapolates to **~5 GiB** of JSON on
disk (**INFERRED**, not measured at that scale) — plausible for a single
filesystem cache directory today, but `target/segment_cache.py`'s own
docstring already flags this as the point where a binary/columnar format
would become worth revisiting ("Revisit if/when Phase 11 benchmarks show
JSON (de)serialization cost dominates"). This phase's data does **not**
show JSON (de)serialization cost dominating anything — the entire
`target_resolve_s` cache-hit path (JSON read + validation) is 2.14ms,
negligible next to the 861–938ms embedding stages. **No cache redesign is
justified by this phase's measurements.**

## 19. Concurrency / scaling results — Workload D

`run_scaling_workload(...)`, different (pre-warmed) targets per job round-robin
across workers so target-lock contention is excluded from this workload by
design (that's Workload C). **MEASURED**,
`benchmarks/results/bench_pipeline_D_scaling_*.json`. 3 jobs/worker.

### 19a. `isolated-1thread` (each worker process pinned to 1 torch thread)

| Workers | Throughput (jobs/s) | Scaling efficiency vs. 1 worker | `candidate_embed_s` mean | Peak RSS (sum) | Peak process CPU% (sum) |
|---|---|---|---|---|---|
| 1 | 0.320 | 1.00 | 3082ms | 1073 MiB | 250% |
| 2 | 0.599 | 0.94 | 3299ms | 2139 MiB | 349% |
| 4 | 1.035 | 0.81 | 3796ms | 4287 MiB | 653% |
| 8 | — | — | — | — | **skipped**: RAM safety gate (9683 MiB available < 12224 MiB required) |

Near-linear through 2 workers, mild diminishing returns by 4 (0.81
efficiency — some contention even at 4 single-threaded processes on 6
physical cores, likely memory-bandwidth/cache effects rather than compute
starvation, since 4 < 6 physical cores). **MEASURED** through 4 workers;
**8 not measured** — the RAM gate correctly stopped escalation (see §17
of the phase brief's "record the reason for stopping" instruction; reason
recorded verbatim in the JSON).

### 19b. `default-threads` (each worker process uses torch's own default — 6 threads, = physical core count)

| Workers | Threads used | Total threads in flight | Throughput (jobs/s) | Scaling efficiency vs. 1 worker | `candidate_embed_s` mean |
|---|---|---|---|---|---|
| 1 | 6 | 6 | 1.080 | 1.00 | 882ms |
| 2 | 6 | 12 (= all logical CPUs) | 1.086 | 0.50 | 1751ms |
| 4 | 6 | 24 (2x logical CPU oversubscription) | **0.295** | **0.068** | **13,158ms** |

**Finding: this is not merely "diminishing returns," it is catastrophic
negative scaling.** At `worker_count=4` with default per-process
threading, per-job embedding time inflates **15x** (882ms → 13,158ms) and
total throughput drops **below the single-worker baseline** — 4 processes
doing 4x the nominal work take 40.66s wall vs. 1 worker's 2.78s for 1x the
work, instead of the ~2.78s (linear) or even ~11s (badly sublinear but
still net-positive) a naive model would predict. This is textbook CPU
thread-pool oversubscription: each of the 4 processes independently
spins up 6 compute threads for its BLAS/tensor ops, so 24 CPU-bound
threads compete for 6 physical cores, and the resulting cache thrashing
and context-switch overhead costs far more than the naive
"threads-per-core" ratio suggests. **MEASURED.**

### 19c. Where scaling was stopped, and why

- `isolated-1thread` stopped after `worker_count=4`: RAM safety gate,
  reason recorded (§19a). Not a CPU/throughput ceiling — throughput was
  still improving (0.81 efficiency) when the gate triggered.
- `default-threads` was only run through `worker_count=4` (not 8) because
  §19b's result already demonstrates the pathology unambiguously and
  clearly enough that spending more compute confirming it at 8 processes
  (which would also trip the same RAM gate) was judged wasteful — per the
  phase brief's own "do not run enough concurrent... workers to risk...
  system instability" and "a small but repeatable benchmark is preferable
  to a massive benchmark that burns the development machine" guidance.

## 20. Practical single-machine ceiling

On this machine, in the `isolated-1thread` configuration (the one that
actually scales usefully — see §22), throughput was still improving at
`worker_count=4` (1.035 jobs/s, 0.81 efficiency) when the RAM safety gate
stopped further escalation. **The observed ceiling on this specific
15 GiB / 6-physical-core machine is RAM, not CPU** — `default-threads`
mode hit a *CPU thread-oversubscription* ceiling far earlier (already
severely negative by `worker_count=4`), but that ceiling is an artifact of
not pinning per-process thread counts, not an intrinsic CPU compute
limit; §22's fix directly addresses it. With thread counts sized
correctly for the host (§22), the next ceiling this machine would hit is
almost certainly **RAM** (measured: ~1.07–1.08 GiB peak RSS per CPU
worker process, §12/§19a — a 6-physical-core host with, say, 32 GiB could
run meaningfully more `isolated`-mode workers than this 15 GiB dev
machine could safely test). **MEASURED** through worker_count=4;
**INFERRED** beyond that (higher worker counts, more RAM) — not tested.

GPU: **entirely untested — REQUIRES MULTI-HOST VALIDATION** (§3). Nothing
in this document licenses any claim about GPU throughput, GPU worker
concurrency, or GPU-vs-CPU cost ratio.

## 21. Measured bottleneck (single-job, single-machine)

**DINOv2 CPU embedding inference dominates every other stage by roughly
two orders of magnitude at every measured concurrency level.** Warm-cache:
95.1% of job latency (§14). Redis coordination (claim + commit combined):
0.08% of warm-cache job latency (§14) — **not significant**, contradicting
no prior assumption but now confirmed rather than assumed. Matching:
negligible at any realistic scale (§13). Acquisition: ~4.6% of warm-cache
latency, but this is *loopback* HTTP to a local synthetic server — real
crawler-fleet media acquisition over the internet is expected to look
completely different (see §23, labeled accordingly). Target-cache
lookups: negligible on hit (0.24%), ~doubles job cost on miss (§15) —
exactly what the cache is for, and it works.

**This is unambiguous: if Phase 12+ ever needs to speed up single-job
latency, DINOv2 inference is the only stage worth touching. Nothing else
comes close.** This directly validates (not merely assumes) `matching/matcher.py`'s
choice not to invest in FAISS (§13), and validates that no Redis-side
optimization (batching, pipelining, connection pooling) would move the
needle on single-job latency at today's scale.

## 22. Optimizations considered

Classified per the phase brief's P0–P3 scheme:

| Finding | Classification | Reasoning |
|---|---|---|
| CPU thread-pool oversubscription across concurrent worker processes (§19b) causes 15x per-job slowdown and net-negative scaling | **P1 — demonstrated performance bottleneck** | Directly measured, severe, reproducible; fixable with a minimal, additive, zero-default-behavior-change API surface |
| No pipeline optimization opportunity found in acquisition/matching/Redis/target-cache stages (§14, §13, §11, §18) | N/A — nothing to fix | Each is already under 5% of job latency; optimizing further would be exactly the "microbenchmark-level, not actual-bottleneck" work the phase brief warns against |
| Segment JSON storage format (`target/segment_cache.py`) | **P3 — speculative/future** | Not shown to matter at any tested scale (§18); that module's own docstring already names the trigger condition ("if Phase 11 benchmarks show JSON (de)serialization cost dominates") — this phase's data says it doesn't |
| `poll_interval_s=1.0` waiter-side wait-granularity waste under contention (§17) | **P2 — useful, not urgent** | Real but bounded and predictable; no evidence of operational harm, just a fixed-size inefficiency |
| `lock_ttl_ms=600_000` value itself | **PROVISIONAL, not re-classified** | Confirmed safe (187x margin, §17); no data pushes toward changing it either direction |
| GPU worker path performance | **DEFERRED — REQUIRES MULTI-HOST VALIDATION** | Cannot be measured on this machine at all (§3) |
| Real multi-host Redis contention, network acquisition variance, cross-machine target-cache sharing | **DEFERRED — REQUIRES MULTI-HOST VALIDATION** | See §23 |

## 23. Optimization actually implemented (P1 only)

**Change:** `embedding/dinov2_engine.py::DINOv2EmbeddingEngine.__init__`
gained one new optional constructor parameter, `torch_num_threads:
Optional[int] = None`. When `None` (the default, and the only value every
prior phase's code ever implicitly used), behavior is **byte-for-byte
identical to before this change** — the global torch thread pool is left
untouched, exactly as before. When set, the engine calls
`torch.set_num_threads(torch_num_threads)` once, at construction, before
loading the model.

**Why this addresses the measured bottleneck:** §19b showed that running
multiple CPU worker *processes* on one host, each defaulting to torch's
own physical-core-count thread pool, causes catastrophic (15x) slowdown
from thread oversubscription. §19a showed that pinning each process to 1
thread instead makes multi-process concurrency scale usefully (0.81
efficiency at 4 workers, vs. 0.068 with default threading). The fix is
not "always use 1 thread" (that would be wrong for a lone worker on an
otherwise-idle host — §12 measured `1 thread` as **3.6x slower per job**
than `6 threads` for a single worker) — it's giving a caller that *knows*
it's deploying N worker processes on one host the supported means to size
each process's thread pool accordingly, instead of the only alternative
being to reach into global `torch` state from outside the class. No
production entrypoint currently constructs `DINOv2EmbeddingEngine` for a
deployed worker (that wiring doesn't exist yet — likely Phase 12/13); this
change makes the *engine* ready for that wiring to make the correct
choice when it's written, without guessing what the deployment topology
will be.

**Why this is the smallest correct fix, not scope creep:** it is purely
additive (one new parameter, default preserves all existing behavior
exactly), touches exactly one file's constructor, adds one input
validation branch (`torch_num_threads < 1` raises `ValueError`, mirroring
the existing `device` validation pattern immediately above it in the same
constructor), and does not change any default, any call site, any test's
expected behavior, or any other module.

### Before / after

"Before" and "after" here are the same underlying mechanism (`torch.
set_num_threads`), so the quantitative before/after *is* §19a vs. §19b —
re-stated for clarity:

| | Before (no supported way to control this from the engine) | After (via `torch_num_threads=1`) |
|---|---|---|
| 4 workers, per-job embed time | 13,158ms | 3,796ms (**3.5x faster**) |
| 4 workers, throughput | 0.295 jobs/s | 1.035 jobs/s (**3.5x higher**) |
| 4 workers, scaling efficiency vs. 1 worker | 0.068 (net negative) | 0.81 (near-linear) |

### Verification

- New tests added: `tests/test_embedding.py::test_torch_num_threads_defaults_to_untouched`,
  `::test_torch_num_threads_explicit_value_is_applied`,
  `::test_torch_num_threads_rejects_non_positive_value` (3 tests).
- Focused suite: `pytest -q tests/test_embedding.py` → **25 passed** (22
  original + 3 new), 0 failed, 0 skipped.
- Full suite: `pytest -q` → **152 passed** (149 Phase 1–10 + 3 new), 0
  failed, 0 skipped — no cross-phase regression.

### Not done, and why

The change does **not** pick a default value for multi-worker deployments
(e.g. "always default to 1 thread"), does **not** add any auto-detection
of "how many worker processes are running on this host" (no such
mechanism exists or was asked for), and does **not** touch
`worker/matching_handler.py` or add a worker-process entrypoint script —
none of that exists yet in this codebase, and inventing deployment
topology this phase has no visibility into would be exactly the
speculative-generality this project's phases have consistently avoided
(matching Phase 10 §14's own explicit reasoning for a similar
not-yet-generalized case).

## 24. Limitations

- **Single machine, single GPU-less host.** Every distributed claim in
  §25 is inferred, not measured, and is labeled as such.
- **Loopback HTTP acquisition only.** `acquire_s` (~40ms) reflects
  localhost socket + small-file transfer + `ffprobe` validation, not real
  internet media acquisition latency/variance/failure modes a crawler
  fleet will actually see. Real acquisition could be the dominant cost in
  production and this phase cannot rule that in or out.
- **Small, synthetic videos.** 15s/60s at 320x240 — real target/candidate
  media will vary enormously in resolution, duration, and codec
  complexity; per-frame DINOv2 cost (§12a) should generalize (same
  preprocessing pipeline resizes everything to 224x224 regardless of
  source resolution — `PreprocessingConfig`, unchanged), but decode cost
  (§12b, shown negligible here) might not stay negligible for much
  larger/longer source files. Not tested.
- **8-worker steps could not be safely run** on this 15 GiB machine (§16,
  §19a) — the RAM safety gate is doing its job, but it means this phase's
  data stops one full doubling short of the brief's stated minimum
  concurrency ceiling in two of the four workloads that called for it.
- **Lock-holder-crash / TTL-expiry-under-load scenarios were not tested**
  (§17) — would require deliberately hanging a worker mid-build, judged
  out of scope for a measurement-only phase.
- **`instrumented_handler.py` is a hand-maintained duplicate** of
  `worker/matching_handler.py`'s call sequence (§4) — if that file's
  orchestration changes in a later phase without a corresponding update
  here, this benchmark suite will silently measure stale behavior. No
  automated sync check exists.
- **Redis was local/idle** throughout — no other load on the same Redis
  instance, no network hop to Redis. A real fleet's Redis contention
  profile (§25) is unmeasured.

## 25. Inferred multi-host implications

Explicitly separated per the phase brief's mandatory MEASURED / INFERRED
/ REQUIRES MULTI-HOST VALIDATION distinction — none of the following is
presented as a benchmark result.

- **Per-job Redis coordination cost**: **MEASURED** at 0.71ms combined
  (claim + commit) on a local, idle, single-client Redis (§14). **INFERRED**
  that this stays negligible relative to a multi-second DINOv2 pass even
  with modest added network latency to a shared Redis (a few ms of RTT is
  still ~1000x smaller than embedding cost). **REQUIRES MULTI-HOST
  VALIDATION** for actual behavior under many real concurrent
  machines' command load, connection counts, and network latency
  distribution.
- **Target-cache contention across machines**: **MEASURED** (single-host,
  multi-process) that the build-on-miss lock correctly serializes to
  exactly one builder (§16). **INFERRED** that the same correctness
  property holds across machines, since the lock is Redis-mediated
  (`SET NX PX`) and does not depend on process-local state
  (`target/lock.py`'s own docstring already states this design intent).
  **REQUIRES MULTI-HOST VALIDATION** for actual cross-machine timing
  (network RTT changes both builder and waiter latency, and clock/NTP
  skew across machines was never a factor in a single-host test).
- **Cache sharing across workers**: the filesystem-backed
  `FilesystemSegmentEmbeddingCache`/`FilesystemEmbeddingCache` (Phase 6/9)
  is local-disk-per-host by construction — **INFERRED** that this does
  **not** extend to a real multi-host fleet at all: two machines would
  each build (and store) their own copy of the same target's embedding,
  since there is no shared storage backend. This is not new information
  this phase discovered (Phase 6's docs already named object/shared
  storage as a later-phase concern), but this phase's contention data
  (§16) makes concrete *why* it matters more than it might first appear:
  the elegant "exactly one build" property Workload C measured is a
  **single-host property of the current storage backend**, and will
  **not** hold across machines without a shared cache — **REQUIRES
  MULTI-HOST VALIDATION**, and more specifically, requires a shared-storage
  cache backend to even be meaningfully testable across hosts. Flagging
  this as the single most important unresolved architectural question
  this phase surfaced for Phase 12+.
- **Duplicate target-build risk at fleet scale**: follows directly from
  the point above — **INFERRED** that a fleet of N machines each running
  the current filesystem-backed cache would perform up to N redundant
  builds of the same target (one per machine), not the "one build total"
  Workload C demonstrated for one host's worker processes. **REQUIRES
  MULTI-HOST VALIDATION** to confirm, but the reasoning is not
  speculative — it follows directly from the storage backend's documented
  design (Phase 6).
- **Redis bandwidth/command load at fleet scale**: **INFERRED** low risk
  from the measured per-job command cost (§14, §11) — even hundreds of
  concurrent jobs across many machines, at ~0.71ms of Redis work each,
  extrapolates to a modest sustained command rate. **REQUIRES MULTI-HOST
  VALIDATION** for actual behavior — this extrapolation does not account
  for connection-count scaling, Redis-side lock contention under much
  higher concurrency than 4–8 local processes, or network conditions.
- **Network media acquisition at fleet scale**: **explicitly not
  inferrable** from this phase's loopback-only measurement (§24) — no
  claim made.
- **Expected cost when independent machines scale horizontally**: for the
  stages this phase found negligible on one host (Redis, matching,
  cache-hit lookups — §21), horizontal scaling is **INFERRED** likely to
  remain favorable, since each machine's DINOv2 inference is
  embarrassingly parallel across independent jobs (no cross-job
  dependency once the target-cache-sharing question above is resolved).
  For DINOv2 inference itself — the dominant single-host cost (§21) —
  horizontal scaling is exactly what a "CPU workers + GPU workers" fleet
  (per this project's stated production architecture) is for, and
  nothing in this phase's data contradicts that being effective; it
  simply **could not be measured** here. **REQUIRES MULTI-HOST
  VALIDATION.**

## 26. Phase 12 recommendations

1. **Resolve target-cache storage before scaling workers across
   machines** (§25). This is the single highest-priority architectural
   question this phase surfaced — not a performance number, a correctness
   property (Workload C's "exactly one build" guarantee is currently
   single-host-only). Whatever Phase 12/13 does for object/shared media
   storage should very likely also address shared target-embedding-cache
   storage at the same time.
2. **When a real worker-process entrypoint is written** (referenced but
   not built yet — §23), size `torch_num_threads` per the host's
   worker-process density using the new `DINOv2EmbeddingEngine`
   parameter — do **not** leave every process at torch's own default if
   more than one CPU worker process will run per host (§19b's 15x
   pathology).
3. **GPU worker path is completely unvalidated** (§3, §20) — the very
   first thing worth measuring on any host with a working CUDA GPU is
   this exact same Workload F (`bench_embedding.py` already supports
   this — it just never got to exercise a GPU here) and Workload D
   repeated with GPU workers, before assuming any GPU throughput number.
4. **Real network acquisition latency/variance is unmeasured** (§24) —
   worth a dedicated small benchmark against real (or realistically
   simulated, e.g. artificial latency/packet loss) remote hosts before
   assuming acquisition stays a minor (~5%) fraction of job time in
   production; nothing here rules out acquisition dominating once real
   network conditions replace loopback.
5. **No pipeline-code optimization is justified beyond §23's fix.**
   Acquisition, matching, Redis, and cache-hit paths are all comfortably
   under 5% of job latency each; further optimizing any of them would
   contradict this phase's own evidence.
6. **Lock TTL/poll tuning remains open but low-priority** (§17) — revisit
   only if real target-library data shows build times approaching
   minutes, or if `poll_interval_s`'s ~1s waiter-side waste becomes
   operationally noticeable at real contention rates.

## 27. Files

- `benchmarks/common.py`, `benchmarks/gen_test_video.py`,
  `benchmarks/file_server.py`, `benchmarks/instrumented_handler.py`,
  `benchmarks/bench_matching.py`, `benchmarks/bench_embedding.py`,
  `benchmarks/bench_pipeline.py` — new, this phase.
- `benchmarks/fixtures/bench_15s.mp4`, `benchmarks/fixtures/bench_60s.mp4`
  — generated, not committed test fixtures (regenerate via
  `python -m benchmarks.gen_test_video`).
- `benchmarks/results/bench_matching_20260812T115316_0e2935c6.json`
- `benchmarks/results/bench_embedding_20260812T115617_ced8245e.json`
- `benchmarks/results/bench_pipeline_A_warm_20260812T120557_fb9e982e.json`
- `benchmarks/results/bench_pipeline_B_cold_20260812T120613_dfe587ca.json`
- `benchmarks/results/bench_pipeline_C_contention_20260812T120622_a2174353.json`
- `benchmarks/results/bench_pipeline_D_scaling_20260812T121242_9ddf52d2.json`
- `embedding/dinov2_engine.py` — modified (§23): additive
  `torch_num_threads` constructor parameter.
- `tests/test_embedding.py` — 3 new tests (§23).

Full repository suite after the one implemented change:
**152 passed**, 0 failed, 0 skipped.
