# Benchmarks

The benchmark suite lives in `benchmarks/` and is **not** part of the
pytest suite — each script is a standalone module, run directly, that
loads the real DINOv2 model and drives real pipeline components. These
are meaningfully more expensive than `python -m pytest` (minutes, not
seconds; real CPU/model load) and are run manually, not on every change.

All four scripts write pretty-printed JSON results to `benchmarks/results/`
via `benchmarks.common.save_result` — never overwriting a previous run
(each filename carries a timestamp + short random id). Existing results
already committed under `benchmarks/results/` are historical measurements
from the environment they were captured in; they document what was
measured once, not a live/current SLA.

## What each benchmark measures

| Script | Measures | Redis? | Network? |
| --- | --- | --- | --- |
| `python -m benchmarks.bench_embedding` | `DINOv2EmbeddingEngine.embed_video_segments`: model-load time, cold vs. steady-state inference, separated per phase-11's "never mix cold and warm into one number" rule | No | No |
| `python -m benchmarks.bench_matching` | `matching.matcher.match_segments` in isolation against synthetic embedding arrays at increasing sizes — is O(N·M) dense cosine matching material next to DINOv2 inference cost? | No | No |
| `python -m benchmarks.bench_integration_overhead` | The cost `integration.submission.FingerprintJobSubmitter` + the full Redis Streams claim/commit path add on top of a bare handler call — isolates queue/submission overhead from DINOv2 inference cost | Yes (`db 14`) | Loopback only |
| `python -m benchmarks.bench_pipeline` | Full end-to-end pipeline: warm-cache latency, cold-cache (target build-on-miss) latency, same-target lock contention, and worker-count scaling under both `TORCH_NUM_THREADS=1` and default-threads configurations. The most expensive of the four (multiple worker subprocesses, real inference, multiple workload phases). | Yes (`db 14`) | Loopback only |

All four use `benchmarks.gen_test_video.generate_all()` to produce
deterministic local synthetic test clips (`benchmarks/fixtures/
bench_15s.mp4`, `bench_60s.mp4`) rather than relying on external media.

## Running a benchmark

```bash
source .venv/bin/activate
redis-cli ping   # required for bench_integration_overhead / bench_pipeline

python -m benchmarks.bench_embedding
```

Each script prints human-readable progress to stdout as it runs and
writes its full result JSON to `benchmarks/results/<name>_<timestamp>_
<id>.json` on completion. `bench_pipeline` in particular can take several
minutes — it spawns real worker subprocesses (`multiprocessing`, `spawn`
start method — genuinely separate processes with their own torch/model
load, mirroring separate machines rather than threads sharing one
process's GIL/thread pool) for its contention and scaling workloads.

**Do not run `bench_pipeline` or `bench_integration_overhead` against a
`REDIS_URL` your production worker also uses** — they use a dedicated
logical database (`BENCH_REDIS_URL`, `redis://localhost:6379/14`) and
`FLUSHDB` it before/after each workload, same convention as the test
suite's db 15.

## Reading benchmark output

Every result JSON begins with an `environment` block
(`benchmarks.common.environment_snapshot()`): git revision + dirty flag,
CPU model, logical CPU count, total RAM, GPU name (via `nvidia-smi`,
independent of whether torch can actually use it), OS, and
Python/torch/transformers/redis-py/ffmpeg versions. Compare this block
before comparing two runs' numbers — hardware/software differences
between runs make direct comparison meaningless.

Latency figures use `benchmarks.common.LatencyStats`: count, mean,
median, p50/p95/p99 (nearest-rank), min, max, stdev, and a
`small_sample_warning` string that is **non-null and must be read**
whenever a sample count is below 30 — several workloads in this suite
run only 5-15 repetitions, at which point p95/p99 are indicative, not
statistically precise. Do not quote a p99 figure from a small-sample
benchmark result as if it were a validated SLA number.

Resource figures (where sampled — `bench_pipeline`'s contention/scaling
workloads) come from `benchmarks.common.ResourceSampler`: process and
system CPU%, process RSS, and best-effort GPU utilization/VRAM via
`nvidia-smi` polling (independent of torch's own CUDA visibility — see
"GPU benchmarking" below).

## GPU benchmarking

**No GPU benchmark numbers exist anywhere in this repository.** The
development environment these benchmarks were originally authored and
run against has a physical GPU visible to `nvidia-smi` but not to torch
(`torch.cuda.is_available()` returns `False` there — see
`docs/architecture/phase-11-performance-benchmarks.md`, "Hardware/
software environment," for the diagnosis). Every benchmark script's own
docstring says explicitly that GPU inference timing is **not** zero and
**not** estimated from CPU numbers — it requires being run on an actual
CUDA-capable host and has not been. Treat any GPU performance claim not
backed by a result JSON in `benchmarks/results/` as unverified. This
documentation pass did not run GPU benchmarks — see the root README's
production status table.

## The `TORCH_NUM_THREADS` finding

`bench_pipeline`'s Workload D (worker-count scaling) is the source of
this project's most consequential benchmark finding, and the reason
`worker/main.py` defaults `TORCH_NUM_THREADS=1`: running multiple CPU
worker *processes* on one host, each left at torch's own default
(physical-core-count) thread pool, oversubscribes the host's cores
combinatorially — measured as a ~15x per-job slowdown and net-negative
scaling at just 4 processes on a 6-physical-core machine. Pinning each
process to 1 thread instead is what actually scales usefully across
multiple processes (measured ~0.81 efficiency at 4 workers). See
`docs/architecture/phase-11-performance-benchmarks.md`, §19a/§19b/§23,
for the full numbers and methodology, and `worker/main.py`'s module
docstring for how this became the shipped default.

## Recommended nightly / periodic benchmark

There is no scheduled/CI-integrated benchmark run configured in this
repository. If you want a periodic check for performance regressions,
`bench_pipeline` is the most representative single script (it exercises
the real end-to-end pipeline including Redis, acquisition, embedding,
target caching, and matching) but is also the slowest; `bench_embedding`
alone is a reasonable, much cheaper proxy for "did DINOv2
inference/model-load cost regress" if a full pipeline run is too
expensive to run often. Compare successive result JSONs' `environment`
blocks first to make sure you're comparing like-for-like hardware before
concluding a number changed for real.
