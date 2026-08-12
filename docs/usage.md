# Usage

Practical, verified workflows for running and exercising the fingerprinter.
For flag/environment-variable reference, see the tables below and
`worker/main.py`. For what each piece actually does, see
`docs/architecture/system-architecture.md`.

## Start a worker

The only production entrypoint is:

```bash
python -m worker.main
```

All configuration is via environment variables — there is no CLI flag
parser. `worker/main.py` reads them once at startup
(`WorkerConfig.from_env`), validates them, and exits non-zero with a
structured `worker_fatal_error` log line if anything is invalid.

### Environment variables

| Variable | Purpose | Default | Required |
| --- | --- | --- | --- |
| `REDIS_URL` | Redis connection URL (`redis://`, `rediss://`, or `unix://`) | `redis://localhost:6379/0` | No |
| `WORKER_CONSUMER_NAME` | Redis Streams consumer name for this process | `worker-{hostname}-{pid}-{thread_id}` | No |
| `WORKER_LEASE_MS` | How long a claimed job may go unacked before another worker may `XAUTOCLAIM` it | `30000` | No |
| `WORKER_BLOCK_MS` | How long `XREADGROUP` blocks waiting for a new job before looping again | `5000` | No |
| `WORKER_RECLAIM_INTERVAL_MS` | How often the run loop checks for stale (reclaimable) entries | `WORKER_LEASE_MS`'s value | No |
| `WORKER_MAX_ATTEMPTS` | Accepted for configuration-surface parity only — **has no effect**. `max_attempts` is a per-job field set by the producer at submission time, not a worker setting. If set, the worker logs a warning at startup. | unset | No |
| `EMBEDDING_DEVICE` | `auto`, `cpu`, or `cuda` | `auto` | No |
| `TORCH_NUM_THREADS` | Torch intra-op thread pool size for this process | `1` | No |
| `TARGET_CACHE_PATH` | Host-local directory for the (non-shared) embedding cache | `./target_cache` | No |
| `SHARED_ARTIFACT_STORE_PATH` | Path to a **genuinely shared mount** enabling the multi-host-safe target cache backend. Unset = host-local only. | unset | No |
| `MEDIA_MAX_BYTES` | Max bytes accepted for one acquired media file | `104857600` (100 MiB) | No |
| `WORKER_OBSERVABILITY_INTERVAL_MS` | Interval between periodic `worker_health` log summaries | `60000` | No |
| `WORKER_RUN_OUTPUT` | Path to write a machine-readable run record (and `<path>.marker` at startup) | unset (no run record written) | No |

Notes:

- `TORCH_NUM_THREADS` defaults to `1`, not "however many cores this host
  has." This is deliberate: Phase 11 benchmarking measured a ~15x
  throughput collapse from oversubscribing a host's cores when running
  multiple worker *processes* per host, each left at torch's own
  physical-core-count default. A lone worker on an otherwise-idle host
  pays a real cost for this safe default (measured ~3.6x slower per job
  than 6 threads) — if you know exactly one worker process will run on a
  host, override it explicitly. See `worker/main.py`'s module docstring
  and `docs/architecture/phase-11-performance-benchmarks.md`.
- `SHARED_ARTIFACT_STORE_PATH` and `REDIS_URL` are independent
  infrastructure dependencies — setting one does not imply or configure
  the other. See `docs/architecture/system-architecture.md`, §6.
- `REDIS_URL` credentials are stripped before ever reaching a log line
  (`worker/main.py`'s `_redact_redis_url`) — safe to include a password in
  the URL.

### Example: small local worker

```bash
source .venv/bin/activate
export REDIS_URL=redis://localhost:6379/0
export TARGET_CACHE_PATH=./target_cache
export WORKER_RUN_OUTPUT=./worker_run.json
python -m worker.main
```

Stop it with `Ctrl-C` (`SIGINT`) or `SIGTERM` — both request the same
graceful shutdown: finish the run loop's current blocking read, then
exit. Neither ever force-aborts an in-flight job.

### Example: multiple worker processes on one host

```bash
export TORCH_NUM_THREADS=1      # keep this — see note above
for i in 1 2 3 4; do
    WORKER_CONSUMER_NAME="worker-$i" python -m worker.main &
done
wait
```

## Register a target

Before any job can match, the target it's being checked against must be
registered. There is no CLI for this — call `TargetRegistry.register_target`
directly (this mirrors how the registry is meant to be driven: by whatever
process owns target ingestion, not a generic script this repo doesn't
otherwise need):

```python
from redis import Redis
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache

redis_client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
registry = TargetRegistry(
    redis_client,
    FilesystemEmbeddingCache("./target_cache/pooled"),
    FilesystemSegmentEmbeddingCache("./target_cache/segments"),
)

record = registry.register_target(
    target_id="movie-123",
    target_version="v1",
    media_path="/path/to/target_movie.mp4",
)
print(record.content_sha256)
```

Segment embeddings are **not** computed at registration time — they are
built lazily, cache-first, on the first job that needs this
`(target_id, target_version)` (see `docs/architecture/system-
architecture.md`, §5, "Target registry and cache").

## Submit a synthetic/test job

The repository has no standalone job-submission CLI; `work_queue.producer.
JobProducer` (bare) and `integration.submission.FingerprintJobSubmitter`
(idempotent, backpressure-aware — the one a real producer should use) are
both plain Python APIs. To exercise the full pipeline end to end locally:

```bash
# 1. Serve a candidate video file over loopback HTTP
cd /path/to/some/dir/with/a/video.mp4
python -m http.server 8123 &
```

```python
# 2. Submit a job against the target registered above
from redis import Redis
from integration.candidate import FingerprintCandidate
from integration.submission import FingerprintJobSubmitter

redis_client = Redis.from_url("redis://localhost:6379/0", decode_responses=True)
submitter = FingerprintJobSubmitter(redis_client)

result = submitter.submit(FingerprintCandidate(
    candidate_url="http://127.0.0.1:8123/video.mp4",
    media_evidence_id="test-evidence-1",
    media_type="video",
    source_domain="127.0.0.1",
    target_id="movie-123",
    target_version="v1",
))
print(result)  # SubmissionResult(outcome=<SubmissionOutcome.ENQUEUED: 'enqueued'>, ...)
```

```bash
# 3. Run a worker to process it (see "Start a worker" above)
python -m worker.main
```

Note: `acquisition/ssrf_guard.py` rejects loopback/private addresses by
default (see `docs/architecture/system-architecture.md`, §7) — a
candidate URL pointed at `127.0.0.1` will be **rejected in production
mode**. This is intentional and correct; it is exactly what the SSRF
protection is for. Use `MediaAcquirer(allow_private_networks=True)` only
in a test/dev script, never in `worker/main.py`'s wiring — which does not
expose this as configurable, on purpose.

## Inspect Redis stream/job state

```bash
redis-cli XLEN fingerprint:jobs:stream:default
redis-cli XINFO GROUPS fingerprint:jobs:stream:default
redis-cli XINFO CONSUMERS fingerprint:jobs:stream:default fingerprinter-workers
redis-cli XPENDING fingerprint:jobs:stream:default fingerprinter-workers

redis-cli HGETALL fingerprint:job:<job_id>:state
redis-cli HGETALL fingerprint:result:<job_id>
redis-cli XRANGE fingerprint:results:stream:default - +

redis-cli ZRANGE fingerprint:retry:delayed:default 0 -1 WITHSCORES
```

Or from Python, via `integration.outcome.resolve_outcome()` for a
higher-level "what happened to this job" read than raw `HGETALL`.

## Run tests

```bash
source .venv/bin/activate
python -m pytest -q
```

Requires a reachable Redis (`redis://localhost:6379/15` by default,
override with `FINGERPRINTER_TEST_REDIS_URL`) and the DINOv2 model
weights cached locally (see `docs/installation.md`). The suite flushes
its Redis DB (15) before and after each test — never point
`FINGERPRINTER_TEST_REDIS_URL` at a database with real data in it.

Run one file or one test:

```bash
python -m pytest tests/test_crash_recovery.py -q
python -m pytest tests/test_worker_main.py::test_config_validation_error_exits_nonzero -q
```

Current baseline on this repository: **269 passed, 0 failed** (see
`docs/development.md` for the exact run this is drawn from).

## Run a controlled pipeline test / benchmark

`benchmarks/` contains real end-to-end pipeline exercises (not pytest —
run as modules, produce JSON results under `benchmarks/results/`). These
load the real DINOv2 model and run real inference — they are
**meaningfully more expensive than the test suite** (multiple minutes,
real CPU/GPU load). See `docs/benchmarks.md` for what each one measures
and how to read its output before running one.

```bash
python -m benchmarks.bench_embedding
python -m benchmarks.bench_matching
python -m benchmarks.bench_integration_overhead
python -m benchmarks.bench_pipeline   # the most expensive of the four
```

## Inspect a worker run JSON

With `WORKER_RUN_OUTPUT=./worker_run.json` set (see "Start a worker"
above), a clean shutdown writes the full run record there; a startup
marker at `./worker_run.json.marker` is removed on that same clean write.
If the marker file still exists and there is no newer, clean run record,
the worker did not shut down cleanly (crash, `SIGKILL`, power loss).

```bash
cat worker_run.json | python -m json.tool
```

Top-level shape (see `docs/architecture/system-architecture.md`, §8, and
`worker/observability.py`'s `build_run_record` for the authoritative
schema):

```json
{
  "metadata": {"worker_id": "...", "hostname": "...", "pid": 0, "schema_version": 1},
  "configuration": { "...": "the full effective WorkerConfig, redacted" },
  "timing": {"started_at": 0.0, "ended_at": 0.0, "uptime_s": 0.0},
  "counters": {"jobs_claimed": 0, "jobs_completed": 0, "...": "..."},
  "error_categories": {"transient_acquisition_failure": 0},
  "latency": {"claim_to_completion_ms": {"count": 0, "p50": null, "p95": null}},
  "pipeline_stage_metrics": {"media_acquisition": {"...": "..."}},
  "redis": {"stream_length": 0, "group_lag": 0, "group_pending": 0},
  "resources": {"rss_peak_bytes": 0, "cpu_time_s": 0.0},
  "shutdown": {"reason": "graceful_shutdown", "clean": true}
}
```

Live monitoring (while a worker runs) is via its structured JSON log
stream — `worker_health` events at `WORKER_OBSERVABILITY_INTERVAL_MS`
intervals — not the run record, which is only written once, at shutdown.
This project does not ship a log viewer or dashboard; pipe the JSON log
lines to whatever your environment already uses to collect/query them.
