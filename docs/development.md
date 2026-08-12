# Development

## Repository structure

```text
acquisition/    HTTP(S) media download, SSRF guard, ffprobe validation
embedding/      DINOv2 model wrapper, frame extraction, embedding config
matching/       Segment matching, aggregation, matcher config/thresholds
target/         Target identity/registry, embedding caches (local + shared),
                 build-on-miss lock, versioning
integration/    Crawler-facing submission API: candidates, idempotency,
                 backpressure, outcome lookup
work_queue/     Redis Streams job/result contract: keys, schemas,
                 producer, state, results
worker/         Production worker: fingerprint_worker.py (claim/lease/
                 retry engine), main.py (process entrypoint),
                 matching_handler.py / acquisition_handler.py (pipeline
                 wiring), observability.py (Phase 13C)
tests/          pytest suite (unit + integration, real Redis + ffmpeg)
benchmarks/     Manual, non-pytest performance benchmarks (see
                 docs/benchmarks.md) + their saved results
docs/           This documentation tree
old/            The pre-Redis-architecture prototype. Research/reference
                 material only — explicitly not refactored into
                 production (see docs/design/design-proposal-1.md).
                 Not imported by, and has no bearing on, anything under
                 the directories above.
```

Each production package (`acquisition/`, `embedding/`, `matching/`,
`target/`, `integration/`, `work_queue/`, `worker/`) is documented at the
module level in its own `.py` files — read the module docstrings before
the phase docs; they describe *current* behavior, where a phase doc
describes the history of *how it got that way*.

## Architecture documentation

- [`docs/architecture/system-architecture.md`](architecture/system-architecture.md)
  — the current system, end to end. Start here.
- [`docs/architecture/phase-13-production-hardening.md`](architecture/phase-13-production-hardening.md),
  [`phase-13d-multi-host-target-cache-audit.md`](architecture/phase-13d-multi-host-target-cache-audit.md),
  [`phase-13d-distributed-target-artifacts.md`](architecture/phase-13d-distributed-target-artifacts.md),
  [`phase-13e-health-summary-interval-fix.md`](architecture/phase-13e-health-summary-interval-fix.md)
  — the most recent phase's audit + implementation + a follow-up fix.
  Read these for *why* the current production-hardening decisions were
  made, and for the exact wording of what is and isn't validated.
- [`docs/architecture/phase-12-crawler-fingerprinter-integration.md`](architecture/phase-12-crawler-fingerprinter-integration.md)
  — the crawler integration boundary decision referenced from
  `system-architecture.md`, §9.
- [`docs/architecture/phase-11-performance-benchmarks.md`](architecture/phase-11-performance-benchmarks.md)
  — the CPU-thread-oversubscription investigation behind
  `TORCH_NUM_THREADS`'s default, and the benchmark methodology
  `docs/benchmarks.md` extends.
- [`docs/architecture/phase-10-multi-technique-aggregation.md`](architecture/phase-10-multi-technique-aggregation.md)
  — the matching handler and result aggregation.
- [`docs/architecture/history/`](architecture/history/) — phases 1-9
  (Redis job contract, lease/crash recovery, retry/backoff, result
  contract, media acquisition, target management, DINOv2 embedding,
  video representation investigation, temporal matching). Preserved for
  their reasoning; describes each concept as it was *first* built, not
  necessarily its current final shape — cross-check against
  `system-architecture.md` and the current source for what's still true.
- [`docs/design/design-proposal-1.md`](design/design-proposal-1.md) —
  the founding architecture proposal this entire project was built from.
  Still the governing reference for what is deliberately excluded from
  production (SQLite as a queue, direct crawler Python imports,
  shared-filesystem assumptions between the two repositories).

## Conventions established across phases

These are load-bearing conventions, not style preferences — later code
depends on earlier code following them:

- **Every durable Redis schema carries an explicit, additive
  `*_schema_version` field** (`work_queue.jobs.JOB_SCHEMA_VERSION`,
  `work_queue.results.RESULT_SCHEMA_VERSION`,
  `target.cache.CACHE_ENTRY_SCHEMA_VERSION`,
  `target.segment_cache.SEGMENT_CACHE_ENTRY_SCHEMA_VERSION`). A missing
  field means "written before this field existed" (unambiguous, since
  there is exactly one schema that predates it); a present-but-mismatched
  value is rejected outright, never guessed at.
- **CAS-fenced finalization.** Every terminal Redis write in
  `worker/fingerprint_worker.py` is a Lua script gated on an
  attempt-counter compare-and-swap, so a worker that lost ownership via
  `XAUTOCLAIM` can never clobber the new owner's outcome. See
  `docs/architecture/system-architecture.md`, §3.
- **Status labeling on unvalidated claims.** Provisional thresholds,
  unmeasured defaults, and anything not run against real multi-host/GPU
  hardware are explicitly labeled — `PROVISIONAL HEURISTIC`,
  `ARCHITECTURAL`, `NOT VALIDATED`, `REQUIRES MULTI-HOST VALIDATION`,
  `REQUIRES GPU VALIDATION`, `SIMULATED MULTI-HOST`, `DEFERRED`. Follow
  this convention in new code and docs — do not present a provisional
  number as tuned, and do not claim multi-host/GPU validation that
  hasn't actually happened. See `matching/config.py`'s module docstring
  for the canonical definitions.
- **Content-addressed identity, never filename/path/mtime.**
  `target.identity.sha256_file` hashes bytes, not paths; cache keys derive
  from `target.versioning.cache_entry_key()`, a pure function of target +
  spec identity. No cache key anywhere in this project depends on
  hostname, PID, or local timestamp — required for the shared-storage
  backend (`target/shared_storage.py`) to be correct across hosts.
- **A cache-unavailable exception is never conflated with a cache miss.**
  `SharedArtifactStoreError` (unreachable/unwritable shared mount) is
  always distinct from `None`/`False` (key genuinely absent) — see
  `target/shared_storage.py`'s module docstring. Preserve this
  distinction in any new storage backend.
- **Never log a raw failure message that might embed a media URL.**
  `TransientFailure`/`PermanentFailure.error_type` exists specifically so
  `worker/observability.py` can classify and log a *safe* category
  (`classify_error_type`) without the raw exception message (which may
  contain the candidate's URL) ever reaching structured logs/metrics.

## How to run tests

```bash
source .venv/bin/activate
python -m pytest -q
```

Prerequisites: reachable Redis (`redis://localhost:6379/15` by default —
see `docs/installation.md`, "Database / namespace conventions"), `ffmpeg`/
`ffprobe` on `PATH`, and the DINOv2 model weights pre-cached locally
(`local_files_only=True` — see `docs/installation.md`). No network access
is required or attempted by the test suite itself once weights are
cached.

Current baseline on this repository (this documentation pass, single
run): **269 passed, 0 failed, 0 skipped**, wall time ~126s. If you see a
different total after a fresh `pip install`, that's a signal something in
your environment differs from the one this baseline was recorded
against — check Redis reachability and cached model weights first.

Run a single file or test:

```bash
python -m pytest tests/test_crash_recovery.py -q
python -m pytest tests/test_worker_observability.py -k health_summary -q
```

## Test suite shape

- `tests/conftest.py` — shared fixtures: a `redis_client` fixture that
  flushes `FINGERPRINTER_TEST_REDIS_URL` (default db 15) before/after
  each test, a `make_job`/`sample_job` factory, and a `media_server`
  fixture (in-process HTTP server, real tiny video/PNG fixtures, no
  external network).
- `tests/media_test_server.py` — the in-process HTTP server behind that
  fixture; simulates slow responses, corrupt bodies, oversized bodies,
  non-media content, and a real ffmpeg-decodable tiny video, so
  acquisition/embedding tests exercise real failure modes without
  needing the public internet.
- `tests/fixtures/` — real, small, checked-in media files
  (`tiny_video.mp4`, `tiny_image.png`) used across acquisition/embedding/
  matching tests.
- One test file roughly per production module (`test_acquisition.py`,
  `test_acquisition_ssrf.py`, `test_embedding.py`, `test_matching.py`,
  `test_target.py`, `test_worker.py`, `test_worker_main.py`,
  `test_worker_observability.py`, `test_crash_recovery.py`, ...) — when
  extending a module, add to (or create) its matching test file rather
  than inventing a new grouping.

## Extending the pipeline

- **A new matching technique** (audio, pHash, watermark, ...) folds into
  `matching.aggregation.combine()` as an additional evidence entry — see
  `docs/architecture/phase-10-multi-technique-aggregation.md` and
  `matching/aggregation.py`'s module docstring for the seam this was
  built to support. It does not require changing the job/result schema.
- **A new storage backend** (e.g. S3-compatible object storage in place
  of `SharedArtifactStore`'s shared-mount assumption) should implement
  the same three-method interface `target/shared_storage.py` defines
  (`get_bytes`/`put_bytes`/`put_file`/`get_file` plus the
  exists/miss/unavailable failure-semantics contract) rather than
  changing any caller.
- **A new job producer** (beyond the crawler) should call
  `integration.submission.FingerprintJobSubmitter.submit()`, not
  `work_queue.producer.JobProducer` directly, unless you deliberately
  want to bypass idempotency/backpressure.

## Verifying documentation-affecting changes

Anything that changes an environment variable, a Redis key, a default
value, or a public function signature referenced from `docs/` should
update the corresponding doc in the same change — this documentation
pass verified every flag/env-var/default against source as of
`git rev-parse HEAD` at the time it was written; keep it that way rather
than letting it drift back out of sync.
