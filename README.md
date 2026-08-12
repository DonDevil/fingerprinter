# Anti-Piracy Fingerprinter

## Overview

The Anti-Piracy Fingerprinter is a distributed worker fleet that answers
one question: does this candidate piece of media match a registered
target video? It consumes fingerprint jobs from Redis, downloads the
candidate, computes a DINOv2 visual embedding, compares it against a
registered target's embeddings using a temporal (partial-clip-aware)
matching algorithm, and writes back a match / no-match / processing-failure
result.

This repository implements fingerprinting only. It does not crawl the
web, discover URLs, or maintain a URL frontier — that is a separate,
independently-deployable project. See
[Repository structure](#repository-structure) and
[Relationship to the crawler](#relationship-to-the-crawler) below.

## Architecture

```
Redis Stream (fingerprint:jobs:stream:{priority})
        │  XREADGROUP, consumer group "fingerprinter-workers"
        ▼
   Worker (claim / lease / retry / crash-recovery)
        │
        ▼
   MediaAcquirer  →  DINOv2EmbeddingEngine  →  TargetRegistry
   (SSRF-hardened     (candidate + target       (cache-first,
    download)          embeddings)               build-on-miss
                                                   under a Redis lock)
        │
        ▼
   Temporal segment matching  →  Result aggregation
        │
        ▼
   Redis: result hash + job state + results stream (atomic commit)
```

Full detail — the Redis Streams job model, crash recovery, distributed
target-artifact storage, SSRF hardening, and observability — is in
[`docs/architecture/system-architecture.md`](docs/architecture/system-architecture.md).

## Repository structure

```text
acquisition/    HTTP(S) media download, SSRF guard, ffprobe validation
embedding/      DINOv2 model wrapper, frame extraction
matching/       Segment matching, result aggregation, thresholds
target/         Target identity/registry, local + shared embedding caches
integration/    Crawler-facing submission API (idempotency, backpressure)
work_queue/     Redis Streams job/result contract
worker/         Production worker process (worker/main.py is the entrypoint)
tests/          pytest suite
benchmarks/     Manual (non-pytest) performance benchmarks + saved results
docs/           Documentation (see below)
old/            Pre-Redis-architecture prototype — research/reference only,
                 not used in production (see docs/design/design-proposal-1.md)
```

## Fingerprinter overview

- **Input:** a Redis Streams job (`fingerprint:jobs:stream:{priority}`) —
  a candidate media URL, a `(target_id, target_version)` to check it
  against, and which technique(s) to run.
- **Pipeline:** SSRF-hardened download → media validation (`ffprobe`) →
  DINOv2 segment embedding of the candidate → cache-first, build-on-miss
  segment embedding of the target → temporal segment matching → result
  aggregation.
- **Output:** an atomically-committed `Result` (`match` / `no_match` /
  `processing_failure`) written to a Redis hash and summarized onto a
  results stream, correlated back to the caller's own
  `media_evidence_id`.
- **Coordination surface:** Redis only — no shared filesystem assumption
  between workers (except the *optional*, explicitly-configured shared
  artifact store for multi-host target-cache sharing), no direct imports
  across repository boundaries.

## Relationship to the crawler

The crawler is a **separate repository** (`/home/darkdevil/Desktop/
anti_piracy/crawler` in this project's layout), with its own git history
and its own installation/usage docs. It discovers candidate URLs and
records media evidence; this repository consumes fingerprint jobs and
determines matches. **The two are not wired together today** — this
repository exposes `integration.submission.FingerprintJobSubmitter` as
the intended submission API for any producer (crawler or otherwise), but
no bridge component that drains the crawler's own evidence-job queue and
calls it currently exists in either repository. See
[`docs/architecture/system-architecture.md`, §9](docs/architecture/system-architecture.md#9-relationship-to-the-crawler-repository)
for the full detail and the reasoning behind this boundary.

## Current status

Implemented, tested end to end on one host: the Redis Streams job
contract (claim/lease/retry/crash-recovery), SSRF-hardened media
acquisition, DINOv2 embedding, temporal segment matching, result
aggregation, a production worker process with structured observability,
and a simulated-multi-host distributed target-artifact cache. **Not yet
validated:** real multi-host deployment (only simulated in-process today)
and GPU execution (implemented by code inspection, never run against real
CUDA hardware in this project's own benchmarking). See the
[production status table](#production-status) below for the
component-by-component breakdown.

## Quick start

```bash
# 1. Install (see docs/installation.md for full detail, including how to
#    pre-cache the DINOv2 model weights and set up ffmpeg/ffprobe)
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

# 2. Start Redis (or point REDIS_URL at an existing one)
redis-server --daemonize yes
redis-cli ping   # -> PONG

# 3. Verify the install
python -m pytest -q   # expect: 269 passed

# 4. Start a worker
export REDIS_URL=redis://localhost:6379/0
python -m worker.main
```

See [`docs/usage.md`](docs/usage.md) for registering a target, submitting
a synthetic job, and inspecting Redis/worker state.

## Installation

Full system requirements (Python, Redis, ffmpeg/ffprobe, GPU), the
DINOv2 model-weight pre-caching step, and Redis setup/verification:
[`docs/installation.md`](docs/installation.md).

## Usage

Starting a worker, every environment variable it reads, registering a
target, submitting a job, inspecting Redis stream/job state, and reading
a worker's run-record JSON: [`docs/usage.md`](docs/usage.md).

## Testing

```bash
python -m pytest -q
```

Current baseline: **269 passed, 0 failed, 0 skipped**. Requires a
reachable Redis (test database 15 by default), `ffmpeg`/`ffprobe` on
`PATH`, and cached DINOv2 model weights — see
[`docs/development.md`](docs/development.md#how-to-run-tests).

## Benchmarking

```bash
python -m benchmarks.bench_embedding      # cheapest
python -m benchmarks.bench_matching
python -m benchmarks.bench_integration_overhead
python -m benchmarks.bench_pipeline       # most expensive, full pipeline
```

What each measures, how to read the output, and why no GPU numbers exist
yet: [`docs/benchmarks.md`](docs/benchmarks.md).

## Security notes

- **SSRF hardening:** outbound media fetches validate the resolved
  destination address (rejecting loopback/private/link-local/reserved
  ranges) on the initial URL and every redirect hop. **Known limitation:**
  DNS rebinding / TOCTOU between this check and the actual socket
  connection is not closed by this alone — see
  [`docs/architecture/system-architecture.md`, §7](docs/architecture/system-architecture.md#7-ssrf--outbound-fetch-security).
- **Media bounds:** scheme allowlist, bounded redirects, a hard byte cap
  enforced against actual bytes written, and `ffprobe`-based validation
  that downloaded bytes decode as real media.
- **Redis credentials** in `REDIS_URL` are stripped before ever reaching
  a log line.
- **No built-in Redis auth/TLS configuration** — this project uses
  whatever `REDIS_URL` you provide; securing the Redis deployment itself
  (auth, ACLs, network access control, TLS) is an operator responsibility
  not automated here.

## Known limitations

- Matcher thresholds (`matching/config.py`) are provisional heuristics,
  not calibrated against a labeled dataset — no such dataset exists in
  this project yet.
- Multi-host distributed target-artifact storage is implemented and
  covered by a **simulated** multi-host test, not a real multi-host
  deployment.
- GPU (`EMBEDDING_DEVICE=cuda`) execution is implemented and correct by
  code inspection but has never been run against real CUDA hardware in
  this project's own benchmarking — no GPU performance numbers exist.
- No bridge exists yet to consume the crawler's own evidence-job queue —
  see [Relationship to the crawler](#relationship-to-the-crawler).
- Backpressure/idempotency defaults (`DEFAULT_MAX_OUTSTANDING_JOBS`,
  submission-marker TTL) are provisional, not load-tested against real
  multi-host throughput.

## Production status

Precise terminology used below: **IMPLEMENTED** (code exists, unit-tested),
**VALIDATED** (measured/confirmed against the real thing it claims),
**SIMULATED** (validated only in a stand-in for the real environment —
e.g. multiple objects in one process standing in for separate hosts),
**REQUIRES MULTI-HOST VALIDATION**, **REQUIRES GPU VALIDATION**,
**DEFERRED** (explicitly out of scope so far). A component is never
called production-validated merely because its unit tests pass.

| Component | Current state | Production-ready status | Remaining validation | Docs |
|---|---|---|---|---|
| Redis job contract (claim/lease/retry/crash-recovery) | IMPLEMENTED, unit + integration tested | VALIDATED (single-host, automated tests) | Real multi-host throughput/latency under sustained load | [system-architecture.md §3](docs/architecture/system-architecture.md#3-redis-streams-job-model) |
| SQLite backend | Not used by this repository at all — SQLite is explicitly excluded from production per the founding design proposal | N/A | N/A | [design-proposal-1.md](docs/design/design-proposal-1.md) |
| Media acquisition | IMPLEMENTED (scheme allowlist, size/redirect bounds, ffprobe validation) | VALIDATED (automated tests against real fixtures) | — | [system-architecture.md §5](docs/architecture/system-architecture.md#5-matching-pipeline-detail) |
| SSRF protection | IMPLEMENTED (resolved-address validation, all hops) | VALIDATED for the address-classification logic itself; DNS rebinding/TOCTOU gap is a known, documented, unsolved limitation | Connection-pinning fix for DNS rebinding | [system-architecture.md §7](docs/architecture/system-architecture.md#7-ssrf--outbound-fetch-security) |
| DINOv2 embedding (CPU) | IMPLEMENTED | VALIDATED (automated tests, benchmarked) | — | [system-architecture.md §5](docs/architecture/system-architecture.md#5-matching-pipeline-detail), [benchmarks.md](docs/benchmarks.md) |
| DINOv2 embedding (GPU) | IMPLEMENTED (device selection + inference hygiene, correct by code inspection) | **REQUIRES GPU VALIDATION** — no benchmark or correctness run against real CUDA hardware exists in this repo | Run against real GPU hardware, capture benchmark numbers | [installation.md](docs/installation.md#optional-gpu-setup), [benchmarks.md](docs/benchmarks.md#gpu-benchmarking) |
| Temporal segment matching | IMPLEMENTED, unit tested | VALIDATED for algorithm correctness against synthetic cases; thresholds are PROVISIONAL HEURISTICS, **not** calibrated against labeled real-world data | Calibration against a labeled dataset (does not exist yet) | [system-architecture.md §5](docs/architecture/system-architecture.md#5-matching-pipeline-detail), `matching/config.py` |
| Result aggregation | IMPLEMENTED, unit tested (single technique: DINOv2 temporal) | VALIDATED for the one technique that exists | Extend when a second technique is added | [system-architecture.md §5](docs/architecture/system-architecture.md#5-matching-pipeline-detail) |
| Target artifact storage — single host | IMPLEMENTED | VALIDATED | — | [system-architecture.md §6](docs/architecture/system-architecture.md#6-distributed-target-artifact-storage-phase-13d) |
| Target artifact storage — multi-host | IMPLEMENTED (shared, content-addressed blob store) | **SIMULATED MULTI-HOST** only (multiple registry instances, one process, one shared directory) | **REQUIRES MULTI-HOST VALIDATION** against genuinely separate machines and a real shared network filesystem | [system-architecture.md §6](docs/architecture/system-architecture.md#6-distributed-target-artifact-storage-phase-13d), [phase-13d-distributed-target-artifacts.md](docs/architecture/phase-13d-distributed-target-artifacts.md) |
| Worker process / deployment | IMPLEMENTED (`worker/main.py`, env-var configuration, structured shutdown) | VALIDATED (single-host, automated tests) | Real multi-host fleet operation | [usage.md](docs/usage.md#start-a-worker) |
| Observability (logs, counters, health summary, run record) | IMPLEMENTED, worker-local only | VALIDATED (automated tests) | Fleet-wide aggregation is explicitly DEFERRED (no dashboard/metrics backend by design) | [system-architecture.md §8](docs/architecture/system-architecture.md#8-observability) |
| Crawler integration (submission API) | IMPLEMENTED (`integration/`: idempotency, backpressure, outcome lookup) | VALIDATED (automated tests) for the API itself | Backpressure/idempotency defaults are PROVISIONAL, not load-tested; no consumer of the crawler's own evidence-job queue exists (DEFERRED) | [system-architecture.md §9](docs/architecture/system-architecture.md#9-relationship-to-the-crawler-repository) |
| Crash recovery (`XAUTOCLAIM`, attempt fencing) | IMPLEMENTED, unit tested | VALIDATED (automated tests simulate worker death) | Real multi-host crash scenarios | `tests/test_crash_recovery.py`, [system-architecture.md §3](docs/arrchitecture/system-architecture.md#3-redis-streams-job-model) |
| Retry system (exponential backoff) | IMPLEMENTED, unit tested | VALIDATED | — | [system-architecture.md §3](docs/architecture/system-architecture.md#3-redis-streams-job-model) |

## Architecture documentation

- [`docs/architecture/system-architecture.md`](docs/architecture/system-architecture.md) — the current system, end to end. Start here.
- [`docs/architecture/phase-13-production-hardening.md`](docs/architecture/phase-13-production-hardening.md), [phase-13d-multi-host-target-cache-audit.md](docs/architecture/phase-13d-multi-host-target-cache-audit.md), [phase-13d-distributed-target-artifacts.md](docs/architecture/phase-13d-distributed-target-artifacts.md), [phase-13e-health-summary-interval-fix.md](docs/architecture/phase-13e-health-summary-interval-fix.md) — the most recent production-hardening phase.
- [`docs/architecture/phase-12-crawler-fingerprinter-integration.md`](docs/architecture/phase-12-crawler-fingerprinter-integration.md) — the crawler integration boundary.
- [`docs/architecture/phase-11-performance-benchmarks.md`](docs/architecture/phase-11-performance-benchmarks.md) — the CPU-thread-oversubscription finding behind `TORCH_NUM_THREADS`.
- [`docs/architecture/phase-10-multi-technique-aggregation.md`](docs/architecture/phase-10-multi-technique-aggregation.md) — the matching handler.
- [`docs/architecture/history/`](docs/architecture/history/) — phases 1-9, preserved for their reasoning.
- [`docs/design/design-proposal-1.md`](docs/design/design-proposal-1.md) — the founding architecture proposal.

## Development/documentation links

- [`docs/installation.md`](docs/installation.md)
- [`docs/usage.md`](docs/usage.md)
- [`docs/development.md`](docs/development.md)
- [`docs/benchmarks.md`](docs/benchmarks.md)

## License

No license file is currently present in this repository.
