"""Phase 12 — integration overhead only.

Per the phase brief: "Do NOT repeat the entire Phase 11 benchmark suite.
Only measure integration overhead." This compares two ways of running the
*same* warm-cache fingerprint job against the *same* real production
primitives (real `DINOv2EmbeddingEngine` on CPU, real `MediaAcquirer` over
loopback HTTP, a real `TargetRegistry`, real Redis):

- **A. Direct handler invocation** — `worker.matching_handler
  .build_matching_handler(...)`'s handler called as a plain Python
  function against a `work_queue.jobs.Job`. No Redis Streams, no
  submission/dedup/backpressure, no result persistence. This is
  Phase 11's `handler_total_s` measurement, reproduced here (not copied
  from its JSON — measured fresh in this process) as the baseline.
- **B. Full crawler-integration path** — `integration.submission
  .FingerprintJobSubmitter.submit()` (candidate validation + backpressure
  check + dedup marker + `XADD`) -> `worker.fingerprint_worker.Worker
  .claim_one()` -> `Worker.process_claim()` (runs the same handler, then
  `commit_result()`) -> `integration.outcome.resolve_outcome()` (two
  `HGETALL`s). This is everything Phase 12 adds on top of A.

The difference (B - A) is "integration overhead": submission +
claim + commit + outcome-resolution cost, isolated from DINOv2 inference
cost, which both paths pay identically and which Phase 11 already
characterized in detail (§14, ~861ms/905ms of warm-cache job latency).

Uses the same fixture/config Phase 11's Workload A used
(`bench_15s.mp4`, `segment_duration_s=2.5`, `torch_num_threads` left at
its default for a single-process run — see phase-11 doc §7) so this
result is directly comparable to that report's numbers, and a dedicated
Redis logical DB (`BENCH_REDIS_URL`, db 14 — same convention as
`bench_pipeline.py`, never db 0 or the test suite's db 15).

Run: `python -m benchmarks.bench_integration_overhead`
"""
from __future__ import annotations

import tempfile
import time
import uuid
from pathlib import Path

from redis import Redis

from acquisition import MediaAcquirer
from benchmarks import common, gen_test_video
from benchmarks.file_server import StaticFileServer
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from integration.candidate import FingerprintCandidate
from integration.outcome import resolve_outcome
from integration.submission import FingerprintJobSubmitter
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from work_queue.jobs import Job
from worker.fingerprint_worker import Worker
from worker.matching_handler import _target_artifact, build_matching_handler

BENCH_REDIS_URL = "redis://localhost:6379/14"
SEGMENT_DURATION_S = 2.5  # matches phase-11 Workload A
VIDEO_PATH = gen_test_video.FIXTURES_DIR / "bench_15s.mp4"
REPS = 15


def _build_registry(redis_client: Redis, cache_dir: Path) -> TargetRegistry:
    pooled = FilesystemEmbeddingCache(cache_dir / "pooled")
    segments = FilesystemSegmentEmbeddingCache(cache_dir / "segments")
    return TargetRegistry(redis_client, pooled, segments)


def _prewarm_target(registry: TargetRegistry, engine: DINOv2EmbeddingEngine, target_id: str, version: str) -> None:
    registry.register_target(target_id, version, str(VIDEO_PATH))
    record = registry.get_target(target_id, version)
    target_artifact, _ = _target_artifact(record)
    result = engine.embed_video_segments(target_artifact)
    registry.register_segment_embedding(target_id, version, result.to_embedding_spec(), result.segments, result.coarse_vector)


def run() -> dict:
    if not VIDEO_PATH.exists():
        gen_test_video.generate_all()

    body = VIDEO_PATH.read_bytes()
    server = StaticFileServer(body, content_type="video/mp4")
    redis_client = Redis.from_url(BENCH_REDIS_URL, decode_responses=True)
    redis_client.flushdb()

    cache_dir = Path(tempfile.mkdtemp(prefix="fingerprinter-bench-overhead-"))
    registry = _build_registry(redis_client, cache_dir)
    engine = DINOv2EmbeddingEngine(
        device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=SEGMENT_DURATION_S)
    )
    acquirer = MediaAcquirer(allow_private_networks=True)  # bench server is loopback
    handler = build_matching_handler(acquirer, engine, registry)

    warm_target_id = "overhead-bench-target"
    _prewarm_target(registry, engine, warm_target_id, "v1")

    # -- A: direct handler invocation ----------------------------------
    direct_samples = []
    for i in range(REPS):
        job = Job(
            job_id=f"direct-{i}-{uuid.uuid4().hex[:8]}",
            media_evidence_id=f"cand-{i}",
            media_url=server.url(),
            media_type="video",
            source_domain="bench.local",
            target_id=warm_target_id,
            target_version="v1",
            techniques=("dinov2",),
            max_attempts=1,
        )
        t0 = time.monotonic()
        handler(job)
        direct_samples.append(time.monotonic() - t0)

    # -- B: full crawler-integration path --------------------------------
    submitter = FingerprintJobSubmitter(redis_client)
    worker = Worker(redis_client, consumer_name="bench-overhead-worker", block_ms=2000)
    full_samples = []
    submission_samples = []
    claim_samples = []
    commit_samples = []
    outcome_samples = []
    for i in range(REPS):
        candidate = FingerprintCandidate(
            candidate_url=server.url() + f"#{i}-{uuid.uuid4().hex[:8]}",  # fragment: unique job_id, same bytes served
            media_evidence_id=f"cand-full-{i}",
            media_type="video",
            source_domain="bench.local",
            target_id=warm_target_id,
            target_version="v1",
        )

        t_total0 = time.monotonic()

        t0 = time.monotonic()
        submission = submitter.submit(candidate)
        submission_samples.append(time.monotonic() - t0)
        assert submission.outcome.value == "enqueued", submission

        t0 = time.monotonic()
        entry = worker.claim_one()
        claim_samples.append(time.monotonic() - t0)
        assert entry is not None and entry.is_valid and entry.job.job_id == submission.job_id

        result = handler(entry.job)

        t0 = time.monotonic()
        worker.commit_result(entry, result)
        commit_samples.append(time.monotonic() - t0)

        t0 = time.monotonic()
        resolve_outcome(redis_client, submission.job_id)
        outcome_samples.append(time.monotonic() - t0)

        full_samples.append(time.monotonic() - t_total0)

    server.shutdown()

    def stats(samples):
        return common.LatencyStats.from_samples(samples).__dict__

    payload = {
        "video": VIDEO_PATH.name,
        "segment_duration_s": SEGMENT_DURATION_S,
        "reps": REPS,
        "direct_handler_s": stats(direct_samples),
        "full_integration_path_s": stats(full_samples),
        "submission_s": stats(submission_samples),
        "claim_s": stats(claim_samples),
        "commit_s": stats(commit_samples),
        "outcome_resolve_s": stats(outcome_samples),
        "integration_overhead_s": stats(
            [s + c + o for s, c, o in zip(submission_samples, commit_samples, outcome_samples)]
        ),
        "environment": common.environment_snapshot(),
    }
    return payload


if __name__ == "__main__":
    result = run()
    path = common.save_result("bench_integration_overhead", result)
    direct_mean = result["direct_handler_s"]["mean_s"]
    overhead_mean = result["integration_overhead_s"]["mean_s"]
    full_mean = result["full_integration_path_s"]["mean_s"]
    print(f"direct handler mean:            {direct_mean * 1000:.2f} ms")
    print(f"full integration path mean:     {full_mean * 1000:.2f} ms")
    print(f"integration overhead mean:      {overhead_mean * 1000:.2f} ms  ({overhead_mean / direct_mean * 100:.2f}% of direct handler time)")
    print(f"  submission:   {result['submission_s']['mean_s'] * 1000:.3f} ms")
    print(f"  claim:        {result['claim_s']['mean_s'] * 1000:.3f} ms")
    print(f"  commit:       {result['commit_s']['mean_s'] * 1000:.3f} ms")
    print(f"  outcome read: {result['outcome_resolve_s']['mean_s'] * 1000:.3f} ms")
    print(f"saved: {path}")
