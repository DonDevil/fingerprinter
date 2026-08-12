"""Workloads A-D — end-to-end pipeline benchmarks (phase-11 brief).

Drives the *real* production pipeline: real Redis Streams (`work_queue`),
real loopback HTTP acquisition (`acquisition.MediaAcquirer` against
`benchmarks.file_server.StaticFileServer`), a real `DINOv2EmbeddingEngine`
(CPU — see phase-11 doc for why GPU is unavailable on this machine), a real
`TargetRegistry` backed by filesystem caches, and a real `worker.
fingerprint_worker.Worker` for claim/commit — all through
`benchmarks.instrumented_handler.run_instrumented`, which mirrors
`worker/matching_handler.py`'s exact call sequence with per-stage timing
(see that module's docstring for why it's a mirror, not a wrapped import).

Uses a dedicated Redis logical DB (`BENCH_REDIS_URL`, default
`redis://localhost:6379/14`) — never the app's default db 0 or the test
suite's db 15 (`tests/conftest.py`) — flushed before and after each
workload so runs don't interfere with each other or with anything else
using this Redis instance.

Concurrency uses `multiprocessing` with the `spawn` start method (each
worker is a genuinely separate OS process with its own fresh Python
interpreter/torch import/model load — the same shape as separate worker
processes or machines in production, not threads sharing one process's
GIL/torch thread pool).

Run: `python -m benchmarks.bench_pipeline`
"""
from __future__ import annotations

import multiprocessing as mp
import time
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from redis import Redis

from benchmarks import common, gen_test_video
from benchmarks.file_server import StaticFileServer

CTX = mp.get_context("spawn")

BENCH_REDIS_URL = "redis://localhost:6379/14"
SEGMENT_DURATION_S = 2.5  # 15s video -> 6 segments; see phase-11 doc for calibration
VIDEO_PATH = gen_test_video.FIXTURES_DIR / "bench_15s.mp4"

RSS_SAFETY_MARGIN_MIB = 1400  # measured single-process peak ~1080 MiB (see bench_embedding); generous margin
MIN_FREE_RAM_FLOOR_MIB = 1024  # never let the safety check plan to consume the last 1 GiB of available RAM


# ---------------------------------------------------------------------------
# Shared setup helpers (used both in-process, for A/B, and inside spawned
# worker processes, for C/D)
# ---------------------------------------------------------------------------


def _build_engine(threads: int, segment_duration_s: float):
    """`threads <= 0` means "leave torch's own default for a fresh process
    alone" (each worker here is spawned fresh — see module docstring — so
    there is no risk of inheriting a previous call's override within the
    same process, unlike `benchmarks/bench_embedding.py`'s same-process
    combos, which had exactly that bug during development)."""
    import torch

    from embedding.config import SegmentSamplingConfig
    from embedding.dinov2_engine import DINOv2EmbeddingEngine

    if threads and threads > 0:
        torch.set_num_threads(threads)
    return DINOv2EmbeddingEngine(
        device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=segment_duration_s)
    )


def _build_registry(redis_client, cache_dir: Path):
    from target.cache import FilesystemEmbeddingCache
    from target.registry import TargetRegistry
    from target.segment_cache import FilesystemSegmentEmbeddingCache

    pooled = FilesystemEmbeddingCache(cache_dir / "pooled")
    segments = FilesystemSegmentEmbeddingCache(cache_dir / "segments")
    return TargetRegistry(redis_client, pooled, segments)


def _embedding_spec_for(engine):
    from embedding.result import SEGMENT_EMBEDDING_SCHEMA_VERSION
    from target.versioning import EmbeddingSpec

    return EmbeddingSpec(
        model_id=engine.model_id,
        model_version=engine.model_version,
        embedding_schema_version=SEGMENT_EMBEDDING_SCHEMA_VERSION,
        preprocessing_config=engine.preprocessing_config.to_dict(),
        sampling_config=engine.segment_sampling_config.to_dict(),
    )


def _register_target(registry, target_id: str, version: str, media_path: Path):
    registry.register_target(target_id, version, str(media_path))


def _prewarm_target(registry, engine, target_id: str, version: str) -> float:
    """Registers + builds a target's segment cache, returns the build wall time."""
    from worker.matching_handler import _target_artifact

    _register_target(registry, target_id, version, VIDEO_PATH)
    spec = _embedding_spec_for(engine)

    def build(record):
        target_artifact, _ = _target_artifact(record)
        r = engine.embed_video_segments(target_artifact)
        return r.segments, r.coarse_vector

    t0 = time.monotonic()
    registry.get_or_build_segment_embedding(target_id, version, spec, build)
    return time.monotonic() - t0


def _make_job(job_id: str, media_url: str, target_id: str, target_version: str):
    from work_queue.jobs import Job

    return Job(
        job_id=job_id,
        media_evidence_id=f"cand-{job_id}",
        media_url=media_url,
        media_type="video",
        source_domain="bench.local",
        target_id=target_id,
        target_version=target_version,
        techniques=("dinov2",),
        max_attempts=1,  # no retries: a benchmark job should fail loudly, not skew latency with backoff
    )


def _ram_ok_for(worker_count: int) -> Optional[str]:
    available = common.available_ram_mib()
    if available is None:
        return None  # can't check -> proceed, but this is itself worth noting in the report
    required = worker_count * RSS_SAFETY_MARGIN_MIB + MIN_FREE_RAM_FLOOR_MIB
    if available < required:
        return (
            f"skipped: available RAM {available:.0f} MiB < required {required:.0f} MiB "
            f"({worker_count} workers x {RSS_SAFETY_MARGIN_MIB} MiB margin + {MIN_FREE_RAM_FLOOR_MIB} MiB floor)"
        )
    return None


# ---------------------------------------------------------------------------
# A / B: single-process stage-latency workloads (no concurrency — the point
# is per-stage cost, not scaling)
# ---------------------------------------------------------------------------


def run_stage_latency_workload(label: str, cold: bool, reps: int) -> dict:
    from benchmarks.instrumented_handler import run_instrumented
    from work_queue.producer import JobProducer
    from worker.fingerprint_worker import Worker

    body = VIDEO_PATH.read_bytes()
    server = StaticFileServer(body, content_type="video/mp4")
    redis_client = Redis.from_url(BENCH_REDIS_URL, decode_responses=True)
    redis_client.flushdb()

    import tempfile

    cache_dir = Path(tempfile.mkdtemp(prefix="fingerprinter-bench-cache-"))
    registry = _build_registry(redis_client, cache_dir)
    engine = _build_engine(threads=0, segment_duration_s=SEGMENT_DURATION_S)
    from acquisition import MediaAcquirer

    acquirer = MediaAcquirer(allow_private_networks=True)  # bench server is loopback

    priority = f"bench-{label}-{uuid.uuid4().hex[:8]}"
    producer = JobProducer(redis_client, priority=priority)
    worker = Worker(redis_client, consumer_name=f"bench-{label}", priority=priority, block_ms=2000)

    target_id_base = f"{label}-target"
    warm_target_id = f"{target_id_base}-shared"
    prewarm_build_s = None
    if not cold:
        prewarm_build_s = _prewarm_target(registry, engine, warm_target_id, "v1")

    redis_before = common.redis_server_info(redis_client)

    per_job = []
    for i in range(reps):
        target_id = warm_target_id if not cold else f"{target_id_base}-{i}-{uuid.uuid4().hex[:8]}"
        if cold:
            _register_target(registry, target_id, "v1", VIDEO_PATH)

        job = _make_job(f"{label}-{i}", server.url(), target_id, "v1")
        producer.enqueue(job)

        t0 = time.monotonic()
        entry = worker.claim_one()
        claim_s = time.monotonic() - t0
        assert entry is not None and entry.is_valid, "benchmark job failed to claim"

        result, timings = run_instrumented(job, acquirer, engine, registry)
        timings.claim_s = claim_s

        t0 = time.monotonic()
        if result is not None:
            worker.commit_result(entry, result)
        elif timings.outcome == "transient_failure":
            worker._handle_transient_failure(entry, timings.error or "transient")
        elif timings.outcome == "permanent_failure":
            worker._fail(entry, timings.error or "permanent")
        else:
            worker.ack(entry)
        timings.commit_s = time.monotonic() - t0

        per_job.append(asdict(timings))

    redis_after = common.redis_server_info(redis_client)
    server.shutdown()

    stage_names = ["claim_s", "acquire_s", "candidate_embed_s", "target_resolve_s", "match_s", "aggregate_s", "commit_s", "handler_total_s"]
    stage_stats = {
        name: common.LatencyStats.from_samples([j[name] for j in per_job if j.get(name) is not None]).__dict__
        for name in stage_names
    }
    outcomes = [j["outcome"] for j in per_job]

    return {
        "label": label,
        "cold": cold,
        "reps": reps,
        "video": VIDEO_PATH.name,
        "segment_duration_s": SEGMENT_DURATION_S,
        "prewarm_build_s": prewarm_build_s,
        "per_job": per_job,
        "stage_latency": stage_stats,
        "outcomes": {o: outcomes.count(o) for o in set(outcomes)},
        "redis_before": redis_before,
        "redis_after": redis_after,
        "redis_delta": common.redis_info_delta(redis_before, redis_after),
    }


# ---------------------------------------------------------------------------
# C: same-target contention
# ---------------------------------------------------------------------------


def _contention_worker_proc(idx: int, redis_url: str, cache_dir: str, target_id: str, threads: int, barrier, out_q):
    engine = _build_engine(threads, SEGMENT_DURATION_S)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    registry = _build_registry(redis_client, Path(cache_dir))
    from worker.matching_handler import _target_artifact

    spec = _embedding_spec_for(engine)

    build_ran = {"flag": False}

    def build(record):
        build_ran["flag"] = True
        target_artifact, _ = _target_artifact(record)
        r = engine.embed_video_segments(target_artifact)
        return r.segments, r.coarse_vector

    barrier.wait()
    t0 = time.monotonic()
    error = None
    try:
        registry.get_or_build_segment_embedding(target_id, "v1", spec, build)
    except Exception as exc:  # noqa: BLE001 - report any failure back to the parent, don't hang it
        error = repr(exc)
    total_s = time.monotonic() - t0

    out_q.put({"idx": idx, "built": build_ran["flag"], "total_s": total_s, "error": error})


def run_contention_workload(num_contenders: int) -> dict:
    ram_reason = _ram_ok_for(num_contenders)
    if ram_reason:
        return {"num_contenders": num_contenders, "skipped": True, "reason": ram_reason}

    redis_client = Redis.from_url(BENCH_REDIS_URL, decode_responses=True)
    redis_client.flushdb()
    import tempfile

    cache_dir = Path(tempfile.mkdtemp(prefix="fingerprinter-bench-cache-"))
    setup_registry = _build_registry(redis_client, cache_dir)
    target_id = f"contend-{num_contenders}-{uuid.uuid4().hex[:8]}"
    _register_target(setup_registry, target_id, "v1", VIDEO_PATH)

    redis_before = common.redis_server_info(redis_client)

    barrier = CTX.Barrier(num_contenders + 1)
    out_q = CTX.Queue()
    procs = [
        CTX.Process(
            target=_contention_worker_proc,
            args=(i, BENCH_REDIS_URL, str(cache_dir), target_id, 1, barrier, out_q),
        )
        for i in range(num_contenders)
    ]
    pids_sampler = common.ResourceSampler(pids=[], interval_s=0.2, gpu=False)
    for p in procs:
        p.start()
        pids_sampler.add_pid(p.pid)
    pids_sampler.start()

    barrier.wait()
    wall_start = time.monotonic()

    results = [out_q.get(timeout=120) for _ in range(num_contenders)]
    wall_s = time.monotonic() - wall_start

    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()
    resource_summary = pids_sampler.stop()

    redis_after = common.redis_server_info(redis_client)

    builders = [r for r in results if r["built"]]
    waiters = [r for r in results if not r["built"]]

    return {
        "num_contenders": num_contenders,
        "target_id": target_id,
        "wall_s": wall_s,
        "results": results,
        "builds": len(builders),
        "waiters": len(waiters),
        "builder_latency_s": builders[0]["total_s"] if builders else None,
        "waiter_latency": common.LatencyStats.from_samples([w["total_s"] for w in waiters]).__dict__ if waiters else None,
        "errors": [r["error"] for r in results if r["error"]],
        "resource": resource_summary.__dict__,
        "redis_before": redis_before,
        "redis_after": redis_after,
        "redis_delta": common.redis_info_delta(redis_before, redis_after),
    }


# ---------------------------------------------------------------------------
# D: different-target worker scaling
# ---------------------------------------------------------------------------


def _stream_worker_proc(idx: int, redis_url: str, cache_dir: str, priority: str, threads: int, barrier, out_q):
    from benchmarks.instrumented_handler import run_instrumented
    from worker.fingerprint_worker import Worker

    engine = _build_engine(threads, SEGMENT_DURATION_S)
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    registry = _build_registry(redis_client, Path(cache_dir))
    from acquisition import MediaAcquirer

    acquirer = MediaAcquirer(allow_private_networks=True)  # bench server is loopback
    worker = Worker(redis_client, consumer_name=f"bench-d-{idx}", priority=priority, block_ms=400)

    barrier.wait()

    consecutive_misses = 0
    processed = 0
    while consecutive_misses < 4:
        entry = worker.claim_one()
        if entry is None or not entry.is_valid:
            consecutive_misses += 1
            continue
        consecutive_misses = 0

        t0 = time.monotonic()
        result, timings = run_instrumented(entry.job, acquirer, engine, registry)
        timings.claim_s = None  # claim latency measured separately in A/B; here we're isolating processing throughput

        tc0 = time.monotonic()
        if result is not None:
            worker.commit_result(entry, result)
        elif timings.outcome == "transient_failure":
            worker._handle_transient_failure(entry, timings.error or "transient")
        elif timings.outcome == "permanent_failure":
            worker._fail(entry, timings.error or "permanent")
        else:
            worker.ack(entry)
        timings.commit_s = time.monotonic() - tc0

        out_q.put({"worker_idx": idx, **asdict(timings)})
        processed += 1

    out_q.put({"worker_idx": idx, "done": True, "processed": processed})


def run_scaling_step(worker_count: int, threads_per_worker: int, jobs_per_worker: int, thread_mode_label: str) -> dict:
    ram_reason = _ram_ok_for(worker_count)
    if ram_reason:
        return {"worker_count": worker_count, "threads_per_worker": threads_per_worker, "skipped": True, "reason": ram_reason}

    body = VIDEO_PATH.read_bytes()
    server = StaticFileServer(body, content_type="video/mp4")
    redis_client = Redis.from_url(BENCH_REDIS_URL, decode_responses=True)
    redis_client.flushdb()

    import tempfile

    cache_dir = Path(tempfile.mkdtemp(prefix="fingerprinter-bench-cache-"))
    setup_engine = _build_engine(threads=1, segment_duration_s=SEGMENT_DURATION_S)
    setup_registry = _build_registry(redis_client, cache_dir)

    n_targets = max(worker_count, 1)
    target_ids = [f"d-target-{thread_mode_label}-{worker_count}-{i}" for i in range(n_targets)]
    for tid in target_ids:
        _prewarm_target(setup_registry, setup_engine, tid, "v1")
    del setup_engine  # free before spawning workers

    priority = f"bench-d-{thread_mode_label}-{worker_count}-{uuid.uuid4().hex[:8]}"
    from work_queue.producer import JobProducer

    producer = JobProducer(redis_client, priority=priority)
    total_jobs = worker_count * jobs_per_worker
    for i in range(total_jobs):
        job = _make_job(f"d-{thread_mode_label}-{worker_count}-{i}", server.url(), target_ids[i % n_targets], "v1")
        producer.enqueue(job)

    redis_before = common.redis_server_info(redis_client)

    barrier = CTX.Barrier(worker_count + 1)
    out_q = CTX.Queue()
    procs = [
        CTX.Process(
            target=_stream_worker_proc,
            args=(i, BENCH_REDIS_URL, str(cache_dir), priority, threads_per_worker, barrier, out_q),
        )
        for i in range(worker_count)
    ]
    sampler = common.ResourceSampler(pids=[], interval_s=0.2, gpu=False)
    for p in procs:
        p.start()
        sampler.add_pid(p.pid)
    sampler.start()

    barrier.wait()
    wall_start = time.monotonic()

    # wall_s (used for throughput) stops the instant the last real job
    # result arrives — NOT when workers' "done" sentinels arrive. A worker
    # only emits "done" after `claim_one()` misses 4 times in a row
    # (up to 4 * block_ms of pure idle polling — see _stream_worker_proc),
    # which is drain-confirmation overhead, not processing time. An
    # earlier version of this benchmark gated wall_s on both counts and
    # measured a ~1.6s inflation from exactly this (worker_count=1,
    # jobs_per_worker=1 wall_s=4.9s when the real handler work was ~3.25s)
    # — caught during development, fixed here.
    per_job = []
    done_count = 0
    wall_s = None
    deadline = time.monotonic() + 180
    while (len(per_job) < total_jobs or done_count < worker_count) and time.monotonic() < deadline:
        try:
            # Short poll interval, re-checked against `deadline` — a gap
            # under severe CPU oversubscription (see thread_mode="default"
            # at higher worker counts) means "still working," not "no more
            # messages ever": an earlier version treated any single 10s gap
            # as end-of-stream and `break`, which under-reported completed
            # jobs as 0 when workers were simply slow, not stuck. Caught
            # during development — see the D[default-threads] worker_count=4
            # result this fix produced vs. the pre-fix run.
            msg = out_q.get(timeout=2)
        except Exception:
            continue
        if msg.get("done"):
            done_count += 1
        else:
            per_job.append(msg)
            if len(per_job) == total_jobs:
                wall_s = time.monotonic() - wall_start
    if wall_s is None:  # didn't reach total_jobs before the deadline
        wall_s = time.monotonic() - wall_start

    for p in procs:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()
    resource_summary = sampler.stop()
    redis_after = common.redis_server_info(redis_client)
    server.shutdown()

    stage_names = ["acquire_s", "candidate_embed_s", "target_resolve_s", "match_s", "aggregate_s", "commit_s", "handler_total_s"]
    stage_stats = {
        name: common.LatencyStats.from_samples([j[name] for j in per_job if j.get(name) is not None]).__dict__
        for name in stage_names
    }
    completed = len(per_job)

    return {
        "worker_count": worker_count,
        "threads_per_worker": threads_per_worker,
        "thread_mode": thread_mode_label,
        "jobs_per_worker": jobs_per_worker,
        "total_jobs": total_jobs,
        "jobs_completed": completed,
        "wall_s": wall_s,
        "throughput_jobs_per_s": completed / wall_s if wall_s > 0 else None,
        "stage_latency": stage_stats,
        "resource": resource_summary.__dict__,
        "redis_before": redis_before,
        "redis_after": redis_after,
        "redis_delta": common.redis_info_delta(redis_before, redis_after),
    }


def run_scaling_workload(worker_counts: Sequence[int], threads_per_worker: int, jobs_per_worker: int, thread_mode_label: str) -> dict:
    steps = []
    baseline_throughput = None
    for wc in worker_counts:
        print(f"  D[{thread_mode_label}] worker_count={wc} threads_per_worker={threads_per_worker} ...")
        step = run_scaling_step(wc, threads_per_worker, jobs_per_worker, thread_mode_label)
        if step.get("skipped"):
            print(f"    skipped: {step['reason']}")
            steps.append(step)
            break
        thr = step["throughput_jobs_per_s"]
        if baseline_throughput is None:
            baseline_throughput = thr
        efficiency = (thr / (wc * baseline_throughput)) if thr and baseline_throughput else None
        step["scaling_efficiency_vs_worker1"] = efficiency
        print(
            f"    completed={step['jobs_completed']}/{step['total_jobs']} wall={step['wall_s']:.2f}s "
            f"throughput={thr:.3f} jobs/s efficiency={efficiency}"
        )
        steps.append(step)
    return {"thread_mode": thread_mode_label, "threads_per_worker": threads_per_worker, "jobs_per_worker": jobs_per_worker, "steps": steps}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> None:
    gen_test_video.generate_all()
    env = common.environment_snapshot()

    print("=== Workload A: warm target cache (single worker, stage latency) ===")
    a = run_stage_latency_workload("A-warm", cold=False, reps=15)
    common.save_result("bench_pipeline_A_warm", {"workload": "A_warm_cache", "environment": env, **a})

    print("=== Workload B: cold target cache (single worker, stage latency) ===")
    b = run_stage_latency_workload("B-cold", cold=True, reps=8)
    common.save_result("bench_pipeline_B_cold", {"workload": "B_cold_cache", "environment": env, **b})

    print("=== Workload C: same-target contention ===")
    c_results = []
    for n in (4, 8):
        print(f"  contenders={n}")
        c = run_contention_workload(n)
        c_results.append(c)
    common.save_result("bench_pipeline_C_contention", {"workload": "C_same_target_contention", "environment": env, "runs": c_results})

    print("=== Workload D: different-target worker scaling ===")
    d_isolated = run_scaling_workload([1, 2, 4, 8], threads_per_worker=1, jobs_per_worker=3, thread_mode_label="isolated-1thread")
    d_default = run_scaling_workload([1, 2, 4], threads_per_worker=0, jobs_per_worker=3, thread_mode_label="default-threads")
    common.save_result(
        "bench_pipeline_D_scaling",
        {"workload": "D_different_target_scaling", "environment": env, "isolated_1thread": d_isolated, "default_threads": d_default},
    )

    print("done.")


if __name__ == "__main__":
    main()
