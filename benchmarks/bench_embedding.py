"""Workload F — embedding-only benchmark (phase-11 brief, "F.
EMBEDDING-ONLY BENCHMARK").

Benchmarks `DINOv2EmbeddingEngine.embed_video_segments` directly against a
local synthetic video file — no network acquisition, no Redis, no worker
plumbing. Separates:

    import/model-load time      (paid once per process)
    first inference              (cold: frame-extraction subprocess spawn,
                                   any lazy torch/BLAS init, first-call
                                   overhead)
    steady-state inference       (repeated calls after warmup)

per phase-11 brief, "Warmup": these must not be mixed into one throughput
number.

GPU: this development machine's DINOv2 engine only ever runs on
`device="cpu"` here — `torch.cuda.is_available()` is `False` in this
environment (confirmed: a physical RTX 2050 is present per `nvidia-smi`,
but `torch.cuda` raises "No CUDA GPUs are available" — see phase-11 doc,
"Hardware/software environment", for the diagnosis). GPU inference timing
is therefore N/A here, not zero and not estimated — it REQUIRES a
CUDA-capable validation host per the phase-11 brief's "Do NOT invent
distributed performance claims" rule.

Run: `python -m benchmarks.bench_embedding`
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Optional

import torch

from acquisition.artifact import MediaArtifact
from benchmarks import common, gen_test_video
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine

STEADY_STATE_REPS = 4

# Captured once at import time, before any benchmark combo calls
# torch.set_num_threads() — `num_threads=None` in a combo means "run at
# this original default," and is applied explicitly every time (not left
# alone), otherwise a later combo would silently inherit an earlier
# combo's thread-count override within the same process. This is exactly
# the kind of measurement confound this benchmark caught during
# development: an earlier version left `None` combos untouched and
# accidentally measured bench_60s at threads=1 (inherited from the prior
# combo) while reporting it as "default".
_DEFAULT_NUM_THREADS = torch.get_num_threads()


def _artifact_for(path: Path) -> MediaArtifact:
    return MediaArtifact(
        local_path=path,
        original_url=f"file://{path}",
        final_url=f"file://{path}",
        content_type="video/mp4",
        byte_size=path.stat().st_size,
        checksum_sha256="",
        acquisition_duration_s=0.0,
    )


def run_combo(video_path: Path, segment_duration_s: float, num_threads: Optional[int]) -> dict:
    torch.set_num_threads(num_threads if num_threads is not None else _DEFAULT_NUM_THREADS)
    actual_threads = torch.get_num_threads()

    t0 = time.monotonic()
    engine = DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=segment_duration_s))
    construct_duration_s = time.monotonic() - t0  # includes engine.model_load_duration_s (processor+model .from_pretrained + .to(device))

    artifact = _artifact_for(video_path)

    # First (cold) call: frame-extraction subprocess spawned for the first
    # time this process, first forward passes.
    t0 = time.monotonic()
    first_result = engine.embed_video_segments(artifact)
    first_wall_s = time.monotonic() - t0

    sampler = common.ResourceSampler(pids=[os.getpid()], interval_s=0.1, gpu=True)
    sampler.start()
    steady_wall = []
    steady_reported = []
    for _ in range(STEADY_STATE_REPS):
        t0 = time.monotonic()
        result = engine.embed_video_segments(artifact)
        steady_wall.append(time.monotonic() - t0)
        steady_reported.append(result.inference_duration_s)
    resource_summary = sampler.stop()

    wall_stats = common.LatencyStats.from_samples(steady_wall)
    reported_stats = common.LatencyStats.from_samples(steady_reported)
    segment_count = first_result.segment_count
    video_duration_s = first_result.total_duration_s

    return {
        "video": str(video_path.name),
        "video_duration_s": video_duration_s,
        "segment_duration_s": segment_duration_s,
        "segment_count": segment_count,
        "torch_num_threads_requested": num_threads,
        "torch_num_threads_actual": actual_threads,
        "model_load_duration_s": engine.model_load_duration_s,
        "engine_construct_duration_s": construct_duration_s,
        "first_inference_wall_s": first_wall_s,
        "first_inference_reported_s": first_result.inference_duration_s,
        "first_inference_note": "includes ffmpeg frame-extraction subprocess spawn + first forward passes; NOT representative of steady state",
        "steady_state_reps": STEADY_STATE_REPS,
        "steady_state_wall_latency": wall_stats.__dict__,
        "steady_state_reported_latency": reported_stats.__dict__,
        "throughput_segments_per_s": (segment_count / wall_stats.mean_s) if wall_stats.mean_s else None,
        "throughput_video_seconds_per_s": (video_duration_s / wall_stats.mean_s) if wall_stats.mean_s else None,
        "throughput_frames_per_s_per_thread": (
            (segment_count / wall_stats.mean_s) / actual_threads if wall_stats.mean_s and actual_threads else None
        ),
        "resource": resource_summary.__dict__,
    }


def main() -> None:
    gen_test_video.generate_all()
    env = common.environment_snapshot()

    combos = [
        # (video, segment_duration_s, num_threads)  None = leave torch default
        (gen_test_video.FIXTURES_DIR / "bench_15s.mp4", 2.5, None),
        (gen_test_video.FIXTURES_DIR / "bench_15s.mp4", 2.5, 1),
        (gen_test_video.FIXTURES_DIR / "bench_60s.mp4", 5.0, None),  # 5.0s = production DEFAULT_SEGMENT_DURATION_S
    ]

    results = []
    for video_path, seg_dur, threads in combos:
        print(f"running: video={video_path.name} segment_duration_s={seg_dur} threads={threads or 'default'}")
        r = run_combo(video_path, seg_dur, threads)
        print(
            f"  segments={r['segment_count']} steady_mean={r['steady_state_wall_latency']['mean_s']*1000:.1f}ms "
            f"throughput={r['throughput_segments_per_s']:.2f} segments/s "
            f"video_throughput={r['throughput_video_seconds_per_s']:.2f} video-s/s "
            f"peak_rss={r['resource']['process_rss_mib_peak']}"
        )
        results.append(r)

    payload = {
        "workload": "F_embedding_only_benchmark",
        "environment": env,
        "device": "cpu",
        "gpu_note": "torch.cuda.is_available() is False in this environment; no GPU timing was collected — see module docstring",
        "combos": results,
    }
    path = common.save_result("bench_embedding", payload)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
