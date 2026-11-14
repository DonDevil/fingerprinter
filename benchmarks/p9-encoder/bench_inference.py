"""B-GPU-a — inference-stage throughput, GPU vs CPU on the same tensors (G-5, P9-D17).

Frames are decoded and preprocessed **once** into RAM; the CPU fallback (all
cores, FP32), CUDA FP32 (the default spec) and CUDA FP16 (a distinct, gated
spec — reported separately, never used for the gate unless enabled) embed the
same tensors. One warm-up pass is discarded; ``--reps`` timed passes over all
frames; per-batch latencies give median/p95.

The approved gate input is C-PREC; any other corpus gives a measurement with
``gate_status: NOT MEASURED``. The threshold (5×, FP32 vs FP32) and this method
are fixed (operator decision OF-3). The GPU batch is, by default, the one the
encoder's calibration (``throughput/v1``, OF-2) selects on this host — measured
here and recorded — and the CPU batch is ``encoder.cpu_batch``; both can be
overridden for exploration, which the report records.

    env/bin/python benchmarks/p9-encoder/bench_inference.py --corpus <dir> \
        --frames 256 --reps 3 --out benchmarks/p9-encoder/results/b-gpu-a-<name>.json
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np

from common import corpus_info, environment, frames, load_manifest, stats, videos, write


def run(emb, x: np.ndarray, batch: int, reps: int, sync) -> dict:
    emb.embed(x[:batch])  # warm-up (discarded)
    sync()
    per_batch, totals = [], []
    for _ in range(reps):
        t0 = time.perf_counter()
        for i in range(0, len(x), batch):
            b0 = time.perf_counter()
            emb.embed(x[i : i + batch])
            sync()
            per_batch.append((time.perf_counter() - b0) / len(x[i : i + batch]))
        totals.append(len(x) / (time.perf_counter() - t0))
    return {"batch": batch, "frames": len(x), "reps": reps,
            "frames_per_s": stats(totals), "s_per_frame_in_batch": stats(per_batch)}


def main() -> None:
    import os

    import torch

    from fingerprinter.calibration import calibrate
    from fingerprinter.config import EncoderConfig
    from fingerprinter.model import Embedder, golden_inputs

    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--frames", type=int, default=256)
    ap.add_argument("--per-item", type=int, default=None, help="at most this many segments per item (spreads the sample)")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--cpu-batch", type=int, default=None, help="default: encoder.cpu_batch")
    ap.add_argument("--gpu-batch", default="calibrated", help="'calibrated' (default) or an integer")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = load_manifest(args.corpus)
    x, keys = frames(videos(manifest), args.frames, args.per_item)
    cores = os.cpu_count() or 1
    report = {"benchmark": "B-GPU-a", "gate": "G-5: GPU inference ≥ 5× CPU inference, same tensors, FP32",
              "environment": environment(), "corpus": corpus_info(manifest),
              "inputs": {"frames": len(x), "sources": len({k[0] for k in keys}), "per_item": args.per_item,
                         "tensor_bytes": int(x.nbytes)}}
    cfg = EncoderConfig()
    cpu_batch = args.cpu_batch or cfg.cpu_batch
    cpu = Embedder("cpu", "float32", threads=cores)
    report["cpu_fp32"] = {**run(cpu, x, cpu_batch, args.reps, lambda: None), "threads": cores,
                          "device": "cpu", "batch_source": "argument" if args.cpu_batch else "encoder.cpu_batch"}
    del cpu
    if torch.cuda.is_available():
        sync = torch.cuda.synchronize
        g32 = Embedder("cuda", "float32")
        if args.gpu_batch == "calibrated":
            cal = calibrate(g32, golden_inputs(8), max_batch=cfg.max_batch, frames=cfg.calibration_frames,
                            reps=cfg.calibration_reps, tolerance=cfg.calibration_tolerance,
                            sync=g32.sync, reset_peak=g32.reset_peak)
            gpu_batch, source = cal["selected_batch"], "encoder calibration throughput/v1"
            report["gpu_calibration"] = cal
        else:
            gpu_batch, source = int(args.gpu_batch), "argument"
        torch.cuda.reset_peak_memory_stats()
        report["cuda_fp32"] = {**run(g32, x, gpu_batch, args.reps, sync), "device": "cuda",
                               "batch_source": source,
                               "max_memory_allocated": torch.cuda.max_memory_allocated(),
                               "max_memory_reserved": torch.cuda.max_memory_reserved()}
        del g32
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        g16 = Embedder("cuda", "float16")
        report["cuda_fp16_gated_spec"] = {**run(g16, x, gpu_batch, args.reps, sync), "device": "cuda",
                                          "note": "distinct gated spec; not usable for the gate unless enabled (D3)",
                                          "max_memory_allocated": torch.cuda.max_memory_allocated()}
        ratio = report["cuda_fp32"]["frames_per_s"]["median"] / report["cpu_fp32"]["frames_per_s"]["median"]
        report["summary"] = {"gpu_fp32_over_cpu_fp32": round(ratio, 2), "threshold": 5.0,
                             "cpu_batch": cpu_batch, "gpu_batch": gpu_batch,
                             "meets_threshold_on_these_inputs": ratio >= 5.0}
    else:
        report["summary"] = {"gpu": "unavailable"}
    # G-5 is defined on C-PREC only (design §13.3); other gate sets are measurements, not the gate.
    eligible = report["corpus"]["name"] == "C-PREC" and torch.cuda.is_available()
    if not torch.cuda.is_available():
        report["gate_status"] = "BLOCKED (no CUDA device)"
    elif not eligible:
        report["gate_status"] = "NOT MEASURED (input set is not C-PREC)"
    else:
        report["gate_status"] = "PASS" if report["summary"]["meets_threshold_on_these_inputs"] else "FAIL"
    write(args.out, report)


if __name__ == "__main__":
    main()
