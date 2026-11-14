"""B-CAL — GPU batch calibration as the encoder runs it (OF-2, design §12.3).

Runs ``fingerprinter.calibration.calibrate`` with the encoder's default
parameters (``EncoderConfig``) and the encoder's calibration inputs
(``golden_inputs(8)``) ``--runs`` times in fresh model instances, and records
per run every candidate's samples, median, spread and peak VRAM, the selected
batch, and — for comparison only — the batch the superseded rule
(``B_max/2``) would have chosen with its measured throughput. GPU clocks and
temperature are sampled before and after each run.

This measures calibration, not a gate (G-5 is ``bench_inference.py`` on C-PREC).

    env/bin/python benchmarks/p9-encoder/calibrate.py --runs 2 \
        --out benchmarks/p9-encoder/results/b-cal-rtx2050.json
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from common import environment, write


def gpu_state() -> dict[str, str]:
    q = "clocks.sm,clocks.mem,temperature.gpu,power.draw,pstate"
    try:
        out = subprocess.run(  # noqa: S603
            ["nvidia-smi", f"--query-gpu={q}", "--format=csv,noheader"],  # noqa: S607
            capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return {}
    return dict(zip(q.split(","), [v.strip() for v in out.split(",")], strict=False))


def main() -> None:
    import torch

    from fingerprinter.calibration import calibrate
    from fingerprinter.config import EncoderConfig
    from fingerprinter.model import Embedder, golden_inputs

    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=2)
    ap.add_argument("--dtype", default="float32")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    if not torch.cuda.is_available():
        raise SystemExit("CUDA unavailable: B-CAL is BLOCKED")
    cfg = EncoderConfig()
    x = golden_inputs(8)
    runs = []
    for i in range(args.runs):
        emb = Embedder("cuda", args.dtype)
        before = gpu_state()
        rep = calibrate(emb, x, max_batch=cfg.max_batch, frames=cfg.calibration_frames,
                        reps=cfg.calibration_reps, tolerance=cfg.calibration_tolerance,
                        sync=emb.sync, reset_peak=emb.reset_peak)
        rep["gpu_before"], rep["gpu_after"] = before, gpu_state()
        old = max(1, rep["b_max_fitting"] // 2)
        old_c = next(c for c in rep["candidates"] if c["batch"] == old)
        rep["superseded_rule"] = {"rule": "B_max/2", "batch": old, "median": old_c.get("median")}
        runs.append({"run": i + 1, **rep})
        print(f"run {i + 1}: selected B={rep['selected_batch']} ({rep['selected_frames_per_s']} f/s); "
              f"B_max={rep['b_max_fitting']}; B_max/2={old} ({old_c.get('median')} f/s)")
        del emb
        torch.cuda.empty_cache()
    selected = [r["selected_batch"] for r in runs]
    write(args.out, {
        "benchmark": "B-CAL", "method": runs[0]["method"], "dtype": args.dtype,
        "environment": environment(), "parameters": runs[0]["parameters"], "runs": runs,
        "summary": {"selected_batch_per_run": selected, "repeatable": len(set(selected)) == 1,
                    "superseded_rule_batch": runs[0]["superseded_rule"]["batch"]},
        "gate_status": "not a gate (calibration measurement)",
    })


if __name__ == "__main__":
    main()
