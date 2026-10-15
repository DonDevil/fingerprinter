"""Shared benchmark helpers (data-preparation §5 methodology).

Every result records hardware, runtime fingerprint, spec digest, batch sizes,
threads and the corpus manifest SHA-256; reports median and p95 with
n ≥ 30 per percentile (else ``small_sample: true``); the actual device is
recorded and a CPU run is never labelled GPU. ``gate_eligible`` is true only
for the input sets the approved gates name (C-PREC, L-PAIRS, SOAK-1000).
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from fingerprinter.media import decode, preprocess  # noqa: E402
from fingerprinter.spec import dinov2_seg_v2  # noqa: E402

GATE_SETS = {"C-PREC", "L-PAIRS", "SOAK-1000"}


def load_manifest(corpus: Path) -> dict[str, Any]:
    path = corpus.expanduser() / "manifest.json"
    data = json.loads(path.read_text())
    data["_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    data["_dir"] = str(corpus.expanduser())
    return data


def videos(manifest: dict[str, Any]) -> list[Path]:
    root = Path(manifest["_dir"])
    return [root / i["file"] for i in manifest["items"] if i.get("kind", "video") in ("video", "clip")]


def frames(paths: list[Path], limit: int | None = None,
           per_item: int | None = None) -> tuple[np.ndarray, list[tuple[str, int]]]:
    """Decode + preprocess once; every mode is then fed the *same* tensors.

    ``per_item`` takes at most that many segments (the first ones) from each
    item, so a fixed-size sample covers every item of the corpus.
    """
    spec = dinov2_seg_v2()
    tensors, keys = [], []
    for p in paths:
        taken = 0
        for f in decode(p, spec):
            if per_item and taken >= per_item:
                break
            tensors.append(preprocess(f.rgb))
            keys.append((p.name, f.segment_index))
            taken += 1
            if limit and len(tensors) >= limit:
                return np.stack(tensors), keys
    return np.stack(tensors), keys


def stats(values: list[float]) -> dict[str, Any]:
    vals = sorted(values)
    p95 = vals[min(len(vals) - 1, int(round(0.95 * (len(vals) - 1))))] if vals else None
    return {"n": len(vals), "median": statistics.median(vals) if vals else None, "p95": p95,
            "min": vals[0] if vals else None, "max": vals[-1] if vals else None,
            "small_sample": len(vals) < 30}


def environment() -> dict[str, Any]:
    import PIL
    import torch
    import transformers

    env: dict[str, Any] = {
        "host": platform.node(), "machine": platform.machine(), "cores": os.cpu_count(),
        "python": platform.python_version(), "torch": torch.__version__,
        "transformers": transformers.__version__, "numpy": np.__version__, "pillow": PIL.__version__,
        "cuda_available": torch.cuda.is_available(), "cuda_runtime": torch.version.cuda,
        "spec": str(dinov2_seg_v2().digest), "spec_fp16": str(dinov2_seg_v2(compute_dtype="float16").digest),
        "runtime_profile": "rt-1", "measured_at": datetime.now(UTC).isoformat(),
    }
    try:
        env["cpu_model"] = next(
            line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        )
        env["ram_gib"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024**3, 1)
    except (OSError, StopIteration, ValueError):
        pass
    if torch.cuda.is_available():
        env["gpu"] = torch.cuda.get_device_name(0)
        free, total = torch.cuda.mem_get_info()
        env["vram_total_mib"], env["vram_free_mib_at_start"] = total // 2**20, free // 2**20
        try:
            env["driver"] = subprocess.run(  # noqa: S603
                ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],  # noqa: S607
                capture_output=True, text=True, check=True, timeout=10).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            env["driver"] = "unknown"
    ff = subprocess.run(["ffmpeg", "-hide_banner", "-version"], capture_output=True, text=True, check=False)  # noqa: S603, S607
    env["ffmpeg"] = ff.stdout.splitlines()[0] if ff.stdout else "unknown"
    return env


def corpus_info(manifest: dict[str, Any]) -> dict[str, Any]:
    name = manifest.get("corpus", "unknown")
    return {"name": name, "items": len(manifest["items"]), "synthetic": bool(manifest.get("synthetic")),
            "manifest_sha256": manifest["_sha256"], "gate_eligible": name in GATE_SETS}


def write(out: Path, report: dict[str, Any]) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1, default=str) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k in ("benchmark", "gate_status", "summary")},
                     indent=1, default=str))
