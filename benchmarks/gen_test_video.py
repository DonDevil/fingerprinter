"""Generates deterministic synthetic benchmark videos via ffmpeg's
`testsrc2` source (a built-in pattern generator — no external asset, no
network fetch, same bytes every time given the same ffmpeg version/args).

Not a fixture used by any test — these live under `benchmarks/fixtures/`
and are regenerated on demand by running this module directly:

    python -m benchmarks.gen_test_video

`tests/fixtures/tiny_video.mp4` (2s, 32x32) is deliberately too small to
say anything about realistic per-frame DINOv2 cost — see phase-11 doc,
"Benchmark inputs" for why a separate, larger synthetic video is used for
performance measurement instead of reusing that fixture.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Tuple

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# (name, duration_s, width, height, fps)
VIDEO_SPECS: Tuple[tuple, ...] = (
    ("bench_15s.mp4", 15, 320, 240, 24),
    ("bench_60s.mp4", 60, 320, 240, 24),
)


def generate(name: str, duration_s: int, width: int, height: int, fps: int, force: bool = False) -> Path:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = FIXTURES_DIR / name
    if out_path.exists() and not force:
        return out_path

    cmd = [
        "ffmpeg",
        "-y",
        "-v",
        "error",
        "-f",
        "lavfi",
        "-i",
        f"testsrc2=size={width}x{height}:rate={fps}:duration={duration_s}",
        "-pix_fmt",
        "yuv420p",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        str(out_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True, timeout=120)
    return out_path


def generate_all(force: bool = False) -> None:
    for name, duration_s, width, height, fps in VIDEO_SPECS:
        path = generate(name, duration_s, width, height, fps, force=force)
        size_kib = path.stat().st_size / 1024.0
        print(f"{path} ({duration_s}s, {width}x{height}@{fps}fps, {size_kib:.1f} KiB)")


if __name__ == "__main__":
    import sys

    generate_all(force="--force" in sys.argv)
