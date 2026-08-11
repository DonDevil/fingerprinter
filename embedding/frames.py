"""Deterministic video frame extraction via `ffmpeg`.

`ffmpeg` is already an external dependency of this project (Phase 5 uses
`ffprobe` from the same package for media validation), so frame extraction
reuses it via subprocess rather than adding a second decoding dependency
(e.g. `opencv-python` or `PyAV`). This module only extracts frames to disk
— it has no model/tensor/embedding knowledge, matching the phase brief's
image-vs-video boundary ("DINOv2 operates on images").

Determinism: `ffmpeg -vf fps=<fps>` samples at a fixed rate starting from
the first frame (t=0), in presentation order — same input bytes + same fps
+ same max_frames always yields the same sampled frame sequence. `-frames:v
<max_frames>` caps output count directly (no need to know duration ahead of
time); ffmpeg simply stops decoding once satisfied or the input ends,
whichever comes first — this is `SamplingConfig.frame_selection ==
"uniform_time_from_start"`.

Resource bounds: frames are written to a temporary directory one at a time
by ffmpeg itself (never buffered fully in this process's memory), and the
caller (`DINOv2EmbeddingEngine`) is expected to process-then-delete each
frame file rather than opening all of them at once — see `extract_frames`'s
docstring.
"""
from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import List

from embedding.config import SamplingConfig
from embedding.errors import UnsupportedMediaError

DEFAULT_FFMPEG_TIMEOUT_S = 60.0


def extract_frames(media_path: Path, sampling: SamplingConfig, frames_dir: Path) -> List[Path]:
    """Extract up to `sampling.max_frames` frames at `sampling.fps` from
    `media_path` into `frames_dir` (must already exist), returning the frame
    file paths in presentation order.

    Frames are written as PNG (lossless — no re-compression artifacts ahead
    of a preprocessing pipeline that will resize/crop/normalize them
    anyway). The caller owns `frames_dir` and should delete each file after
    embedding it (and remove the directory when done) to keep peak disk/RAM
    bounded — this function does not clean up after itself.

    Raises `UnsupportedMediaError` if ffmpeg exits non-zero or produces no
    frames at all (corrupt/empty/undecodable video).
    """
    if sampling.fps <= 0 or sampling.max_frames <= 0:
        raise UnsupportedMediaError(
            f"invalid sampling config for video: fps={sampling.fps}, max_frames={sampling.max_frames}"
        )

    output_pattern = str(frames_dir / "frame_%06d.png")
    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(media_path),
        "-vf",
        f"fps={sampling.fps}",
        "-frames:v",
        str(sampling.max_frames),
        output_pattern,
    ]
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=DEFAULT_FFMPEG_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        raise UnsupportedMediaError(f"ffmpeg timed out extracting frames from {media_path}") from exc
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is not installed / not on PATH") from exc

    if proc.returncode != 0:
        raise UnsupportedMediaError(
            f"ffmpeg failed to extract frames from {media_path}: {proc.stderr.decode('utf-8', 'replace').strip()}"
        )

    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    if not frame_paths:
        raise UnsupportedMediaError(f"ffmpeg produced no frames from {media_path} (empty/corrupt video)")
    return frame_paths


def make_frames_dir() -> Path:
    """A fresh temp directory for one video's extracted frames. Kept as a
    tiny helper (rather than inlined `tempfile.mkdtemp` at each call site)
    so the naming convention lives in one place."""
    return Path(tempfile.mkdtemp(prefix="fingerprinter-frames-"))
