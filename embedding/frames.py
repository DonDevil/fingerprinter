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

import logging
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional

from embedding.config import SamplingConfig, SegmentSamplingConfig
from embedding.errors import UnsupportedMediaError

logger = logging.getLogger(__name__)

DEFAULT_FFMPEG_TIMEOUT_S = 60.0
# Segment-mode extraction walks the whole file (no `-frames:v` cap), so a
# short video can still take a while to decode on a slow/loaded host.
# Generous relative to DEFAULT_FFMPEG_TIMEOUT_S because duration is no
# longer bounded by a frame cap the way the Phase 7 path is.
DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S = 300.0


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
    logger.debug("ffmpeg extract_frames starting: %s (fps=%s, max_frames=%s, timeout=%.1fs)",
                 media_path, sampling.fps, sampling.max_frames, DEFAULT_FFMPEG_TIMEOUT_S)
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=DEFAULT_FFMPEG_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        logger.debug("ffmpeg extract_frames timed out after %.1fs: %s", time.monotonic() - started, media_path)
        raise UnsupportedMediaError(f"ffmpeg timed out extracting frames from {media_path}") from exc
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is not installed / not on PATH") from exc

    if proc.returncode != 0:
        logger.debug("ffmpeg extract_frames failed (returncode=%d) after %.2fs: %s",
                     proc.returncode, time.monotonic() - started, media_path)
        raise UnsupportedMediaError(
            f"ffmpeg failed to extract frames from {media_path}: {proc.stderr.decode('utf-8', 'replace').strip()}"
        )

    frame_paths = sorted(frames_dir.glob("frame_*.png"))
    if not frame_paths:
        raise UnsupportedMediaError(f"ffmpeg produced no frames from {media_path} (empty/corrupt video)")
    logger.debug("ffmpeg extract_frames done: %d frame(s) in %.2fs", len(frame_paths), time.monotonic() - started)
    return frame_paths


def extract_segment_frames(
    media_path: Path,
    sampling: SegmentSamplingConfig,
    frames_dir: Path,
    timeout: Optional[float] = DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S,
) -> List[Path]:
    """Extract one representative frame per `sampling.segment_duration_s`
    window, spanning the *entire* video (no frame-count cap) — the Phase 9
    replacement for `extract_frames`'s fixed-window sampling. Returns frame
    file paths in presentation order; `frame_paths[i]` corresponds to the
    segment starting at `i * sampling.segment_duration_s` seconds (see
    `embedding.result.SegmentEmbedding`, which pairs each frame's embedding
    with its `(segment_index, start_time, end_time)`).

    Same `ffmpeg -vf fps=<rate>` mechanism as `extract_frames` (see that
    function's docstring for the determinism argument), just with
    `rate = 1 / segment_duration_s` and no `-frames:v` cap — ffmpeg decodes
    until the input ends rather than until a frame count is reached.

    `timeout` is the subprocess execution-policy this extraction runs
    under, seconds or `None`. It defaults to `DEFAULT_SEGMENT_FFMPEG_TIMEOUT_S`
    — the existing bounded behavior every pre-existing caller (runtime
    fingerprint-worker candidate/target processing) still gets automatically.
    Passing `timeout=None` disables the subprocess timeout entirely (no
    `subprocess.TimeoutExpired` can occur from this call) — reserved for the
    explicit, operator-triggered `target.cli build` path, where a
    full-length target is allowed to take as long as it takes to decode
    (see `target/build.py`). Never widen this to a large finite value to
    simulate "unlimited"; pass `None`.

    Raises `UnsupportedMediaError` if ffmpeg exits non-zero or produces no
    frames at all (corrupt/empty/undecodable video).
    """
    if sampling.segment_duration_s <= 0:
        raise UnsupportedMediaError(f"invalid segment sampling config: segment_duration_s={sampling.segment_duration_s}")

    output_pattern = str(frames_dir / "segment_%06d.png")
    fps = 1.0 / sampling.segment_duration_s
    cmd = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(media_path),
        "-vf",
        f"fps={fps!r}",
        output_pattern,
    ]
    timeout_desc = f"{timeout:.1f}s" if timeout is not None else "unbounded"
    logger.debug("ffmpeg extract_segment_frames starting: %s (segment_duration_s=%s, timeout=%s)",
                 media_path, sampling.segment_duration_s, timeout_desc)
    started = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        logger.debug("ffmpeg extract_segment_frames timed out after %.1fs: %s",
                     time.monotonic() - started, media_path)
        raise UnsupportedMediaError(f"ffmpeg timed out extracting segment frames from {media_path}") from exc
    except FileNotFoundError as exc:
        raise RuntimeError("ffmpeg is not installed / not on PATH") from exc

    if proc.returncode != 0:
        logger.debug("ffmpeg extract_segment_frames failed (returncode=%d) after %.2fs: %s",
                     proc.returncode, time.monotonic() - started, media_path)
        raise UnsupportedMediaError(
            f"ffmpeg failed to extract segment frames from {media_path}: "
            f"{proc.stderr.decode('utf-8', 'replace').strip()}"
        )

    frame_paths = sorted(frames_dir.glob("segment_*.png"))
    if not frame_paths:
        raise UnsupportedMediaError(f"ffmpeg produced no segment frames from {media_path} (empty/corrupt video)")
    logger.debug("ffmpeg extract_segment_frames done: %d segment frame(s) in %.2fs",
                 len(frame_paths), time.monotonic() - started)
    return frame_paths


def make_frames_dir() -> Path:
    """A fresh temp directory for one video's extracted frames. Kept as a
    tiny helper (rather than inlined `tempfile.mkdtemp` at each call site)
    so the naming convention lives in one place."""
    return Path(tempfile.mkdtemp(prefix="fingerprinter-frames-"))
