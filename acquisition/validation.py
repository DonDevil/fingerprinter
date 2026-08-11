"""Lightweight media validation via ffprobe.

This is NOT fingerprinting — it only establishes that the downloaded bytes
are a decodable media file the pipeline can later open, and opportunistically
pulls a few cheap metadata fields (duration, codec, dimensions) while doing
so. `ffprobe` is an explicit external dependency: it must be on PATH.

Bounded by a subprocess timeout so a pathological file can't hang
acquisition indefinitely.
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Mapping

from acquisition.errors import InvalidMediaError

DEFAULT_FFPROBE_TIMEOUT_S = 5.0


def probe_media(
    path: Path,
    ffprobe_path: str = "ffprobe",
    timeout_s: float = DEFAULT_FFPROBE_TIMEOUT_S,
) -> Mapping[str, object]:
    """Run ffprobe against `path` and return cheap metadata.

    Raises InvalidMediaError if ffprobe can't identify any stream in the
    file (corrupt, empty, or not actually media) or times out.

    Raises RuntimeError if ffprobe itself is not installed — that's an
    environment/config problem, not a per-job outcome, so it is deliberately
    not classified as Transient/Permanent; it's left uncaught by callers so
    it surfaces like the process-crash path Phase 3 already defines for
    unclassified exceptions, rather than silently mis-marking jobs.
    """
    if shutil.which(ffprobe_path) is None:
        raise RuntimeError(f"{ffprobe_path!r} not found on PATH — required for media validation")

    try:
        proc = subprocess.run(
            [
                ffprobe_path,
                "-v", "error",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise InvalidMediaError(f"media probe timed out after {timeout_s}s") from exc

    if proc.returncode != 0:
        raise InvalidMediaError(f"media probe failed: {proc.stderr.strip()[:300]!r}")

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise InvalidMediaError("media probe returned unparseable output") from exc

    streams = data.get("streams") or []
    if not streams:
        raise InvalidMediaError("media probe found no streams — likely corrupt or empty file")

    fmt = data.get("format", {})
    first_stream = streams[0]
    metadata = {
        "format_name": fmt.get("format_name"),
        "duration_s": float(fmt["duration"]) if fmt.get("duration") else None,
        "codec_type": first_stream.get("codec_type"),
        "codec_name": first_stream.get("codec_name"),
        "width": first_stream.get("width"),
        "height": first_stream.get("height"),
    }
    return {k: v for k, v in metadata.items() if v is not None}
