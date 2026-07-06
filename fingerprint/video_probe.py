from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path


def _find_ffmpeg_binary() -> str | None:
    found = shutil.which("ffmpeg")
    if found:
        return found

    try:
        from imageio_ffmpeg import get_ffmpeg_exe

        return str(get_ffmpeg_exe())
    except Exception:
        return None


def _find_ffprobe_binary() -> str | None:
    found = shutil.which("ffprobe")
    if found:
        return found

    ffmpeg_path = _find_ffmpeg_binary()
    if not ffmpeg_path:
        return None

    ffprobe_guess = str(Path(ffmpeg_path).with_name("ffprobe"))
    if Path(ffprobe_guess).exists():
        return ffprobe_guess
    return None


def _safe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _probe_with_ffprobe(path: Path) -> dict[str, object] | None:
    ffprobe_bin = _find_ffprobe_binary()
    if not ffprobe_bin:
        return None

    command = [
        ffprobe_bin,
        "-v",
        "error",
        "-show_entries",
        "format=duration,size:stream=index,codec_type,codec_name,width,height,r_frame_rate",
        "-of",
        "json",
        str(path),
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        payload = json.loads(result.stdout or "{}")
    except (ValueError, OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None

    format_info = payload.get("format", {}) if isinstance(payload, dict) else {}
    streams = payload.get("streams", []) if isinstance(payload, dict) else []

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), {})

    return {
        "duration_seconds": _safe_float(format_info.get("duration")),
        "size_bytes": int(format_info.get("size")) if str(format_info.get("size", "")).isdigit() else None,
        "video_codec": video_stream.get("codec_name"),
        "audio_codec": audio_stream.get("codec_name"),
        "width": int(video_stream.get("width")) if str(video_stream.get("width", "")).isdigit() else None,
        "height": int(video_stream.get("height")) if str(video_stream.get("height", "")).isdigit() else None,
        "fps": str(video_stream.get("r_frame_rate")) if video_stream.get("r_frame_rate") else None,
    }


_DURATION_RE = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")
_VIDEO_RE = re.compile(r"Video:\s*([^,]+),.*?(\d{2,5})x(\d{2,5})")
_AUDIO_RE = re.compile(r"Audio:\s*([^,]+)")


def _probe_with_ffmpeg(path: Path) -> dict[str, object] | None:
    ffmpeg_bin = _find_ffmpeg_binary()
    if not ffmpeg_bin:
        return None

    command = [ffmpeg_bin, "-v", "info", "-i", str(path), "-f", "null", "-"]

    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        output = (result.stderr or "") + "\n" + (result.stdout or "")
    except (OSError, subprocess.SubprocessError):
        return None

    duration_seconds = None
    match_duration = _DURATION_RE.search(output)
    if match_duration:
        hours = int(match_duration.group(1))
        minutes = int(match_duration.group(2))
        seconds = float(match_duration.group(3))
        duration_seconds = float(hours * 3600 + minutes * 60) + seconds

    width = None
    height = None
    video_codec = None
    match_video = _VIDEO_RE.search(output)
    if match_video:
        video_codec = match_video.group(1).strip()
        width = int(match_video.group(2))
        height = int(match_video.group(3))

    audio_codec = None
    match_audio = _AUDIO_RE.search(output)
    if match_audio:
        audio_codec = match_audio.group(1).strip()

    return {
        "duration_seconds": duration_seconds,
        "size_bytes": path.stat().st_size if path.exists() else None,
        "video_codec": video_codec,
        "audio_codec": audio_codec,
        "width": width,
        "height": height,
        "fps": None,
    }


def get_media_probe(file_path: str) -> dict[str, object]:
    """Return media probe fields used across all matcher stages."""

    path = Path(file_path)
    empty = {
        "duration_seconds": None,
        "size_bytes": None,
        "video_codec": None,
        "audio_codec": None,
        "width": None,
        "height": None,
        "fps": None,
    }
    if not path.exists():
        return empty

    ffprobe_payload = _probe_with_ffprobe(path)
    if ffprobe_payload is not None:
        return ffprobe_payload

    ffmpeg_payload = _probe_with_ffmpeg(path)
    if ffmpeg_payload is not None:
        return ffmpeg_payload

    empty["size_bytes"] = path.stat().st_size if path.exists() else None
    return empty


def get_video_duration_seconds(file_path: str) -> float | None:
    """Probe media duration using ffprobe and return seconds when available."""

    payload = get_media_probe(file_path)
    return _safe_float(payload.get("duration_seconds"))
