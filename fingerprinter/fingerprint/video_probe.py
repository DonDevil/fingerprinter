from __future__ import annotations

import json
import subprocess
from pathlib import Path


def get_video_duration_seconds(file_path: str) -> float | None:
    """Probe media duration using ffprobe and return seconds when available."""

    path = Path(file_path)
    if not path.exists():
        return None

    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(path),
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        payload = json.loads(result.stdout or "{}")
        duration = payload.get("format", {}).get("duration")
        if duration is None:
            return None
        return float(duration)
    except (ValueError, OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None
