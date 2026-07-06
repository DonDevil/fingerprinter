from __future__ import annotations

import hashlib
import math
import struct
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class VideoSegmentFingerprint:
    start_second: float
    end_second: float
    digest: str
    frame_count: int


@dataclass(slots=True)
class AudioSegmentFingerprint:
    start_second: float
    end_second: float
    digest: str
    rms: float
    zero_crossing_rate: float


def _ffmpeg_bins() -> list[str]:
    bins: list[str] = []
    try:
        from imageio_ffmpeg import get_ffmpeg_exe

        bins.append(str(get_ffmpeg_exe()))
    except Exception:
        pass
    bins.append("ffmpeg")
    # Preserve insertion order and remove duplicates.
    ordered: list[str] = []
    for item in bins:
        if item not in ordered:
            ordered.append(item)
    return ordered


def _run_ffmpeg(command: list[str]) -> bytes:
    for ffmpeg_bin in _ffmpeg_bins():
        with_bin = [ffmpeg_bin] + command[1:]
        try:
            result = subprocess.run(with_bin, capture_output=True, check=False)
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except OSError:
            continue
    return b""


def _hash_similarity(left: str, right: str) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    try:
        left_int = int(left, 16)
        right_int = int(right, 16)
    except ValueError:
        return 0.0
    xor = left_int ^ right_int
    bits = max(1, len(left) * 4)
    diff = xor.bit_count()
    return max(0.0, 1.0 - (float(diff) / float(bits)))


def _pack_float(value: float) -> bytes:
    return struct.pack("<f", float(value))


def extract_video_segment_fingerprints(
    file_path: str,
    *,
    segment_seconds: float,
    frame_sample_fps: float,
    width: int = 64,
    height: int = 64,
) -> list[VideoSegmentFingerprint]:
    path = Path(file_path)
    if not path.exists() or segment_seconds <= 0.0 or frame_sample_fps <= 0.0:
        return []

    frame_size = width * height
    command = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(path),
        "-vf",
        f"fps={frame_sample_fps},scale={width}:{height},format=gray",
        "-f",
        "rawvideo",
        "-",
    ]
    payload = _run_ffmpeg(command)
    if not payload or len(payload) < frame_size:
        return []

    total_frames = len(payload) // frame_size
    if total_frames <= 0:
        return []

    frames_per_segment = max(1, int(round(segment_seconds * frame_sample_fps)))
    out: list[VideoSegmentFingerprint] = []

    frame_hashes: list[str] = []
    frame_energies: list[float] = []
    for idx in range(total_frames):
        start = idx * frame_size
        chunk = payload[start : start + frame_size]
        frame_hashes.append(hashlib.sha1(chunk).hexdigest())
        frame_energies.append(sum(chunk) / float(frame_size * 255.0))

    seg_index = 0
    for start_idx in range(0, total_frames, frames_per_segment):
        end_idx = min(total_frames, start_idx + frames_per_segment)
        hashes = frame_hashes[start_idx:end_idx]
        energies = frame_energies[start_idx:end_idx]
        if not hashes:
            continue

        aggregate = hashlib.sha1()
        for value in hashes:
            aggregate.update(value.encode("ascii"))
        aggregate.update(_pack_float(sum(energies) / float(len(energies))))

        start_second = float(start_idx) / float(frame_sample_fps)
        end_second = float(end_idx) / float(frame_sample_fps)
        out.append(
            VideoSegmentFingerprint(
                start_second=start_second,
                end_second=end_second,
                digest=aggregate.hexdigest(),
                frame_count=(end_idx - start_idx),
            )
        )
        seg_index += 1

    return out


def extract_audio_segment_fingerprints(
    file_path: str,
    *,
    segment_seconds: float,
    sample_rate: int = 8000,
) -> list[AudioSegmentFingerprint]:
    path = Path(file_path)
    if not path.exists() or segment_seconds <= 0.0 or sample_rate <= 0:
        return []

    command = [
        "ffmpeg",
        "-v",
        "error",
        "-i",
        str(path),
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-f",
        "s16le",
        "-",
    ]
    payload = _run_ffmpeg(command)
    if not payload or len(payload) < 2:
        return []

    total_samples = len(payload) // 2
    if total_samples <= 0:
        return []

    samples = struct.unpack("<" + "h" * total_samples, payload[: total_samples * 2])
    samples_per_segment = max(1, int(round(segment_seconds * float(sample_rate))))

    out: list[AudioSegmentFingerprint] = []
    for start_idx in range(0, total_samples, samples_per_segment):
        end_idx = min(total_samples, start_idx + samples_per_segment)
        segment = samples[start_idx:end_idx]
        if not segment:
            continue

        length = float(len(segment))
        rms = math.sqrt(sum(float(x) * float(x) for x in segment) / length) / 32768.0
        zero_crossings = 0
        for idx in range(1, len(segment)):
            a = segment[idx - 1]
            b = segment[idx]
            if (a <= 0 < b) or (a >= 0 > b):
                zero_crossings += 1
        zcr = float(zero_crossings) / max(1.0, length - 1.0)

        digest = hashlib.sha1()
        digest.update(str(start_idx).encode("ascii"))
        digest.update(str(end_idx).encode("ascii"))
        digest.update(_pack_float(rms))
        digest.update(_pack_float(zcr))

        start_second = float(start_idx) / float(sample_rate)
        end_second = float(end_idx) / float(sample_rate)
        out.append(
            AudioSegmentFingerprint(
                start_second=start_second,
                end_second=end_second,
                digest=digest.hexdigest(),
                rms=rms,
                zero_crossing_rate=zcr,
            )
        )

    return out


def segment_hash_similarity(left: str, right: str) -> float:
    return _hash_similarity(left, right)
