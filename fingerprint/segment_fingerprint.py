from __future__ import annotations

import hashlib
import math
import struct
import subprocess
from dataclasses import dataclass
from pathlib import Path

import numpy as np

# Bump this constant whenever the fingerprint algorithm changes so cached target
# fingerprints are automatically invalidated and rebuilt.
FINGERPRINT_ALGO_VERSION = "v3"


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


def _run_ffmpeg_vdpau(
    args_after_input: list[str],
    input_path: str,
    gpu_device: int = 0,
) -> bytes:
    """Attempt VDPAU hardware-accelerated video decode; returns b'' on any failure."""
    for ffmpeg_bin in _ffmpeg_bins():
        command = [
            ffmpeg_bin,
            "-v", "error",
            "-hwaccel", "vdpau",
            "-i", input_path,
        ] + args_after_input
        try:
            result = subprocess.run(command, capture_output=True, check=False)
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


def _batch_frame_phash(frames_np: "np.ndarray", *, grid: int = 8) -> list[str]:
    """Compute perceptual block-hash for every frame in a (N, H, W) uint8 array.

    Returns a list of 16-char hex strings (64-bit pHash per frame).
    Near-uniform frames (low variance — black leaders, colour cards) return ""
    since they produce degenerate hashes that inflate cross-movie similarity.
    """
    n, h, w = frames_np.shape
    gh = h // grid
    gw = w // grid
    if gh == 0 or gw == 0 or n == 0:
        return [""] * n

    cropped = frames_np[:, :gh * grid, :gw * grid].astype(np.float32)
    blocked = cropped.reshape(n, grid, gh, grid, gw)
    block_means = blocked.mean(axis=(2, 4))              # (n, grid, grid)
    frame_avgs = block_means.mean(axis=(1, 2), keepdims=True)  # (n, 1, 1)
    frame_stds = block_means.std(axis=(1, 2))            # (n,)

    # Skip near-uniform frames (std of block means < 4 out of 255 range)
    # They produce identical hashes for unrelated content (black/white cards, titles).
    valid_mask = frame_stds >= 4.0                        # (n,)

    bits_2d = (block_means > frame_avgs).astype(np.uint8)  # strict > avoids all-same-bit output
    bits_flat = bits_2d.reshape(n, grid * grid)           # (n, 64)
    packed = np.packbits(bits_flat, axis=1)               # (n, 8)

    result: list[str] = []
    for i in range(n):
        result.append(packed[i].tobytes().hex() if valid_mask[i] else "")
    return result


def _aggregate_segment_phash(frame_hashes: list[str], *, bit_count: int = 64) -> str:
    """Majority-vote aggregation of per-frame pHashes into one segment pHash.

    Each bit in the output is 1 iff strictly more than half of the input frames
    have that bit set, producing a stable representative hash for the segment.
    """
    if not frame_hashes:
        return ""
    valid = [h for h in frame_hashes if h]
    if not valid:
        return ""

    bits_rows = np.array(
        [list(map(int, format(int(h, 16), f"0{bit_count}b"))) for h in valid],
        dtype=np.uint8,
    )  # shape: (n_frames, bit_count)
    # Strict majority: bit is 1 only if more than half agree
    majority = (bits_rows.sum(axis=0) > len(valid) / 2.0).astype(np.uint8)
    packed = np.packbits(majority).tobytes()
    width_hex = max(1, bit_count // 4)
    return packed.hex()[:width_hex]


def _spectral_band_hash(
    segment_float: "np.ndarray",
    *,
    sample_rate: int,
    n_bands: int = 32,
) -> str:
    """32-band spectral energy pHash via FFT — content-specific and phase-invariant.

    Frequency bands are log-spaced from 60 Hz to min(4 kHz, Nyquist).
    Returns an 8-char hex string (32 bits).
    Same audio content → near-identical hash; different content → very different hash.
    """
    n = len(segment_float)
    if n < 64:
        return ""

    # Hanning window reduces spectral leakage
    window = np.hanning(n)
    spectrum = np.abs(np.fft.rfft(segment_float * window))  # only positive freqs
    n_freqs = len(spectrum)
    freq_res = float(sample_rate) / float(n)  # Hz per bin

    # Log-spaced band edges in frequency-bin index space
    f_min = 60.0
    f_max = min(4000.0, sample_rate / 2.0 - freq_res)
    idx_min = max(1, int(f_min / freq_res))
    idx_max = min(n_freqs - 1, int(f_max / freq_res))

    if idx_max <= idx_min:
        return ""

    edges = np.unique(
        np.round(
            np.logspace(np.log10(idx_min), np.log10(idx_max), n_bands + 1)
        ).astype(int)
    )
    edges = np.clip(edges, 0, n_freqs - 1)
    n_actual = len(edges) - 1
    if n_actual <= 0:
        return ""

    # Energy (power) per band
    band_energies = np.array(
        [np.sum(spectrum[edges[i] : edges[i + 1]] ** 2) for i in range(n_actual)]
    )

    # Pad or trim to exactly n_bands
    if len(band_energies) < n_bands:
        band_energies = np.pad(band_energies, (0, n_bands - len(band_energies)))
    else:
        band_energies = band_energies[:n_bands]

    # Normalise by total energy so the hash encodes spectral SHAPE not level.
    # Near-silent segments produce near-zero total energy — return "" (no fingerprint).
    total_energy = band_energies.sum()
    if total_energy < 1e-10:
        return ""
    normalised = band_energies / total_energy

    # pHash: bit is 1 if band fraction >= mean fraction (1/n_bands)
    avg_frac = 1.0 / float(n_bands)
    bits = (normalised >= avg_frac).astype(np.uint8)
    packed = np.packbits(bits).tobytes()  # 4 bytes = 8 hex chars
    return packed.hex()


def extract_video_segment_fingerprints(
    file_path: str,
    *,
    segment_seconds: float,
    frame_sample_fps: float,
    width: int = 64,
    height: int = 64,
    use_gpu: bool = False,
    gpu_device: int = 0,
) -> list[VideoSegmentFingerprint]:
    path = Path(file_path)
    if not path.exists() or segment_seconds <= 0.0 or frame_sample_fps <= 0.0:
        return []

    frame_size = width * height
    vf_args = [
        "-vf", f"fps={frame_sample_fps},scale={width}:{height},format=gray",
        "-f", "rawvideo", "-",
    ]
    cpu_command = ["ffmpeg", "-v", "error", "-i", str(path)] + vf_args

    # Try VDPAU GPU decode first (offloads video decode to NVIDIA GPU on Linux)
    payload = b""
    if use_gpu:
        payload = _run_ffmpeg_vdpau(vf_args, str(path), gpu_device=gpu_device)

    if not payload:
        payload = _run_ffmpeg(cpu_command)
    if not payload or len(payload) < frame_size:
        return []

    total_frames = len(payload) // frame_size
    if total_frames <= 0:
        return []

    frames_per_segment = max(1, int(round(segment_seconds * frame_sample_fps)))

    # Batch numpy processing: (total_frames, height, width)
    all_frames = np.frombuffer(payload[: total_frames * frame_size], dtype=np.uint8)
    all_frames = all_frames.reshape(total_frames, height, width)

    frame_phashes = _batch_frame_phash(all_frames, grid=8)
    # Energy (brightness) per frame for bonus context
    frame_energies = all_frames.mean(axis=(1, 2)) / 255.0  # (total_frames,)

    out: list[VideoSegmentFingerprint] = []
    for start_idx in range(0, total_frames, frames_per_segment):
        end_idx = min(total_frames, start_idx + frames_per_segment)
        seg_hashes = frame_phashes[start_idx:end_idx]
        seg_hash = _aggregate_segment_phash(seg_hashes, bit_count=64)
        if not seg_hash:
            continue

        start_second = float(start_idx) / float(frame_sample_fps)
        end_second = float(end_idx) / float(frame_sample_fps)
        out.append(
            VideoSegmentFingerprint(
                start_second=start_second,
                end_second=end_second,
                digest=seg_hash,
                frame_count=(end_idx - start_idx),
            )
        )

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
        "ffmpeg", "-v", "error",
        "-i", str(path),
        "-ac", "1",
        "-ar", str(sample_rate),
        "-f", "s16le", "-",
    ]
    payload = _run_ffmpeg(command)
    if not payload or len(payload) < 2:
        return []

    # Vectorised decode: int16 PCM → float32 in [-1, 1]
    samples = np.frombuffer(payload, dtype=np.int16).astype(np.float32) / 32768.0
    total_samples = len(samples)
    samples_per_segment = max(1, int(round(segment_seconds * float(sample_rate))))

    out: list[AudioSegmentFingerprint] = []
    for start_idx in range(0, total_samples, samples_per_segment):
        end_idx = min(total_samples, start_idx + samples_per_segment)
        seg = samples[start_idx:end_idx]
        if len(seg) < 4:
            continue

        # RMS energy
        rms = float(np.sqrt(np.mean(seg ** 2)))

        # Zero-crossing rate
        signs = np.sign(seg)
        zcr = float(np.sum(np.abs(np.diff(signs)) > 0)) / max(1.0, float(len(seg) - 1))

        # Spectral band pHash — the primary discriminative feature
        digest = _spectral_band_hash(seg, sample_rate=sample_rate)

        start_second = float(start_idx) / float(sample_rate)
        end_second = float(end_idx) / float(sample_rate)
        out.append(
            AudioSegmentFingerprint(
                start_second=start_second,
                end_second=end_second,
                digest=digest,
                rms=rms,
                zero_crossing_rate=zcr,
            )
        )

    return out


def segment_hash_similarity(left: str, right: str) -> float:
    return _hash_similarity(left, right)


# ---------------------------------------------------------------------------
# Audio cross-correlation verification
# ---------------------------------------------------------------------------

def _run_ffmpeg_slice(
    file_path: str,
    start_seconds: float,
    duration_seconds: float,
    sample_rate: int,
) -> bytes:
    """Extract a PCM slice from a media file starting at `start_seconds`.

    Uses ffmpeg `-ss` seek for fast extraction without decoding the whole file.
    """
    for ffmpeg_bin in _ffmpeg_bins():
        command = [
            ffmpeg_bin, "-v", "error",
            "-ss", str(max(0.0, start_seconds)),
            "-t", str(max(0.1, duration_seconds)),
            "-i", file_path,
            "-ac", "1",
            "-ar", str(sample_rate),
            "-f", "s16le", "-",
        ]
        try:
            result = subprocess.run(command, capture_output=True, check=False)
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except OSError:
            continue
    return b""


def verify_audio_at_offset(
    target_path: str,
    candidate_path: str,
    target_offset_seconds: float,
    *,
    sample_rate: int = 8000,
    hop_ms: int = 50,
) -> float:
    """Energy-envelope cross-correlation between candidate audio and a target slice.

    Seeks to `target_offset_seconds` in the target file and extracts a window
    matching the candidate duration (+ 4 s padding).  Returns the normalised
    peak cross-correlation in [0, 1].

    Same audio content:   peak \u2248 0.8\u20131.0 (high \u2014 true piracy)
    Different content:    peak < 0.2      (low  \u2014 false positive)
    """
    candidate = Path(candidate_path)
    target = Path(target_path)
    if not candidate.exists() or not target.exists():
        return 0.0

    # Full candidate audio
    cand_raw = _run_ffmpeg(
        ["ffmpeg", "-v", "error", "-i", str(candidate),
         "-ac", "1", "-ar", str(sample_rate), "-f", "s16le", "-"]
    )
    if not cand_raw or len(cand_raw) < 2:
        return 0.0

    cand = np.frombuffer(cand_raw, dtype=np.int16).astype(np.float32) / 32768.0
    cand_duration = float(len(cand)) / float(sample_rate)

    # Target slice: offset \u2212 2 s  \u2026  offset + candidate_duration + 2 s
    seek = max(0.0, target_offset_seconds - 2.0)
    extract_dur = cand_duration + 4.0
    tgt_raw = _run_ffmpeg_slice(str(target), seek, extract_dur, sample_rate)
    if not tgt_raw or len(tgt_raw) < 2:
        return 0.0

    tgt = np.frombuffer(tgt_raw, dtype=np.int16).astype(np.float32) / 32768.0

    # Energy envelopes (RMS per hop)
    hop = max(1, int(hop_ms * sample_rate / 1000.0))

    def _envelope(s: "np.ndarray") -> "np.ndarray":
        n_hops = len(s) // hop
        if n_hops < 1:
            return np.array([float(np.sqrt(np.mean(s ** 2)))])
        seg = s[: n_hops * hop].reshape(n_hops, hop)
        return np.sqrt(np.mean(seg ** 2, axis=1))

    cand_env = _envelope(cand)
    tgt_env = _envelope(tgt)

    n_c = len(cand_env)
    n_t = len(tgt_env)
    if n_c < 2 or n_t < n_c:
        return 0.0

    # Zero-mean normalisation
    c_norm = cand_env - cand_env.mean()
    t_norm = tgt_env - tgt_env.mean()

    c_rms = float(np.linalg.norm(c_norm))
    if c_rms < 1e-8:
        return 0.0

    # Sliding normalised cross-correlation using numpy correlate
    # mode='valid' produces (n_t - n_c + 1,) output
    xcorr = np.correlate(t_norm, c_norm, mode="valid")

    # Local target energy at each lag for proper normalisation
    t_sq = t_norm ** 2
    # Cumulative sum trick for fast windowed energy
    cs = np.concatenate(([0.0], np.cumsum(t_sq)))
    local_tgt_energy = np.sqrt(np.maximum(0.0, cs[n_c:] - cs[: n_t - n_c + 1]))

    denom = c_rms * local_tgt_energy
    # Avoid division by zero
    safe_denom = np.where(denom < 1e-8, 1e-8, denom)
    norm_xcorr = xcorr / safe_denom

    return float(np.max(np.abs(norm_xcorr)))
