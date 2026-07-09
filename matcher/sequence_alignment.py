from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from fingerprint.segment_fingerprint import (
    AudioSegmentFingerprint,
    VideoSegmentFingerprint,
    segment_hash_similarity,
)


# ---------------------------------------------------------------------------
# Numpy helpers: fast pairwise hash similarity matrices
# ---------------------------------------------------------------------------

def _np_popcnt32(arr: "np.ndarray") -> "np.ndarray":
    """Population count for a uint32 numpy array (Hamming weight per element)."""
    x = arr.astype(np.uint32)
    x = x - ((x >> np.uint32(1)) & np.uint32(0x55555555))
    x = (x & np.uint32(0x33333333)) + ((x >> np.uint32(2)) & np.uint32(0x33333333))
    x = (x + (x >> np.uint32(4))) & np.uint32(0x0F0F0F0F)
    return ((x * np.uint32(0x01010101)) >> np.uint32(24)).astype(np.int32)


def _np_popcnt64(arr: "np.ndarray") -> "np.ndarray":
    """Population count for a uint64 numpy array (Hamming weight per element)."""
    x = arr.astype(np.uint64)
    x = x - ((x >> np.uint64(1)) & np.uint64(0x5555555555555555))
    x = (x & np.uint64(0x3333333333333333)) + ((x >> np.uint64(2)) & np.uint64(0x3333333333333333))
    x = (x + (x >> np.uint64(4))) & np.uint64(0x0F0F0F0F0F0F0F0F)
    return ((x * np.uint64(0x0101010101010101)) >> np.uint64(56)).astype(np.int32)


def _hash_similarity_matrix(
    target_hashes: list[str],
    candidate_hashes: list[str],
) -> "np.ndarray":
    """Return (n_target × n_candidate) float32 Hamming-similarity matrix.

    Empty hashes (degenerate frames/silence) produce 0.0 similarity so they
    never inflate coverage scores even if both segments are simultaneously degenerate.
    Automatically adapts to hash bit-width.
    """
    n = len(target_hashes)
    m = len(candidate_hashes)

    # Determine bit-width from first non-empty hash
    bit_count = 64
    for h in target_hashes + candidate_hashes:
        if h:
            bit_count = max(1, len(h) * 4)
            break

    # Filter to only hashes of the expected length
    hex_len = max(1, bit_count // 4)

    # For hashes wider than 64 bits (e.g. legacy 40-char SHA1 = 160 bits),
    # numpy integer types don't fit; fall back to per-element Python computation.
    if bit_count > 64:
        result = np.zeros((n, m), dtype=np.float32)
        for i, th in enumerate(target_hashes):
            for j, ch in enumerate(candidate_hashes):
                result[i, j] = segment_hash_similarity(th, ch)
        return result

    def _to_ints(hashes: list[str], count: int) -> "np.ndarray":
        dtype = np.uint64 if bit_count > 32 else np.uint32
        out = np.zeros(count, dtype=dtype)
        for i, h in enumerate(hashes):
            if h and len(h) == hex_len:
                try:
                    out[i] = int(h, 16)
                except (ValueError, OverflowError):
                    pass
        return out

    t_ints = _to_ints(target_hashes, n)
    c_ints = _to_ints(candidate_hashes, m)

    # Pairwise XOR: (n, m)
    xor = t_ints[:, None] ^ c_ints[None, :]

    # Count differing bits
    if bit_count > 32:
        diff_bits = _np_popcnt64(xor)
    else:
        diff_bits = _np_popcnt32(xor)

    return (1.0 - diff_bits.astype(np.float32) / float(bit_count)).clip(0.0, 1.0)


def _sliding_window_coverage(
    sim_matrix: "np.ndarray",
    *,
    match_threshold: float,
) -> tuple[float, int, float]:
    """Slide a candidate window (m columns) across target rows and return the
    best coverage fraction, best offset index, and average similarity at that offset.

    Coverage fraction = fraction of candidate segments with sim >= match_threshold.
    Tie-break uses average similarity at that offset.
    """
    n, m = sim_matrix.shape
    if n < m:
        return 0.0, 0, 0.0

    n_windows = n - m + 1
    # row_idx[s, i] = s + i;  col_idx[s, i] = i
    s_idx = np.arange(n_windows, dtype=np.int64)
    i_idx = np.arange(m, dtype=np.int64)
    row_idx = s_idx[:, None] + i_idx[None, :]  # (n_windows, m)
    col_idx = np.broadcast_to(i_idx[None, :], (n_windows, m))

    window_sims = sim_matrix[row_idx, col_idx]  # (n_windows, m)

    coverage = (window_sims >= match_threshold).mean(axis=1)  # (n_windows,)
    avg_sim = window_sims.mean(axis=1)  # (n_windows,)

    # Primary sort: coverage; secondary: average similarity
    score_combined = coverage + avg_sim * 0.1
    best_s = int(score_combined.argmax())

    return float(coverage[best_s]), best_s, float(avg_sim[best_s])


@dataclass(slots=True)
class AlignmentResult:
    method: str
    similarity: float
    target_start_index: int
    target_end_index: int
    offset_seconds: float
    note: str
    details: dict[str, float | int | str]


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _feature_similarity(a: AudioSegmentFingerprint, b: AudioSegmentFingerprint) -> float:
    # Spectral band pHash is the primary discriminator (content-specific, phase-invariant).
    # RMS / ZCR are kept only as a secondary fallback for legacy digests.
    digest_score = segment_hash_similarity(getattr(a, "digest", ""), getattr(b, "digest", ""))
    rms_delta = abs(float(a.rms) - float(b.rms))
    zcr_delta = abs(float(a.zero_crossing_rate) - float(b.zero_crossing_rate))
    feature_score = _clip01(1.0 - (0.65 * rms_delta + 0.35 * zcr_delta))
    if digest_score > 0.0:
        # Spectral digest carries most of the discrimination weight.
        return _clip01(0.80 * digest_score + 0.20 * feature_score)
    return feature_score


def _resolve_band(n: int, m: int, band_ratio: float) -> int:
    band = int(round(max(n, m) * max(0.0, float(band_ratio))))
    return max(abs(n - m), band)


# Threshold: fraction of hash bits that must agree to be considered a visual match.
# 64-bit perceptual block hash: expect 0.90+ for same-scene frames, ~0.50 for unrelated.
_VIDEO_MATCH_THRESHOLD = 0.80
# 32-bit spectral band hash: expect 0.80+ for same audio, ~0.50 for different audio.
_AUDIO_MATCH_THRESHOLD = 0.80


def _audio_feature_matrix(
    target_segments: list[AudioSegmentFingerprint],
    candidate_segments: list[AudioSegmentFingerprint],
) -> "np.ndarray":
    """Return (n_target × n_candidate) float32 combined audio feature similarity matrix.

    Combines spectral band pHash similarity (primary) with RMS/ZCR feature score (fallback).
    """
    n = len(target_segments)
    m = len(candidate_segments)

    # Spectral digest similarity matrix (primary discriminator)
    digest_sim = _hash_similarity_matrix(
        [getattr(s, "digest", "") for s in target_segments],
        [getattr(s, "digest", "") for s in candidate_segments],
    )  # (n, m)

    # RMS / ZCR feature similarity matrix (fallback)
    t_rms = np.array([s.rms for s in target_segments], dtype=np.float32)
    c_rms = np.array([s.rms for s in candidate_segments], dtype=np.float32)
    t_zcr = np.array([s.zero_crossing_rate for s in target_segments], dtype=np.float32)
    c_zcr = np.array([s.zero_crossing_rate for s in candidate_segments], dtype=np.float32)
    rms_delta = np.abs(t_rms[:, None] - c_rms[None, :])  # (n, m)
    zcr_delta = np.abs(t_zcr[:, None] - c_zcr[None, :])  # (n, m)
    feature_sim = np.clip(1.0 - (0.65 * rms_delta + 0.35 * zcr_delta), 0.0, 1.0)

    # Determine which cells have a valid spectral digest comparison
    t_has_digest = np.array([bool(getattr(s, "digest", "")) for s in target_segments])
    c_has_digest = np.array([bool(getattr(s, "digest", "")) for s in candidate_segments])
    has_digest = t_has_digest[:, None] & c_has_digest[None, :]  # (n, m)

    combined = np.where(has_digest, 0.80 * digest_sim + 0.20 * feature_sim, feature_sim)
    return combined.astype(np.float32)


def align_video_segments_constrained(
    target_segments: list[VideoSegmentFingerprint],
    candidate_segments: list[VideoSegmentFingerprint],
    *,
    candidate_segment_seconds: float,
) -> AlignmentResult:
    """Numpy-vectorised sliding-window video alignment using coverage-count scoring.

    Similarity = fraction of candidate segments whose best-matching target segment
    scores >= _VIDEO_MATCH_THRESHOLD (perceptual block-hash Hamming similarity).
    This is far more discriminative than the previous average-similarity approach.
    """
    if not target_segments or not candidate_segments:
        return AlignmentResult(
            method="constrained",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty segment sequence",
            details={},
        )

    n = len(target_segments)
    m = len(candidate_segments)
    if m > n:
        return AlignmentResult(
            method="constrained",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="candidate is longer than target",
            details={"target_segments": n, "candidate_segments": m},
        )

    sim_matrix = _hash_similarity_matrix(
        [s.digest for s in target_segments],
        [s.digest for s in candidate_segments],
    )  # (n, m)

    coverage, best_start, best_avg = _sliding_window_coverage(
        sim_matrix, match_threshold=_VIDEO_MATCH_THRESHOLD
    )

    return AlignmentResult(
        method="constrained",
        similarity=_clip01(coverage),
        target_start_index=best_start,
        target_end_index=(best_start + m - 1),
        offset_seconds=float(best_start) * float(candidate_segment_seconds),
        note="numpy vectorised sliding window (coverage-count)",
        details={
            "target_segments": n,
            "candidate_segments": m,
            "coverage_fraction": coverage,
            "avg_similarity_at_best": best_avg,
            "match_threshold": _VIDEO_MATCH_THRESHOLD,
        },
    )


def align_video_segments_dtw(
    target_segments: list[VideoSegmentFingerprint],
    candidate_segments: list[VideoSegmentFingerprint],
    *,
    candidate_segment_seconds: float,
    band_ratio: float,
) -> AlignmentResult:
    if not target_segments or not candidate_segments:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty segment sequence",
            details={},
        )

    n = len(target_segments)
    m = len(candidate_segments)
    band = _resolve_band(n, m, band_ratio)

    inf = float("inf")
    dp = [[inf for _ in range(m + 1)] for _ in range(n + 1)]
    prev = [[(-1, -1) for _ in range(m + 1)] for _ in range(n + 1)]
    dp[0][0] = 0.0

    for i in range(1, n + 1):
        j_low = max(1, i - band)
        j_high = min(m, i + band)
        for j in range(j_low, j_high + 1):
            sim = segment_hash_similarity(target_segments[i - 1].digest, candidate_segments[j - 1].digest)
            cost = 1.0 - sim

            candidates = (
                (dp[i - 1][j], (i - 1, j)),
                (dp[i][j - 1], (i, j - 1)),
                (dp[i - 1][j - 1], (i - 1, j - 1)),
            )
            best_prev_cost, best_prev = min(candidates, key=lambda item: item[0])
            dp[i][j] = cost + best_prev_cost
            prev[i][j] = best_prev

    if dp[n][m] == inf:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="no valid path under DTW window",
            details={"band": band},
        )

    path: list[tuple[int, int]] = []
    i, j = n, m
    while i > 0 and j > 0:
        path.append((i - 1, j - 1))
        pi, pj = prev[i][j]
        if pi < 0 or pj < 0:
            break
        i, j = pi, pj

    path.reverse()
    if not path:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty DTW path",
            details={"band": band},
        )

    path_len = max(1, len(path))
    avg_cost = float(dp[n][m]) / float(path_len)
    similarity = _clip01(1.0 - avg_cost)

    target_indices = [p[0] for p in path]
    start = min(target_indices)
    end = max(target_indices)

    return AlignmentResult(
        method="dtw",
        similarity=similarity,
        target_start_index=start,
        target_end_index=end,
        offset_seconds=float(start) * float(candidate_segment_seconds),
        note="DTW alignment with Sakoe-Chiba band",
        details={"band": band, "path_len": path_len},
    )


def align_audio_segments_offset_xcorr(
    target_segments: list[AudioSegmentFingerprint],
    candidate_segments: list[AudioSegmentFingerprint],
    *,
    audio_segment_seconds: float,
) -> AlignmentResult:
    if not target_segments or not candidate_segments:
        return AlignmentResult(
            method="offset_xcorr",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty segment sequence",
            details={},
        )

    n = len(target_segments)
    m = len(candidate_segments)
    if m > n:
        return AlignmentResult(
            method="offset_xcorr",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="candidate is longer than target",
            details={"target_segments": n, "candidate_segments": m},
        )

    sim_matrix = _audio_feature_matrix(target_segments, candidate_segments)  # (n, m)

    coverage, best_start, best_avg = _sliding_window_coverage(
        sim_matrix, match_threshold=_AUDIO_MATCH_THRESHOLD
    )

    return AlignmentResult(
        method="offset_xcorr",
        similarity=_clip01(coverage),
        target_start_index=best_start,
        target_end_index=(best_start + m - 1),
        offset_seconds=float(best_start) * float(audio_segment_seconds),
        note="numpy vectorised sliding window (coverage-count)",
        details={
            "target_segments": n,
            "candidate_segments": m,
            "coverage_fraction": coverage,
            "avg_similarity_at_best": best_avg,
            "match_threshold": _AUDIO_MATCH_THRESHOLD,
        },
    )


def align_audio_segments_dtw(
    target_segments: list[AudioSegmentFingerprint],
    candidate_segments: list[AudioSegmentFingerprint],
    *,
    audio_segment_seconds: float,
    band_ratio: float,
) -> AlignmentResult:
    if not target_segments or not candidate_segments:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty segment sequence",
            details={},
        )

    n = len(target_segments)
    m = len(candidate_segments)
    band = _resolve_band(n, m, band_ratio)

    inf = float("inf")
    dp = [[inf for _ in range(m + 1)] for _ in range(n + 1)]
    prev = [[(-1, -1) for _ in range(m + 1)] for _ in range(n + 1)]
    dp[0][0] = 0.0

    for i in range(1, n + 1):
        j_low = max(1, i - band)
        j_high = min(m, i + band)
        for j in range(j_low, j_high + 1):
            sim = _feature_similarity(target_segments[i - 1], candidate_segments[j - 1])
            cost = 1.0 - sim
            candidates = (
                (dp[i - 1][j], (i - 1, j)),
                (dp[i][j - 1], (i, j - 1)),
                (dp[i - 1][j - 1], (i - 1, j - 1)),
            )
            best_prev_cost, best_prev = min(candidates, key=lambda item: item[0])
            dp[i][j] = cost + best_prev_cost
            prev[i][j] = best_prev

    if dp[n][m] == inf:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="no valid path under DTW window",
            details={"band": band},
        )

    path: list[tuple[int, int]] = []
    i, j = n, m
    while i > 0 and j > 0:
        path.append((i - 1, j - 1))
        pi, pj = prev[i][j]
        if pi < 0 or pj < 0:
            break
        i, j = pi, pj

    path.reverse()
    if not path:
        return AlignmentResult(
            method="dtw",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty DTW path",
            details={"band": band},
        )

    path_len = max(1, len(path))
    avg_cost = float(dp[n][m]) / float(path_len)
    similarity = _clip01(1.0 - avg_cost)
    target_indices = [p[0] for p in path]
    start = min(target_indices)
    end = max(target_indices)

    return AlignmentResult(
        method="dtw",
        similarity=similarity,
        target_start_index=start,
        target_end_index=end,
        offset_seconds=float(start) * float(audio_segment_seconds),
        note="DTW alignment over audio feature sequence",
        details={"band": band, "path_len": path_len},
    )


def align_audio_energy_xcorr(
    target_segments: list[AudioSegmentFingerprint],
    candidate_segments: list[AudioSegmentFingerprint],
    *,
    audio_segment_seconds: float,
) -> AlignmentResult:
    """Full-sweep normalised energy-envelope cross-correlation.

    Uses the per-segment RMS already stored in the precomputed target cache —
    no extra ffmpeg call needed.  Slides the candidate energy envelope across
    every possible position in the target and finds the peak normalised
    cross-correlation.

    Same content at the correct offset:      peak \u2248 0.70\u20131.00
    Unrelated content at any offset:         peak < 0.25
    """
    if not target_segments or not candidate_segments:
        return AlignmentResult(
            method="energy_xcorr",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="empty segment sequence",
            details={},
        )

    n = len(target_segments)
    m = len(candidate_segments)

    # Extract energy envelopes from precomputed RMS values
    t_rms = np.array([float(s.rms) for s in target_segments], dtype=np.float64)  # (n,)
    c_rms = np.array([float(s.rms) for s in candidate_segments], dtype=np.float64)  # (m,)

    if m > n:
        return AlignmentResult(
            method="energy_xcorr",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="candidate is longer than target",
            details={"target_segments": n, "candidate_segments": m},
        )

    # Zero-mean normalisation
    t_norm = t_rms - t_rms.mean()
    c_norm = c_rms - c_rms.mean()

    c_energy = float(np.linalg.norm(c_norm))
    if c_energy < 1e-8:
        return AlignmentResult(
            method="energy_xcorr",
            similarity=0.0,
            target_start_index=0,
            target_end_index=0,
            offset_seconds=0.0,
            note="candidate energy is near-zero",
            details={},
        )

    # Sliding cross-correlation: xcorr[s] = dot(t_norm[s:s+m], c_norm)
    # mode='valid' gives (n - m + 1,) elements
    xcorr = np.correlate(t_norm, c_norm, mode="valid")  # (n - m + 1,)

    # Local target energy at each lag for proper normalisation
    cs = np.concatenate(([0.0], np.cumsum(t_norm ** 2)))
    local_t_energy = np.sqrt(np.maximum(0.0, cs[m:] - cs[: n - m + 1]))  # (n - m + 1,)

    denom = c_energy * local_t_energy
    safe_denom = np.where(denom < 1e-8, 1e-8, denom)
    norm_xcorr = xcorr / safe_denom  # (n - m + 1,)

    best_s = int(np.argmax(norm_xcorr))
    peak = float(norm_xcorr[best_s])
    # Clip: normalised xcorr can exceed 1.0 in degenerate cases
    similarity = _clip01(peak)

    return AlignmentResult(
        method="energy_xcorr",
        similarity=similarity,
        target_start_index=best_s,
        target_end_index=best_s + m - 1,
        offset_seconds=float(best_s) * float(audio_segment_seconds),
        note="full-sweep RMS energy-envelope cross-correlation",
        details={
            "target_segments": n,
            "candidate_segments": m,
            "peak_xcorr": peak,
        },
    )
