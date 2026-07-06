from __future__ import annotations

from dataclasses import dataclass

from fingerprint.segment_fingerprint import (
    AudioSegmentFingerprint,
    VideoSegmentFingerprint,
    segment_hash_similarity,
)


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
    rms_delta = abs(float(a.rms) - float(b.rms))
    zcr_delta = abs(float(a.zero_crossing_rate) - float(b.zero_crossing_rate))
    return _clip01(1.0 - (0.65 * rms_delta + 0.35 * zcr_delta))


def _resolve_band(n: int, m: int, band_ratio: float) -> int:
    band = int(round(max(n, m) * max(0.0, float(band_ratio))))
    return max(abs(n - m), band)


def align_video_segments_constrained(
    target_segments: list[VideoSegmentFingerprint],
    candidate_segments: list[VideoSegmentFingerprint],
    *,
    candidate_segment_seconds: float,
) -> AlignmentResult:
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

    best_score = -1.0
    best_start = 0
    for start in range(0, n - m + 1):
        total = 0.0
        for i in range(m):
            total += segment_hash_similarity(
                target_segments[start + i].digest,
                candidate_segments[i].digest,
            )
        avg = total / float(m)
        if avg > best_score:
            best_score = avg
            best_start = start

    return AlignmentResult(
        method="constrained",
        similarity=_clip01(best_score),
        target_start_index=best_start,
        target_end_index=(best_start + m - 1),
        offset_seconds=float(best_start) * float(candidate_segment_seconds),
        note="sliding constrained alignment",
        details={"target_segments": n, "candidate_segments": m},
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

    best_score = -1.0
    best_start = 0
    for start in range(0, n - m + 1):
        total = 0.0
        for i in range(m):
            total += _feature_similarity(target_segments[start + i], candidate_segments[i])
        avg = total / float(m)
        if avg > best_score:
            best_score = avg
            best_start = start

    return AlignmentResult(
        method="offset_xcorr",
        similarity=_clip01(best_score),
        target_start_index=best_start,
        target_end_index=(best_start + m - 1),
        offset_seconds=float(best_start) * float(audio_segment_seconds),
        note="sliding offset correlation over audio segment features",
        details={"target_segments": n, "candidate_segments": m},
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
