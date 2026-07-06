from __future__ import annotations

import hashlib
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fingerprint.segment_fingerprint import extract_audio_segment_fingerprints
from fingerprint.segment_fingerprint import extract_video_segment_fingerprints
from fingerprint.video_probe import get_media_probe
from matcher.sequence_alignment import align_audio_segments_dtw
from matcher.sequence_alignment import align_audio_segments_offset_xcorr
from matcher.sequence_alignment import align_video_segments_constrained
from matcher.sequence_alignment import align_video_segments_dtw

TOKEN_RE = re.compile(r"[a-z0-9]+")
ALL_TECHNIQUES = ("metadata", "visual", "audio", "temporal")
ALL_ALIGNMENT_METHODS = ("constrained", "dtw")
ALL_AUDIO_ALIGNMENT_METHODS = ("offset_xcorr", "dtw")


@dataclass(slots=True)
class StageOutcome:
    stage_name: str
    score: float
    decision: str
    note: str
    details: dict[str, Any]


@dataclass(slots=True)
class PipelineResult:
    final_status: str
    piracy_score: float
    outcomes: list[StageOutcome]


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _tokenize(value: str) -> set[str]:
    return {token for token in TOKEN_RE.findall((value or "").lower()) if len(token) >= 3}


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    inter = len(left & right)
    union = len(left | right)
    return float(inter) / float(union) if union else 0.0


def _sha1_hex(path: Path, limit_bytes: int | None = None) -> str | None:
    if not path.exists():
        return None

    digest = hashlib.sha1()
    remaining = limit_bytes if limit_bytes is not None else -1
    with path.open("rb") as handle:
        while True:
            if remaining == 0:
                break
            chunk = handle.read(65536 if remaining < 0 else min(65536, remaining))
            if not chunk:
                break
            digest.update(chunk)
            if remaining > 0:
                remaining -= len(chunk)
    return digest.hexdigest()


def _file_fingerprint(path: Path) -> str:
    # Approximate pHash-friendly signature: combine sparse hashes from head/mid/tail.
    if not path.exists() or path.stat().st_size == 0:
        return ""

    size = path.stat().st_size
    window = min(262144, size)

    head = _sha1_hex(path, limit_bytes=window) or ""

    mid_hash = hashlib.sha1()
    with path.open("rb") as handle:
        handle.seek(max(0, (size // 2) - (window // 2)))
        mid_hash.update(handle.read(window))
    mid = mid_hash.hexdigest()

    tail_hash = hashlib.sha1()
    with path.open("rb") as handle:
        handle.seek(max(0, size - window))
        tail_hash.update(handle.read(window))
    tail = tail_hash.hexdigest()

    joined = f"{head}:{mid}:{tail}"
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _audio_digest(path: Path) -> str | None:
    # Lightweight audio fingerprint proxy using ffmpeg PCM extraction when available.
    ffmpeg_candidates = ["ffmpeg"]
    try:
        from imageio_ffmpeg import get_ffmpeg_exe

        ffmpeg_candidates.insert(0, str(get_ffmpeg_exe()))
    except Exception:
        pass

    for ffmpeg_bin in ffmpeg_candidates:
        command = [
            ffmpeg_bin,
            "-v",
            "error",
            "-i",
            str(path),
            "-ac",
            "1",
            "-ar",
            "8000",
            "-f",
            "s16le",
            "-",
        ]
        try:
            result = subprocess.run(command, capture_output=True, check=False)
            if result.returncode == 0 and result.stdout:
                return hashlib.sha1(result.stdout).hexdigest()
        except OSError:
            continue
    return None


def _duration_similarity(a: float | None, b: float | None) -> float:
    if a is None or b is None or a <= 0 or b <= 0:
        return 0.0
    ratio = min(a, b) / max(a, b)
    return _clip01(ratio)


def _normalize_enabled_techniques(enabled_techniques: set[str] | None) -> set[str]:
    if not enabled_techniques:
        return set(ALL_TECHNIQUES)
    normalized = {item.strip().lower() for item in enabled_techniques if item and item.strip()}
    unknown = sorted(normalized - set(ALL_TECHNIQUES))
    if unknown:
        raise ValueError(f"Unknown techniques: {', '.join(unknown)}")
    if not normalized:
        raise ValueError("At least one technique must be enabled")
    return normalized


def _normalize_alignment_method(method: str) -> str:
    normalized = (method or "constrained").strip().lower()
    if normalized not in ALL_ALIGNMENT_METHODS:
        raise ValueError(f"Unknown alignment method: {method}")
    return normalized


def _normalize_audio_alignment_method(method: str) -> str:
    normalized = (method or "offset_xcorr").strip().lower()
    if normalized not in ALL_AUDIO_ALIGNMENT_METHODS:
        raise ValueError(f"Unknown audio alignment method: {method}")
    return normalized


def run_staged_fingerprint(
    *,
    target_title: str,
    candidate_url: str,
    candidate_path: str,
    target_path: str,
    low_threshold: float,
    high_threshold: float,
    enabled_techniques: set[str] | None = None,
    target_segment_seconds: float = 1.0,
    candidate_segment_seconds: float = 2.0,
    frame_sample_fps: float = 2.0,
    sequence_alignment_method: str = "constrained",
    sequence_band_ratio: float = 0.15,
    audio_segment_seconds: float = 1.5,
    audio_alignment_method: str = "offset_xcorr",
    audio_band_ratio: float = 0.2,
) -> PipelineResult:
    candidate = Path(candidate_path)
    target = Path(target_path)
    enabled = _normalize_enabled_techniques(enabled_techniques)
    sequence_alignment_method = _normalize_alignment_method(sequence_alignment_method)
    audio_alignment_method = _normalize_audio_alignment_method(audio_alignment_method)

    outcomes: list[StageOutcome] = []

    candidate_probe = get_media_probe(str(candidate))
    target_probe = get_media_probe(str(target))

    stage0_score = 1.0 if candidate.exists() and candidate.stat().st_size > 0 else 0.0
    outcomes.append(
        StageOutcome(
            stage_name="stage0_sanitization",
            score=stage0_score,
            decision="pass" if stage0_score >= 0.5 else "reject",
            note="candidate readability and basic file sanity",
            details={
                "candidate_size": candidate.stat().st_size if candidate.exists() else None,
                "candidate_exists": candidate.exists(),
            },
        )
    )
    if stage0_score < 0.5:
        return PipelineResult(
            final_status="failed",
            piracy_score=0.0,
            outcomes=outcomes,
        )

    # Stage 1: metadata/container similarity
    duration_score = _duration_similarity(
        candidate_probe.get("duration_seconds"),
        target_probe.get("duration_seconds"),
    )
    resolution_score = 0.0
    if candidate_probe.get("width") and target_probe.get("width") and candidate_probe.get("height") and target_probe.get("height"):
        cw = int(candidate_probe["width"])
        ch = int(candidate_probe["height"])
        tw = int(target_probe["width"])
        th = int(target_probe["height"])
        resolution_score = _clip01(min(cw, tw) / max(cw, tw)) * _clip01(min(ch, th) / max(ch, th))

    stage1_score = _clip01((duration_score * 0.7) + (resolution_score * 0.3))
    if "metadata" not in enabled:
        stage1_score = 0.0
    outcomes.append(
        StageOutcome(
            stage_name="stage1_metadata",
            score=stage1_score,
            decision=("skipped" if "metadata" not in enabled else ("pass" if stage1_score >= 0.15 else "weak")),
            note=("disabled by --techniques" if "metadata" not in enabled else "duration and resolution consistency"),
            details={
                "candidate_probe": candidate_probe,
                "target_probe": target_probe,
                "duration_score": duration_score,
                "resolution_score": resolution_score,
            },
        )
    )

    # Stage 2: URL/title lexical overlap + segment alignment against movie timeline.
    title_tokens = _tokenize(target_title)
    candidate_tokens = _tokenize(candidate_url + " " + candidate.name)
    lexical_score = _jaccard(title_tokens, candidate_tokens)

    target_video_segments = extract_video_segment_fingerprints(
        str(target),
        segment_seconds=max(0.1, float(target_segment_seconds)),
        frame_sample_fps=max(0.1, float(frame_sample_fps)),
    )
    candidate_video_segments = extract_video_segment_fingerprints(
        str(candidate),
        segment_seconds=max(0.1, float(candidate_segment_seconds)),
        frame_sample_fps=max(0.1, float(frame_sample_fps)),
    )

    if sequence_alignment_method == "dtw":
        sequence_alignment = align_video_segments_dtw(
            target_video_segments,
            candidate_video_segments,
            candidate_segment_seconds=max(0.1, float(candidate_segment_seconds)),
            band_ratio=max(0.0, float(sequence_band_ratio)),
        )
    else:
        sequence_alignment = align_video_segments_constrained(
            target_video_segments,
            candidate_video_segments,
            candidate_segment_seconds=max(0.1, float(candidate_segment_seconds)),
        )

    stage2_score = _clip01((lexical_score * 0.3) + (sequence_alignment.similarity * 0.7))
    if "visual" not in enabled:
        stage2_score = 0.0
    outcomes.append(
        StageOutcome(
            stage_name="stage2_visual_quick",
            score=stage2_score,
            decision=("skipped" if "visual" not in enabled else ("pass" if stage2_score >= 0.2 else "weak")),
            note=(
                "disabled by --techniques"
                if "visual" not in enabled
                else "token overlap plus segment sequence alignment"
            ),
            details={
                "lexical_score": lexical_score,
                "alignment_method": sequence_alignment.method,
                "alignment_similarity": sequence_alignment.similarity,
                "offset_seconds": sequence_alignment.offset_seconds,
                "target_start_index": sequence_alignment.target_start_index,
                "target_end_index": sequence_alignment.target_end_index,
                "target_segment_seconds": float(target_segment_seconds),
                "candidate_segment_seconds": float(candidate_segment_seconds),
                "target_segment_count": len(target_video_segments),
                "candidate_segment_count": len(candidate_video_segments),
                "matched_tokens": sorted(title_tokens & candidate_tokens),
            },
        )
    )

    # Stage 3: audio segment fingerprinting and offset-aware alignment.
    target_audio_segments = extract_audio_segment_fingerprints(
        str(target),
        segment_seconds=max(0.1, float(audio_segment_seconds)),
    )
    candidate_audio_segments = extract_audio_segment_fingerprints(
        str(candidate),
        segment_seconds=max(0.1, float(audio_segment_seconds)),
    )

    if audio_alignment_method == "dtw":
        audio_alignment = align_audio_segments_dtw(
            target_audio_segments,
            candidate_audio_segments,
            audio_segment_seconds=max(0.1, float(audio_segment_seconds)),
            band_ratio=max(0.0, float(audio_band_ratio)),
        )
    else:
        audio_alignment = align_audio_segments_offset_xcorr(
            target_audio_segments,
            candidate_audio_segments,
            audio_segment_seconds=max(0.1, float(audio_segment_seconds)),
        )

    audio_score = audio_alignment.similarity
    if "audio" not in enabled:
        audio_score = 0.0
    outcomes.append(
        StageOutcome(
            stage_name="stage3_audio_fingerprint",
            score=audio_score,
            decision=("skipped" if "audio" not in enabled else ("pass" if audio_score >= 0.5 else "weak")),
            note=(
                "disabled by --techniques"
                if "audio" not in enabled
                else "audio segment fingerprint alignment"
            ),
            details={
                "alignment_method": audio_alignment.method,
                "alignment_similarity": audio_alignment.similarity,
                "offset_seconds": audio_alignment.offset_seconds,
                "target_start_index": audio_alignment.target_start_index,
                "target_end_index": audio_alignment.target_end_index,
                "audio_segment_seconds": float(audio_segment_seconds),
                "target_segment_count": len(target_audio_segments),
                "candidate_segment_count": len(candidate_audio_segments),
            },
        )
    )

    # Stage 4: temporal consistency from duration + sequence/audio offset agreement.
    alignment_agreement = 1.0
    if sequence_alignment.offset_seconds or audio_alignment.offset_seconds:
        offset_delta = abs(sequence_alignment.offset_seconds - audio_alignment.offset_seconds)
        alignment_agreement = _clip01(1.0 - (offset_delta / max(1.0, float(candidate_segment_seconds) * 10.0)))
    temporal_score = _clip01(
        (duration_score * 0.25)
        + (sequence_alignment.similarity * 0.45)
        + (audio_alignment.similarity * 0.20)
        + (alignment_agreement * 0.10)
    )
    if "temporal" not in enabled:
        temporal_score = 0.0
    outcomes.append(
        StageOutcome(
            stage_name="stage4_temporal_alignment",
            score=temporal_score,
            decision=("skipped" if "temporal" not in enabled else ("pass" if temporal_score >= 0.3 else "weak")),
            note=(
                "disabled by --techniques"
                if "temporal" not in enabled
                else "duration and sequence/audio offset consistency"
            ),
            details={
                "duration_score": duration_score,
                "video_alignment_similarity": sequence_alignment.similarity,
                "audio_alignment_similarity": audio_alignment.similarity,
                "alignment_agreement": alignment_agreement,
            },
        )
    )

    weights = {
        "stage1_metadata": 0.20,
        "stage2_visual_quick": 0.30,
        "stage3_audio_fingerprint": 0.25,
        "stage4_temporal_alignment": 0.25,
    }
    technique_to_stage = {
        "metadata": "stage1_metadata",
        "visual": "stage2_visual_quick",
        "audio": "stage3_audio_fingerprint",
        "temporal": "stage4_temporal_alignment",
    }
    active_stages = {technique_to_stage[item] for item in enabled}
    active_weight = sum(float(weight) for stage, weight in weights.items() if stage in active_stages)
    weighted = 0.0
    for item in outcomes:
        weighted += float(weights.get(item.stage_name, 0.0)) * float(item.score)
    piracy_score = _clip01(weighted / active_weight) if active_weight > 0 else 0.0

    if piracy_score >= high_threshold:
        final_status = "matched"
    elif piracy_score <= low_threshold:
        final_status = "no_match_pending_review"
    else:
        final_status = "uncertain_manual_review"

    return PipelineResult(
        final_status=final_status,
        piracy_score=piracy_score,
        outcomes=outcomes,
    )
