from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from matcher.multi_stage_fingerprint import PipelineResult
from matcher.multi_stage_fingerprint import StageOutcome

DINO_MATCH_ALGO_VERSION = "dinov2_v1"


@dataclass(slots=True)
class DinoV2Config:
    model_name: str = "facebook/dinov2-base"
    target_sample_fps: float = 2.0
    candidate_sample_fps: float = 2.0
    cosine_threshold: float = 0.93
    l2_score_threshold: float = 0.70
    margin_threshold: float = 0.03
    min_consecutive_frames: int = 6
    max_target_frame_step: int = 3
    min_run_avg_cosine: float = 0.94


@dataclass(slots=True)
class DinoV2EmbeddingIndex:
    video_path: str
    model_name: str
    sample_fps: float
    embeddings: np.ndarray
    timestamps: np.ndarray
    frame_numbers: np.ndarray


@dataclass(slots=True)
class DinoRunSummary:
    start_candidate_index: int
    end_candidate_index: int
    start_target_index: int
    end_target_index: int
    length: int
    avg_cosine: float
    avg_l2_score: float
    avg_margin: float
    temporal_consistency: float


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _l2_score_from_cosine(cosine_similarity: np.ndarray) -> np.ndarray:
    distance = np.sqrt(np.maximum(0.0, 2.0 - (2.0 * cosine_similarity)))
    return 1.0 / (1.0 + distance)


def _ensure_normalized(matrix: np.ndarray) -> np.ndarray:
    arr = np.asarray(matrix, dtype=np.float32)
    if arr.ndim != 2 or arr.shape[0] == 0:
        return np.zeros((0, 0), dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms = np.where(norms < 1e-8, 1.0, norms)
    return (arr / norms).astype(np.float32)


def _build_runs(
    best_target_indices: np.ndarray,
    best_cosines: np.ndarray,
    best_l2_scores: np.ndarray,
    margins: np.ndarray,
    accepted: np.ndarray,
    *,
    max_target_frame_step: int,
) -> list[DinoRunSummary]:
    runs: list[DinoRunSummary] = []
    current: list[int] = []

    def flush() -> None:
        if not current:
            return
        cand_start = current[0]
        cand_end = current[-1]
        target_slice = best_target_indices[current]
        cos_slice = best_cosines[current]
        l2_slice = best_l2_scores[current]
        margin_slice = margins[current]

        if len(target_slice) <= 1:
            temporal_consistency = 1.0
        else:
            deltas = np.diff(target_slice)
            valid = ((deltas >= 0) & (deltas <= max_target_frame_step)).astype(np.float32)
            temporal_consistency = float(valid.mean()) if len(valid) else 1.0

        runs.append(
            DinoRunSummary(
                start_candidate_index=int(cand_start),
                end_candidate_index=int(cand_end),
                start_target_index=int(target_slice[0]),
                end_target_index=int(target_slice[-1]),
                length=len(current),
                avg_cosine=float(cos_slice.mean()),
                avg_l2_score=float(l2_slice.mean()),
                avg_margin=float(margin_slice.mean()),
                temporal_consistency=float(temporal_consistency),
            )
        )

    for i in range(len(accepted)):
        if not accepted[i]:
            flush()
            current = []
            continue

        if not current:
            current = [i]
            continue

        prev_i = current[-1]
        step = int(best_target_indices[i] - best_target_indices[prev_i])
        if 0 <= step <= max_target_frame_step:
            current.append(i)
        else:
            flush()
            current = [i]

    flush()
    return runs


def _score_runs(
    *,
    runs: list[DinoRunSummary],
    total_candidate_frames: int,
    best_cosines: np.ndarray,
    accepted: np.ndarray,
) -> tuple[float, DinoRunSummary | None, dict[str, float]]:
    accepted_ratio = float(accepted.mean()) if len(accepted) else 0.0
    global_avg_cos = float(best_cosines.mean()) if len(best_cosines) else 0.0

    if not runs:
        score = _clip01((0.60 * accepted_ratio) + (0.40 * global_avg_cos))
        return score, None, {
            "accepted_ratio": accepted_ratio,
            "global_avg_cosine": global_avg_cos,
            "coverage": 0.0,
            "temporal_consistency": 0.0,
        }

    best = max(runs, key=lambda item: (item.length, item.avg_cosine, item.temporal_consistency))
    coverage = float(best.length) / float(max(1, total_candidate_frames))

    score = _clip01(
        (0.45 * best.avg_cosine)
        + (0.20 * best.avg_l2_score)
        + (0.10 * best.avg_margin)
        + (0.15 * coverage)
        + (0.10 * best.temporal_consistency)
    )
    return score, best, {
        "accepted_ratio": accepted_ratio,
        "global_avg_cosine": global_avg_cos,
        "coverage": coverage,
        "temporal_consistency": best.temporal_consistency,
    }


def compute_candidate_to_target_metrics(
    target_embeddings: np.ndarray,
    candidate_embeddings: np.ndarray,
    *,
    config: DinoV2Config,
) -> dict[str, Any]:
    target = _ensure_normalized(target_embeddings)
    candidate = _ensure_normalized(candidate_embeddings)

    if target.shape[0] == 0 or candidate.shape[0] == 0:
        return {
            "status": "failed",
            "reason": "empty_embeddings",
            "score": 0.0,
            "best_run": None,
            "accepted_ratio": 0.0,
            "global_avg_cosine": 0.0,
            "coverage": 0.0,
            "temporal_consistency": 0.0,
        }

    sim = np.matmul(candidate, target.T)  # (candidate_frames, target_frames)
    best_idx = np.argmax(sim, axis=1)
    best_cos = sim[np.arange(sim.shape[0]), best_idx]

    if sim.shape[1] >= 2:
        top2 = np.partition(sim, -2, axis=1)[:, -2:]
        second_best = np.min(top2, axis=1)
    else:
        second_best = np.zeros_like(best_cos)

    margin = best_cos - second_best
    l2_score = _l2_score_from_cosine(best_cos)

    accepted = (
        (best_cos >= float(config.cosine_threshold))
        & (l2_score >= float(config.l2_score_threshold))
        & (margin >= float(config.margin_threshold))
    )

    runs = _build_runs(
        best_idx,
        best_cos,
        l2_score,
        margin,
        accepted,
        max_target_frame_step=max(1, int(config.max_target_frame_step)),
    )
    score, best_run, global_metrics = _score_runs(
        runs=runs,
        total_candidate_frames=candidate.shape[0],
        best_cosines=best_cos,
        accepted=accepted,
    )

    qualifies_consecutive = (
        best_run is not None
        and best_run.length >= int(config.min_consecutive_frames)
        and best_run.avg_cosine >= float(config.min_run_avg_cosine)
    )

    return {
        "status": "ok",
        "score": float(score),
        "best_run": best_run,
        "best_target_indices": best_idx,
        "best_cosine": best_cos,
        "best_l2_score": l2_score,
        "margin": margin,
        "accepted": accepted,
        "qualifies_consecutive": bool(qualifies_consecutive),
        **global_metrics,
    }


class DinoV2Embedder:
    def __init__(self, *, model_name: str, device: str = "auto"):
        self.model_name = model_name
        self.device_label = device
        self._processor = None
        self._model = None
        self._torch = None

    def _ensure_model(self) -> None:
        if self._processor is not None and self._model is not None and self._torch is not None:
            return

        try:
            import torch
            from transformers import AutoImageProcessor, AutoModel
        except Exception as exc:  # pragma: no cover - integration path
            raise RuntimeError(
                "DINOv2 dependencies missing. Install torch, transformers, pillow, and opencv-python."
            ) from exc

        if self.device_label == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            device = self.device_label

        processor = AutoImageProcessor.from_pretrained(self.model_name)
        model = AutoModel.from_pretrained(self.model_name).to(device)
        model.eval()

        self._torch = torch
        self._processor = processor
        self._model = model
        self.device_label = device

    def _embed_rgb_uint8(self, frame_rgb: np.ndarray) -> np.ndarray:
        self._ensure_model()

        from PIL import Image

        img = Image.fromarray(frame_rgb)
        inputs = self._processor(images=img, return_tensors="pt").to(self.device_label)
        with self._torch.no_grad():
            outputs = self._model(**inputs)

        vec = outputs.last_hidden_state[:, 0].detach().cpu().numpy()[0].astype(np.float32)
        norm = float(np.linalg.norm(vec))
        if norm < 1e-8:
            return vec
        return vec / norm

    def embed_video(self, video_path: str, *, sample_fps: float) -> DinoV2EmbeddingIndex:
        try:
            import cv2
        except Exception as exc:  # pragma: no cover - integration path
            raise RuntimeError("opencv-python is required for DINOv2 video embedding extraction") from exc

        sample_fps = max(0.1, float(sample_fps))
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            raise RuntimeError(f"Cannot open video: {video_path}")

        try:
            video_fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)
            if video_fps <= 0.0:
                raise RuntimeError(f"Cannot determine FPS for video: {video_path}")

            frame_interval = max(1, int(round(video_fps / sample_fps)))

            frame_number = 0
            embeddings: list[np.ndarray] = []
            timestamps: list[float] = []
            frame_numbers: list[int] = []

            while True:
                ok, frame = cap.read()
                if not ok:
                    break

                if frame_number % frame_interval == 0:
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    emb = self._embed_rgb_uint8(rgb)
                    embeddings.append(emb)
                    timestamps.append(float(frame_number / video_fps))
                    frame_numbers.append(int(frame_number))

                frame_number += 1
        finally:
            cap.release()

        emb_matrix = np.asarray(embeddings, dtype=np.float32)
        return DinoV2EmbeddingIndex(
            video_path=str(Path(video_path).expanduser().resolve()),
            model_name=self.model_name,
            sample_fps=float(sample_fps),
            embeddings=emb_matrix,
            timestamps=np.asarray(timestamps, dtype=np.float64),
            frame_numbers=np.asarray(frame_numbers, dtype=np.int64),
        )


def target_embedding_cache_key(
    *,
    target_path: Path,
    model_name: str,
    sample_fps: float,
) -> str:
    stat = target_path.stat()
    payload = "|".join(
        [
            str(target_path.expanduser().resolve()),
            str(stat.st_size),
            str(int(stat.st_mtime_ns)),
            model_name,
            f"fps={float(sample_fps):.5f}",
            f"algo={DINO_MATCH_ALGO_VERSION}",
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def save_embedding_index(index: DinoV2EmbeddingIndex, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        str(file_path),
        embeddings=index.embeddings.astype(np.float32),
        timestamps=index.timestamps.astype(np.float64),
        frame_numbers=index.frame_numbers.astype(np.int64),
        video_path=np.asarray(index.video_path),
        model_name=np.asarray(index.model_name),
        sample_fps=np.asarray(float(index.sample_fps)),
    )


def load_embedding_index(file_path: Path) -> DinoV2EmbeddingIndex:
    payload = np.load(str(file_path), allow_pickle=False)
    return DinoV2EmbeddingIndex(
        video_path=str(payload["video_path"].item()),
        model_name=str(payload["model_name"].item()),
        sample_fps=float(payload["sample_fps"].item()),
        embeddings=np.asarray(payload["embeddings"], dtype=np.float32),
        timestamps=np.asarray(payload["timestamps"], dtype=np.float64),
        frame_numbers=np.asarray(payload["frame_numbers"], dtype=np.int64),
    )


def run_dinov2_fingerprint(
    *,
    target_title: str,
    candidate_url: str,
    candidate_path: str,
    target_index: DinoV2EmbeddingIndex,
    embedder: DinoV2Embedder,
    config: DinoV2Config,
    low_threshold: float,
    high_threshold: float,
) -> PipelineResult:
    candidate_index = embedder.embed_video(candidate_path, sample_fps=config.candidate_sample_fps)
    metrics = compute_candidate_to_target_metrics(
        target_index.embeddings,
        candidate_index.embeddings,
        config=config,
    )

    outcomes: list[StageOutcome] = [
        StageOutcome(
            stage_name="stage0_sanitization",
            score=1.0,
            decision="pass",
            note="candidate readability and basic file sanity",
            details={
                "candidate_path": candidate_path,
                "target_path": target_index.video_path,
                "target_title": target_title,
                "candidate_url": candidate_url,
            },
        )
    ]

    if metrics["status"] != "ok":
        outcomes.append(
            StageOutcome(
                stage_name="stage1_dinov2_ann",
                score=0.0,
                decision="reject",
                note="could not score candidate against target embeddings",
                details={"reason": metrics.get("reason", "unknown")},
            )
        )
        return PipelineResult(final_status="failed", piracy_score=0.0, outcomes=outcomes)

    best_run = metrics["best_run"]
    stage1_score = float(metrics["global_avg_cosine"])
    outcomes.append(
        StageOutcome(
            stage_name="stage1_dinov2_ann",
            score=stage1_score,
            decision="pass" if stage1_score >= config.cosine_threshold else "weak",
            note="nearest-neighbor frame retrieval using DINOv2 embeddings",
            details={
                "model_name": config.model_name,
                "target_sample_fps": config.target_sample_fps,
                "candidate_sample_fps": config.candidate_sample_fps,
                "global_avg_cosine": metrics["global_avg_cosine"],
                "accepted_ratio": metrics["accepted_ratio"],
            },
        )
    )

    temporal_score = 0.0
    temporal_decision = "weak"
    temporal_details: dict[str, Any] = {
        "coverage": metrics["coverage"],
        "temporal_consistency": metrics["temporal_consistency"],
        "qualifies_consecutive": bool(metrics["qualifies_consecutive"]),
    }

    if best_run is not None:
        temporal_score = _clip01(
            (0.55 * best_run.avg_cosine)
            + (0.20 * best_run.avg_l2_score)
            + (0.15 * best_run.temporal_consistency)
            + (0.10 * (best_run.length / max(1, candidate_index.embeddings.shape[0])))
        )
        temporal_decision = "pass" if metrics["qualifies_consecutive"] else "weak"
        temporal_details.update(
            {
                "run_start_candidate_index": best_run.start_candidate_index,
                "run_end_candidate_index": best_run.end_candidate_index,
                "run_start_target_index": best_run.start_target_index,
                "run_end_target_index": best_run.end_target_index,
                "run_length": best_run.length,
                "run_avg_cosine": best_run.avg_cosine,
                "run_avg_l2_score": best_run.avg_l2_score,
                "run_avg_margin": best_run.avg_margin,
                "run_temporal_consistency": best_run.temporal_consistency,
                "min_required_consecutive": int(config.min_consecutive_frames),
                "min_required_run_avg_cosine": float(config.min_run_avg_cosine),
            }
        )

    outcomes.append(
        StageOutcome(
            stage_name="stage2_consecutive_consistency",
            score=temporal_score,
            decision=temporal_decision,
            note="candidate frames must map to a coherent consecutive region in the target",
            details=temporal_details,
        )
    )

    final_score = _clip01(
        (0.50 * float(metrics["score"]))
        + (0.30 * temporal_score)
        + (0.20 * float(metrics["accepted_ratio"]))
    )

    if metrics["qualifies_consecutive"] and final_score >= max(high_threshold, 0.80):
        final_status = "matched"
    elif final_score <= low_threshold:
        final_status = "no_match_pending_review"
    else:
        final_status = "uncertain_manual_review"

    outcomes.append(
        StageOutcome(
            stage_name="stage3_final_decision",
            score=final_score,
            decision=final_status,
            note="combined score from ANN retrieval quality and consecutive-run validation",
            details={
                "ann_score": float(metrics["score"]),
                "temporal_score": float(temporal_score),
                "accepted_ratio": float(metrics["accepted_ratio"]),
                "low_threshold": float(low_threshold),
                "high_threshold": float(high_threshold),
            },
        )
    )

    return PipelineResult(
        final_status=final_status,
        piracy_score=float(final_score),
        outcomes=outcomes,
    )
