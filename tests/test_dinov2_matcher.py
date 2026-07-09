from __future__ import annotations

import numpy as np

from matcher.dinov2_matcher import DinoV2Config
from matcher.dinov2_matcher import DinoV2EmbeddingIndex
from matcher.dinov2_matcher import compute_candidate_to_target_metrics
from matcher.dinov2_matcher import run_dinov2_fingerprint


def _normalize_rows(arr: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms = np.where(norms < 1e-8, 1.0, norms)
    return (arr / norms).astype(np.float32)


def _make_target_embeddings(seed: int = 7) -> np.ndarray:
    rng = np.random.default_rng(seed)
    raw = rng.normal(size=(40, 32)).astype(np.float32)
    return _normalize_rows(raw)


def test_compute_candidate_to_target_metrics_detects_consecutive_match():
    target = _make_target_embeddings()
    candidate = _normalize_rows(target[8:18] + 0.01)

    config = DinoV2Config(
        cosine_threshold=0.90,
        l2_score_threshold=0.68,
        margin_threshold=0.02,
        min_consecutive_frames=6,
        max_target_frame_step=2,
        min_run_avg_cosine=0.92,
    )

    metrics = compute_candidate_to_target_metrics(target, candidate, config=config)

    assert metrics["status"] == "ok"
    assert metrics["qualifies_consecutive"] is True
    assert metrics["best_run"] is not None
    assert metrics["best_run"].length >= 6
    assert metrics["score"] >= 0.8


def test_compute_candidate_to_target_metrics_rejects_unrelated_embeddings():
    target = _make_target_embeddings(seed=13)
    candidate = _make_target_embeddings(seed=27)[0:10]

    config = DinoV2Config(
        cosine_threshold=0.95,
        l2_score_threshold=0.72,
        margin_threshold=0.06,
        min_consecutive_frames=6,
        max_target_frame_step=2,
        min_run_avg_cosine=0.96,
    )

    metrics = compute_candidate_to_target_metrics(target, candidate, config=config)

    assert metrics["status"] == "ok"
    assert metrics["qualifies_consecutive"] is False
    assert metrics["score"] < 0.8


class _FakeEmbedder:
    def __init__(self, candidate_index: DinoV2EmbeddingIndex):
        self._candidate_index = candidate_index

    def embed_video(self, _candidate_path: str, *, sample_fps: float) -> DinoV2EmbeddingIndex:
        assert sample_fps > 0
        return self._candidate_index


def test_run_dinov2_fingerprint_marks_match_on_strong_consecutive_run():
    target_embeddings = _make_target_embeddings(seed=101)
    candidate_embeddings = _normalize_rows(target_embeddings[12:24] + 0.005)

    target_index = DinoV2EmbeddingIndex(
        video_path="/tmp/target.mp4",
        model_name="facebook/dinov2-base",
        sample_fps=2.0,
        embeddings=target_embeddings,
        timestamps=np.arange(target_embeddings.shape[0], dtype=np.float64) / 2.0,
        frame_numbers=np.arange(target_embeddings.shape[0], dtype=np.int64),
    )
    candidate_index = DinoV2EmbeddingIndex(
        video_path="/tmp/candidate.mp4",
        model_name="facebook/dinov2-base",
        sample_fps=2.0,
        embeddings=candidate_embeddings,
        timestamps=np.arange(candidate_embeddings.shape[0], dtype=np.float64) / 2.0,
        frame_numbers=np.arange(candidate_embeddings.shape[0], dtype=np.int64),
    )

    config = DinoV2Config(
        cosine_threshold=0.90,
        l2_score_threshold=0.68,
        margin_threshold=0.02,
        min_consecutive_frames=8,
        max_target_frame_step=2,
        min_run_avg_cosine=0.93,
    )

    result = run_dinov2_fingerprint(
        target_title="Blast",
        candidate_url="https://example.com/clip.mp4",
        candidate_path="/tmp/candidate.mp4",
        target_index=target_index,
        embedder=_FakeEmbedder(candidate_index),
        config=config,
        low_threshold=0.3,
        high_threshold=0.85,
    )

    assert result.final_status == "matched"
    assert result.piracy_score >= 0.85
    assert [stage.stage_name for stage in result.outcomes] == [
        "stage0_sanitization",
        "stage1_dinov2_ann",
        "stage2_consecutive_consistency",
        "stage3_final_decision",
    ]
