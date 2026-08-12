"""Phase 10 — matching/aggregation.py: technique-agnostic folding of one
or more technique-specific match results into a work_queue.results.Result.

Synthetic TemporalMatchResult/TechniqueEvidence throughout — no DINOv2, no
Redis, matching this module's own "pure function" scope.
"""
import json

import pytest

from matching.aggregation import TechniqueEvidence, combine, temporal_match_to_evidence
from matching.result import MatchedSegmentPair, TemporalMatchResult
from work_queue.results import ResultDecision


def _temporal_result(matched=True, score=0.95) -> TemporalMatchResult:
    return TemporalMatchResult(
        matched=matched,
        score=score,
        target_id="target-1",
        target_version="v1",
        candidate_id="evidence-1",
        matcher_version="temporal_v1",
        matched_duration_s=15.0,
        target_start=5.0,
        target_end=20.0,
        candidate_start=0.0,
        candidate_end=15.0,
        matched_segment_count=3,
        total_target_segments=10,
        total_candidate_segments=5,
        temporal_offset_s=5.0,
        mean_similarity=score,
        coarse_similarity=0.8,
        matched_pairs=(MatchedSegmentPair(target_segment_index=1, candidate_segment_index=0, similarity=score),),
    )


def test_temporal_match_converts_to_evidence():
    evidence = temporal_match_to_evidence(_temporal_result())

    assert evidence.technique == "dinov2"
    assert evidence.matcher_version == "temporal_v1"
    assert evidence.matched is True
    assert evidence.score == pytest.approx(0.95)
    assert evidence.detail["matched_segment_count"] == 3
    assert evidence.detail["temporal_offset_s"] == 5.0
    assert evidence.detail["target_start"] == 5.0


def test_combine_single_matched_technique_yields_match_decision():
    evidence = temporal_match_to_evidence(_temporal_result(matched=True, score=0.95))
    result = combine([evidence], processing_started_at=100.0, processing_completed_at=101.0)

    assert result.decision == ResultDecision.MATCH
    assert result.algorithm == "dinov2"
    assert result.confidence == pytest.approx(0.95)
    assert "dinov2=match" in result.summary
    assert result.processing_started_at == 100.0
    assert result.processing_completed_at == 101.0


def test_combine_single_no_match_technique_yields_no_match_decision():
    evidence = temporal_match_to_evidence(_temporal_result(matched=False, score=0.1))
    result = combine([evidence], processing_started_at=100.0, processing_completed_at=101.0)

    assert result.decision == ResultDecision.NO_MATCH
    assert "dinov2=no_match" in result.summary


def test_combine_matches_if_any_technique_matched():
    matched_evidence = TechniqueEvidence(
        technique="dinov2", matcher_version="temporal_v1", matched=True, score=0.9, detail={}
    )
    unmatched_evidence = TechniqueEvidence(
        technique="phash", matcher_version="v0", matched=False, score=0.2, detail={}
    )

    result = combine([unmatched_evidence, matched_evidence], processing_started_at=0.0, processing_completed_at=1.0)

    assert result.decision == ResultDecision.MATCH
    assert result.algorithm == "phash+dinov2"
    assert result.confidence == pytest.approx(0.9)  # highest score across techniques


def test_combine_evidence_json_round_trips_detail():
    evidence = temporal_match_to_evidence(_temporal_result())
    result = combine([evidence], processing_started_at=0.0, processing_completed_at=1.0)

    parsed = json.loads(result.evidence)
    assert len(parsed) == 1
    assert parsed[0]["technique"] == "dinov2"
    assert parsed[0]["matcher_version"] == "temporal_v1"
    assert parsed[0]["matched"] is True
    assert parsed[0]["detail"]["matched_segment_count"] == 3


def test_combine_empty_evidence_raises():
    with pytest.raises(ValueError):
        combine([], processing_started_at=0.0, processing_completed_at=1.0)


def test_combine_with_no_scores_leaves_confidence_none():
    evidence = TechniqueEvidence(technique="ocr", matcher_version="v1", matched=False, score=None, detail={})
    result = combine([evidence], processing_started_at=0.0, processing_completed_at=1.0)

    assert result.confidence is None
