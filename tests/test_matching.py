"""Phase 9 — temporal/video matcher tests.

Runs entirely on deterministic synthetic embeddings (one-hot vectors per
letter, A-Z) rather than real DINOv2 inference, per the phase brief:
"The primary Phase 9 correctness tests should operate on deterministic
embedding arrays, not expensive DINO inference." Because one-hot vectors
give exact cosine similarity (1.0 for the same letter, 0.0 for different
letters), every test's expected outcome is computable by hand — this is
what makes the temporal-consistency algorithm's behavior legible on
purpose, not because real embeddings behave this cleanly.

The letter-sequence notation mirrors the phase brief's own test-data
example:

    Target:    A B C D E F G H I J
    Candidate: X Y C D E F Z

    -> matcher should detect target C-F <-> candidate C-F
"""
from __future__ import annotations

import string
from typing import Tuple

import numpy as np
import pytest

from embedding.result import SegmentEmbedding
from matching.config import MatcherConfig
from matching.matcher import coarse_screen, match_segments

_ALPHABET = string.ascii_uppercase
_DIM = len(_ALPHABET)
_LETTER_VECTORS = {ch: tuple(float(x) for x in np.eye(_DIM)[i]) for i, ch in enumerate(_ALPHABET)}
_SEGMENT_DURATION = 5.0


def make_segments(sequence: str, segment_duration: float = _SEGMENT_DURATION) -> Tuple[SegmentEmbedding, ...]:
    return tuple(
        SegmentEmbedding(
            segment_index=i,
            start_time=i * segment_duration,
            end_time=(i + 1) * segment_duration,
            vector=_LETTER_VECTORS[ch],
        )
        for i, ch in enumerate(sequence)
    )


def coarse_vector_for(sequence: str) -> Tuple[float, ...]:
    vectors = np.array([_LETTER_VECTORS[ch] for ch in sequence])
    mean = vectors.mean(axis=0)
    norm = np.linalg.norm(mean)
    if norm > 0:
        mean = mean / norm
    return tuple(float(x) for x in mean)


def _match(target_seq: str, candidate_seq: str, config: MatcherConfig = None):
    return match_segments(
        make_segments(target_seq),
        make_segments(candidate_seq),
        target_id="target-1",
        target_version="v1",
        candidate_id="candidate-1",
        config=config,
    )


# 1. exact full match
def test_exact_full_match():
    result = _match("ABCDEFGHIJ", "ABCDEFGHIJ")
    assert result.matched is True
    assert result.matched_segment_count == 10
    assert result.temporal_offset_s == pytest.approx(0.0)
    assert result.mean_similarity == pytest.approx(1.0)
    assert result.target_start == 0.0 and result.candidate_start == 0.0
    assert result.target_end == 50.0 and result.candidate_end == 50.0


# 2. partial middle match
def test_partial_middle_match():
    result = _match("ABCDEFGHIJ", "CDEF")
    assert result.matched is True
    assert result.matched_segment_count == 4
    assert result.target_start == 10.0 and result.target_end == 30.0
    assert result.candidate_start == 0.0 and result.candidate_end == 20.0
    assert result.temporal_offset_s == pytest.approx(10.0)


# 3. shifted start (extra unrelated segments before the matched region)
def test_shifted_start():
    result = _match("ABCDEFGHIJ", "XYCDEF")
    assert result.matched is True
    assert result.matched_segment_count == 4
    assert result.target_start == 10.0 and result.target_end == 30.0
    assert result.candidate_start == 10.0 and result.candidate_end == 30.0
    assert result.temporal_offset_s == pytest.approx(0.0)


# 4. shifted end (extra unrelated segment after the matched region)
def test_shifted_end():
    result = _match("ABCDEFGHIJ", "CDEFZ")
    assert result.matched is True
    assert result.matched_segment_count == 4
    assert result.candidate_start == 0.0 and result.candidate_end == 20.0


# 5. different candidate duration — the phase brief's own worked example
def test_brief_example_shifted_start_and_end():
    result = _match("ABCDEFGHIJ", "XYCDEFZ")
    assert result.matched is True
    assert result.matched_segment_count == 4
    assert result.target_start == 10.0 and result.target_end == 30.0
    assert result.candidate_start == 10.0 and result.candidate_end == 30.0
    assert result.temporal_offset_s == pytest.approx(0.0)
    pair_targets = sorted(p.target_segment_index for p in result.matched_pairs)
    pair_candidates = sorted(p.candidate_segment_index for p in result.matched_pairs)
    assert pair_targets == [2, 3, 4, 5]
    assert pair_candidates == [2, 3, 4, 5]


# 6. isolated high-similarity segment must not count as a match
def test_isolated_high_similarity_segment_is_not_a_match():
    result = _match("ABCDEFGHIJ", "XCY")
    assert result.matched is False
    assert result.matched_segment_count == 1
    assert result.mean_similarity == pytest.approx(1.0)


# 7. non-monotonic accidental matches
def test_non_monotonic_accidental_matches_do_not_form_a_run():
    result = _match("ABCDEFGHIJ", "FAC")
    assert result.matched is False
    assert result.matched_segment_count == 1


# 8. reordered segments — same content, wrong order
def test_reordered_segments_do_not_match():
    result = _match("ABCDEFGHIJ", "FEDC")
    assert result.matched is False
    assert result.matched_segment_count == 1


# 9. repeated scene/content resolves to the correct continuous run
def test_repeated_content_resolves_correct_run():
    result = _match("ABCDECFG", "CDE")
    assert result.matched is True
    assert result.matched_segment_count == 3
    assert result.target_start == 10.0 and result.target_end == 25.0
    assert result.candidate_start == 0.0 and result.candidate_end == 15.0


# 10. no match
def test_no_match():
    result = _match("ABCDEFGHIJ", "KLMNOP")
    assert result.matched is False
    assert result.matched_segment_count == 0
    assert result.score == 0.0
    assert result.mean_similarity is None
    assert result.temporal_offset_s is None


# 11. empty input
def test_empty_candidate_segments():
    result = match_segments(
        make_segments("ABCDE"), (), target_id="t", target_version="v1", candidate_id="c"
    )
    assert result.matched is False
    assert result.total_target_segments == 5
    assert result.total_candidate_segments == 0


def test_empty_target_segments():
    result = match_segments(
        (), make_segments("ABCDE"), target_id="t", target_version="v1", candidate_id="c"
    )
    assert result.matched is False
    assert result.total_target_segments == 0
    assert result.total_candidate_segments == 5


def test_both_empty():
    result = match_segments((), (), target_id="t", target_version="v1", candidate_id="c")
    assert result.matched is False
    assert result.total_target_segments == 0
    assert result.total_candidate_segments == 0


# 12. malformed segment metadata (segment_index not strictly increasing)
def test_malformed_segment_metadata_raises():
    good = make_segments("AB")
    malformed = (good[1], good[0])  # out of order
    with pytest.raises(ValueError):
        match_segments(malformed, make_segments("AB"), target_id="t", target_version="v1", candidate_id="c")


def test_inconsistent_dimensionality_within_one_side_raises():
    a = SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(1.0, 0.0, 0.0))
    b = SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(1.0, 0.0))
    with pytest.raises(ValueError):
        match_segments((a, b), make_segments("AB"), target_id="t", target_version="v1", candidate_id="c")


# 13. mismatched embedding dimensions between target and candidate
def test_mismatched_embedding_dimensions_between_sides_raises():
    target = (SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(1.0, 0.0, 0.0)),)
    candidate = (SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(1.0, 0.0)),)
    with pytest.raises(ValueError):
        match_segments(target, candidate, target_id="t", target_version="v1", candidate_id="c")


# -- coarse screening ------------------------------------------------------


def test_coarse_screen_passes_for_similar_videos():
    passed, sim = coarse_screen(coarse_vector_for("ABCDEFGHIJ"), coarse_vector_for("ABCDEFG"))
    assert passed is True
    assert sim > 0.6


def test_coarse_screen_rejects_dissimilar_videos():
    passed, sim = coarse_screen(coarse_vector_for("ABCDEFGHIJ"), coarse_vector_for("KLMNOPQRST"))
    assert passed is False
    assert sim == pytest.approx(0.0)


def test_coarse_gate_short_circuits_segment_matching():
    """Even though the segments would fully match, a failing coarse
    screen must prevent the (expensive) segment-level pass from running
    at all — the coarse-to-fine behavior the phase brief requires."""
    result = match_segments(
        make_segments("ABCDEFGHIJ"),
        make_segments("ABCDEFGHIJ"),
        target_id="t",
        target_version="v1",
        candidate_id="c",
        target_coarse_vector=coarse_vector_for("ABCDEFGHIJ"),
        candidate_coarse_vector=coarse_vector_for("KLMNOPQRST"),
    )
    assert result.matched is False
    assert result.matched_segment_count == 0
    assert result.coarse_similarity == pytest.approx(0.0)


def test_matcher_version_is_recorded():
    result = _match("ABC", "ABC")
    assert result.matcher_version == "temporal_v1"
