"""Phase 9 temporal/video matcher.

    segment embeddings -> similarity search -> temporal consistency ->
    match decision + localization

Pipeline (per phase brief, "Coarse-to-fine"):

1. `coarse_screen` — cheap whole-video cosine check on the mean-pooled
   `coarse_vector` both `VideoSegmentEmbeddingResult`s already carry
   (derived from the segments, not a separate embedding pass — see
   `DINOv2EmbeddingEngine.embed_video_segments`). A candidate that fails
   this never pays for the O(N*M) segment comparison. This is a screening
   step, not a rejection Phase 9 is confident in — see
   `MatcherConfig.coarse_similarity_threshold`'s docstring for why it is
   intentionally loose.
2. `match_segments` — dense cosine similarity between every candidate
   segment and every target segment (brute force; see
   `matching/similarity.py` for why FAISS is not justified at this
   phase's scale), then a temporal-consistency pass that keeps only
   segment pairs forming a single dominant (target_index -
   candidate_index) offset, and extracts the longest ordered run within
   that offset. Isolated accidental high-similarity segments never form a
   run long enough to pass `MatcherConfig.min_matched_segments`, which is
   precisely the "one lucky frame doesn't mean a match" requirement in the
   phase brief.

Deliberately not implemented (see phase-09 doc, "Deferred decisions" for
the full list and reasoning): dynamic time warping, multi-target search,
weighted/EM-style offset estimation. A single dominant-offset-plus-longest-
run model is the simplest first temporal-consistency model that
distinguishes sustained/ordered matches from scattered coincidences, per
the phase brief's explicit "do not over-engineer" instruction.
"""
from __future__ import annotations

from collections import Counter
from typing import Optional, Sequence, Tuple

import numpy as np

from embedding.result import SegmentEmbedding
from matching.config import MATCHER_VERSION, MatcherConfig
from matching.result import MatchedSegmentPair, TemporalMatchResult
from matching.similarity import cosine_similarity, cosine_similarity_matrix


def coarse_screen(
    target_coarse_vector: Sequence[float],
    candidate_coarse_vector: Sequence[float],
    config: Optional[MatcherConfig] = None,
) -> Tuple[bool, float]:
    """Cheap first-pass check: does this candidate deserve a full
    segment-level comparison against this target at all? Returns
    `(passed, coarse_similarity)`."""
    config = config or MatcherConfig()
    sim = cosine_similarity(np.asarray(target_coarse_vector, dtype=np.float64), np.asarray(candidate_coarse_vector, dtype=np.float64))
    return sim >= config.coarse_similarity_threshold, sim


def _validate_segments(segments: Sequence[SegmentEmbedding], label: str) -> None:
    """Sequence-level checks beyond what `SegmentEmbedding.__post_init__`
    already enforces per-segment (non-negative index, end >= start,
    non-empty vector). Raises `ValueError` with a message identifying
    which side (`label`) is malformed."""
    if not segments:
        return
    first_dim = len(segments[0].vector)
    prev_index = None
    for seg in segments:
        if len(seg.vector) != first_dim:
            raise ValueError(
                f"malformed {label} segments: inconsistent embedding dimensionality "
                f"({len(seg.vector)} vs {first_dim}) at segment_index={seg.segment_index}"
            )
        if prev_index is not None and seg.segment_index <= prev_index:
            raise ValueError(
                f"malformed {label} segments: segment_index must be strictly increasing "
                f"in presentation order, got {seg.segment_index} after {prev_index}"
            )
        prev_index = seg.segment_index


def _empty_result(
    target_id: str,
    target_version: str,
    candidate_id: str,
    total_target: int,
    total_candidate: int,
    coarse_similarity: Optional[float],
) -> TemporalMatchResult:
    return TemporalMatchResult(
        matched=False,
        score=0.0,
        target_id=target_id,
        target_version=target_version,
        candidate_id=candidate_id,
        matcher_version=MATCHER_VERSION,
        matched_duration_s=0.0,
        target_start=None,
        target_end=None,
        candidate_start=None,
        candidate_end=None,
        matched_segment_count=0,
        total_target_segments=total_target,
        total_candidate_segments=total_candidate,
        temporal_offset_s=None,
        mean_similarity=None,
        coarse_similarity=coarse_similarity,
        matched_pairs=(),
    )


def match_segments(
    target_segments: Sequence[SegmentEmbedding],
    candidate_segments: Sequence[SegmentEmbedding],
    target_id: str,
    target_version: str,
    candidate_id: str,
    config: Optional[MatcherConfig] = None,
    target_coarse_vector: Optional[Sequence[float]] = None,
    candidate_coarse_vector: Optional[Sequence[float]] = None,
) -> TemporalMatchResult:
    """Match one candidate's segments against one target's segments.

    Behavior for the edge cases the phase brief calls out explicitly:

    - Empty `target_segments` or `candidate_segments`: returns a
      not-matched result (`matched=False`, all counts/timestamps
      reflecting the empty input) rather than raising — an empty segment
      set is a legitimate "nothing to compare" state, not malformed input.
    - Out-of-order / duplicate `segment_index` within one side, or
      inconsistent vector dimensionality within one side: raises
      `ValueError` — this is malformed upstream data, not a matching
      outcome.
    - Mismatched embedding dimensionality *between* target and candidate
      (e.g. two different model/schema versions were compared): raises
      `ValueError` — comparing incompatible vector spaces is a caller
      bug, not a similarity score of 0.
    """
    config = config or MatcherConfig()
    total_target = len(target_segments)
    total_candidate = len(candidate_segments)

    if total_target == 0 or total_candidate == 0:
        return _empty_result(target_id, target_version, candidate_id, total_target, total_candidate, None)

    _validate_segments(target_segments, "target")
    _validate_segments(candidate_segments, "candidate")

    target_dim = len(target_segments[0].vector)
    candidate_dim = len(candidate_segments[0].vector)
    if target_dim != candidate_dim:
        raise ValueError(f"embedding dimension mismatch: target={target_dim}, candidate={candidate_dim}")

    coarse_similarity = None
    if target_coarse_vector is not None and candidate_coarse_vector is not None:
        passed, coarse_similarity = coarse_screen(target_coarse_vector, candidate_coarse_vector, config)
        if not passed:
            return _empty_result(target_id, target_version, candidate_id, total_target, total_candidate, coarse_similarity)

    target_matrix = np.array([s.vector for s in target_segments], dtype=np.float64)
    candidate_matrix = np.array([s.vector for s in candidate_segments], dtype=np.float64)
    # rows = candidate segments (by list position), cols = target segments (by list position)
    sim_matrix = cosine_similarity_matrix(candidate_matrix, target_matrix)

    cand_positions, targ_positions = np.where(sim_matrix >= config.segment_similarity_threshold)
    if cand_positions.size == 0:
        return _empty_result(target_id, target_version, candidate_id, total_target, total_candidate, coarse_similarity)

    # (candidate_segment_index, target_segment_index, similarity) using each
    # segment's own segment_index, not raw list position — tolerant of a
    # segment list that is a sparse subset (e.g. filtered) rather than a
    # contiguous 0..N-1 run.
    hits = [
        (
            candidate_segments[ci].segment_index,
            target_segments[ti].segment_index,
            float(sim_matrix[ci, ti]),
        )
        for ci, ti in zip(cand_positions.tolist(), targ_positions.tolist())
    ]

    offset_counts = Counter(t - c for c, t, _ in hits)
    dominant_offset = max(offset_counts.items(), key=lambda kv: kv[1])[0]

    tol = config.offset_tolerance_segments
    consistent = [(c, t, s) for c, t, s in hits if abs((t - c) - dominant_offset) <= tol]

    # One best (highest-similarity) target match per candidate segment,
    # so a segment that ties multiple targets (e.g. a repeated scene)
    # doesn't fork the run into ambiguous branches — see module docstring
    # on why a simple model is preferred over exhaustive path search.
    best_per_candidate: dict[int, Tuple[int, float]] = {}
    for c, t, s in consistent:
        current = best_per_candidate.get(c)
        if current is None or s > current[1]:
            best_per_candidate[c] = (t, s)

    ordered = sorted(best_per_candidate.items())  # [(candidate_index, (target_index, sim)), ...]

    runs: list[list[Tuple[int, int, float]]] = []
    current_run: list[Tuple[int, int, float]] = []
    prev_c: Optional[int] = None
    prev_t: Optional[int] = None
    for c, (t, s) in ordered:
        breaks_run = current_run and (c - prev_c > config.max_index_gap or t < prev_t)
        if breaks_run:
            runs.append(current_run)
            current_run = []
        current_run.append((c, t, s))
        prev_c, prev_t = c, t
    if current_run:
        runs.append(current_run)

    best_run = max(runs, key=len)
    matched_segment_count = len(best_run)
    mean_similarity = sum(s for _, _, s in best_run) / matched_segment_count

    matched = matched_segment_count >= config.min_matched_segments and mean_similarity >= config.segment_similarity_threshold

    target_by_index = {s.segment_index: s for s in target_segments}
    candidate_by_index = {s.segment_index: s for s in candidate_segments}

    run_candidate_indices = [c for c, _, _ in best_run]
    run_target_indices = [t for _, t, _ in best_run]

    candidate_start = candidate_by_index[min(run_candidate_indices)].start_time
    candidate_end = candidate_by_index[max(run_candidate_indices)].end_time
    target_start = target_by_index[min(run_target_indices)].start_time
    target_end = target_by_index[max(run_target_indices)].end_time

    matched_pairs = tuple(
        MatchedSegmentPair(target_segment_index=t, candidate_segment_index=c, similarity=s) for c, t, s in best_run
    )

    return TemporalMatchResult(
        matched=matched,
        score=mean_similarity,
        target_id=target_id,
        target_version=target_version,
        candidate_id=candidate_id,
        matcher_version=MATCHER_VERSION,
        matched_duration_s=min(candidate_end - candidate_start, target_end - target_start),
        target_start=target_start,
        target_end=target_end,
        candidate_start=candidate_start,
        candidate_end=candidate_end,
        matched_segment_count=matched_segment_count,
        total_target_segments=total_target,
        total_candidate_segments=total_candidate,
        temporal_offset_s=target_start - candidate_start,
        mean_similarity=mean_similarity,
        coarse_similarity=coarse_similarity,
        matched_pairs=matched_pairs,
    )
