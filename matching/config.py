"""Matcher configuration — and, critically, the status of every threshold
in it.

Per the phase brief, thresholds must not be presented as validated when
they are not. None of the values below have been calibrated against a real
labeled dataset (no such dataset exists in this project yet). Every field
is annotated with its status:

- ARCHITECTURAL — a parameter whose *existence* is a design decision, not
  a fitted number (e.g. "there is a minimum run length" is architectural;
  the value 3 is provisional).
- PROVISIONAL HEURISTIC — a concrete default chosen for plausibility
  (consistent with the prototype's own values in
  `docs/design/design-proposal-1.md` §6, e.g. `cosine_threshold=0.93`) but
  not measured against labeled matches/non-matches in this codebase.
- NOT VALIDATED — explicitly called out where a value could be mistaken
  for a tuned constant.

See docs/architecture/phase-09-temporal-video-matching.md, "Threshold
status" for the full accounting.
"""
from __future__ import annotations

from dataclasses import dataclass

# Bumped when the matching *algorithm* changes in a way that would change
# match_decision for the same inputs (offset model, run-extraction logic,
# scoring formula) — independent of embedding_schema_version, which
# governs how the input vectors themselves were produced. Recorded on
# every TemporalMatchResult so Phase 10 can tell which algorithm produced
# a given piece of evidence.
MATCHER_VERSION = "temporal_v1"


@dataclass(frozen=True)
class MatcherConfig:
    """All fields below are PROVISIONAL HEURISTICS / NOT VALIDATED unless
    stated otherwise — see module docstring. Defaults are directionally
    consistent with the prototype's DINOv2 thresholds carried over in
    `docs/design/design-proposal-1.md` §6 (cosine_threshold=0.93,
    min_consecutive_frames=6, max_target_frame_step=3), scaled down for
    segment-level (coarser, sparser) matching rather than frame-level.
    """

    # Minimum cosine similarity for one candidate segment / target segment
    # pair to count as a "hit". PROVISIONAL HEURISTIC.
    segment_similarity_threshold: float = 0.90

    # Minimum coarse (whole-video mean-pooled) cosine similarity for a
    # candidate to proceed to segment-level matching against a target at
    # all — the cheap reject-fast pass. Deliberately looser than
    # segment_similarity_threshold: a real partial-clip match can have a
    # mediocre coarse similarity (most of the target is irrelevant to a
    # short clip) while still containing a strong segment-level match, so
    # this must not be tuned as tight as the segment threshold. PROVISIONAL
    # HEURISTIC — see phase-09 doc for why this number is intentionally
    # permissive rather than mirroring segment_similarity_threshold.
    coarse_similarity_threshold: float = 0.60

    # Minimum number of segments in one temporally-consistent run before a
    # candidate counts as "matched" at all. This is the mechanism that
    # prevents one accidental high-similarity segment from producing a
    # match_decision=matched result (see phase brief, "Temporal
    # consistency"). The *existence* of this gate is ARCHITECTURAL; the
    # value 3 is a PROVISIONAL HEURISTIC.
    min_matched_segments: int = 3

    # How many segments of drift from the dominant (target_index -
    # candidate_index) offset are still considered part of the same
    # temporal alignment. Tolerates small boundary/off-by-one differences
    # between target and candidate segmentation. ARCHITECTURAL parameter
    # (some tolerance must exist); value is a PROVISIONAL HEURISTIC.
    offset_tolerance_segments: int = 1

    # Maximum allowed gap, in target-segment-index units, between two
    # consecutive members of the same matched run. Tolerates missing
    # segments (a dropped frame, a brief encoding artifact, a segment that
    # individually fell under threshold) without breaking an otherwise
    # continuous run. ARCHITECTURAL parameter; value is a PROVISIONAL
    # HEURISTIC.
    max_index_gap: int = 2
