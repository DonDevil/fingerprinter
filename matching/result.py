"""Structured output of the Phase 9 temporal matcher.

`TemporalMatchResult` is evidence *for one technique* (DINOv2 segment
matching), scoped to one (target, candidate) pair — it is not
`work_queue.results.Result` (Phase 4), which is the final, possibly
multi-technique outcome a worker commits for a job. Phase 10
("Multi-technique aggregation") is expected to consume one or more
`TemporalMatchResult`s (this technique, plus audio/OCR/watermark later)
and fold them into a `Result`/`ResultRecord`. Every field here is included
because it is either directly useful to a human/consumer or because Phase
10 will plausibly need it to combine this technique's evidence with
others — no speculative fields.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass(frozen=True)
class MatchedSegmentPair:
    """One (target segment, candidate segment) pair that is part of the
    matched run — the raw evidence a human or Phase 10 could re-verify."""

    target_segment_index: int
    candidate_segment_index: int
    similarity: float


@dataclass(frozen=True)
class TemporalMatchResult:
    """Outcome of matching one candidate video's segments against one
    target video's segments.

    - `matched` — whether a temporally-consistent run met
      `MatcherConfig.min_matched_segments` and
      `MatcherConfig.segment_similarity_threshold`. See matching/config.py
      for why these are PROVISIONAL, not validated, thresholds.
    - `score` — mean cosine similarity across the best matched run's
      pairs. Not a calibrated probability.
    - `target_start`/`target_end`/`candidate_start`/`candidate_end` —
      seconds, from the matched run's first/last segment on each side.
      `None` when `matched_segment_count == 0`.
    - `temporal_offset_s` — `target_start - candidate_start` for the
      matched run; the constant offset a legitimate copied clip should
      exhibit (see phase brief, "Temporal consistency"). `None` when there
      is no run.
    - `coarse_similarity` — the whole-video screening score that decided
      whether segment-level matching ran at all (`None` if the caller
      didn't supply coarse vectors and skipped that stage).
    - `matched_pairs` — every (target_segment_index, candidate_segment_index,
      similarity) in the winning run, most useful for debugging/audit and
      for Phase 10 to inspect evidence density, not just the summary
      fields.
    """

    matched: bool
    score: float
    target_id: str
    target_version: str
    candidate_id: str
    matcher_version: str

    matched_duration_s: float
    target_start: Optional[float]
    target_end: Optional[float]
    candidate_start: Optional[float]
    candidate_end: Optional[float]

    matched_segment_count: int
    total_target_segments: int
    total_candidate_segments: int

    temporal_offset_s: Optional[float]
    mean_similarity: Optional[float]
    coarse_similarity: Optional[float] = None

    matched_pairs: Tuple[MatchedSegmentPair, ...] = field(default_factory=tuple)
