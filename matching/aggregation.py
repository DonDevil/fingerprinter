"""Phase 10 — folds one or more technique-specific match results into the
Phase 4 `Result` contract.

`matching.result.TemporalMatchResult` (Phase 9) is evidence for exactly
one technique, scoped to one (target, candidate) pair — see its own
docstring, "Phase 10 ... is expected to consume one or more
TemporalMatchResults ... and fold them into a Result". This module is
that folding step, and it is deliberately technique-agnostic: it doesn't
know DINOv2 exists. A caller (currently `worker/matching_handler.py`)
converts whatever technique-specific result it produced into a
`TechniqueEvidence` — the one shape every technique's evidence is reduced
to — and this module combines any number of those into one `Result`.
Adding audio/OCR/watermark later (explicitly out of scope for every phase
through 9) means writing one more `<technique>_to_evidence()` converter,
not rewriting `combine()`.

Combination rule, chosen for being the simplest one that is still correct
for a single technique today and doesn't need revisiting when a second
technique is added: `matched` if *any* technique matched (piracy detection
is a disjunction of evidence — one confirmed technique is enough to flag
a candidate, per `docs/design/design-proposal-1.md`'s job model where
`techniques` on a `Job` names which run, not that all must agree).
`algorithm` records every technique that ran (`"dinov2"`, later e.g.
`"dinov2+phash"`), `confidence` is the highest score across techniques,
and the full per-technique detail — not just the summary numbers — is
preserved in `Result.evidence` as JSON so a human or a later phase can see
exactly which technique(s) contributed and why. This is a PROVISIONAL
combination rule (no cross-technique calibration data exists), not a
tuned one — see phase-10 doc, "Threshold status".

`work_queue.results` is imported lazily, inside `combine()`, not at module
level — same reasoning as `embedding/result.py`'s lazy `target.versioning`
import: `work_queue.results` pulls in `redis` (for `ResultStore`), and the
rest of `matching` (`matcher.py`, `result.py`) is deliberately
redis-independent pure similarity math. Deferring the import means
`import matching` alone never pulls in `redis` — only a caller that
actually calls `combine()` does.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Mapping, Optional, Sequence

from matching.result import TemporalMatchResult

if TYPE_CHECKING:
    from work_queue.results import Result

# The technique name Phase 9's temporal matcher's evidence is recorded
# under. Matches `Job.techniques`'s "dinov2" entry (see tests/conftest.py's
# `make_job` default) — the two are independent strings today (nothing
# enforces they match), but they name the same thing.
DINOV2_TEMPORAL_TECHNIQUE = "dinov2"


@dataclass(frozen=True)
class TechniqueEvidence:
    """One technique's verdict for one (target, candidate) pair, reduced to
    the shape `combine()` needs. `detail` is technique-specific and must be
    JSON-serializable as-is (plain str/float/int/bool/None/list/dict) —
    this module does not know how to serialize anything richer."""

    technique: str
    matcher_version: str
    matched: bool
    score: Optional[float]
    detail: Mapping[str, object]


def temporal_match_to_evidence(result: TemporalMatchResult) -> TechniqueEvidence:
    """Converts Phase 9's `TemporalMatchResult` into the technique-agnostic
    `TechniqueEvidence` shape `combine()` consumes. Keeps every field a
    human/later-phase reader could want for audit or localization, per
    `TemporalMatchResult`'s own docstring — this is exactly the "fold into
    a Result" step it names Phase 10 as owning."""
    detail = {
        "matched_duration_s": result.matched_duration_s,
        "target_start": result.target_start,
        "target_end": result.target_end,
        "candidate_start": result.candidate_start,
        "candidate_end": result.candidate_end,
        "matched_segment_count": result.matched_segment_count,
        "total_target_segments": result.total_target_segments,
        "total_candidate_segments": result.total_candidate_segments,
        "temporal_offset_s": result.temporal_offset_s,
        "mean_similarity": result.mean_similarity,
        "coarse_similarity": result.coarse_similarity,
    }
    return TechniqueEvidence(
        technique=DINOV2_TEMPORAL_TECHNIQUE,
        matcher_version=result.matcher_version,
        matched=result.matched,
        score=result.score,
        detail=detail,
    )


def combine(
    evidence: Sequence[TechniqueEvidence],
    processing_started_at: float,
    processing_completed_at: float,
) -> "Result":
    """Folds `evidence` (one entry per technique that actually ran) into
    one `Result`. Raises `ValueError` for an empty sequence — "no technique
    produced any evidence" is a caller bug (the handler should not have
    called this at all), not a `no_match` decision."""
    from work_queue.results import Result, ResultDecision

    if not evidence:
        raise ValueError("combine() requires at least one TechniqueEvidence")

    matched = any(e.matched for e in evidence)
    decision = ResultDecision.MATCH if matched else ResultDecision.NO_MATCH
    algorithm = "+".join(e.technique for e in evidence)

    scores = [e.score for e in evidence if e.score is not None]
    confidence = max(scores) if scores else None

    summary = "; ".join(
        f"{e.technique}={'match' if e.matched else 'no_match'}"
        + (f" (score={e.score:.4f})" if e.score is not None else "")
        for e in evidence
    )

    evidence_json = json.dumps(
        [
            {
                "technique": e.technique,
                "matcher_version": e.matcher_version,
                "matched": e.matched,
                "score": e.score,
                "detail": dict(e.detail),
            }
            for e in evidence
        ],
        sort_keys=True,
    )

    return Result(
        decision=decision,
        algorithm=algorithm,
        processing_started_at=processing_started_at,
        processing_completed_at=processing_completed_at,
        confidence=confidence,
        summary=summary,
        evidence=evidence_json,
    )
