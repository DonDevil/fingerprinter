"""Temporal verifier ``temporal/v2`` and equivalence rule ``equiv/v1`` (P10 design §8, §10; P10-D7, D13).

A pure, deterministic function of exactly: the two ``fp-seg-npz/v1`` arrays
(media side, target/other side), an immutable parameter set and the set of
excluded target segments (usable low-information flags, design §7.3). No
ANN hit, score, rank, generation or direction is an input (corrected C-14):
the same pair gives the same decision whichever direction or index proposed it.

Algorithm (keeps V1's structure — dense similarity, temporal offset, ordered
runs, a minimum run length — and changes what audit F-1/F-2 showed wrong):

1. ``S = M · Tᵀ`` in float64, rounded to 1e-9 so that BLAS summation order
   cannot flip a comparison between hosts.
2. **Offset modes from the dense similarity** (never from ANN votes): every
   media segment votes ``Δ = t_T[j*] - t_M[i]`` (seconds, from ``pts_s``) for
   its best non-excluded target segment ``j*`` if ``S[i, j*] ≥ vote_floor``.
   Up to ``n_offsets`` modes: the vote with the largest support (votes within
   ``±d``; ties: larger similarity sum, then smaller Δ) gives the mode, refined
   to the lower median of its supporting votes; supporting votes are removed
   and the next mode is chosen (support ≥ ``min_mode_votes``).
3. **Phase-tolerant comparison**: for a mode Δ, media segment ``i`` at time
   ``t`` is compared with the target segments whose sampled frames lie within
   ``±d`` of ``t + Δ``; the maximum over the non-excluded ones is used. All
   in-window segments excluded ⇒ the segment is *neutral* (neither counts nor
   breaks a run); no target sample in the window ⇒ a miss.
4. **Runs**: matched (``≥ s_seg``) segments in media order, monotone in the
   target index, tolerating ``max_gap`` consecutive unmatched segments. A run
   qualifies with ``≥ L`` matched segments and mean similarity ``≥ s_mean``.
   Every qualifying run is reported (≤ 32), across all modes.
5. **Score** (definition ``run-length+mean/1``): ``n + m`` of the best run
   (``n`` matched segments, ``m`` their mean similarity) — monotone in run
   length first. Verdict: ``confirmed`` if a run qualifies and ``score ≥ τ_c``;
   ``probable`` if it qualifies and ``τ_p ≤ score < τ_c``; otherwise no match.
   ``confidence`` is *not* computed here (calibration, design §9.1).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np

TECHNIQUE_NAME = "dinov2-seg-temporal"
VERIFIER = "temporal/v2"
SCORE_DEFINITION = "run-length+mean/1"
EQUIV_RULE = "equiv/v1"
MAX_RUNS = 32
_ROUND = 1e9


@dataclass(frozen=True)
class Vectors:
    """The arrays of one ``fp-seg-npz/v1`` artifact that verification uses."""

    segment_index: np.ndarray
    pts_s: np.ndarray
    vectors: np.ndarray
    sha256: str = ""

    @classmethod
    def from_arrays(cls, arrays: dict[str, np.ndarray], sha256: str = "") -> Vectors:
        order = np.argsort(arrays["pts_s"], kind="stable")
        return cls(np.asarray(arrays["segment_index"], dtype=np.int64)[order],
                   np.asarray(arrays["pts_s"], dtype=np.float64)[order],
                   np.ascontiguousarray(np.asarray(arrays["vectors"], dtype=np.float32)[order]), sha256)

    def __len__(self) -> int:
        return int(self.segment_index.shape[0])


@dataclass(frozen=True)
class VerifierParams:
    """An immutable ``temporal/v2`` parameter set; its digest is provenance (design §9.2)."""

    s_seg: float = 0.80
    s_mean: float = 0.80
    min_run: int = 3
    phase_tolerance_s: float = 5.0
    n_offsets: int = 3
    vote_floor: float = 0.60
    min_mode_votes: int = 2
    max_gap: int = 1
    tau_confirmed: float = 3.80
    tau_probable: float = 3.60
    segment_duration_s: float = 5.0
    verifier: str = VERIFIER
    score_definition: str = SCORE_DEFINITION

    def document(self) -> dict[str, Any]:
        return dict(sorted(asdict(self).items()))

    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.document(), sort_keys=True, separators=(",", ":"))
                              .encode()).hexdigest()

    @classmethod
    def from_document(cls, doc: dict[str, Any]) -> VerifierParams:
        return cls(**doc)


@dataclass(frozen=True)
class Run:
    offset_s: float
    matched: int
    mean_similarity: float
    media_start_s: float
    media_end_s: float
    target_start_s: float
    target_end_s: float
    media_segments: tuple[int, ...]
    target_segments: tuple[int, ...]

    def aligned(self, segment_duration_s: float) -> dict[str, float]:
        """The ``AlignedSegment`` of ``match.found`` 1.0 (non-negative offsets, positive duration)."""
        duration = max(self.media_end_s - self.media_start_s, 0.0) + segment_duration_s
        return {"media_offset_seconds": round(max(self.media_start_s, 0.0), 3),
                "target_offset_seconds": round(max(self.target_start_s, 0.0), 3),
                "duration_seconds": round(duration, 3)}


@dataclass
class Verification:
    verdict: str | None              # "confirmed" | "probable" | None
    score: float
    runs: list[Run]
    params_digest: str
    exclusion_digest: str
    excluded: int
    neutral_segments: int
    offsets: list[float]
    media_segments: int
    target_segments: int
    inputs: dict[str, str] = field(default_factory=dict)

    @property
    def best(self) -> Run | None:
        return self.runs[0] if self.runs else None

    def summary(self) -> dict[str, Any]:
        b = self.best
        return {"verdict": self.verdict, "score": self.score, "runs": len(self.runs),
                "best": None if b is None else {"offset_s": b.offset_s, "matched": b.matched,
                                                 "mean_similarity": b.mean_similarity},
                "offsets": self.offsets, "excluded": self.excluded, "neutral": self.neutral_segments,
                "params_digest": self.params_digest, "exclusion_digest": self.exclusion_digest,
                "media_segments": self.media_segments, "target_segments": self.target_segments,
                "inputs": self.inputs}


def exclusion_digest(excluded: frozenset[int] | set[int]) -> str:
    return hashlib.sha256(",".join(str(int(x)) for x in sorted(excluded)).encode()).hexdigest()


def similarity(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    s = a.astype(np.float64) @ b.astype(np.float64).T
    return np.round(s * _ROUND) / _ROUND


def _modes(votes: np.ndarray, sims: np.ndarray, d: float, n_modes: int, min_votes: int) -> list[float]:
    order = np.lexsort((sims, votes))
    v, s = votes[order], sims[order]
    alive = np.ones(len(v), dtype=bool)
    out: list[float] = []
    for _ in range(n_modes):
        idx = np.where(alive)[0]
        if len(idx) < min_votes:
            break
        va, sa = v[idx], s[idx]
        lo = np.searchsorted(va, va - d - 1e-9, side="left")
        hi = np.searchsorted(va, va + d + 1e-9, side="right")
        support = hi - lo
        csum = np.concatenate([[0.0], np.cumsum(sa)])
        ssum = csum[hi] - csum[lo]
        # max support, then larger similarity sum, then smaller offset (deterministic)
        best = np.lexsort((va, -ssum, -support))[0]
        if support[best] < min_votes:
            break
        members = idx[lo[best]:hi[best]]
        members_v = np.sort(v[members])
        out.append(float(members_v[(len(members_v) - 1) // 2]))
        alive[members] = False
    return out


def _runs(sim_m: np.ndarray, media: Vectors, target: Vectors, delta: float, excl: np.ndarray,
          p: VerifierParams) -> tuple[list[Run], int]:
    t_sorted = target.pts_s
    runs: list[Run] = []
    neutral = 0
    cur: list[tuple[int, int, float]] = []
    gap = 0

    def close() -> None:
        if cur:
            n = len(cur)
            mean = float(np.mean([c[2] for c in cur]))
            runs.append(Run(delta, n, round(mean, 9), float(media.pts_s[cur[0][0]]), float(media.pts_s[cur[-1][0]]),
                            float(target.pts_s[cur[0][1]]), float(target.pts_s[cur[-1][1]]),
                            tuple(int(media.segment_index[c[0]]) for c in cur),
                            tuple(int(target.segment_index[c[1]]) for c in cur)))
            cur.clear()

    for i in range(len(media)):
        centre = media.pts_s[i] + delta
        lo = int(np.searchsorted(t_sorted, centre - p.phase_tolerance_s - 1e-9, side="left"))
        hi = int(np.searchsorted(t_sorted, centre + p.phase_tolerance_s + 1e-9, side="right"))
        if hi <= lo:
            state, j, sim = "miss", -1, 0.0
        else:
            window = np.arange(lo, hi)
            usable = window[~excl[window]]
            if usable.size == 0:
                neutral += 1
                continue  # neutral: neither counts nor breaks a run
            row = sim_m[i, usable]
            k = int(np.argmax(row))  # first maximum: smallest target index on ties
            j, sim = int(usable[k]), float(row[k])
            state = "match" if sim >= p.s_seg else "miss"
        if state == "match":
            if cur and j < cur[-1][1]:
                close()  # runs are monotone in both time axes
            cur.append((i, j, sim))
            gap = 0
        else:
            gap += 1
            if gap > p.max_gap:
                close()
                gap = 0
    close()
    return runs, neutral


def verify(media: Vectors, target: Vectors, params: VerifierParams,
           excluded: frozenset[int] = frozenset()) -> Verification:
    """Decide one (media, target) pair. ``excluded`` = target segment indexes with a usable flag."""
    excl_digest = exclusion_digest(excluded)
    base = dict(params_digest=params.digest(), exclusion_digest=excl_digest, excluded=len(excluded),
                media_segments=len(media), target_segments=len(target),
                inputs={"media_sha256": media.sha256, "target_sha256": target.sha256})
    if len(media) == 0 or len(target) == 0:
        return Verification(None, 0.0, [], neutral_segments=0, offsets=[], **base)
    sim_m = similarity(media.vectors, target.vectors)
    excl = np.isin(target.segment_index, np.fromiter(excluded, dtype=np.int64, count=len(excluded)))
    masked = np.where(excl[None, :], -np.inf, sim_m)
    jstar = np.argmax(masked, axis=1)
    best = masked[np.arange(len(media)), jstar]
    voting = best >= params.vote_floor
    votes = target.pts_s[jstar[voting]] - media.pts_s[voting]
    offsets = _modes(np.round(votes, 6), best[voting], params.phase_tolerance_s, params.n_offsets,
                     params.min_mode_votes)
    runs: list[Run] = []
    neutral = 0
    for delta in offsets:
        r, n = _runs(sim_m, media, target, delta, excl, params)
        runs.extend(r)
        neutral = max(neutral, n)
    qualifying = [r for r in runs if r.matched >= params.min_run and r.mean_similarity >= params.s_mean]
    qualifying.sort(key=lambda r: (-r.matched, -r.mean_similarity, r.media_start_s, r.offset_s))
    qualifying = qualifying[:MAX_RUNS]
    score = 0.0
    verdict = None
    if qualifying:
        b = qualifying[0]
        score = round(b.matched + b.mean_similarity, 9)
        if score >= params.tau_confirmed:
            verdict = "confirmed"
        elif score >= params.tau_probable:
            verdict = "probable"
    return Verification(verdict, score, qualifying, neutral_segments=neutral, offsets=offsets, **base)


def best_score(media: Vectors, target: Vectors, params: VerifierParams,
               excluded: frozenset[int] = frozenset()) -> tuple[float, int, float]:
    """(score, run length, mean) of the best run that meets ``s_seg``, ignoring ``L``/``s_mean``/τ —
    the raw score used to fit thresholds and calibration (validation §3)."""
    res = verify(media, target, VerifierParams(**{**params.document(), "min_run": 1, "s_mean": -1.0,
                                                  "tau_confirmed": 1e9, "tau_probable": 1e9}), excluded)
    if not res.runs:
        return 0.0, 0, 0.0
    b = res.runs[0]
    return round(b.matched + b.mean_similarity, 9), b.matched, b.mean_similarity


# --- equivalence (design §10, equiv/v1) ---------------------------------------------------


@dataclass(frozen=True)
class EquivParams:
    """An immutable equivalence rule version. ``segment`` (``equiv/v2`` onwards) overrides the segment
    parameters of the run search (``s_seg``, ``s_mean``, ``vote_floor``, ``max_gap``,
    ``phase_tolerance_s``) for equivalence only; ``None`` (``equiv/v1``) uses the matching technique's.
    The coverage, duration and offset rule is the same in every version (P10-D13)."""

    coverage: float = 0.90
    max_duration_diff_s: float = 5.0
    max_duration_diff_frac: float = 0.02
    max_offset_s: float = 10.0
    rule: str = EQUIV_RULE
    segment: dict[str, Any] | None = None

    def document(self) -> dict[str, Any]:
        doc = asdict(self)
        if doc["segment"] is None:
            del doc["segment"]  # equiv/v1's digest is unchanged by the field's introduction
        return doc

    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.document(), sort_keys=True).encode()).hexdigest()

    def run_params(self, params: VerifierParams) -> VerifierParams:
        return params if self.segment is None else VerifierParams.from_document({**params.document(),
                                                                                 **self.segment})


@dataclass
class Equivalence:
    linked: bool
    coverage_a: float
    coverage_b: float
    offset_s: float | None
    duration_a_s: float
    duration_b_s: float
    reason: str
    verification: Verification
    rule: str = EQUIV_RULE
    rule_digest: str = ""

    def evidence(self) -> dict[str, Any]:
        return {"linked": self.linked, "coverage_a": self.coverage_a, "coverage_b": self.coverage_b,
                "offset_s": self.offset_s, "duration_a_s": self.duration_a_s, "duration_b_s": self.duration_b_s,
                "reason": self.reason, "rule": self.rule, "rule_digest": self.rule_digest,
                "verification": self.verification.summary()}


# Immutable equivalence rule versions (design §10, validation §13.19). A new version is added here, never
# changed in place; a link records its rule and rule digest, and links of a rule other than equiv/v1 are
# keyed by ``<technique version>+<rule>`` so two rule versions never share a link row.
EQUIV_RULES: dict[str, EquivParams] = {
    EQUIV_RULE: EquivParams(),
    # selected on E-TUNE only (max recall at zero false links, most conservative of the plateau), held-out
    # E-SET 280/300 linked, 0/12,420 false links: benchmarks/p10-index/results/equiv-tune.json
    "equiv/v2": EquivParams(rule="equiv/v2", segment={"s_seg": 0.65, "s_mean": 0.65, "vote_floor": 0.55,
                                                      "max_gap": 1, "phase_tolerance_s": 2.5}),
}


def equiv_link_version(technique_version: str, rule: str) -> str:
    return technique_version if rule == EQUIV_RULE else f"{technique_version}+{rule}"


def _duration(v: Vectors, seg: float) -> float:
    return float(v.pts_s.max() - v.pts_s.min() + seg) if len(v) else 0.0


def equivalence(a: Vectors, b: Vectors, params: VerifierParams, eq: EquivParams | None = None) -> Equivalence:
    """``equiv/v1``: one verified run covering ≥ 90 % of **all** segments of both items, durations
    within max(5 s, 2 %), |offset| ≤ 10 s. No exclusion applies between media (C-14n).
    Callers order the pair canonically (by content id) so the decision is symmetric."""
    eq = eq or EquivParams()
    res = verify(a, b, eq.run_params(params), frozenset())
    da, db_ = _duration(a, params.segment_duration_s), _duration(b, params.segment_duration_s)
    if not res.runs:
        return Equivalence(False, 0.0, 0.0, None, da, db_, "no qualifying run", res, eq.rule, eq.digest())
    best = max(res.runs, key=lambda r: (r.matched, r.mean_similarity, -abs(r.offset_s)))
    cov_a = len(set(best.media_segments)) / max(len(a), 1)
    cov_b = len(set(best.target_segments)) / max(len(b), 1)
    limit = max(eq.max_duration_diff_s, eq.max_duration_diff_frac * max(da, db_))
    if min(cov_a, cov_b) < eq.coverage:
        reason, ok = f"coverage {cov_a:.3f}/{cov_b:.3f} < {eq.coverage}", False
    elif abs(da - db_) > limit:
        reason, ok = f"duration difference {abs(da - db_):.2f}s > {limit:.2f}s", False
    elif abs(best.offset_s) > eq.max_offset_s:
        reason, ok = f"offset {best.offset_s:.2f}s > {eq.max_offset_s}s", False
    else:
        reason, ok = "equivalent", True
    return Equivalence(ok, round(cov_a, 6), round(cov_b, 6), best.offset_s, round(da, 3), round(db_, 3), reason, res,
                       eq.rule, eq.digest())
