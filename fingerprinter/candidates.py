"""Candidate generation: adaptive-k ANN search, grouping, caps (design §6.2-§6.3, §7; P10-D6, corrected C-14).

One **search step** pins one generation (the pointer read once) and sends
every query of the step — all query segments, every adaptive-k round — to
that collection, before a deadline of ``search_timeout_s`` measured from the
pointer read. A failed or timed-out query fails the step (never "zero
hits"); a step past its deadline is abandoned and repeated with a fresh
pointer (C-14i).

Adaptive k (§7.1): every informative query segment is searched with ``k0``;
while the k-th score is still ≥ ``floor`` and ``k < k_max`` it is searched
again with ``2k``. A segment that ends at ``k_max`` with its k-th score ≥
floor is **saturated** (more neighbours above the floor exist than were
retrieved); any saturated segment makes the run INCOMPLETE in every
direction. Hits ≥ floor are grouped by subject (target representation in
direction A, content in B and media↔media), a group is a candidate with ≥
``min_hits`` distinct query segments, groups are ranked by (distinct hits,
best score, id) and capped; the truncated count is recorded (INCOMPLETE
``candidate_cap``). Nothing here is a fallback: no exact or full scan.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fingerprinter import metrics
from fingerprinter.generations import Generations, Pointer
from fingerprinter.p10ctx import KIND_A, OPPOSITE, P10
from fingerprinter.verify import Vectors
from fingerprinter.vindex import TARGET, SearchRequest


class SearchStepAbandonedError(Exception):
    """The step's deadline passed before every query was sent; repeat it with a fresh pointer."""


class NoActiveGenerationError(Exception):
    def __init__(self, spec: str, role: str) -> None:
        super().__init__(f"no ACTIVE generation for {role} of {spec[:19]}")
        self.role = role


@dataclass
class Policy:
    k0: int
    k_max: int
    floor: float
    min_hits: int
    cap: int

    def document(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class Group:
    key: str
    payload: dict[str, Any]
    point_id: str
    segments: set[int] = field(default_factory=set)
    best: float = -1.0

    def summary(self) -> dict[str, Any]:
        return {"key": self.key, "hits": len(self.segments), "best": round(self.best, 6), "point_id": self.point_id,
                "payload": self.payload}


@dataclass
class SearchOutcome:
    pointer: Pointer
    candidates: list[Group]
    truncated: int
    saturated: int
    stats: dict[str, Any]


def policy_for(p10: P10, kind: str, cap_override: int | None = None) -> Policy:
    m = p10.settings.matching
    if kind == KIND_A:
        return Policy(m.k0, m.k_max_a, m.floor_a, m.min_hits, cap_override or m.cap_a)
    if kind == "target_vs_media":
        return Policy(m.k0, m.k_max_b, m.floor_b, m.min_hits, cap_override or m.cap_b_total)
    return Policy(m.k0, m.k_max_eq, m.floor_eq, m.min_hits, cap_override or m.cap_eq_total)


class Searcher:
    def __init__(self, p10: P10, gens: Generations) -> None:
        self.p = p10
        self.gens = gens
        self.hook = p10.hook

    def search(self, spec: str, kind: str, query: Vectors, policy: Policy, *, skip_query: set[int],
               exclude_key: str | None, hit_excluded: Callable[[dict[str, Any]], bool] | None) -> SearchOutcome:
        role = OPPOSITE[kind]
        ptr = self.gens.active(spec, role)
        if ptr is None:
            ptr = self.gens.active(spec, role, fresh=True)
        if ptr is None:
            raise NoActiveGenerationError(spec, role)
        deadline = ptr.read_mono + self.p.settings.matching.search_timeout_s
        ix = self.p.settings.index
        segs = [i for i in range(len(query)) if int(query.segment_index[i]) not in skip_query]
        results: dict[int, list] = {}
        saturated = 0
        queries = 0
        k = min(policy.k0, policy.k_max)
        active = segs
        t0 = time.monotonic()
        rounds = 0
        while active:
            if time.monotonic() > deadline:
                raise SearchStepAbandonedError(f"search step past its deadline after {queries} queries")
            reqs = [SearchRequest(vector=query.vectors[i], limit=k) for i in active]
            self.hook("searcher.before_round", kind=kind, k=k, generation=ptr.generation)
            remaining = max(deadline - time.monotonic(), 0.001)
            hits = self.p.index.search_batch(ptr.collection, reqs, hnsw_ef=max(k, ix.search_ef or 0) or None,
                                             oversampling=ix.oversampling, timeout_s=remaining)
            queries += len(reqs)
            rounds += 1
            nxt = []
            for i, h in zip(active, hits, strict=True):
                results[i] = h
                if len(h) >= k and h[-1].score >= policy.floor:
                    if k < policy.k_max:
                        nxt.append(i)
                    else:
                        saturated += 1
            active = nxt
            k = min(k * 2, policy.k_max)
        metrics.P10_QUERY_SECONDS.labels(kind=kind).observe(time.monotonic() - t0)
        metrics.P10_QUERIES.labels(kind=kind, outcome="ok").inc(queries)
        metrics.P10_SATURATED.labels(kind=kind).inc(saturated)
        groups: dict[str, Group] = {}
        n_hits = above = excluded_hits = foreign = 0
        field_ = "trep" if role == TARGET else "cid"
        for i, h in results.items():
            n_hits += len(h)
            for hit in h:
                if hit.score < policy.floor:
                    break
                above += 1
                pl = hit.payload
                if pl.get("spec") != spec:
                    foreign += 1   # a foreign point: never a candidate (orphan repair by verification)
                    continue
                key = pl.get(field_)
                if not key or key == exclude_key or (exclude_key and pl.get("cid") == exclude_key):
                    continue  # the subject's own content (B: a target's reference; A: a reference vs its target)
                if hit_excluded is not None and hit_excluded(pl):
                    excluded_hits += 1   # per-hit exclusion (C-14h): this hit does not vote
                    continue
                g = groups.get(key)
                if g is None:
                    g = groups[key] = Group(key, {k2: pl.get(k2) for k2 in ("rep", "cid", "trep", "tid", "tver", "seq",
                                                                           "k", "spec")}, hit.id)
                g.segments.add(i)
                g.best = max(g.best, hit.score)
        metrics.P10_ANN_HITS.labels(kind=kind).inc(n_hits)
        metrics.P10_HITS_ABOVE_FLOOR.labels(kind=kind).inc(above)
        metrics.P10_LOWINFO_EXCLUDED.labels(kind=kind).inc(excluded_hits + len(skip_query))
        eligible = [g for g in groups.values() if len(g.segments) >= policy.min_hits]
        eligible.sort(key=lambda g: (-len(g.segments), -g.best, g.key))
        stored = eligible[: policy.cap]
        truncated = len(eligible) - len(stored)
        metrics.P10_CANDIDATES.labels(kind=kind, stage="grouped").inc(len(groups))
        metrics.P10_CANDIDATES.labels(kind=kind, stage="below_min_hits").inc(len(groups) - len(eligible))
        metrics.P10_CANDIDATES.labels(kind=kind, stage="stored").inc(len(stored))
        metrics.P10_CANDIDATES.labels(kind=kind, stage="truncated").inc(truncated)
        stats = {"generation": ptr.generation, "query_segments": len(segs), "skipped_query_segments": len(skip_query),
                 "queries": queries, "rounds": rounds, "hits": n_hits, "hits_above_floor": above,
                 "lowinfo_excluded_hits": excluded_hits, "foreign_hits": foreign, "groups": len(groups),
                 "candidates": len(eligible), "stored": len(stored), "truncated": truncated, "saturated": saturated,
                 "policy": policy.document(), "rank_threshold": None if not truncated else
                 [len(stored[-1].segments), round(stored[-1].best, 6)],
                 "seconds": round(time.monotonic() - t0, 3)}
        return SearchOutcome(ptr, stored, truncated, saturated, stats)
