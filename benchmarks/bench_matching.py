"""Workload E — matching-only microbenchmark (phase-11 brief, "E.
MATCHING-ONLY MICROBENCHMARK").

Benchmarks `matching.matcher.match_segments` in isolation against
deterministic synthetic embedding arrays — no video, no DINOv2, no Redis,
no GPU. The question this answers: is the O(N*M) dense cosine-similarity
matching step material compared to DINOv2 inference cost, or negligible?
See `matching/matcher.py`'s module docstring for why brute-force
(not FAISS) was chosen at Phase 9's scale — this benchmark is what Phase 11
was asked to check that choice against.

Coarse-screen is deliberately bypassed (`target_coarse_vector`/
`candidate_coarse_vector` left as `None`) so every run pays the full
O(N*M) segment-level cost this benchmark exists to measure, rather than
short-circuiting on the cheap coarse check real jobs would normally hit
first.

Run: `python -m benchmarks.bench_matching`
"""
from __future__ import annotations

import time

import numpy as np

from benchmarks import common
from embedding.result import SegmentEmbedding
from matching.config import MatcherConfig
from matching.matcher import match_segments

EMBEDDING_DIM = 768  # matches facebook/dinov2-base's hidden_size
SIZES = [(100, 100), (500, 500), (1000, 1000), (2000, 2000)]
LARGER_SIZE_BUDGET_S = 3.0  # only attempt a bigger size if the previous one was fast
REPS = 5
SEED = 20260812


def _normalize(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v)
    return v / n if n else v


def _make_segments(n: int, rng: np.random.Generator, overlap_source: np.ndarray = None, overlap_k: int = 0):
    vectors = rng.standard_normal((n, EMBEDDING_DIM)).astype(np.float64)
    vectors = np.array([_normalize(v) for v in vectors])
    if overlap_source is not None and overlap_k > 0:
        k = min(overlap_k, n, len(overlap_source))
        noise = rng.standard_normal((k, EMBEDDING_DIM)) * 0.01
        vectors[:k] = np.array([_normalize(v) for v in (overlap_source[:k] + noise)])
    segments = tuple(
        SegmentEmbedding(segment_index=i, start_time=float(i), end_time=float(i + 1), vector=tuple(vectors[i]))
        for i in range(n)
    )
    return segments, vectors


def run_one_size(target_n: int, candidate_n: int, reps: int, rng: np.random.Generator) -> dict:
    target_segments, target_vectors = _make_segments(target_n, rng)
    # Give the candidate a genuine overlapping run against the target so the
    # run-extraction path (not just the raw matrix multiply) is exercised —
    # see module docstring.
    overlap_k = min(50, target_n, candidate_n)
    candidate_segments, _ = _make_segments(candidate_n, rng, overlap_source=target_vectors, overlap_k=overlap_k)

    config = MatcherConfig()
    timings = []
    matched_flags = []
    for _ in range(reps):
        t0 = time.monotonic()
        result = match_segments(
            target_segments=list(target_segments),
            candidate_segments=list(candidate_segments),
            target_id="bench-target",
            target_version="v1",
            candidate_id="bench-candidate",
            config=config,
        )
        timings.append(time.monotonic() - t0)
        matched_flags.append(result.matched)

    stats = common.LatencyStats.from_samples(timings)
    return {
        "target_segments": target_n,
        "candidate_segments": candidate_n,
        "pairwise_comparisons": target_n * candidate_n,
        "reps": reps,
        "matched_in_any_rep": any(matched_flags),
        "matched_segment_overlap_designed": overlap_k,
        "latency": stats.__dict__,
        "comparisons_per_second_mean": (target_n * candidate_n / stats.mean_s) if stats.mean_s else None,
    }


def run_coarse_screen_benchmark(reps: int, rng: np.random.Generator) -> dict:
    from matching.matcher import coarse_screen

    target_coarse = _normalize(rng.standard_normal(EMBEDDING_DIM))
    candidate_coarse = _normalize(rng.standard_normal(EMBEDDING_DIM))
    timings = []
    for _ in range(reps):
        t0 = time.monotonic()
        coarse_screen(target_coarse, candidate_coarse, MatcherConfig())
        timings.append(time.monotonic() - t0)
    return common.LatencyStats.from_samples(timings).__dict__


def main() -> None:
    rng = np.random.default_rng(SEED)
    env = common.environment_snapshot()

    results = []
    for target_n, candidate_n in SIZES:
        r = run_one_size(target_n, candidate_n, REPS, rng)
        print(
            f"{target_n}x{candidate_n}: mean={r['latency']['mean_s']*1000:.2f}ms "
            f"p95={r['latency']['p95_s']*1000:.2f}ms "
            f"({r['comparisons_per_second_mean']:,.0f} comparisons/s)"
        )
        results.append(r)
        if r["latency"]["mean_s"] and r["latency"]["mean_s"] > LARGER_SIZE_BUDGET_S:
            print(f"stopping size sweep: {target_n}x{candidate_n} already exceeded {LARGER_SIZE_BUDGET_S}s budget")
            break
    else:
        # every configured size stayed cheap; try one larger size to see where it bends
        extra_n = SIZES[-1][0] * 2
        r = run_one_size(extra_n, extra_n, REPS, rng)
        print(f"{extra_n}x{extra_n} (extra): mean={r['latency']['mean_s']*1000:.2f}ms")
        results.append(r)

    coarse = run_coarse_screen_benchmark(reps=50, rng=rng)
    print(f"coarse_screen: mean={coarse['mean_s']*1e6:.1f}us")

    payload = {
        "workload": "E_matching_only_microbenchmark",
        "environment": env,
        "embedding_dim": EMBEDDING_DIM,
        "matcher_config": MatcherConfig().__dict__,
        "note": "coarse_screen bypassed (target/candidate_coarse_vector=None) so full O(N*M) segment matrix always computes; overlap_k synthetic segments are seeded with target vectors + noise to guarantee a real matched run exercises run-extraction, not just the matrix multiply.",
        "sizes": results,
        "coarse_screen_only": coarse,
    }
    path = common.save_result("bench_matching", payload)
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
