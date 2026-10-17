"""Unit tests of ``temporal/v2`` and ``equiv/v1`` (design §8, §10; validation V-1 unit part, group C).

Synthetic, seeded vectors: a "film" of unit vectors sampled every 5 s with a
random phase; clips are re-sampled views of it (another phase, noise for a
re-encode), so the tests exercise the same geometry the real data has
(audit F-1: sampling phase; F-2: identical frames in unrelated works).
"""

from __future__ import annotations

import inspect
import subprocess
import sys

import numpy as np

from fingerprinter.verify import (
    EquivParams,
    Vectors,
    VerifierParams,
    equivalence,
    exclusion_digest,
    verify,
)

DIM = 64
P = VerifierParams(s_seg=0.80, s_mean=0.80, min_run=3, tau_confirmed=3.8, tau_probable=3.6)


def scene(seed: int, seconds: float) -> callable:
    """A continuous "film": f(t) -> unit vector, smooth over ~12 s shots, distinct across shots."""
    rng = np.random.default_rng(seed)
    knots = rng.standard_normal((int(seconds // 12) + 3, DIM))

    def f(t: float) -> np.ndarray:
        k = t / 12.0
        i = int(k)
        w = k - i
        v = (1 - w) * knots[i] + w * knots[i + 1]
        return v / np.linalg.norm(v)

    return f


def sample(f, start: float, seconds: float, phase: float, *, noise: float = 0.0, seed: int = 0,
           origin: float = 0.0) -> Vectors:
    rng = np.random.default_rng(seed)
    ts = np.arange(phase, seconds, 5.0)
    vecs = []
    for t in ts:
        v = f(start + t) + noise * rng.standard_normal(DIM) / np.sqrt(DIM)
        vecs.append(v / np.linalg.norm(v))
    return Vectors(np.arange(len(ts)), ts + origin, np.asarray(vecs, dtype=np.float32), sha256=f"s{seed}")


FILM = scene(1, 900)
OTHER = scene(2, 900)


def film(phase: float = 0.0) -> Vectors:
    return sample(FILM, 0.0, 900.0, phase, seed=99)


def test_partial_clip_found_with_offset_and_phase() -> None:
    clip = sample(FILM, 300.0, 120.0, 2.3, noise=0.15, seed=3)  # starts at film time 302.3
    res = verify(clip, film(0.7), P)
    assert res.verdict == "confirmed"
    b = res.best
    assert abs(b.offset_s - 300.0) <= 5.0
    assert b.matched >= 15
    assert abs(b.target_start_s - (b.media_start_s + 300.0)) <= 5.0


def test_unrelated_content_not_matched() -> None:
    clip = sample(OTHER, 100.0, 300.0, 1.0, seed=4)
    res = verify(clip, film(), P)
    assert res.verdict is None and res.score == 0.0


def test_identical_frames_scattered_do_not_form_a_run() -> None:
    """F-2: an unrelated clip shares a few *identical* frames with the film at scattered times."""
    clip = sample(OTHER, 0.0, 300.0, 0.0, seed=5)
    tgt = film()
    vec = clip.vectors.copy()
    for i, j in ((3, 40), (11, 7), (20, 150), (31, 90)):  # high similarity, no consistent offset
        vec[i] = tgt.vectors[j]
    res = verify(Vectors(clip.segment_index, clip.pts_s, vec), tgt, P)
    assert res.verdict is None


def test_identical_frames_need_minimum_run_length() -> None:
    """Two consecutive identical frames (black/fade) at a consistent offset are below L = 3."""
    clip = sample(OTHER, 0.0, 300.0, 0.0, seed=6)
    tgt = film()
    vec = clip.vectors.copy()
    vec[10], vec[11] = tgt.vectors[50], tgt.vectors[51]
    res = verify(Vectors(clip.segment_index, clip.pts_s, vec), tgt, P)
    assert res.verdict is None


def test_low_information_segments_are_neutral_not_negative_and_not_evidence() -> None:
    clip = sample(OTHER, 0.0, 300.0, 0.0, seed=7)
    tgt = film()
    vec = clip.vectors.copy()
    for k in range(5):  # five consecutive identical "logo" frames: a run of 5 without exclusion
        vec[10 + k] = tgt.vectors[60 + k]
    forged = Vectors(clip.segment_index, clip.pts_s, vec)
    assert verify(forged, tgt, P).verdict == "confirmed"
    excluded = frozenset(int(x) for x in tgt.segment_index[60:65])
    res = verify(forged, tgt, P, excluded)
    assert res.verdict is None and res.excluded == 5
    assert res.exclusion_digest == exclusion_digest(excluded)
    # a true clip whose middle crosses excluded target segments keeps one run (neutral, not a break)
    true = sample(FILM, 200.0, 150.0, 0.0, seed=8)
    excl = frozenset(range(45, 48))  # film segments at 225..235 s
    r = verify(true, film(), VerifierParams(**{**P.document(), "max_gap": 0}), excl)
    assert r.verdict == "confirmed" and r.neutral_segments >= 1
    assert all(s not in excl for s in r.best.target_segments)


def test_reordered_segments_break_runs() -> None:
    clip = sample(FILM, 300.0, 100.0, 0.0, seed=9)
    order = np.r_[10:20, 0:10]  # second half first
    shuffled = Vectors(clip.segment_index, clip.pts_s, clip.vectors[order])
    res = verify(shuffled, film(), P)
    assert all(r.matched <= 10 for r in res.runs)
    assert len({r.offset_s for r in res.runs}) == 2  # two modes, each verified separately


def test_repeated_scene_reports_several_runs() -> None:
    tgt = film()
    clip_a = sample(FILM, 100.0, 60.0, 0.0, seed=10)
    clip_b = sample(FILM, 600.0, 60.0, 0.0, seed=11)
    both = Vectors(np.arange(len(clip_a) * 2), np.r_[clip_a.pts_s, clip_b.pts_s + 60.0],
                   np.vstack([clip_a.vectors, clip_b.vectors]))
    res = verify(both, tgt, P)
    assert res.verdict == "confirmed" and len(res.runs) >= 2
    assert {round(r.offset_s / 10) for r in res.runs[:2]} == {10, 54}


def test_sparse_matches_respect_gap_tolerance() -> None:
    clip = sample(FILM, 300.0, 100.0, 0.0, seed=12)
    vec = clip.vectors.copy()
    rnd = np.random.default_rng(1).standard_normal((len(vec), DIM))
    for i in range(1, len(vec), 2):  # every other segment destroyed
        vec[i] = rnd[i] / np.linalg.norm(rnd[i])
    sparse = Vectors(clip.segment_index, clip.pts_s, vec)
    assert verify(sparse, film(), VerifierParams(**{**P.document(), "max_gap": 1})).verdict == "confirmed"
    assert verify(sparse, film(), VerifierParams(**{**P.document(), "max_gap": 0})).verdict is None


def test_phase_tolerance_boundary() -> None:
    """A clip sampled 2.5 s out of phase: found with d = 5 (both neighbours), weaker with d = 0.5."""
    clip = sample(FILM, 300.0, 120.0, 2.5, seed=13)
    wide = verify(clip, film(0.0), VerifierParams(**{**P.document(), "phase_tolerance_s": 5.0}))
    narrow = verify(clip, film(0.0), VerifierParams(**{**P.document(), "phase_tolerance_s": 0.5}))
    assert wide.verdict == "confirmed"
    assert narrow.score <= wide.score


def test_offsets_in_seconds_not_indexes() -> None:
    """Different segment numbering on the two sides does not matter: pts_s alone aligns them."""
    clip = sample(FILM, 400.0, 80.0, 1.0, seed=14)
    renum = Vectors(clip.segment_index + 1000, clip.pts_s, clip.vectors)
    a, b = verify(clip, film(), P), verify(renum, film(), P)
    assert a.verdict == b.verdict == "confirmed" and a.score == b.score and a.best.offset_s == b.best.offset_s


def test_empty_inputs() -> None:
    empty = Vectors(np.zeros(0, np.int64), np.zeros(0), np.zeros((0, DIM), np.float32))
    assert verify(empty, film(), P).verdict is None
    assert verify(film(), empty, P).verdict is None


def test_verifier_signature_has_no_ann_inputs() -> None:
    """V-1 unit: verify() takes two artifacts, a parameter set and an exclusion set — nothing else."""
    assert list(inspect.signature(verify).parameters) == ["media", "target", "params", "excluded"]


_DET = """
import sys, json, numpy as np
sys.path.insert(0, %r)
from tests.p10.test_verify import sample, FILM, film, P
from fingerprinter.verify import verify
out = []
for seed in range(20):
    clip = sample(FILM, 10.0 * seed, 90.0, seed / 7, noise=0.2, seed=seed)
    r = verify(clip, film(0.3), P)
    out.append([r.verdict, repr(r.score), [repr(x.mean_similarity) for x in r.runs], r.offsets])
print(json.dumps(out))
"""


def test_deterministic_across_processes_and_thread_counts() -> None:
    """V-1 unit: bit-identical output in two processes with different BLAS thread counts."""
    import os

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    outs = []
    for threads in ("1", "8"):
        env = {**os.environ, "OMP_NUM_THREADS": threads, "OPENBLAS_NUM_THREADS": threads, "MKL_NUM_THREADS": threads}
        outs.append(subprocess.run([sys.executable, "-c", _DET % root], capture_output=True, text=True, env=env,
                                   check=True).stdout)
    assert outs[0] == outs[1]


def test_equivalence_rule() -> None:
    full = sample(FILM, 0.0, 300.0, 0.0, noise=0.1, seed=20)
    recoded = sample(FILM, 0.0, 300.0, 1.5, noise=0.15, seed=21)
    eq = equivalence(full, recoded, P)
    assert eq.linked, eq.reason
    sub = sample(FILM, 0.0, 150.0, 0.0, seed=22)  # 50 % sub-clip: not equivalent
    assert not equivalence(full, sub, P).linked
    shifted = sample(FILM, 30.0, 300.0, 0.0, seed=23)  # same duration, shifted by 30 s
    assert not equivalence(full, shifted, P).linked
    other = sample(OTHER, 0.0, 300.0, 0.0, seed=24)
    assert not equivalence(full, other, P).linked
    assert EquivParams().coverage == 0.90
