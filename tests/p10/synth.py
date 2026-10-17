"""Seeded 768-dim synthetic "films" for P10 integration tests (no labels; geometry like test_verify)."""

from __future__ import annotations

import numpy as np

DIM = 768


def scene(seed: int, seconds: float):
    rng = np.random.default_rng(seed)
    knots = rng.standard_normal((int(seconds // 12) + 3, DIM))

    def f(t: float) -> np.ndarray:
        k = t / 12.0
        i = int(k)
        w = k - i
        v = (1 - w) * knots[i] + w * knots[i + 1]
        return v / np.linalg.norm(v)

    return f


def sample(f, start: float, seconds: float, phase: float = 0.0, noise: float = 0.0, seed: int = 0):
    rng = np.random.default_rng(seed)
    ts = np.arange(phase, seconds, 5.0)
    vecs = []
    for t in ts:
        v = f(start + t) + noise * rng.standard_normal(DIM) / np.sqrt(DIM)
        vecs.append(v / np.linalg.norm(v))
    return np.asarray(vecs, dtype=np.float32), ts.astype(np.float64)


def film(seed: int, seconds: float = 300.0):
    return sample(scene(seed, seconds), 0.0, seconds, 0.0, seed=seed)


def clip_of(seed: int, start: float, seconds: float, phase: float = 1.3, noise: float = 0.1):
    return sample(scene(seed, 2000.0), start, seconds, phase, noise, seed=seed + 1000)


def random_subject(seed: int, n: int = 6):
    rng = np.random.default_rng(seed)
    v = rng.standard_normal((n, DIM)).astype(np.float32)
    return v / np.linalg.norm(v, axis=1, keepdims=True), np.arange(n) * 5.0
