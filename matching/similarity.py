"""Cosine similarity helpers, plain numpy — no FAISS.

Phase 8 sized a realistic target library at ~100 movies x ~540 segments
(10s granularity) = ~54,000 vectors. Phase 9's matching model (per the
worker lifecycle in `docs/design/design-proposal-1.md` §5) compares one
candidate against the *one* target its job already names — a
`len(candidate_segments) x len(target_segments)` dense matrix, not a
search across the whole target library. Even a worst-case 2-hour target at
5s segments (~1,440 segments) against a 2-hour candidate is a
1,440 x 1,440 x 768-dim matmul — roughly 1.6 x 10^9 multiply-adds, well
under a second with numpy's BLAS backend on a single CPU core. There is no
quantitative case for FAISS at this phase's scale; see phase-09 doc,
"Coarse-to-fine behavior" for the full justification and where FAISS would
actually start to matter (many-target search at crawl scale, deferred to
whichever phase builds that — not Phase 9's 1:1 job-scoped comparison).
"""
from __future__ import annotations

import numpy as np

_EPS = 1e-12


def cosine_similarity_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Pairwise cosine similarity between every row of `a` (N x D) and
    every row of `b` (M x D), returning an (N x M) matrix. Normalizes
    defensively — does not assume inputs are already unit-norm, even
    though `DINOv2EmbeddingEngine` normalizes by default — a matcher
    should not silently produce wrong scores if fed un-normalized vectors.
    """
    a_norm = np.linalg.norm(a, axis=1, keepdims=True)
    b_norm = np.linalg.norm(b, axis=1, keepdims=True)
    a_unit = a / np.clip(a_norm, _EPS, None)
    b_unit = b / np.clip(b_norm, _EPS, None)
    return a_unit @ b_unit.T


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two single vectors. Returns 0.0 for a
    zero vector on either side (undefined direction) rather than raising
    or dividing by zero."""
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na < _EPS or nb < _EPS:
        return 0.0
    return float(np.dot(a, b) / (na * nb))
