"""Vector loading + matcher parameter scaling shared by the hard-negative and evaluation stages."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fingerprinter.verify import EQUIV_RULES, EquivParams, VerifierParams, Vectors  # noqa: E402

from common import d  # noqa: E402

RATE_STEP = {"0.2fps": 5.0, "1fps": 1.0, "2fps": 0.5, "5fps": 0.2}
RATE_MOD = {"5fps": 2, "2fps": 5, "1fps": 10, "0.2fps": 50}


def load(kind: str, name: str, rate: str, grid_s: float = 0.1) -> Vectors | None:
    """kind: full (1 s grid) | clips | windows (0.1 s grid). Time axis is zeroed at the first frame (the production
    pts are relative to the first decoded frame)."""
    p = d("embeddings", kind, f"{name}.npz")
    if not p.exists():
        return None
    z = np.load(p)
    k, pts, v = z["k"], z["pts"], z["vec"]
    if kind == "full":
        m = {"1fps": 1, "0.2fps": 5}.get(rate)
        if m is None:
            return None
    else:
        m = RATE_MOD[rate]
    sel = (k % m) == 0
    if not sel.any():
        return None
    k, pts, v = k[sel], pts[sel], v[sel]
    return Vectors.from_arrays({"segment_index": k // m, "pts_s": pts - pts[0], "vectors": v})


def params_for(rate: str) -> tuple[VerifierParams, dict[str, EquivParams], dict]:
    """0.2fps = the registered production parameters, unchanged. Denser rates: the run structure keeps its *time* meaning
    (min_run, min_mode_votes, max_gap scaled by 5 s / step; tau_* = count part scaled + the mean part) — EXPERIMENTAL."""
    step = RATE_STEP[rate]
    f = 5.0 / step
    base = VerifierParams()
    if f == 1.0:
        p = base
    else:
        p = VerifierParams(**{**base.document(), "min_run": int(round(base.min_run * f)),
                              "min_mode_votes": int(round(base.min_mode_votes * f)), "max_gap": int(round(base.max_gap * f)),
                              "segment_duration_s": step, "tau_confirmed": (base.tau_confirmed - 0.8) * f + 0.8,
                              "tau_probable": (base.tau_probable - 0.6) * f + 0.6})
    eqs = {}
    for name, e in EQUIV_RULES.items():
        if f != 1.0 and e.segment:
            e = EquivParams(**{**e.__dict__, "segment": {**e.segment, "max_gap": int(round(e.segment.get("max_gap", 1) * f))}})
        eqs[name] = e
    return p, eqs, {"scale_factor": f, "registered_parameters": f == 1.0}
