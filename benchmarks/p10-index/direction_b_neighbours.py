"""Direction B: why runs saturated, and what a floor change would cost (validation §13.18). Offline, exact.

1. For each of the 760 target segments (the 7 S-SRC films, cached ``dinov2-seg/2`` vectors), the exact
   number of media segments with cosine >= floor, at the three §6 scales: base = L-PAIRS clips,
   10x = + the first 500 SOAK-1000 items, 19x = + all of SOAK-1000 (reference contents excluded, as the
   own-content exclusion does). A segment whose count exceeds ``k_max_b`` saturates in every policy.
2. At 19x, per film: the direction-B candidate set (>= 2 distinct query segments, unlimited k, exact) at
   floor 0.70 and 0.80, each candidate verified with the deployed 2.1 parameters and with the *sintel*
   fold's parameters (s_seg 0.70, the only fold below 0.80): confirmed pairs and labelled true positives.

    OMP_NUM_THREADS=2 env/bin/python benchmarks/p10-index/direction_b_neighbours.py \\
        --data ~/anti_piracy/datasets/p9 --vectors ~/anti_piracy/datasets/p10/vectors \\
        --out benchmarks/p10-index/results/direction-b-neighbours.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))

from corpus import load_lpairs, load_nsoak, load_vectors  # noqa: E402

FLOORS = (0.70, 0.75, 0.80, 0.85)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    from fingerprinter.confidence import params_from_tune_eval
    from fingerprinter.verify import Vectors, VerifierParams, verify

    lp = load_lpairs(a.data)
    ns = load_nsoak(a.data, a.vectors)
    V = load_vectors(a.vectors, lp, ns)
    soak = sorted(ns.items)
    scales = {"base": list(lp.items), "10x": list(lp.items) + soak[:500], "19x": list(lp.items) + soak}
    report: dict = {"benchmark": "P10-DIRECTION-B-NEIGHBOURS", "floors": FLOORS, "scales": {}}
    T = np.vstack([V["film:" + f].vectors for f in lp.films])
    for name, items in scales.items():
        M = np.vstack([V[n].vectors for n in items])
        S = T @ M.T
        row = {"media_segments": int(M.shape[0])}
        for fl in FLOORS:
            c = (S >= fl).sum(1)
            row[f">={fl}"] = {"max": int(c.max()), "p99": int(np.percentile(c, 99)), "p50": int(np.median(c)),
                              "segments_over_512": int((c > 512).sum()), "segments_over_2048": int((c > 2048).sum()),
                              "segments_over_4096": int((c > 4096).sum())}
        report["scales"][name] = row
        print(name, json.dumps(row), flush=True)

    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    p21, _, _ = params_from_tune_eval(tune)
    f = tune["outer_folds"]["sintel"]
    c = f["selected"]
    psin = VerifierParams(s_seg=c["s_seg"], s_mean=round(c["s_seg"] + c["s_mean_plus"], 6), min_run=c["min_run"],
                          phase_tolerance_s=c["phase_tolerance_s"], max_gap=c["max_gap"],
                          vote_floor=round(c["s_seg"] - 0.10, 6), tau_confirmed=f["tau_c"], tau_probable=f["tau_p"])
    media = scales["19x"]
    M = np.vstack([V[n].vectors for n in media])
    owner = np.concatenate([[i] * len(V[n].vectors) for i, n in enumerate(media)])
    labels = {(p.item, p.target): p.label for p in lp.pairs + ns.pairs}

    def vv(r):
        return Vectors(r.segment_index, r.pts_s, r.vectors)

    per_film = {}
    for film in lp.films:
        t = V["film:" + film]
        S = t.vectors @ M.T
        sets = {}
        for fl in (0.70, 0.80):
            segs: dict[int, set[int]] = {}
            for r, cidx in zip(*np.nonzero(S >= fl), strict=True):
                segs.setdefault(int(owner[cidx]), set()).add(int(r))
            sets[fl] = {media[i] for i, s in segs.items() if len(s) >= 2}
        pos = {n for (n, tt), lab in labels.items() if tt == film and lab}
        out = {"candidates_floor_070": len(sets[0.70]), "candidates_floor_080": len(sets[0.80]),
               "labelled_positives": len(pos),
               "candidate_recall_vs_provenance_070": round(len(sets[0.70] & pos) / len(pos), 4),
               "candidate_recall_vs_provenance_080": round(len(sets[0.80] & pos) / len(pos), 4)}
        for pname, pp in (("2.1", p21), ("sintel_fold", psin)):
            conf = {fl: {n for n in sets[fl] if verify(vv(V[n]), vv(t), pp).verdict == "confirmed"} for fl in sets}
            out[pname] = {"confirmed_070": len(conf[0.70]), "confirmed_080": len(conf[0.80]),
                          "lost_by_080": sorted(conf[0.70] - conf[0.80]),
                          "true_positives_070": len(conf[0.70] & pos), "true_positives_080": len(conf[0.80] & pos)}
        per_film[film] = out
        print(film, json.dumps(out), flush=True)
    report["floor_comparison_19x"] = per_film
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
