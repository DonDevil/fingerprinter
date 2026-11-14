"""Level-5 equivalence: tune equivalence-specific segment parameters on E-TUNE, evaluate on E-SET
(validation §13.19; design §10; P10-D13).

``equiv/v1`` reuses the matching technique's segment parameters (2.1: ``s_seg`` 0.80,
``vote_floor`` 0.70, ``max_gap`` 1, ``phase_tolerance_s`` 2.5). On E-SET it linked 228/300
equivalent pairs: heavy compression (H.264 400 kb/s, VP9) and a 2-frame container
timestamp shift (fMP4) leave isolated segments below 0.80, so the best run covers < 90 % of
one side. This tool selects **equivalence-only** segment parameters; the rule itself is
unchanged (one run covering ≥ 90 % of all segments of both items, duration difference ≤
max(5 s, 2 %), |offset| ≤ 10 s) and the matching thresholds of 2.1 are not touched.

Protocol: the grid is evaluated on **E-TUNE** only (``make_eset.py --avoid``: no excerpt
overlaps an E-SET excerpt in time); selection = maximum E-TUNE recall with **zero** false
links, ties broken towards the most conservative setting (higher ``s_seg``, then smaller
``max_gap``, then smaller ``phase_tolerance_s``). The selected setting is then evaluated
**once** on E-SET (held out: never used for selection), with full confusion counts, the
negative classes separately and the closest negative's coverage (margin).

    env/bin/python benchmarks/p10-index/equiv_tune.py --tune ~/anti_piracy/datasets/p10/e-tune \\
        --tune-vectors ~/anti_piracy/datasets/p10/vectors/e-tune --heldout ~/anti_piracy/datasets/p10/e-set \\
        --heldout-vectors ~/anti_piracy/datasets/p10/vectors/e-set --out benchmarks/p10-index/results/equiv-tune.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))

from corpus import confusion  # noqa: E402

GRID = {"s_seg": [0.80, 0.75, 0.70, 0.65, 0.60, 0.55], "max_gap": [1, 2], "phase_tolerance_s": [2.5, 5.0]}


def load(manifest_dir: Path, vectors: Path):
    from fingerprinter.verify import Vectors

    man = json.loads((manifest_dir / "manifest.json").read_text())
    vec = {}
    for it in man["items"]:
        z = np.load(vectors / (it["file"].replace("/", "__") + ".npz"))
        vec[it["file"]] = Vectors(z["segment_index"], z["pts_s"], z["vectors"].astype(np.float32))
    return man, vec


def segment_override(cfg: dict) -> dict:
    return {"s_seg": cfg["s_seg"], "s_mean": cfg["s_seg"], "vote_floor": round(cfg["s_seg"] - 0.10, 6),
            "max_gap": cfg["max_gap"], "phase_tolerance_s": cfg["phase_tolerance_s"]}


def evaluate(args) -> dict:
    manifest_dir, vectors, base_doc, override = args
    from fingerprinter.verify import EquivParams, VerifierParams, equivalence

    man, vec = load(Path(manifest_dir), Path(vectors))
    by = {it["file"]: it for it in man["items"]}
    params = VerifierParams.from_document(base_doc)
    eq = EquivParams() if override is None else EquivParams(rule="equiv/v2", segment=override)
    rows, neg_cls, misses = [], {}, []
    worst: dict[str, tuple[float, str, list[str]]] = {}   # closest negative per rejecting rule
    for x, y in itertools.combinations(sorted(vec), 2):
        i, j = by[x], by[y]
        label = i["role"] == j["role"] == "variant" and i["excerpt"] == j["excerpt"]
        r = equivalence(vec[x], vec[y], params, eq)
        rows.append((label, r.linked))
        if not label:
            cls = "other_excerpt" if i["excerpt"] != j["excerpt"] else \
                next(k["role"] for k in (i, j) if k["role"] != "variant") if "variant" in (i["role"], j["role"]) \
                else "negative_vs_negative"
            c = neg_cls.setdefault(cls, [0, 0])
            c[0] += 1
            c[1] += int(r.linked)
            cov = min(r.coverage_a, r.coverage_b)
            why = r.reason.split(" ")[0] if not r.linked else "LINKED"
            if cov > worst.get(why, (-1.0,))[0]:
                worst[why] = (round(cov, 4), r.reason, [x, y])
        elif not r.linked:
            misses.append({"pair": [x, y], "variants": [i["variant"], j["variant"]], "reason": r.reason})
    return {"override": override, "confusion": confusion(rows),
            "negatives_by_class": {k: {"pairs": v[0], "linked": v[1]} for k, v in sorted(neg_cls.items())},
            "closest_negative_by_rejecting_rule": {k: {"min_coverage": v[0], "reason": v[1], "pair": v[2]}
                                                   for k, v in sorted(worst.items())},
            "misses": misses}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tune", type=Path, required=True)
    ap.add_argument("--tune-vectors", type=Path, required=True)
    ap.add_argument("--heldout", type=Path, required=True)
    ap.add_argument("--heldout-vectors", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    from fingerprinter.verify import EquivParams

    t0 = time.time()
    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    base = tune["final_parameter_set"]["params"]
    configs = [dict(zip(GRID, v)) for v in itertools.product(*GRID.values())]
    jobs = [(str(a.tune), str(a.tune_vectors), base, None)] + \
           [(str(a.tune), str(a.tune_vectors), base, segment_override(c)) for c in configs]
    with ProcessPoolExecutor(a.workers) as pool:
        res = list(pool.map(evaluate, jobs))
    v1_tune, grid = res[0], res[1:]
    safe = [r for r in grid if r["confusion"]["fp"] == 0]
    if not safe:
        raise SystemExit("no grid setting has zero false links on E-TUNE")
    best = max(safe, key=lambda r: (r["confusion"]["recall"], r["override"]["s_seg"], -r["override"]["max_gap"],
                                    -r["override"]["phase_tolerance_s"]))
    with ProcessPoolExecutor(2) as pool:
        held_v1, held_sel = pool.map(evaluate, [(str(a.heldout), str(a.heldout_vectors), base, None),
                                                (str(a.heldout), str(a.heldout_vectors), base, best["override"])])
    tm = json.loads((a.tune / "manifest.json").read_text())
    hm = json.loads((a.heldout / "manifest.json").read_text())
    report = {
        "benchmark": "P10-EQUIV-TUNE", "seconds": round(time.time() - t0),
        "rule_unchanged": {k: v for k, v in EquivParams().__dict__.items() if k not in ("rule", "segment")},
        "matching_params_2_1_digest": tune["final_parameter_set"]["params_digest"],
        "grid": GRID, "selection": "max E-TUNE recall at zero E-TUNE false links; ties -> higher s_seg, smaller "
                                   "max_gap, smaller phase_tolerance_s",
        "tuning_set": {"corpus": tm.get("corpus"), "items": len(tm["items"]),
                       "excerpts": len({i["excerpt"] for i in tm["items"]}),
                       "films": sorted({i["film"] for i in tm["items"]}), "skipped": tm.get("skipped"),
                       "avoided": tm.get("avoided"), "seed": tm.get("seed")},
        "heldout_set": {"corpus": hm.get("corpus", "E-SET"), "items": len(hm["items"]),
                        "excerpts": len({i["excerpt"] for i in hm["items"]}),
                        "films": sorted({i["film"] for i in hm["items"]})},
        "tune": {"equiv_v1": {k: v for k, v in v1_tune.items() if k != "misses"},
                 "grid": [{k: v for k, v in r.items() if k != "misses"} for r in grid]},
        "selected": best["override"],
        "selected_digest": EquivParams(rule="equiv/v2", segment=best["override"]).digest(),
        "heldout": {"equiv_v1": held_v1, "selected": held_sel},
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    print(json.dumps({"selected": best["override"], "tune": best["confusion"],
                      "heldout_v1": held_v1["confusion"], "heldout_selected": held_sel["confusion"],
                      "heldout_neg": held_sel["negatives_by_class"],
                      "closest_negatives": held_sel["closest_negative_by_rejecting_rule"]}, indent=1, default=str))


if __name__ == "__main__":
    main()
