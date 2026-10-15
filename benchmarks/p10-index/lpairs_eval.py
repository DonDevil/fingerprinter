"""P10 audit: matcher evidence on L-PAIRS from the local vector cache (no index involved).

1. Reproduces the P9 V1-oracle result (``matching.matcher.match_segments``,
   today's ``MatcherConfig``, no coarse screen — exactly as ``precision.py``
   ran it) to show that the cache holds the vectors P9 evaluated
   (expected: TP 37, FN 113, FP 0, TN 330). Also runs the V1 *worker* path,
   which applies the coarse screen first (``worker/matching_handler.py``).
2. Ground-truth alignment of every positive: clip time t maps to film time
   ``t + clip.reel_start_s - film.reel_start_s`` (labels by construction);
   records the cosine to the nearest target segment frame and the time
   distance between the two sampled frames (segment-phase offset).
3. Hard-negative similarity: per negative pair, the best cosine of any clip
   segment against any segment of the wrong film.
4. Exploratory threshold sensitivity of the V1 algorithm (in-sample, NOT a
   tuned setting: P10 tunes on a held-out split, validation plan §3).
5. Coarse (mean-pooled) cosine, positives vs negatives.

    env/bin/python benchmarks/p10-index/lpairs_eval.py --cache ~/anti_piracy/datasets/p10/vectors/l-pairs \
        --corpus ~/anti_piracy/datasets/p9/l-pairs --reel ~/anti_piracy/datasets/p9/reel/manifest.json \
        --out benchmarks/p10-index/results/lpairs-eval.json
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]


def q(a, ps=(0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)) -> dict:
    a = np.asarray(a, dtype=np.float64)
    if a.size == 0:
        return {"n": 0}
    return {"n": int(a.size), "min": round(float(a.min()), 4), "max": round(float(a.max()), 4),
            **{f"p{int(p * 100):02d}": round(float(np.quantile(a, p)), 4) for p in ps}}


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from embedding.result import SegmentEmbedding
    from matching.config import MatcherConfig
    from matching.matcher import match_segments

    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", type=Path, required=True)
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--reel", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    manifest = json.loads((a.corpus / "manifest.json").read_text())
    reel = json.loads(a.reel.read_text())
    film_start = {f["id"]: f["reel_start_s"] for f in reel["films"]}
    items = {Path(i["file"]).name: i for i in manifest["items"]}
    data = {}
    for i in manifest["items"]:
        z = np.load(a.cache / (i["file"].replace("/", "__") + ".npz"))
        data[Path(i["file"]).name] = (z["segment_index"], z["pts_s"], z["vectors"].astype(np.float32))
    rows = list(csv.DictReader((a.corpus / "l-pairs.csv").open()))
    report: dict = {"benchmark": "P10-AUDIT-LPAIRS", "corpus_manifest_sha256":
                    hashlib.sha256((a.corpus / "manifest.json").read_bytes()).hexdigest(),
                    "pairs_csv_sha256": hashlib.sha256((a.corpus / "l-pairs.csv").read_bytes()).hexdigest(),
                    "cache_index": json.loads((a.cache / "index.json").read_text()).get("spec_digest"),
                    "items": len(data), "segments": int(sum(len(v[0]) for v in data.values()))}

    def segs(name):
        k, t, v = data[name]
        return [SegmentEmbedding(int(kk), float(tt), float(tt) + 5.0, tuple(map(float, vv)))
                for kk, tt, vv in zip(k, t, v)]

    seg_cache = {n: segs(n) for n in data}

    def coarse(name):
        v = data[name][2].mean(0)
        return v / max(np.linalg.norm(v), 1e-12)

    # 1. V1 oracle reproduction (no coarse) and V1 worker path (coarse screen 0.60)
    def run(cfg, use_coarse):
        out = {"tp": 0, "fn": 0, "fp": 0, "tn": 0}
        per = []
        for r in rows:
            kw = {}
            if use_coarse:
                kw = {"target_coarse_vector": coarse(r["target"]), "candidate_coarse_vector": coarse(r["clip"])}
            res = match_segments(seg_cache[r["target"]], seg_cache[r["clip"]], "t", "1", r["clip"], cfg, **kw)
            pos = r["label"] == "1"
            out["tp" if res.matched and pos else "fn" if pos else "fp" if res.matched else "tn"] += 1
            per.append((r, res))
        p = out["tp"] / max(out["tp"] + out["fp"], 1)
        rc = out["tp"] / max(out["tp"] + out["fn"], 1)
        return {**out, "precision": round(p, 4), "recall": round(rc, 4)}, per

    base_cfg = MatcherConfig()
    v1_oracle, per_oracle = run(base_cfg, False)
    v1_worker, _ = run(base_cfg, True)
    report["v1_oracle_no_coarse"] = {**v1_oracle, "config": base_cfg.__dict__,
                                     "expected_from_p9": {"tp": 37, "fn": 113, "fp": 0, "tn": 330}}
    report["v1_worker_path_with_coarse_0_60"] = v1_worker
    by_t = defaultdict(lambda: [0, 0])
    for r, res in per_oracle:
        if r["label"] == "1":
            by_t[r["transform"]][0] += int(res.matched)
            by_t[r["transform"]][1] += 1
    report["v1_oracle_recall_by_transform"] = {t: f"{m}/{n}" for t, (m, n) in sorted(by_t.items())}

    # 2. ground-truth alignment for positives
    pos_cos, pos_dt, by_transform, by_phase = [], [], defaultdict(list), defaultdict(list)
    best_any = []
    frac_ge = defaultdict(list)
    for r in rows:
        if r["label"] != "1":
            continue
        clip, tgt = items[r["clip"]], items[r["target"]]
        off = clip["reel_start_s"] - film_start[tgt["film"]]
        _, ct, cv = data[r["clip"]]
        _, tt, tv = data[r["target"]]
        sims = cv @ tv.T
        for j in range(len(ct)):
            tau = ct[j] + off
            i = int(np.argmin(np.abs(tt - tau)))
            c, dt = float(sims[j, i]), float(abs(tt[i] - tau))
            pos_cos.append(c)
            pos_dt.append(dt)
            by_transform[r["transform"]].append(c)
            by_phase["0-0.5s" if dt < 0.5 else "0.5-1.5s" if dt < 1.5 else "1.5-2.5s"].append(c)
            best_any.append(float(sims[j].max()))
        cs = np.array([float(sims[j, int(np.argmin(np.abs(tt - (ct[j] + off))))]) for j in range(len(ct))])
        for thr in (0.7, 0.8, 0.85, 0.9):
            frac_ge[thr].append(float((cs >= thr).mean()) if len(cs) else 0.0)
    report["positives_true_alignment"] = {
        "basis": "film time = clip pts_s + clip.reel_start_s - film.reel_start_s; nearest target sampled frame",
        "cosine_to_true_segment": q(pos_cos), "phase_distance_s": q(pos_dt),
        "cosine_by_transform": {k: q(v) for k, v in sorted(by_transform.items())},
        "cosine_by_phase_distance": {k: q(v) for k, v in sorted(by_phase.items())},
        "best_cosine_any_target_segment": q(best_any),
        "per_pair_fraction_of_segments_at_or_above": {str(t): q(v) for t, v in frac_ge.items()},
    }

    # 3. hard negatives: best cosine of each clip segment against the wrong film
    neg_best, neg_pair_max = [], []
    for r in rows:
        if r["label"] != "0":
            continue
        sims = data[r["clip"]][2] @ data[r["target"]][2].T
        m = sims.max(1)
        neg_best.extend(m.tolist())
        neg_pair_max.append(float(m.max()))
    report["negatives_best_cosine_per_segment"] = q(neg_best, ps=(0.5, 0.9, 0.99, 0.999))
    report["negatives_best_cosine_per_pair"] = q(neg_pair_max, ps=(0.5, 0.9, 0.99))

    # 4. exploratory sensitivity of the V1 algorithm (in-sample)
    sweep = []
    for thr in (0.70, 0.75, 0.80, 0.85, 0.90):
        for mrun in (2, 3, 4):
            res, _ = run(replace(base_cfg, segment_similarity_threshold=thr, min_matched_segments=mrun), False)
            sweep.append({"segment_threshold": thr, "min_matched_segments": mrun, **res})
    report["exploratory_v1_sweep_in_sample"] = {
        "warning": "in-sample on all 480 pairs; shows sensitivity only, not a tuned or validated setting",
        "rows": sweep}

    # 5. coarse vectors
    cpos, cneg = [], []
    for r in rows:
        c = float(coarse(r["clip"]) @ coarse(r["target"]))
        (cpos if r["label"] == "1" else cneg).append(c)
    report["coarse_cosine"] = {"positives": q(cpos), "negatives": q(cneg),
                               "positives_below_0_60": int(sum(c < 0.6 for c in cpos))}
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True))
    print(json.dumps({k: report[k] for k in ("v1_oracle_no_coarse", "v1_worker_path_with_coarse_0_60")}, indent=1))


if __name__ == "__main__":
    main()
