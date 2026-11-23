"""FP-01/FP-02: evaluate the registered matcher (temporal/v2 verify, equiv/v1, equiv/v2) per population, rate, film, transform.

No threshold is tuned or changed. Rates other than 0.2fps use *scaled* run parameters and are EXPERIMENTAL evidence only.
Populations (never pooled): P = same underlying segment, transformed; SF = same film, different scene; CF = different film
(random excerpts, and full-film targets); HN = mined cross-film hard-negative candidates (original vs original)."""
from __future__ import annotations

import json
import random
from collections import defaultdict

import numpy as np

from common import cfg_fp, d, jdump, jload, mark_done
from fingerprinter.verify import equivalence, verify
from rfdata import RATE_STEP, load, params_for

RATES = ["0.2fps", "1fps", "2fps", "5fps"]
FOLDS = {f"movie_{i:03d}": (i - 1) % 5 for i in range(1, 11)}  # film-grouped folds (no tuning is done; reported for leakage control)


def task(media, target, p, eqs, equiv: bool) -> dict:
    v = verify(media, target, p)
    r = {"verdict": v.verdict, "score": v.score, "run_len": v.best.matched if v.best else 0,
         "offset_s": v.best.offset_s if v.best else None, "target_start_s": v.best.target_start_s if v.best else None}
    if equiv:
        for name, e in eqs.items():
            q = equivalence(media, target, p, e)
            r[name] = q.linked
            r[name + "_cov"] = [q.coverage_a, q.coverage_b]
    return r


def run(emb=None) -> None:
    exs = {e["excerpt_id"]: e for e in jload(d("manifests", "excerpts.json"))}
    clips = [c for c in jload(d("manifests", "clips.json")) if c["result"].get("ok") or c["result"].get("cached")]
    movies = [m["movie_id"] for m in jload(d("manifests", "movies.json"))]
    hn = jload(d("pairs", "hard-negatives", "candidates.json"))
    rng = random.Random(20261123)
    rows = []
    for rate in RATES:
        p, eqs, info = params_for(rate)
        wins = {e: load("windows", e, rate) for e in exs}
        fulls = {m: load("full", m, rate) for m in movies} if rate in ("0.2fps", "1fps") else {}
        for c in clips:
            e = exs[c["excerpt_id"]]
            media = load("clips", c["clip_id"], rate)
            if media is None or wins[c["excerpt_id"]] is None:
                continue
            base = {"rate": rate, "clip_id": c["clip_id"], "movie": c["movie_id"], "transform": c["transform"], "fold": FOLDS[c["movie_id"]]}
            rows.append({**base, "pop": "P", **task(media, wins[c["excerpt_id"]], p, eqs, True)})
            for oid, oe in exs.items():
                if oid == c["excerpt_id"] or wins[oid] is None:
                    continue
                if oe["movie_id"] == c["movie_id"]:
                    rows.append({**base, "pop": "SF", "target": oid, **task(media, wins[oid], p, eqs, True)})
            others = [o for o, oe in exs.items() if oe["movie_id"] != c["movie_id"] and wins[o] is not None]
            for oid in rng.sample(others, min(8, len(others))):
                rows.append({**base, "pop": "CF", "target": oid, **task(media, wins[oid], p, eqs, True)})
            for m, fv in fulls.items():  # full-film targets: localisation + false positives against whole films
                if fv is None:
                    continue
                t = task(media, fv, p, eqs, False)
                if m == c["movie_id"]:
                    t["localised"] = bool(t["verdict"] and t["target_start_s"] is not None and abs(t["target_start_s"] - e["start_s"]) <= 10)
                    rows.append({**base, "pop": "PF", **t})
                else:
                    rows.append({**base, "pop": "CFF", "target": m, **t})
        for h in hn:  # hard-negative candidates: original vs original
            if rate in ("0.2fps", "1fps"):
                mk = h.get(f"matcher_{rate}")
                if mk:
                    rows.append({"rate": rate, "pop": "HN", "pair_id": h["pair_id"], "verdict": mk["verdict"], "score": mk["score"], "fold": -1})
            else:
                a, b = load("windows", h["pair_id"] + "_a", rate), load("windows", h["pair_id"] + "_b", rate)
                if a is not None and b is not None:
                    rows.append({"rate": rate, "pop": "HN", "pair_id": h["pair_id"], **task(a, b, p, eqs, False), "fold": -1})
    jdump(rows, d("metrics", "evaluation_tasks.json"))
    agg = aggregate(rows)
    jdump(agg, d("metrics", "evaluation_summary.json"))
    mark_done("evaluate", cfg_fp({"v": 1, "n": len(rows)}), {"tasks": len(rows)})


def pct(x):
    return None if not x else [round(float(np.percentile(x, q)), 3) for q in (0, 25, 50, 75, 100)]


def aggregate(rows: list[dict]) -> dict:
    out: dict = {}
    for rate in RATES:
        R = [r for r in rows if r["rate"] == rate]
        o = {}
        for pop in ("P", "SF", "CF", "PF", "CFF", "HN"):
            X = [r for r in R if r["pop"] == pop]
            if not X:
                continue
            m = [r for r in X if r["verdict"]]
            o[pop] = {"n": len(X), "match_rate_any": round(len(m) / len(X), 4), "match_rate_confirmed": round(sum(r["verdict"] == "confirmed" for r in X) / len(X), 4),
                      "score_pcts_0_25_50_75_100": pct([r["score"] for r in X])}
            for k in ("equiv/v1", "equiv/v2"):
                if k in X[0]:
                    o[pop][k + "_linked"] = round(sum(bool(r.get(k)) for r in X) / len(X), 4)
            if pop == "PF":
                o[pop]["localised_rate"] = round(sum(bool(r.get("localised")) for r in X) / len(X), 4)
        P = [r for r in R if r["pop"] == "P"]
        for neg in ("SF", "CF", "CFF", "HN"):
            N = [r for r in R if r["pop"] == neg]
            if P and N:
                tp = sum(bool(r["verdict"]) for r in P); fn = len(P) - tp; fp = sum(bool(r["verdict"]) for r in N)
                prec = tp / (tp + fp) if tp + fp else None; rec = tp / len(P)
                o[f"P_vs_{neg}"] = {"TP": tp, "FN": fn, "FP": fp, "TN": len(N) - fp, "precision": prec and round(prec, 4), "recall": round(rec, 4),
                                    "f1": round(2 * prec * rec / (prec + rec), 4) if prec and rec else None, "note": "negatives are not the same size as positives; precision depends on the ratio"}
        o["per_transform_recall"] = {t: round(np.mean([bool(r["verdict"]) for r in P if r["transform"] == t]), 3) for t in sorted({r["transform"] for r in P})}
        o["per_film_recall"] = {m: round(np.mean([bool(r["verdict"]) for r in P if r["movie"] == m]), 3) for m in sorted({r["movie"] for r in P})}
        o["per_film_false_match_SF"] = {m: round(np.mean([bool(r["verdict"]) for r in R if r["pop"] == "SF" and r["movie"] == m] or [0]), 3) for m in sorted({r["movie"] for r in P})}
        o["registered_parameters"] = rate == "0.2fps"
        out[rate] = o
    return out
