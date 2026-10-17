"""P10 I-1 (b)(c)(d): nested film-grouped tuning, calibration and lowinfo evaluation (validation §3).

Offline, on the cached ``dinov2-seg/2`` vectors (no Qdrant): the candidate
path is emulated with **exact** search (the offline oracle) using the
provisional direction-A policy (k0 32 → k_max 256 doubling while the k-th
score ≥ floor 0.60, ≥ 2 distinct query segments, cap 50; a saturated query
segment makes the item's run INCOMPLETE). Candidate generation does not
depend on the verifier parameters, so the live ANN candidate sets can later
be scored with the per-fold parameter sets chosen here (validation §2.2).

Protocol (validation §3.1-§3.4):
- grouping unit = source film (7); outer loop leave-one-film-out; inside
  the 6 training films an inner leave-one-film-out produces out-of-fold
  verifier decisions/scores for every training pair;
- selection on training groups: maximum recall subject to zero training
  false positives (``τ_c`` = lowest score with zero training FP; ``τ_p`` =
  lowest score with training precision ≥ 0.9); ties prefer stricter settings;
- calibration map fitted on the inner out-of-fold scores of the 6 films:
  isotonic if ≥ 50 positives, ≥ 50 negatives, ≥ 5 films and ≥ 10 negatives
  above ``τ_p``; else Platt if it passes the reliability check; else UNAVAILABLE;
- reliability on the outer held-out folds (Brier vs constant prevalence and
  vs min-max-scaled raw score; ≤ 5 equal-mass bins, film-level bootstrap);
- final parameter set = the same procedure on all 7 films;
- ``lowinfo/v1`` (offline): target segments with cosine ≥ 0.95 to a segment
  of a different film; filtered vs unfiltered on the outer held-out pairs.

    env/bin/python benchmarks/p10-index/tune_eval.py --data ~/anti_piracy/datasets/p9 \
        --vectors ~/anti_piracy/datasets/p10/vectors --out benchmarks/p10-index/results/tune-eval.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import platform
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from corpus import Rep, confusion, load_lpairs, load_nsoak, load_vectors  # noqa: E402

from fingerprinter.verify import Vectors, VerifierParams, verify  # noqa: E402

# candidate policy, direction A (design §7.2, provisional)
K0, K_MAX, FLOOR, MIN_HITS, CAP_A = 32, 256, 0.60, 2, 50
LOWINFO_COS = 0.95

BASE_GRID = {"s_seg": (0.70, 0.75, 0.80, 0.85, 0.90), "phase_tolerance_s": (2.5, 5.0), "max_gap": (0, 1)}
FILTER_GRID = {"min_run": (2, 3, 4, 5), "s_mean_plus": (0.0, 0.05)}


def vec(r: Rep) -> Vectors:
    return Vectors(r.segment_index, r.pts_s, r.vectors)


# --- candidate generation (exact oracle emulation) ------------------------------------


def candidates(item: Rep, film_names: list[str], tvecs: np.ndarray, tfilm: np.ndarray, tseg: np.ndarray,
               excluded: set[tuple[str, int]] | None) -> tuple[dict[str, int], int]:
    """Direction A for one item: {film: distinct query segments hitting it}, saturated segment count."""
    sims = item.vectors.astype(np.float32) @ tvecs.T
    order = np.argsort(-sims, axis=1, kind="stable")
    hits: dict[str, set[int]] = defaultdict(set)
    saturated = 0
    for i in range(sims.shape[0]):
        k = K0
        while True:
            kk = min(k, sims.shape[1])
            kth = sims[i, order[i, kk - 1]]
            if kth < FLOOR or kk >= K_MAX or kk == sims.shape[1]:
                break
            k *= 2
        kk = min(k, K_MAX, sims.shape[1])
        top = order[i, :kk]
        if kk >= K_MAX and sims[i, order[i, kk - 1]] >= FLOOR and kk < sims.shape[1]:
            saturated += 1
        for j in top:
            if sims[i, j] < FLOOR:
                break
            f = film_names[tfilm[j]]
            if excluded is not None and (f, int(tseg[j])) in excluded:
                continue  # per-hit exclusion (C-14h): this hit does not vote
            hits[f].add(i)
    cands = {f: len(s) for f, s in hits.items() if len(s) >= MIN_HITS}
    ranked = sorted(cands, key=lambda f: -cands[f])[:CAP_A]
    return {f: cands[f] for f in ranked}, saturated


# --- raw runs per base configuration ------------------------------------------------------


_REPS: dict[str, Rep] = {}


def _init(reps: dict[str, Rep]) -> None:
    global _REPS
    _REPS = reps


def runs_for(args: tuple[dict, list[tuple[str, str]], dict[str, frozenset[int]]]) -> list[list[tuple[int, float]]]:
    base, pairs, excl = args
    p = VerifierParams(s_seg=base["s_seg"], s_mean=-1.0, min_run=1, phase_tolerance_s=base["phase_tolerance_s"],
                       max_gap=base["max_gap"], vote_floor=base["s_seg"] - 0.10, tau_confirmed=1e9, tau_probable=1e9)
    out = []
    for item, film in pairs:
        res = verify(vec(_REPS[item]), vec(_REPS["film:" + film]), p, excl.get(film, frozenset()))
        out.append([(r.matched, r.mean_similarity) for r in res.runs])
    return out


def score_of(runs: list[tuple[int, float]], cfg: dict) -> float:
    best = 0.0
    for n, m in runs:
        if n >= cfg["min_run"] and m >= cfg["s_seg"] + cfg["s_mean_plus"]:
            best = max(best, n + m)
    return round(best, 9)


# --- selection, calibration -----------------------------------------------------------------


def thresholds(scores: np.ndarray, labels: np.ndarray) -> tuple[float, float]:
    """τ_c: lowest score with zero FP (just above the largest negative score; scores > 0 only);
    τ_p: lowest score with precision >= 0.9 among decisions >= it."""
    neg = scores[(~labels) & (scores > 0)]
    tau_c = float(np.nextafter(neg.max(), np.inf)) if neg.size else float(scores[scores > 0].min(initial=1.0))
    cand = np.unique(scores[scores > 0])
    tau_p = tau_c
    for t in cand:  # ascending: the first t whose precision >= 0.9
        sel = scores >= t
        if sel.sum() and labels[sel].mean() >= 0.9:
            tau_p = float(min(t, tau_c))
            break
    return tau_c, tau_p


def select(scores_by_cfg: dict[int, np.ndarray], labels: np.ndarray, mask: np.ndarray,
           cfgs: list[dict]) -> tuple[int, float, float]:
    """Max training recall at zero training FP; ties: larger min_run, larger s_seg, larger s_mean_plus,
    smaller max_gap, smaller phase tolerance."""
    best = None
    for ci, cfg in enumerate(cfgs):
        s, y = scores_by_cfg[ci][mask], labels[mask]
        tau_c, tau_p = thresholds(s, y)
        recall = float(((s >= tau_c) & y).sum() / max(y.sum(), 1))
        key = (recall, cfg["min_run"], cfg["s_seg"], cfg["s_mean_plus"], -cfg["max_gap"], -cfg["phase_tolerance_s"])
        if best is None or key > best[0]:
            best = (key, ci, tau_c, tau_p)
    return best[1], best[2], best[3]


def pav(x: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Isotonic regression (pool adjacent violators): breakpoints and fitted values."""
    order = np.argsort(x, kind="stable")
    xs, ys = x[order], y[order].astype(np.float64)
    blocks = [[ys[0], 1.0, xs[0], xs[0]]]
    for xi, yi in zip(xs[1:], ys[1:]):
        blocks.append([yi, 1.0, xi, xi])
        while len(blocks) > 1 and blocks[-2][0] / blocks[-2][1] > blocks[-1][0] / blocks[-1][1]:
            s, w, lo, _ = blocks.pop(-2)
            blocks[-1][0] += s
            blocks[-1][1] += w
            blocks[-1][2] = lo
    return np.array([b[2] for b in blocks]), np.array([b[0] / b[1] for b in blocks])


def iso_apply(model: tuple[np.ndarray, np.ndarray], x: np.ndarray) -> np.ndarray:
    lo, val = model
    idx = np.clip(np.searchsorted(lo, x, side="right") - 1, 0, len(val) - 1)
    return val[idx]


def platt(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """Two-parameter logistic fit by Newton's method (Platt's target smoothing)."""
    n1, n0 = y.sum(), (~y).sum()
    t = np.where(y, (n1 + 1) / (n1 + 2), 1 / (n0 + 2))
    a, b = 0.0, float(np.log((n0 + 1) / (n1 + 1)))
    for _ in range(100):
        f = a * x + b
        p = 1 / (1 + np.exp(f))
        g_a, g_b = float(((t - p) * x).sum()), float((t - p).sum())  # gradient of the log-loss wrt (a, b)
        w = p * (1 - p)
        h_aa, h_ab, h_bb = float((w * x * x).sum()) + 1e-12, float((w * x).sum()), float(w.sum()) + 1e-12
        det = h_aa * h_bb - h_ab * h_ab
        da = (h_bb * g_a - h_ab * g_b) / det
        db = (-h_ab * g_a + h_aa * g_b) / det
        a, b = a - da, b - db
        if abs(da) < 1e-10 and abs(db) < 1e-10:
            break
    return a, b


def platt_apply(ab: tuple[float, float], x: np.ndarray) -> np.ndarray:
    return 1 / (1 + np.exp(ab[0] * x + ab[1]))


def fit_calibration(scores: np.ndarray, labels: np.ndarray, films: np.ndarray, tau_p: float) -> dict:
    pos, neg = int(labels.sum()), int((~labels).sum())
    nfilms = len(set(films.tolist()))
    neg_above = int(((~labels) & (scores >= tau_p)).sum())
    doc = {"positives": pos, "negatives": neg, "films": nfilms, "negatives_above_tau_p": neg_above,
           "isotonic_minimum": {"positives": 50, "negatives": 50, "films": 5, "negatives_above_tau_p": 10}}
    if pos >= 50 and neg >= 50 and nfilms >= 5 and neg_above >= 10:
        lo, val = pav(scores, labels)
        doc.update(method="isotonic", model={"lo": lo.tolist(), "val": val.tolist()})
    elif pos >= 2 and neg >= 2:
        a, b = platt(scores, labels)
        doc.update(method="platt", model={"a": a, "b": b})
    else:
        doc.update(method="none", model=None)
    return doc


def calib_apply(doc: dict, x: np.ndarray) -> np.ndarray:
    if doc["method"] == "isotonic":
        return iso_apply((np.array(doc["model"]["lo"]), np.array(doc["model"]["val"])), x)
    if doc["method"] == "platt":
        return platt_apply((doc["model"]["a"], doc["model"]["b"]), x)
    return np.full(len(x), np.nan)


def reliability(pred: np.ndarray, y: np.ndarray, raw: np.ndarray, films: np.ndarray, seed: int = 7) -> dict:
    ok = ~np.isnan(pred)
    pred, y, raw, films = pred[ok], y[ok].astype(float), raw[ok], films[ok]
    if len(pred) == 0:
        return {"evaluable": False}
    brier = float(((pred - y) ** 2).mean())
    prev = float(y.mean())
    brier_const = float(((prev - y) ** 2).mean())
    rs = (raw - raw.min()) / max(raw.max() - raw.min(), 1e-12)
    brier_raw = float(((rs - y) ** 2).mean())
    order = np.argsort(pred, kind="stable")
    bins = np.array_split(order, min(5, len(order)))
    uniq = sorted(set(films.tolist()))
    rng = np.random.default_rng(seed)
    rows, bins_ok = [], True
    for b in bins:
        if len(b) == 0:
            continue
        mean_pred, obs = float(pred[b].mean()), float(y[b].mean())
        boots = []
        for _ in range(1000):  # film-level bootstrap of the mean predicted probability in this bin
            pick = rng.choice(uniq, size=len(uniq), replace=True)
            idx = np.concatenate([b[films[b] == f] for f in pick]) if len(b) else b
            if len(idx):
                boots.append(float(pred[idx].mean()))
        lo, hi = (float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5))) if boots else (None, None)
        within = None if len(b) < 10 or lo is None else bool(lo - 1e-9 <= obs <= hi + 1e-9)
        if within is False:
            bins_ok = False
        rows.append({"n": len(b), "mean_pred": round(mean_pred, 4), "observed": round(obs, 4),
                     "boot95_mean_pred": [None if lo is None else round(lo, 4), None if hi is None else round(hi, 4)],
                     "within": within})
    ece = float(sum(r["n"] * abs(r["mean_pred"] - r["observed"]) for r in rows) / len(pred))
    passed = brier <= brier_const and brier <= brier_raw and bins_ok
    return {"evaluable": True, "n": len(pred), "brier": round(brier, 5), "brier_constant": round(brier_const, 5),
            "brier_raw_minmax": round(brier_raw, 5), "ece_5bins": round(ece, 5), "bins": rows, "pass": passed}


# --- V1 baselines on V2 vectors ----------------------------------------------------------------


def v1_decisions(pairs, reps) -> tuple[list[bool], list[bool]]:
    from embedding.result import SegmentEmbedding
    from matching.config import MatcherConfig
    from matching.matcher import match_segments

    cfg = MatcherConfig()
    film_cache: dict[str, list] = {}
    last: list = [None, None]

    def convert(key):
        r = reps[key]
        return [SegmentEmbedding(int(k), float(t), float(t) + 5.0, tuple(map(float, v)))
                for k, t, v in zip(r.segment_index, r.pts_s, r.vectors)]

    def segs(key):
        if key.startswith("film:"):
            if key not in film_cache:
                film_cache[key] = convert(key)
            return film_cache[key]
        if last[0] != key:  # items are visited grouped; keep one converted item only (memory)
            last[0], last[1] = key, convert(key)
        return last[1]

    def coarse(key):
        v = reps[key].vectors.mean(0)
        return v / max(np.linalg.norm(v), 1e-12)

    order = sorted(range(len(pairs)), key=lambda i: pairs[i][0])
    oracle, worker = [False] * len(pairs), [False] * len(pairs)
    for i in order:
        item, film = pairs[i]
        t, c = segs("film:" + film), segs(item)
        oracle[i] = bool(match_segments(t, c, "t", "1", item, cfg).matched)
        worker[i] = bool(match_segments(t, c, "t", "1", item, cfg, target_coarse_vector=coarse("film:" + film),
                                        candidate_coarse_vector=coarse(item)).matched)
    return oracle, worker


# --- main ------------------------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    t_start = time.time()
    lp, ns = load_lpairs(a.data), load_nsoak(a.data, a.vectors)
    reps = load_vectors(a.vectors, lp, ns)
    films = lp.films
    pairs = [(p.item, p.target, p.label, p.fold, p.corpus, p.transform,
              ns.items[p.item].vfr if p.corpus == "soak" else False)
             for p in lp.pairs + ns.pairs]
    keys = [(p[0], p[1]) for p in pairs]
    labels = np.array([p[2] for p in pairs])
    folds = np.array([p[3] for p in pairs])
    corpus_of = np.array([p[4] for p in pairs])
    print(f"pairs {len(pairs)} (+{labels.sum()}) items {len(reps)}", flush=True)

    # target role (7 films) for candidate generation and lowinfo
    tvecs = np.vstack([reps["film:" + f].vectors for f in films]).astype(np.float32)
    tfilm = np.concatenate([np.full(len(reps["film:" + f].segment_index), i) for i, f in enumerate(films)])
    tseg = np.concatenate([reps["film:" + f].segment_index for f in films])
    # lowinfo/v1 offline: flag target segments with cosine >= 0.95 to a segment of another film
    tsims = tvecs @ tvecs.T
    other = tfilm[:, None] != tfilm[None, :]
    flagged_mask = ((tsims >= LOWINFO_COS) & other).any(axis=1)
    flagged = {(films[tfilm[j]], int(tseg[j])) for j in np.where(flagged_mask)[0]}
    excl_by_film = {f: frozenset(k for ff, k in flagged if ff == f) for f in films}
    print(f"lowinfo flagged target segments: {len(flagged)}", flush=True)

    items = sorted({p[0] for p in pairs})
    cand, sat = {}, {}
    cand_f, sat_f = {}, {}
    for it in items:
        cand[it], sat[it] = candidates(reps[it], films, tvecs, tfilm, tseg, None)
        cand_f[it], sat_f[it] = candidates(reps[it], films, tvecs, tfilm, tseg, flagged)
    proposed = np.array([p[1] in cand[p[0]] for p in pairs])
    proposed_f = np.array([p[1] in cand_f[p[0]] for p in pairs])
    incomplete = np.array([sat[p[0]] > 0 for p in pairs])
    cand_stats = {
        "policy": {"k0": K0, "k_max": K_MAX, "floor": FLOOR, "min_hits": MIN_HITS, "cap": CAP_A, "search": "exact"},
        "pairs_proposed": int(proposed.sum()), "positives_proposed": int((proposed & labels).sum()),
        "positives": int(labels.sum()), "negatives_proposed": int((proposed & ~labels).sum()),
        "items_with_saturated_segments": int(sum(1 for it in items if sat[it] > 0)),
        "saturated_segments_total": int(sum(sat.values())), "items": len(items),
        "candidate_recall_vs_provenance": round(float((proposed & labels).sum() / labels.sum()), 4),
    }
    print("candidates", cand_stats, flush=True)

    # raw runs per base config (unfiltered and filtered exclusion sets)
    bases = [dict(zip(BASE_GRID, v)) for v in itertools.product(*BASE_GRID.values())]
    cfgs = [{**b, **dict(zip(FILTER_GRID, f))} for b in bases for f in itertools.product(*FILTER_GRID.values())]
    with ProcessPoolExecutor(a.workers, initializer=_init, initargs=(reps,)) as ex:
        runs_u = list(ex.map(runs_for, [(b, keys, {}) for b in bases]))
        runs_f = list(ex.map(runs_for, [(b, keys, excl_by_film) for b in bases]))
    print(f"runs computed in {time.time() - t_start:.0f}s", flush=True)

    def scores(runs_by_base, prop):
        out = {}
        for ci, cfg in enumerate(cfgs):
            bi = bases.index({k: cfg[k] for k in BASE_GRID})
            out[ci] = np.array([score_of(r, cfg) if prop[i] else 0.0 for i, r in enumerate(runs_by_base[bi])])
        return out

    S_u, S_f = scores(runs_u, proposed), scores(runs_f, proposed_f)

    def procedure(train_films: list[str], S: dict[int, np.ndarray]) -> dict:
        """Selection + inner LOFO out-of-fold scores + calibration fit, on the given training films."""
        train = np.isin(folds, train_films)
        ci, tau_c, tau_p = select(S, labels, train, cfgs)
        oof_score = np.zeros(len(pairs))
        oof_dec = np.zeros(len(pairs), dtype=bool)
        for g in train_films:
            inner = np.isin(folds, [f for f in train_films if f != g])
            cj, tc, _ = select(S, labels, inner, cfgs)
            sel = folds == g
            oof_score[sel] = S[cj][sel]
            oof_dec[sel] = S[cj][sel] >= tc
        # calibration population: proposed (candidate-generated, verified) training pairs, scored out of fold
        pop = train & proposed
        cal = fit_calibration(oof_score[pop], labels[pop], folds[pop], tau_p)
        return {"cfg_index": ci, "cfg": cfgs[ci], "tau_c": tau_c, "tau_p": tau_p, "calibration": cal,
                "inner_oof": confusion(list(zip(labels[train].tolist(), oof_dec[train].tolist())))}

    outer = {}
    pooled_pred, pooled_y, pooled_raw, pooled_f = [], [], [], []
    held_dec = np.zeros(len(pairs), dtype=bool)
    held_dec_f = np.zeros(len(pairs), dtype=bool)
    held_dec_prob = np.zeros(len(pairs), dtype=bool)
    for f in films:
        train_films = [x for x in films if x != f]
        proc = procedure(train_films, S_u)
        sel = folds == f
        s_held = S_u[proc["cfg_index"]][sel]
        dec = s_held >= proc["tau_c"]
        held_dec[sel] = dec
        held_dec_prob[sel] = (s_held >= proc["tau_p"]) & ~dec
        # lowinfo: the same procedure on filtered scores
        proc_f = procedure(train_films, S_f)
        held_dec_f[sel] = S_f[proc_f["cfg_index"]][sel] >= proc_f["tau_c"]
        popm = sel & proposed
        pred = calib_apply(proc["calibration"], S_u[proc["cfg_index"]][popm])
        pooled_pred.append(pred)
        pooled_y.append(labels[popm])
        pooled_raw.append(S_u[proc["cfg_index"]][popm])
        pooled_f.append(folds[popm])
        outer[f] = {"selected": proc["cfg"], "tau_c": proc["tau_c"], "tau_p": proc["tau_p"],
                    "calibration_method": proc["calibration"]["method"],
                    "calibration_data": {k: v for k, v in proc["calibration"].items() if k not in ("model",)},
                    "inner_oof_training": proc["inner_oof"],
                    "heldout": confusion(list(zip(labels[sel].tolist(), dec.tolist()))),
                    "heldout_lowinfo_filtered": confusion(list(zip(labels[sel].tolist(), held_dec_f[sel].tolist()))),
                    "heldout_pairs": int(sel.sum())}
        print(f"fold {f}: {outer[f]['selected']} tau_c={proc['tau_c']:.4f} heldout={outer[f]['heldout']}", flush=True)

    # V1 baselines on V2 vectors, identical pairs
    v1_oracle, v1_worker = v1_decisions(keys, reps)
    v1_oracle, v1_worker = np.array(v1_oracle), np.array(v1_worker)

    def report_for(mask):
        return {
            "p10_temporal_v2_confirmed": confusion(list(zip(labels[mask].tolist(), held_dec[mask].tolist()))),
            "p10_lowinfo_filtered": confusion(list(zip(labels[mask].tolist(), held_dec_f[mask].tolist()))),
            "v1_matcher_on_v2_vectors_no_coarse": confusion(list(zip(labels[mask].tolist(), v1_oracle[mask].tolist()))),
            "v1_worker_path_coarse_0_60": confusion(list(zip(labels[mask].tolist(), v1_worker[mask].tolist()))),
            "p10_incomplete_pairs": int((incomplete & mask).sum()),
            "pairs": int(mask.sum()),
        }

    lp_mask, ns_mask = corpus_of == "l-pairs", corpus_of == "soak"
    by_transform = {}
    for t in sorted({p[5] for p in pairs if p[5]}):
        m = np.array([p[5] == t for p in pairs])
        by_transform[t] = {"p10": confusion(list(zip(labels[m].tolist(), held_dec[m].tolist()))),
                           "v1_oracle": confusion(list(zip(labels[m].tolist(), v1_oracle[m].tolist())))}
    vfr = np.array([p[6] for p in pairs])
    lost_tp = int((held_dec & ~held_dec_f & labels).sum())
    fp_u, fp_f = int((held_dec & ~labels).sum()), int((held_dec_f & ~labels).sum())

    # final deployed parameter set: the same procedure on all 7 films
    final = procedure(films, S_u)
    rel = reliability(np.concatenate(pooled_pred), np.concatenate(pooled_y), np.concatenate(pooled_raw),
                      np.concatenate(pooled_f))
    fitted = final["calibration"]["method"] in ("isotonic", "platt")
    status = "CALIBRATED" if fitted and rel.get("pass") else "UNAVAILABLE"
    fc = final["cfg"]
    params = VerifierParams(s_seg=fc["s_seg"], s_mean=round(fc["s_seg"] + fc["s_mean_plus"], 6), min_run=fc["min_run"],
                            phase_tolerance_s=fc["phase_tolerance_s"], max_gap=fc["max_gap"],
                            vote_floor=round(fc["s_seg"] - 0.10, 6), tau_confirmed=final["tau_c"],
                            tau_probable=final["tau_p"])
    report = {
        "benchmark": "P10-I1-TUNE-EVAL",
        "populations": {"l-pairs": {"pairs": int(lp_mask.sum()), "positives": int((labels & lp_mask).sum())},
                        "n-soak": {"pairs": int(ns_mask.sum()), "positives": int((labels & ns_mask).sum()),
                                   "rule": "relevant: reel overlap >= 30 s; irrelevant: >= 15 s apart; else excluded; "
                                           "multi-film items paired only with their largest-overlap film"}},
        "manifests_sha256": {**lp.manifests, **ns.manifests},
        "spec": json.loads((a.vectors / "l-pairs" / "index.json").read_text()).get("spec_digest"),
        "candidate_generation": cand_stats,
        "grid": {"base": BASE_GRID, "filter": FILTER_GRID, "configs": len(cfgs), "vote_floor": "s_seg - 0.10",
                 "n_offsets": 3, "min_mode_votes": 2},
        "outer_folds": outer,
        "heldout_pooled": {"all": report_for(np.ones(len(pairs), dtype=bool)), "l-pairs": report_for(lp_mask),
                           "n-soak": report_for(ns_mask), "vfr_items": report_for(vfr)},
        "heldout_probable_decisions": {"total": int(held_dec_prob.sum()), "true": int((held_dec_prob & labels).sum())},
        "heldout_by_transform_l_pairs": by_transform,
        "lowinfo": {"rule": "lowinfo/v1 offline: target segment cosine >= 0.95 to a segment of another film",
                    "flagged_target_segments": len(flagged), "target_segments": len(tseg),
                    "lost_tp": lost_tp, "fp_unfiltered": fp_u, "fp_filtered": fp_f,
                    "adoption_rule": "lost_tp = 0 and FP(filtered) <= FP(unfiltered); "
                                     "benefit not established if < 20 flagged",
                    "adopt": bool(lost_tp == 0 and fp_f <= fp_u),
                    "benefit_established": len(flagged) >= 20},
        "calibration": {"final_method": final["calibration"]["method"],
                        "final_data": {k: v for k, v in final["calibration"].items() if k != "model"},
                        "final_model": final["calibration"].get("model"),
                        "outer_reliability": rel, "status": status},
        "final_parameter_set": {"technique": "dinov2-seg-temporal", "version": "2.1", "params": params.document(),
                                "params_digest": params.digest(), "selected_cfg": fc,
                                "inner_oof_all_films": final["inner_oof"]},
        "limits": ["seven animated films from one reel; clips of one film are correlated (effective sample size closer "
                   "to the number of films than to the number of pairs); no live action",
                   "candidate generation emulated with exact search; the live ANN path is measured separately "
                   "(validation §4)"],
        "versions": {"python": platform.python_version(), "numpy": np.__version__},
        "seconds": round(time.time() - t_start, 1),
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=float))
    print(json.dumps({"pooled": report["heldout_pooled"]["all"], "lowinfo": report["lowinfo"],
                      "calibration_status": status, "final": report["final_parameter_set"]["selected_cfg"]}, indent=1))


if __name__ == "__main__":
    main()
